//! Store the latest reply to every active client session.
//!
//! This allows them to be resent to the corresponding client if the client missed the original
//! reply message (e.g. dropped packet).
//!
//! - Client replies' headers are stored in the `client_sessions` trailer.
//! - Client replies (header and body) are only stored by ClientReplies in the `client_replies` zone
//!   when `reply.header.size ≠ sizeOf(Header)` – that is, when the body is non-empty.
//! - Corrupt client replies can be repaired from other replicas.
//!
//! Replies are written asynchronously. Subsequent writes for the same client may be coalesced –
//! we only care about the last reply to each client session.
//!
//! ClientReplies guarantees that the latest replies are durable at checkpoint.
//!
//! If the same reply is corrupted by all replicas, the cluster is still available.
//! If the respective client also never received the reply (due to a network fault), the client may
//! be "locked out" of the cluster – continually retrying a request which has been executed, but
//! whose reply has been permanently lost. This can be resolved by the operator restarting the
//! client to create a new session.
const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const log = std.log.scoped(.client_replies);

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const RingBufferType = @import("../ring_buffer.zig").RingBufferType;
const IOPSType = @import("../iops.zig").IOPSType;
const vsr = @import("../vsr.zig");
const Message = @import("../message_pool.zig").MessagePool.Message;
const MessagePool = @import("../message_pool.zig").MessagePool;
const Slot = @import("client_sessions.zig").ReplySlot;
const ClientSessions = @import("client_sessions.zig").ClientSessions;

fn slot_offset(slot: Slot) usize {
    return slot.index * constants.message_size_max;
}

// TODO Optimization:
// Don't always immediately start writing a reply. Instead, hold onto it in the hopes that
// the same client will queue another request. If they do (within the same checkpoint),
// then we no longer need to persist the original reply.
pub fn ClientRepliesType(comptime Storage: type) type {
    return struct {
        const ClientReplies = @This();

        const Read = struct {
            client_replies: *ClientReplies,
            completion: Storage.Read,
            callback: ?*const fn (
                client_replies: *ClientReplies,
                reply_header: *const vsr.Header.Reply,
                reply: ?*Message.Reply,
                destination_replica: ?u8,
            ) void,
            slot: Slot,
            message: *Message.Reply,
            /// The header of the expected reply.
            header: vsr.Header.Reply,
            destination_replica: ?u8,
        };

        const Write = struct {
            client_replies: *ClientReplies,
            completion: Storage.Write,
            slot: Slot,
            message: *Message.Reply,
            trigger: WriteTrigger,
        };

        const WriteTrigger = enum { commit, repair };

        const WriteQueue = RingBufferType(*Write, .{
            .array = constants.client_replies_iops_write_max,
        });

        storage: *Storage,
        message_pool: *MessagePool,
        replica: u8,

        reads: IOPSType(Read, constants.client_replies_iops_read_max) = .{},
        writes: IOPSType(Write, constants.client_replies_iops_write_max) = .{},

        /// Track which slots have a write currently in progress.
        writing: std.StaticBitSet(constants.clients_max) =
            std.StaticBitSet(constants.clients_max).initEmpty(),
        /// Track which slots hold a corrupt reply, or are otherwise missing the reply
        /// that ClientSessions believes they should hold.
        ///
        /// Invariants:
        /// - Set bits must correspond to occupied slots in ClientSessions.
        /// - Set bits must correspond to entries in ClientSessions with
        ///   `header.size > @sizeOf(vsr.Header)`.
        faulty: std.StaticBitSet(constants.clients_max) =
            std.StaticBitSet(constants.clients_max).initEmpty(),

        /// Guard against multiple concurrent writes to the same slot.
        /// Pointers are into `writes`.
        write_queue: WriteQueue = WriteQueue.init(),

        ready_callback: ?*const fn (*ClientReplies) void = null,

        checkpoint_next_tick: Storage.NextTick = undefined,
        checkpoint_callback: ?*const fn (*ClientReplies) void = null,

        pub fn init(options: struct {
            storage: *Storage,
            message_pool: *MessagePool,
            replica_index: u8,
        }) ClientReplies {
            return .{
                .storage = options.storage,
                .message_pool = options.message_pool,
                .replica = options.replica_index,
            };
        }

        pub fn deinit(client_replies: *ClientReplies) void {
            {
                var it = client_replies.reads.iterate();
                while (it.next()) |read| client_replies.message_pool.unref(read.message);
            }
            {
                var it = client_replies.writes.iterate();
                while (it.next()) |write| client_replies.message_pool.unref(write.message);
            }
            // Don't unref `write_queue`'s Writes — they are a subset of `writes`.
        }

        /// Returns true if the reply at the given slot is  durably persisted to disk. The
        /// difference with `faulty` bit set is that `faulty` is cleared at the start of a write
        /// when the reply is still in RAM. In contrast, `reply_durable` checks that the
        /// corresponding reply hit the disk.
        pub fn reply_durable(
            client_replies: *const ClientReplies,
            slot: Slot,
        ) bool {
            return !client_replies.faulty.isSet(slot.index) and
                !client_replies.writing.isSet(slot.index);
        }

        pub fn read_reply_sync(
            client_replies: *ClientReplies,
            slot: Slot,
            session: *const ClientSessions.Entry,
        ) ?*Message.Reply {
            const client = session.header.client;

            if (!client_replies.writing.isSet(slot.index)) return null;

            var writes = client_replies.writes.iterate();
            var write_latest: ?*const Write = null;
            while (writes.next()) |write| {
                if (write.message.header.client == client) {
                    if (write_latest == null or
                        write_latest.?.message.header.request < write.message.header.request)
                    {
                        write_latest = write;
                    }
                }
            }

            // The reply being written to the target slot may not be for the client that we're
            // looking for. For example, it may be an old reply for a different client.
            maybe(write_latest == null);

            if (write_latest == null or
                write_latest.?.message.header.checksum != session.header.checksum)
            {
                // We are writing a reply, but that's a wrong reply according to `client_sessions`.
                // This happens after state sync, where we update `client_sessions` without
                // waiting for the in-flight write requests to complete.
                assert(client_replies.faulty.isSet(slot.index));
                return null;
            }

            assert(!client_replies.faulty.isSet(slot.index));
            return write_latest.?.message;
        }

        /// Caller must check read_reply_sync() first.
        /// (They are split up to avoid complicated NextTick bounds.)
        pub fn read_reply(
            client_replies: *ClientReplies,
            slot: Slot,
            session: *const ClientSessions.Entry,
            callback: *const fn (
                *ClientReplies,
                *const vsr.Header.Reply,
                ?*Message.Reply,
                ?u8,
            ) void,
            destination_replica: ?u8,
        ) error{Busy}!void {
            assert(client_replies.read_reply_sync(slot, session) == null);

            const read = client_replies.reads.acquire() orelse {
                log.debug("{}: read_reply: busy (client={} reply={})", .{
                    client_replies.replica,
                    session.header.client,
                    session.header.checksum,
                });

                return error.Busy;
            };

            log.debug("{}: read_reply: start (client={} reply={})", .{
                client_replies.replica,
                session.header.client,
                session.header.checksum,
            });

            const message = client_replies.message_pool.get_message(.reply);
            defer client_replies.message_pool.unref(message);

            read.* = .{
                .client_replies = client_replies,
                .completion = undefined,
                .slot = slot,
                .message = message.ref(),
                .callback = callback,
                .header = session.header,
                .destination_replica = destination_replica,
            };

            client_replies.storage.read_sectors(
                read_reply_callback,
                &read.completion,
                message.buffer[0..vsr.sector_ceil(session.header.size)],
                .client_replies,
                slot_offset(slot),
            );
        }

        fn read_reply_callback(completion: *Storage.Read) void {
            const read: *ClientReplies.Read = @alignCast(@fieldParentPtr("completion", completion));
            const client_replies = read.client_replies;
            const header = read.header;
            const message = read.message;
            const callback_or_null = read.callback;
            const destination_replica = read.destination_replica;

            client_replies.reads.release(read);

            defer {
                client_replies.message_pool.unref(message);
                client_replies.write_reply_next();
            }

            const callback = callback_or_null orelse {
                log.debug("{}: read_reply: already resolved (client={} reply={})", .{
                    client_replies.replica,
                    header.client,
                    header.checksum,
                });
                return;
            };

            if (!message.header.valid_checksum() or
                !message.header.valid_checksum_body(message.body_used()))
            {
                log.warn("{}: read_reply: corrupt reply (client={} reply={})", .{
                    client_replies.replica,
                    header.client,
                    header.checksum,
                });

                callback(client_replies, &header, null, destination_replica);
                return;
            }

            // Possible causes:
            // - The read targets an older reply.
            // - The read targets a newer reply (that we haven't seen/written yet).
            // - The read targets a reply that we wrote, but was misdirected.
            if (message.header.checksum != header.checksum) {
                log.warn("{}: read_reply: unexpected header (client={} reply={} found={})", .{
                    client_replies.replica,
                    header.client,
                    header.checksum,
                    message.header.checksum,
                });

                callback(client_replies, &header, null, destination_replica);
                return;
            }

            assert(message.header.command == .reply);
            assert(message.header.cluster == header.cluster);

            log.debug("{}: read_reply: done (client={} reply={})", .{
                client_replies.replica,
                header.client,
                header.checksum,
            });

            callback(client_replies, &header, message, destination_replica);
        }

        pub fn ready_sync(client_replies: *ClientReplies) bool {
            maybe(client_replies.ready_callback == null);
            assert(client_replies.writing.count() + client_replies.write_queue.count ==
                client_replies.writes.executing());

            return client_replies.writes.available() > 0;
        }

        /// Caller must check ready_sync() first.
        /// Call `callback` when ClientReplies is able to start another write_reply().
        pub fn ready(
            client_replies: *ClientReplies,
            callback: *const fn (client_replies: *ClientReplies) void,
        ) void {
            assert(client_replies.ready_callback == null);
            assert(!client_replies.ready_sync());
            assert(client_replies.writes.available() == 0);
            assert(client_replies.writing.count() + client_replies.write_queue.count ==
                client_replies.writes.executing());

            // ready_callback will be called the next time a write completes.
            client_replies.ready_callback = callback;
        }

        pub fn remove_reply(client_replies: *ClientReplies, slot: Slot) void {
            maybe(client_replies.faulty.isSet(slot.index));

            client_replies.faulty.unset(slot.index);
        }

        /// The caller is responsible for ensuring that the ClientReplies is able to write
        /// by calling `write_reply()` after `ready()` finishes.
        pub fn write_reply(
            client_replies: *ClientReplies,
            slot: Slot,
            message: *Message.Reply,
            trigger: WriteTrigger,
        ) void {
            assert(client_replies.ready_sync());
            assert(client_replies.ready_callback == null);
            assert(client_replies.writes.available() > 0);
            maybe(client_replies.writing.isSet(slot.index));
            assert(client_replies.writing.count() + client_replies.write_queue.count ==
                client_replies.writes.executing());
            assert(message.header.command == .reply);
            // There is never any need to write a body-less message, since the header is
            // stored safely in the `client_sessions` trailer.
            assert(message.header.size != @sizeOf(vsr.Header));

            switch (trigger) {
                .commit => {
                    assert(client_replies.checkpoint_callback == null);
                    maybe(client_replies.faulty.isSet(slot.index));
                },
                .repair => {
                    maybe(client_replies.checkpoint_callback == null);
                    assert(client_replies.faulty.isSet(slot.index));
                },
            }

            // Resolve any pending reads for this reply.
            // If we don't do this, an earlier started read can complete with an error, and
            // erroneously clobber the faulty bit.
            // For simplicity, resolve the reads synchronously, instead of going through next tick
            // machinery.
            var reads = client_replies.reads.iterate();
            while (reads.next()) |read| {
                if (read.callback == null) continue; // Already resolved.
                if (read.header.checksum == message.header.checksum) {
                    defer read.callback = null;

                    read.callback.?(
                        client_replies,
                        &read.header,
                        message,
                        read.destination_replica,
                    );
                }
            }

            // Clear the fault *before* the write completes, not after.
            // Otherwise, a replica exiting state sync might mark a reply as faulty, then the
            // ClientReplies clears that bit due to an unrelated write that was already queued.
            client_replies.faulty.unset(slot.index);

            const write = client_replies.writes.acquire().?;
            write.* = .{
                .client_replies = client_replies,
                .completion = undefined,
                .message = message.ref(),
                .slot = slot,
                .trigger = trigger,
            };

            // If there is already a write to the same slot queued (but not started), replace it.
            var write_queue = client_replies.write_queue.iterator_mutable();
            while (write_queue.next_ptr()) |queued| {
                if (queued.*.slot.index == slot.index) {
                    client_replies.message_pool.unref(queued.*.message);
                    client_replies.writes.release(queued.*);

                    queued.* = write;
                    break;
                }
            } else {
                client_replies.write_queue.push_assume_capacity(write);
                client_replies.write_reply_next();
            }

            assert(client_replies.writing.isSet(write.slot.index));
        }

        fn write_reply_next(client_replies: *ClientReplies) void {
            while (client_replies.write_queue.head()) |write| {
                if (client_replies.writing.isSet(write.slot.index)) return;

                const message = write.message;
                _ = client_replies.write_queue.pop();

                // Zero sector padding to ensure deterministic storage.
                const size = message.header.size;
                const size_ceil = vsr.sector_ceil(size);
                @memset(message.buffer[size..size_ceil], 0);

                client_replies.writing.set(write.slot.index);
                client_replies.storage.write_sectors(
                    write_reply_callback,
                    &write.completion,
                    message.buffer[0..size_ceil],
                    .client_replies,
                    slot_offset(write.slot),
                    .{},
                );
            }
        }

        fn write_reply_callback(completion: *Storage.Write) void {
            const write: *ClientReplies.Write = @fieldParentPtr("completion", completion);
            const client_replies = write.client_replies;
            const message = write.message;
            assert(client_replies.writing.isSet(write.slot.index));
            maybe(client_replies.faulty.isSet(write.slot.index));

            var reads = client_replies.reads.iterate();
            while (reads.next()) |read| {
                if (read.slot.index == write.slot.index) {
                    if (read.header.checksum == message.header.checksum) {
                        assert(read.callback == null);
                    } else {
                        // A read and a write can race on the slot if:
                        // - the write is from before the latest state sync (outdated write)
                        // - the read is from before the write (outdated read)
                    }
                }
            }

            log.debug("{}: write_reply: wrote (client={} request={})", .{
                client_replies.replica,
                message.header.client,
                message.header.request,
            });

            // Release the write *before* invoking the callback, so that if the callback checks
            // .writes.available() we don't erroneously appear busy.
            client_replies.writing.unset(write.slot.index);
            client_replies.writes.release(write);

            client_replies.message_pool.unref(message);
            client_replies.write_reply_next();

            if (client_replies.ready_callback) |ready_callback| {
                client_replies.ready_callback = null;
                ready_callback(client_replies);
            }

            if (client_replies.checkpoint_callback != null and
                client_replies.writes_executing_by_trigger(.commit) == 0)
            {
                client_replies.checkpoint_done();
            }
        }

        // Wait until all writes with trigger=commit are done, and then invoke the callback.
        // (Writes with trigger=repair may still be in progress.)
        pub fn checkpoint(
            client_replies: *ClientReplies,
            callback: *const fn (*ClientReplies) void,
        ) void {
            assert(client_replies.checkpoint_callback == null);
            client_replies.checkpoint_callback = callback;

            if (client_replies.writes_executing_by_trigger(.commit) == 0) {
                client_replies.storage.on_next_tick(
                    .vsr,
                    checkpoint_next_tick_callback,
                    &client_replies.checkpoint_next_tick,
                );
            }
        }

        fn checkpoint_next_tick_callback(next_tick: *Storage.NextTick) void {
            const client_replies: *ClientReplies =
                @alignCast(@fieldParentPtr("checkpoint_next_tick", next_tick));

            if (client_replies.checkpoint_callback != null) {
                client_replies.checkpoint_done();
            }
        }

        fn checkpoint_done(client_replies: *ClientReplies) void {
            assert(client_replies.writes_executing_by_trigger(.commit) == 0);

            const repairing = client_replies.writes_executing_by_trigger(.repair);
            assert(client_replies.writes.executing() == repairing);
            assert(client_replies.writing.count() + client_replies.write_queue.count == repairing);

            const callback = client_replies.checkpoint_callback.?;
            client_replies.checkpoint_callback = null;

            callback(client_replies);
        }

        fn writes_executing_by_trigger(
            client_replies: *const ClientReplies,
            trigger: WriteTrigger,
        ) u32 {
            assert(client_replies.writing.count() + client_replies.write_queue.count ==
                client_replies.writes.executing());

            var writes = client_replies.writes.iterate_const();
            var writes_by_trigger: u32 = 0;
            while (writes.next()) |write| {
                writes_by_trigger += @intFromBool(write.trigger == trigger);
            }
            return writes_by_trigger;
        }
    };
}
