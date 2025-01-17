const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const log = std.log.scoped(.tb_client_context);

const api = @import("../tb_client.zig");
const tb_completion_t = api.tb_completion_t;
const vsr = api.vsr;

const constants = vsr.constants;
const stdx = vsr.stdx;
const maybe = stdx.maybe;
const Header = vsr.Header;

const IO = vsr.io.IO;
const FIFOType = vsr.fifo.FIFOType;
const message_pool = vsr.message_pool;

const BatchEncoder = vsr.batch.BatchEncoder;
const BatchDecoder = vsr.batch.BatchDecoder;
const batch_trailer_total_size = vsr.batch.batch_trailer_total_size;

const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;

const Packet = @import("packet.zig").Packet;
const Signal = @import("signal.zig").Signal;

pub const ContextImplementation = struct {
    completion_ctx: usize,
    submit_fn: *const fn (*ContextImplementation, *Packet.Extern) void,
    deinit_fn: *const fn (*ContextImplementation) void,
};

pub const Error = std.mem.Allocator.Error || error{
    Unexpected,
    AddressInvalid,
    AddressLimitExceeded,
    SystemResources,
    NetworkSubsystemFailed,
};

pub fn ContextType(
    comptime Client: type,
) type {
    return struct {
        const Context = @This();

        const StateMachine = Client.StateMachine;
        const allowed_operations = [_]StateMachine.Operation{
            .create_accounts,
            .create_transfers,
            .lookup_accounts,
            .lookup_transfers,
            .get_account_transfers,
            .get_account_balances,
            .query_accounts,
            .query_transfers,
        };

        const UserData = extern struct {
            self: *Context,
            packet: *Packet,
        };

        comptime {
            assert(@sizeOf(UserData) == @sizeOf(u128));
        }

        fn operation_from_int(op: u8) ?StateMachine.Operation {
            inline for (allowed_operations) |operation| {
                if (op == @intFromEnum(operation)) {
                    return operation;
                }
            }
            return null;
        }

        fn get_context(implementation: *ContextImplementation) *Context {
            return @alignCast(@fieldParentPtr("implementation", implementation));
        }

        const PacketError = error{
            TooMuchData,
            ClientShutdown,
            ClientEvicted,
            ClientReleaseTooLow,
            ClientReleaseTooHigh,
            InvalidOperation,
            InvalidDataSize,
        };

        allocator: std.mem.Allocator,
        client_id: u128,

        addresses: stdx.BoundedArrayType(std.net.Address, constants.replicas_max),
        io: IO,
        message_pool: MessagePool,
        client: Client,
        batch_size_limit: ?u32,

        completion_fn: tb_completion_t,
        implementation: ContextImplementation,

        submitted: Packet.SubmissionQueue,
        pending: FIFOType(Packet),

        signal: Signal,
        eviction_reason: ?vsr.Header.Eviction.Reason,
        thread: std.Thread,

        pub fn init(
            allocator: std.mem.Allocator,
            cluster_id: u128,
            addresses: []const u8,
            completion_ctx: usize,
            completion_fn: tb_completion_t,
        ) Error!*Context {
            var context = try allocator.create(Context);
            errdefer allocator.destroy(context);

            context.allocator = allocator;
            context.client_id = stdx.unique_u128();

            log.debug("{}: init: parsing vsr addresses: {s}", .{ context.client_id, addresses });
            context.addresses = .{};
            const addresses_parsed = vsr.parse_addresses(
                addresses,
                context.addresses.unused_capacity_slice(),
            ) catch |err| return switch (err) {
                error.AddressLimitExceeded => error.AddressLimitExceeded,
                error.AddressHasMoreThanOneColon,
                error.AddressHasTrailingComma,
                error.AddressInvalid,
                error.PortInvalid,
                error.PortOverflow,
                => error.AddressInvalid,
            };
            assert(addresses_parsed.len > 0);
            assert(addresses_parsed.len <= constants.replicas_max);
            context.addresses.resize(addresses_parsed.len) catch unreachable;

            log.debug("{}: init: initializing IO", .{context.client_id});
            context.io = IO.init(32, 0) catch |err| {
                log.err("{}: failed to initialize IO: {s}", .{
                    context.client_id,
                    @errorName(err),
                });
                return switch (err) {
                    error.ProcessFdQuotaExceeded => error.SystemResources,
                    error.Unexpected => error.Unexpected,
                    else => unreachable,
                };
            };
            errdefer context.io.deinit();

            log.debug("{}: init: initializing MessagePool", .{context.client_id});
            context.message_pool = try MessagePool.init(allocator, .client);
            errdefer context.message_pool.deinit(context.allocator);

            log.debug("{}: init: initializing client (cluster_id={x:0>32}, addresses={any})", .{
                context.client_id,
                cluster_id,
                context.addresses.const_slice(),
            });
            context.client = Client.init(
                allocator,
                .{
                    .id = context.client_id,
                    .cluster = cluster_id,
                    .replica_count = context.addresses.count_as(u8),
                    .time = .{},
                    .message_pool = &context.message_pool,
                    .message_bus_options = .{
                        .configuration = context.addresses.const_slice(),
                        .io = &context.io,
                    },
                    .eviction_callback = client_eviction_callback,
                },
            ) catch |err| {
                log.err("{}: failed to initialize Client: {s}", .{
                    context.client_id,
                    @errorName(err),
                });
                return switch (err) {
                    error.TimerUnsupported => error.Unexpected,
                    error.OutOfMemory => error.OutOfMemory,
                    else => unreachable,
                };
            };
            errdefer context.client.deinit(context.allocator);

            context.completion_fn = completion_fn;
            context.implementation = .{
                .completion_ctx = completion_ctx,
                .submit_fn = Context.on_submit,
                .deinit_fn = Context.on_deinit,
            };

            context.submitted = .{};
            context.pending = .{
                .name = null,
                .verify_push = builtin.is_test,
            };
            context.eviction_reason = null;

            log.debug("{}: init: initializing signal", .{context.client_id});
            try context.signal.init(&context.io, Context.signal_notify_callback);
            errdefer context.signal.deinit();

            context.batch_size_limit = null;
            context.client.register(client_register_callback, @intFromPtr(context));

            log.debug("{}: init: spawning thread", .{context.client_id});
            context.thread = std.Thread.spawn(.{}, Context.io_thread, .{context}) catch |err| {
                log.err("{}: failed to spawn thread: {s}", .{
                    context.client_id,
                    @errorName(err),
                });
                return switch (err) {
                    error.Unexpected => error.Unexpected,
                    error.OutOfMemory => error.OutOfMemory,
                    error.SystemResources,
                    error.ThreadQuotaExceeded,
                    error.LockedMemoryLimitExceeded,
                    => error.SystemResources,
                };
            };

            return context;
        }

        /// Only one thread calls `deinit()`.
        /// Since it frees the Context, any further interaction is undefined behavior.
        pub fn deinit(self: *Context) void {
            self.signal.stop();
            self.thread.join();

            assert(self.submitted.pop() == null);
            assert(self.pending.pop() == null);

            self.io.cancel_all();

            self.signal.deinit();
            self.client.deinit(self.allocator);
            self.message_pool.deinit(self.allocator);
            self.io.deinit();

            self.allocator.destroy(self);
        }

        fn tick(self: *Context) void {
            if (self.eviction_reason == null) {
                self.client.tick();
            }
        }

        fn io_thread(self: *Context) void {
            while (!self.signal.stop_requested()) {
                self.tick();
                self.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms) catch |err| {
                    log.err("{}: IO.run() failed: {s}", .{
                        self.client_id,
                        @errorName(err),
                    });
                    @panic("IO.run() failed");
                };
            }

            // If evicted, the inflight request was already canceled during eviction.
            if (self.eviction_reason == null) {
                self.cancel_request_inflight();
            }
            self.cancel_queued_packets();
        }

        /// Cancels all submitted/pending packets.
        fn cancel_queued_packets(self: *Context) void {
            while (self.pending.pop()) |packet| {
                packet.assert_phase(.pending);
                self.packet_cancel(packet);
            }

            while (self.submitted.pop()) |packet| {
                packet.assert_phase(.submitted);
                self.packet_cancel(packet);
            }
        }

        /// Cancel the current inflight packet, as it won't be replied anymore.
        fn cancel_request_inflight(self: *Context) void {
            if (self.client.request_inflight) |*inflight| {
                if (inflight.message.header.operation != .register) {
                    const packet = @as(UserData, @bitCast(inflight.user_data)).packet;
                    packet.assert_phase(.sent);
                    self.packet_cancel(packet);
                }
            }
        }

        /// Calls the user callback when a packet is canceled due to the client
        /// being either evicted or shutdown.
        fn packet_cancel(self: *Context, packet: *Packet) void {
            assert(packet.next == null);
            const result = if (self.eviction_reason) |reason| switch (reason) {
                .reserved => unreachable,
                .client_release_too_low => error.ClientReleaseTooLow,
                .client_release_too_high => error.ClientReleaseTooHigh,
                else => error.ClientEvicted,
            } else result: {
                assert(self.signal.stop_requested());
                break :result error.ClientShutdown;
            };

            if (packet.batch_count == 0) {
                assert(packet.batch_next == null);
                self.notify_completion(packet, result);
            } else {
                // Although the protocol allows `batch_count == 1`,
                // the client never packages a single packet into a batched request.
                assert(packet.batch_count > 1);
                assert(packet.batch_next != null);
                var it: ?*Packet = packet;
                while (it) |batched| {
                    it = batched.batch_next;
                    self.notify_completion(batched, result);
                }
            }
        }

        fn packet_enqueue(self: *Context, packet: *Packet) void {
            assert(self.batch_size_limit != null);
            packet.assert_phase(.submitted);

            if (self.eviction_reason != null) {
                return self.packet_cancel(packet);
            }

            const operation: StateMachine.Operation = operation_from_int(packet.operation) orelse {
                self.notify_completion(packet, error.InvalidOperation);
                return;
            };

            const event_size: usize, const events_batch_max: usize = switch (operation) {
                .pulse => unreachable,
                inline else => |operation_comptime| blk: {
                    // Make sure the packet.data wouldn't overflow a request,
                    // and that the corresponding results won't overflow a reply.
                    const Event = StateMachine.EventType(operation_comptime);
                    const event_size = @sizeOf(Event);
                    const slice: []const u8 = packet.slice();

                    if (slice.len % event_size != 0) {
                        self.notify_completion(packet, error.InvalidDataSize);
                        return;
                    }

                    if (slice.len > self.batch_size_limit.?) {
                        self.notify_completion(packet, error.TooMuchData);
                        return;
                    }

                    const events_batch_max = if (StateMachine.event_is_slice(operation))
                        StateMachine.operation_batch_max(
                            operation_comptime,
                            self.batch_size_limit.?,
                        )
                    else
                        1;

                    if (@divExact(slice.len, event_size) > events_batch_max) {
                        self.notify_completion(packet, error.TooMuchData);
                        return;
                    }

                    break :blk .{ event_size, events_batch_max };
                },
            };
            assert(self.batch_size_limit.? >= event_size * events_batch_max);

            // Nothing inflight means the packet should be submitted right now.
            if (self.client.request_inflight == null) {
                assert(self.pending.count == 0);

                packet.phase = .pending;
                self.packet_send(packet);
                return;
            }

            // If allowed, try to batch the packet with another already in `pending`.
            if (events_batch_max > 1) {
                var it = self.pending.peek();
                while (it) |root| {
                    root.assert_phase(.pending);
                    it = root.next;

                    if (root.operation != packet.operation) continue;

                    // Although the protocol allows `batch_count == 1`,
                    // the client never packages a single packet into a batched request.
                    // Instead, it sends a non-batched request with `batch_count == 0` to avoid
                    // wasting message body bytes with the batch trailer.
                    if (root.batch_count == 0) {
                        assert(root.batch_next == null);
                        const total_size = root.data_size + packet.data_size +
                            batch_trailer_total_size(.{
                            .element_size = event_size,
                            .batch_count = 2,
                        });
                        if (total_size > self.batch_size_limit.?) continue;
                        if (@divExact(total_size, event_size) > events_batch_max) continue;

                        root.batch_count = 2;
                        root.batch_next = packet;
                        root.batch_tail = packet;
                        root.batch_events = @intCast(@divExact(
                            root.data_size + packet.data_size,
                            event_size,
                        ));
                    } else {
                        assert(root.batch_count > 1);
                        assert(root.batch_next != null);
                        const total_size = (root.batch_events * event_size) +
                            packet.data_size + batch_trailer_total_size(.{
                            .element_size = event_size,
                            .batch_count = root.batch_count + 1,
                        });
                        if (total_size > self.batch_size_limit.?) continue;
                        if (@divExact(total_size, event_size) > events_batch_max) continue;

                        root.batch_count += 1;
                        root.batch_tail.?.batch_next = packet;
                        root.batch_tail = packet;
                        root.batch_events += @intCast(@divExact(packet.data_size, event_size));
                    }

                    packet.phase = .batched;
                    return;
                }
            }

            // Couldn't batch with existing packet so push to pending directly.
            packet.phase = .pending;
            self.pending.push(packet);
        }

        /// Sends the packet (the entire batched linked list of packets) through the vsr client.
        /// Always called by the io thread.
        fn packet_send(self: *Context, packet: *Packet) void {
            assert(self.batch_size_limit != null);
            assert(self.client.request_inflight == null);
            packet.assert_phase(.pending);

            if (self.signal.stop_requested()) {
                self.packet_cancel(packet);
                return;
            }

            const operation: StateMachine.Operation = operation_from_int(packet.operation).?;
            const event_size: usize = switch (operation) {
                .pulse => unreachable,
                inline else => |operation_comptime| @sizeOf(
                    StateMachine.EventType(operation_comptime),
                ),
            };

            const message = self.client.get_message().build(.request);
            defer {
                self.client.release_message(message.base());
                packet.assert_phase(.sent);
            }

            const bytes_written: usize = bytes_written: {
                if (packet.batch_count == 0) {
                    assert(packet.batch_next == null);
                    const slice: []const u8 = packet.slice();
                    stdx.copy_disjoint(
                        .inexact,
                        u8,
                        message.buffer[@sizeOf(Header)..],
                        slice,
                    );
                    break :bytes_written slice.len;
                } else {
                    // Although the protocol allows `batch_count == 1`,
                    // the client never packages a single packet into a batched request.
                    assert(packet.batch_count > 1);
                    assert(packet.batch_next != null);

                    var encoder = BatchEncoder.init(
                        event_size,
                        message.buffer[@sizeOf(Header)..],
                    );

                    var it: ?*Packet = packet;
                    while (it) |batched| {
                        it = batched.batch_next;
                        if (encoder.batch_count > 0) batched.assert_phase(.batched);

                        const slice: []const u8 = batched.slice();
                        stdx.copy_disjoint(.inexact, u8, encoder.writable(), slice);
                        encoder.add(slice.len);
                    }
                    assert(encoder.batch_count == packet.batch_count);
                    assert(packet.batch_events == @divExact(encoder.bytes_written, event_size));
                    encoder.finish();

                    break :bytes_written encoder.bytes_written;
                }
            };
            assert(bytes_written % event_size == 0);
            assert(bytes_written <= self.batch_size_limit.?);

            message.header.* = .{
                .release = self.client.release,
                .client = self.client.id,
                .request = 0, // Set by client.raw_request.
                .cluster = self.client.cluster,
                .command = .request,
                .operation = vsr.Operation.from(StateMachine, operation),
                .size = @intCast(@sizeOf(vsr.Header) + bytes_written),
                .batch_count = packet.batch_count,
            };

            packet.phase = .sent;
            self.client.raw_request(
                Context.client_result_callback,
                @bitCast(UserData{
                    .self = self,
                    .packet = packet,
                }),
                message.ref(),
            );
            assert(message.header.request != 0);
        }

        fn signal_notify_callback(signal: *Signal) void {
            const self: *Context = @alignCast(@fieldParentPtr("signal", signal));

            // Don't send any requests until registration completes.
            if (self.batch_size_limit == null) {
                assert(self.client.request_inflight != null);
                assert(self.client.request_inflight.?.message.header.operation == .register);
                return;
            }

            // Prevents IO thread starvation under heavy client load.
            // Process only the minimal number of packets for the next pending request.
            const enqueued_count = self.pending.count;
            // Avoid unbounded loop in case of invalid packets.
            const safety_limit = 8 * 1024;
            for (0..safety_limit) |_| {
                const packet = self.submitted.pop() orelse return;
                self.packet_enqueue(packet);

                // Packets can be processed without increasing `pending.count`:
                // - If the packet is invalid.
                // - If there's no in-flight request, the packet is sent immediately without
                //   using the pending queue.
                // - If the packet can be batched with another previously enqueued packet.
                if (self.pending.count > enqueued_count) break;
            }

            // Defer this work to later,
            // allowing the IO thread to remain free for processing completions.
            if (!self.submitted.empty()) {
                self.signal.notify();
            }
        }

        fn client_register_callback(user_data: u128, result: *const vsr.RegisterResult) void {
            const self: *Context = @ptrFromInt(@as(usize, @intCast(user_data)));
            assert(self.client.request_inflight == null);
            assert(self.batch_size_limit == null);
            assert(result.batch_size_limit > 0);

            // The client might have a smaller message size limit.
            maybe(constants.message_body_size_max < result.batch_size_limit);
            self.batch_size_limit = @min(result.batch_size_limit, constants.message_body_size_max);

            // Some requests may have queued up while the client was registering.
            signal_notify_callback(&self.signal);
        }

        fn client_eviction_callback(client: *Client, eviction: *const Message.Eviction) void {
            const self: *Context = @fieldParentPtr("client", client);
            assert(self.eviction_reason == null);

            log.debug("{}: client_eviction_callback: reason={?s} reason_int={}", .{
                self.client_id,
                std.enums.tagName(vsr.Header.Eviction.Reason, eviction.header.reason),
                @intFromEnum(eviction.header.reason),
            });

            self.eviction_reason = eviction.header.reason;

            self.cancel_request_inflight();
            self.cancel_queued_packets();
        }

        fn client_result_callback(
            raw_user_data: u128,
            operation: StateMachine.Operation,
            timestamp: u64,
            reply: []const u8,
            batch_count: u16,
        ) void {
            const user_data: UserData = @bitCast(raw_user_data);
            const self = user_data.self;
            const packet = user_data.packet;
            assert(packet.operation == @intFromEnum(operation));
            assert(packet.batch_count == batch_count);
            assert(timestamp > 0);
            packet.assert_phase(.sent);

            // Submit the next pending packet (if any) now that VSR has completed this one.
            assert(self.client.request_inflight == null);
            while (self.pending.pop()) |packet_next| {
                self.packet_send(packet_next);
                if (self.client.request_inflight != null) break;
            }

            const result_size: usize = switch (operation) {
                .pulse => unreachable,
                inline else => |operation_comptime| @sizeOf(
                    // `StateMachine.result_size_bytes` is intended for use on the replica side,
                    // as it provides backward compatibility with older clients.
                    StateMachine.ResultType(operation_comptime),
                ),
            };
            assert(reply.len % result_size == 0);

            if (batch_count == 0) {
                assert(packet.batch_next == null);
                self.notify_completion(packet, .{
                    .timestamp = timestamp,
                    .reply = reply,
                });
            } else {
                // Although the protocol allows `batch_count == 1`,
                // the client never packages a single packet into a batched request.
                assert(batch_count > 1);
                assert(packet.batch_next != null);
                var decoder = BatchDecoder.init(
                    result_size,
                    reply,
                    batch_count,
                ) catch std.debug.panic("client_result_callback: invalid batch: " ++
                    "operation={s} reply.len={} batch_count={}", .{
                    @tagName(operation),
                    reply.len,
                    batch_count,
                });

                var it: ?*Packet = packet;
                while (it) |batched| {
                    it = batched.batch_next;
                    if (batched != packet) batched.assert_phase(.batched);

                    const batched_reply: []const u8 = decoder.next().?;
                    maybe(batched_reply.len == 0);
                    assert(batched_reply.len % result_size == 0);

                    self.notify_completion(batched, .{
                        .timestamp = timestamp,
                        .reply = batched_reply,
                    });
                }
                assert(decoder.next() == null);
            }
        }

        /// Called by the user thread when a packet is submitted.
        /// This function is thread-safe.
        fn on_submit(implementation: *ContextImplementation, packet_extern: *Packet.Extern) void {
            // Packet is caller-allocated to enable elastic intrusive-link-list-based memory
            // management. However, some of Packet's fields are essentially private. Initialize
            // them here to avoid threading default fields through FFI boundary.
            packet_extern.* = .{
                .user_data = packet_extern.user_data,
                .operation = packet_extern.operation,
                .data_size = packet_extern.data_size,
                .data = packet_extern.data,
                .tag = packet_extern.tag,
                .status = .ok,
            };

            const self = get_context(implementation);
            assert(!self.signal.stop_requested());

            // Enqueue the packet and notify the IO thread to process it asynchronously.
            self.submitted.push(packet_extern.cast());
            self.signal.notify();
        }

        /// Called by the user thread when the client is deinited.
        /// This function is thread-safe.
        fn on_deinit(implementation: *ContextImplementation) void {
            const self = get_context(implementation);
            self.deinit();
        }

        /// Calls the user callback when a packet is completed.
        fn notify_completion(
            self: *Context,
            packet: *Packet,
            completion: PacketError!struct {
                timestamp: u64,
                reply: []const u8,
            },
        ) void {
            const completion_ctx = self.implementation.completion_ctx;
            const tb_client = api.context_to_client(&self.implementation);
            const result = completion catch |err| {
                packet.status = switch (err) {
                    error.TooMuchData => .too_much_data,
                    error.ClientEvicted => .client_evicted,
                    error.ClientReleaseTooLow => .client_release_too_low,
                    error.ClientReleaseTooHigh => .client_release_too_high,
                    error.ClientShutdown => .client_shutdown,
                    error.InvalidOperation => .invalid_operation,
                    error.InvalidDataSize => .invalid_data_size,
                };
                assert(packet.status != .ok);
                packet.phase = .complete;

                // The packet completed with an error.
                (self.completion_fn)(
                    completion_ctx,
                    tb_client,
                    packet.cast(),
                    0,
                    null,
                    0,
                );
                return;
            };

            // The packet completed normally.
            assert(packet.status == .ok);
            packet.phase = .complete;
            (self.completion_fn)(
                completion_ctx,
                tb_client,
                packet.cast(),
                result.timestamp,
                result.reply.ptr,
                @intCast(result.reply.len),
            );
        }
    };
}
