const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

// When referenced from unit_test.zig, there is no vsr import module so use path.
const vsr = if (@import("root") == @This()) @import("vsr") else @import("../../../vsr.zig");

const constants = vsr.constants;
const log = std.log.scoped(.tb_client_context);

const stdx = vsr.stdx;
const maybe = stdx.maybe;
const Header = vsr.Header;

const IO = vsr.io.IO;
const FIFOType = vsr.fifo.FIFOType;
const message_pool = vsr.message_pool;

const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;
const Packet = @import("packet.zig").Packet;
const Signal = @import("signal.zig").Signal;

const api = @import("../tb_client.zig");
const tb_completion_t = api.tb_completion_t;

pub const ContextImplementation = struct {
    completion_ctx: usize,
    submit_fn: *const fn (*ContextImplementation, *Packet) void,
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

        fn operation_event_size(op: u8) ?usize {
            inline for (allowed_operations) |operation| {
                if (op == @intFromEnum(operation)) {
                    return @sizeOf(StateMachine.EventType(operation));
                }
            }
            return null;
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
        canceled: bool,
        evicted: ?vsr.Header.Eviction.Reason,
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
            context.canceled = false;
            context.evicted = null;

            log.debug("{}: init: initializing signal", .{context.client_id});
            try context.signal.init(&context.io, Context.on_signal);
            errdefer context.signal.deinit();

            context.batch_size_limit = null;
            context.client.register(client_register_callback, @intFromPtr(context));

            log.debug("{}: init: spawning thread", .{context.client_id});
            context.thread = std.Thread.spawn(.{}, Context.run, .{context}) catch |err| {
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
            self.io.cancel_all();

            self.signal.deinit();
            self.client.deinit(self.allocator);
            self.message_pool.deinit(self.allocator);
            self.io.deinit();

            self.allocator.destroy(self);
        }

        fn client_register_callback(user_data: u128, result: *const vsr.RegisterResult) void {
            const self: *Context = @ptrFromInt(@as(usize, @intCast(user_data)));
            assert(self.batch_size_limit == null);
            assert(result.batch_size_limit > 0);
            assert(result.batch_size_limit <= constants.message_body_size_max);

            self.batch_size_limit = result.batch_size_limit;
            // Some requests may have queued up while the client was registering.
            on_signal(&self.signal);
        }

        fn client_eviction_callback(client: *Client, eviction: *const Message.Eviction) void {
            const self: *Context = @fieldParentPtr("client", client);
            assert(self.evicted == null);
            log.debug("{}: client_eviction_callback: reason={?s} reason_int={}", .{
                self.client_id,
                std.enums.tagName(vsr.Header.Eviction.Reason, eviction.header.reason),
                @intFromEnum(eviction.header.reason),
            });

            self.evicted = eviction.header.reason;
            self.cancel_all();
        }

        fn tick(self: *Context) void {
            if (self.evicted == null) {
                self.client.tick();
            }
        }
        pub fn run(self: *Context) void {
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
            self.cancel_all();
        }

        fn on_signal(signal: *Signal) void {
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
            const safety_limit = 8 * 1024; // Avoid unbounded loop in case of invalid packets.
            for (0..safety_limit) |_| {
                const packet = self.submitted.pop() orelse return;
                self.request(packet);

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

        fn request(self: *Context, packet: *Packet) void {
            assert(self.batch_size_limit != null);

            if (self.evicted) |reason| {
                return self.on_complete(packet, client_eviction_error(reason));
            }

            const operation: StateMachine.Operation = operation_from_int(packet.operation) orelse {
                return self.on_complete(packet, error.InvalidOperation);
            };

            // Get the size of each request structure in the packet.data:
            const event_size: usize = switch (operation) {
                inline else => |operation_comptime| blk: {
                    break :blk @sizeOf(StateMachine.EventType(operation_comptime));
                },
            };

            // Make sure the packet.data size is correct:
            const events: []const u8 = if (packet.data_size > 0) events: {
                const data: [*]const u8 = @ptrCast(packet.data.?);
                break :events data[0..packet.data_size];
            } else empty: {
                // It may be an empty array (null pointer)
                // or a buffer with no elements (valid pointer and size == 0).
                stdx.maybe(packet.data == null);
                break :empty &[0]u8{};
            };
            if (events.len % event_size != 0) {
                return self.on_complete(packet, error.InvalidDataSize);
            }

            // Make sure the packet.data wouldn't overflow a request, and that the corresponding
            // results won't overflow a reply.
            const events_batch_max = switch (operation) {
                .pulse => unreachable,
                inline else => |operation_comptime| StateMachine.operation_batch_max(
                    operation_comptime,
                    self.batch_size_limit.?,
                ),
            };
            if (@divExact(events.len, event_size) > events_batch_max) {
                return self.on_complete(packet, error.TooMuchData);
            } else {
                assert(events.len <= self.batch_size_limit.?);
            }

            packet.batch_next = null;
            packet.batch_tail = packet;
            packet.batch_size = packet.data_size;
            packet.batch_allowed = batch_logical_allowed(
                operation,
                packet.data,
                packet.data_size,
            );
            // Avoid making a packet inflight by cancelling it if the client was shutdown.
            if (self.signal.stop_requested()) {
                return self.cancel(packet);
            }

            // Nothing inflight means the packet should be submitted right now.
            if (self.client.request_inflight == null) {
                return self.submit(packet);
            }

            // If allowed, try to batch the packet with another already in self.pending.
            if (packet.batch_allowed) {
                var it = self.pending.peek();
                while (it) |root| {
                    it = root.next;

                    // Check for pending packets of the same operation which can be batched.
                    if (root.operation != packet.operation) continue;
                    if (!root.batch_allowed) continue;

                    const merged_events = @divExact(root.batch_size + packet.data_size, event_size);
                    if (merged_events > events_batch_max) continue;

                    root.batch_size += packet.data_size;
                    root.batch_tail.?.batch_next = packet;
                    root.batch_tail = packet;
                    return;
                }
            }

            // Couldn't batch with existing packet so push to pending directly.
            packet.next = null;
            self.pending.push(packet);
        }

        fn batch_logical_allowed(
            operation: StateMachine.Operation,
            data: ?*const anyopaque,
            data_size: u32,
        ) bool {
            if (!StateMachine.batch_logical_allowed.get(operation)) return false;

            // TODO(king): Remove this code once protocol batching is implemented.
            //
            // If the application submits an unclosed linked chain, it can inadvertently make
            // the elements of the next batch part of it.
            // To work around this issue, we don't allow unclosed linked chains to be batched.
            if (data_size > 0) {
                assert(data != null);
                const linked_chain_open: bool = switch (operation) {
                    inline .create_accounts,
                    .create_transfers,
                    => |tag| linked_chain_open: {
                        const Event = StateMachine.EventType(tag);
                        // Packet data isn't necessarily aligned.
                        const events: [*]align(@alignOf(u8)) const Event = @ptrCast(data.?);
                        const events_count: usize = @divExact(data_size, @sizeOf(Event));
                        break :linked_chain_open events[events_count - 1].flags.linked;
                    },
                    else => false,
                };

                if (linked_chain_open) return false;
            }

            return true;
        }

        fn submit(self: *Context, packet: *Packet) void {
            assert(self.client.request_inflight == null);
            // On shutdown, cancel this packet as well as any others batched onto it.
            if (self.signal.stop_requested()) {
                return self.cancel(packet);
            }

            const message = self.client.get_message().build(.request);
            errdefer self.client.release_message(message.base());

            const operation: StateMachine.Operation = @enumFromInt(packet.operation);
            message.header.* = .{
                .release = self.client.release,
                .client = self.client.id,
                .request = 0, // Set by client.raw_request.
                .cluster = self.client.cluster,
                .command = .request,
                .operation = vsr.Operation.from(StateMachine, operation),
                .size = @sizeOf(vsr.Header) + packet.batch_size,
            };

            // Copy all batched packet event data into the message.
            var offset: u32 = 0;
            var it: ?*Packet = packet;
            while (it) |batched| {
                assert(batched.batch_next == null or batched.batch_allowed);
                it = batched.batch_next;

                const event_data: []const u8 = if (batched.data_size > 0)
                    @as([*]const u8, @ptrCast(batched.data.?))[0..batched.data_size]
                else empty: {
                    // It may be an empty array (null pointer)
                    // or a buffer with no elements (valid pointer and size == 0).
                    stdx.maybe(batched.data == null);
                    break :empty &[0]u8{};
                };
                stdx.copy_disjoint(.inexact, u8, message.body_used()[offset..], event_data);
                offset += @intCast(event_data.len);
            }

            assert(offset == packet.batch_size);
            self.client.raw_request(
                Context.on_result,
                @bitCast(UserData{
                    .self = self,
                    .packet = packet,
                }),
                message,
            );
        }

        fn on_result(
            raw_user_data: u128,
            op: StateMachine.Operation,
            timestamp: u64,
            reply: []u8,
        ) void {
            const user_data: UserData = @bitCast(raw_user_data);
            const self = user_data.self;
            const packet = user_data.packet;
            assert(packet.next == null); // (previously) inflight packet should not be pending.
            assert(timestamp > 0);

            // Submit the next pending packet (if any) now that VSR has completed this one.
            // The submit() call may complete it inline so keep submitting until there's
            // an inflight.
            while (self.pending.pop()) |packet_next| {
                self.submit(packet_next);
                if (self.client.request_inflight != null) break;
            }

            switch (op) {
                inline else => |operation| {
                    // on_result should never be called with an operation not green-lit by request()
                    // This also guards from passing an unsupported operation into DemuxerType.
                    if (comptime operation_event_size(@intFromEnum(operation)) == null) {
                        unreachable;
                    }

                    // Demuxer expects []u8 but VSR callback provides []const u8.
                    // The bytes are known to come from a Message body that will be soon discarded
                    // therefore it's safe to @constCast and potentially modify the data in-place.
                    var demuxer = Client.DemuxerType(operation).init(@constCast(reply));

                    var it: ?*Packet = packet;
                    var event_offset: u32 = 0;
                    while (it) |batched| {
                        assert(batched.batch_next == null or batched.batch_allowed);
                        it = batched.batch_next;

                        const event_count = @divExact(
                            batched.data_size,
                            @sizeOf(StateMachine.EventType(operation)),
                        );
                        const batched_reply = demuxer.decode(event_offset, event_count);
                        event_offset += event_count;

                        if (!StateMachine.batch_logical_allowed.get(operation)) {
                            assert(batched_reply.len == reply.len);
                        }

                        assert(batched.operation == @intFromEnum(operation));
                        self.on_complete(batched, .{
                            .timestamp = timestamp,
                            .reply = batched_reply,
                        });
                    }
                },
            }
        }

        fn cancel_all(self: *Context) void {
            maybe(self.canceled);
            defer self.canceled = true;

            // Cancel the request_inflight packet if any.
            //
            // TODO: Look into completing the inflight packet with a different error than
            // `error.ClientShutdown`, allow the client user to make a more informed decision
            // e.g. retrying the inflight packet and just abandoning the ClientShutdown ones.
            if (!self.canceled) {
                if (self.client.request_inflight) |*inflight| {
                    if (inflight.message.header.operation != .register) {
                        const packet = @as(UserData, @bitCast(inflight.user_data)).packet;
                        assert(packet.next == null); // Inflight packet should not be pending.
                        self.cancel(packet);
                    }
                }
            }

            // Cancel pending and submitted packets.
            while (self.pending.pop()) |packet| self.cancel(packet);
            while (self.submitted.pop()) |packet| self.cancel(packet);
        }

        fn cancel(self: *Context, packet: *Packet) void {
            const result = if (self.evicted) |reason|
                client_eviction_error(reason)
            else reason: {
                assert(self.signal.stop_requested());
                break :reason error.ClientShutdown;
            };

            var it: ?*Packet = packet;
            while (it) |batched| {
                assert(batched.batch_next == null or batched.batch_allowed);
                it = batched.batch_next;
                self.on_complete(batched, result);
            }
        }

        fn on_complete(
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
                (self.completion_fn)(completion_ctx, tb_client, packet, 0, null, 0);
                return;
            };

            // The packet completed normally.
            packet.status = .ok;
            (self.completion_fn)(
                completion_ctx,
                tb_client,
                packet,
                result.timestamp,
                result.reply.ptr,
                @intCast(result.reply.len),
            );
        }

        inline fn get_context(implementation: *ContextImplementation) *Context {
            return @alignCast(@fieldParentPtr("implementation", implementation));
        }

        fn on_submit(implementation: *ContextImplementation, packet: *Packet) void {
            // Packet is caller-allocated to enable elastic intrusive-link-list-based memory
            // management. However, some of Packet's fields are essentially private. Initialize
            // them here to avoid threading default fields through FFI boundary.
            packet.* = .{
                .next = null,
                .user_data = packet.user_data,
                .operation = packet.operation,
                .status = .ok,
                .data_size = packet.data_size,
                .data = packet.data,
                .batch_next = null,
                .batch_tail = null,
                .batch_size = 0,
                .batch_allowed = false,
                .reserved = [_]u8{0} ** 7,
            };
            const self = get_context(implementation);
            assert(!self.signal.stop_requested());

            self.submitted.push(packet);
            self.signal.notify();
        }

        fn on_deinit(implementation: *ContextImplementation) void {
            const self = get_context(implementation);
            self.deinit();
        }

        test "client_batch_linked_chain" {
            inline for ([_]StateMachine.Operation{
                .create_accounts,
                .create_transfers,
            }) |operation| {
                const Event = StateMachine.EventType(operation);
                var data = [_]Event{std.mem.zeroInit(Event, .{})} ** 3;

                // Broken linked chain cannot be batched.
                for (&data) |*item| item.flags.linked = true;
                try std.testing.expect(!batch_logical_allowed(
                    operation,
                    data[0..],
                    data.len * @sizeOf(Event),
                ));

                // Valid linked chain.
                data[data.len - 1].flags.linked = false;
                try std.testing.expect(batch_logical_allowed(
                    operation,
                    data[0..],
                    data.len * @sizeOf(Event),
                ));

                // Single element.
                try std.testing.expect(batch_logical_allowed(
                    operation,
                    &data[data.len - 1],
                    1 * @sizeOf(Event),
                ));

                // No elements.
                try std.testing.expect(batch_logical_allowed(
                    operation,
                    null,
                    0,
                ));
            }
        }
    };
}

fn client_eviction_error(reason: vsr.Header.Eviction.Reason) error{
    ClientEvicted,
    ClientReleaseTooLow,
    ClientReleaseTooHigh,
} {
    return switch (reason) {
        .client_release_too_low => error.ClientReleaseTooLow,
        .client_release_too_high => error.ClientReleaseTooHigh,
        else => error.ClientEvicted,
    };
}
