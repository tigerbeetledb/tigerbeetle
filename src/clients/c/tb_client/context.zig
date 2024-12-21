const std = @import("std");
const assert = std.debug.assert;
const Atomic = std.atomic.Value;

// When referenced from unit_test.zig, there is no vsr import module so use path.
const vsr = if (@import("root") == @This()) @import("vsr") else @import("../../../vsr.zig");

const constants = vsr.constants;
const log = std.log.scoped(.tb_client_context);

const stdx = vsr.stdx;
const maybe = stdx.maybe;
const Header = vsr.Header;

const IO = vsr.io.IO;
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

        const State = enum(u8) {
            running,
            shutdown,
            evicted,
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

        signal: Signal,
        submitted: Packet.SubmissionQueue,
        state: Atomic(State),
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
            context.state = Atomic(State).init(.running);
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

        pub fn deinit(self: *Context) void {
            // Since `self` is dealocated here, it's UB for any further Context interaction.
            const state_previous = self.state.swap(.shutdown, .release);
            assert(state_previous == .running or state_previous == .evicted);

            // Wake up the run() thread for it to observe `state != .running`,
            // cancel inflight/pending packets, and finish running.
            self.signal.stop();
            self.thread.join();

            self.io.cancel_all();

            self.signal.deinit();
            self.client.deinit(self.allocator);
            self.message_pool.deinit(self.allocator);
            self.io.deinit();

            self.allocator.destroy(self);
        }

        pub fn tick(self: *Context) void {
            // `Tick` is invoked by the result callback that may have arrived during shutdown.
            if (self.state.load(.acquire) == .running) {
                self.client.tick();
            }
        }

        pub fn io_thread(self: *Context) void {
            while (!self.signal.stop_requested()) {
                self.client.tick();
                self.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms) catch |err| {
                    log.err("{}: IO.run() failed: {s}", .{
                        self.client_id,
                        @errorName(err),
                    });
                    @panic("IO.run() failed");
                };
            }

            // Cancel the request_inflight packet if any.
            if (self.client.request_inflight) |*inflight| {
                if (inflight.message.header.operation != .register) {
                    const user_data: UserData = @bitCast(inflight.user_data);
                    self.cancel(user_data.packet);
                }
            }

            while (self.submitted.pop()) |packet| {
                self.cancel(packet);
            }
        }

        /// Submit the enqueued requests through the vsr client.
        /// Always called by the io thread.
        fn submit(self: *Context, packet: *Packet) void {
            assert(self.batch_size_limit != null);
            assert(self.client.request_inflight == null);
            assert(packet.next == null);

            const message = self.client.get_message().build(.request);
            defer self.client.release_message(message.base());

            // The batching logic will be inserted here:
            const head: *Packet, const bytes_written: usize, const batch_count: u16 = blk: {
                if (!self.packet_valid(packet)) {
                    // Packet failed, nothing to send.
                    self.signal.notify();
                    return;
                }

                const event_data = packet.events();
                stdx.copy_disjoint(.inexact, u8, message.buffer[@sizeOf(Header)..], event_data);
                break :blk .{ packet, event_data.len, 0 };
            };

            const operation: StateMachine.Operation = operation_from_int(head.operation).?;
            const event_size: usize, const events_batch_max: usize =
                switch (operation) {
                .pulse => unreachable,
                inline else => |operation_comptime| blk: {
                    const event_size = @sizeOf(StateMachine.EventType(operation_comptime));
                    const events_batch_max = StateMachine.operation_batch_max(
                        operation_comptime,
                        self.batch_size_limit.?,
                    );

                    break :blk .{ event_size, events_batch_max };
                },
            };
            assert(bytes_written % event_size == 0);
            assert(bytes_written <= events_batch_max * event_size);

            message.header.* = .{
                .release = self.client.release,
                .client = self.client.id,
                .request = 0, // Set by client.raw_request.
                .cluster = self.client.cluster,
                .command = .request,
                .operation = vsr.Operation.from(StateMachine, operation),
                .size = @intCast(@sizeOf(vsr.Header) + bytes_written),
                .batch_count = batch_count,
            };

            self.client.raw_request(
                Context.client_result_callback,
                @bitCast(UserData{
                    .self = self,
                    .packet = head,
                }),
                message.ref(),
            );
        }

        /// Validate the packet, calling the completion callback if invalid.
        /// When the result is `false` then `packet: *Packet` isn't valid anymore.
        fn packet_valid(self: *Context, packet: *Packet) bool {
            assert(packet.status == .ok);
            assert(packet.user_data != null);
            assert(packet.data != null or packet.data_size == 0);
            assert(stdx.zeroed(&packet.reserved));
            assert(packet.next == null);

            const operation: StateMachine.Operation = operation_from_int(packet.operation) orelse {
                self.notify_completion(packet, error.InvalidOperation);
                return false;
            };

            switch (operation) {
                .pulse => unreachable,
                inline else => |operation_comptime| {
                    // Make sure the packet.data wouldn't overflow a request,
                    // and that the corresponding results won't overflow a reply.
                    const Event = StateMachine.EventType(operation_comptime);
                    const event_size = @sizeOf(Event);
                    const events: []const u8 = packet.events();

                    const events_batch_max = StateMachine.operation_batch_max(
                        operation_comptime,
                        self.batch_size_limit.?,
                    );

                    if (events.len % event_size != 0) {
                        self.notify_completion(packet, error.InvalidDataSize);
                        return false;
                    }

                    if (@divExact(events.len, event_size) > events_batch_max) {
                        self.notify_completion(packet, error.TooMuchData);
                        return false;
                    }

                    if (events.len > self.batch_size_limit.?) {
                        self.notify_completion(packet, error.TooMuchData);
                        return false;
                    }
                },
            }

            return true;
        }

        /// Calls the user callback when a packet is canceled due to the client
        /// being either evicted or shutdown.
        fn cancel(self: *Context, packet: *Packet) void {
            assert(packet.next == null);
            const result = switch (self.state.load(.monotonic)) {
                .running => unreachable,
                .shutdown => error.ClientShutdown,
                .evicted => switch (self.eviction_reason.?) {
                    .reserved => unreachable,
                    .client_release_too_low => error.ClientReleaseTooLow,
                    .client_release_too_high => error.ClientReleaseTooHigh,
                    else => error.ClientEvicted,
                },
            };
            self.notify_completion(packet, result);
        }

        fn client_register_callback(user_data: u128, result: *const vsr.RegisterResult) void {
            const self: *Context = @ptrFromInt(@as(usize, @intCast(user_data)));
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
            const state_previous = self.state.cmpxchgStrong(
                .running,
                .evicted,
                .release,
                .acquire,
            ) orelse {
                // The previous state was "running" as expected.
                return;
            };

            // Cannot be evicted twice.
            assert(state_previous != .evicted);

            // The eviction can arrive during the shutdown,
            // ignoring the eviction reason.
            assert(state_previous == .shutdown);
            self.eviction_reason = null;
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
            assert(timestamp > 0);

            // Submit the next pending packet (if any) now that VSR has completed this one.
            assert(self.client.request_inflight == null);
            if (self.submitted.pop()) |packet_next| {
                self.submit(packet_next);
            }

            // `StateMachine.result_size_bytes` is intended for use on the replica side,
            // as it provides backward compatibility with older clients.
            // On the client side, the size of the compiled result type should be used instead.
            const result_size: usize = inline for (allowed_operations) |operation_comptime| {
                if (operation == operation_comptime) {
                    break @sizeOf(StateMachine.ResultType(operation_comptime));
                }
            } else unreachable;

            assert(result_size % result_size == 0);
            // Implement batching decode here, using
            // the linked list and batch_count.
            assert(packet.next == null);
            assert(batch_count == 0);
            self.notify_completion(packet, .{
                .timestamp = timestamp,
                .reply = reply,
            });
        }

        fn signal_notify_callback(signal: *Signal) void {
            const self: *Context = @alignCast(@fieldParentPtr("signal", signal));

            // Don't send any requests until registration completes.
            if (self.batch_size_limit == null) {
                assert(self.client.request_inflight != null);
                assert(self.client.request_inflight.?.message.header.operation == .register);
                return;
            }

            switch (self.state.load(.acquire)) {
                .running => if (self.client.request_inflight == null) {
                    if (self.submitted.pop()) |packet| {
                        self.submit(packet);
                    }
                },
                .evicted => return,
                .shutdown => return,
            }
        }

        /// Called by the user thread when a packet is submitted.
        /// This function is thread-safe.
        fn on_submit(implementation: *ContextImplementation, packet: *Packet) void {
            const self = get_context(implementation);
            // Packet is caller-allocated to enable elastic intrusive-link-list-based memory
            // management. However, some of Packet's fields are essentially private. Initialize
            // them here to avoid threading default fields through FFI boundary.
            packet.* = .{
                .next = null,
                .user_data = packet.user_data,
                .operation = packet.operation,
                .data_size = packet.data_size,
                .data = packet.data,
                .status = .ok,
            };

            // The caller can try to submit during shudown/eviction.
            const state = self.state.load(.acquire);
            if (state != .running) {
                self.cancel(packet);
                return;
            }

            // Enqueue the packet and notify the IO thread to process it asynchronously.
            self.submitted.push(packet);
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
            assert(packet.next == null);

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

                // The packet completed with an error.
                (self.completion_fn)(
                    completion_ctx,
                    tb_client,
                    packet,
                    0,
                    null,
                    0,
                );
                return;
            };

            // The packet completed normally.
            assert(packet.status == .ok);
            (self.completion_fn)(
                completion_ctx,
                tb_client,
                packet,
                result.timestamp,
                result.reply.ptr,
                @intCast(result.reply.len),
            );
        }
    };
}
