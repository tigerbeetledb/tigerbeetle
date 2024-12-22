const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const vsr = @import("../tb_client.zig").vsr;
const Header = vsr.Header;
const stdx = vsr.stdx;
const constants = vsr.constants;
const MessagePool = vsr.message_pool.MessagePool;
const Message = MessagePool.Message;

pub fn EchoClientType(
    comptime StateMachine_: type,
    comptime MessageBus: type,
    comptime Time: type,
) type {
    return struct {
        const EchoClient = @This();

        // Exposing the same types the real client does:
        const VSRClient = vsr.ClientType(StateMachine_, MessageBus, Time);

        pub const StateMachine = EchoStateMachineType(VSRClient.StateMachine);
        pub const Request = VSRClient.Request;

        id: u128,
        cluster: u128,
        release: vsr.Release = vsr.Release.minimum,
        request_number: u32 = 0,
        reply_timestamp: u64 = 0, // Fake timestamp, just a counter.
        request_inflight: ?Request = null,
        message_pool: *MessagePool,

        pub fn init(
            allocator: mem.Allocator,
            options: struct {
                id: u128,
                cluster: u128,
                replica_count: u8,
                time: Time,
                message_pool: *MessagePool,
                message_bus_options: MessageBus.Options,
                eviction_callback: ?*const fn (
                    client: *EchoClient,
                    eviction: *const Message.Eviction,
                ) void = null,
            },
        ) !EchoClient {
            _ = allocator;
            _ = options.replica_count;
            _ = options.message_bus_options;

            return EchoClient{
                .id = options.id,
                .cluster = options.cluster,
                .message_pool = options.message_pool,
            };
        }

        pub fn deinit(self: *EchoClient, allocator: std.mem.Allocator) void {
            _ = allocator;
            if (self.request_inflight) |inflight| self.release_message(inflight.message.base());
        }

        pub fn tick(self: *EchoClient) void {
            const inflight = self.request_inflight orelse return;
            self.request_inflight = null;

            self.reply_timestamp += 1;
            const timestamp = self.reply_timestamp;
            const batch_count = inflight.message.header.batch_count;

            // Allocate a reply message.
            const reply = self.get_message().build(.request);
            defer self.release_message(reply.base());

            // Copy the request message's entire content including header into the reply.
            const operation = inflight.message.header.operation;
            stdx.copy_disjoint(
                .exact,
                u8,
                reply.buffer,
                inflight.message.buffer,
            );

            // Similarly to the real client, release the request message before invoking the
            // callback. This necessitates a `copy_disjoint` above.
            self.release_message(inflight.message.base());

            switch (inflight.callback) {
                .request => |callback| {
                    callback(
                        inflight.user_data,
                        operation.cast(EchoClient.StateMachine),
                        timestamp,
                        reply.body_used(),
                        batch_count,
                    );
                },
                .register => |callback| {
                    const result = vsr.RegisterResult{
                        .batch_size_limit = constants.message_body_size_max,
                    };
                    callback(inflight.user_data, &result);
                },
            }
        }

        pub fn register(
            self: *EchoClient,
            callback: Request.RegisterCallback,
            user_data: u128,
        ) void {
            assert(self.request_inflight == null);
            assert(self.request_number == 0);

            const message = self.get_message().build(.request);
            errdefer self.release_message(message.base());

            // We will set parent, session, view and checksums only when sending for the first time:
            message.header.* = .{
                .client = self.id,
                .request = self.request_number,
                .cluster = self.cluster,
                .command = .request,
                .operation = .register,
                .release = vsr.Release.minimum,
                .batch_count = 0,
            };

            assert(self.request_number == 0);
            self.request_number += 1;

            self.request_inflight = .{
                .message = message,
                .user_data = user_data,
                .callback = .{ .register = callback },
            };
        }

        pub fn request(
            self: *EchoClient,
            callback: Request.Callback,
            user_data: u128,
            operation: StateMachine.Operation,
            events: []const u8,
            batch_count: u16,
        ) void {
            const event_size: usize = switch (operation) {
                inline else => |operation_comptime| @sizeOf(
                    StateMachine.EventType(operation_comptime),
                ),
            };
            assert(events.len <= constants.message_body_size_max);
            assert(events.len % event_size == 0);

            const message = self.get_message().build(.request);
            errdefer self.release_message(message.base());

            message.header.* = .{
                .client = self.id,
                .request = 0, // Set by raw_request() below.
                .cluster = self.cluster,
                .command = .request,
                .release = vsr.Release.minimum,
                .operation = vsr.Operation.from(StateMachine, operation),
                .size = @intCast(@sizeOf(Header) + events.len),
                .batch_count = batch_count,
            };

            stdx.copy_disjoint(.exact, u8, message.body_used(), events);
            self.raw_request(callback, user_data, message);
        }

        pub fn raw_request(
            self: *EchoClient,
            callback: Request.Callback,
            user_data: u128,
            message: *Message.Request,
        ) void {
            assert(message.header.client == self.id);
            assert(message.header.cluster == self.cluster);
            assert(message.header.release.value == self.release.value);
            assert(!message.header.operation.vsr_reserved());
            assert(message.header.size >= @sizeOf(Header));
            assert(message.header.size <= constants.message_size_max);

            message.header.request = self.request_number;
            self.request_number += 1;

            assert(self.request_inflight == null);
            self.request_inflight = .{
                .message = message,
                .user_data = user_data,
                .callback = .{ .request = callback },
            };
        }

        pub fn get_message(self: *EchoClient) *Message {
            return self.message_pool.get_message(null);
        }

        pub fn release_message(self: *EchoClient, message: *Message) void {
            self.message_pool.unref(message);
        }
    };
}

/// Re-exports all StateMachine symbols used by the client, but making `Event` and `Result`
/// the same types.
fn EchoStateMachineType(comptime StateMachine: type) type {
    return struct {
        pub const Operation = StateMachine.Operation;

        pub const EventType = StateMachine.EventType;
        pub const event_size_bytes = StateMachine.event_size_bytes;
        pub const event_is_slice = StateMachine.event_is_slice;

        pub const ResultType = StateMachine.EventType; // Results are equal Events.
        pub const result_size_bytes = StateMachine.event_size_bytes;

        pub const operation_batch_max = StateMachine.operation_batch_max;
        pub const operation_from_vsr = StateMachine.operation_from_vsr;
    };
}
