const std = @import("std");
const stdx = @import("../stdx.zig");
const mem = std.mem;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const Header = vsr.Header;

const MessagePool = @import("../message_pool.zig").MessagePool;
const Message = @import("../message_pool.zig").MessagePool.Message;

const log = stdx.log.scoped(.client);

pub fn ClientType(
    comptime StateMachine_: type,
    comptime MessageBus: type,
    comptime Time: type,
) type {
    return struct {
        const Client = @This();

        pub const StateMachine = StateMachine_;
        pub const DemuxerType = StateMachine.DemuxerType;
        pub const Request = struct {
            pub const Callback = *const fn (
                user_data: u128,
                operation: StateMachine.Operation,
                timestamp: u64,
                results: []u8,
            ) void;

            pub const RegisterCallback = *const fn (
                user_data: u128,
                result: *const vsr.RegisterResult,
            ) void;

            message: *Message.Request,
            user_data: u128,
            callback: union(enum) {
                /// When message.header.operation ≠ .register
                request: Callback,
                /// When message.header.operation = .register
                register: RegisterCallback,
            },
        };

        message_bus: MessageBus,

        time: Time,

        /// A universally unique identifier for the client (must not be zero).
        /// Used for routing replies back to the client via any network path (multi-path routing).
        /// The client ID must be ephemeral and random per process, and never persisted, so that
        /// lingering or zombie deployment processes cannot break correctness and/or liveness.
        /// A cryptographic random number generator must be used to ensure these properties.
        id: u128,

        /// The identifier for the cluster that this client intends to communicate with.
        cluster: u128,

        /// The number of replicas in the cluster.
        replica_count: u8,

        /// Only tests should ever override the release.
        release: vsr.Release = constants.config.process.release,

        /// The total number of ticks elapsed since the client was initialized.
        ticks: u64 = 0,

        /// We hash-chain request/reply checksums to verify linearizability within a client session:
        /// * so that the parent of the next request is the checksum of the latest reply, and
        /// * so that the parent of the next reply is the checksum of the latest request.
        parent: u128 = 0,

        /// The session number for the client, zero when registering a session, non-zero thereafter.
        session: u64 = 0,

        /// The request number of the next request.
        request_number: u32 = 0,

        /// Measures the time elapsed between sending a request (in `raw_request`) and receiving the
        /// corresponding reply (in `on_reply`).
        request_completion_timer: std.time.Timer,

        /// The maximum body size for `command=request` messages.
        /// Set by the `register`'s reply.
        batch_size_limit: ?u32 = null,

        /// The highest view number seen by the client in messages exchanged with the cluster. Used
        /// to locate the current primary, and provide more information to a partitioned primary.
        view: u32 = 0,

        /// Tracks a currently processing (non-register) request message submitted by `register()`
        /// or `raw_request()`.
        request_inflight: ?Request = null,

        /// The number of ticks without a reply before the client resends the inflight request.
        /// Dynamically adjusted as a function of recent request round-trip time.
        request_timeout: vsr.Timeout,

        /// The number of ticks before the client broadcasts a ping to the cluster.
        /// Used for end-to-end keepalive, and to discover a new primary between requests.
        ping_timeout: vsr.Timeout,

        /// The round-trip time (estimated by the latest ping/pong pair) from each replica.
        replica_round_trip_times_ns: [constants.replicas_max]?u64 =
            .{null} ** constants.replicas_max,

        /// Used to calculate exponential backoff with random jitter.
        /// Seeded with the client's ID.
        prng: std.rand.DefaultPrng,

        on_reply_context: ?*anyopaque = null,
        /// Used for testing. Called for replies to all operations (including `register`).
        on_reply_callback: ?*const fn (
            client: *Client,
            request: *Message.Request,
            reply: *Message.Reply,
        ) void = null,

        evicted: bool = false,
        on_eviction_callback: ?*const fn (
            client: *Client,
            eviction: *const Message.Eviction,
        ) void = null,

        pub fn init(
            allocator: mem.Allocator,
            options: struct {
                id: u128,
                cluster: u128,
                replica_count: u8,
                time: Time,
                message_pool: *MessagePool,
                message_bus_options: MessageBus.Options,
                /// When eviction_callback is null, the client will panic on eviction.
                ///
                /// When eviction_callback is non-null, it must `deinit()` the Client.
                /// After eviction, the client must not send or process any additional messages.
                eviction_callback: ?*const fn (
                    client: *Client,
                    eviction: *const Message.Eviction,
                ) void = null,
            },
        ) !Client {
            assert(options.id > 0);
            assert(options.replica_count > 0);

            var message_bus = try MessageBus.init(
                allocator,
                options.cluster,
                .{ .client = options.id },
                options.message_pool,
                Client.on_message,
                options.message_bus_options,
            );
            errdefer message_bus.deinit(allocator);

            var self = Client{
                .message_bus = message_bus,
                .time = options.time,
                .id = options.id,
                .cluster = options.cluster,
                .replica_count = options.replica_count,
                .request_completion_timer = try std.time.Timer.start(),
                .request_timeout = .{
                    .name = "request_timeout",
                    .id = options.id,
                    .after = constants.rtt_ticks * constants.rtt_multiple,
                },
                .ping_timeout = .{
                    .name = "ping_timeout",
                    .id = options.id,
                    .after = 30000 / constants.tick_ms,
                },
                .prng = std.rand.DefaultPrng.init(@as(u64, @truncate(options.id))),
                .on_eviction_callback = options.eviction_callback,
            };

            self.ping_timeout.start();

            return self;
        }

        pub fn deinit(self: *Client, allocator: std.mem.Allocator) void {
            if (self.request_inflight) |inflight| self.release_message(inflight.message.base());
            self.message_bus.deinit(allocator);
        }

        pub fn on_message(message_bus: *MessageBus, message: *Message) void {
            const self: *Client = @fieldParentPtr("message_bus", message_bus);
            assert(!self.evicted);

            // Switch on the header type so that we don't log opaque bytes for the per-command data.
            switch (message.header.into_any()) {
                inline else => |header| {
                    log.debug("{}: on_message: {}", .{ self.id, header });
                },
            }

            if (message.header.invalid()) |reason| {
                log.debug("{}: on_message: invalid ({s})", .{ self.id, reason });
                return;
            }
            if (message.header.cluster != self.cluster) {
                log.warn("{}: on_message: wrong cluster (cluster should be {}, not {})", .{
                    self.id,
                    self.cluster,
                    message.header.cluster,
                });
                return;
            }
            switch (message.into_any()) {
                .pong_client => |m| self.on_pong_client(m),
                .reply => |m| self.on_reply(m),
                .eviction => |m| self.on_eviction(m),
                else => {
                    log.warn("{}: on_message: ignoring misdirected {s} message", .{
                        self.id,
                        @tagName(message.header.command),
                    });
                    return;
                },
            }
        }

        pub fn tick(self: *Client) void {
            assert(!self.evicted);

            self.ticks += 1;

            self.message_bus.tick();
            self.time.tick();

            self.ping_timeout.tick();
            self.request_timeout.tick();

            if (self.ping_timeout.fired()) self.on_ping_timeout();
            if (self.request_timeout.fired()) self.on_request_timeout();
        }

        /// Registers a session with the cluster for the client, if this has not yet been done.
        pub fn register(self: *Client, callback: Request.RegisterCallback, user_data: u128) void {
            assert(!self.evicted);
            assert(self.request_inflight == null);
            assert(self.request_number == 0);

            const message = self.get_message().build(.request);
            errdefer self.release_message(message.base());

            // We will set parent, session, view and checksums only when sending for the first time:
            message.header.* = .{
                .size = @sizeOf(Header) + @sizeOf(vsr.RegisterRequest),
                .client = self.id,
                .request = self.request_number,
                .cluster = self.cluster,
                .command = .request,
                .operation = .register,
                .release = self.release,
            };

            std.mem.bytesAsValue(
                vsr.RegisterRequest,
                message.body_used()[0..@sizeOf(vsr.RegisterRequest)],
            ).* = .{
                .batch_size_limit = 0,
            };

            assert(self.request_number == 0);
            self.request_number += 1;

            log.debug(
                "{}: register: registering a session with the cluster user_data={}",
                .{ self.id, user_data },
            );

            self.request_inflight = .{
                .message = message,
                .user_data = user_data,
                .callback = .{ .register = callback },
            };
            self.send_request_for_the_first_time(message);
        }

        /// Sends a request message with the operation and events payload to the replica.
        /// There must be no other request message currently inflight.
        pub fn request(
            self: *Client,
            callback: Request.Callback,
            user_data: u128,
            operation: StateMachine.Operation,
            events: []const u8,
        ) void {
            const event_size: usize = switch (operation) {
                inline else => |operation_comptime| @sizeOf(
                    StateMachine.EventType(operation_comptime),
                ),
            };
            assert(!self.evicted);
            assert(self.request_inflight == null);
            assert(self.request_number > 0);
            assert(events.len <= constants.message_body_size_max);
            assert(events.len <= self.batch_size_limit.?);
            assert(events.len % event_size == 0);

            const message = self.get_message().build(.request);
            errdefer self.release_message(message.base());

            message.header.* = .{
                .client = self.id,
                .request = 0, // Set inside `raw_request` down below.
                .cluster = self.cluster,
                .command = .request,
                .release = self.release,
                .operation = vsr.Operation.from(StateMachine, operation),
                .size = @intCast(@sizeOf(Header) + events.len),
            };

            stdx.copy_disjoint(.exact, u8, message.body_used(), events);
            self.raw_request(callback, user_data, message);
        }

        /// Sends a request, only setting request_number in the header.
        /// There must be no other request message currently inflight.
        pub fn raw_request(
            self: *Client,
            callback: Request.Callback,
            user_data: u128,
            message: *Message.Request,
        ) void {
            assert(self.request_inflight == null);
            assert(self.request_number > 0);
            assert(message.header.client == self.id);
            assert(message.header.release.value == self.release.value);
            assert(message.header.cluster == self.cluster);
            assert(message.header.command == .request);
            assert(message.header.size >= @sizeOf(Header));
            assert(message.header.size <= constants.message_size_max);
            assert(message.header.size <= @sizeOf(Header) + self.batch_size_limit.?);
            assert(message.header.operation.valid(StateMachine));
            assert(message.header.view == 0);
            assert(message.header.parent == 0);
            assert(message.header.session == 0);
            assert(message.header.request == 0);

            if (!constants.aof_recovery) {
                assert(!message.header.operation.vsr_reserved());
            }

            // TODO: Re-investigate this state for AOF as it currently traps.
            // assert(message.header.timestamp == 0 or constants.aof_recovery);

            message.header.request = self.request_number;
            self.request_number += 1;
            self.request_completion_timer.reset();

            log.debug("{}: request: user_data={} request={} size={} {s}", .{
                self.id,
                user_data,
                message.header.request,
                message.header.size,
                message.header.operation.tag_name(StateMachine),
            });

            self.request_inflight = .{
                .message = message,
                .user_data = user_data,
                .callback = .{ .request = callback },
            };
            self.send_request_for_the_first_time(message);
        }

        /// Acquires a message from the message bus.
        /// The caller must ensure that a message is available.
        ///
        /// Either use it in `client.raw_request()` or discard via `client.release_message()`,
        /// the reference is not guaranteed to be valid after both actions.
        /// Do NOT use the reference counter function `message.ref()` for storing the message.
        pub fn get_message(self: *Client) *Message {
            return self.message_bus.get_message(null);
        }

        /// Releases a message back to the message bus.
        pub fn release_message(self: *Client, message: *Message) void {
            self.message_bus.unref(message);
        }

        fn on_eviction(self: *Client, eviction: *const Message.Eviction) void {
            assert(!self.evicted);
            assert(eviction.header.command == .eviction);
            assert(eviction.header.cluster == self.cluster);

            if (eviction.header.client != self.id) {
                log.warn("{}: on_eviction: ignoring (wrong client={})", .{
                    self.id,
                    eviction.header.client,
                });
                return;
            }

            if (eviction.header.view < self.view) {
                log.debug("{}: on_eviction: ignoring (older view={})", .{
                    self.id,
                    eviction.header.view,
                });
                return;
            }

            assert(eviction.header.client == self.id);
            assert(eviction.header.view >= self.view);

            if (self.on_eviction_callback) |callback| {
                const eviction_specific_log = switch (eviction.header.reason) {
                    .client_release_too_low => " - your client is too old; upgrade to a version " ++
                        "compatible with your cluster",
                    .client_release_too_high => " - your client is too new; downgrade to the " ++
                        "same version as your cluster",
                    else => "",
                };
                log.err(
                    "{}: session evicted: reason={?s} (cluster_release={}, client_release={}){s}",
                    .{
                        self.id,
                        std.enums.tagName(vsr.Header.Eviction.Reason, eviction.header.reason),
                        eviction.header.release,
                        self.release,
                        eviction_specific_log,
                    },
                );

                self.evicted = true;
                self.on_eviction_callback = null;
                callback(self, eviction);
            } else {
                std.debug.panic("session evicted: {?s} (cluster_release={})", .{
                    std.enums.tagName(vsr.Header.Eviction.Reason, eviction.header.reason),
                    eviction.header.release,
                });
            }
        }

        fn on_pong_client(self: *Client, pong: *const Message.PongClient) void {
            assert(pong.header.command == .pong_client);
            assert(pong.header.cluster == self.cluster);

            if (pong.header.view > self.view) {
                log.debug("{}: on_pong: newer view={}..{}", .{
                    self.id,
                    self.view,
                    pong.header.view,
                });
                self.view = pong.header.view;
            }

            const ping_timestamp_monotonic = pong.header.ping_timestamp_monotonic;
            const pong_timestamp_monotonic = self.time.monotonic();
            if (ping_timestamp_monotonic <= pong_timestamp_monotonic) {
                self.replica_round_trip_times_ns[pong.header.replica] =
                    pong_timestamp_monotonic - ping_timestamp_monotonic;

                var round_trip_times_ns = stdx.BoundedArrayType(u64, constants.replicas_max){};
                for (self.replica_round_trip_times_ns) |round_trip_time_ns| {
                    if (round_trip_time_ns) |rtt_ns| {
                        round_trip_times_ns.append_assume_capacity(rtt_ns);
                    }
                }
                std.mem.sort(u64, round_trip_times_ns.slice(), {}, std.sort.asc(u64));
                assert(round_trip_times_ns.count() > 0);

                const rtt_median_ns =
                    round_trip_times_ns.get(@divFloor(round_trip_times_ns.count(), 2));
                self.request_timeout.set_rtt_ns(rtt_median_ns);
            } else {
                log.debug("{}: on_pong: monotonic timestamp regressed {}..{} replica={}", .{
                    self.id,
                    ping_timestamp_monotonic,
                    pong_timestamp_monotonic,
                    pong.header.replica,
                });
            }
        }

        fn on_reply(self: *Client, reply: *Message.Reply) void {
            // We check these checksums again here because this is the last time we get to downgrade
            // a correctness bug into a liveness bug, before we return data back to the application.
            assert(reply.header.valid_checksum());
            assert(reply.header.valid_checksum_body(reply.body_used()));
            assert(reply.header.command == .reply);
            assert(reply.header.release.value == self.release.value);

            if (reply.header.client != self.id) {
                log.debug("{}: on_reply: ignoring (wrong client={})", .{
                    self.id,
                    reply.header.client,
                });
                return;
            }

            var inflight = self.request_inflight orelse {
                assert(reply.header.request < self.request_number);
                log.debug("{}: on_reply: ignoring (no inflight request)", .{self.id});
                return;
            };

            if (reply.header.request < inflight.message.header.request) {
                assert(inflight.message.header.request > 0);
                assert(inflight.message.header.operation != .register);

                log.debug("{}: on_reply: ignoring (request {} < {})", .{
                    self.id,
                    reply.header.request,
                    inflight.message.header.request,
                });
                return;
            }

            assert(reply.header.request == inflight.message.header.request);
            assert(reply.header.request_checksum == inflight.message.header.checksum);
            const inflight_vsr_operation = inflight.message.header.operation;
            const inflight_request = inflight.message.header.request;

            if (inflight_vsr_operation == .register) {
                assert(inflight_request == 0);
            } else {
                assert(inflight_request > 0);
            }
            // Consume the inflight request here before invoking callbacks down below in case they
            // wish to queue a new `request_inflight`.
            assert(inflight.message == self.request_inflight.?.message);
            self.request_inflight = null;

            if (self.on_reply_callback) |on_reply_callback| {
                on_reply_callback(self, inflight.message, reply);
            }

            log.debug("{}: on_reply: user_data={} request={} size={} {s}", .{
                self.id,
                inflight.user_data,
                reply.header.request,
                reply.header.size,
                reply.header.operation.tag_name(StateMachine),
            });

            assert(reply.header.request_checksum == self.parent);
            assert(reply.header.client == self.id);
            assert(reply.header.request == inflight_request);
            assert(reply.header.cluster == self.cluster);
            assert(reply.header.op == reply.header.commit);
            assert(reply.header.operation == inflight_vsr_operation);

            const request_completion_time_ms = @divFloor(
                self.request_completion_timer.read(),
                std.time.ns_per_ms,
            );
            if (request_completion_time_ms > constants.client_request_completion_warn_ms) {
                log.warn("{}: on_reply: request={} size={} {s} time={}ms", .{
                    self.id,
                    reply.header.request,
                    reply.header.size,
                    reply.header.operation.tag_name(StateMachine),
                    request_completion_time_ms,
                });
            }

            // The context of this reply becomes the parent of our next request:
            self.parent = reply.header.context;

            if (reply.header.view > self.view) {
                log.debug("{}: on_reply: newer view={}..{}", .{
                    self.id,
                    self.view,
                    reply.header.view,
                });
                self.view = reply.header.view;
            }

            self.request_timeout.stop();

            // Release request message to ensure that inflight's callback can submit a new one.
            self.release_message(inflight.message.base());
            inflight.message = undefined;

            if (inflight_vsr_operation == .register) {
                assert(inflight_request == 0);
                assert(self.batch_size_limit == null);
                assert(self.session == 0);
                assert(reply.header.commit > 0);
                assert(reply.header.size == @sizeOf(Header) + @sizeOf(vsr.RegisterResult));

                const result = std.mem.bytesAsValue(
                    vsr.RegisterResult,
                    reply.body_used()[0..@sizeOf(vsr.RegisterResult)],
                );
                assert(result.batch_size_limit > 0);
                assert(result.batch_size_limit <= constants.message_body_size_max);

                self.session = reply.header.commit; // The commit number becomes the session number.
                self.batch_size_limit = result.batch_size_limit;
                inflight.callback.register(inflight.user_data, result);
            } else {
                // The message is the result of raw_request(), so invoke the user callback.
                // NOTE: the callback is allowed to mutate `reply.body_used()` here.
                inflight.callback.request(
                    inflight.user_data,
                    inflight_vsr_operation.cast(StateMachine),
                    reply.header.timestamp,
                    reply.body_used(),
                );
            }
        }

        fn on_ping_timeout(self: *Client) void {
            self.ping_timeout.reset();

            const ping = Header.PingClient{
                .command = .ping_client,
                .cluster = self.cluster,
                .release = self.release,
                .client = self.id,
                .ping_timestamp_monotonic = self.time.monotonic(),
            };

            // TODO If we haven't received a pong from a replica since our last ping, then back off.
            self.send_header_to_replicas(ping.frame_const().*);
        }

        fn on_request_timeout(self: *Client) void {
            self.request_timeout.backoff(self.prng.random());

            const message = self.request_inflight.?.message;
            assert(message.header.command == .request);
            assert(message.header.request < self.request_number);
            assert(message.header.checksum == self.parent);
            assert(message.header.session == self.session);

            log.debug("{}: on_request_timeout: resending request={} checksum={}", .{
                self.id,
                message.header.request,
                message.header.checksum,
            });

            // We assume the primary is down and round-robin through the cluster:
            self.send_message_to_replica(
                @as(u8, @intCast((self.view + self.request_timeout.attempts) % self.replica_count)),
                message.base(),
            );
        }

        /// The caller owns the returned message, if any, which has exactly 1 reference.
        fn create_message_from_header(self: *Client, header: Header) *Message {
            assert(header.cluster == self.cluster);
            assert(header.size == @sizeOf(Header));

            const message = self.message_bus.get_message(null);
            defer self.message_bus.unref(message);

            message.header.* = header;
            message.header.set_checksum_body(message.body_used());
            message.header.set_checksum();

            return message.ref();
        }

        fn send_header_to_replicas(self: *Client, header: Header) void {
            const message = self.create_message_from_header(header);
            defer self.message_bus.unref(message);

            var replica: u8 = 0;
            while (replica < self.replica_count) : (replica += 1) {
                self.send_message_to_replica(replica, message);
            }
        }

        fn send_message_to_replica(self: *Client, replica: u8, message: *Message) void {
            // Switch on the header type so that we don't log opaque bytes for the per-command data.
            switch (message.header.into_any()) {
                inline else => |header| {
                    log.debug("{}: sending {s} to replica {}: {}", .{
                        self.id,
                        @tagName(message.header.command),
                        replica,
                        header,
                    });
                },
            }

            assert(replica < self.replica_count);
            assert(message.header.valid_checksum());
            assert(message.header.cluster == self.cluster);

            switch (message.into_any()) {
                inline .request,
                .ping_client,
                => |m| assert(m.header.client == self.id),
                else => unreachable,
            }

            self.message_bus.send_message_to_replica(replica, message);
        }

        fn send_request_for_the_first_time(self: *Client, message: *Message.Request) void {
            assert(self.request_inflight.?.message == message);
            assert(self.request_number > 0);

            assert(message.header.command == .request);
            assert(message.header.parent == 0);
            assert(message.header.session == 0);
            assert(message.header.request < self.request_number);
            assert(message.header.view == 0);
            assert(message.header.size <= constants.message_size_max);

            // We set the message checksums only when sending the request for the first time,
            // which is when we have the checksum of the latest reply available to set as `parent`,
            // and similarly also the session number if requests were queued while registering:
            message.header.parent = self.parent;
            message.header.session = self.session;
            // We also try to include our highest view number, so we wait until the request is ready
            // to be sent for the first time. However, beyond that, it is not necessary to update
            // the view number again, for example if it should change between now and resending.
            message.header.view = self.view;
            message.header.set_checksum_body(message.body_used());
            message.header.set_checksum();

            // The checksum of this request becomes the parent of our next reply:
            self.parent = message.header.checksum;

            log.debug("{}: send_request_for_the_first_time: request={} checksum={}", .{
                self.id,
                message.header.request,
                message.header.checksum,
            });

            assert(!self.request_timeout.ticking);
            self.request_timeout.start();

            // If our view number is out of date, then the old primary will forward our request.
            // If the primary is offline, then our request timeout will fire and we will
            // round-robin.
            self.send_message_to_replica(
                @as(u8, @intCast(self.view % self.replica_count)),
                message.base(),
            );
        }
    };
}
