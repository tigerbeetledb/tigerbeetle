const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const Command = vsr.Command;
const Operation = vsr.Operation;
const schema = @import("../lsm/schema.zig");
const Snapshot = schema.Snapshot;

const checksum_body_empty = vsr.checksum(&.{});

/// Network message and journal entry header:
/// We reuse the same header for both so that prepare messages from the primary can simply be
/// journalled as is by the backups without requiring any further modification.
pub const Header = extern struct {
    /// A checksum covering only the remainder of this header.
    /// This allows the header to be trusted without having to recv() or read() the associated body.
    /// This checksum is enough to uniquely identify a network message or journal entry.
    checksum: u128,

    // TODO(zig): When Zig supports u256 in extern-structs, merge this into `checksum`.
    checksum_padding: u128,

    /// A checksum covering only the associated body after this header.
    checksum_body: u128,

    // TODO(zig): When Zig supports u256 in extern-structs, merge this into `checksum_body`.
    checksum_body_padding: u128,

    /// Reserved for future use by AEAD.
    nonce_reserved: u128,

    /// The cluster number binds intention into the header, so that a client or replica can indicate
    /// the cluster it believes it is speaking to, instead of accidentally talking to the wrong
    /// cluster (for example, staging vs production).
    cluster: u128,

    /// The size of the Header structure (always), plus any associated body.
    size: u32,

    /// The cluster reconfiguration epoch number (for future use).
    epoch: u32,

    /// Every message sent from one replica to another contains the sending replica's current view.
    /// A `u32` allows for a minimum lifetime of 136 years at a rate of one view change per second.
    view: u32,

    /// The version of the protocol implementation that originated this message.
    version: u16,

    /// The Viewstamped Replication protocol command for this message.
    command: Command,

    /// The index of the replica in the cluster configuration array that authored this message.
    /// This identifies only the ultimate author because messages may be forwarded amongst replicas.
    replica: u8,

    /// Reserved for future use by the header frame (i.e. to be shared by all message types).
    reserved_frame: [16]u8,

    /// This data's schema is different depending on the `Header.command`.
    /// (No default value – `Header`s should not be constructed directly.)
    reserved_command: [128]u8,

    comptime {
        assert(@sizeOf(Header) == 256);
        assert(stdx.no_padding(Header));
        assert(@offsetOf(Header, "reserved_command") % @sizeOf(u256) == 0);
    }

    pub fn Type(comptime command: Command) type {
        return switch (command) {
            .reserved => Reserved,
            .ping => Ping,
            .pong => Pong,
            .ping_client => PingClient,
            .pong_client => PongClient,
            .request => Request,
            .prepare => Prepare,
            .prepare_ok => PrepareOk,
            .reply => Reply,
            .commit => Commit,
            .start_view_change => StartViewChange,
            .do_view_change => DoViewChange,
            .start_view => StartView,
            .request_start_view => RequestStartView,
            .request_headers => RequestHeaders,
            .request_prepare => RequestPrepare,
            .request_reply => RequestReply,
            .headers => Headers,
            .eviction => Eviction,
            .request_blocks => RequestBlocks,
            .block => Block,
            .request_sync_checkpoint => RequestSyncCheckpoint,
            .sync_checkpoint => SyncCheckpoint,
        };
    }

    pub fn calculate_checksum(self: *const Header) u128 {
        const checksum_size = @sizeOf(@TypeOf(self.checksum));
        assert(checksum_size == 16);
        const checksum_value = vsr.checksum(std.mem.asBytes(self)[checksum_size..]);
        assert(@TypeOf(checksum_value) == @TypeOf(self.checksum));
        return checksum_value;
    }

    pub fn calculate_checksum_body(self: *const Header, body: []const u8) u128 {
        assert(self.size == @sizeOf(Header) + body.len);
        const checksum_size = @sizeOf(@TypeOf(self.checksum_body));
        assert(checksum_size == 16);
        const checksum_value = vsr.checksum(body);
        assert(@TypeOf(checksum_value) == @TypeOf(self.checksum_body));
        return checksum_value;
    }

    /// This must be called only after set_checksum_body() so that checksum_body is also covered:
    pub fn set_checksum(self: *Header) void {
        self.checksum = self.calculate_checksum();
    }

    pub fn set_checksum_body(self: *Header, body: []const u8) void {
        self.checksum_body = self.calculate_checksum_body(body);
    }

    pub fn valid_checksum(self: *const Header) bool {
        return self.checksum == self.calculate_checksum();
    }

    pub fn valid_checksum_body(self: *const Header, body: []const u8) bool {
        return self.checksum_body == self.calculate_checksum_body(body);
    }

    pub const AnyHeaderPointer = stdx.EnumUnionType(Command, struct {
        fn PointerForCommand(comptime variant: Command) type {
            return *const Type(variant);
        }
    }.PointerForCommand);

    pub fn into_any(self: *const Header) AnyHeaderPointer {
        switch (self.command) {
            inline else => |command| {
                return @unionInit(AnyHeaderPointer, @tagName(command), self.into_const(command).?);
            },
        }
    }

    pub fn into(self: *Header, comptime command: Command) ?*Type(command) {
        if (self.command != command) return null;
        return std.mem.bytesAsValue(Type(command), std.mem.asBytes(self));
    }

    pub fn into_const(self: *const Header, comptime command: Command) ?*const Type(command) {
        if (self.command != command) return null;
        return std.mem.bytesAsValue(Type(command), std.mem.asBytes(self));
    }

    /// Returns null if all fields are set correctly according to the command, or else a warning.
    /// This does not verify that checksum is valid, and expects that this has already been done.
    pub fn invalid(self: *const Header) ?[]const u8 {
        if (self.checksum_padding != 0) return "checksum_padding != 0";
        if (self.checksum_body_padding != 0) return "checksum_body_padding != 0";
        if (self.nonce_reserved != 0) return "nonce_reserved != 0";
        if (self.version != vsr.Version) return "version != Version";
        if (self.size < @sizeOf(Header)) return "size < @sizeOf(Header)";
        if (self.epoch != 0) return "epoch != 0";
        if (!stdx.zeroed(&self.reserved_frame)) return "reserved_frame != 0";

        switch (self.into_any()) {
            inline else => |command_header| return command_header.invalid_header(),
            // The `Command` enum is exhaustive, so we can't write an "else" branch here. An unknown
            // command is a possibility, but that means that someone has send us a message with
            // matching cluster, matching version, correct checksum, and a command we don't know
            // about. Ignoring unknown commands might be unsafe, so the replica intentionally
            // crashes here, which is guaranteed by Zig's ReleaseSafe semantics.
            //
            // _ => unreachable
        }
    }

    /// Returns whether the immediate sender is a replica or client (if this can be determined).
    /// Some commands such as .request or .prepare may be forwarded on to other replicas so that
    /// Header.replica or Header.client only identifies the ultimate origin, not the latest peer.
    pub fn peer_type(self: *const Header) union(enum) {
        unknown,
        replica: u8,
        client: u128,
    } {
        switch (self.into_any()) {
            .reserved => unreachable,
            // These messages cannot always identify the peer as they may be forwarded:
            .request => |request| {
                switch (request.operation) {
                    // However, we do not forward the first .register request sent by a client:
                    .register => return .{ .client = request.client },
                    else => return .unknown,
                }
            },
            .prepare => return .unknown,
            // These messages identify the peer as either a replica or a client:
            .ping_client => |ping| return .{ .client = ping.client },
            // All other messages identify the peer as a replica:
            else => return .{ .replica = self.replica },
        }
    }

    fn HeaderFunctions(comptime CommandHeader: type) type {
        return struct {
            pub fn frame(header: *CommandHeader) *Header {
                return std.mem.bytesAsValue(Header, std.mem.asBytes(header));
            }

            pub fn frame_const(header: *const CommandHeader) *const Header {
                return std.mem.bytesAsValue(Header, std.mem.asBytes(header));
            }

            pub fn invalid(self: *const CommandHeader) ?[]const u8 {
                return self.frame_const().invalid();
            }

            pub fn calculate_checksum(self: *const CommandHeader) u128 {
                return self.frame_const().calculate_checksum();
            }

            pub fn calculate_checksum_body(self: *const CommandHeader, body: []const u8) u128 {
                return self.frame_const().calculate_checksum_body(body);
            }

            pub fn set_checksum(self: *CommandHeader) void {
                self.frame().set_checksum();
            }

            pub fn set_checksum_body(self: *CommandHeader, body: []const u8) void {
                self.frame().set_checksum_body(body);
            }

            pub fn valid_checksum(self: *const CommandHeader) bool {
                return self.frame_const().valid_checksum();
            }

            pub fn valid_checksum_body(self: *const CommandHeader, body: []const u8) bool {
                return self.frame_const().valid_checksum_body(body);
            }
        };
    }

    /// This type isn't ever actually a constructed, but makes Type() simpler by providing a header
    /// type for each command.
    pub const Reserved = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128,
        checksum_padding: u128 = 0,
        checksum_body: u128,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128,
        cluster: u128,
        size: u32,
        epoch: u32 = 0,
        view: u32 = 0,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8 = 0,
        reserved_frame: [16]u8,

        reserved: [128]u8 = [_]u8{0} ** 128,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .reserved);
            return "reserved is invalid";
        }
    };

    pub const Ping = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        // NB: unlike every other message, pings and pongs use on disk view, rather than in-memory
        // view, to avoid disrupting clock synchronization while the view is being updated.
        view: u32 = 0,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        /// Current checkpoint id.
        checkpoint_id: u128,
        /// Current checkpoint op.
        checkpoint_op: u64,

        ping_timestamp_monotonic: u64,

        reserved: [96]u8 = [_]u8{0} ** 96,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .ping);
            if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
            if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
            if (!vsr.Checkpoint.valid(self.checkpoint_op)) return "checkpoint_op invalid";
            if (self.ping_timestamp_monotonic == 0) return "ping_timestamp_monotonic != expected";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const Pong = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        // NB: unlike every other message, pings and pongs use on disk view, rather than in-memory
        // view, to avoid disrupting clock synchronization while the view is being updated.
        view: u32 = 0,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8 = 0,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        ping_timestamp_monotonic: u64,
        pong_timestamp_wall: u64,

        reserved: [112]u8 = [_]u8{0} ** 112,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .pong);
            if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
            if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
            if (self.ping_timestamp_monotonic == 0) return "ping_timestamp_monotonic == 0";
            if (self.pong_timestamp_wall == 0) return "pong_timestamp_wall == 0";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const PingClient = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32 = 0, // Always 0.
        version: u16 = vsr.Version,
        command: Command,
        replica: u8 = 0, // Always 0.
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        client: u128,
        reserved: [112]u8 = [_]u8{0} ** 112,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .ping_client);
            if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
            if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
            if (self.replica != 0) return "replica != 0";
            if (self.view != 0) return "view != 0";
            if (self.client == 0) return "client == 0";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const PongClient = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        reserved: [128]u8 = [_]u8{0} ** 128,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .pong_client);
            if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
            if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const Request = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32 = 0,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8 = 0, // Always 0.
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        /// Clients hash-chain their requests to verify linearizability:
        /// - A session's first request (operation=register) sets `parent=0`.
        /// - A session's subsequent requests (operation≠register) set `parent` to the checksum of
        ///   the preceding request.
        parent: u128 = 0,
        parent_padding: u128 = 0,
        /// Each client process generates a unique, random and ephemeral client ID at
        /// initialization. The client ID identifies connections made by the client to the cluster
        /// for the sake of routing messages back to the client.
        ///
        /// With the client ID in hand, the client then registers a monotonically increasing session
        /// number (committed through the cluster) to allow the client's session to be evicted
        /// safely from the client table if too many concurrent clients cause the client table to
        /// overflow. The monotonically increasing session number prevents duplicate client requests
        /// from being replayed.
        ///
        /// The problem of routing is therefore solved by the 128-bit client ID, and the problem of
        /// detecting whether a session has been evicted is solved by the session number.
        client: u128,
        /// When operation=register, this is zero.
        /// When operation≠register, this is the commit number of register.
        session: u64 = 0,
        /// Only nonzero during AOF recovery.
        /// TODO: Use this for bulk-import to state machine?
        timestamp: u64 = 0,
        /// Each request is given a number by the client and later requests must have larger numbers
        /// than earlier ones. The request number is used by the replicas to avoid running requests
        /// more than once; it is also used by the client to discard duplicate replies to its
        /// requests.
        ///
        /// A client is allowed to have at most one request inflight at a time.
        request: u32,
        operation: Operation,
        reserved: [59]u8 = [_]u8{0} ** 59,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .request);
            if (self.replica != 0) return "replica != 0";
            if (self.parent_padding != 0) return "parent_padding != 0";
            if (self.client == 0) return "client == 0";
            if (self.timestamp != 0 and !constants.aof_recovery) return "timestamp != 0";
            switch (self.operation) {
                .reserved => return "operation == .reserved",
                .root => return "operation == .root",
                .register => {
                    // The first request a client makes must be to register with the cluster:
                    if (self.parent != 0) return "register: parent != 0";
                    if (self.session != 0) return "register: session != 0";
                    if (self.request != 0) return "register: request != 0";
                    // The .register operation carries no payload:
                    if (self.checksum_body != checksum_body_empty) {
                        return "register: checksum_body != expected";
                    }
                    if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
                },
                else => {
                    if (self.operation == .reconfigure) {
                        if (self.size != @sizeOf(Header) + @sizeOf(vsr.ReconfigurationRequest)) {
                            return "size != @sizeOf(Header) + @sizeOf(ReconfigurationRequest)";
                        }
                    } else if (@intFromEnum(self.operation) < constants.vsr_operations_reserved) {
                        return "operation is reserved";
                    }
                    // Thereafter, the client must provide the session number:
                    // These requests should set `parent` to the `checksum` of the previous reply.
                    if (self.session == 0) return "session == 0";
                    if (self.request == 0) return "request == 0";
                    // The Replica is responsible for checking the `Operation` is a valid variant –
                    // the check requires the StateMachine type.
                },
            }
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const Prepare = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8 = 0,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        /// A backpointer to the previous prepare checksum for hash chain verification.
        /// This provides a cryptographic guarantee for linearizability across our distributed log
        /// of prepares.
        ///
        /// This may also be used as the initialization vector for AEAD encryption at rest, provided
        /// that the primary ratchets the encryption key every view change to ensure that prepares
        /// reordered through a view change never repeat the same IV for the same encryption key.
        parent: u128,
        parent_padding: u128 = 0,
        /// The checksum of the client's request.
        request_checksum: u128,
        request_checksum_padding: u128 = 0,
        /// The id of the checkpoint where:
        ///
        ///   prepare.op > checkpoint_op
        ///   prepare.op ≤ checkpoint_after(checkpoint_op)
        ///
        /// The purpose of including the checkpoint id is to strictly bound the number of commits
        /// that it may take to discover a divergent replica. If a replica diverges, then that
        /// divergence will be discovered *at latest* when the divergent replica attempts to commit
        /// the first op after the next checkpoint.
        checkpoint_id: u128,
        client: u128,
        /// The op number of the latest prepare that may or may not yet be committed. Uncommitted
        /// ops may be replaced by different ops if they do not survive through a view change.
        op: u64,
        /// The commit number of the latest committed prepare. Committed ops are immutable.
        commit: u64,
        /// The primary's state machine `prepare_timestamp`.
        /// For `create_accounts` and `create_transfers` this is the batch's highest timestamp.
        timestamp: u64,
        request: u32,
        /// The state machine operation to apply.
        operation: Operation,
        reserved: [3]u8 = [_]u8{0} ** 3,

        fn invalid_header(self: *const Prepare) ?[]const u8 {
            assert(self.command == .prepare);
            if (self.parent_padding != 0) return "parent_padding != 0";
            if (self.request_checksum_padding != 0) return "request_checksum_padding != 0";
            switch (self.operation) {
                .reserved => {
                    if (self.size != @sizeOf(Header)) return "reserved: size != @sizeOf(Header)";
                    if (self.checksum_body != checksum_body_empty) {
                        return "reserved: checksum_body != expected";
                    }
                    if (self.view != 0) return "reserved: view != 0";
                    if (self.replica != 0) return "reserved: replica != 0";
                    if (self.parent != 0) return "reserved: parent != 0";
                    if (self.client != 0) return "reserved: client != 0";
                    if (self.request_checksum != 0) return "reserved: request_checksum != 0";
                    if (self.checkpoint_id != 0) return "reserved: checkpoint_id != 0";
                    maybe(self.op == 0);
                    if (self.commit != 0) return "reserved: commit != 0";
                    if (self.request != 0) return "reserved: request != 0";
                    if (self.timestamp != 0) return "reserved: timestamp != 0";
                },
                .root => {
                    if (self.size != @sizeOf(Header)) return "root: size != @sizeOf(Header)";
                    if (self.checksum_body != checksum_body_empty) {
                        return "root: checksum_body != expected";
                    }
                    if (self.view != 0) return "root: view != 0";
                    if (self.replica != 0) return "root: replica != 0";
                    if (self.parent != 0) return "root: parent != 0";
                    if (self.client != 0) return "root: client != 0";
                    if (self.request_checksum != 0) return "root: request_checksum != 0";
                    if (self.checkpoint_id != 0) return "root: checkpoint_id != 0";
                    if (self.op != 0) return "root: op != 0";
                    if (self.commit != 0) return "root: commit != 0";
                    if (self.timestamp != 0) return "root: timestamp != 0";
                    if (self.request != 0) return "root: request != 0";
                },
                else => {
                    if (self.client == 0) return "client == 0";
                    if (self.op == 0) return "op == 0";
                    if (self.op <= self.commit) return "op <= commit";
                    if (self.timestamp == 0) return "timestamp == 0";
                    if (self.operation == .register) {
                        if (self.request != 0) return "request != 0";
                    } else {
                        if (self.request == 0) return "request == 0";
                    }
                },
            }
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }

        pub fn reserved(cluster: u128, slot: u64) Prepare {
            assert(slot < constants.journal_slot_count);

            var header = Prepare{
                .command = .prepare,
                .cluster = cluster,
                .op = slot,
                .operation = .reserved,
                .view = 0,
                .request_checksum = 0,
                .checkpoint_id = 0,
                .parent = 0,
                .client = 0,
                .commit = 0,
                .timestamp = 0,
                .request = 0,
            };
            header.set_checksum_body(&[0]u8{});
            header.set_checksum();
            assert(header.invalid() == null);
            return header;
        }

        pub fn root(cluster: u128) Prepare {
            var header = Prepare{
                .cluster = cluster,
                .size = @sizeOf(Header),
                .command = .prepare,
                .operation = .root,
                .op = 0,
                .view = 0,
                .request_checksum = 0,
                .checkpoint_id = 0,
                .parent = 0,
                .client = 0,
                .commit = 0,
                .timestamp = 0,
                .request = 0,
            };
            header.set_checksum_body(&[0]u8{});
            header.set_checksum();
            assert(header.invalid() == null);
            return header;
        }
    };

    pub const PrepareOk = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        /// The previous prepare's checksum.
        /// (Same as the corresponding Prepare's `parent`.)
        parent: u128,
        parent_padding: u128 = 0,
        /// The corresponding prepare's checksum.
        prepare_checksum: u128,
        prepare_checksum_padding: u128 = 0,
        /// The corresponding prepare's checkpoint_id.
        checkpoint_id: u128,
        client: u128,
        op: u64,
        commit: u64,
        timestamp: u64,
        request: u32,
        operation: Operation = .reserved,
        reserved: [3]u8 = [_]u8{0} ** 3,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .prepare_ok);
            if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
            if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
            if (self.prepare_checksum_padding != 0) return "prepare_checksum_padding != 0";
            switch (self.operation) {
                .reserved => return "operation == .reserved",
                .root => {
                    const root_checksum = Header.Prepare.root(self.cluster).checksum;
                    if (self.parent != 0) return "root: parent != 0";
                    if (self.client != 0) return "root: client != 0";
                    if (self.prepare_checksum != root_checksum) {
                        return "root: prepare_checksum != expected";
                    }
                    if (self.request != 0) return "root: request != 0";
                    if (self.op != 0) return "root: op != 0";
                    if (self.commit != 0) return "root: commit != 0";
                    if (self.timestamp != 0) return "root: timestamp != 0";
                },
                else => {
                    if (self.client == 0) return "client == 0";
                    if (self.op == 0) return "op == 0";
                    if (self.op <= self.commit) return "op <= commit";
                    if (self.timestamp == 0) return "timestamp == 0";
                    if (self.operation == .register) {
                        if (self.request != 0) return "request != 0";
                    } else {
                        if (self.request == 0) return "request == 0";
                    }
                },
            }
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const Reply = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        /// The checksum of the corresponding Request.
        request_checksum: u128,
        request_checksum_padding: u128 = 0,
        /// The checksum of the prepare message to which this message refers.
        /// This allows for cryptographic guarantees beyond request, op, and commit numbers, which
        /// have low entropy and may otherwise collide in the event of any correctness bugs.
        context: u128 = 0,
        context_padding: u128 = 0,
        client: u128,
        op: u64,
        commit: u64,
        /// The corresponding `prepare`'s timestamp.
        /// This allows the test workload to verify transfer timeouts.
        timestamp: u64,
        request: u32,
        operation: Operation = .reserved,
        reserved: [19]u8 = [_]u8{0} ** 19,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .reply);
            // Initialization within `client.zig` asserts that client `id` is greater than zero:
            if (self.client == 0) return "client == 0";
            if (self.request_checksum_padding != 0) return "request_checksum_padding != 0";
            if (self.context_padding != 0) return "context_padding != 0";
            if (self.op != self.commit) return "op != commit";
            if (self.timestamp == 0) return "timestamp == 0";
            if (self.operation == .register) {
                // In this context, the commit number is the newly registered session number.
                // The `0` commit number is reserved for cluster initialization.
                if (self.commit == 0) return "commit == 0";
                if (self.request != 0) return "request != 0";
            } else {
                if (self.commit == 0) return "commit == 0";
                if (self.request == 0) return "request == 0";
            }
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const Commit = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        /// The latest committed prepare's checksum.
        commit_checksum: u128,
        commit_checksum_padding: u128 = 0,

        /// Current checkpoint id.
        checkpoint_id: u128,

        /// Current checkpoint op.
        checkpoint_op: u64,

        /// The latest committed prepare's op.
        commit: u64,

        timestamp_monotonic: u64,

        reserved: [56]u8 = [_]u8{0} ** 56,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .commit);
            if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
            if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
            if (self.commit < self.checkpoint_op) return "commit < checkpoint_op";
            if (self.timestamp_monotonic == 0) return "timestamp_monotonic == 0";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const StartViewChange = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        reserved: [128]u8 = [_]u8{0} ** 128,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .start_view_change);
            if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
            if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const DoViewChange = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        /// A bitset of "present" prepares. If a bit is set, then the corresponding header is not
        /// "blank", the replica has the prepare, and the prepare is not known to be faulty.
        present_bitset: u128,
        /// A bitset, with set bits indicating headers in the message body which it has definitely
        /// not prepared (i.e. "nack"). The corresponding header may be an actual prepare header, or
        /// it may be a "blank" header.
        nack_bitset: u128,
        op: u64,
        /// Set to `commit_min`, to indicate the sending replica's progress.
        /// The sending replica may continue to commit after sending the DVC.
        commit_min: u64,
        checkpoint_op: u64,
        log_view: u32,
        reserved: [68]u8 = [_]u8{0} ** 68,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .do_view_change);
            if ((self.size - @sizeOf(Header)) % @sizeOf(Header) != 0) {
                return "size multiple invalid";
            }
            if (self.op < self.commit_min) return "op < commit_min";
            if (self.commit_min < self.checkpoint_op) return "commit_min < checkpoint_op";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const StartView = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        /// Set to zero for a new view, and to a nonce from an RSV when responding to the RSV.
        nonce: u128,
        op: u64,
        /// Set to `commit_min`/`commit_max` (they are the same).
        commit: u64,
        /// The replica's `op_checkpoint`.
        checkpoint_op: u64,
        reserved: [88]u8 = [_]u8{0} ** 88,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .start_view);
            if (self.op < self.commit) return "op < commit_min";
            if (self.commit < self.checkpoint_op) return "commit_min < checkpoint_op";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const RequestStartView = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        nonce: u128,
        reserved: [112]u8 = [_]u8{0} ** 112,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .request_start_view);
            if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
            if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
            if (self.nonce == 0) return "nonce == 0";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const RequestHeaders = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32 = 0, // Always 0.
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        /// The minimum op requested (inclusive).
        op_min: u64,
        /// The maximum op requested (inclusive).
        op_max: u64,
        reserved: [112]u8 = [_]u8{0} ** 112,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .request_headers);
            if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
            if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
            if (self.view != 0) return "view == 0";
            if (self.op_min > self.op_max) return "op_min > op_max";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const RequestPrepare = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32 = 0, // Always 0.
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        prepare_checksum: u128,
        prepare_checksum_padding: u128 = 0,
        prepare_op: u64,
        reserved: [88]u8 = [_]u8{0} ** 88,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .request_prepare);
            if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
            if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
            if (self.prepare_checksum_padding != 0) return "prepare_checksum_padding != 0";
            if (self.view != 0) return "view == 0";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const RequestReply = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32 = 0, // Always 0.
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        reply_checksum: u128,
        reply_checksum_padding: u128 = 0,
        reply_client: u128,
        reply_op: u64,
        reserved: [72]u8 = [_]u8{0} ** 72,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .request_reply);
            if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
            if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
            if (self.reply_checksum_padding != 0) return "reply_checksum_padding != 0";
            if (self.view != 0) return "view == 0";
            if (self.reply_client == 0) return "reply_client == 0";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const Headers = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        reserved: [128]u8 = [_]u8{0} ** 128,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .headers);
            if (self.size == @sizeOf(Header)) return "size == @sizeOf(Header)";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const Eviction = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32,
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        client: u128,
        reserved: [112]u8 = [_]u8{0} ** 112,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .eviction);
            if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
            if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
            if (self.client == 0) return "client == 0";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const RequestBlocks = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32 = 0, // Always 0.
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        reserved: [128]u8 = [_]u8{0} ** 128,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .request_blocks);
            if (self.view != 0) return "view != 0";
            if (self.size == @sizeOf(Header)) return "size == @sizeOf(Header)";
            if ((self.size - @sizeOf(Header)) % @sizeOf(vsr.BlockRequest) != 0) {
                return "size multiple invalid";
            }
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const Block = extern struct {
        pub usingnamespace HeaderFunctions(@This());
        pub const metadata_size = 96;

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32 = 0, // Always 0.
        version: u16 = vsr.Version,
        command: Command,
        replica: u8 = 0, // Always 0.
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        // Schema is determined by `block_type`.
        metadata_bytes: [metadata_size]u8,

        // Fields shared by all block types:
        address: u64,
        snapshot: Snapshot,
        block_type: schema.BlockType,
        reserved_block: [15]u8 = [_]u8{0} ** 15,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .block);
            if (self.size > constants.block_size) return "size > block_size";
            if (self.size == @sizeOf(Header)) return "size = @sizeOf(Header)";
            if (self.view != 0) return "view != 0";
            if (self.replica != 0) return "replica != 0";
            if (self.address == 0) return "address == 0"; // address ≠ 0
            if (!self.block_type.valid()) return "block_type invalid";
            if (self.block_type == .reserved) return "block_type == .reserved";
            // TODO When manifest blocks include a snapshot, verify that snapshot≠0.
            return null;
        }
    };

    pub const RequestSyncCheckpoint = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32 = 0, // Always 0.
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        checkpoint_id: u128,
        checkpoint_op: u64,
        reserved: [104]u8 = [_]u8{0} ** 104,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .request_sync_checkpoint);
            if (self.size != @sizeOf(Header)) return "size != @sizeOf(Header)";
            if (self.checksum_body != checksum_body_empty) return "checksum_body != expected";
            if (self.view != 0) return "view != 0";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };

    pub const SyncCheckpoint = extern struct {
        pub usingnamespace HeaderFunctions(@This());

        checksum: u128 = 0,
        checksum_padding: u128 = 0,
        checksum_body: u128 = 0,
        checksum_body_padding: u128 = 0,
        nonce_reserved: u128 = 0,
        cluster: u128,
        size: u32 = @sizeOf(Header),
        epoch: u32 = 0,
        view: u32 = 0, // Always 0.
        version: u16 = vsr.Version,
        command: Command,
        replica: u8,
        reserved_frame: [16]u8 = [_]u8{0} ** 16,

        /// Strictly speaking, this is identical to `checksum_body`.
        /// It is included separately to mirror the RequestSyncCheckpoint header.
        checkpoint_id: u128,
        checkpoint_op: u64,
        reserved: [104]u8 = [_]u8{0} ** 104,

        fn invalid_header(self: *const @This()) ?[]const u8 {
            assert(self.command == .sync_checkpoint);
            if (self.size != @sizeOf(Header) + @sizeOf(vsr.CheckpointState)) {
                return "size != @sizeOf(Header) + @sizeOf(CheckpointState)";
            }
            if (self.view != 0) return "view != 0";
            if (self.checkpoint_id != self.checksum_body) return "checkpoint_id != checksum_body";
            if (!stdx.zeroed(&self.reserved)) return "reserved != 0";
            return null;
        }
    };
};

// Verify each Command's header type.
comptime {
    @setEvalBranchQuota(20000);

    for (std.enums.values(Command)) |command| {
        const CommandHeader = Header.Type(command);
        assert(@sizeOf(CommandHeader) == @sizeOf(Header));
        assert(@alignOf(CommandHeader) == @alignOf(Header));
        assert(@typeInfo(CommandHeader) == .Struct);
        assert(@typeInfo(CommandHeader).Struct.layout == .Extern);
        assert(stdx.no_padding(CommandHeader));

        // Verify that the command's header's frame is identical to Header's.
        for (std.meta.fields(Header)) |header_field| {
            if (std.mem.eql(u8, header_field.name, "reserved_command")) {
                assert(std.meta.fieldIndex(CommandHeader, header_field.name) == null);
            } else {
                const command_field_index = std.meta.fieldIndex(CommandHeader, header_field.name).?;
                const command_field = std.meta.fields(CommandHeader)[command_field_index];
                assert(command_field.type == header_field.type);
                assert(command_field.alignment == header_field.alignment);
                assert(@offsetOf(CommandHeader, command_field.name) ==
                    @offsetOf(Header, header_field.name));
            }
        }
    }
}
