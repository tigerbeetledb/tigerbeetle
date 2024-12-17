const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const global_constants = @import("../constants.zig");
const GrooveType = @import("../lsm/groove.zig").GrooveType;
const ForestType = @import("../lsm/forest.zig").ForestType;

pub fn StateMachineType(
    comptime Storage: type,
    comptime config: global_constants.StateMachineConfig,
) type {
    return struct {
        const StateMachine = @This();
        const Grid = @import("../vsr/grid.zig").GridType(Storage);

        pub const Workload = WorkloadType(StateMachine);

        pub const Operation = enum(u8) {
            echo = config.vsr_operations_reserved + 0,
        };

        pub fn operation_from_vsr(operation: vsr.Operation) ?Operation {
            if (operation.vsr_reserved()) return null;

            return vsr.Operation.to(StateMachine, operation);
        }

        pub const constants = struct {
            pub const message_body_size_max = config.message_body_size_max;
        };

        pub const batch_logical_allowed = std.enums.EnumArray(Operation, bool).init(.{
            // Batching not supported by test StateMachine.
            .echo = false,
        });

        pub fn EventType(comptime _: Operation) type {
            return u8; // Must be non-zero-sized for sliceAsBytes().
        }

        pub fn ResultType(comptime _: Operation) type {
            return u8; // Must be non-zero-sized for sliceAsBytes().
        }

        /// Empty demuxer to be compatible with vsr.Client batching.
        pub fn DemuxerType(comptime operation: Operation) type {
            return struct {
                const Demuxer = @This();

                reply: []ResultType(operation),
                offset: u32 = 0,

                pub fn init(reply: []u8) Demuxer {
                    return .{
                        .reply = @alignCast(std.mem.bytesAsSlice(
                            ResultType(operation),
                            reply,
                        )),
                    };
                }

                pub fn decode(self: *Demuxer, event_offset: u32, event_count: u32) []u8 {
                    assert(self.offset == event_offset);
                    assert(event_offset + event_count <= self.reply.len);
                    defer self.offset += event_count;
                    return std.mem.sliceAsBytes(self.reply[self.offset..][0..event_count]);
                }
            };
        }

        pub const Options = struct {
            batch_size_limit: u32,
            lsm_forest_node_count: u32,
        };

        pub const Forest = ForestType(Storage, .{ .things = ThingGroove });

        const ThingGroove = GrooveType(
            Storage,
            Thing,
            .{
                .ids = .{
                    .timestamp = 1,
                    .id = 2,
                    .value = 3,
                },
                .batch_value_count_max = .{
                    .timestamp = 1,
                    .id = 1,
                    .value = 1,
                },
                .ignored = &[_][]const u8{},
                .optional = &[_][]const u8{},
                .derived = .{},
                .orphaned_ids = false,
            },
        );

        const Thing = extern struct {
            timestamp: u64,
            value: u64,
            id: u128,
        };

        options: Options,
        forest: Forest,

        prefetch_timestamp: u64 = 0,
        prepare_timestamp: u64 = 0,
        commit_timestamp: u64 = 0,

        prefetch_context: ThingGroove.PrefetchContext = undefined,
        callback: ?*const fn (state_machine: *StateMachine) void = null,

        pub fn init(
            self: *StateMachine,
            allocator: std.mem.Allocator,
            grid: *Grid,
            options: Options,
        ) !void {
            self.* = .{
                .options = options,
                .forest = undefined,
            };

            const things_cache_entries_max =
                ThingGroove.ObjectsCache.Cache.value_count_max_multiple;

            try self.forest.init(
                allocator,
                grid,
                .{
                    .compaction_block_count = Forest.Options.compaction_block_count_min,
                    .node_count = options.lsm_forest_node_count,
                },
                .{
                    .things = .{
                        .cache_entries_max = things_cache_entries_max,
                        .prefetch_entries_for_read_max = 0,
                        .prefetch_entries_for_update_max = 1,
                        .tree_options_object = .{ .batch_value_count_limit = 1 },
                        .tree_options_id = .{ .batch_value_count_limit = 1 },
                        .tree_options_index = .{ .value = .{ .batch_value_count_limit = 1 } },
                    },
                },
            );
            errdefer self.forest.deinit(allocator);
        }

        pub fn deinit(state_machine: *StateMachine, allocator: std.mem.Allocator) void {
            state_machine.forest.deinit(allocator);
        }

        pub fn reset(state_machine: *StateMachine) void {
            state_machine.forest.reset();

            state_machine.* = .{
                .options = state_machine.options,
                .forest = state_machine.forest,
            };
        }

        pub fn open(state_machine: *StateMachine, callback: *const fn (*StateMachine) void) void {
            assert(state_machine.callback == null);

            state_machine.callback = callback;
            state_machine.forest.open(open_callback);
        }

        fn open_callback(forest: *Forest) void {
            const state_machine: *StateMachine = @fieldParentPtr("forest", forest);
            const callback = state_machine.callback.?;
            state_machine.callback = null;

            callback(state_machine);
        }

        pub fn pulse_needed(state_machine: *const StateMachine, timestamp: u64) bool {
            _ = state_machine;
            _ = timestamp;
            return false;
        }

        pub fn input_valid(
            state_machine: *const StateMachine,
            client_release: vsr.Release,
            operation: Operation,
            input: []const u8,
        ) bool {
            _ = state_machine;
            _ = client_release;
            _ = operation;
            _ = input;
            return true;
        }

        pub fn prepare(
            state_machine: *StateMachine,
            client_release: vsr.Release,
            operation: Operation,
            input: []const u8,
        ) void {
            _ = state_machine;
            _ = client_release;
            _ = operation;
            _ = input;
        }

        pub fn prefetch(
            state_machine: *StateMachine,
            callback: *const fn (*StateMachine) void,
            client_release: vsr.Release,
            op: u64,
            operation: Operation,
            input: []const u8,
        ) void {
            _ = client_release;
            _ = operation;
            _ = input;

            assert(state_machine.callback == null);
            state_machine.callback = callback;

            // TODO(Snapshots) Pass in the target snapshot.
            state_machine.forest.grooves.things.prefetch_setup(null);
            state_machine.forest.grooves.things.prefetch_enqueue(op);
            state_machine.forest.grooves.things.prefetch(
                prefetch_callback,
                &state_machine.prefetch_context,
            );
        }

        fn prefetch_callback(completion: *ThingGroove.PrefetchContext) void {
            const state_machine: *StateMachine = @fieldParentPtr("prefetch_context", completion);
            const callback = state_machine.callback.?;
            state_machine.callback = null;

            callback(state_machine);
        }

        pub fn commit(
            state_machine: *StateMachine,
            client: u128,
            client_release: vsr.Release,
            op: u64,
            timestamp: u64,
            operation: Operation,
            input: []const u8,
            output: []u8,
        ) usize {
            assert(op != 0);

            switch (operation) {
                .echo => {
                    assert(state_machine.forest.grooves.things.get(op) == .not_found);

                    var value = vsr.ChecksumStream.init();
                    value.add(std.mem.asBytes(&client));
                    value.add(std.mem.asBytes(&op));
                    value.add(std.mem.asBytes(&timestamp));
                    value.add(std.mem.asBytes(&operation));
                    value.add(std.mem.asBytes(&client_release));
                    value.add(input);

                    state_machine.forest.grooves.things.insert(&.{
                        .timestamp = timestamp,
                        .id = op,
                        .value = @as(u64, @truncate(value.checksum())),
                    });

                    stdx.copy_disjoint(.inexact, u8, output, input);
                    return input.len;
                },
            }
        }

        pub fn compact(
            state_machine: *StateMachine,
            callback: *const fn (*StateMachine) void,
            op: u64,
        ) void {
            assert(op != 0);
            assert(state_machine.callback == null);

            state_machine.callback = callback;
            state_machine.forest.compact(compact_callback, op);
        }

        fn compact_callback(forest: *Forest) void {
            const state_machine: *StateMachine = @fieldParentPtr("forest", forest);
            const callback = state_machine.callback.?;
            state_machine.callback = null;

            callback(state_machine);
        }

        pub fn checkpoint(
            state_machine: *StateMachine,
            callback: *const fn (*StateMachine) void,
        ) void {
            assert(state_machine.callback == null);

            state_machine.callback = callback;
            state_machine.forest.checkpoint(checkpoint_callback);
        }

        fn checkpoint_callback(forest: *Forest) void {
            const state_machine: *StateMachine = @fieldParentPtr("forest", forest);
            const callback = state_machine.callback.?;
            state_machine.callback = null;

            callback(state_machine);
        }
    };
}

fn WorkloadType(comptime StateMachine: type) type {
    return struct {
        const Workload = @This();
        const constants = StateMachine.constants;

        random: std.rand.Random,
        options: Options,
        requests_sent: usize = 0,
        requests_delivered: usize = 0,

        pub fn init(
            allocator: std.mem.Allocator,
            random: std.rand.Random,
            options: Options,
        ) !Workload {
            _ = allocator;

            return Workload{
                .random = random,
                .options = options,
            };
        }

        pub fn deinit(workload: *Workload, allocator: std.mem.Allocator) void {
            _ = workload;
            _ = allocator;
        }

        pub fn done(workload: *const Workload) bool {
            return workload.requests_sent == workload.requests_delivered;
        }

        pub fn build_request(
            workload: *Workload,
            client_index: usize,
            body: []align(@alignOf(vsr.Header)) u8,
        ) struct {
            operation: StateMachine.Operation,
            size: usize,
        } {
            _ = client_index;

            workload.requests_sent += 1;

            // +1 for inclusive limit.
            const size = workload.random.uintAtMost(usize, workload.options.batch_size_limit);
            workload.random.bytes(body[0..size]);

            return .{
                .operation = .echo,
                .size = size,
            };
        }

        pub fn on_reply(
            workload: *Workload,
            client_index: usize,
            operation: StateMachine.Operation,
            timestamp: u64,
            request_body: []align(@alignOf(vsr.Header)) const u8,
            reply_body: []align(@alignOf(vsr.Header)) const u8,
        ) void {
            _ = client_index;
            _ = timestamp;

            workload.requests_delivered += 1;
            assert(workload.requests_delivered <= workload.requests_sent);

            assert(operation == .echo);
            assert(std.mem.eql(u8, request_body, reply_body));
        }

        pub fn on_pulse(
            workload: *Workload,
            operation: StateMachine.Operation,
            timestamp: u64,
        ) void {
            _ = workload;
            _ = operation;
            _ = timestamp;

            // This state machine does not implement a pulse operation.
            unreachable;
        }

        pub const Options = struct {
            batch_size_limit: u32,

            pub fn generate(random: std.rand.Random, options: struct {
                batch_size_limit: u32,
                client_count: usize,
                in_flight_max: usize,
            }) Options {
                _ = random;

                return .{
                    .batch_size_limit = options.batch_size_limit,
                };
            }
        };
    };
}
