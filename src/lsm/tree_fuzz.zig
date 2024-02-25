const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const fuzz = @import("../testing/fuzz.zig");
const vsr = @import("../vsr.zig");
const schema = @import("schema.zig");
const binary_search = @import("binary_search.zig");
const allocator = fuzz.allocator;

const log = std.log.scoped(.lsm_tree_fuzz);
const tracer = @import("../tracer.zig");

const Direction = @import("../direction.zig").Direction;
const Transfer = @import("../tigerbeetle.zig").Transfer;
const Account = @import("../tigerbeetle.zig").Account;
const Storage = @import("../testing/storage.zig").Storage;
const ClusterFaultAtlas = @import("../testing/storage.zig").ClusterFaultAtlas;
const StateMachine = @import("../state_machine.zig").StateMachineType(Storage, constants.state_machine_config);
const GridType = @import("../vsr/grid.zig").GridType;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);
const TableUsage = @import("table.zig").TableUsage;
const TableType = @import("table.zig").TableType;
const ManifestLog = @import("manifest_log.zig").ManifestLogType(Storage);

const Grid = @import("../vsr/grid.zig").GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);
const ScanBuffer = @import("scan_buffer.zig").ScanBuffer;
const ScanTreeType = @import("scan_tree.zig").ScanTreeType;
const FreeSetEncoded = vsr.FreeSetEncodedType(Storage);
const Snapshot = @import("schema.zig").Snapshot;

const Value = packed struct(u128) {
    id: u64,
    value: u63,
    tombstone: u1 = 0,

    comptime {
        assert(@bitSizeOf(Value) == @sizeOf(Value) * 8);
    }

    inline fn key_from_value(value: *const Value) u64 {
        return value.id;
    }

    const sentinel_key = std.math.maxInt(u64);

    inline fn tombstone(value: *const Value) bool {
        return value.tombstone != 0;
    }

    inline fn tombstone_from_key(key: u64) Value {
        return Value{
            .id = key,
            .value = 0,
            .tombstone = 1,
        };
    }
};

const FuzzOpTag = std.meta.Tag(FuzzOp);
const FuzzOp = union(enum) {
    compact: struct {
        op: u64,
        checkpoint: bool,
    },
    put: Value,
    remove: Value,
    get: u64,
    scan: struct {
        min: u64,
        max: u64,
        direction: Direction,
    },
};

const batch_size_max = constants.message_size_max - @sizeOf(vsr.Header);
const commit_entries_max = @divFloor(batch_size_max, @sizeOf(Value));
const value_count_max = constants.lsm_batch_multiple * commit_entries_max;
const table_count_max = @import("tree.zig").table_count_max;

const cluster = 32;
const replica = 4;
const replica_count = 6;
const node_count = 1024;
const scan_results_max = 4096;
const tree_options = .{
    // This is the smallest size that set_associative_cache will allow us.
    .cache_entries_max = 2048,
};

// We must call compact after every 'batch'.
// Every `lsm_batch_multiple` batches may put/remove `value_count_max` values.
// Every `FuzzOp.put` issues one remove and one put.
const puts_since_compact_max = @divTrunc(commit_entries_max, 2);
const compacts_per_checkpoint = std.math.divCeil(
    usize,
    constants.journal_slot_count,
    constants.lsm_batch_multiple,
) catch unreachable;

fn EnvironmentType(comptime table_usage: TableUsage) type {
    return struct {
        const Environment = @This();

        const Tree = @import("tree.zig").TreeType(Table, Storage);
        const Table = TableType(
            u64,
            Value,
            Value.key_from_value,
            Value.sentinel_key,
            Value.tombstone,
            Value.tombstone_from_key,
            value_count_max,
            table_usage,
        );
        const ScanTree = ScanTreeType(*Environment, Tree, Storage);

        const State = enum {
            init,
            superblock_format,
            superblock_open,
            free_set_open,
            tree_init,
            manifest_log_open,
            fuzzing,
            tree_compact,
            manifest_log_compact,
            grid_checkpoint,
            superblock_checkpoint,
            tree_lookup,
            scan_tree,
        };

        state: State,
        storage: *Storage,
        superblock: SuperBlock,
        superblock_context: SuperBlock.Context,
        grid: Grid,
        manifest_log: ManifestLog,
        node_pool: NodePool,
        tree: Tree,
        scan_tree: ScanTree,
        lookup_context: Tree.LookupContext,
        lookup_value: ?Value,
        scan_buffer: ScanBuffer,
        scan_results: []Value,
        scan_results_count: u32,

        pub fn run(storage: *Storage, fuzz_ops: []const FuzzOp) !void {
            var env: Environment = undefined;
            env.state = .init;
            env.storage = storage;

            env.superblock = try SuperBlock.init(allocator, .{
                .storage = env.storage,
                .storage_size_limit = constants.storage_size_limit_max,
            });
            defer env.superblock.deinit(allocator);

            env.grid = try Grid.init(allocator, .{
                .superblock = &env.superblock,
                .missing_blocks_max = 0,
                .missing_tables_max = 0,
            });
            defer env.grid.deinit(allocator);

            env.manifest_log = try ManifestLog.init(allocator, &env.grid, .{
                .tree_id_min = 1,
                .tree_id_max = 1,
                .forest_table_count_max = table_count_max,
            });
            defer env.manifest_log.deinit(allocator);

            env.node_pool = try NodePool.init(allocator, node_count);
            defer env.node_pool.deinit(allocator);

            env.tree = undefined;
            env.lookup_value = null;

            env.scan_buffer = try ScanBuffer.init(allocator);
            defer env.scan_buffer.deinit(allocator);

            env.scan_results = try allocator.alloc(Value, scan_results_max);
            env.scan_results_count = 0;
            defer allocator.free(env.scan_results);

            try env.open_then_apply(fuzz_ops);
        }

        fn change_state(env: *Environment, current_state: State, next_state: State) void {
            assert(env.state == current_state);
            env.state = next_state;
        }

        fn tick_until_state_change(env: *Environment, current_state: State, next_state: State) void {
            // Sometimes operations complete synchronously so we might already be in next_state before ticking.
            while (env.state == current_state) env.storage.tick();
            assert(env.state == next_state);
        }

        pub fn open_then_apply(env: *Environment, fuzz_ops: []const FuzzOp) !void {
            env.change_state(.init, .superblock_format);
            env.superblock.format(superblock_format_callback, &env.superblock_context, .{
                .cluster = cluster,
                .replica = replica,
                .replica_count = replica_count,
            });

            env.tick_until_state_change(.superblock_format, .superblock_open);
            env.superblock.open(superblock_open_callback, &env.superblock_context);

            env.tick_until_state_change(.superblock_open, .free_set_open);
            env.grid.open(grid_open_callback);

            env.tick_until_state_change(.free_set_open, .tree_init);
            env.tree = try Tree.init(allocator, &env.node_pool, &env.grid, .{
                .id = 1,
                .name = "Key.Value",
            }, .{});
            defer env.tree.deinit(allocator);

            env.change_state(.tree_init, .manifest_log_open);

            env.tree.open_commence(&env.manifest_log);
            env.manifest_log.open(manifest_log_open_event, manifest_log_open_callback);

            env.tick_until_state_change(.manifest_log_open, .fuzzing);
            env.tree.open_complete();

            try env.apply(fuzz_ops);
        }

        fn superblock_format_callback(superblock_context: *SuperBlock.Context) void {
            const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
            env.change_state(.superblock_format, .superblock_open);
        }

        fn superblock_open_callback(superblock_context: *SuperBlock.Context) void {
            const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
            env.change_state(.superblock_open, .free_set_open);
        }

        fn grid_open_callback(grid: *Grid) void {
            const env = @fieldParentPtr(Environment, "grid", grid);
            env.change_state(.free_set_open, .tree_init);
        }

        fn manifest_log_open_event(
            manifest_log: *ManifestLog,
            table: *const schema.ManifestNode.TableInfo,
        ) void {
            _ = manifest_log;
            _ = table;

            // This ManifestLog is only opened during setup, when it has no blocks.
            unreachable;
        }

        fn manifest_log_open_callback(manifest_log: *ManifestLog) void {
            const env = @fieldParentPtr(@This(), "manifest_log", manifest_log);
            env.change_state(.manifest_log_open, .fuzzing);
        }

        pub fn compact(env: *Environment, op: u64) void {
            env.change_state(.fuzzing, .tree_compact);
            env.tree.compact(tree_compact_callback, op);
            env.tick_until_state_change(.tree_compact, .fuzzing);

            if (op % @divExact(constants.lsm_batch_multiple, 2) == 0 and
                op >= constants.lsm_batch_multiple)
            {
                env.change_state(.fuzzing, .manifest_log_compact);
                env.manifest_log.compact(manifest_log_compact_callback, op);
                env.tick_until_state_change(.manifest_log_compact, .fuzzing);
            }

            env.tree.compact_end();

            if ((op + 1) % @divExact(constants.lsm_batch_multiple, 2) == 0 and
                op >= constants.lsm_batch_multiple)
            {
                env.manifest_log.compact_end();
            }
        }

        fn manifest_log_compact_callback(manifest_log: *ManifestLog) void {
            const env = @fieldParentPtr(@This(), "manifest_log", manifest_log);
            env.change_state(.manifest_log_compact, .fuzzing);
        }

        fn tree_compact_callback(tree: *Tree) void {
            const env = @fieldParentPtr(@This(), "tree", tree);
            env.change_state(.tree_compact, .fuzzing);
        }

        pub fn checkpoint(env: *Environment, op: u64) void {
            env.tree.assert_between_bars();

            env.grid.checkpoint(grid_checkpoint_callback);
            env.change_state(.fuzzing, .grid_checkpoint);
            env.tick_until_state_change(.grid_checkpoint, .fuzzing);

            const checkpoint_op = op - constants.lsm_batch_multiple;
            env.superblock.checkpoint(superblock_checkpoint_callback, &env.superblock_context, .{
                .manifest_references = std.mem.zeroes(vsr.SuperBlockManifestReferences),
                .free_set_reference = env.grid.free_set_checkpoint.checkpoint_reference(),
                .client_sessions_reference = .{
                    .last_block_checksum = 0,
                    .last_block_address = 0,
                    .trailer_size = 0,
                    .checksum = vsr.checksum(&.{}),
                },
                .commit_min_checksum = env.superblock.working.vsr_state.checkpoint.commit_min_checksum + 1,
                .commit_min = checkpoint_op,
                .commit_max = checkpoint_op + 1,
                .sync_op_min = 0,
                .sync_op_max = 0,
                .storage_size = vsr.superblock.data_file_size_min +
                    (env.grid.free_set.highest_address_acquired() orelse 0) * constants.block_size,
            });

            env.change_state(.fuzzing, .superblock_checkpoint);
            env.tick_until_state_change(.superblock_checkpoint, .fuzzing);
        }

        fn grid_checkpoint_callback(grid: *Grid) void {
            const env = @fieldParentPtr(Environment, "grid", grid);
            env.change_state(.grid_checkpoint, .fuzzing);
        }

        fn superblock_checkpoint_callback(superblock_context: *SuperBlock.Context) void {
            const env = @fieldParentPtr(@This(), "superblock_context", superblock_context);
            env.change_state(.superblock_checkpoint, .fuzzing);
        }

        pub fn get(env: *Environment, key: u64) ?Value {
            env.change_state(.fuzzing, .tree_lookup);
            env.lookup_value = null;

            if (env.tree.lookup_from_memory(key)) |value| {
                get_callback(&env.lookup_context, Tree.unwrap_tombstone(value));
            } else {
                env.tree.lookup_from_levels_storage(.{
                    .callback = get_callback,
                    .context = &env.lookup_context,
                    .snapshot = Snapshot.latest,
                    .key = key,
                    .level_min = 0,
                });
            }

            env.tick_until_state_change(.tree_lookup, .fuzzing);
            return env.lookup_value;
        }

        fn get_callback(lookup_context: *Tree.LookupContext, value: ?*const Value) void {
            const env = @fieldParentPtr(Environment, "lookup_context", lookup_context);
            assert(env.lookup_value == null);
            env.lookup_value = if (value) |val| val.* else null;
            env.change_state(.tree_lookup, .fuzzing);
        }

        pub fn scan(env: *Environment, key_min: u64, key_max: u64, direction: Direction) []Value {
            assert(key_min <= key_max);

            env.change_state(.fuzzing, .scan_tree);
            env.scan_tree = ScanTree.init(
                &env.tree,
                &env.scan_buffer,
                Snapshot.latest,
                key_min,
                key_max,
                direction,
            );

            env.scan_results_count = 0;
            env.scan_tree.read(env, on_scan_read);
            env.tick_until_state_change(.scan_tree, .fuzzing);

            return env.scan_results[0..env.scan_results_count];
        }

        fn on_scan_read(env: *Environment, scan_tree: *ScanTree) void {
            while (scan_tree.next() catch |err| switch (err) {
                error.ReadAgain => return scan_tree.read(env, on_scan_read),
            }) |value| {
                if (env.scan_results_count == scan_results_max) break;

                env.scan_results[env.scan_results_count] = value;
                env.scan_results_count += 1;
            }

            env.change_state(.scan_tree, .fuzzing);
        }

        pub fn apply(env: *Environment, fuzz_ops: []const FuzzOp) !void {
            // The tree should behave like a simple key-value data-structure.
            // We'll compare it to a hash map.
            var model = std.hash_map.AutoHashMap(u64, Value).init(allocator);
            defer model.deinit();

            for (fuzz_ops, 0..) |fuzz_op, fuzz_op_index| {
                assert(env.state == .fuzzing);
                log.debug("Running fuzz_ops[{}/{}] == {}", .{ fuzz_op_index, fuzz_ops.len, fuzz_op });

                const storage_size_used = env.storage.size_used();
                log.debug("storage.size_used = {}/{}", .{ storage_size_used, env.storage.size });

                const model_size = model.count() * @sizeOf(Value);
                log.debug("space_amplification = {d:.2}", .{
                    @as(f64, @floatFromInt(storage_size_used)) / @as(f64, @floatFromInt(model_size)),
                });

                // Apply fuzz_op to the tree and the model.
                switch (fuzz_op) {
                    .compact => |c| {
                        env.compact(c.op);
                        if (c.checkpoint) env.checkpoint(c.op);
                    },
                    .put => |value| {
                        if (table_usage == .secondary_index) {
                            if (model.get(Value.key_from_value(&value))) |old_value| {
                                // Not allowed to put a present key without removing the old value first.
                                env.tree.remove(&old_value);
                            }
                        }
                        env.tree.put(&value);
                        try model.put(Value.key_from_value(&value), value);
                    },
                    .remove => |value| {
                        if (table_usage == .secondary_index and !model.contains((Value.key_from_value(&value)))) {
                            // Not allowed to remove non-present keys
                        } else {
                            env.tree.remove(&value);
                        }
                        _ = model.remove(Value.key_from_value(&value));
                    },
                    .get => |key| {
                        // Get account from lsm.
                        const tree_value = env.get(key);

                        // Compare result to model.
                        const model_value = model.get(key);
                        if (model_value == null) {
                            assert(tree_value == null);
                        } else {
                            switch (table_usage) {
                                .general => {
                                    assert(std.mem.eql(
                                        u8,
                                        std.mem.asBytes(&model_value.?),
                                        std.mem.asBytes(&tree_value.?),
                                    ));
                                },
                                .secondary_index => {
                                    // secondary_index only preserves keys - may return old values
                                    assert(std.mem.eql(
                                        u8,
                                        std.mem.asBytes(&Value.key_from_value(&model_value.?)),
                                        std.mem.asBytes(&Value.key_from_value(&tree_value.?)),
                                    ));
                                },
                            }
                        }
                    },
                    .scan => |scan_range| {
                        assert(scan_range.min <= scan_range.max);

                        const tree_values = env.scan(
                            scan_range.min,
                            scan_range.max,
                            scan_range.direction,
                        );

                        // Asserting the positive space:
                        // all keys found by the scan must exist in our model.
                        var tree_value_last: ?Value = null;
                        for (tree_values) |tree_value| {

                            // Asserting boundaries.
                            assert(scan_range.min <= Table.key_from_value(&tree_value));
                            assert(Table.key_from_value(&tree_value) <= scan_range.max);

                            // Asserting direction.
                            if (tree_value_last) |value_last| {
                                switch (scan_range.direction) {
                                    .ascending => assert(Table.key_from_value(&tree_value) >
                                        Table.key_from_value(&value_last)),
                                    .descending => assert(Table.key_from_value(&tree_value) <
                                        Table.key_from_value(&value_last)),
                                }
                            }
                            tree_value_last = tree_value;

                            // Compare result to model.
                            if (model.get(Table.key_from_value(&tree_value))) |model_value| {
                                switch (table_usage) {
                                    .general => {
                                        assert(std.mem.eql(
                                            u8,
                                            std.mem.asBytes(&model_value),
                                            std.mem.asBytes(&tree_value),
                                        ));
                                    },
                                    .secondary_index => {
                                        // secondary_index only preserves keys - may return old values
                                        assert(Table.key_from_value(&model_value) ==
                                            Table.key_from_value(&tree_value));
                                    },
                                }
                            } else {
                                assert(Table.tombstone(&tree_value));
                            }
                        }

                        // Asserting the negative space:
                        // All keys existing in our model must be checked against the scan range.
                        if (scan_range.direction == .descending) std.mem.sort(
                            Value,
                            tree_values,
                            {},
                            struct {
                                fn sort(_: void, a: Value, b: Value) bool {
                                    return Table.key_from_value(&a) <
                                        Table.key_from_value(&b);
                                }
                            }.sort,
                        );

                        var it = model.iterator();
                        while (it.next()) |entry| {
                            const model_value_key = Value.key_from_value(entry.value_ptr);
                            const value_maybe = binary_search.binary_search_values(
                                u64,
                                Value,
                                Table.key_from_value,
                                tree_values,
                                model_value_key,
                                .{},
                            );

                            if (model_value_key >= scan_range.min and
                                model_value_key <= scan_range.max)
                            {
                                // Must be found:
                                if (value_maybe == null) {
                                    // Or our buffer has exceeded, in this case the key should
                                    // be less than the first element or greater than the last element,
                                    // depending on the scan direction.
                                    assert(tree_values.len == scan_results_max);
                                    switch (scan_range.direction) {
                                        .ascending => assert(
                                            Table.key_from_value(&tree_values[tree_values.len - 1]) <
                                                model_value_key,
                                        ),
                                        .descending => assert(
                                            model_value_key <
                                                Table.key_from_value(&tree_values[0]),
                                        ),
                                    }
                                }
                            } else {
                                // Must not be found:
                                if (value_maybe) |value| {
                                    // Or it's a tombstone.
                                    assert(Table.tombstone(value));
                                }
                            }
                        }
                    },
                }
            }
        }
    };
}

fn random_id(random: std.rand.Random, comptime Int: type) Int {
    // We have two opposing desires for random ids:
    const avg_int: Int = if (random.boolean())
        // 1. We want to cause many collisions.
        constants.lsm_growth_factor * 2048
    else
        // 2. We want to generate enough ids that the cache can't hold them all.
        100 * constants.lsm_growth_factor * 2048;
    return fuzz.random_int_exponential(random, Int, avg_int);
}

pub fn generate_fuzz_ops(random: std.rand.Random, fuzz_op_count: usize) ![]const FuzzOp {
    log.info("fuzz_op_count = {}", .{fuzz_op_count});

    const fuzz_ops = try allocator.alloc(FuzzOp, fuzz_op_count);
    errdefer allocator.free(fuzz_ops);

    var fuzz_op_distribution = fuzz.Distribution(FuzzOpTag){
        // Maybe compact more often than forced to by `puts_since_compact`.
        .compact = if (random.boolean()) 0 else 1,
        // Always do puts, and always more puts than removes.
        .put = constants.lsm_batch_multiple * 2,
        // Maybe do some removes.
        .remove = if (random.boolean()) 0 else constants.lsm_batch_multiple,
        // Maybe do some gets.
        .get = if (random.boolean()) 0 else constants.lsm_batch_multiple,
        // Maybe do some scans.
        .scan = if (random.boolean()) 0 else constants.lsm_batch_multiple,
    };
    log.info("fuzz_op_distribution = {:.2}", .{fuzz_op_distribution});

    log.info("puts_since_compact_max = {}", .{puts_since_compact_max});
    log.info("compacts_per_checkpoint = {}", .{compacts_per_checkpoint});

    var op: u64 = 1;
    var puts_since_compact: usize = 0;
    for (fuzz_ops) |*fuzz_op| {
        const fuzz_op_tag = if (puts_since_compact >= puts_since_compact_max)
            // We have to compact before doing any other operations.
            FuzzOpTag.compact
        else
            // Otherwise pick a random FuzzOp.
            fuzz.random_enum(random, FuzzOpTag, fuzz_op_distribution);
        fuzz_op.* = switch (fuzz_op_tag) {
            .compact => compact: {
                const compact_op = op;
                op += 1;
                const is_checkpoint =
                    // Can only checkpoint on the last beat of the bar.
                    compact_op % constants.lsm_batch_multiple == constants.lsm_batch_multiple - 1 and
                    compact_op > constants.lsm_batch_multiple and
                    // Checkpoint at roughly the same rate as log wraparound.
                    random.uintLessThan(usize, compacts_per_checkpoint) == 0;
                break :compact FuzzOp{
                    .compact = .{
                        .op = compact_op,
                        .checkpoint = is_checkpoint,
                    },
                };
            },
            .put => FuzzOp{ .put = .{
                .id = random_id(random, u64),
                .value = random.int(u63),
            } },
            .remove => FuzzOp{ .remove = .{
                .id = random_id(random, u64),
                .value = random.int(u63),
            } },
            .get => FuzzOp{ .get = random_id(random, u64) },
            .scan => blk: {
                const min = random_id(random, u64);
                const max = min + random_id(random, u64);
                const direction = random.enumValue(Direction);
                assert(min <= max);
                break :blk FuzzOp{
                    .scan = .{
                        .min = min,
                        .max = max,
                        .direction = direction,
                    },
                };
            },
        };
        switch (fuzz_op.*) {
            .compact => puts_since_compact = 0,
            // Tree.remove() works by inserting a tombstone, so it counts as a put.
            .put, .remove => puts_since_compact += 1,
            .get, .scan => {},
        }
    }

    return fuzz_ops;
}

pub fn main(fuzz_args: fuzz.FuzzArgs) !void {
    try tracer.init(allocator);
    defer tracer.deinit(allocator);

    var rng = std.rand.DefaultPrng.init(fuzz_args.seed);
    const random = rng.random();

    const table_usage = random.enumValue(TableUsage);
    log.info("table_usage={}", .{table_usage});

    const storage_fault_atlas = ClusterFaultAtlas.init(3, random, .{
        .faulty_superblock = false,
        .faulty_wal_headers = false,
        .faulty_wal_prepares = false,
        .faulty_client_replies = false,
        .faulty_grid = true,
    });

    const storage_options = .{
        .seed = random.int(u64),
        .replica_index = 0,
        .read_latency_min = 0,
        .read_latency_mean = 0 + fuzz.random_int_exponential(random, u64, 20),
        .write_latency_min = 0,
        .write_latency_mean = 0 + fuzz.random_int_exponential(random, u64, 20),
        .read_fault_probability = 0,
        .write_fault_probability = 0,
        .fault_atlas = &storage_fault_atlas,
    };

    const fuzz_op_count = @min(
        fuzz_args.events_max orelse @as(usize, 1E7),
        fuzz.random_int_exponential(random, usize, 1E6),
    );

    const fuzz_ops = try generate_fuzz_ops(random, fuzz_op_count);
    defer allocator.free(fuzz_ops);

    // Init mocked storage.
    var storage = try Storage.init(allocator, constants.storage_size_limit_max, storage_options);
    defer storage.deinit(allocator);

    switch (table_usage) {
        inline else => |usage| {
            try EnvironmentType(usage).run(&storage, fuzz_ops);
        },
    }

    log.info("Passed!", .{});
}
