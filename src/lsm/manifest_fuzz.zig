//! Fuzz Manifest. Most public methods are covered.
//! `compaction_must_drop_tombstones` is not called as the fuzzer deals with Key ranges, not Values.
//! `verify` is not called as it checks Storage for valid Values in data blocks (see above).
//!
//! Strategy:
//!
//! Simulates operations on a Tree using only Keys instead of Values. Keys are inserted either in
//! random order or ascending order, matching Id & Object or Derived Trees respectively. Key ranges
//! are compacted/moved down levels for each op, simulating Compaction. This covers most of the
//! Manifest while occasional lookup of inserted Keys covers the rest.
//!
//! Invariants:
//!
//! - Inserted keys can have their range (not the exact key) retried from a following lookup.
//! - Visible ManifestLevel tables are properly bounded after compaction between them completes.
//! - The ingress and egress of tables between ManifestLevels are matched to avoid overflows.
//!

const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.lsm_manifest_fuzz);

const vsr = @import("../vsr.zig");
const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");
const fuzz = @import("../testing/fuzz.zig");
const allocator = fuzz.allocator;

const snapshot_latest = @import("tree.zig").snapshot_latest;
const compaction_tables_input_max = @import("tree.zig").compaction_tables_input_max;

const Key = u64;
const Value = extern struct { key: Key, is_tombstone: u64 };

inline fn compare_keys(a: Key, b: Key) std.math.Order {
    return std.math.order(a, b);
}

inline fn key_from_value(value: *const Value) Key {
    return value.key;
}

inline fn tombstone(value: *const Value) bool {
    return value.is_tombstone != 0;
}

inline fn tombstone_from_key(key: Key) Value {
    return .{ .key = key, .is_tombstone = 1 };
}

const Table = @import("table.zig").TableType(
    Key,
    Value,
    compare_keys,
    key_from_value,
    std.math.maxInt(Key),
    tombstone,
    tombstone_from_key,
    constants.state_machine_config.lsm_batch_multiple * 1024,
    .secondary_index,
);

const ClusterFaultAtlas = @import("../testing/storage.zig").ClusterFaultAtlas;
const Storage = @import("../testing/storage.zig").Storage;
const Grid = @import("../vsr/grid.zig").GridType(Storage);
const SuperBlock = vsr.SuperBlockType(Storage);
const TableInfo = @import("manifest.zig").TableInfoType(Table);
const Manifest = @import("manifest.zig").ManifestType(Table, Storage);
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);

pub const tigerbeetle_config = @import("../config.zig").configs.test_min;

const FuzzOpTag = std.meta.Tag(FuzzOp);
const FuzzOp = union(enum) {
    create: Key,
    lookup: Key,
};

fn generate_fuzz_ops(random: std.rand.Random, fuzz_op_count: usize) ![]const FuzzOp {
    log.info("fuzz_op_count = {}", .{fuzz_op_count});

    const fuzz_ops = try allocator.alloc(FuzzOp, fuzz_op_count);
    errdefer allocator.free(fuzz_ops);

    var fuzz_op_distribution = fuzz.Distribution(FuzzOpTag){
        .create = 8,
        .lookup = 2,
    };
    log.info("fuzz_op_distribution = {:.2}", .{fuzz_op_distribution});

    const KeyGen = enum { ascending, randomized };
    const key_gen = random.enumValue(KeyGen);
    log.info("key_generation = {}", .{key_gen});

    const CreatedContext = struct {
        pub inline fn eql(_: @This(), a: Key, b: Key, b_index: usize) bool {
            _ = b_index;
            return Table.compare_keys(a, b) == .eq;
        }

        pub inline fn hash(_: @This(), key: Key) u32 {
            return @truncate(stdx.hash_inline(key));
        }
    };

    // Set of keys already created.
    var created = std.ArrayHashMap(Key, void, CreatedContext, true).init(allocator);
    try created.ensureTotalCapacity(fuzz_op_count);
    defer created.deinit();

    for (fuzz_ops, 0..) |*fuzz_op, i| {
        var tag = fuzz.random_enum(random, FuzzOpTag, fuzz_op_distribution);
        while (true) {
            fuzz_op.* = switch (tag) {
                .create => blk: {
                    // Limit the amount of attempts at creating non-existing keys.
                    var attempts = fuzz_op_count;
                    while (attempts > 0) : (attempts -= 1) {
                        const key = switch (key_gen) {
                            .ascending => @as(u64, i),
                            .randomized => random.int(u64),
                        };

                        // Skip already created keys.
                        const result = created.getOrPutAssumeCapacity(key);
                        if (result.found_existing) continue;

                        result.key_ptr.* = key;
                        break :blk FuzzOp{ .create = key };
                    }
                    @panic("failed to create a new/random key to be inserted");
                },
                .lookup => blk: {
                    // Make sure values have been made to lookup. If not, create some.
                    const keys = created.keys();
                    if (keys.len == 0) {
                        tag = .create;
                        continue;
                    }

                    const key = keys[random.uintLessThanBiased(usize, keys.len)];
                    break :blk FuzzOp{ .lookup = key };
                },
            };
            break;
        }
    }

    return fuzz_ops;
}

pub fn main() !void {
    const args = try fuzz.parse_fuzz_args(allocator);

    var prng = std.rand.DefaultPrng.init(args.seed);
    const random = prng.random();

    const fuzz_op_count: usize = @min(
        args.events_max orelse @as(usize, 2e5),
        fuzz.random_int_exponential(random, usize, 1e5),
    );

    const fuzz_ops = try generate_fuzz_ops(random, fuzz_op_count);
    defer allocator.free(fuzz_ops);

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
        .read_fault_probability = random.uintLessThan(u8, 100),
        .write_fault_probability = random.uintLessThan(u8, 100),
        .fault_atlas = &storage_fault_atlas,
    };

    var storage = try Storage.init(allocator, constants.storage_size_max, storage_options);
    defer storage.deinit(allocator);

    var superblock = try SuperBlock.init(allocator, .{
        .storage = &storage,
        .storage_size_limit = constants.storage_size_max,
    });
    defer superblock.deinit(allocator);

    const SuperBlockFormat = struct {
        var formatted = false;
        fn callback(_: *SuperBlock.Context) void {
            assert(!@This().formatted);
            @This().formatted = true;
        }
    };

    var superblock_context: SuperBlock.Context = undefined;
    superblock.format(SuperBlockFormat.callback, &superblock_context, .{
        .cluster = 0,
        .replica = 0,
        .replica_count = 1,
    });
    while (!SuperBlockFormat.formatted) storage.tick();

    const SuperBlockOpen = struct {
        var opened = false;
        fn callback(_: *SuperBlock.Context) void {
            assert(!@This().opened);
            @This().opened = true;
        }
    };

    superblock.open(SuperBlockOpen.callback, &superblock_context);
    while (!SuperBlockOpen.opened) storage.tick();

    var grid = try Grid.init(allocator, .{ .superblock = &superblock });
    defer grid.deinit(allocator);

    const node_count = 16;
    var node_pool = try NodePool.init(allocator, node_count);
    errdefer node_pool.deinit(allocator);

    const tree_name = "ManifestFuzzTree";
    const tree_hash = blk: {
        var hash: u256 = undefined;
        std.crypto.hash.Blake3.hash(tree_name, std.mem.asBytes(&hash), .{});
        break :blk @as(u128, @truncate(hash));
    };

    var manifest = try Manifest.init(allocator, &node_pool, &grid, tree_hash);
    defer manifest.deinit(allocator);

    const ManifestOpen = struct {
        var opened = false;
        fn callback(_: *Manifest) void {
            assert(!@This().opened);
            @This().opened = true;
        }
    };

    manifest.open(ManifestOpen.callback);
    while (!ManifestOpen.opened) storage.tick();

    // Mock TreeType's TableMutable/TableImmutable using key ranges.
    const KeyRange = struct { min: Key, max: Key };
    var keys_mutable: ?KeyRange = null;
    var keys_immutable: ?KeyRange = null;
    var keys_immutable_snapshot: u64 = undefined;

    // Mock CompactionType using key ranges.
    const Compaction = struct {
        const updates_max = constants.lsm_growth_factor + 1;

        state: enum { idle, running, done },
        level_b: u8,
        range_b: Manifest.CompactionRange,
        op_min: u64,
        move_table: bool,
        split_table: u32,
        info_a: union(enum) { immutable: KeyRange, level: Manifest.TableInfoReference },
        updates: stdx.BoundedArray(struct {
            operation: enum { insert_to_level_b, move_to_level_b },
            table: TableInfo,
        }, updates_max) = .{ .buffer = undefined },

        fn snapshot_max_for_table_input(compaction: *const @This()) u64 {
            const op_min = compaction.op_min;
            assert(op_min % @divExact(constants.lsm_batch_multiple, 2) == 0);
            return op_min + @divExact(constants.lsm_batch_multiple, 2) - 1;
        }

        fn snapshot_min_for_table_output(compaction: *const @This()) u64 {
            const op_min = compaction.op_min;
            assert(op_min % @divExact(constants.lsm_batch_multiple, 2) == 0);
            return op_min + @divExact(constants.lsm_batch_multiple, 2);
        }

        fn drive(compaction: *@This(), random_: std.rand.Random) void {
            assert(compaction.state == .running);

            if (compaction.move_table) {
                const snapshot_max = compaction.snapshot_max_for_table_input();
                const table_a = compaction.info_a.level.table_info;
                assert(table_a.snapshot_max >= snapshot_max);

                compaction.updates.appendAssumeCapacity(.{
                    .operation = .move_to_level_b,
                    .table = table_a.*,
                });

                compaction.state = .done;
                return;
            }

            const range_b = compaction.range_b;
            const range_a = switch (compaction.info_a) {
                .immutable => |key_range| key_range,
                .level => |table_info_ref| KeyRange{
                    .min = table_info_ref.table_info.key_min,
                    .max = table_info_ref.table_info.key_max,
                },
            };

            // Union key range of A and B.
            var range = range_a;
            if (compare_keys(range_b.key_min, range.min) == .lt) range.min = range_b.key_min;
            if (compare_keys(range_b.key_max, range.max) == .gt) range.max = range_b.key_max;

            // Decide how to split up the unioned range using split_table.
            // split_table = 1 for info_a.immutable as TableImmutable only has one table.
            // split_table = rand(0..Compaction.updates.len) to simulate creating many tables.
            var split = @max(1, compaction.split_table);
            const step = (range.max - range.min) / split;

            while (split > 0) : (split -= 1) {
                const key_min = range.min;
                range.min += step;
                const key_max = range.min;

                compaction.updates.appendAssumeCapacity(.{
                    .operation = .insert_to_level_b,
                    .table = .{
                        .checksum = random_.int(u128),
                        .address = random_.int(u64),
                        .snapshot_min = compaction.snapshot_min_for_table_output(),
                        .key_min = key_min,
                        .key_max = key_max,
                    },
                });
            }

            compaction.state = .done;
        }

        fn apply_updates(compaction: *@This(), manifest_: *Manifest) void {
            const snapshot_max = compaction.snapshot_max_for_table_input();
            const level_b = compaction.level_b;

            if (!compaction.move_table) {
                switch (compaction.info_a) {
                    .immutable => {},
                    .level => |table_info_ref| {
                        manifest_.update_table(level_b - 1, snapshot_max, table_info_ref);
                    },
                }
                for (compaction.range_b.tables.slice()) |table_info_ref| {
                    manifest_.update_table(level_b, snapshot_max, table_info_ref);
                }
            }

            for (compaction.updates.slice()) |*entry| {
                switch (entry.operation) {
                    .insert_to_level_b => {
                        manifest_.insert_table(
                            level_b,
                            &entry.table,
                        );
                    },
                    .move_to_level_b => {
                        manifest_.move_table(
                            level_b - 1,
                            level_b,
                            &entry.table,
                        );
                    },
                }
            }
        }

        fn reset(compaction: *@This()) void {
            assert(compaction.state == .done);
            compaction.state = .idle;
            compaction.updates.len = 0;
        }
    };

    var compaction_immutable: Compaction = undefined;
    compaction_immutable.state = .idle;

    var compactions: [@divFloor(constants.lsm_levels, 2)]Compaction = undefined;
    for (&compactions) |*compaction| compaction.state = .idle;

    var op: u64 = 0; //constants.lsm_batch_multiple;
    for (fuzz_ops, 0..) |fuzz_op, fuzz_op_index| {
        const lookup_snapshot_max = op + 1;
        defer op += 1;

        log.debug("Running fuzz_ops[{}/{}] == {}", .{ fuzz_op_index, fuzz_ops.len, fuzz_op });
        storage.tick();

        switch (fuzz_op) {
            .create => |key| {
                // Add to mutable table range.
                if (keys_mutable) |*range| {
                    if (compare_keys(key, range.min) == .lt) range.min = key;
                    if (compare_keys(key, range.max) == .gt) range.max = key;
                } else {
                    keys_mutable = KeyRange{ .min = key, .max = key };
                }
            },
            .lookup => |key| {
                var found: u1 = 0;
                const snapshot = lookup_snapshot_max;

                // Check "table mutable".
                if (keys_mutable) |range| {
                    found |= @intFromBool(compare_keys(key, range.min) != .lt and
                        compare_keys(key, range.max) != .gt);
                }

                // Check "table immutable".
                if (keys_immutable) |range| {
                    if (keys_immutable_snapshot <= snapshot) {
                        found |= @intFromBool(compare_keys(key, range.min) != .lt and
                            compare_keys(key, range.max) != .gt);
                    }
                }

                // Check if in iterators
                var it = manifest.lookup(snapshot, key, 0);
                while (it.next()) |table| {
                    assert(table.visible(snapshot));
                    assert(compare_keys(key, table.key_min) != .lt);
                    assert(compare_keys(key, table.key_max) != .gt);
                    found |= 1;
                }

                assert(found != 0);
            },
        }

        // Compaction
        const beat = op % constants.lsm_batch_multiple;
        const half_bar = @divExact(constants.lsm_batch_multiple, 2);
        log.debug("op:{} beat:{}/{}", .{ op, beat, constants.lsm_batch_multiple });

        const op_min = op - op % half_bar;
        assert(op_min < snapshot_latest);
        assert(op_min % half_bar == 0);

        if (beat == 0 or beat == half_bar) {
            // Start of a bar
            manifest.reserve();

            const ManifestCompact = struct {
                var compacted: bool = undefined;
                fn callback(_: *Manifest) void {
                    assert(!@This().compacted);
                    @This().compacted = true;
                }
            };

            ManifestCompact.compacted = false;
            manifest.compact(ManifestCompact.callback);
            while (!ManifestCompact.compacted) storage.tick();

            // Try to compact immutable.
            if (beat >= half_bar) blk: {
                const keys = keys_immutable orelse break :blk;
                const range_b = manifest.immutable_table_compaction_range(keys.min, keys.max);

                // +1 to count the input table from level A.
                assert(range_b.tables.count() + 1 <= compaction_tables_input_max);
                assert(compare_keys(range_b.key_min, keys.min) != .gt);
                assert(compare_keys(range_b.key_max, keys.max) != .lt);

                compaction_immutable = .{
                    .state = .running,
                    .level_b = 0,
                    .range_b = range_b,
                    .op_min = op_min,
                    .move_table = false,
                    .split_table = 1,
                    .info_a = .{ .immutable = keys },
                };
                compaction_immutable.drive(random);
            } else {
                assert(compaction_immutable.state == .idle);
            }

            // Try to compact levels.
            for (&compactions, 0..) |*compaction, i| {
                const level_a = @as(u8, @intCast(i * 2)) + @intFromBool(beat >= half_bar);
                if (level_a >= constants.lsm_levels - 1) break;

                const table_range = manifest.compaction_table(level_a) orelse continue;
                const table_a = table_range.table_a.table_info;
                const range_b = table_range.range_b;

                assert(range_b.tables.count() + 1 <= compaction_tables_input_max);
                assert(compare_keys(table_a.key_min, table_a.key_max) != .gt);
                assert(compare_keys(range_b.key_min, table_a.key_min) != .gt);
                assert(compare_keys(range_b.key_max, table_a.key_max) != .lt);

                compaction.* = .{
                    .state = .running,
                    .level_b = level_a + 1,
                    .range_b = range_b,
                    .op_min = op_min,
                    .move_table = range_b.tables.count() == 1,
                    .split_table = random.uintLessThan(u32, Compaction.updates_max),
                    .info_a = .{ .level = table_range.table_a },
                };
                compaction.drive(random);
            }
        } else if (beat == half_bar - 1 or beat == constants.lsm_batch_multiple - 1) {
            // End of a bar

            if (beat >= half_bar) {
                switch (compaction_immutable.state) {
                    .idle => assert(keys_immutable == null),
                    .running => unreachable,
                    .done => {
                        compaction_immutable.apply_updates(&manifest);
                        manifest.remove_invisible_tables(
                            compaction_immutable.level_b,
                            lookup_snapshot_max,
                            compaction_immutable.range_b.key_min,
                            compaction_immutable.range_b.key_max,
                        );
                        keys_immutable = null;
                        compaction_immutable.reset();
                    },
                }
            }

            for (&compactions, 0..) |*compaction, i| {
                const level_a = @as(u8, @intCast(i * 2)) + @intFromBool(beat >= half_bar);
                if (level_a >= constants.lsm_levels - 1) break;
                switch (compaction.state) {
                    .idle => {},
                    .running => unreachable,
                    .done => {
                        compaction.apply_updates(&manifest);
                        manifest.remove_invisible_tables(
                            compaction.level_b,
                            lookup_snapshot_max,
                            compaction.range_b.key_min,
                            compaction.range_b.key_max,
                        );
                        if (compaction.level_b > 0) {
                            manifest.remove_invisible_tables(
                                compaction.level_b - 1,
                                lookup_snapshot_max,
                                compaction.range_b.key_min,
                                compaction.range_b.key_max,
                            );
                        }
                        compaction.reset();
                    },
                }
            }

            manifest.forfeit();

            assert(compaction_immutable.state == .idle);
            for (&compactions, 0..) |*compaction, i| {
                const level_a = @as(u8, @intCast(i * 2)) + @intFromBool(beat >= half_bar);
                if (level_a >= constants.lsm_levels - 1) break;
                assert(compaction.state == .idle);
            }

            // At the end of the measure.
            if (beat == constants.lsm_batch_multiple - 1) {
                manifest.assert_level_table_counts();

                // Compact immutable into mutable.
                assert(keys_immutable == null);
                if (keys_mutable) |keys| {
                    keys_immutable_snapshot = lookup_snapshot_max;
                    keys_immutable = keys;
                    keys_mutable = null;
                }
            }
        }

        // Checkpoint

        if (beat == constants.lsm_batch_multiple - 1) {
            assert(compaction_immutable.state == .idle);
            for (&compactions) |*compaction| assert(compaction.state == .idle);

            manifest.assert_level_table_counts();
            manifest.assert_no_invisible_tables(op_min);

            const ManifestCheckpoint = struct {
                var checkpointed: bool = undefined;
                fn callback(_: *Manifest) void {
                    assert(!@This().checkpointed);
                    @This().checkpointed = true;
                }
            };

            ManifestCheckpoint.checkpointed = false;
            manifest.checkpoint(ManifestCheckpoint.callback);
            while (!ManifestCheckpoint.checkpointed) storage.tick();
        }
    }

    log.info("Passed!", .{});
}
