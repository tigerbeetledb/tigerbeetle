//! Fuzz ManifestLevel. All public methods are covered.
//!
//! Strategy:
//!
//! Applies operations to both the ManifestLevel and a separate table buffer to ensure the tables in
//! both match up along the way. Sporadic usage similar to Manifest/Tree is applied to make sure it
//! covers a good amount of positive space.
//!
//! Under various interleavings (those not common during normal usage but still allowed), tables are
//! inserted and eventually either have their snapshot_max updated to the current snapshot or
//! removed directly (e.g. move_table). If their snapshot_max is updated, they will eventually be
//! removed once the current snapshot is bumped either due to the level being full of tables or the
//! fuzzer deciding it wants to clean them up.
//!
//! Invariants:
//!
//! - Inserted tables are visible to the current snapshot and snapshot_latest.
//! - Updated tables are visible to the current snapshot but no longer snapshot_latest.
//! - Updated tables become completely invisible when the current snapshot is bumped.
//!
//! - Tables visible to both snapshot_latest and the current snapshot can be removed.
//! - Tables invisible to snapshot_latest but still the current snapshot cannot be removed.
//! - The current snapshot must be bumped which puts them in the next category:
//! - Tables invisible to snapshot_latest and the current snapshot can be removed.
//!
const std = @import("std");
const assert = std.debug.assert;

const log = std.log.scoped(.lsm_manifest_level_fuzz);
const constants = @import("../constants.zig");
const fuzz = @import("../testing/fuzz.zig");
const binary_search = @import("binary_search.zig");
const lsm = @import("tree.zig");
const Snapshot = @import("schema.zig").Snapshot;
const allocator = fuzz.allocator;

const Key = u64;
const Value = packed struct(u128) {
    key: Key,
    tombstone: bool,
    padding: u63 = 0,
};

inline fn key_from_value(value: *const Value) Key {
    return value.key;
}

inline fn tombstone_from_key(key: Key) Value {
    return .{ .key = key, .tombstone = true };
}

inline fn tombstone(value: *const Value) bool {
    return value.tombstone;
}

const Table = @import("table.zig").TableType(
    Key,
    Value,
    key_from_value,
    std.math.maxInt(Key),
    tombstone,
    tombstone_from_key,
    1, // Doesn't matter for this test.
    .general,
);

pub const tigerbeetle_config = @import("../config.zig").configs.test_min;

pub fn main(args: fuzz.FuzzArgs) !void {
    var prng = std.rand.DefaultPrng.init(args.seed);
    const random = prng.random();

    const fuzz_op_count = @min(
        args.events_max orelse @as(usize, 2e5),
        fuzz.random_int_exponential(random, usize, 1e5),
    );

    const table_count_max = 1024;
    const node_size = 1024;

    const fuzz_ops = try generate_fuzz_ops(random, table_count_max, fuzz_op_count);
    defer allocator.free(fuzz_ops);

    const Environment = EnvironmentType(@as(u32, table_count_max), node_size);
    var env = try Environment.init(random);
    try env.run_fuzz_ops(fuzz_ops);
    try env.deinit();
}

const FuzzOpTag = std.meta.Tag(FuzzOp);
const FuzzOp = union(enum) {
    insert_tables: usize,
    update_tables: usize,
    take_snapshot,
    remove_invisible: usize,
    remove_visible: usize,
};

// TODO: Pretty arbitrary.
const max_tables_per_insert = 10;

fn generate_fuzz_ops(
    random: std.rand.Random,
    table_count_max: usize,
    fuzz_op_count: usize,
) ![]const FuzzOp {
    log.info("fuzz_op_count = {}", .{fuzz_op_count});

    const fuzz_ops = try allocator.alloc(FuzzOp, fuzz_op_count);
    errdefer allocator.free(fuzz_ops);

    // TODO: These seem good enough, but we should find proper distributions.
    var fuzz_op_distribution = fuzz.Distribution(FuzzOpTag){
        .insert_tables = 8,
        .update_tables = 5,
        .take_snapshot = 3,
        .remove_invisible = 3,
        .remove_visible = 3,
    };
    log.info("fuzz_op_distribution = {:.2}", .{fuzz_op_distribution});

    var ctx = GenerateContext{ .max_inserted = table_count_max, .random = random };
    for (fuzz_ops) |*fuzz_op| {
        const fuzz_op_tag = fuzz.random_enum(random, FuzzOpTag, fuzz_op_distribution);
        fuzz_op.* = ctx.next(fuzz_op_tag);
    }

    return fuzz_ops;
}

const GenerateContext = struct {
    inserted: usize = 0,
    updated: usize = 0,
    invisible: usize = 0,
    max_inserted: usize,
    random: std.rand.Random,

    fn next(ctx: *GenerateContext, fuzz_op_tag: FuzzOpTag) FuzzOp {
        switch (fuzz_op_tag) {
            .insert_tables => {
                // If there's no room for new tables, existing ones should be removed.
                const insertable = @min(ctx.max_inserted - ctx.inserted, max_tables_per_insert);
                if (insertable == 0) {
                    // Decide whether to remove visible or invisible tables:
                    if (ctx.invisible > 0) return ctx.next(.remove_invisible);
                    return ctx.next(.remove_visible);
                }

                var amount = ctx.random.intRangeAtMostBiased(usize, 1, insertable);
                ctx.inserted += amount;
                assert(ctx.invisible <= ctx.inserted);
                assert(ctx.invisible + ctx.updated <= ctx.inserted);
                return FuzzOp{ .insert_tables = amount };
            },
            .update_tables => {
                // If there's no tables visible to snapshot_latest to update, make more tables.
                const visible_latest = (ctx.inserted - ctx.invisible) - ctx.updated;
                if (visible_latest == 0) return ctx.next(.insert_tables);

                // Decide if all, or tables visible to snapshot_latest should be updated.
                var amount = if (ctx.random.boolean())
                    visible_latest
                else
                    ctx.random.intRangeAtMostBiased(usize, 1, visible_latest);

                ctx.updated += amount;
                assert(ctx.invisible <= ctx.inserted);
                assert(ctx.invisible + ctx.updated <= ctx.inserted);
                return FuzzOp{ .update_tables = amount };
            },
            .take_snapshot => {
                ctx.invisible += ctx.updated;
                ctx.updated = 0;
                return FuzzOp.take_snapshot;
            },
            .remove_invisible => {
                // Decide what to do if there's no invisible tables to be removed:
                const invisible = ctx.invisible;
                if (invisible == 0) {
                    // Either insert more tables to later be made invisible,
                    // update currently inserted tables to be made invisible on the next snapshot,
                    // or take a snapshot to make existing updated tables invisible for next remove.
                    if (ctx.inserted == 0) return ctx.next(.insert_tables);
                    if (ctx.updated == 0) return ctx.next(.update_tables);
                    return ctx.next(.take_snapshot);
                }

                // Decide if all invisible tables should be removed.
                var amount = if (ctx.random.boolean())
                    invisible
                else
                    ctx.random.intRangeAtMostBiased(usize, 1, invisible);

                ctx.inserted -= amount;
                ctx.invisible -= amount;
                assert(ctx.invisible <= ctx.inserted);
                assert(ctx.invisible + ctx.updated <= ctx.inserted);
                return FuzzOp{ .remove_invisible = amount };
            },
            .remove_visible => {
                // If there are no tables visible to snapshot_latest for removal,
                // we either create new ones for future removal or remove invisible ones.
                const visible_latest = (ctx.inserted - ctx.invisible) - ctx.updated;
                if (visible_latest == 0) {
                    if (ctx.inserted < ctx.max_inserted) return ctx.next(.insert_tables);
                    return ctx.next(.remove_invisible);
                }

                // Decide if all tables visible ot snapshot_latest should be removed.
                var amount = if (ctx.random.boolean())
                    visible_latest
                else
                    ctx.random.intRangeAtMostBiased(usize, 1, visible_latest);

                ctx.inserted -= amount;
                assert(ctx.invisible <= ctx.inserted);
                assert(ctx.invisible + ctx.updated <= ctx.inserted);
                return FuzzOp{ .remove_visible = amount };
            },
        }
    }
};

pub fn EnvironmentType(comptime table_count_max: u32, comptime node_size: u32) type {
    return struct {
        const Environment = @This();

        const TableBuffer = std.ArrayList(TableInfo);
        const NodePool = @import("node_pool.zig").NodePool(node_size, @alignOf(TableInfo));
        pub const ManifestLevel = @import("manifest_level.zig").ManifestLevelType(
            NodePool,
            Key,
            TableInfo,
            table_count_max,
        );
        pub const TableInfo = @import("manifest.zig").TreeTableInfoType(Table);
        pool: NodePool,
        level: ManifestLevel,
        buffer: TableBuffer,
        tables: TableBuffer,
        random: std.rand.Random,
        snapshot: Snapshot,

        pub fn init(random: std.rand.Random) !Environment {
            var env: Environment = undefined;

            const node_pool_size = ManifestLevel.Keys.node_count_max +
                ManifestLevel.Tables.node_count_max;
            env.pool = try NodePool.init(allocator, node_pool_size);
            errdefer env.pool.deinit(allocator);

            env.level = try ManifestLevel.init(allocator);
            errdefer env.level.deinit(allocator, &env.pool);

            env.buffer = TableBuffer.init(allocator);
            errdefer env.buffer.deinit();

            env.tables = TableBuffer.init(allocator);
            errdefer env.tables.deinit();

            env.random = random;
            env.snapshot = .{ .timestamp = 1 }; // the first snapshot is reserved.
            return env;
        }

        pub fn deinit(env: *Environment) !void {
            env.tables.deinit();
            env.buffer.deinit();
            env.level.deinit(allocator, &env.pool);
            env.pool.deinit(allocator);
        }

        pub fn run_fuzz_ops(env: *Environment, fuzz_ops: []const FuzzOp) !void {
            for (fuzz_ops, 0..) |fuzz_op, op_index| {
                log.debug("Running fuzz_ops[{}/{}] == {}", .{ op_index, fuzz_ops.len, fuzz_op });
                switch (fuzz_op) {
                    .insert_tables => |amount| try env.insert_tables(amount),
                    .update_tables => |amount| try env.update_tables(amount),
                    .take_snapshot => try env.take_snapshot(),
                    .remove_invisible => |amount| try env.remove_invisible(amount),
                    .remove_visible => |amount| try env.remove_visible(amount),
                }
            }
        }

        pub fn insert_tables(env: *Environment, amount: usize) !void {
            assert(amount > 0);
            assert(env.buffer.items.len == 0);

            // Generate random, non-overlapping TableInfo's into env.buffer:
            {
                var insert_amount = amount;
                var key = env.random.uintAtMostBiased(Key, table_count_max * 64);

                while (insert_amount > 0) : (insert_amount -= 1) {
                    const table = env.generate_non_overlapping_table(key);
                    try env.buffer.append(table);
                    key = table.key_max;
                }
            }

            const tables = env.buffer.items;
            defer env.buffer.clearRetainingCapacity();

            // Insert the generated tables into the ManifestLevel:
            for (tables) |*table| {
                assert(table.visible(env.snapshot));
                assert(table.visible(Snapshot.latest));
                env.level.insert_table(&env.pool, table);
            }

            // Insert the generated tables into the Environment for reference:
            for (tables) |*table| {
                const index = binary_search.binary_search_values_upsert_index(
                    Key,
                    TableInfo,
                    key_min_from_table,
                    env.tables.items,
                    table.key_max,
                    .{},
                );

                // Can't be equal as the tables may not overlap.
                if (index < env.tables.items.len) {
                    assert(env.tables.items[index].key_min > table.key_max);
                }

                try env.tables.insert(index, table.*);
            }
        }

        fn generate_non_overlapping_table(env: *Environment, key: Key) TableInfo {
            var new_key_min = key + env.random.uintLessThanBiased(Key, 31) + 1;
            assert(new_key_min > key);

            var i = binary_search.binary_search_values_upsert_index(
                Key,
                TableInfo,
                key_min_from_table,
                env.tables.items,
                new_key_min,
                .{},
            );

            if (i > 0) {
                if (new_key_min <= env.tables.items[i - 1].key_max) {
                    new_key_min = env.tables.items[i - 1].key_max + 1;
                }
            }

            const next_key_min = for (env.tables.items[i..]) |table| {
                switch (std.math.order(new_key_min, table.key_min)) {
                    .lt => break table.key_min,
                    .eq => new_key_min = table.key_max + 1,
                    .gt => unreachable,
                }
            } else std.math.maxInt(Key);

            const max_delta = @min(32, next_key_min - 1 - new_key_min);
            const new_key_max = new_key_min + env.random.uintAtMostBiased(Key, max_delta);

            return .{
                .checksum = env.random.int(u128),
                // Zero addresses are used to indicate the table being removed.
                .address = env.random.intRangeAtMostBiased(u64, 1, std.math.maxInt(u64)),
                .snapshot_min = env.snapshot,
                .key_min = new_key_min,
                .key_max = new_key_max,
                .value_count = 64,
            };
        }

        inline fn key_min_from_table(table: *const TableInfo) Key {
            return table.key_min;
        }

        fn update_tables(env: *Environment, amount: usize) !void {
            var update_amount = amount;
            assert(amount > 0);

            // Only update the snapshot_max of those visible to snapshot_latest.
            // Those visible to env.snapshot would include tables with updated snapshot_max.
            const snapshots = @as(*const [1]Snapshot, &Snapshot.latest);

            var it = env.level.iterator(.visible, snapshots, .descending, null);
            while (it.next()) |level_table| {
                assert(level_table.visible(env.snapshot));
                assert(level_table.visible(Snapshot.latest));

                const env_table = env.find_exact(level_table);
                assert(level_table.equal(env_table));

                env.level.set_snapshot_max(env.snapshot, .{
                    .table_info = @constCast(level_table),
                    .generation = env.level.generation,
                });
                // This is required to keep the table in the fuzzer's environment consistent with
                // the table in the ManifestLevel.
                env_table.snapshot_max = env.snapshot;
                assert(level_table.snapshot_max.timestamp == env.snapshot.timestamp);
                assert(!level_table.visible(Snapshot.latest));
                assert(level_table.visible(env.snapshot));

                update_amount -= 1;
                if (update_amount == 0) break;
            }

            assert(update_amount == 0);
        }

        fn find_exact(env: *Environment, level_table: *const TableInfo) *TableInfo {
            const index = binary_search.binary_search_values_upsert_index(
                Key,
                TableInfo,
                key_min_from_table,
                env.tables.items,
                level_table.key_min,
                .{},
            );

            assert(index < env.tables.items.len);
            const tables = env.tables.items[index..];

            assert(tables[0].key_min == level_table.key_min);
            for (tables) |*env_table| {
                if (env_table.key_max == level_table.key_max) {
                    return env_table;
                }
            }

            std.debug.panic("table not found in fuzzer reference model: {any}", .{level_table.*});
        }

        fn take_snapshot(env: *Environment) !void {
            env.snapshot.timestamp += 1;
            assert(env.snapshot.timestamp < Snapshot.latest.timestamp);
        }

        fn remove_invisible(env: *Environment, amount: usize) !void {
            var remove_amount = amount;
            assert(amount > 0);

            // Remove tables not visible to the current snapshot.
            const snapshots = [_]Snapshot{env.snapshot};

            // Remove invisible tables from ManifestLevel and mark them as removed in env.tables:
            var it = env.level.iterator(.invisible, &snapshots, .descending, null);
            while (it.next()) |level_table| {
                env.mark_removed_table(level_table);

                assert(level_table.invisible(&snapshots));
                var level_table_copy = level_table.*;
                env.level.remove_table(&env.pool, &level_table_copy);

                remove_amount -= 1;
                if (remove_amount == 0) break;
            }

            assert(remove_amount == 0);
            try env.purge_removed_tables();
        }

        fn remove_visible(env: *Environment, amount: usize) !void {
            var remove_amount = amount;
            assert(amount > 0);

            // ManifestLevel.remove_table_visible() only removes those visible to snapshot_latest.
            const snapshots = @as(*const [1]Snapshot, &Snapshot.latest);

            // Remove visible tables from ManifestLevel and mark them as removed in env.tables:
            var it = env.level.iterator(.visible, snapshots, .descending, null);
            while (it.next()) |level_table| {
                env.mark_removed_table(level_table);

                assert(level_table.visible(Snapshot.latest));
                var level_table_copy = level_table.*;
                env.level.remove_table(&env.pool, &level_table_copy);

                remove_amount -= 1;
                if (remove_amount == 0) break;
            }

            assert(remove_amount == 0);
            try env.purge_removed_tables();
        }

        /// Mark the matching table as removed in env.tables.
        /// It will be removed from env.tables from a later call to env.purge_removed_tables().
        fn mark_removed_table(env: *Environment, level_table: *const TableInfo) void {
            const env_table = env.find_exact(level_table);
            assert(level_table.equal(env_table));

            // Zero address means the table is removed.
            assert(env_table.address != 0);
            env_table.address = 0;
        }

        /// Filter out all env.tables removed with env.mark_removed_table() by copying all
        /// non-removed tables into env.buffer and flipping it with env.tables.
        /// TODO: This clears removed tables in O(n).
        fn purge_removed_tables(env: *Environment) !void {
            assert(env.buffer.items.len == 0);
            try env.buffer.ensureTotalCapacity(env.tables.items.len);

            for (env.tables.items) |*table| {
                if (table.address == 0) continue;
                try env.buffer.append(table.*);
            }

            std.mem.swap(TableBuffer, &env.buffer, &env.tables);
            env.buffer.clearRetainingCapacity();
        }
    };
}
