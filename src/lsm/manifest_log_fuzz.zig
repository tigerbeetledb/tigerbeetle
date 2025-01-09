//! Fuzz ManifestLog open()/insert()/update()/remove()/compact()/checkpoint().
//!
//! Invariants checked:
//!
//! - Checkpoint flushes all buffered log blocks (including partial blocks).
//! - The state of the ManifestLog immediately after recovery matches
//!   the state of the ManifestLog immediately after the latest checkpoint.
//! - ManifestLog.open() only returns the latest version of each table.
//! - The ManifestLog performs enough compaction to not "fall behind" (i.e. run out of blocks).
//!
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_lsm_manifest_log);

const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const constants = @import("../constants.zig");
const SuperBlock = @import("../vsr/superblock.zig").SuperBlockType(Storage);
const Storage = @import("../testing/storage.zig").Storage;
const Grid = @import("../vsr/grid.zig").GridType(Storage);
const ManifestLog = @import("manifest_log.zig").ManifestLogType(Storage);
const ManifestLogPace = @import("manifest_log.zig").Pace;
const fuzz = @import("../testing/fuzz.zig");
const schema = @import("./schema.zig");
const compaction_tables_input_max = @import("./compaction.zig").compaction_tables_input_max;
const TableInfo = schema.ManifestNode.TableInfo;

const manifest_log_compaction_pace = ManifestLogPace.init(.{
    // Use many trees so that we fill manifest blocks quickly.
    // (This makes it easier to hit "worst case" scenarios in manifest compaction pacing.)
    .tree_count = 20,
    // Use a artificially low table-count-max so that we can easily fill the manifest log and verify
    // that pacing is correct.
    .tables_max = schema.ManifestNode.entry_count_max * 100,
    .compact_extra_blocks = constants.lsm_manifest_compact_extra_blocks,
});

pub fn main(args: fuzz.FuzzArgs) !void {
    const allocator = fuzz.allocator;

    var prng = std.rand.DefaultPrng.init(args.seed);

    const events_count = @min(
        args.events_max orelse @as(usize, 1e7),
        fuzz.random_int_exponential(prng.random(), usize, 1e6),
    );

    const events = try generate_events(allocator, prng.random(), events_count);
    defer allocator.free(events);

    try run_fuzz(allocator, prng.random(), events);
    log.info("Passed!", .{});
}

fn run_fuzz(
    allocator: std.mem.Allocator,
    random: std.rand.Random,
    events: []const ManifestEvent,
) !void {
    const storage_options = .{
        .seed = random.int(u64),
        .read_latency_min = 1,
        .read_latency_mean = 1 + random.uintLessThan(u64, 40),
        .write_latency_min = 1,
        .write_latency_mean = 1 + random.uintLessThan(u64, 40),
    };

    var env: Environment = undefined;
    try env.init(allocator, storage_options);
    defer env.deinit();

    {
        env.format_superblock();
        env.wait(&env.manifest_log);

        env.open_superblock();
        env.wait(&env.manifest_log);

        env.open_grid();
        env.wait(&env.manifest_log);

        // The fuzzer runs in a single process, all checkpoints are trivially durable.
        env.grid.free_set.checkpoint_durable = true;

        env.open();
        env.wait(&env.manifest_log);
    }

    // The manifest doesn't compact during the first bar.
    for (0..2) |_| {
        try env.half_bar_commence();
        try env.half_bar_complete();
    }

    try env.half_bar_commence();

    for (events) |event| {
        log.debug("event={}", .{event});
        switch (event) {
            .append => |table_info| try env.append(&table_info),
            .compact => {
                try env.half_bar_complete();
                try env.half_bar_commence();
            },
            .checkpoint => {
                // Checkpoint always immediately follows compaction.
                try env.half_bar_complete();
                try env.checkpoint();
                try env.half_bar_commence();
            },
            .noop => {},
        }
    }

    try env.half_bar_complete();
}

const ManifestEvent = union(enum) {
    append: TableInfo,
    compact,
    checkpoint,
    /// The random EventType could not be generated — this simplifies event generation.
    noop,
};

fn generate_events(
    allocator: std.mem.Allocator,
    random: std.rand.Random,
    events_count: usize,
) ![]const ManifestEvent {
    var events = std.ArrayList(ManifestEvent).init(allocator);
    errdefer events.deinit();

    var tables = std.ArrayList(TableInfo).init(allocator);
    defer tables.deinit();

    // The maximum number of (live) tables that the manifest has at any point in time.
    var tables_max: usize = 0;

    // Dummy table address for Table Infos.
    var table_address: u64 = 1;

    const compacts_per_checkpoint = fuzz.random_int_exponential(random, usize, 16);
    log.info("compacts_per_checkpoint = {d}", .{compacts_per_checkpoint});

    // When true, create as many entries as possible.
    // This tries to test the manifest upper-bound calculation.
    const fill_always = random.uintLessThan(usize, 4) == 0;

    // The maximum number of snapshot-max updates per half-bar.
    // For now, half of the total compactions.
    const updates_max = stdx.div_ceil(constants.lsm_levels, 2) * compaction_tables_input_max;

    while (events.items.len < events_count) {
        const fill = fill_always or random.boolean();
        // All of the trees we are inserting/modifying have the same id (for simplicity), but we
        // want to perform more updates if there are more trees, to better simulate a real state
        // machine.
        for (0..20) |_| {
            const operations: struct {
                update_levels: usize,
                update_snapshots: usize,
                inserts: usize,
            } = operations: {
                const move = !fill and random.uintLessThan(usize, 10) == 0;
                if (move) {
                    break :operations .{
                        .update_levels = 1,
                        .update_snapshots = 0,
                        .inserts = 0,
                    };
                } else {
                    const updates =
                        if (fill) updates_max else random.uintAtMost(usize, updates_max);
                    break :operations .{
                        .update_levels = 0,
                        .update_snapshots = updates,
                        .inserts = updates,
                    };
                }
            };

            for (0..operations.inserts) |_| {
                if (tables.items.len == manifest_log_compaction_pace.tables_max) break;

                const table = TableInfo{
                    .checksum = 0,
                    .address = table_address,
                    .snapshot_min = 1,
                    .snapshot_max = std.math.maxInt(u64),
                    .key_min = std.mem.zeroes(TableInfo.KeyPadded),
                    .key_max = std.mem.zeroes(TableInfo.KeyPadded),
                    .value_count = 1,
                    .tree_id = 1,
                    .label = .{
                        .event = .insert,
                        .level = random.uintLessThan(u6, constants.lsm_levels),
                    },
                };

                table_address += 1;
                try tables.append(table);
                try events.append(.{ .append = table });
            }
            tables_max = @max(tables_max, tables.items.len);

            for (0..operations.update_levels) |_| {
                if (tables.items.len == 0) break;

                var table = tables.items[random.uintLessThan(usize, tables.items.len)];
                if (table.label.level == constants.lsm_levels - 1) continue;
                table.label.event = .update;
                table.label.level += 1;
                try events.append(.{ .append = table });
            }

            for (0..operations.update_snapshots) |_| {
                if (tables.items.len == 0) break;

                var table = tables.items[random.uintLessThan(usize, tables.items.len)];
                // Only update a table snapshot_max once (like real compaction).
                if (table.snapshot_max == 2) continue;
                table.label.event = .update;
                table.snapshot_max = 2;
                try events.append(.{ .append = table });
            }
        }

        // We apply removes only after all inserts/updates (rather than mixing them together) to
        // mimic how compaction is followed by remove_invisible_tables().
        var i: usize = 0;
        while (i < tables.items.len) {
            if (tables.items[i].snapshot_max == 2) {
                var table = tables.swapRemove(i);
                table.label.event = .remove;
                try events.append(.{ .append = table });
            } else {
                i += 1;
            }
        }

        if (random.uintAtMost(usize, compacts_per_checkpoint) == 0) {
            try events.append(.checkpoint);
        } else {
            try events.append(.compact);
        }
    }
    log.info("event_count = {d}", .{events.items.len});
    log.info("tables_max = {d}/{d}", .{ tables_max, manifest_log_compaction_pace.tables_max });

    return events.toOwnedSlice();
}

const Environment = struct {
    allocator: std.mem.Allocator,
    storage: Storage,
    storage_verify: Storage,
    trace: vsr.trace.Tracer,
    trace_verify: vsr.trace.Tracer,
    superblock: SuperBlock,
    superblock_verify: SuperBlock,
    superblock_context: SuperBlock.Context,

    grid: Grid,
    grid_verify: Grid,

    manifest_log: ManifestLog,
    manifest_log_verify: ManifestLog,
    manifest_log_model: ManifestLogModel,
    manifest_log_opening: ?ManifestLogModel.TableMap,
    pending: u32,

    fn init(
        env: *Environment, // In-place construction for stable addresses.
        allocator: std.mem.Allocator,
        storage_options: Storage.Options,
    ) !void {
        comptime var fields_initialized = 0;

        fields_initialized += 1;
        env.allocator = allocator;

        fields_initialized += 1;
        env.storage =
            try Storage.init(allocator, constants.storage_size_limit_default, storage_options);
        errdefer env.storage.deinit(allocator);

        fields_initialized += 1;
        env.storage_verify =
            try Storage.init(allocator, constants.storage_size_limit_default, storage_options);
        errdefer env.storage_verify.deinit(allocator);

        fields_initialized += 1;
        env.trace = try vsr.trace.Tracer.init(allocator, 0, .{});
        errdefer env.trace.deinit(allocator);

        fields_initialized += 1;
        env.trace_verify = try vsr.trace.Tracer.init(allocator, 0, .{});
        errdefer env.trace_verify.deinit(allocator);

        fields_initialized += 1;
        env.superblock = try SuperBlock.init(allocator, .{
            .storage = &env.storage,
            .storage_size_limit = constants.storage_size_limit_default,
        });
        errdefer env.superblock.deinit(allocator);

        fields_initialized += 1;
        env.superblock_verify = try SuperBlock.init(allocator, .{
            .storage = &env.storage_verify,
            .storage_size_limit = constants.storage_size_limit_default,
        });
        errdefer env.superblock_verify.deinit(allocator);

        fields_initialized += 1;
        env.superblock_context = undefined;

        fields_initialized += 1;
        env.grid = try Grid.init(allocator, .{
            .superblock = &env.superblock,
            .trace = &env.trace,
            .missing_blocks_max = 0,
            .missing_tables_max = 0,
            .blocks_released_prior_checkpoint_durability_max = 0,
        });
        errdefer env.grid.deinit(allocator);

        fields_initialized += 1;
        env.grid_verify = try Grid.init(allocator, .{
            .superblock = &env.superblock_verify,
            .trace = &env.trace_verify,
            .missing_blocks_max = 0,
            .missing_tables_max = 0,
            .blocks_released_prior_checkpoint_durability_max = 0,
        });
        errdefer env.grid_verify.deinit(allocator);

        fields_initialized += 1;
        try env.manifest_log.init(allocator, &env.grid, &manifest_log_compaction_pace);
        errdefer env.manifest_log.deinit(allocator);

        fields_initialized += 1;
        try env.manifest_log_verify.init(
            allocator,
            &env.grid_verify,
            &manifest_log_compaction_pace,
        );
        errdefer env.manifest_log_verify.deinit(allocator);

        fields_initialized += 1;
        env.manifest_log_model = try ManifestLogModel.init(allocator);
        errdefer env.manifest_log_model.deinit();

        fields_initialized += 1;
        env.manifest_log_opening = null;
        fields_initialized += 1;
        env.pending = 0;

        comptime assert(fields_initialized == std.meta.fields(@This()).len);
    }

    fn deinit(env: *Environment) void {
        assert(env.manifest_log_opening == null);
        env.manifest_log_model.deinit();
        env.manifest_log_verify.deinit(env.allocator);
        env.manifest_log.deinit(env.allocator);
        env.grid_verify.deinit(env.allocator);
        env.grid.deinit(env.allocator);
        env.superblock_verify.deinit(env.allocator);
        env.superblock.deinit(env.allocator);
        env.trace_verify.deinit(env.allocator);
        env.trace.deinit(env.allocator);
        env.storage_verify.deinit(env.allocator);
        env.storage.deinit(env.allocator);
        env.* = undefined;
    }

    fn wait(env: *Environment, manifest_log: *ManifestLog) void {
        while (env.pending > 0) {
            manifest_log.superblock.storage.tick();
        }
    }

    fn format_superblock(env: *Environment) void {
        assert(env.pending == 0);
        env.pending += 1;
        env.manifest_log.superblock.format(format_superblock_callback, &env.superblock_context, .{
            .cluster = 0,
            .release = vsr.Release.minimum,
            .replica = 0,
            .replica_count = 6,
        });
    }

    fn format_superblock_callback(context: *SuperBlock.Context) void {
        const env: *Environment = @fieldParentPtr("superblock_context", context);
        env.pending -= 1;
    }

    fn open_superblock(env: *Environment) void {
        assert(env.pending == 0);
        env.pending += 1;
        env.manifest_log.superblock.open(open_superblock_callback, &env.superblock_context);
    }

    fn open_superblock_callback(context: *SuperBlock.Context) void {
        const env: *Environment = @fieldParentPtr("superblock_context", context);
        env.pending -= 1;
    }

    fn open_grid(env: *Environment) void {
        assert(env.pending == 0);
        env.pending += 1;
        env.grid.open(open_grid_callback);
    }

    fn open_grid_callback(grid: *Grid) void {
        const env: *Environment = @fieldParentPtr("grid", grid);
        env.pending -= 1;
    }

    fn open(env: *Environment) void {
        assert(env.pending == 0);

        env.pending += 1;
        env.manifest_log.open(open_event, open_callback);
    }

    fn open_event(manifest_log: *ManifestLog, table: *const TableInfo) void {
        _ = manifest_log;
        _ = table;

        // This ManifestLog is only opened during setup, when it has no blocks.
        unreachable;
    }

    fn open_callback(manifest_log: *ManifestLog) void {
        const env: *Environment = @fieldParentPtr("manifest_log", manifest_log);
        env.pending -= 1;
    }

    fn append(env: *Environment, table: *const TableInfo) !void {
        try env.manifest_log_model.append(table);
        env.manifest_log.append(table);
    }

    fn half_bar_commence(env: *Environment) !void {
        env.pending += 1;
        const op = vsr.Checkpoint.checkpoint_after(
            env.manifest_log.superblock.working.vsr_state.checkpoint.header.op,
        );
        env.manifest_log.compact(
            manifest_log_compact_callback,
            op,
        );
        env.wait(&env.manifest_log);
    }

    fn manifest_log_compact_callback(manifest_log: *ManifestLog) void {
        const env: *Environment = @fieldParentPtr("manifest_log", manifest_log);
        env.pending -= 1;
    }

    fn half_bar_complete(env: *Environment) !void {
        env.manifest_log.compact_end();
    }

    fn checkpoint(env: *Environment) !void {
        assert(env.manifest_log.grid_reservation == null);

        try env.manifest_log_model.checkpoint();

        env.pending += 1;
        env.manifest_log.checkpoint(checkpoint_manifest_log_callback);
        env.wait(&env.manifest_log);

        env.pending += 1;
        env.grid.checkpoint(checkpoint_grid_callback);
        env.wait(&env.manifest_log);

        const vsr_state = &env.manifest_log.superblock.working.vsr_state;

        env.pending += 1;
        env.manifest_log.superblock.checkpoint(
            checkpoint_superblock_callback,
            &env.superblock_context,
            .{
                .header = header: {
                    var header = vsr.Header.Prepare.root(0);
                    header.op = vsr.Checkpoint.checkpoint_after(vsr_state.checkpoint.header.op);
                    header.set_checksum();
                    break :header header;
                },
                .view_attributes = null,
                .manifest_references = env.manifest_log.checkpoint_references(),
                .free_set_references = .{
                    .blocks_acquired = env.grid
                        .free_set_checkpoint_blocks_acquired.checkpoint_reference(),
                    .blocks_released = env.grid
                        .free_set_checkpoint_blocks_released.checkpoint_reference(),
                },
                .client_sessions_reference = .{
                    .last_block_checksum = 0,
                    .last_block_address = 0,
                    .trailer_size = 0,
                    .checksum = vsr.checksum(&.{}),
                },
                .commit_max = vsr.Checkpoint.checkpoint_after(vsr_state.commit_max),
                .sync_op_min = 0,
                .sync_op_max = 0,
                .storage_size = vsr.superblock.data_file_size_min +
                    (env.grid.free_set.highest_address_acquired() orelse 0) * constants.block_size,
                .release = vsr.Release.minimum,
            },
        );
        env.wait(&env.manifest_log);

        env.grid.free_set.free_released_blocks();

        try env.verify();
    }

    fn checkpoint_manifest_log_callback(manifest_log: *ManifestLog) void {
        const env: *Environment = @fieldParentPtr("manifest_log", manifest_log);
        env.pending -= 1;
    }

    fn checkpoint_grid_callback(grid: *Grid) void {
        const env: *Environment = @fieldParentPtr("grid", grid);
        env.pending -= 1;
    }

    fn checkpoint_superblock_callback(context: *SuperBlock.Context) void {
        const env: *Environment = @fieldParentPtr("superblock_context", context);
        env.pending -= 1;
    }

    /// Verify that the state of a ManifestLog restored from checkpoint matches the state
    /// immediately after the checkpoint was created.
    fn verify(env: *Environment) !void {
        const test_trace = &env.trace;
        const test_superblock = env.manifest_log_verify.superblock;
        const test_storage = test_superblock.storage;
        const test_grid = env.manifest_log_verify.grid;
        const test_manifest_log = &env.manifest_log_verify;

        {
            test_storage.copy(env.manifest_log.superblock.storage);
            test_storage.reset();

            test_trace.deinit(env.allocator);
            test_trace.* = try vsr.trace.Tracer.init(env.allocator, 0, .{});

            // Reset the state so that the manifest log (and dependencies) can be reused.
            // Do not "defer deinit()" because these are cleaned up by Env.deinit().
            test_superblock.deinit(env.allocator);
            test_superblock.* = try SuperBlock.init(
                env.allocator,
                .{
                    .storage = test_storage,
                    .storage_size_limit = constants.storage_size_limit_default,
                },
            );

            test_grid.deinit(env.allocator);
            test_grid.* = try Grid.init(env.allocator, .{
                .superblock = test_superblock,
                .trace = test_trace,
                .missing_blocks_max = 0,
                .missing_tables_max = 0,
                .blocks_released_prior_checkpoint_durability_max = 0,
            });

            test_manifest_log.deinit(env.allocator);
            try test_manifest_log.init(env.allocator, test_grid, &manifest_log_compaction_pace);
        }

        env.pending += 1;
        test_superblock.open(verify_superblock_open_callback, &env.superblock_context);
        env.wait(test_manifest_log);

        assert(env.manifest_log_opening == null);
        env.manifest_log_opening = try env.manifest_log_model.tables.clone();
        defer {
            assert(env.manifest_log_opening.?.count() == 0);
            env.manifest_log_opening.?.deinit();
            env.manifest_log_opening = null;
        }

        env.pending += 1;
        test_manifest_log.open(verify_manifest_open_event, verify_manifest_open_callback);
        env.wait(test_manifest_log);

        try std.testing.expect(hash_map_equals(
            u64,
            ManifestLog.TableExtent,
            &env.manifest_log.table_extents,
            &test_manifest_log.table_extents,
        ));
    }

    fn verify_superblock_open_callback(superblock_context: *SuperBlock.Context) void {
        const env: *Environment = @fieldParentPtr("superblock_context", superblock_context);
        env.pending -= 1;
    }

    fn verify_manifest_open_event(
        manifest_log_verify: *ManifestLog,
        table: *const TableInfo,
    ) void {
        const env: *Environment = @fieldParentPtr("manifest_log_verify", manifest_log_verify);
        assert(env.pending > 0);

        const expect = env.manifest_log_opening.?.fetchRemove(table.address).?;
        assert(std.meta.eql(expect.value, table.*));
    }

    fn verify_manifest_open_callback(manifest_log_verify: *ManifestLog) void {
        const env: *Environment = @fieldParentPtr("manifest_log_verify", manifest_log_verify);
        env.pending -= 1;
    }
};

const ManifestLogModel = struct {
    /// Stores the latest checkpointed version of every table.
    /// Indexed by table address.
    const TableMap = std.AutoHashMap(u64, TableInfo);

    /// Stores table updates that are not yet checkpointed.
    const AppendList = std.ArrayList(TableInfo);

    tables: TableMap,
    appends: AppendList,

    fn init(allocator: std.mem.Allocator) !ManifestLogModel {
        const tables = TableMap.init(allocator);
        errdefer tables.deinit(allocator);

        const appends = AppendList.init(allocator);
        errdefer appends.deinit(allocator);

        return ManifestLogModel{
            .tables = tables,
            .appends = appends,
        };
    }

    fn deinit(model: *ManifestLogModel) void {
        model.tables.deinit();
        model.appends.deinit();
    }

    fn append(model: *ManifestLogModel, table: *const TableInfo) !void {
        try model.appends.append(table.*);
    }

    fn checkpoint(model: *ManifestLogModel) !void {
        for (model.appends.items) |table_info| {
            switch (table_info.label.event) {
                .insert,
                .update,
                => try model.tables.put(table_info.address, table_info),
                .remove => {
                    const removed = model.tables.fetchRemove(table_info.address).?;
                    assert(std.meta.eql(removed.value, table_info));
                },
                .reserved => unreachable,
            }
        }
        model.appends.clearRetainingCapacity();
    }
};

fn hash_map_equals(
    comptime K: type,
    comptime V: type,
    a: *const std.AutoHashMapUnmanaged(K, V),
    b: *const std.AutoHashMapUnmanaged(K, V),
) bool {
    if (a.count() != b.count()) return false;

    var a_iterator = a.iterator();
    while (a_iterator.next()) |a_entry| {
        const a_value = a_entry.value_ptr.*;
        const b_value = b.get(a_entry.key_ptr.*) orelse return false;
        if (!std.meta.eql(a_value, b_value)) return false;
    }
    return true;
}
