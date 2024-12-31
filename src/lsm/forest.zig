const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const mem = std.mem;
const log = std.log.scoped(.forest);

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");

const schema = @import("schema.zig");
const GridType = @import("../vsr/grid.zig").GridType;
const NodePool = @import("node_pool.zig").NodePoolType(constants.lsm_manifest_node_size, 16);
const ManifestLogType = @import("manifest_log.zig").ManifestLogType;
const ScanBufferPool = @import("scan_buffer.zig").ScanBufferPool;
const ResourcePoolType = @import("compaction.zig").ResourcePoolType;
const snapshot_min_for_table_output = @import("compaction.zig").snapshot_min_for_table_output;
const compaction_op_min = @import("compaction.zig").compaction_op_min;
const compaction_block_count_bar_max = @import("compaction.zig").compaction_block_count_bar_max;
const compaction_block_count_beat_min = @import("compaction.zig").compaction_block_count_beat_min;

/// The maximum number of tables for the forest as a whole. This is set a bit backwards due to how
/// the code is structured: a single tree should be able to use all the tables in the forest, so the
/// table_count_max of the forest is equal to the table_count_max of a single tree.
/// In future, Forest.table_count_max could exceed Tree.table_count_max.
pub const table_count_max = @import("tree.zig").table_count_max;

pub fn ForestType(comptime _Storage: type, comptime groove_cfg: anytype) type {
    var groove_fields: []const std.builtin.Type.StructField = &.{};
    var groove_options_fields: []const std.builtin.Type.StructField = &.{};

    for (std.meta.fields(@TypeOf(groove_cfg))) |field| {
        const Groove = @field(groove_cfg, field.name);
        groove_fields = groove_fields ++ [_]std.builtin.Type.StructField{
            .{
                .name = field.name,
                .type = Groove,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(Groove),
            },
        };

        groove_options_fields = groove_options_fields ++ [_]std.builtin.Type.StructField{
            .{
                .name = field.name,
                .type = Groove.Options,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(Groove),
            },
        };
    }

    const _Grooves = @Type(.{
        .Struct = .{
            .layout = .auto,
            .fields = groove_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    const _GroovesOptions = @Type(.{
        .Struct = .{
            .layout = .auto,
            .fields = groove_options_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    {
        // Verify that every tree id is unique.
        comptime var ids: []const u16 = &.{};

        inline for (std.meta.fields(_Grooves)) |groove_field| {
            const Groove = groove_field.type;

            for (std.meta.fields(@TypeOf(Groove.config.ids))) |field| {
                const id = @field(Groove.config.ids, field.name);
                assert(id > 0);
                assert(std.mem.indexOfScalar(u16, ids, id) == null);

                ids = ids ++ [_]u16{id};
            }
        }
    }

    const TreeInfo = struct {
        Tree: type,
        tree_name: []const u8,
        tree_id: u16,
        groove_name: []const u8,
        groove_tree: union(enum) { objects, ids, indexes: []const u8 },
    };

    // Invariants:
    // - tree_infos[tree_id - tree_id_range.min].tree_id == tree_id
    // - tree_infos.len == tree_id_range.max - tree_id_range.min
    const _tree_infos = tree_infos: {
        var tree_infos: []const TreeInfo = &[_]TreeInfo{};
        for (std.meta.fields(_Grooves)) |groove_field| {
            const Groove = groove_field.type;

            tree_infos = tree_infos ++ &[_]TreeInfo{.{
                .Tree = Groove.ObjectTree,
                .tree_name = groove_field.name,
                .tree_id = @field(Groove.config.ids, "timestamp"),
                .groove_name = groove_field.name,
                .groove_tree = .objects,
            }};

            if (Groove.IdTree != void) {
                tree_infos = tree_infos ++ &[_]TreeInfo{.{
                    .Tree = Groove.IdTree,
                    .tree_name = groove_field.name ++ ".id",
                    .tree_id = @field(Groove.config.ids, "id"),
                    .groove_name = groove_field.name,
                    .groove_tree = .ids,
                }};
            }

            for (std.meta.fields(Groove.IndexTrees)) |tree_field| {
                tree_infos = tree_infos ++ &[_]TreeInfo{.{
                    .Tree = tree_field.type,
                    .tree_name = groove_field.name ++ "." ++ tree_field.name,
                    .tree_id = @field(Groove.config.ids, tree_field.name),
                    .groove_name = groove_field.name,
                    .groove_tree = .{ .indexes = tree_field.name },
                }};
            }
        }

        var tree_id_min = std.math.maxInt(u16);
        for (tree_infos) |tree_info| tree_id_min = @min(tree_id_min, tree_info.tree_id);

        var tree_infos_sorted: [tree_infos.len]TreeInfo = undefined;
        var tree_infos_set = std.StaticBitSet(tree_infos.len).initEmpty();
        for (tree_infos) |tree_info| {
            const tree_index = tree_info.tree_id - tree_id_min;
            assert(!tree_infos_set.isSet(tree_index));

            tree_infos_sorted[tree_index] = tree_info;
            tree_infos_set.set(tree_index);
        }

        // There are no gaps in the tree ids.
        assert(tree_infos_set.count() == tree_infos.len);

        break :tree_infos tree_infos_sorted;
    };

    const _TreeID = comptime tree_id: {
        var fields: []const std.builtin.Type.EnumField = &.{};
        for (_tree_infos) |tree_info| {
            fields = fields ++ [1]std.builtin.Type.EnumField{.{
                .name = @ptrCast(tree_info.tree_name),
                .value = tree_info.tree_id,
            }};
        }
        break :tree_id @Type(.{ .Enum = .{
            .tag_type = u16,
            .fields = fields,
            .decls = &.{},
            .is_exhaustive = true,
        } });
    };

    comptime {
        assert(std.enums.values(_TreeID).len == _tree_infos.len);
        for (std.enums.values(_TreeID)) |tree_id| {
            const tree_info = _tree_infos[@intFromEnum(tree_id) - _tree_infos[0].tree_id];
            assert(tree_id == @as(_TreeID, @enumFromInt(tree_info.tree_id)));
        }
    }

    const Grid = GridType(_Storage);

    return struct {
        const Forest = @This();

        pub const ManifestLog = ManifestLogType(Storage);
        const CompactionSchedule = CompactionScheduleType(Forest, Grid);

        const Callback = *const fn (*Forest) void;

        pub const Storage = _Storage;
        pub const groove_config = groove_cfg;
        pub const Grooves = _Grooves;
        pub const GroovesOptions = _GroovesOptions;
        // TreeID is an enum with a value for each tree type.
        // Individual trees use `u16` to store their own id, to avoid dependency on the entire
        // forest.
        // Use `tree_id_cast` function to convert this type-erased u16 to a TreeID.
        pub const TreeID = _TreeID;
        pub const tree_infos = _tree_infos;
        pub const tree_id_range = .{
            .min = tree_infos[0].tree_id,
            .max = tree_infos[tree_infos.len - 1].tree_id,
        };

        pub const Options = struct {
            node_count: u32,
            /// The amount of blocks allocated for compactions. Compactions will be deterministic
            /// regardless of how much blocks you give them, but will run in fewer steps with more
            /// memory.
            compaction_block_count: u32,

            pub const compaction_block_count_min: u32 = CompactionSchedule.forest_block_count_min;
        };

        progress: ?union(enum) {
            open: struct { callback: Callback },
            checkpoint: struct { callback: Callback },
            compact: struct {
                op: u64,
                callback: Callback,
            },
        } = null,

        compaction_progress: ?struct {
            trees_done: bool,
            manifest_log_done: bool,

            fn all_done(compaction_progress: @This()) bool {
                return compaction_progress.trees_done and compaction_progress.manifest_log_done;
            }
        } = null,

        grid: *Grid,
        grooves: Grooves,
        node_pool: NodePool,
        manifest_log: ManifestLog,

        compaction_schedule: CompactionSchedule,

        scan_buffer_pool: ScanBufferPool,

        pub fn init(
            forest: *Forest,
            allocator: mem.Allocator,
            grid: *Grid,
            options: Options,
            // (e.g.) .{ .transfers = .{ .cache_entries_max = 128, … }, .accounts = … }
            grooves_options: GroovesOptions,
        ) !void {
            assert(options.compaction_block_count >= Options.compaction_block_count_min);

            forest.* = .{
                .grid = grid,
                .grooves = undefined,
                .node_pool = undefined,
                .manifest_log = undefined,
                .compaction_schedule = undefined,
                .scan_buffer_pool = undefined,
            };

            // TODO: look into using lsm_table_size_max for the node_count.
            try forest.node_pool.init(allocator, options.node_count);
            errdefer forest.node_pool.deinit(allocator);

            try forest.manifest_log.init(allocator, grid, .{
                .tree_id_min = tree_id_range.min,
                .tree_id_max = tree_id_range.max,
                // TODO Make this a runtime argument (from the CLI, derived from storage-size-max if
                // possible).
                .forest_table_count_max = table_count_max,
            });
            errdefer forest.manifest_log.deinit(allocator);

            var grooves_initialized: usize = 0;
            errdefer inline for (std.meta.fields(Grooves), 0..) |field, field_index| {
                if (grooves_initialized >= field_index + 1) {
                    const Groove = field.type;
                    const groove: *Groove = &@field(forest.grooves, field.name);
                    groove.deinit(allocator);
                }
            };

            inline for (std.meta.fields(Grooves)) |field| {
                const Groove = field.type;
                const groove: *Groove = &@field(forest.grooves, field.name);
                const groove_options: Groove.Options = @field(grooves_options, field.name);

                try groove.init(allocator, &forest.node_pool, grid, groove_options);
                grooves_initialized += 1;
            }

            try forest.compaction_schedule.init(
                allocator,
                grid,
                forest,
                options.compaction_block_count,
            );
            errdefer forest.compaction_schedule.deinit(allocator);

            try forest.scan_buffer_pool.init(allocator);
            errdefer forest.scan_buffer_pool.deinit(allocator);
        }

        pub fn deinit(forest: *Forest, allocator: mem.Allocator) void {
            inline for (std.meta.fields(Grooves)) |field| {
                const Groove = field.type;
                const groove: *Groove = &@field(forest.grooves, field.name);
                groove.deinit(allocator);
            }

            forest.manifest_log.deinit(allocator);
            forest.node_pool.deinit(allocator);

            forest.compaction_schedule.deinit(allocator);
            forest.scan_buffer_pool.deinit(allocator);
        }

        pub fn reset(forest: *Forest) void {
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).reset();
            }

            forest.grid.trace.reset(.lookup);
            forest.grid.trace.reset(.lookup_worker);
            forest.grid.trace.reset(.scan_tree);
            forest.grid.trace.reset(.scan_tree_level);

            forest.manifest_log.reset();
            forest.node_pool.reset();
            forest.scan_buffer_pool.reset();
            forest.compaction_schedule.reset();

            forest.* = .{
                // Don't reset the grid – replica is responsible for grid cancellation.
                .grid = forest.grid,
                .grooves = forest.grooves,
                .node_pool = forest.node_pool,
                .manifest_log = forest.manifest_log,

                .compaction_schedule = forest.compaction_schedule,

                .scan_buffer_pool = forest.scan_buffer_pool,
            };
        }

        pub fn open(forest: *Forest, callback: Callback) void {
            assert(forest.progress == null);
            assert(forest.compaction_progress == null);

            forest.progress = .{ .open = .{ .callback = callback } };

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).open_commence(&forest.manifest_log);
            }

            forest.manifest_log.open(manifest_log_open_event, manifest_log_open_callback);
        }

        fn manifest_log_open_event(
            manifest_log: *ManifestLog,
            table: *const schema.ManifestNode.TableInfo,
        ) void {
            const forest: *Forest = @fieldParentPtr("manifest_log", manifest_log);
            assert(forest.progress.? == .open);
            assert(forest.compaction_progress == null);
            assert(table.label.level < constants.lsm_levels);
            assert(table.label.event != .remove);

            if (table.tree_id < tree_id_range.min or table.tree_id > tree_id_range.max) {
                log.err("manifest_log_open_event: unknown table in manifest: {}", .{table});
                @panic("Forest.manifest_log_open_event: unknown table in manifest");
            }
            switch (tree_id_cast(table.tree_id)) {
                inline else => |tree_id| {
                    var tree: *TreeForIdType(tree_id) = forest.tree_for_id(tree_id);
                    tree.open_table(table);
                },
            }
        }

        fn manifest_log_open_callback(manifest_log: *ManifestLog) void {
            const forest: *Forest = @fieldParentPtr("manifest_log", manifest_log);
            assert(forest.progress.? == .open);
            assert(forest.compaction_progress == null);
            forest.verify_tables_recovered();

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).open_complete();
            }
            forest.verify_table_extents();

            const callback = forest.progress.?.open.callback;
            forest.progress = null;
            callback(forest);
        }

        pub fn compact(forest: *Forest, callback: Callback, op: u64) void {
            const compaction_beat = op % constants.lsm_compaction_ops;

            const first_beat = compaction_beat == 0;
            const last_half_beat = compaction_beat ==
                @divExact(constants.lsm_compaction_ops, 2) - 1;
            const half_beat = compaction_beat == @divExact(constants.lsm_compaction_ops, 2);
            const last_beat = compaction_beat == constants.lsm_compaction_ops - 1;
            assert(@as(usize, @intFromBool(first_beat)) + @intFromBool(last_half_beat) +
                @intFromBool(half_beat) + @intFromBool(last_beat) <= 1);

            log.debug("entering forest.compact() op={} constants.lsm_compaction_ops={} " ++
                "first_beat={} last_half_beat={} half_beat={} last_beat={}", .{
                op,
                constants.lsm_compaction_ops,
                first_beat,
                last_half_beat,
                half_beat,
                last_beat,
            });

            assert(forest.progress == null);
            forest.progress = .{ .compact = .{
                .op = op,
                .callback = callback,
            } };

            // Run trees and manifest log compaction in parallel, join in compact_finish.
            assert(forest.compaction_progress == null);
            forest.compaction_progress = .{
                .trees_done = false,
                .manifest_log_done = false,
            };

            forest.compaction_schedule.beat_start(compact_trees_callback, op);

            // Manifest log compaction. Run on the last beat of each half-bar.
            // TODO: Figure out a plan wrt the pacing here. Putting it on the last beat kinda-sorta
            // balances out, because we expect to naturally do less other compaction work on the
            // last beat.
            // The first bar has no manifest compaction.
            if ((last_beat or last_half_beat) and op > constants.lsm_compaction_ops) {
                forest.manifest_log.compact(compact_manifest_log_callback, op);
            } else {
                forest.compaction_progress.?.manifest_log_done = true;
            }
        }

        fn compact_trees_callback(forest: *Forest) void {
            assert(forest.progress.? == .compact);
            assert(forest.compaction_progress != null);
            assert(!forest.compaction_progress.?.trees_done);
            forest.compaction_progress.?.trees_done = true;

            if (forest.compaction_progress.?.all_done()) {
                forest.compact_finish();
            }
        }

        fn compact_manifest_log_callback(manifest_log: *ManifestLog) void {
            const forest: *Forest = @fieldParentPtr("manifest_log", manifest_log);

            assert(forest.progress.? == .compact);
            assert(forest.compaction_progress != null);
            assert(!forest.compaction_progress.?.manifest_log_done);
            forest.compaction_progress.?.manifest_log_done = true;

            if (forest.compaction_progress.?.all_done()) {
                forest.compact_finish();
            }
        }

        fn compact_finish(forest: *Forest) void {
            assert(forest.progress.? == .compact);
            assert(forest.compaction_progress != null);
            assert(forest.compaction_progress.?.trees_done);
            assert(forest.compaction_progress.?.manifest_log_done);
            assert(forest.compaction_schedule.pool.idle());
            assert(forest.compaction_schedule.pool.blocks_acquired() <=
                CompactionSchedule.forest_block_count_bar_max);

            forest.verify_table_extents();

            assert(forest.progress.? == .compact);
            const op = forest.progress.?.compact.op;

            const compaction_beat = op % constants.lsm_compaction_ops;
            const last_half_beat = compaction_beat ==
                @divExact(constants.lsm_compaction_ops, 2) - 1;
            const last_beat = compaction_beat == constants.lsm_compaction_ops - 1;

            if (op < constants.lsm_compaction_ops or
                forest.grid.superblock.working.vsr_state.op_compacted(op))
            {
                // No compaction was run.
            } else {
                // Apply the changes to the manifest. This will run at the target compaction beat
                // that is requested.
                if (last_beat or last_half_beat) {
                    for (0..constants.lsm_levels) |level_b| {
                        if (level_active(.{ .level_b = level_b, .op = op })) {
                            inline for (comptime std.enums.values(Forest.TreeID)) |tree_id| {
                                forest.compaction_schedule.compaction_at(level_b, tree_id)
                                    .bar_complete();
                            }
                        }
                    }
                }
            }

            // Groove sync compaction - must be done after all async work for the beat completes.
            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).compact(op);
            }

            // Swap the mutable and immutable tables; this must happen on the last beat, regardless
            // of pacing.
            if (last_beat) {
                inline for (comptime std.enums.values(TreeID)) |tree_id| {
                    const tree = tree_for_id(forest, tree_id);

                    log.debug("swap_mutable_and_immutable({s})", .{tree.config.name});
                    tree.swap_mutable_and_immutable(
                        snapshot_min_for_table_output(compaction_op_min(op)),
                    );

                    // Ensure tables haven't overflowed.
                    tree.manifest.assert_level_table_counts();
                }
            }

            // On the last beat of the bar, make sure that manifest log compaction is finished.
            if ((last_beat or last_half_beat) and op > constants.lsm_compaction_ops) {
                forest.manifest_log.compact_end();
            }

            const callback = forest.progress.?.compact.callback;
            forest.progress = null;
            forest.compaction_progress = null;

            callback(forest);
        }

        pub fn checkpoint(forest: *Forest, callback: Callback) void {
            assert(forest.progress == null);
            assert(forest.compaction_progress == null);
            forest.grid.assert_only_repairing();
            forest.verify_table_extents();

            forest.progress = .{ .checkpoint = .{ .callback = callback } };

            inline for (std.meta.fields(Grooves)) |field| {
                @field(forest.grooves, field.name).assert_between_bars();
            }

            inline for (comptime std.enums.values(TreeID)) |tree_id| {
                const tree = tree_for_id(forest, tree_id);

                // The last immutable table constructed before the checkpoint must not absorb any
                // mutable table, because otherwise recovering from checkpoint would construct a
                // different immutable table.
                assert(!tree.table_immutable.mutability.immutable.absorbed);
                maybe(tree.table_immutable.count() > 0);
                assert(tree.table_mutable.count() == 0);
            }

            forest.manifest_log.checkpoint(checkpoint_manifest_log_callback);
        }

        fn checkpoint_manifest_log_callback(manifest_log: *ManifestLog) void {
            const forest: *Forest = @fieldParentPtr("manifest_log", manifest_log);
            assert(forest.progress.? == .checkpoint);
            assert(forest.compaction_progress == null);
            forest.verify_table_extents();
            forest.verify_tables_recovered();

            const callback = forest.progress.?.checkpoint.callback;
            forest.progress = null;
            callback(forest);
        }

        pub fn tree_id_cast(tree_id: u16) TreeID {
            return @enumFromInt(tree_id);
        }

        fn TreeForIdType(comptime tree_id: TreeID) type {
            const tree_info = tree_infos[@intFromEnum(tree_id) - tree_id_range.min];
            assert(tree_info.tree_id == @intFromEnum(tree_id));

            return tree_info.Tree;
        }

        pub fn tree_for_id(forest: *Forest, comptime tree_id: TreeID) *TreeForIdType(tree_id) {
            const tree_info = tree_infos[@intFromEnum(tree_id) - tree_id_range.min];
            assert(tree_info.tree_id == @intFromEnum(tree_id));

            var groove = &@field(forest.grooves, tree_info.groove_name);

            switch (tree_info.groove_tree) {
                .objects => return &groove.objects,
                .ids => return &groove.ids,
                .indexes => |index_name| return &@field(groove.indexes, index_name),
            }
        }

        pub fn tree_for_id_const(
            forest: *const Forest,
            comptime tree_id: TreeID,
        ) *const TreeForIdType(tree_id) {
            const tree_info = tree_infos[@intFromEnum(tree_id) - tree_id_range.min];
            assert(tree_info.tree_id == @intFromEnum(tree_id));

            const groove = &@field(forest.grooves, tree_info.groove_name);

            switch (tree_info.groove_tree) {
                .objects => return &groove.objects,
                .ids => return &groove.ids,
                .indexes => |index_name| return &@field(groove.indexes, index_name),
            }
        }

        /// Returns whether the forest contains this table (ignoring snapshot_max) at any level.
        pub fn contains_table(
            forest: *const Forest,
            table: *const schema.ManifestNode.TableInfo,
        ) bool {
            switch (tree_id_cast(table.tree_id)) {
                inline else => |tree_id| {
                    const tree = forest.tree_for_id_const(tree_id);
                    const Tree = Forest.TreeForIdType(tree_id);
                    const tree_table = Tree.Manifest.TreeTableInfo.decode(table);
                    for (&tree.manifest.levels) |manifest_level| {
                        if (manifest_level.find(&tree_table)) |level_table| {
                            assert(table.checksum == level_table.table_info.checksum);
                            assert(table.snapshot_max <= level_table.table_info.snapshot_max);
                            return true;
                        }
                    }
                    return false;
                },
            }
        }

        /// Verify that `ManifestLog.table_extents` has an extent for every active table.
        ///
        /// (Invoked between beats.)
        fn verify_table_extents(forest: *const Forest) void {
            var tables_count: usize = 0;
            inline for (comptime std.enums.values(TreeID)) |tree_id| {
                for (0..constants.lsm_levels) |level| {
                    const tree_level = forest.tree_for_id_const(tree_id).manifest.levels[level];
                    tables_count += tree_level.tables.len();

                    if (constants.verify) {
                        var tables_iterator = tree_level.tables.iterator_from_index(0, .ascending);
                        while (tables_iterator.next()) |table| {
                            assert(forest.manifest_log.table_extents.get(table.address) != null);
                        }
                    }
                }
            }
            assert(tables_count == forest.manifest_log.table_extents.count());
        }

        /// Verify the tables recovered into the ManifestLevels after opening the manifest log.
        ///
        /// There are two strategies to reconstruct the LSM's manifest levels (i.e. the list of
        /// tables) from a superblock manifest:
        ///
        /// 1. Iterate the manifest events in chronological order, replaying each
        ///    insert/update/remove in sequence.
        /// 2. Iterate the manifest events in reverse-chronological order, ignoring events for
        ///    tables that have already been encountered.
        ///
        /// The manifest levels constructed by each strategy are identical.
        ///
        /// 1. This function implements strategy 1, to validate `ManifestLog.open()`.
        /// 2. `ManifestLog.open()` implements strategy 2.
        ///
        /// (Strategy 2 minimizes the number of ManifestLevel mutations.)
        ///
        /// (Invoked immediately after open() or checkpoint()).
        fn verify_tables_recovered(forest: *const Forest) void {
            const ForestTableIteratorType =
                @import("./forest_table_iterator.zig").ForestTableIteratorType;
            const ForestTableIterator = ForestTableIteratorType(Forest);

            assert(forest.grid.superblock.opened);
            assert(forest.manifest_log.opened);

            if (Forest.Storage != @import("../testing/storage.zig").Storage) return;

            // The manifest log is opened, which means we have all of the manifest blocks.
            // But if the replica is syncing, those blocks might still be writing (and thus not in
            // the TestStorage when we go to retrieve them).
            if (forest.grid.superblock.working.vsr_state.sync_op_max > 0) return;

            // The latest version of each table, keyed by table checksum.
            // Null when the table has been deleted.
            var tables_latest = std.AutoHashMap(u128, struct {
                table: schema.ManifestNode.TableInfo,
                manifest_block: u64,
                manifest_entry: u32,
            }).init(forest.grid.superblock.storage.allocator);
            defer tables_latest.deinit();

            // Replay manifest events in chronological order.
            // Accumulate all tables that belong in the recovered forest's ManifestLevels.
            for (0..forest.manifest_log.log_block_checksums.count) |i| {
                const block_checksum = forest.manifest_log.log_block_checksums.get(i).?;
                const block_address = forest.manifest_log.log_block_addresses.get(i).?;
                assert(block_address > 0);

                const block = forest.grid.superblock.storage.grid_block(block_address).?;
                const block_header = schema.header_from_block(block);
                assert(block_header.address == block_address);
                assert(block_header.checksum == block_checksum);
                assert(block_header.block_type == .manifest);

                const block_schema = schema.ManifestNode.from(block);
                assert(block_schema.entry_count > 0);
                assert(block_schema.entry_count <= schema.ManifestNode.entry_count_max);

                for (block_schema.tables_const(block), 0..) |*table, entry| {
                    if (table.label.event == .remove) {
                        maybe(tables_latest.remove(table.checksum));
                    } else {
                        tables_latest.put(table.checksum, .{
                            .table = table.*,
                            .manifest_block = block_address,
                            .manifest_entry = @intCast(entry),
                        }) catch @panic("oom");
                    }
                }

                if (i > 0) {
                    // Verify the linked-list.
                    const block_previous = schema.ManifestNode.previous(block).?;
                    assert(block_previous.checksum ==
                        forest.manifest_log.log_block_checksums.get(i - 1).?);
                    assert(block_previous.address ==
                        forest.manifest_log.log_block_addresses.get(i - 1).?);
                }
            }

            // Verify that the SuperBlock Manifest's table extents are correct.
            var tables_latest_iterator = tables_latest.valueIterator();
            var table_extent_counts: usize = 0;
            while (tables_latest_iterator.next()) |table| {
                const table_extent = forest.manifest_log.table_extents.get(table.table.address).?;
                assert(table.manifest_block == table_extent.block);
                assert(table.manifest_entry == table_extent.entry);

                table_extent_counts += 1;
            }
            assert(table_extent_counts == forest.manifest_log.table_extents.count());

            // Verify the tables in `tables` are exactly the tables recovered by the Forest.
            var forest_tables_iterator = ForestTableIterator{};
            while (forest_tables_iterator.next(forest)) |forest_table_item| {
                const table_latest = tables_latest.get(forest_table_item.checksum).?;
                assert(table_latest.table.label.level == forest_table_item.label.level);
                assert(std.meta.eql(table_latest.table.key_min, forest_table_item.key_min));
                assert(std.meta.eql(table_latest.table.key_max, forest_table_item.key_max));
                assert(table_latest.table.checksum == forest_table_item.checksum);
                assert(table_latest.table.address == forest_table_item.address);
                assert(table_latest.table.snapshot_min == forest_table_item.snapshot_min);
                assert(table_latest.table.snapshot_max == forest_table_item.snapshot_max);
                assert(table_latest.table.tree_id == forest_table_item.tree_id);

                const table_removed = tables_latest.remove(forest_table_item.checksum);
                assert(table_removed);
            }
            assert(tables_latest.count() == 0);
        }
    };
}

fn CompactionScheduleType(comptime Forest: type, comptime Grid: type) type {
    return struct {
        grid: *Grid,
        forest: *Forest,
        pool: ResourcePool,
        next_tick: Grid.NextTick = undefined,
        callback: ?*const fn (*Forest) void = null,

        const CompactionSchedule = @This();
        const ResourcePool = ResourcePoolType(Grid);

        const forest_compactions_concurrent_max =
            stdx.div_ceil(constants.lsm_levels, 2) * Forest.tree_infos.len;
        const forest_block_count_bar_max =
            forest_compactions_concurrent_max * compaction_block_count_bar_max;
        /// The minimum number of blocks required to complete all compactions. This is the sum of
        /// blocks reserved between the bars plus enough blocks to complete a single beat.
        const forest_block_count_min = forest_block_count_bar_max +
            // beat_min includes bar_max for that beat.
            (compaction_block_count_beat_min - compaction_block_count_bar_max);

        pub fn init(
            self: *CompactionSchedule,
            allocator: mem.Allocator,
            grid: *Grid,
            forest: *Forest,
            block_count: u32,
        ) !void {
            assert(block_count >= CompactionSchedule.forest_block_count_min);

            self.* = .{ .grid = grid, .forest = forest, .pool = undefined };
            self.pool = try ResourcePool.init(allocator, block_count);
            errdefer self.pool.deinit(allocator);
        }

        pub fn deinit(self: *CompactionSchedule, allocator: mem.Allocator) void {
            self.pool.deinit(allocator);
        }

        pub fn reset(self: *CompactionSchedule) void {
            self.pool.reset();

            self.* = .{
                .grid = self.grid,
                .forest = self.forest,
                .pool = self.pool,
            };
        }

        pub fn beat_start(
            self: *CompactionSchedule,
            callback: Forest.Callback,
            op: u64,
        ) void {
            assert(self.pool.idle());
            assert(self.callback == null);
            self.callback = callback;

            if (op < constants.lsm_compaction_ops or
                self.grid.superblock.working.vsr_state.op_compacted(op))
            {
                self.beat_finish();
                return;
            }

            const half_bar = @divExact(constants.lsm_compaction_ops, 2);
            const compaction_beat = op % constants.lsm_compaction_ops;

            const first_beat = compaction_beat == 0;
            const half_beat = compaction_beat == half_bar;

            const last_beat = compaction_beat == constants.lsm_compaction_ops - 1;
            const last_half_beat = compaction_beat == half_bar - 1;

            if (first_beat or half_beat) assert(self.pool.blocks_acquired() == 0);

            if (first_beat or half_beat) {
                for (0..constants.lsm_levels) |level_b| {
                    if (level_active(.{ .level_b = level_b, .op = op })) {
                        inline for (comptime std.enums.values(Forest.TreeID)) |tree_id| {
                            self.compaction_at(level_b, tree_id)
                                .bar_commence(op);
                        }
                    }
                }
            }

            for (0..constants.lsm_levels) |level_b| {
                if (level_active(.{ .level_b = level_b, .op = op })) {
                    inline for (comptime std.enums.values(Forest.TreeID)) |tree_id| {
                        const beats_total = half_bar;
                        const beats_done = compaction_beat % half_bar;
                        const beats_remaining = beats_total - beats_done;
                        self.compaction_at(level_b, tree_id)
                            .beat_commence(beats_remaining);
                        if (last_beat or last_half_beat) {
                            const quotas = self.compaction_at(level_b, tree_id).quotas;
                            assert(beats_remaining == 1);
                            assert(quotas.beat == quotas.bar);
                        }
                    }
                }
            }

            self.beat_resume();
        }

        fn beat_resume(self: *CompactionSchedule) void {
            assert(self.callback != null);

            const op = self.forest.progress.?.compact.op;
            for (0..constants.lsm_levels) |level_b| {
                if (level_active(.{ .level_b = level_b, .op = op })) {
                    inline for (comptime std.enums.values(Forest.TreeID)) |tree_id| {
                        const resumed = self.compaction_at(level_b, tree_id)
                            .compaction_dispatch_enter(&self.pool, beat_resume_callback);

                        switch (resumed) {
                            .beat_finished => {},
                            .active => return,
                        }
                    }
                }
            }
            self.beat_finish();
        }

        fn beat_resume_callback(pool: *ResourcePool) void {
            const self: *CompactionSchedule = @fieldParentPtr("pool", pool);
            assert(self.callback != null);
            self.beat_resume();
        }

        fn beat_finish(self: *CompactionSchedule) void {
            assert(self.callback != null);
            self.grid.on_next_tick(beat_finish_next_tick, &self.next_tick);
        }

        fn beat_finish_next_tick(next_tick: *Grid.NextTick) void {
            const self: *CompactionSchedule = @alignCast(
                @fieldParentPtr("next_tick", next_tick),
            );
            const callback = self.callback.?;
            self.callback = null;
            callback(self.forest);
        }

        fn compaction_at(
            self: *CompactionSchedule,
            level_b: usize,
            comptime tree_id: Forest.TreeID,
        ) *Forest.TreeForIdType(tree_id).Compaction {
            return &self.forest.tree_for_id(tree_id).compactions[level_b];
        }
    };
}

fn level_active(options: struct { level_b: usize, op: u64 }) bool {
    const half_bar_beat_count = @divExact(constants.lsm_compaction_ops, 2);
    const compaction_beat = options.op % constants.lsm_compaction_ops;
    return (compaction_beat < half_bar_beat_count) == (options.level_b % 2 == 1);
}

test level_active {
    assert(!level_active(.{ .level_b = 0, .op = constants.lsm_compaction_ops }));
    assert(level_active(.{ .level_b = 1, .op = constants.lsm_compaction_ops }));
    assert(!level_active(.{ .level_b = 2, .op = constants.lsm_compaction_ops }));

    assert(level_active(.{ .level_b = 0, .op = @divExact(constants.lsm_compaction_ops, 2) }));
    assert(!level_active(.{ .level_b = 1, .op = @divExact(constants.lsm_compaction_ops, 2) }));
    assert(level_active(.{ .level_b = 2, .op = @divExact(constants.lsm_compaction_ops, 2) }));
}
