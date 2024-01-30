//! Compaction moves or merges a table's values from the previous level.
//!
//! Each Compaction is paced to run in an arbitrary amount of beats, by the forest.
//!
//!
//! Compaction overview:
//!
//! 1. Given:
//!
//!   - levels A and B, where A+1=B
//!   - a single table in level A ("table A")
//!   - all tables from level B which intersect table A's key range ("tables B")
//!     (This can include anything between 0 tables and all of level B's tables.)
//!
//! 2. If table A's key range is disjoint from the keys in level B, move table A into level B.
//!    All done! (But if the key ranges intersect, jump to step 3).
//! FIXME: Update
//! 3. Create an iterator from the sort-merge of table A and the concatenation of tables B.
//!    If the same key exists in level A and B, take A's and discard B's. †
//!
//! 4. Write the sort-merge iterator into a sequence of new tables on disk.
//!
//! 5. Update the input tables in the Manifest with their new `snapshot_max` so that they become
//!    invisible to subsequent read transactions.
//!
//! 6. Insert the new level-B tables into the Manifest.
//!
//! † When A's value is a tombstone, there is a special case for garbage collection. When either:
//! * level B is the final level, or
//! * A's key does not exist in B or any deeper level,
//! then the tombstone is omitted from the compacted output, see: `compaction_must_drop_tombstones`.
//!
const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const log = std.log.scoped(.compaction);
const tracer = @import("../tracer.zig");

const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const GridType = @import("../vsr/grid.zig").GridType;
const BlockPtr = @import("../vsr/grid.zig").BlockPtr;
const BlockPtrConst = @import("../vsr/grid.zig").BlockPtrConst;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const TableInfoType = @import("manifest.zig").TreeTableInfoType;
const ManifestType = @import("manifest.zig").ManifestType;
const schema = @import("schema.zig");

/// The upper-bound count of input tables to a single tree's compaction.
///
/// - +1 from level A.
/// - +lsm_growth_factor from level B. The A-input table cannot overlap with an extra B-input table
///   because input table selection is least-overlap. If the input table overlaps on one or both
///   edges, there must be another table with less overlap to select.
pub const compaction_tables_input_max = 1 + constants.lsm_growth_factor;

/// The upper-bound count of output tables from a single tree's compaction.
/// In the "worst" case, no keys are overwritten/merged, and no tombstones are dropped.
pub const compaction_tables_output_max = compaction_tables_input_max;

/// Information used when scheduling compactions. Kept unspecalized to make the forest
/// code easier.
pub const CompactionInfo = struct {
    /// How many values, across all tables, need to be processed.
    compaction_tables_value_count: usize,

    // Keys are integers in TigerBeetle, with a maximum size of u128. Store these
    // here, instead of Key, to keep this unspecalized.
    target_key_min: u128,
    target_key_max: u128,
};

// FIXME: Change input / output to source / target
pub const CompactionBlocks = struct {
    /// Index blocks are global, and shared between blips.
    /// FIXME: This complicates things somewhat.
    source_index_blocks: []BlockPtr,

    /// For each split, for each input level, we have a buffer of blocks.
    source_value_blocks: [2][2][]BlockPtr,

    /// For each split we have a buffer of blocks.
    target_value_blocks: [2][]BlockPtr,

    // Used to reset source_value_blocks and target_value_blocks when we change their length.
    source_value_blocks_max: [2][2]usize,
    target_value_blocks_max: [2]usize,
};

const BlipCallback = *const fn (*anyopaque, ?bool, ?bool) void;
pub const BlipStage = enum { read, merge, write };

pub fn CompactionInterfaceType(comptime Grid: type, comptime tree_infos: anytype) type {
    return struct {
        const Dispatcher = T: {
            var type_info = @typeInfo(union(enum) {});

            // Union fields for each compaction type.
            for (tree_infos) |tree_info| {
                const Compaction = tree_info.Tree.Compaction;
                const type_name = @typeName(Compaction);

                for (type_info.Union.fields) |field| {
                    if (std.mem.eql(u8, field.name, type_name)) {
                        break;
                    }
                } else {
                    type_info.Union.fields = type_info.Union.fields ++ [_]std.builtin.Type.UnionField{.{
                        .name = type_name,
                        .type = *Compaction,
                        .alignment = @alignOf(*Compaction),
                    }};
                }
            }

            // We need a tagged union for dynamic dispatching.
            type_info.Union.tag_type = blk: {
                const union_fields = type_info.Union.fields;
                var tag_fields: [union_fields.len]std.builtin.Type.EnumField =
                    undefined;
                for (&tag_fields, union_fields, 0..) |*tag_field, union_field, i| {
                    tag_field.* = .{
                        .name = union_field.name,
                        .value = i,
                    };
                }

                break :blk @Type(.{ .Enum = .{
                    .tag_type = std.math.IntFittingRange(0, tag_fields.len - 1),
                    .fields = &tag_fields,
                    .decls = &.{},
                    .is_exhaustive = true,
                } });
            };

            break :T @Type(type_info);
        };

        const Self = @This();

        dispatcher: Dispatcher,
        info: CompactionInfo,

        pub fn init(info: CompactionInfo, compaction: anytype) @This() {
            const Compaction = @TypeOf(compaction.*);
            const type_name = @typeName(Compaction);

            return .{
                .info = info,
                .dispatcher = @unionInit(Dispatcher, type_name, compaction),
            };
        }

        pub fn bar_setup_budget(self: *const Self, beats_max: u64, output_index_blocks: [2][]BlockPtr) void {
            return switch (self.dispatcher) {
                inline else => |compaction_impl| compaction_impl.bar_setup_budget(beats_max, output_index_blocks),
            };
        }

        pub fn beat_grid_reserve(self: *Self) void {
            return switch (self.dispatcher) {
                inline else => |compaction_impl| compaction_impl.beat_grid_reserve(),
            };
        }

        pub fn beat_blocks_assign(self: *Self, blocks: CompactionBlocks, grid_reads: []Grid.FatRead, grid_writes: []Grid.FatWrite) void {
            return switch (self.dispatcher) {
                inline else => |compaction_impl| compaction_impl.beat_blocks_assign(blocks, grid_reads, grid_writes),
            };
        }

        // FIXME: Very unhappy with the callback style here!
        pub fn blip_read(self: *Self, callback: BlipCallback) void {
            return switch (self.dispatcher) {
                inline else => |compaction_impl| compaction_impl.blip_read(callback, self),
            };
        }

        pub fn fixup_buffers(self: *Self) void {
            return switch (self.dispatcher) {
                inline else => |compaction_impl| compaction_impl.fixup_buffers(),
            };
        }

        pub fn undo_blip_read(self: *Self) void {
            return switch (self.dispatcher) {
                inline else => |compaction_impl| compaction_impl.undo_blip_read(),
            };
        }

        pub fn blip_merge(self: *Self, callback: BlipCallback) void {
            return switch (self.dispatcher) {
                inline else => |compaction_impl| compaction_impl.blip_merge(callback, self),
            };
        }

        pub fn blip_write(self: *Self, callback: BlipCallback) void {
            return switch (self.dispatcher) {
                inline else => |compaction_impl| compaction_impl.blip_write(callback, self),
            };
        }

        pub fn beat_grid_forfeit(self: *Self) void {
            return switch (self.dispatcher) {
                inline else => |compaction_impl| compaction_impl.beat_grid_forfeit(),
            };
        }
    };
}

pub fn CompactionType(
    comptime Table: type,
    comptime Tree: type,
    comptime Storage: type,
) type {
    return struct {
        const Compaction = @This();

        const Grid = GridType(Storage);
        pub const Tree_ = Tree;

        const Manifest = ManifestType(Table, Storage);
        const TableInfo = TableInfoType(Table);
        const TableInfoReference = Manifest.TableInfoReference;
        const CompactionRange = Manifest.CompactionRange;

        const Key = Table.Key;
        const Value = Table.Value;
        const key_from_value = Table.key_from_value;
        const tombstone = Table.tombstone;

        const TableInfoA = union(enum) {
            immutable: *Tree.TableMemory,
            disk: TableInfoReference,
        };

        const ValueBlocksIterator = struct {
            blocks: []BlockPtrConst,
            source_index: usize = 0,

            active_block_values: []const Value,
            active_block_index: usize = 0,

            global_position: usize = 0,
            global_count: usize,

            fn init(blocks: []BlockPtrConst, source_index: usize) ValueBlocksIterator {
                var global_count: usize = 0;
                for (blocks) |block| {
                    global_count += Table.data_block_values_used(block).len;
                }

                return .{
                    .blocks = blocks,
                    .source_index = source_index,
                    .active_block_values = if (blocks.len > 0) Table.data_block_values_used(blocks[0]) else &.{},
                    .global_count = global_count,
                };
            }

            fn increment_block(self: *ValueBlocksIterator) ?void {
                if (self.active_block_index == self.blocks.len) return null;
                self.active_block_index += 1;
                if (self.active_block_index == self.blocks.len) return null;

                self.active_block_values = Table.data_block_values_used(self.blocks[self.active_block_index]);
                self.source_index = 0;
            }

            pub fn next(self: *ValueBlocksIterator) ?Table.Value {
                if (self.source_index == self.active_block_values.len) {
                    if (self.increment_block() == null) {
                        return null;
                    }
                }

                const val = self.active_block_values[self.source_index];
                self.source_index += 1;
                self.global_position += 1;

                return val;
            }

            pub fn peek(self: *ValueBlocksIterator) ?Table.Value {
                const old_state = self.*;

                const next_val = self.next();
                self.* = old_state;

                return next_val;
            }

            pub fn remaining(self: *const ValueBlocksIterator) usize {
                return self.global_count - self.global_position;
            }

            pub fn copy(self: *ValueBlocksIterator, table_builder: *Table.Builder) void {
                _ = table_builder;
                _ = self;
                // TODO
                assert(false);
                //     log.info("blip_merge: Merging via ValueBlocksIterator.copy()", .{});
                //     assert(table_builder.value_count < Table.layout.block_value_count_max);

                //     const values_in = self.values[self.source_index..];
                //     const values_out = table_builder.data_block_values();

                //     var values_out_index = table_builder.value_count;

                //     assert(values_in.len > 0);

                //     const len = @min(values_in.len, values_out.len - values_out_index);
                //     assert(len > 0);
                //     stdx.copy_disjoint(
                //         .exact,
                //         Value,
                //         values_out[values_out_index..][0..len],
                //         values_in[0..len],
                //     );

                //     self.source_index += len;
                //     table_builder.value_count += @as(u32, @intCast(len));
            }
        };

        const UnifiedIterator = struct {
            const Dispatcher = union(enum) {
                value_block_iterator: ValueBlocksIterator,
                table_memory_iterator: Tree.TableMemory.Iterator,
            };

            dispatcher: Dispatcher,

            // FIXME: Do we want this iterator to return ptrs rather?
            pub fn next(self: *UnifiedIterator) ?Table.Value {
                return switch (self.dispatcher) {
                    inline else => |*iterator_impl| iterator_impl.next(),
                };
            }

            pub fn peek(self: *UnifiedIterator) ?Table.Value {
                return switch (self.dispatcher) {
                    inline else => |*iterator_impl| iterator_impl.peek(),
                };
            }

            pub fn remaining(self: *const UnifiedIterator) usize {
                return switch (self.dispatcher) {
                    inline else => |iterator_impl| iterator_impl.remaining(),
                };
            }

            pub fn get_source_index(self: *const UnifiedIterator) usize {
                return switch (self.dispatcher) {
                    inline else => |iterator_impl| iterator_impl.source_index,
                };
            }

            pub fn get_block_index(self: *const UnifiedIterator) usize {
                return switch (self.dispatcher) {
                    inline else => |iterator_impl| iterator_impl.active_block_index,
                };
            }

            /// We allow each underlying iterator type to implement its own optimized copy method.
            pub fn copy(self: *UnifiedIterator, table_builder: *Table.Builder) void {
                return switch (self.dispatcher) {
                    inline else => |*iterator_impl| iterator_impl.copy(table_builder),
                };
            }
        };

        const ValuesIn = struct {
            UnifiedIterator,
            UnifiedIterator,
        };

        const Bar = struct {
            tree: *Tree,

            /// `op_min` is the first op/beat of this compaction's half-bar.
            /// `op_min` is used as a snapshot — the compaction's input tables must be visible
            /// to `op_min`.
            ///
            /// After this compaction finishes:
            /// - `op_min + half_bar_beat_count - 1` will be the input tables' snapshot_max.
            /// - `op_min + half_bar_beat_count` will be the output tables' snapshot_min.
            op_min: u64,

            /// Whether this compaction will use the move-table optimization.
            /// Specifically, this field is set to True if the optimal compaction
            /// table in level A can simply be moved to level B.
            move_table: bool,

            table_info_a: TableInfoA,
            range_b: CompactionRange,

            /// Levels may choose to drop tombstones if keys aren't included in the lower levels.
            /// This invariant is always true for the last level as it doesn't have any lower ones.
            drop_tombstones: bool,

            /// Number of beats we should aim to finish this compaction in. It might be fewer, but it'll
            /// never be more.
            beats_max: ?u64,
            compaction_tables_value_count: u64,
            per_beat_input_goal: u64 = 0,

            /// The total number of input values processed by this compaction across the bar. Must equal
            /// compaction_tables_value_count by the bar_finish.
            input_values_processed: u64 = 0,

            // We have to track all of our input positions across bars. This breaks down to:
            // * The current index block for table b (table a always has 1 at most).
            // * The current value block position, _within_ that index block, for both tables.
            // * The current value _within_ that value block, for both tables.
            //
            // This allows us to resume reads from the correct place across beats. There's an
            // additional trick we do when we enter blip_merge():
            // There's no guarantee that all values will be exhausted, so we move our blocks around
            current_table_b_index_block_index: usize = 0,

            current_table_a_value_block_index: usize = 0,
            current_table_b_value_block_index: usize = 0,

            previous_table_b_index_block_index: usize = 0,

            previous_table_a_value_block_index: usize = 0,
            previous_table_b_value_block_index: usize = 0,

            // These are only touched by blip_merge
            current_block_a_index: usize = 0,
            current_block_b_index: usize = 0,
            iterator_block_b_position: usize = 0,

            /// At least 2 output index blocks needs to span beat boundaries, otherwise it wouldn't be
            /// possible to pace at a more granular level than tables. We need 2 because of our pipeline
            /// split.
            output_index_blocks: [2][]BlockPtr,
            output_index_block_split: usize = 0,
            output_index_block: usize = 0, // FIXME: assert less than len above in places

            /// Manifest log appends are queued up until `finish()` is explicitly called to ensure
            /// they are applied deterministically relative to other concurrent compactions.
            manifest_entries: stdx.BoundedArray(struct {
                operation: enum {
                    // FIXME: Perhaps MSB ordering (level_b_insert) etc
                    insert_to_level_b,
                    move_to_level_b,
                },
                table: TableInfo,
            }, manifest_entries_max: {
                // Worst-case manifest updates:
                // See docs/internals/lsm.md "Compaction Table Overlap" for more detail.
                var count = 0;

                count += constants.lsm_growth_factor + 1; // Insert the output tables to level B.
                // (In the move-table case, only a single TableInfo is inserted, and none are updated.)
                break :manifest_entries_max count;
            }) = .{},

            table_builder: Table.Builder = .{},

            // Keep track of the last key from table a and b respectively. Used to assert we are
            // processing data in sequential order and not missing anything.
            // last_blip_merge_keys: [2]?Key = .{ null, null },
        };

        const Beat = struct {
            const Read = struct {
                callback: BlipCallback,
                ptr: *anyopaque,

                pending_reads_index: usize = 0,
                pending_reads_data: usize = 0,

                next_tick: Grid.NextTick = undefined,
                timer: std.time.Timer,
                timer_read: usize = 0,
            };
            const Merge = struct {
                callback: BlipCallback,
                ptr: *anyopaque,

                // These are blip local state. The value of target_value_block_index is copied out
                // at the end of the merge (by reslicing the blocks), so that the upcoming
                // blip_write knows what to write.
                target_value_block_index: usize = 0,

                next_tick: Grid.NextTick = undefined,
                timer: std.time.Timer,
            };
            const Write = struct {
                callback: BlipCallback,
                ptr: *anyopaque,

                pending_writes: usize = 0,

                next_tick: Grid.NextTick = undefined,
                timer: std.time.Timer,
            };

            grid_reservation: ?Grid.Reservation,

            // FIXME: undefined!
            blocks: CompactionBlocks = undefined,

            grid_reads: []Grid.FatRead,
            grid_writes: []Grid.FatWrite,

            input_values_processed: u64 = 0,

            // Unlike other places where we can use a single state enum, a single Compaction
            // instance is _expected_ to be reading, writing and merging all at once. These
            // are not disjoint states!
            //
            // {read,merge,write} are considered inactive if their context is null.
            read: ?Read = null,
            read_split: usize = 0,

            merge: ?Merge = null,
            merge_split: usize = 0,

            write: ?Write = null,
            write_split: usize = 0,

            fn activate_and_assert(self: *Beat, stage: BlipStage, callback: BlipCallback, ptr: *anyopaque) void {
                switch (stage) {
                    .read => {
                        assert(self.read == null);
                        assert(self.merge == null or self.merge_split != self.read_split);
                        assert(self.write == null or self.read_split == self.write_split);

                        self.read = .{
                            .callback = callback,
                            .ptr = ptr,
                            .timer = std.time.Timer.start() catch unreachable,
                        };
                        self.read.?.timer.reset();
                    },
                    .merge => {
                        assert(self.merge == null);
                        assert(self.read == null or self.merge_split != self.read_split);
                        assert(self.write == null or self.merge_split != self.write_split);

                        self.merge = .{
                            .callback = callback,
                            .ptr = ptr,
                            .timer = std.time.Timer.start() catch unreachable,
                        };
                        self.merge.?.timer.reset();
                    },
                    .write => {
                        assert(self.write == null);
                        assert(self.merge == null or self.merge_split != self.write_split);
                        assert(self.read == null or self.read_split == self.write_split);

                        self.write = .{
                            .callback = callback,
                            .ptr = ptr,
                            .timer = std.time.Timer.start() catch unreachable,
                        };
                        self.write.?.timer.reset();
                    },
                }
            }

            fn deactivate_and_assert_and_callback(self: *Beat, stage: BlipStage, arg1: ?bool, arg2: ?bool) void {
                switch (stage) {
                    .read => {
                        assert(self.read != null);
                        assert(self.read.?.pending_reads_index == 0);
                        assert(self.read.?.pending_reads_data == 0);

                        const callback = self.read.?.callback;
                        const ptr = self.read.?.ptr;

                        self.read_split = (self.read_split + 1) % 2;
                        self.read = null;

                        callback(ptr, arg1, arg2);
                    },
                    .merge => {
                        assert(self.merge != null);

                        const callback = self.merge.?.callback;
                        const ptr = self.merge.?.ptr;

                        self.merge_split = (self.merge_split + 1) % 2;
                        self.merge = null;

                        callback(ptr, arg1, arg2);
                    },
                    .write => {
                        assert(self.write != null);
                        assert(self.write.?.pending_writes == 0);

                        const callback = self.write.?.callback;
                        const ptr = self.write.?.ptr;

                        self.write_split = (self.write_split + 1) % 2;
                        self.write = null;

                        callback(ptr, arg1, arg2);
                    },
                }
            }

            fn assert_all_inactive(self: *Beat) void {
                assert(self.read == null);
                assert(self.merge == null);
                assert(self.write == null);
            }
        };

        // Passed by `init`.
        tree_config: Tree.Config,
        level_b: u8,
        grid: *Grid,

        // Populated by {bar,beat}_setup.
        bar: ?Bar,
        beat: ?Beat,

        pub fn init(tree_config: Tree.Config, grid: *Grid, level_b: u8) !Compaction {
            assert(level_b < constants.lsm_levels);

            return Compaction{
                .tree_config = tree_config,
                .grid = grid,
                .level_b = level_b,

                .bar = null,
                .beat = null,
            };
        }

        pub fn deinit(compaction: *Compaction) void {
            _ = compaction;
        }

        pub fn reset(compaction: *Compaction) void {
            // FIXME: Ensure blocks are released... Also what if bar is null.
            compaction.bar.?.table_builder.reset();

            compaction.* = .{
                .tree_config = compaction.tree_config,
                .grid = compaction.grid,
                .level_b = compaction.level_b,

                .bar = null,
                .beat = null,
            };
        }

        /// Perform the bar-wise setup, and returns the compaction work that needs to be done for
        /// scheduling decisions. Returns null if there's no compaction work, or if move_table
        /// is happening (since it only touches the manifest).
        pub fn bar_setup(compaction: *Compaction, tree: *Tree, op: u64) ?CompactionInfo {
            assert(compaction.bar == null);
            assert(compaction.beat == null);

            var compaction_tables_value_count: usize = 0;

            // level_b 0 is special; unlike all the others which have level_a on disk, level 0's
            // level_a comes from the immutable table. This means that blip_read will be a partial,
            // no-op, and that the minimum input blocks are lowered by one.
            // TODO: Actually make use of the above information.
            if (compaction.level_b == 0) {
                // Do not start compaction if the immutable table does not require compaction.
                if (tree.table_immutable.mutability.immutable.flushed) {
                    return null;
                }

                const values = tree.table_immutable.values_used();
                const values_count = values.len;
                assert(values_count > 0);

                const range_b = tree.manifest.immutable_table_compaction_range(
                    tree.table_immutable.key_min(),
                    tree.table_immutable.key_max(),
                );

                // +1 to count the immutable table (level A).
                assert(range_b.tables.count() + 1 <= compaction_tables_input_max);
                assert(range_b.key_min <= tree.table_immutable.key_min());
                assert(tree.table_immutable.key_max() <= range_b.key_max);

                log.info("{s}: compacting immutable table to level 0 " ++
                    "(snapshot_min={d} compaction.op_min={d} table_count={d} values={d})", .{
                    tree.config.name,
                    tree.table_immutable.mutability.immutable.snapshot_min,
                    op,
                    range_b.tables.count() + 1,
                    values_count,
                });

                compaction_tables_value_count += values_count;
                for (range_b.tables.const_slice()) |*table|
                    compaction_tables_value_count += table.table_info.value_count;

                compaction.bar = .{
                    .tree = tree,
                    .op_min = op,

                    .move_table = false,
                    .table_info_a = .{ .immutable = &tree.table_immutable },
                    .range_b = range_b,
                    .drop_tombstones = tree.manifest.compaction_must_drop_tombstones(
                        compaction.level_b,
                        range_b,
                    ),

                    .compaction_tables_value_count = compaction_tables_value_count,

                    // FIXME: Don't like this! How to do it better?
                    .output_index_blocks = undefined,
                    .beats_max = null,
                };
            } else {
                const level_a = compaction.level_b - 1;

                // Do not start compaction if level A does not require compaction.
                const table_range = tree.manifest.compaction_table(level_a) orelse return null;
                const table_a = table_range.table_a.table_info;
                const range_b = table_range.range_b;

                assert(range_b.tables.count() + 1 <= compaction_tables_input_max);
                assert(table_a.key_min <= table_a.key_max);
                assert(range_b.key_min <= table_a.key_min);
                assert(table_a.key_max <= range_b.key_max);

                log.info("{s}: compacting {d} tables from level {d} to level {d}", .{
                    tree.config.name,
                    range_b.tables.count() + 1,
                    level_a,
                    compaction.level_b,
                });

                compaction_tables_value_count += table_a.value_count;
                for (range_b.tables.const_slice()) |*table|
                    compaction_tables_value_count += table.table_info.value_count;

                const perform_move_table = range_b.tables.empty();

                compaction.bar = .{
                    .tree = tree,
                    .op_min = op,

                    .move_table = perform_move_table,
                    .table_info_a = .{ .disk = table_range.table_a },
                    .range_b = range_b,
                    .drop_tombstones = tree.manifest.compaction_must_drop_tombstones(
                        compaction.level_b,
                        range_b,
                    ),

                    .compaction_tables_value_count = compaction_tables_value_count,

                    // FIXME: Don't like this! How to do it better?
                    .output_index_blocks = undefined,
                    .beats_max = null,
                };

                // Return null if move_table is used. We still need to set our bar state above, as
                // it's what keeps track of the manifest updates to be applied synchronously at
                // the end of the bar.
                if (perform_move_table) {
                    compaction.move_table();
                    return null;
                }
            }

            // The last level must always drop tombstones.
            assert(compaction.bar.?.drop_tombstones or compaction.level_b < constants.lsm_levels - 1);

            return .{
                .compaction_tables_value_count = compaction_tables_value_count,
                .target_key_min = compaction.bar.?.range_b.key_min,
                .target_key_max = compaction.bar.?.range_b.key_max,
            };
        }

        /// Setup the per beat budget, as well as the output index block. Done in a separate step to bar_setup()
        /// since the forest requires information from that step to calculate how it should split the work, and
        /// if there's move table, output_index_blocks must be len 0.
        // Minimum of 1, max lsm_growth_factor+1 of output_index_blocks.
        // FIXME: Distill to set_value_count_per_beat
        /// beats_max is the number of beats that this compaction will have available to do its work.
        /// A compaction may be done before beats_max, if eg tables are mostly empty.
        /// Output index blocks are special, and are allocated at a bar level unlike all the other blocks
        /// which are done at a beat level. This is because while we can ensure we fill a value block, index
        /// blocks are too infrequent (one per table) to divide compaction by.
        pub fn bar_setup_budget(compaction: *Compaction, beats_max: u64, output_index_blocks: [2][]BlockPtr) void {
            assert(beats_max <= constants.lsm_batch_multiple);
            assert(compaction.bar != null);
            assert(compaction.beat == null);

            const bar = &compaction.bar.?;

            assert(bar.per_beat_input_goal == 0);

            // FIXME: Actually move this calc into beat_grid_reserve, and subtract the values we've already done from it.
            // This way we self correct our pacing!
            // FIXME: Naming of per_beat_input_goal: value_count_per_beat
            bar.per_beat_input_goal = stdx.div_ceil(bar.compaction_tables_value_count, beats_max);
            bar.output_index_blocks = output_index_blocks;

            // FIXME: Not a hard requirement...
            assert(bar.output_index_blocks[0].len == bar.output_index_blocks[1].len);

            if (bar.move_table) {
                // FIXME: Asserts here
                // assert(output_index_blocks.len == 0);
                // assert(compaction.bar.?.per_beat_input_goal == 0);
            } else {
                assert(output_index_blocks.len > 0);
                assert(bar.per_beat_input_goal > 0);
            }

            log.info("Set up budget: OI: [0][0]: {*}, [1][0]: {*}", .{ output_index_blocks[0][0], output_index_blocks[1][0] });

            // FIXME: Ok, so this gets set once, but we do an extra value block. What we should do is recalculate this dynamically after each beat, to better spread
            // the work out....
            log.info("Total: {} per beat goal: {}", .{ bar.compaction_tables_value_count, bar.per_beat_input_goal });
        }

        /// Reserve blocks from the grid for this beat's worth of work, in the semi-worst case:
        /// - no tombstones are dropped,
        /// - no values are overwritten,
        /// - but, we know exact input value counts, so table fullness *is* accounted for.
        ///
        /// We must reserve before doing any async work so that the block acquisition order
        /// is deterministic (relative to other concurrent compactions).
        pub fn beat_grid_reserve(
            compaction: *Compaction,
        ) void {
            assert(compaction.bar != null);
            assert(compaction.beat == null);

            const bar = &compaction.bar.?;
            assert(bar.per_beat_input_goal > 0);

            // If we're move_table, only the manifest is being updated, *not* the grid.
            assert(!bar.move_table);

            const value_blocks_per_beat = stdx.div_ceil(
                bar.per_beat_input_goal,
                Table.layout.block_value_count_max,
            );
            // FIXME: What if we have partially filled index block from previous beat, that we now fill - do we need a +1?
            const index_blocks_per_beat = stdx.div_ceil(
                value_blocks_per_beat,
                Table.data_block_count_max,
            );
            const total_blocks_per_beat = index_blocks_per_beat + value_blocks_per_beat;

            // TODO The replica must stop accepting requests if it runs out of blocks/capacity,
            // rather than panicking here.
            // (actually, we want to still panic but with something nicer like vsr.fail)
            const grid_reservation = compaction.grid.reserve(total_blocks_per_beat).?;

            compaction.beat = .{
                .grid_reservation = grid_reservation,
                .grid_reads = undefined,
                .grid_writes = undefined,
            };
        }

        pub fn beat_blocks_assign(compaction: *Compaction, blocks: CompactionBlocks, grid_reads: []Grid.FatRead, grid_writes: []Grid.FatWrite) void {
            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const beat = &compaction.beat.?;
            const bar = &compaction.bar.?;

            beat.grid_reads = grid_reads;
            beat.grid_writes = grid_writes;
            beat.blocks = blocks;

            // FIXME: Not sure best way to handle this. Blocks are identical for r/w but read / writes arent'.
            assert(
                blocks.source_index_blocks.len + blocks.source_value_blocks[0][0].len + blocks.source_value_blocks[0][1].len + blocks.source_value_blocks[1][0].len + blocks.source_value_blocks[1][1].len <= grid_reads.len,
            );
            assert(bar.output_index_blocks[0].len + bar.output_index_blocks[1].len + blocks.target_value_blocks[0].len + blocks.target_value_blocks[1].len <= grid_writes.len);

            assert(blocks.source_value_blocks[0][0].len == blocks.source_value_blocks_max[0][0]);
            assert(blocks.source_value_blocks[0][1].len == blocks.source_value_blocks_max[0][1]);
            assert(blocks.source_value_blocks[1][0].len == blocks.source_value_blocks_max[1][0]);
            assert(blocks.source_value_blocks[0][1].len == blocks.source_value_blocks_max[0][1]);
            assert(blocks.target_value_blocks[0].len == blocks.target_value_blocks_max[0]);
            assert(blocks.target_value_blocks[1].len == blocks.target_value_blocks_max[1]);
        }

        // Our blip pipeline is 3 stages long, and split into read, merge and write stages. The
        // merge stage has a data dependency on both the read (source) and write (target) stages.
        //
        // Within a single compaction, the pipeline looks something like:
        // --------------------------------------------------
        // | Ra₀    | Ca₀     | Wa₀     | Ra₁ -> B|          |
        // --------------------------------------------------
        // |       | Ra₁     | Ca₁     | Wa₁     |           |
        // --------------------------------------------------
        // |       |        | Ra₀     | Ca₀ → E | Wb₀       |
        // --------------------------------------------------
        //
        // Internally, we have a split counter - the first time blip_read() is called, it works on
        // buffer split 0, the next time on buffer set 1 and this alternates. The same process
        // happens with blip_merge() and blip_write(), which we represent as eg blip_merge(0).
        //
        // TODO: even without a threadpool, we can likely drive better performance by doubling up
        // the stages. The reason for this is that we expect blip_merge() to be quite a bit quicker
        // than blip_write().
        //
        // At the moment, the forest won't pipeline different compactions from other tree-levels
        // together. It _can_ do this, but it requires a bit more thought in how memory is managed.
        //
        // IO work is always submitted to the kernel _before_ entering blip_merge().

        /// Perform read IO to fill our source_index_blocks and source_value_blocks with as many
        /// blocks as we can, given their sizes, and where we are in the amount of work we need to
        /// do this beat.
        pub fn blip_read(compaction: *Compaction, callback: BlipCallback, ptr: *anyopaque) void {
            // FIXME: Is there a point to asserting != null if we use .? later?
            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const bar = &compaction.bar.?;
            _ = bar;
            const beat = &compaction.beat.?;

            beat.activate_and_assert(.read, callback, ptr);

            // FIXME: Verify the value count matches the number of values we actually compact.
            compaction.blip_read_index();
        }

        fn blip_read_index(compaction: *Compaction) void {
            // FIXME: This should only read the index blocks if we need to.
            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;

            assert(beat.read != null);
            const read = &beat.read.?;

            // index_block_a will always point to source_index_blocks[0] (even though if our source is immutable this isn't needed! future optimization)
            // index_block_b will be the index block of the table we're currently merging with. This will start from the left and move to the right
            // (although we might want to be able to reverse that in future...)

            var read_target: usize = 0;
            switch (bar.table_info_a) {
                .disk => |table_ref| {
                    beat.grid_reads[read_target].target = compaction;
                    beat.grid_reads[read_target].hack = read_target;
                    compaction.grid.read_block(
                        .{ .from_local_or_global_storage = blip_read_index_callback },
                        &beat.grid_reads[read_target].read,
                        table_ref.table_info.address,
                        table_ref.table_info.checksum,
                        .{ .cache_read = true, .cache_write = true },
                    );
                    read.pending_reads_index += 1;
                },
                .immutable => {
                    // Immutable values come from the in memory immutable table - no need to do any reads.
                },
            }
            // ALWAYS increment read_target for now.
            read_target += 1;

            // FIXME: This of course assumes infinite buffer space :)
            for (bar.range_b.tables.const_slice()) |table_ref| {
                beat.grid_reads[read_target].target = compaction;
                beat.grid_reads[read_target].hack = read_target;
                compaction.grid.read_block(
                    .{ .from_local_or_global_storage = blip_read_index_callback },
                    &beat.grid_reads[read_target].read,
                    table_ref.table_info.address,
                    table_ref.table_info.checksum,
                    .{ .cache_read = true, .cache_write = true },
                );
                read.pending_reads_index += 1;
                read_target += 1;
            }

            log.info("blip_read({}): Scheduled {} index reads ", .{ beat.read_split, read.pending_reads_index });

            // Either we have pending index reads, in which case blip_read_data gets called by
            // blip_read_index_callback once all reads are done, or we don't, in which case call it
            // here.
            if (read.pending_reads_index == 0) {
                return compaction.blip_read_data();
            }
        }

        fn blip_read_index_callback(grid_read: *Grid.Read, index_block: BlockPtrConst) void {
            const parent = @fieldParentPtr(Grid.FatRead, "read", grid_read);
            const compaction: *Compaction = @alignCast(@ptrCast(parent.target));
            assert(compaction.bar != null);
            assert(compaction.beat != null);
            assert(compaction.tree_config.id == schema.TableIndex.metadata(index_block).tree_id);

            const bar = &compaction.bar.?;
            _ = bar;
            const beat = &compaction.beat.?;

            // FIXME: Figure out where our read target should go in a better way...
            assert(beat.read != null);
            const read = &beat.read.?;

            read.pending_reads_index -= 1;
            read.timer_read += 1;

            const read_index = parent.hack;
            stdx.copy_disjoint(.exact, u8, beat.blocks.source_index_blocks[read_index], index_block);
            log.info("blip_read({}): Copied index block to {}", .{ beat.read_split, read_index });

            // FIXME: Not sure if I like this too much. According to release_table_blocks, it'll only release at the end of the bar, so should be ok?
            // FIXME: It's critical to release blocks, so ensure this is done properly.
            // compaction.release_table_blocks(index_block);

            if (read.pending_reads_index != 0) return;

            compaction.blip_read_data();
        }

        fn blip_read_data(compaction: *Compaction) void {
            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;

            assert(beat.read != null);
            const read = &beat.read.?;

            // Used to select the Grid.Read to use - so shared between table a and table b.
            var read_target: usize = 0;
            var value_blocks_read_a: usize = 0;
            var value_blocks_read_b: usize = 0;

            // Save values before this read, in case we need to discard it.
            bar.previous_table_b_index_block_index = bar.current_table_b_index_block_index;
            bar.previous_table_a_value_block_index = bar.current_table_a_value_block_index;
            bar.previous_table_b_value_block_index = bar.current_table_b_value_block_index;

            // Read data for table a - which we'll only have if we're coming from disk.
            if (bar.table_info_a == .disk) {
                assert(false); // FIXME: Unsupported for now!
                const index_block = beat.blocks.source_index_blocks[0];
                const index_schema = schema.TableIndex.from(index_block);

                const value_blocks_used = index_schema.data_blocks_used(index_block);
                assert(bar.current_table_a_value_block_index < value_blocks_used);

                const value_block_addresses = index_schema.data_addresses_used(index_block);
                const value_block_checksums = index_schema.data_checksums_used(index_block);

                while (value_blocks_read_a < beat.blocks.source_value_blocks[beat.read_split][0].len and value_blocks_read_a < value_blocks_used) {
                    beat.grid_reads[read_target].target = compaction;
                    beat.grid_reads[read_target].hack = read_target;
                    compaction.grid.read_block(
                        .{ .from_local_or_global_storage = blip_read_data_callback },
                        &beat.grid_reads[read_target].read,
                        value_block_addresses[value_blocks_read_a],
                        value_block_checksums[value_blocks_read_a].value,
                        .{ .cache_read = true, .cache_write = true },
                    );

                    read.pending_reads_data += 1;
                    read_target += 1;
                    value_blocks_read_a += 1;
                }
            }

            // Read data for our tables in range b.
            const table_b_count = bar.range_b.tables.count();

            // FIXME: bar.current_table_b_index_block_index is incremented here, which means if we do a blip_read and then throw it away,
            // if it crosses tables it'll be a problem... Ditto for bar.current_table_b_value_block_index
            var previous_schema: ?schema.TableIndex = null;
            outer: while (bar.current_table_b_index_block_index < table_b_count) : (bar.current_table_b_index_block_index += 1) {
                // The 1 + is to skip over the table a index block.
                const index_block = beat.blocks.source_index_blocks[1 + bar.current_table_b_index_block_index];
                const index_schema = schema.TableIndex.from(index_block);
                assert(previous_schema == null or stdx.equal_bytes(schema.TableIndex, &previous_schema.?, &index_schema));
                previous_schema = index_schema;

                const value_blocks_used = index_schema.data_blocks_used(index_block);

                const value_block_addresses = index_schema.data_addresses_used(index_block);
                const value_block_checksums = index_schema.data_checksums_used(index_block);

                std.log.info(".... this bar.current_table_b_index_block_index({}) has {} used value blocks. We are on {}", .{ bar.current_table_b_index_block_index, value_blocks_used, bar.current_table_b_value_block_index });
                assert(bar.current_table_b_value_block_index < value_blocks_used);
                // Try read in as many value blocks as this index block has...
                while (bar.current_table_b_value_block_index < value_blocks_used) {
                    beat.grid_reads[read_target].target = compaction;
                    beat.grid_reads[read_target].hack = read_target;
                    std.log.info("Issuing read for index block {} value block {}", .{ bar.current_table_b_index_block_index, bar.current_table_b_value_block_index });
                    compaction.grid.read_block(
                        .{ .from_local_or_global_storage = blip_read_data_callback },
                        &beat.grid_reads[read_target].read,
                        value_block_addresses[bar.current_table_b_value_block_index],
                        value_block_checksums[bar.current_table_b_value_block_index].value,
                        .{ .cache_read = true, .cache_write = true },
                    );

                    read.pending_reads_data += 1;
                    read_target += 1;
                    value_blocks_read_b += 1;
                    bar.current_table_b_value_block_index += 1;

                    // But, once our read buffer is full, break out of the outer loop.
                    if (value_blocks_read_b == beat.blocks.source_value_blocks[beat.read_split][1].len) {
                        break :outer;
                    }
                }

                // If we're here, it means we're incrementing our active index block. Reset the value block index to 0 too
                bar.current_table_b_value_block_index = 0;
            }

            beat.blocks.source_value_blocks[beat.read_split][0] = beat.blocks.source_value_blocks[beat.read_split][0][0..value_blocks_read_a];
            beat.blocks.source_value_blocks[beat.read_split][1] = beat.blocks.source_value_blocks[beat.read_split][1][0..value_blocks_read_b];

            log.info("blip_read({}): Scheduled {} data reads.", .{ beat.read_split, read.pending_reads_data });

            // Either we have pending data reads, in which case blip_read_next_tick gets called by
            // blip_read_data_callback once all reads are done, or we don't, in which case call it
            // here via next_tick.
            if (read.pending_reads_data == 0) {
                compaction.grid.on_next_tick(blip_read_next_tick, &read.next_tick);
            }
        }

        fn blip_read_data_callback(grid_read: *Grid.Read, value_block: BlockPtrConst) void {
            const parent = @fieldParentPtr(Grid.FatRead, "read", grid_read);
            const compaction: *Compaction = @alignCast(@ptrCast(parent.target));

            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const bar = &compaction.bar.?;
            _ = bar;
            const beat = &compaction.beat.?;

            assert(beat.read != null);
            const read = &beat.read.?;

            read.pending_reads_data -= 1;
            read.timer_read += 1;

            // FIXME: Hack, hard code our reads to table b for now. This also copies the block
            // we should instead steal it...
            const table_a_or_b = 1;
            const read_index = parent.hack;

            // log.info("blip_read({}): copying value block to [{}][{}][{}]", .{ beat.read_split, beat.read_split, table_a_or_b, read_index });
            stdx.copy_disjoint(.exact, u8, beat.blocks.source_value_blocks[beat.read_split][table_a_or_b][read_index], value_block);

            // Join on all outstanding reads before continuing.
            if (read.pending_reads_data != 0) return;

            // Call the next tick handler directly. This callback is invoked async, so it's safe
            // from stack overflows.
            blip_read_next_tick(&read.next_tick);
        }

        fn blip_read_next_tick(next_tick: *Grid.NextTick) void {
            const read: *?Beat.Read = @ptrCast(@fieldParentPtr(Beat.Read, "next_tick", next_tick));
            const beat = @fieldParentPtr(Beat, "read", read);

            const duration = read.*.?.timer.read();
            log.info("blip_read({}): Took {} to read all blocks - {}", .{ beat.read_split, std.fmt.fmtDuration(duration), read.*.?.timer_read });

            beat.deactivate_and_assert_and_callback(.read, null, null);
        }

        pub fn undo_blip_read(compaction: *Compaction) void {
            const bar = &compaction.bar.?;

            std.log.info("current_table_b_index_block_index: was {} reverted to {}", .{ bar.current_table_b_index_block_index, bar.previous_table_b_index_block_index });
            std.log.info("current_table_a_value_block_index: was {} reverted to {}", .{ bar.current_table_a_value_block_index, bar.previous_table_a_value_block_index });
            std.log.info("current_table_b_value_block_index: was {} reverted to {}", .{ bar.current_table_b_value_block_index, bar.previous_table_b_value_block_index });

            bar.current_table_b_index_block_index = bar.previous_table_b_index_block_index;
            bar.current_table_a_value_block_index = bar.previous_table_a_value_block_index;
            bar.current_table_b_value_block_index = bar.previous_table_b_value_block_index;
        }

        /// When we do a blip_read, we assume that the position to start reading from is the end of
        /// the previous read. For example, if blip_read(0) read blocks 0..10, blip_read(1) will
        /// read blocks 10..20.
        ///
        /// However, there's no guarantee that our blip_merge(0) will actually consume all 10
        /// blocks. In fact, it's likely not to, depending on buffer sizing. This presents a
        /// problem for blip_merge(1) which needs to now operate on (nominally) 5..15.
        ///
        /// That's where this function comes in. We rearrange our blocks, so that the upcoming
        /// merge split's input memory is continuous. This does result in reads being thrown
        /// away.
        /// TODO: Track those throw aways as a metric.
        pub fn fixup_buffers(compaction: *Compaction) void {
            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;

            // We get called between blips, on a pipeline boundary. Nothing should be active.
            beat.assert_all_inactive();

            // deactivate_and_assert_and_callback is responsible for incrementing the split, so
            // beat.merge_split will be the next blip_merge that will run.
            const current_split = beat.merge_split;
            const previous_split = (beat.merge_split + 1) % 2;

            std.log.info("Fixing buffers... Our last iterator got to {}", .{bar.iterator_block_b_position});
            std.log.info(".... therefore, we need to make blocks.source_value_blocks[{}][1] start from blocks.source_value_blocks[{}][1][{}..]", .{ current_split, previous_split, bar.iterator_block_b_position });
        }

        fn calculate_values_in(compaction: *Compaction) ValuesIn {
            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;

            assert(beat.merge != null);

            const blocks_a = beat.blocks.source_value_blocks[beat.merge_split][0];
            const blocks_b = beat.blocks.source_value_blocks[beat.merge_split][1];

            std.log.info("blocks_a.len: {}, blocks_b.len: {}", .{ blocks_a.len, blocks_b.len });

            // // Assert that we're reading value blocks in key order.
            // const values_in = compaction.values_in[index];
            // assert(values_in.len > 0);
            // if (constants.verify) {
            //     for (values_in[0 .. values_in.len - 1], values_in[1..]) |*value, *value_next| {
            //         assert(key_from_value(value) < key_from_value(value_next));
            //     }
            // }
            // const first_key = key_from_value(&values_in[0]);
            // const last_key = key_from_value(&values_in[values_in.len - 1]);
            // if (compaction.last_keys_in[index]) |last_key_prev| {
            //     assert(last_key_prev < first_key);
            // }
            // if (values_in.len > 1) {
            //     assert(first_key < last_key);
            // }
            // compaction.last_keys_in[index] = last_key;

            // FIXME: Assert we're not exhausted.
            switch (bar.table_info_a) {
                .immutable => {
                    log.info("blip_merge({}): values_a from immutable and blocks_b from source_value_blocks[{}][{}]", .{ beat.merge_split, beat.merge_split, 1 });
                    return .{
                        UnifiedIterator{ .dispatcher = .{
                            .table_memory_iterator = Tree.TableMemory.Iterator.init(bar.table_info_a.immutable, bar.current_block_a_index),
                        } },
                        UnifiedIterator{ .dispatcher = .{
                            .value_block_iterator = ValueBlocksIterator.init(blocks_b, bar.current_block_b_index),
                        } },
                    };
                },
                .disk => {
                    assert(false); // FIXME: Unimplemented for now.
                    @panic("foo");
                    // const values_a = Table.data_block_values_used(blocks_a[merge.current_block_a]);
                    // return .{
                    //     UnifiedIterator{ .dispatcher = .{
                    //         .value_block_iterator = ValueBlocksIterator.init(values_a, bar.current_block_a_index),
                    //     } },
                    //     UnifiedIterator{ .dispatcher = .{
                    //         .value_block_iterator = ValueBlocksIterator.init(values_b, bar.current_block_b_index),
                    //     } },
                    // };
                },
            }
        }

        /// Perform CPU merge work, to transform our source tables to our target tables.
        /// This has a data dependency on both the read buffers and the write buffers for the
        /// active split.
        ///
        /// blip_merge is also responsible for signalling when to stop blipping entirely. A
        /// sequence of blips is over when one of the following condition are met, considering we
        /// don't want to output partial value blocks unless we really really have to:
        ///
        /// * We have reached our per_beat_input_goal. Finish up the next value block, and we're
        ///   done.
        /// * We have no more source values remaining, at all - the bar is done. This will likely
        ///   result in a partially full value block, but that's OK (end of a table).
        /// * We have no more output value blocks remaining in our buffer - we might need more
        ///   blips, but that's up to the forest to orchestrate.
        /// * We have no more output index blocks remaining in our buffer - we might have a partial
        ///   value block here, but that's OK (end of a table).
        pub fn blip_merge(compaction: *Compaction, callback: BlipCallback, ptr: *anyopaque) void {
            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;

            beat.activate_and_assert(.merge, callback, ptr);
            const merge = &beat.merge.?;

            var input_exhausted_bar = false;
            var input_exhausted_beat = false;

            assert(bar.table_builder.value_count < Table.layout.block_value_count_max);

            var values_in = compaction.calculate_values_in();

            var values_in_a_len = values_in[0].remaining();
            var values_in_b_len = values_in[1].remaining();

            // We are responsible for not iterating again if all work is done.
            assert(values_in_a_len > 0 or values_in_b_len > 0);

            // Loop through the CPU work until we have nothing left.
            while (values_in_a_len > 0 or values_in_b_len > 0) {
                // FIXME: We can store our key here (or even a full value) across blips, to assert we're starting
                // from the correct place each time!
                // const peek_key_a = if (values_in[0].peek()) |value| key_from_value(&value) else null;
                // const peek_key_b = if (values_in[1].peek()) |value| key_from_value(&value) else null;
                // assert(bar.last_blip_merge_keys[0] == null or bar.last_blip_merge_keys[0] == peek_key_a);
                // assert(bar.last_blip_merge_keys[1] == null or bar.last_blip_merge_keys[1] == peek_key_b);

                std.log.info(">>>>>>>>>>>>> Merge a starting on: {?}", .{if (values_in[0].peek()) |*val| key_from_value(val) else null});
                std.log.info(">>>>>>>>>>>>> Merge b starting on: {?}", .{if (values_in[1].peek()) |*val| key_from_value(val) else null});

                log.info("blip_merge({}): values_a: {} and values_b: {}", .{ beat.merge_split, values_in_a_len, values_in_b_len });

                // Set the index block if needed.
                if (bar.table_builder.state == .no_blocks) {
                    const index_block = bar.output_index_blocks[bar.output_index_block_split][bar.output_index_block];
                    log.info("blip_merge({}): Setting target index block to [{}][{}].", .{
                        beat.merge_split,
                        bar.output_index_block_split,
                        bar.output_index_block,
                    });
                    @memset(index_block, 0); // FIXME: We don't need to zero the whole block; just the part of the padding that's not covered by alignment.
                    bar.table_builder.set_index_block(index_block);
                }

                // Set the value block if needed.
                if (bar.table_builder.state == .index_block) {
                    const value_block = beat.blocks.target_value_blocks[beat.merge_split][merge.target_value_block_index];
                    log.info("blip_merge({}): Setting target value block to [{}][{}].", .{
                        beat.merge_split,
                        beat.merge_split,
                        merge.target_value_block_index,
                    });
                    @memset(value_block, 0); // FIXME: We don't need to zero the whole block; just the part of the padding that's not covered by alignment.
                    bar.table_builder.set_data_block(value_block);
                }

                if (values_in_a_len == 0) {
                    values_in[1].copy(&bar.table_builder);
                } else if (values_in_b_len == 0) {
                    if (bar.drop_tombstones) {
                        copy_drop_tombstones(&bar.table_builder, &values_in);
                    } else {
                        values_in[0].copy(&bar.table_builder);
                    }
                } else {
                    merge_values(&bar.table_builder, &values_in, bar.drop_tombstones);
                }

                // FIXME: Update these in a nicer way...
                const values_in_a_len_new = values_in[0].remaining();
                const values_in_b_len_new = values_in[1].remaining();

                // Save our iterator position, if they still had any values left.
                bar.current_block_a_index = if (values_in_a_len_new > 0) values_in[0].get_source_index() else 0;
                bar.current_block_b_index = if (values_in_b_len_new > 0) values_in[1].get_source_index() else 0;

                bar.iterator_block_b_position = values_in[1].get_block_index();
                log.info("blip_merge({}): values_in[1] ended on block: {}", .{ beat.merge_split, values_in[1].get_block_index() });

                std.log.info(">>>>>>>>>>>>> Merge a ending on: {?}", .{if (values_in[0].peek()) |*val| key_from_value(val) else null});
                std.log.info(">>>>>>>>>>>>> Merge b ending on: {?}", .{if (values_in[1].peek()) |*val| key_from_value(val) else null});

                // bar.last_blip_merge_keys[0] = if (values_in[0].peek()) |value| key_from_value(&value) else null;
                // bar.last_blip_merge_keys[1] = if (values_in[1].peek()) |value| key_from_value(&value) else null;

                beat.input_values_processed += (values_in_a_len - values_in_a_len_new) + (values_in_b_len - values_in_b_len_new);
                bar.input_values_processed += (values_in_a_len - values_in_a_len_new) + (values_in_b_len - values_in_b_len_new);
                values_in_a_len = values_in_a_len_new;
                values_in_b_len = values_in_b_len_new;

                // When checking if we're done, there are two things we need to consider:
                // 1. Have we finished our input entirely? If so, we flush what we have - it's
                //    likely to be a partial block but that's OK.
                // 2. Have we reached our per_beat_input_goal? If so, we'll flush at the next
                //    complete value block.
                //
                // This means that we'll potentially overrun our per_beat_input_goal by up to
                // a full value block.
                input_exhausted_bar = values_in_a_len + values_in_b_len == 0;
                input_exhausted_beat = beat.input_values_processed >= bar.per_beat_input_goal;

                log.info("blip_merge({}): merged {} so far, goal {}. (input_exhausted_bar: {}, input_exhausted_beat: {})", .{
                    beat.merge_split,
                    beat.input_values_processed,
                    bar.per_beat_input_goal,
                    input_exhausted_bar,
                    input_exhausted_beat,
                });

                switch (compaction.check_and_finish_blocks(input_exhausted_bar)) {
                    .unfinished_value_block => continue,
                    .finished_value_block => if (input_exhausted_beat) break,
                    .need_write => break,
                }
            }

            // Cap our slice at the number of blocks that have actual data in them.
            beat.blocks.target_value_blocks[beat.merge_split] = beat.blocks.target_value_blocks[beat.merge_split][0..merge.target_value_block_index];

            // FIXME: Check at least one output value.
            // assert(filled <= target.len);
            // if (filled == 0) assert(Table.usage == .secondary_index);

            const d = merge.timer.read();
            log.info("blip_merge({}): Took {} to CPU merge block", .{ beat.merge_split, std.fmt.fmtDuration(d) });

            beat.deactivate_and_assert_and_callback(.merge, input_exhausted_bar or input_exhausted_beat, input_exhausted_bar);
        }

        fn check_and_finish_blocks(compaction: *Compaction, force_flush: bool) enum {
            unfinished_value_block,
            finished_value_block,
            need_write,
        } {
            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;

            assert(beat.merge != null);
            const merge = &beat.merge.?;

            const table_builder = &bar.table_builder;

            var output_blocks_full = false;
            var finished_value_block = false;

            // Flush the value block if needed.
            if (table_builder.data_block_full() or
                table_builder.index_block_full() or
                (force_flush and !table_builder.data_block_empty()))
            {
                table_builder.data_block_finish(.{
                    .cluster = compaction.grid.superblock.working.cluster,
                    .address = compaction.grid.acquire(compaction.beat.?.grid_reservation.?),
                    .snapshot_min = snapshot_min_for_table_output(bar.op_min),
                    .tree_id = compaction.tree_config.id,
                });
                log.info("blip_merge({}): Finished target value block to [{}][{}]", .{
                    beat.merge_split,
                    beat.merge_split,
                    merge.target_value_block_index,
                });

                merge.target_value_block_index += 1;
                finished_value_block = true;
                if (merge.target_value_block_index == beat.blocks.target_value_blocks[beat.merge_split].len or force_flush) {
                    output_blocks_full = true;
                }
            }

            // Flush the index block if needed.
            if (table_builder.index_block_full() or
                // If the input is exhausted then we need to flush all blocks before finishing.
                (force_flush and !table_builder.index_block_empty()))
            {
                const table = table_builder.index_block_finish(.{
                    .cluster = compaction.grid.superblock.working.cluster,
                    .address = compaction.grid.acquire(compaction.beat.?.grid_reservation.?),
                    .snapshot_min = snapshot_min_for_table_output(bar.op_min),
                    .tree_id = compaction.tree_config.id,
                });
                log.info("blip_merge({}): Finished target index block to [{}][{}]", .{
                    beat.merge_split,
                    bar.output_index_block_split,
                    bar.output_index_block,
                });

                bar.output_index_block += 1;
                if (bar.output_index_block == bar.output_index_blocks[bar.output_index_block_split].len or force_flush) {
                    output_blocks_full = true;
                }

                // Make this table visible at the end of this bar.
                bar.manifest_entries.append_assume_capacity(.{
                    .operation = .insert_to_level_b,
                    .table = table,
                });
            }

            if (output_blocks_full) return .need_write;
            if (finished_value_block) return .finished_value_block;
            return .unfinished_value_block;
        }

        /// Perform write IO to write our output_index_blocks and target_value_blocks to disk.
        pub fn blip_write(compaction: *Compaction, callback: BlipCallback, ptr: *anyopaque) void {
            // FIXME: Is there a point to asserting != null if we use .? later?
            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const bar = &compaction.bar.?;
            const beat = &compaction.beat.?;

            beat.activate_and_assert(.write, callback, ptr);

            assert(beat.write != null);
            const write = &beat.write.?;

            log.info("blip_write({}): starting", .{beat.write_split});

            // FIXME: Can just use write.pending_write?
            var writes_issued: usize = 0;

            // Write any complete index blocks.
            for (bar.output_index_blocks[bar.output_index_block_split][0..bar.output_index_block], 0..) |*block, i| {
                log.info("blip_write({}): Issuing an index write for [{}][{}] - {*}", .{ beat.write_split, bar.output_index_block_split, i, block.* });

                beat.grid_writes[writes_issued].target = compaction;
                beat.grid_writes[writes_issued].hack = writes_issued;
                write.pending_writes += 1;
                compaction.grid.create_block(blip_write_callback, &beat.grid_writes[writes_issued].write, block);
                writes_issued += 1;
            }

            // Move our output_index_block_split along since it's bar global state, and reset the count.
            // IFF we've written any index blocks.
            if (writes_issued > 0) {
                bar.output_index_block_split = (bar.output_index_block_split + 1) % 2;
                bar.output_index_block = 0;
            }

            // Write any complete value blocks.
            for (beat.blocks.target_value_blocks[beat.write_split], 0..) |*block, i| {
                log.info("blip_write({}): Issuing a data write for [{}][{}]", .{ beat.write_split, beat.write_split, i });

                beat.grid_writes[writes_issued].target = compaction;
                beat.grid_writes[writes_issued].hack = writes_issued;
                write.pending_writes += 1;
                compaction.grid.create_block(blip_write_callback, &beat.grid_writes[writes_issued].write, block);
                writes_issued += 1;
            }

            const d = write.timer.read();
            log.info("blip_write({}): Took {} to create {} blocks", .{ beat.write_split, std.fmt.fmtDuration(d), writes_issued });
            write.timer.reset();

            // FIXME: From 2023-12-21
            // FIXME: Pace our compaction by input *values* not input blocks. Blocks might be empty, values will be a far better metric.
            // FIXME: Whenever we run and pace compaction, in the one worst case we'll have 9 input tables forming 7 output tables, and the
            // other will be 9 input tables forming 9 output tables. We should assert that we always do this.
            // The other note is that we don't want to hang on to value blocks across beat boundaries, so we'll always end when one is full
            // and not try to be too perfect.
            // FIXME: The big idea is to make compaction pacing explicit and asserted behaviour rather than just an implicit property of the code

            if (write.pending_writes == 0) {
                compaction.grid.on_next_tick(blip_write_next_tick, &write.next_tick);
            }
        }

        fn blip_write_callback(grid_write: *Grid.Write) void {
            const fat_write = @fieldParentPtr(Grid.FatWrite, "write", grid_write);
            const compaction: *Compaction = @alignCast(@ptrCast(fat_write.target));
            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const beat = &compaction.beat.?;

            // const duration = beat.write.?.timer.read();
            // std.log.info("Write complete for {} - timer at {}", .{ fat_write.hack, std.fmt.fmtDuration(duration) });

            assert(beat.write != null);
            const write = &beat.write.?;
            write.pending_writes -= 1;

            // log.info("blip_write_callback for split {}", .{write_split});
            // Join on all outstanding writes before continuing.
            if (write.pending_writes != 0) return;

            // Call the next tick handler directly. This callback is invoked async, so it's safe
            // from stack overflows.
            blip_write_next_tick(&write.next_tick);
        }

        fn blip_write_next_tick(next_tick: *Grid.NextTick) void {
            const write: *?Beat.Write = @ptrCast(
                @fieldParentPtr(Beat.Write, "next_tick", next_tick),
            );
            const beat = @fieldParentPtr(Beat, "write", write);

            const duration = write.*.?.timer.read();
            log.info("blip_write({}): all writes done - took {}.", .{ beat.write_split, std.fmt.fmtDuration(duration) });
            beat.deactivate_and_assert_and_callback(.write, null, null);
        }

        pub fn beat_grid_forfeit(compaction: *Compaction) void {
            // FIXME: Big hack - see beat_end comment
            if (compaction.beat == null) {
                return;
            }

            assert(compaction.bar != null);
            assert(compaction.beat != null);

            const beat = &compaction.beat.?;

            beat.assert_all_inactive();
            assert(compaction.bar.?.table_builder.data_block_empty());
            // assert(compaction.bar.?.table_builder.state == .index_block); // Hmmm

            if (beat.grid_reservation) |grid_reservation| {
                log.info("beat_grid_forfeit: forfeiting... {}", .{grid_reservation});
                compaction.grid.forfeit(grid_reservation);
                // We set the whole beat to null later.
            } else {
                assert(compaction.bar.?.move_table);
            }

            // Our beat is done!
            compaction.beat = null;
        }

        /// FIXME: Describe
        pub fn bar_finish(compaction: *Compaction, op: u64, tree: *Tree) void {
            log.info("bar_finish: running for compaction: {s} into level: {}", .{ compaction.tree_config.name, compaction.level_b });

            // If we're the compaction for immutable -> level 0, we need to swap our mutable / immutable
            // tables too. This needs to happen at the end of the first ever bar, which would normally
            // not have any work to do, so put it before our asserts.
            // FIXME: Do this in a better way
            if (compaction.level_b == 0) {
                // Mark the immutable table as flushed, if we were compacting into level 0.
                // FIXME: This is janky; current_block_a_index will be set to 0 above because
                // if there are no remaining values, it sets it like that.
                if (compaction.bar != null) {
                    assert(compaction.bar.?.current_block_a_index == 0);
                    compaction.bar.?.tree.table_immutable.mutability.immutable.flushed = true;
                }

                // FIXME: Figure out wtf I'm doing with snapshots
                tree.swap_mutable_and_immutable(
                    snapshot_min_for_table_output(op + 1),
                );
            }

            if (compaction.bar == null and op + 1 == constants.lsm_batch_multiple) {
                return;
            }

            // Fixme: hmm
            if (compaction.bar == null) {
                return;
            }

            assert(compaction.beat == null);
            assert(compaction.bar != null);

            const bar = &compaction.bar.?;

            // Assert our input has been fully exhausted.
            assert(bar.input_values_processed == bar.compaction_tables_value_count);
            std.log.info("Processed a total of {} values this bar, out of {}", .{ bar.input_values_processed, bar.compaction_tables_value_count });

            // Each compaction's manifest updates are deferred to the end of the last
            // bar to ensure:
            // - manifest log updates are ordered deterministically relative to one another, and
            // - manifest updates are not visible until after the blocks are all on disk.
            const manifest = &bar.tree.manifest;
            const level_b = compaction.level_b;
            const snapshot_max = snapshot_max_for_table_input(bar.op_min);

            if (bar.move_table) {
                // If no compaction is required, don't update snapshot_max.
            } else {
                // These updates MUST precede insert_table() and move_table() since they use
                // references to modify the ManifestLevel in-place.
                switch (bar.table_info_a) {
                    .immutable => {},
                    .disk => |table_info| {
                        manifest.update_table(level_b - 1, snapshot_max, table_info);
                    },
                }
                for (bar.range_b.tables.const_slice()) |table| {
                    manifest.update_table(level_b, snapshot_max, table);
                }
            }

            for (bar.manifest_entries.slice()) |*entry| {
                switch (entry.operation) {
                    .insert_to_level_b => manifest.insert_table(level_b, &entry.table),
                    .move_to_level_b => manifest.move_table(level_b - 1, level_b, &entry.table),
                }
            }

            // Our bar is done!
            compaction.bar = null;
        }

        /// If we can just move the table, don't bother with merging.
        fn move_table(compaction: *Compaction) void {
            const bar = &compaction.bar.?;
            assert(bar.move_table);

            log.info(
                "{s}: Moving table: level_b={}",
                .{ compaction.tree_config.name, compaction.level_b },
            );

            const snapshot_max = snapshot_max_for_table_input(bar.op_min);
            const table_a = bar.table_info_a.disk.table_info;
            assert(table_a.snapshot_max >= snapshot_max);

            bar.manifest_entries.append_assume_capacity(.{
                .operation = .move_to_level_b,
                .table = table_a.*,
            });
        }

        // TODO: Support for LSM snapshots would require us to only remove blocks
        // that are invisible.
        fn release_table_blocks(compaction: *Compaction, index_block: BlockPtrConst) void {
            // Release the table's block addresses in the Grid as it will be made invisible.
            // This is safe; compaction.index_block_b holds a copy of the index block for a
            // table in Level B. Additionally, compaction.index_block_a holds
            // a copy of the index block for the Level A table being compacted.

            const grid = compaction.grid;
            const index_schema = schema.TableIndex.from(index_block);
            for (index_schema.data_addresses_used(index_block)) |address| grid.release(address);
            grid.release(Table.block_address(index_block));
        }

        // TODO: Add benchmarks for these CPU merge methods. merge_values() is the most general,
        // and could handle both what copy_drop_tombstones() and Iterator.copy() do, but we expect
        // copy() -> copy_drop_tombstones() -> merge_values() in terms of performance.

        // Looking for fn copy()? It exists as a method on either TableMemory.Iterator or
        // ValueBlocksIterator. Potentially, the value block case could be more optimal than
        // looping through an iterator (just a straight memcpy) but this should be benchmarked!

        /// Copy values from table_a to table_b, dropping tombstones as we go.
        fn copy_drop_tombstones(table_builder: *Table.Builder, values_in: *ValuesIn) void {
            log.info("blip_merge: Merging via copy_drop_tombstones()", .{});
            assert(values_in[1].remaining() == 0);
            assert(table_builder.value_count < Table.layout.block_value_count_max);

            // Copy variables locally to ensure a tight loop.
            // TODO: Actually benchmark this.
            var values_in_a = values_in[0];
            const values_out = table_builder.data_block_values();
            var values_out_index = table_builder.value_count;

            // Merge as many values as possible.
            while (values_in_a.next()) |value_a| {
                if (tombstone(&value_a)) {
                    // TODO: What's the impact of this check? We could invert it since Table.usage
                    // is comptime known.
                    assert(Table.usage != .secondary_index);
                    continue;
                }
                values_out[values_out_index] = value_a;
                values_out_index += 1;

                if (values_out_index == values_out.len) break;
            }

            // Copy variables back out.
            values_in[0] = values_in_a;
            table_builder.value_count = values_out_index;
        }

        /// Merge values from table_a and table_b, with table_a taking precedence. Tombstones may
        /// or may not be dropped depending on drop_tombstones.
        fn merge_values(table_builder: *Table.Builder, values_in: *ValuesIn, drop_tombstones: bool) void {
            log.info("blip_merge: Merging via merge_values()", .{});
            assert(values_in[0].remaining() > 0);
            assert(values_in[1].remaining() > 0);
            assert(table_builder.value_count < Table.layout.block_value_count_max);

            // Copy variables locally to ensure a tight loop.
            // TODO: Actually benchmark this.
            var values_in_a = values_in[0];
            var values_in_b = values_in[1];
            const values_out = table_builder.data_block_values();
            var values_out_index = table_builder.value_count;

            var value_a = values_in_a.next();
            var value_b = values_in_b.next();

            // Merge as many values as possible.
            while (values_out_index < values_out.len) {
                if (value_a == null or value_b == null) break;

                switch (std.math.order(key_from_value(&value_a.?), key_from_value(&value_b.?))) {
                    .lt => {
                        if (drop_tombstones and
                            tombstone(&value_a.?))
                        {
                            assert(Table.usage != .secondary_index);
                            continue;
                        }
                        values_out[values_out_index] = value_a.?;
                        values_out_index += 1;

                        value_a = values_in_a.next();
                    },
                    .gt => {
                        values_out[values_out_index] = value_b.?;
                        values_out_index += 1;

                        value_b = values_in_b.next();
                    },
                    .eq => {
                        if (Table.usage == .secondary_index) {
                            // Secondary index optimization --- cancel out put and remove.
                            assert(tombstone(&value_a.?) != tombstone(&value_b.?));
                            continue;
                        } else if (drop_tombstones) {
                            if (tombstone(&value_a.?)) {
                                continue;
                            }
                        }

                        values_out[values_out_index] = value_a.?;
                        values_out_index += 1;

                        value_a = values_in_a.next();
                        value_b = values_in_b.next();
                    },
                }
            }

            // Copy variables back out.
            values_in[0] = values_in_a;
            values_in[1] = values_in_b;
            table_builder.value_count = values_out_index;
        }
    };
}

fn snapshot_max_for_table_input(op_min: u64) u64 {
    return snapshot_min_for_table_output(op_min) - 1;
}

pub fn snapshot_min_for_table_output(op_min: u64) u64 {
    assert(op_min > 0);
    assert(op_min % @divExact(constants.lsm_batch_multiple, 2) == 0);
    return op_min + @divExact(constants.lsm_batch_multiple, 2);
}

/// Returns the first op of the compaction (Compaction.op_min) for a given op/beat.
///
/// After this compaction finishes:
/// - `op_min + half_bar_beat_count - 1` will be the input tables' snapshot_max.
/// - `op_min + half_bar_beat_count` will be the output tables' snapshot_min.
///
/// Each half-bar has a separate op_min (for deriving the output snapshot_min) instead of each full
/// bar because this allows the output tables of the first half-bar's compaction to be prefetched
/// against earlier — hopefully while they are still warm in the cache from being written.
///
///
/// These charts depict the commit/compact ops over a series of
/// commits and compactions (with lsm_batch_multiple=8).
///
/// Legend:
///
///   ┼   full bar (first half-bar start)
///   ┬   half bar (second half-bar start)
///       This is incremented at the end of each compact().
///   .   op is in mutable table (in memory)
///   ,   op is in immutable table (in memory)
///   #   op is on disk
///   ✓   checkpoint() may follow compact()
///
///   0 2 4 6 8 0 2 4 6
///   ┼───┬───┼───┬───┼
///   .       ╷       ╷     init(superblock.commit_min=0)⎤ Compaction is effectively a noop for the
///   ..      ╷       ╷     commit;compact( 1) start/end ⎥ first bar because there are no tables on
///   ...     ╷       ╷     commit;compact( 2) start/end ⎥ disk yet, and no immutable table to
///   ....    ╷       ╷     commit;compact( 3) start/end ⎥ flush.
///   .....   ╷       ╷     commit;compact( 4) start/end ⎥
///   ......  ╷       ╷     commit;compact( 5) start/end ⎥ This applies:
///   ....... ╷       ╷     commit;compact( 6) start/end ⎥ - when the LSM is starting on a freshly
///   ........╷       ╷     commit;compact( 7) start    ⎤⎥   formatted data file, and also
///   ,,,,,,,,.       ╷  ✓         compact( 7)       end⎦⎦ - when the LSM is recovering from a crash
///   ,,,,,,,,.       ╷     commit;compact( 8) start/end     (see below).
///   ,,,,,,,,..      ╷     commit;compact( 9) start/end
///   ,,,,,,,,...     ╷     commit;compact(10) start/end
///   ,,,,,,,,....    ╷     commit;compact(11) start/end
///   ,,,,,,,,.....   ╷     commit;compact(12) start/end
///   ,,,,,,,,......  ╷     commit;compact(13) start/end
///   ,,,,,,,,....... ╷     commit;compact(14) start/end
///   ,,,,,,,,........╷     commit;compact(15) start    ⎤
///   ########,,,,,,,,╷  ✓         compact(15)       end⎦
///   ########,,,,,,,,.     commit;compact(16) start/end
///   ┼───┬───┼───┬───┼
///   0 2 4 6 8 0 2 4 6
///   ┼───┬───┼───┬───┼                                    Recover with a checkpoint taken at op 15.
///   ########        ╷     init(superblock.commit_min=7)  At op 15, ops 8…15 are in memory, so they
///   ########.       ╷     commit        ( 8) start/end ⎤ were dropped by the crash.
///   ########..      ╷     commit        ( 9) start/end ⎥
///   ########...     ╷     commit        (10) start/end ⎥ But compaction is not run for ops 8…15
///   ########....    ╷     commit        (11) start/end ⎥ because it was already performed
///   ########.....   ╷     commit        (12) start/end ⎥ before the checkpoint.
///   ########......  ╷     commit        (13) start/end ⎥
///   ########....... ╷     commit        (14) start/end ⎥ We can begin to compact again at op 16,
///   ########........╷     commit        (15) start    ⎤⎥ because those compactions (if previously
///   ########,,,,,,,,╷  ✓                (15)       end⎦⎦ performed) are not included in the
///   ########,,,,,,,,.     commit;compact(16) start/end   checkpoint.
///   ┼───┬───┼───┬───┼
///   0 2 4 6 8 0 2 4 6
///
/// Notice how in the checkpoint recovery example above, we are careful not to `compact(op)` twice
/// for any op (even if we crash/recover), since that could lead to differences between replicas'
/// storage. The last bar of `commit()`s is always only in memory, so it is safe to repeat.
pub fn compaction_op_min(op: u64) u64 {
    _ = op;
    // return op - op % half_bar_beat_count;
}
