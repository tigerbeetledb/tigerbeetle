//! Compaction moves or merges a table's values from the previous level.
//!
//! Each Compaction is paced to run in an arbitrary amount of beats, by the forest.
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
//!
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

const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const maybe = stdx.maybe;
const vsr = @import("../vsr.zig");
const trace = @import("../trace.zig");
const FIFOType = @import("../fifo.zig").FIFOType;
const IOPSType = @import("../iops.zig").IOPSType;
const GridType = @import("../vsr/grid.zig").GridType;
const BlockPtr = @import("../vsr/grid.zig").BlockPtr;
const BlockPtrConst = @import("../vsr/grid.zig").BlockPtrConst;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const TableInfoType = @import("manifest.zig").TreeTableInfoType;
const ManifestType = @import("manifest.zig").ManifestType;
const schema = @import("schema.zig");
const RingBufferType = stdx.RingBufferType;

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

/// The maximum number of blocks that a compaction is allowed to carry over between the beats.
/// This is just a single block --- the index block of the output tables. The input tables will be
/// re-read in the next beat, and the compaction is paced in such a way as to end the beat only when
/// an output value block is full.
pub const compaction_block_count_bar_max: u32 = 1;

// The minimum number of blocks required for a single beat of a single compaction:
// one index and one value block for two input and one output table.
pub const compaction_block_count_beat_min: u32 = (1 + 1) * 3;

const half_bar_beat_count = @divExact(constants.lsm_compaction_ops, 2);

/// Resources shared by all compactions.
///
/// ResourcePool is a singleton owned by the Forest, but it doesn't depend on Forest type.
pub fn ResourcePoolType(comptime Grid: type) type {
    return struct {
        reads: IOPSType(BlockRead, constants.lsm_compaction_iops_read_max) = .{},
        writes: IOPSType(BlockWrite, constants.lsm_compaction_iops_write_max) = .{},
        cpus: IOPSType(CPU, 1) = .{},
        // Use FIFO instead of IOPS here because blocks are allocated at runtime.
        blocks: FIFOType(Block),
        blocks_backing_storage: []Block,

        const ResourcePool = @This();

        const BlockRead = struct {
            grid_read: Grid.Read,
            block: *Block,
            compaction: *anyopaque,

            fn parent(read: *BlockRead, comptime Compaction: type) *Compaction {
                return @as(*Compaction, @ptrCast(@alignCast(read.compaction)));
            }
        };

        const BlockWrite = struct {
            grid_write: Grid.Write,
            block: *Block,
            compaction: *anyopaque,

            fn parent(write: *BlockWrite, comptime Compaction: type) *Compaction {
                return @as(*Compaction, @ptrCast(@alignCast(write.compaction)));
            }
        };

        /// While we don't currently have a CPU pool, we already treat CPU as a resource, by storing
        /// it in a ring-buffer of length one.
        const CPU = struct {
            next_tick: Grid.NextTick,
            block: *Block,
            compaction: *anyopaque,

            fn parent(cpu: *CPU, comptime Compaction: type) *Compaction {
                return @as(*Compaction, @ptrCast(@alignCast(cpu.compaction)));
            }
        };

        const Block = struct {
            ptr: BlockPtr,
            stage: enum {
                // block is in the resource pool.
                free,

                // Block is owned by a table builder.
                build_index_block,
                build_value_block,

                // Block is in the read queue.
                read_index_block,
                read_index_block_done,
                read_value_block,
                read_value_block_done,

                // Block is in the read queue and is used by merge.
                // Next stage is either free or loops back to read_value_block_done.
                merge,

                // Block is in the write queue. Goes directly to free from here.
                write_value_block,
                write_index_block,
            },
            // The index block is freed immediately after the read for the last value block is
            // submitted so, by the time merge consumes a value block, the index block is gone
            // already. Use this field to mark the spots when the compaction position should advance
            // to the next index block.
            last_block_in_the_table: bool,

            next: ?*Block, // For FIFO.
        };

        pub fn init(allocator: mem.Allocator, block_count: u32) !ResourcePool {
            const blocks_backing_storage = try allocator.alloc(Block, block_count);
            var blocks_allocated: u32 = 0;
            errdefer {
                for (blocks_backing_storage[0..blocks_allocated]) |block| {
                    allocator.free(block.ptr);
                }
                allocator.free(blocks_backing_storage);
            }

            for (blocks_backing_storage) |*block| {
                block.* = .{
                    .ptr = try allocate_block(allocator),
                    .stage = .free,
                    .last_block_in_the_table = false,
                    .next = null,
                };
                blocks_allocated += 1;
            }
            assert(blocks_allocated == block_count);

            var blocks: FIFOType(Block) = .{
                .name = "compaction_blocks",
                .verify_push = false,
            };
            for (blocks_backing_storage) |*block| blocks.push(block);

            return .{
                .blocks = blocks,
                .blocks_backing_storage = blocks_backing_storage,
            };
        }

        pub fn deinit(pool: *ResourcePool, allocator: Allocator) void {
            for (pool.blocks_backing_storage) |block| {
                allocator.free(block.ptr);
            }
            allocator.free(pool.blocks_backing_storage);
        }

        pub fn reset(pool: *ResourcePool) void {
            pool.* = .{
                .blocks = .{
                    .name = "compaction_blocks",
                    .verify_push = false,
                },
                .blocks_backing_storage = pool.blocks_backing_storage,
            };
            for (pool.blocks_backing_storage) |*block| {
                block.* = .{
                    .ptr = block.ptr,
                    .stage = .free,
                    .last_block_in_the_table = false,
                    .next = null,
                };
                pool.blocks.push(block);
            }
        }

        // NB: idle does not check that no blocks are acquired! Although no IO can happen between
        // the beats, it is valid to carry over some blocks.
        pub fn idle(pool: *ResourcePool) bool {
            return pool.reads.executing() == 0 and
                pool.writes.executing() == 0 and
                pool.cpus.executing() == 0;
        }

        pub fn blocks_acquired(pool: *ResourcePool) u32 {
            assert(pool.blocks.count <= pool.blocks_backing_storage.len);
            return @as(u32, @intCast(pool.blocks_backing_storage.len - pool.blocks.count));
        }

        pub fn blocks_free(pool: *ResourcePool) u32 {
            return @as(u32, @intCast(pool.blocks.count));
        }

        fn block_acquire(pool: *@This()) ?*Block {
            const block = pool.blocks.pop() orelse return null;
            assert(block.stage == .free);
            assert(block.next == null);
            assert(!block.last_block_in_the_table);
            return block;
        }

        fn block_release(pool: *@This(), block: *Block) void {
            assert(block.stage == .free);
            assert(block.next == null);
            assert(!block.last_block_in_the_table);
            pool.blocks.push(block);
        }

        pub fn format(
            self: @This(),
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            return writer.print("ResourcePool{{ " ++
                ".reads = {}/{},  .writes = {}/{}, .cpus = {}/{}, .blocks = {}/{} }}", .{
                self.reads.available(),  self.reads.total(),
                self.writes.available(), self.writes.total(),
                self.cpus.available(),   self.cpus.total(),
                self.blocks.count,       self.blocks_backing_storage.len,
            });
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
        const ResourcePool = ResourcePoolType(Grid);

        const Manifest = ManifestType(Table, Storage);
        const TableInfo = TableInfoType(Table);
        const TableInfoReference = Manifest.TableInfoReference;
        const CompactionRange = Manifest.CompactionRange;

        const Value = Table.Value;
        const key_from_value = Table.key_from_value;
        const tombstone = Table.tombstone;

        const TableInfoA = union(enum) {
            immutable: []Value,
            disk: TableInfoReference,
        };

        const Position = struct {
            index_block: u32 = 0,
            value_block: u32 = 0,
            value: u32 = 0,

            pub fn format(
                self: @This(),
                comptime _: []const u8,
                _: std.fmt.FormatOptions,
                writer: anytype,
            ) !void {
                return writer.print("Position{{ .index_block = {}, " ++
                    ".value_block = {}, .value = {} }}", .{
                    self.index_block,
                    self.value_block,
                    self.value,
                });
            }
        };

        // Globally scoped fields:
        // ----------------------
        grid: *Grid,
        tree: *Tree,
        level_b: u8,

        stage: enum {
            inactive,
            beat,
            beat_quota_done,
            beat_table_done,
            paused,
        } = .inactive,

        // Bar-scoped fields:
        // -----------------

        /// `op_min` is the first op/beat of this compaction's half-bar.
        /// `op_min` is used as a snapshot — the compaction's input tables must be visible
        /// to `op_min`.
        ///
        /// After this compaction finishes:
        /// - `op_min + half_bar_beat_count - 1` will be the input tables' snapshot_max.
        /// - `op_min + half_bar_beat_count` will be the output tables' snapshot_min.
        op_min: u64 = 0,

        table_info_a: ?TableInfoA = null,
        range_b: ?CompactionRange = null,

        /// Whether this compaction will use the move-table optimization.
        /// Specifically, this field is set to True if the optimal compaction
        /// table in level A can simply be moved to level B.
        move_table: bool = false,
        /// Levels may choose to drop tombstones if keys aren't included in the lower levels.
        /// This invariant is always true for the last level as it doesn't have any lower ones.
        drop_tombstones: bool = false,

        /// Counters track physical IO and are not fully deterministic. In particular, `in` and
        /// `dropped` values can vary between the replicas.
        ///
        /// Counters obey accounting equation of compaction:
        ///     out = in - (dropped + wasted)
        counters: struct {
            in: u64 = 0,
            dropped: u64 = 0, // Tombstones.
            // Counts values needlessly read from disk. Wastage can happen both at the end and at
            // the start of a beat (when part of the value block was already merged previously).
            wasted: u64 = 0,
            out: u64 = 0,

            fn consistent(counters: @This()) bool {
                return counters.out == counters.in - counters.dropped - counters.wasted;
            }
        } = .{},

        /// Quotas track logical progress of compaction, determine pacing and must be deterministic.
        /// Quotas count consumed input values. That is, every time an output block is written,
        /// the done quota is incremented by the number of input values which contributed to the
        /// output block.
        ///
        /// At the start of the bar, the total number of input values is known. The beat quota is
        /// then set based on the number of values left and beats left.
        quotas: struct {
            done: u64 = 0,
            beat: u64 = 0,
            bar: u64 = 0,

            fn beat_done(quotas: @This()) bool {
                assert(quotas.done <= quotas.bar);
                return quotas.done >= quotas.beat;
            }

            fn bar_done(quotas: @This()) bool {
                assert(quotas.done <= quotas.bar);
                return quotas.done == quotas.bar;
            }
        } = .{},

        // Position points at the next value from the layer that should be feed into the merge
        // algorithm.
        level_a_position: Position = .{},
        level_b_position: Position = .{},

        /// Manifest log appends are queued up until bar_complete is explicitly called to ensure
        /// they are applied deterministically relative to other concurrent compactions.
        // Worst-case manifest updates:
        // See docs/about/internals/lsm.md "Compaction Table Overlap" for more detail.
        manifest_entries: stdx.BoundedArrayType(struct {
            operation: enum {
                insert_to_level_b,
                move_to_level_b,
            },
            table: TableInfo,
        }, compaction_tables_output_max) = .{},

        table_builder: Table.Builder = .{},
        // A beat always ends with writing a value block, but the index block can be carried
        // over to the next beat.
        table_builder_index_block: ?*ResourcePool.Block = null,

        // The progress through immutable table is persisted throughout the bar.
        level_a_immutable_stage: enum { ready, merge, exhausted } = .ready,

        // Beat-scoped fields:
        // ------------------
        pool: ?*ResourcePool = null,
        callback: ?*const fn (*ResourcePool) void = null,

        grid_reservation: ?Grid.Reservation = null,
        table_builder_value_block: ?*ResourcePool.Block = null,

        // IO queues:
        //
        // Compaction structure is such that the data can be read (and written) concurrently, but
        // the merge must happen sequentially. It is reminiscent of state machine's prefetch/execute
        // split.
        //
        // When a block is read from disk, it is added to the tail of the corresponding queue. When
        // the head block from both level a and level b queues is in the .read_value_block_done
        // state, the two blocks are popped off the queues and passed down to the merge. At this
        // point, any number of the blocks still in the queues can continue their read operations.
        //
        // For index blocks, queues of length one are used. Because an index block is freed as soon
        // as the read for the last value block is scheduled, the pipeline should not dry out even
        // when switching between the tables.
        //
        // Note that level_{a,b}_position fields track the logical, deterministic progression of
        // compaction. To track which block should be read next, separate `_next` fields are used.
        //
        // For output blocks:
        // - the order of completions doesn't matter,
        // - the blocks are not used after the completion of the IO. That is, only the number of
        //   outstanding operations needs to be tracked. Use a RingBuffer with void elements rather
        //   than an u32 for API uniformity and comptime upper bound. We have
        //   <https://github.com/ziglang/zig/issues/3806> at home.
        //
        // In addition to static max size, the queues are additionally limited at runtime by the
        // number of available free blocks. The queues are not limited by IOPS --- it is assumed
        // that there are enough IOPS to fill up all the queues.
        level_a_index_block: RingBufferType(*ResourcePool.Block, .{
            .array = 1,
        }) = .{ .buffer = undefined },
        level_a_index_block_next: u32 = 0,

        level_a_value_block: RingBufferType(*ResourcePool.Block, .{
            .array = @divExact(constants.lsm_compaction_queue_read_max, 2),
        }) = .{ .buffer = undefined },
        level_a_value_block_next: u32 = 0,

        level_b_index_block: RingBufferType(*ResourcePool.Block, .{
            .array = 1,
        }) = .{ .buffer = undefined },
        level_b_index_block_next: u32 = 0,

        level_b_value_block: RingBufferType(*ResourcePool.Block, .{
            .array = @divExact(constants.lsm_compaction_queue_read_max, 2),
        }) = .{ .buffer = undefined },
        level_b_value_block_next: u32 = 0,

        output_blocks: RingBufferType(void, .{
            .array = constants.lsm_compaction_queue_write_max,
        }) = .{ .buffer = undefined },

        pub fn init(tree: *Tree, grid: *Grid, level_b: u8) Compaction {
            assert(level_b < constants.lsm_levels);

            return Compaction{
                .grid = grid,
                .tree = tree,
                .level_b = level_b,
            };
        }

        pub fn reset(compaction: *Compaction) void {
            compaction.grid.trace.reset(.compact_beat);
            compaction.grid.trace.reset(.compact_beat_merge);
            compaction.* = .{
                .grid = compaction.grid,
                .tree = compaction.tree,
                .level_b = compaction.level_b,
            };
        }

        pub fn assert_between_bars(compaction: *const Compaction) void {
            assert(compaction.stage == .inactive);
            assert(compaction.idle());
            assert(compaction.table_builder_index_block == null);
            assert(compaction.manifest_entries.empty());
        }

        pub fn idle(compaction: *const Compaction) bool {
            return compaction.pool == null and compaction.callback == null and
                // level_a
                compaction.level_a_index_block.count == 0 and
                compaction.level_a_index_block_next == 0 and
                compaction.level_a_value_block.count == 0 and
                compaction.level_a_value_block_next == 0 and
                // level_b
                compaction.level_b_index_block.count == 0 and
                compaction.level_b_index_block_next == 0 and
                compaction.level_b_value_block.count == 0 and
                compaction.level_b_value_block_next == 0 and
                // output
                compaction.table_builder_value_block == null and
                compaction.output_blocks.count == 0;
        }

        /// Plan the work for the bar:
        /// - check if compaction is needed at all (if the level_a is full),
        /// - find a table on level_a and the corresponding range on level_b that should be
        ///   compacted,
        /// - compute the bar quota (just the total number of values in all input tables),
        /// - execute move table optimization if range_b turns out to be empty.
        pub fn bar_commence(compaction: *Compaction, op: u64) void {
            assert(compaction.idle());
            assert(compaction.stage == .inactive);
            assert(op == compaction_op_min(op));

            compaction.stage = .paused;
            compaction.op_min = op;

            if (compaction.level_b == 0) {
                // Do not start compaction if the immutable table does not require compaction.
                if (compaction.tree.table_immutable.mutability.immutable.flushed) {
                    assert(compaction.quotas.bar == 0);
                    assert(compaction.quotas.bar_done());
                    log.debug("{s}:{}: bar_commence: immutable table flushed", .{
                        compaction.tree.config.name,
                        compaction.level_b,
                    });
                    return;
                }

                const table_value_count_limit = Table.value_count_max;
                assert(compaction.tree.table_immutable.count() > 0);
                assert(compaction.tree.table_immutable.count() <= table_value_count_limit);

                // If the mutable table will fit in the free capacity of the immutable table (even
                // in the projected "worst" case of all full batches during the second half-bar),
                // then defer compacting the immutable table into level 0.
                //
                // This optimization cannot apply to the last bar before a checkpoint trigger, since
                // recovery from the checkpoint only replays that final bar, which must reconstruct
                // the original immutable table.
                // TODO(Snapshots) This optimization must be disabled to take a persistent snapshot.
                const mutable_count_half_bar_first = compaction.tree.table_mutable.count();
                const mutable_count_half_bar_last = @divExact(table_value_count_limit, 2);
                const mutable_count = mutable_count_half_bar_first + mutable_count_half_bar_last;
                const immutable_count = compaction.tree.table_immutable.count();
                if (immutable_count + mutable_count <= table_value_count_limit) {
                    const op_checkpoint =
                        compaction.grid.superblock.working.vsr_state.checkpoint.header.op;
                    const op_checkpoint_next = vsr.Checkpoint.checkpoint_after(op_checkpoint);
                    const op_checkpoint_trigger_next =
                        vsr.Checkpoint.trigger_for_checkpoint(op_checkpoint_next).?;
                    const compaction_op_max = op + (half_bar_beat_count - 1);
                    const last_half_bar_of_checkpoint =
                        compaction_op_max == op_checkpoint_trigger_next;

                    if (!last_half_bar_of_checkpoint) {
                        assert(compaction.quotas.bar == 0);
                        assert(compaction.quotas.bar_done());
                        log.debug("{s}:{}: bar_commence: immutable table flush skipped " ++
                            "({}+{}+{} ≤ {})", .{
                            compaction.tree.config.name,
                            compaction.level_b,
                            immutable_count,
                            mutable_count_half_bar_first,
                            mutable_count_half_bar_last,
                            table_value_count_limit,
                        });
                        return;
                    }
                }

                compaction.table_info_a = .{
                    .immutable = compaction.tree.table_immutable.values_used(),
                };

                compaction.range_b = compaction.tree.manifest.immutable_table_compaction_range(
                    compaction.tree.table_immutable.key_min(),
                    compaction.tree.table_immutable.key_max(),
                    .{ .value_count = compaction.tree.table_immutable.count() },
                );

                // +1 to count the immutable table (level A).
                assert(compaction.range_b.?.tables.count() + 1 <= compaction_tables_input_max);
                assert(compaction.range_b.?.key_min <= compaction.tree.table_immutable.key_min());
                assert(compaction.tree.table_immutable.key_max() <= compaction.range_b.?.key_max);
            } else {
                const level_a = compaction.level_b - 1;

                // Do not start compaction if level A does not require compaction.
                const table_range = compaction.tree.manifest.compaction_table(level_a) orelse {
                    assert(compaction.quotas.bar == 0);
                    assert(compaction.quotas.bar_done());
                    log.debug("{s}:{}: bar_commence: nothing to compact", .{
                        compaction.tree.config.name,
                        compaction.level_b,
                    });
                    return;
                };

                compaction.table_info_a = .{ .disk = table_range.table_a };
                compaction.range_b = table_range.range_b;

                assert(compaction.range_b.?.tables.count() + 1 <= compaction_tables_input_max);
                assert(compaction.table_info_a.?.disk.table_info.key_min <=
                    compaction.table_info_a.?.disk.table_info.key_max);
                assert(compaction.range_b.?.key_min <=
                    compaction.table_info_a.?.disk.table_info.key_min);
                assert(compaction.table_info_a.?.disk.table_info.key_max <=
                    compaction.range_b.?.key_max);
            }

            switch (compaction.table_info_a.?) {
                .immutable => {},
                .disk => |table| {
                    assert(!compaction.grid.free_set.is_released(table.table_info.address));
                    assert(!compaction.grid.free_set.is_free(table.table_info.address));
                },
            }
            for (compaction.range_b.?.tables.slice()) |table| {
                assert(!compaction.grid.free_set.is_released(table.table_info.address));
                assert(!compaction.grid.free_set.is_free(table.table_info.address));
            }

            var quota_bar = switch (compaction.table_info_a.?) {
                .immutable => compaction.tree.table_immutable.count(),
                .disk => |table| table.table_info.value_count,
            };
            for (compaction.range_b.?.tables.const_slice()) |*table| {
                quota_bar += table.table_info.value_count;
            }
            compaction.quotas = .{
                .done = 0,
                .beat = 0,
                .bar = quota_bar,
            };
            compaction.move_table = compaction.table_info_a.? == .disk and
                compaction.range_b.?.tables.empty();
            compaction.drop_tombstones = compaction.tree.manifest
                .compaction_must_drop_tombstones(compaction.level_b, &compaction.range_b.?);

            // The last level must always drop tombstones.
            if (compaction.level_b == constants.lsm_levels - 1) assert(compaction.drop_tombstones);

            assert(std.meta.eql(compaction.counters, .{}));
            inline for (.{ compaction.level_a_position, compaction.level_b_position }) |position| {
                assert(std.meta.eql(position, .{}));
            }

            // Append the entries to the manifest update queue here and now if we're doing
            // move table. They'll be applied later by bar_complete().
            if (compaction.move_table) {
                const snapshot_max = snapshot_max_for_table_input(compaction.op_min);
                assert(compaction.table_info_a.?.disk.table_info.snapshot_max >= snapshot_max);

                compaction.manifest_entries.append_assume_capacity(.{
                    .operation = .move_to_level_b,
                    .table = compaction.table_info_a.?.disk.table_info.*,
                });
                compaction.quotas.done = compaction.table_info_a.?.disk.table_info.value_count;
                compaction.quotas.beat = compaction.table_info_a.?.disk.table_info.value_count;
                assert(compaction.quotas.beat_done());
                assert(compaction.quotas.bar_done());
            }
        }

        /// Apply the changes that have been accumulated in memory to the manifest and remove any
        /// tables that are now invisible.
        pub fn bar_complete(compaction: *Compaction) void {
            assert(compaction.idle());
            assert(compaction.stage == .paused);
            assert(compaction.counters.consistent());
            assert(compaction.quotas.bar_done());
            // Assert blocks have been released back to the pipeline.
            assert(compaction.table_builder.state == .no_blocks);
            assert(compaction.table_builder_index_block == null);
            assert(compaction.table_builder_value_block == null);

            defer {
                compaction.* = .{
                    .grid = compaction.grid,
                    .tree = compaction.tree,
                    .level_b = compaction.level_b,
                };
                assert(compaction.stage == .inactive);
            }

            if (compaction.table_info_a == null) {
                assert(compaction.range_b == null);
                assert(compaction.manifest_entries.count() == 0);
                assert(compaction.quotas.bar == 0);
                if (compaction.level_b == 0) {
                    // Either:
                    // - the immutable table is empty (already flushed), or
                    // - the mutable table will be absorbed into the immutable table.
                    maybe(compaction.tree.table_immutable.mutability.immutable.flushed);
                }
                return;
            }
            assert(compaction.table_info_a != null);
            assert(compaction.range_b != null);
            assert(compaction.quotas.bar > 0);

            switch (compaction.table_info_a.?) {
                .immutable => {},
                .disk => |table| {
                    if (compaction.move_table) {
                        assert(!compaction.grid.free_set.is_released(table.table_info.address));
                        assert(!compaction.grid.free_set.is_free(table.table_info.address));
                    } else {
                        assert(compaction.grid.free_set.is_released(table.table_info.address));
                    }
                },
            }
            for (compaction.range_b.?.tables.slice()) |table| {
                assert(compaction.grid.free_set.is_released(table.table_info.address));
            }

            log.debug("{s}:{}: bar_complete: " ++
                "values_in={} values_out={} values_dropped={} values_wasted={}", .{
                compaction.tree.config.name,
                compaction.level_b,
                compaction.counters.in,
                compaction.counters.out,
                compaction.counters.dropped,
                compaction.counters.wasted,
            });

            // Mark the immutable table as flushed, if we were compacting into level 0.
            if (compaction.level_b == 0) {
                assert(!compaction.tree.table_immutable.mutability.immutable.flushed);
                compaction.tree.table_immutable.mutability.immutable.flushed = true;
            }

            // Each compaction's manifest updates are deferred to the end of the last
            // bar to ensure:
            // - manifest log updates are ordered deterministically relative to one another, and
            // - manifest updates are not visible until after the blocks are all on disk.
            const manifest = &compaction.tree.manifest;
            const level_b = compaction.level_b;
            const snapshot_max = snapshot_max_for_table_input(compaction.op_min);

            var manifest_removed_value_count: u64 = 0;
            var manifest_added_value_count: u64 = 0;

            if (compaction.move_table) {
                // If no compaction is required, don't update snapshot_max.
            } else {
                // These updates MUST precede insert_table() and move_table() since they use
                // references to modify the ManifestLevel in-place.
                switch (compaction.table_info_a.?) {
                    .immutable => {
                        manifest_removed_value_count = compaction.tree.table_immutable.count();
                    },
                    .disk => |table_info| {
                        manifest_removed_value_count += table_info.table_info.value_count;
                        manifest.update_table(level_b - 1, snapshot_max, table_info);
                    },
                }
                for (compaction.range_b.?.tables.const_slice()) |table| {
                    manifest_removed_value_count += table.table_info.value_count;
                    manifest.update_table(level_b, snapshot_max, table);
                }
            }

            for (compaction.manifest_entries.slice()) |*entry| {
                switch (entry.operation) {
                    .insert_to_level_b => {
                        manifest.insert_table(level_b, &entry.table);
                        manifest_added_value_count += entry.table.value_count;
                    },
                    .move_to_level_b => {
                        manifest.move_table(level_b - 1, level_b, &entry.table);
                        manifest_removed_value_count += entry.table.value_count;
                        manifest_added_value_count += entry.table.value_count;
                    },
                }
            }
            if (compaction.move_table) {
                assert(std.meta.eql(compaction.counters, .{}));
                assert(manifest_added_value_count == manifest_removed_value_count);
                assert(manifest_added_value_count > 0);
            } else {
                assert(manifest_added_value_count == compaction.counters.out);
                assert(manifest_removed_value_count ==
                    compaction.counters.in - compaction.counters.wasted);
                assert(manifest_removed_value_count - manifest_added_value_count ==
                    compaction.counters.dropped);
            }

            // Hide any tables that are now invisible.
            manifest.remove_invisible_tables(
                level_b,
                &.{},
                compaction.range_b.?.key_min,
                compaction.range_b.?.key_max,
            );
            if (level_b > 0) {
                manifest.remove_invisible_tables(
                    level_b - 1,
                    &.{},
                    compaction.range_b.?.key_min,
                    compaction.range_b.?.key_max,
                );
            }
        }

        /// Plan the work for the beat:
        /// - determine our quota in terms of minimum number of input values to process,
        /// - reserve grid blocks for the output.
        pub fn beat_commence(
            compaction: *Compaction,
            beats_remaining: u64,
        ) void {
            assert(compaction.idle());
            assert(compaction.stage == .paused);
            if (compaction.move_table) assert(compaction.quotas.bar_done());
            assert(compaction.quotas.done >= compaction.quotas.beat);
            defer assert(compaction.quotas.done <= compaction.quotas.beat);

            assert(beats_remaining > 0);
            assert(beats_remaining <= half_bar_beat_count);

            // Calculate how many values we have to compact each beat, to self-correct our pacing.
            // Pacing will have imperfections due to rounding up to fill target value blocks and
            // immutable table filtering duplicate values.
            const values_remaining = compaction.quotas.bar - compaction.quotas.done;
            const beat_values_quota = stdx.div_ceil(values_remaining, beats_remaining);
            compaction.quotas.beat = compaction.quotas.done + beat_values_quota;
            assert(compaction.quotas.done <= compaction.quotas.beat);
            assert(compaction.quotas.beat <= compaction.quotas.bar);
            if (beats_remaining == 1) {
                assert(compaction.quotas.beat == compaction.quotas.bar);
            }

            if (compaction.quotas.beat_done()) {
                assert(compaction.stage == .paused);
                log.debug("{s}:{}: beat_commence: quota fulfilled", .{
                    compaction.tree.config.name,
                    compaction.level_b,
                });
                return;
            }
            assert(!compaction.move_table);
            compaction.stage = .beat;

            // The +1 is for imperfections in pacing our immutable table, which might cause us
            // to overshoot by a single block (limited to 1 due to how the immutable table values
            // are consumed.)
            const beat_value_blocks_max = stdx.div_ceil(
                beat_values_quota,
                Table.layout.block_value_count_max,
            ) + 1;

            // The +1 is in case we had a partially finished index block from a previous beat.
            const beat_index_blocks_max = stdx.div_ceil(
                beat_value_blocks_max,
                Table.data_block_count_max,
            ) + 1;
            const beat_blocks_max = beat_value_blocks_max + beat_index_blocks_max;

            assert(compaction.grid_reservation == null);
            compaction.grid_reservation = compaction.grid.reserve(beat_blocks_max);

            // These values were already processed during the previous beat and add to wastage.
            if (compaction.table_info_a.? == .disk) {
                compaction.counters.wasted += compaction.level_a_position.value;
            }
            compaction.counters.wasted += compaction.level_b_position.value;

            log.debug("{s}:{}: beat_commence: quota_done={} quota_beat={} quota_bar={} " ++
                "blocks_reserved={} values_wasted={}", .{
                compaction.tree.config.name,
                compaction.level_b,
                compaction.quotas.done,
                compaction.quotas.beat,
                compaction.quotas.bar,
                beat_blocks_max,
                compaction.counters.wasted,
            });
        }

        // While beat_commence is called by the forest sequentially for each compaction, to get
        // deterministic grid reservations, each compaction completes its own beat's work
        // asynchronously
        fn beat_complete(compaction: *Compaction) void {
            assert(compaction.stage == .beat_table_done);
            switch (compaction.table_builder.state) {
                .no_blocks => {},
                .index_block => assert(!compaction.table_builder.index_block_full()),
                .index_and_data_block => unreachable,
            }
            if (compaction.table_info_a.? == .immutable) {
                switch (compaction.level_a_immutable_stage) {
                    .ready, .exhausted => {},
                    .merge => unreachable,
                }
            }
            assert(compaction.level_a_index_block.empty());
            assert(compaction.level_a_value_block.empty());
            assert(compaction.level_b_index_block.empty());
            assert(compaction.level_b_value_block.empty());
            assert(compaction.output_blocks.empty());

            assert(compaction.pool.?.idle());
            maybe(compaction.pool.?.blocks_acquired() > 0);
            assert(compaction.grid_reservation != null);
            compaction.grid.forfeit(compaction.grid_reservation.?);
            compaction.grid_reservation = null;

            assert(compaction.counters.consistent());

            assert(compaction.quotas.beat_done());
            assert(compaction.table_builder.data_block_empty());
            assert(compaction.table_builder_value_block == null);
            assert(compaction.table_builder.state == .no_blocks or
                !compaction.table_builder.index_block_full());

            if (compaction.quotas.bar_done()) {
                assert(compaction.table_builder.state == .no_blocks);
                assert(compaction.table_builder_index_block == null);
            }

            compaction.stage = .paused;
            const pool = compaction.pool.?;
            const callback = compaction.callback.?;
            compaction.pool = null;
            compaction.callback = null;
            assert(compaction.idle());
            assert(pool.idle());
            log.debug("{s}:{}: beat_complete: quota_done={} quota_beat={} quota_bar={} " ++
                "values_wasted={}", .{
                compaction.tree.config.name,
                compaction.level_b,
                compaction.quotas.done,
                compaction.quotas.beat,
                compaction.quotas.bar,
                compaction.counters.wasted,
            });
            callback(pool);
        }

        /// The entry point to the actual compaction work for the beat. Called by the forest.
        pub fn compaction_dispatch_enter(
            compaction: *Compaction,
            pool: *ResourcePool,
            callback: *const fn (*ResourcePool) void,
        ) enum { active, beat_finished } {
            assert(compaction.idle());
            switch (compaction.stage) {
                .inactive => unreachable,
                .paused => {
                    assert(compaction.quotas.beat_done());
                    return .beat_finished;
                },
                .beat => {},
                .beat_quota_done, .beat_table_done => unreachable,
            }
            assert(pool.idle());
            compaction.pool = pool;
            compaction.callback = callback;
            assert(
                pool.blocks_free() >= compaction_block_count_beat_min -
                    @intFromBool(compaction.table_builder_index_block != null),
            );
            compaction.grid.trace.start(.compact_beat, .{
                .tree = compaction.tree.config.name,
                .level_b = compaction.level_b,
            });
            compaction.level_a_index_block_next = compaction.level_a_position.index_block;
            compaction.level_a_value_block_next = compaction.level_a_position.value_block;
            compaction.level_b_index_block_next = compaction.level_b_position.index_block;
            compaction.level_b_value_block_next = compaction.level_b_position.value_block;
            compaction.compaction_dispatch();
            return .active;
        }

        // Compaction is a lot of work: read input tables from both levels, merge their value
        // blocks, write the results to disk. Many of these jobs can proceed in parallel. For
        // example, only a single value block from each level is needed to start a merge.
        //
        // The job of compaction_dispatch is to kick off all the jobs. There are several additional
        // concerns:
        // - All jobs use the same common pool of resources (ResourcePool). The jobs are started
        //   in the order that splits resources fairly (e.g., reads from level a and level b
        //   alternate). Fairness also ensures that the process does not deadlock.
        // - Jobs have dependencies --- merging needs value blocks, reading a value block needs the
        //   corresponding index blocks.
        // - A single bar of compaction should process only a fraction of the input, so the
        //   processes can be suspended in the middle.
        //
        // A beat of compaction ends when both:
        //   - at least quota.bar of input values is consumed,
        //   - there's no incomplete output value blocks.
        //
        // In other words, the only compaction state that gets carried over to the next beat is a
        // partially full index block. The current beat must end with writing a value block, and the
        // next beat must start with re-reading level_a and level_b index and value blocks.
        fn compaction_dispatch(compaction: *Compaction) void {
            switch (compaction.stage) {
                .beat,
                .beat_quota_done,
                .beat_table_done,
                => {},
                .inactive,
                .paused,
                => unreachable,
            }

            // The loop below runs while (progressed) and, every time progressed is set to true,
            // one of the safety_counter resources is acquired.
            var progressed = true;
            const safety_counter =
                compaction.pool.?.reads.available() +
                compaction.pool.?.writes.available() +
                compaction.pool.?.cpus.available() + 1;
            for (0..safety_counter) |_| {
                if (!progressed) break;
                progressed = false;

                if (compaction.stage == .beat_table_done) {
                    // Just wait for all in-flight jobs to complete.
                    return compaction.compaction_dispatch_beat_table_done();
                }

                // To avoid deadlocks, allocate blocks for the table builder first.
                if (compaction.table_builder.state == .no_blocks) {
                    assert(compaction.table_builder_index_block == null);
                    if (compaction.pool.?.block_acquire()) |block| {
                        assert(block.stage == .free);
                        block.stage = .build_index_block;
                        compaction.table_builder.set_index_block(block.ptr);
                        compaction.table_builder_index_block = block;
                    } else {
                        assert(compaction.output_blocks.count > 0);
                    }
                }

                if (compaction.table_builder.state == .index_block) {
                    assert(compaction.table_builder_value_block == null);
                    if (compaction.pool.?.block_acquire()) |block| {
                        assert(block.stage == .free);
                        block.stage = .build_value_block;
                        compaction.table_builder.set_data_block(block.ptr);
                        compaction.table_builder_value_block = block;
                    } else {
                        assert(compaction.output_blocks.count > 0);
                    }
                }

                // Read level A index block (for level_b > 0).
                if (compaction.table_info_a.? == .disk) {
                    assert(compaction.level_b > 0);
                    if (!compaction.level_a_index_block.full() and
                        compaction.level_a_index_block_next < 1)
                    {
                        if (compaction.pool.?.block_acquire()) |block| {
                            const read = compaction.pool.?.reads.acquire().?;

                            assert(block.stage == .free);
                            block.stage = .read_index_block;
                            compaction.level_a_index_block.push_assume_capacity(block);

                            compaction.read_index_block(.level_a, read, block);
                            progressed = true;
                        } else {
                            assert(compaction.level_a_index_block.count > 0 or
                                compaction.output_blocks.count > 0);
                        }
                    }
                }

                // Read level B index block.
                if (!compaction.level_b_index_block.full() and
                    compaction.level_b_index_block_next < compaction.range_b.?.tables.count())
                {
                    if (compaction.pool.?.block_acquire()) |block| {
                        const read = compaction.pool.?.reads.acquire().?;

                        assert(block.stage == .free);
                        block.stage = .read_index_block;
                        compaction.level_b_index_block.push_assume_capacity(block);

                        compaction.read_index_block(.level_b, read, block);
                        progressed = true;
                    } else {
                        assert(compaction.level_b_index_block.count > 0 or
                            compaction.output_blocks.count > 0);
                    }
                }

                // Read level A value block.
                if (compaction.table_info_a.? == .immutable) {
                    // The whole table is in memory, no need to read anything.
                    assert(compaction.level_a_index_block.count == 0);
                } else {
                    if (compaction.level_a_index_block.head()) |index_block| {
                        assert(index_block.stage == .read_index_block or
                            index_block.stage == .read_index_block_done);

                        const level_a_priority = compaction.level_b_index_block.count == 0 or
                            compaction.level_a_value_block.count <
                            compaction.level_b_value_block.count;
                        if (index_block.stage == .read_index_block_done and
                            !compaction.level_a_value_block.full() and
                            level_a_priority)
                        {
                            if (compaction.stage == .beat_quota_done and
                                compaction.level_a_value_block.count >= 2)
                            {
                                // Avoid over-read at the end of the beat.
                            } else {
                                if (compaction.pool.?.block_acquire()) |block| {
                                    const read = compaction.pool.?.reads.acquire().?;

                                    assert(block.stage == .free);
                                    block.stage = .read_value_block;
                                    compaction.level_a_value_block.push_assume_capacity(block);

                                    compaction.read_value_block(.level_a, read, block);
                                    progressed = true;
                                } else {
                                    assert(compaction.level_a_value_block.count > 0 or
                                        compaction.output_blocks.count > 0);
                                }
                            }
                        }
                    }
                }

                // Read level B value block.
                if (compaction.level_b_index_block.head()) |index_block| {
                    assert(index_block.stage == .read_index_block or
                        index_block.stage == .read_index_block_done);
                    const level_b_priority = compaction.level_a_index_block.count == 0 or
                        compaction.level_b_value_block.count <=
                        compaction.level_a_value_block.count;
                    if (index_block.stage == .read_index_block_done and
                        !compaction.level_b_value_block.full() and
                        level_b_priority)
                    {
                        if (compaction.stage == .beat_quota_done and
                            compaction.level_b_value_block.count >= 2)
                        {
                            // Avoid over-read at the end of the beat.
                        } else {
                            if (compaction.pool.?.block_acquire()) |block| {
                                const read = compaction.pool.?.reads.acquire().?;

                                assert(block.stage == .free);
                                block.stage = .read_value_block;
                                compaction.level_b_value_block.push_assume_capacity(block);

                                compaction.read_value_block(.level_b, read, block);
                                progressed = true;
                            } else {
                                assert(compaction.level_b_value_block.count > 0 or
                                    compaction.output_blocks.count > 0);
                            }
                        }
                    }
                }

                const level_a_ready_immutable = compaction.table_info_a.? == .immutable and
                    compaction.level_a_immutable_stage == .ready;
                const level_a_ready_disk = compaction.table_info_a.? == .disk and
                    compaction.level_a_value_block.head() != null and
                    compaction.level_a_value_block.head().?.stage == .read_value_block_done;
                const level_a_ready = level_a_ready_immutable or level_a_ready_disk;

                const level_a_exhausted_immutable = compaction.table_info_a.? == .immutable and
                    compaction.level_a_immutable_stage == .exhausted;
                const level_a_exhausted_disk = compaction.table_info_a.? == .disk and
                    compaction.level_a_index_block.count == 0 and
                    compaction.level_a_value_block.count == 0;
                const level_a_exhausted = level_a_exhausted_immutable or level_a_exhausted_disk;

                const level_b_ready = compaction.level_b_value_block.head() != null and
                    compaction.level_b_value_block.head().?.stage == .read_value_block_done;

                const level_b_exhausted =
                    compaction.level_b_index_block.count == 0 and
                    compaction.level_b_value_block.count == 0;
                assert((level_a_exhausted and level_b_exhausted) == (compaction.quotas.bar_done()));

                if (compaction.table_builder.state == .index_and_data_block) {
                    if (level_a_exhausted and level_b_exhausted) {
                        assert(compaction.stage == .beat_quota_done);
                    } else if ((level_a_exhausted or level_a_ready) and
                        (level_b_exhausted or level_b_ready) and
                        !compaction.table_builder.data_block_full())
                    {
                        const cpu = compaction.pool.?.cpus.acquire().?;
                        compaction.merge(cpu);
                        progressed = true;
                    }

                    if (compaction.table_builder.data_block_full() and
                        // Ensure that it is possible to immediately write the index block.
                        compaction.output_blocks.spare_capacity() >= 2)
                    {
                        const write = compaction.pool.?.writes.acquire().?;
                        compaction.write_value_block(write, .{
                            .address = compaction.grid.acquire(compaction.grid_reservation.?),
                        });
                        progressed = true;
                    }

                    if (compaction.table_builder.index_block_full()) {
                        assert(!compaction.output_blocks.full());
                        const write = compaction.pool.?.writes.acquire().?;
                        compaction.write_index_block(write, .{
                            .address = compaction.grid.acquire(compaction.grid_reservation.?),
                        });
                        progressed = true;
                    }
                }
            } else unreachable;
            assert(!progressed);
            assert(!compaction.pool.?.idle());
        }

        fn compaction_dispatch_beat_table_done(compaction: *Compaction) void {
            assert(compaction.stage == .beat_table_done);

            if (compaction.table_builder.state == .index_and_data_block) {
                if (!compaction.table_builder.data_block_full()) {
                    assert(compaction.quotas.bar_done());
                }
                if (compaction.table_builder.data_block_empty()) {
                    const value_block = compaction.table_builder_value_block.?;
                    compaction.table_builder_value_block = null;
                    compaction.table_builder.state = .index_block;

                    assert(value_block.stage == .build_value_block);
                    value_block.stage = .free;
                    compaction.pool.?.block_release(value_block);
                } else {
                    if (!compaction.output_blocks.full()) {
                        const write = compaction.pool.?.writes.acquire().?;
                        compaction.write_value_block(write, .{
                            .address = compaction.grid.acquire(compaction.grid_reservation.?),
                        });
                        assert(compaction.table_builder.state == .index_block);
                        assert(compaction.table_builder_value_block == null);
                    }
                }
            }

            if (compaction.table_builder.state == .index_block and
                (compaction.table_builder.index_block_full() or compaction.quotas.bar_done()))
            {
                if (compaction.table_builder.index_block_empty()) {
                    assert(compaction.quotas.bar_done());
                    const index_block = compaction.table_builder_index_block.?;
                    compaction.table_builder_index_block = null;
                    compaction.table_builder.state = .no_blocks;

                    assert(index_block.stage == .build_index_block);
                    index_block.stage = .free;
                    compaction.pool.?.block_release(index_block);
                } else {
                    if (!compaction.output_blocks.full()) {
                        const write = compaction.pool.?.writes.acquire().?;
                        compaction.write_index_block(write, .{
                            .address = compaction.grid.acquire(compaction.grid_reservation.?),
                        });
                    }
                }
            }

            if (compaction.output_blocks.count > 0) {
                return;
            }
            switch (compaction.table_builder.state) {
                .no_blocks => {},
                .index_block => {
                    assert(!compaction.table_builder.index_block_full());
                    assert(!compaction.quotas.bar_done());
                },
                .index_and_data_block => unreachable,
            }

            while (compaction.level_a_value_block.head()) |block| {
                if (block.stage == .read_value_block) {
                    return;
                }
                assert(block.stage == .read_value_block_done);
                compaction.counters.wasted +=
                    Table.data_block_values_used(block.ptr).len;

                _ = compaction.level_a_value_block.pop();
                block.stage = .free;
                block.last_block_in_the_table = false;
                compaction.pool.?.block_release(block);
            }
            assert(compaction.level_a_value_block.count == 0);
            compaction.level_a_value_block_next = 0;

            while (compaction.level_a_index_block.head()) |block| {
                if (block.stage == .read_index_block) {
                    return;
                }
                assert(block.stage == .read_index_block_done);
                _ = compaction.level_a_index_block.pop();
                block.stage = .free;
                compaction.pool.?.block_release(block);
            }
            assert(compaction.level_a_index_block.count == 0);
            compaction.level_a_index_block_next = 0;

            while (compaction.level_b_value_block.head()) |block| {
                if (block.stage == .read_value_block) {
                    return;
                }
                assert(block.stage == .read_value_block_done);
                compaction.counters.wasted +=
                    Table.data_block_values_used(block.ptr).len;

                _ = compaction.level_b_value_block.pop();
                block.stage = .free;
                block.last_block_in_the_table = false;
                compaction.pool.?.block_release(block);
            }
            assert(compaction.level_b_value_block.count == 0);
            compaction.level_b_value_block_next = 0;

            while (compaction.level_b_index_block.head()) |block| {
                if (block.stage == .read_index_block) {
                    return;
                }
                assert(block.stage == .read_index_block_done);
                _ = compaction.level_b_index_block.pop();
                block.stage = .free;
                compaction.pool.?.block_release(block);
            }
            assert(compaction.level_b_index_block.count == 0);
            compaction.level_b_index_block_next = 0;

            // When computing wastage from over-read blocks, we don't bother accounting for
            // partially consumed blocks and instead compensate here.
            if (compaction.table_info_a.? == .disk) {
                compaction.counters.wasted -= compaction.level_a_position.value;
            }
            compaction.counters.wasted -= compaction.level_b_position.value;
            compaction.grid.trace.stop(.compact_beat, .{
                .tree = compaction.tree.config.name,
                .level_b = compaction.level_b,
            });
            compaction.beat_complete();
        }

        fn read_index_block(
            compaction: *Compaction,
            level: enum { level_a, level_b },
            read: *ResourcePool.BlockRead,
            index_block: *ResourcePool.Block,
        ) void {
            assert(compaction.stage == .beat or compaction.stage == .beat_quota_done);
            assert(index_block.stage == .read_index_block);
            switch (level) {
                .level_a => assert(compaction.level_a_position.index_block == 0),
                .level_b => assert(
                    compaction.level_b_index_block_next < compaction.range_b.?.tables.count(),
                ),
            }

            const table_ref = switch (level) {
                .level_a => compaction.table_info_a.?.disk,
                .level_b => compaction.range_b.?.tables.get(compaction.level_b_index_block_next),
            };
            read.block = index_block;
            read.compaction = compaction;
            compaction.grid.read_block(
                .{ .from_local_or_global_storage = read_index_block_callback },
                &read.grid_read,
                table_ref.table_info.address,
                table_ref.table_info.checksum,
                .{ .cache_read = true, .cache_write = true },
            );
            switch (level) {
                .level_a => compaction.level_a_index_block_next += 1,
                .level_b => compaction.level_b_index_block_next += 1,
            }
        }

        fn read_index_block_callback(grid_read: *Grid.Read, index_block: BlockPtrConst) void {
            const read: *ResourcePool.BlockRead = @fieldParentPtr("grid_read", grid_read);
            const compaction: *Compaction = read.parent(Compaction);
            const block = read.block;
            compaction.pool.?.reads.release(read);

            assert(block.stage == .read_index_block);
            stdx.copy_disjoint(.exact, u8, block.ptr, index_block);
            block.stage = .read_index_block_done;
            compaction.compaction_dispatch();
        }

        fn read_value_block(
            compaction: *Compaction,
            level: enum { level_a, level_b },
            read: *ResourcePool.BlockRead,
            value_block: *ResourcePool.Block,
        ) void {
            assert(compaction.stage == .beat or compaction.stage == .beat_quota_done);
            assert(value_block.stage == .read_value_block);
            if (level == .level_a) assert(compaction.table_info_a.? == .disk);

            const index_block = switch (level) {
                .level_a => compaction.level_a_index_block.head().?,
                .level_b => compaction.level_b_index_block.head().?,
            };
            const value_block_index = switch (level) {
                .level_a => compaction.level_a_value_block_next,
                .level_b => compaction.level_b_value_block_next,
            };

            const index_schema = schema.TableIndex.from(index_block.ptr);

            const value_block_address =
                index_schema.data_addresses_used(index_block.ptr)[value_block_index];
            const value_block_checksum =
                index_schema.data_checksums_used(index_block.ptr)[value_block_index];

            read.block = value_block;
            read.compaction = compaction;
            compaction.grid.read_block(
                .{ .from_local_or_global_storage = read_value_block_callback },
                &read.grid_read,
                value_block_address,
                value_block_checksum.value,
                .{ .cache_read = true, .cache_write = true },
            );

            switch (level) {
                .level_a => {
                    compaction.level_a_value_block_next += 1;
                    if (compaction.level_a_value_block_next ==
                        index_schema.data_blocks_used(index_block.ptr))
                    {
                        value_block.last_block_in_the_table = true;
                        const pop = compaction.level_a_index_block.pop().?;
                        assert(pop.ptr == index_block.ptr);
                        assert(index_block.stage == .read_index_block_done);

                        compaction.read_value_block_release_table(index_block.ptr);
                        index_block.stage = .free;
                        compaction.pool.?.block_release(index_block);
                        compaction.level_a_value_block_next = 0;
                    }
                },
                .level_b => {
                    compaction.level_b_value_block_next += 1;
                    if (compaction.level_b_value_block_next ==
                        index_schema.data_blocks_used(index_block.ptr))
                    {
                        value_block.last_block_in_the_table = true;
                        const pop = compaction.level_b_index_block.pop().?;
                        assert(pop.ptr == index_block.ptr);
                        assert(index_block.stage == .read_index_block_done);

                        compaction.read_value_block_release_table(index_block.ptr);
                        index_block.stage = .free;
                        compaction.pool.?.block_release(index_block);
                        compaction.level_b_value_block_next = 0;
                    }
                },
            }
        }

        // TODO: Support for LSM snapshots would require us to only remove blocks
        // that are invisible.
        fn read_value_block_release_table(
            compaction: *Compaction,
            index_block: BlockPtrConst,
        ) void {
            const index_schema = schema.TableIndex.from(index_block);
            const index_block_address = Table.block_address(index_block);
            const data_block_addresses = index_schema.data_addresses_used(index_block);

            // Tables are released when the index block is no longer needed. Given that the same
            // index block can get re-read across the bar, the same table can be released twice.
            if (compaction.grid.free_set.is_released(index_block_address)) {
                for (data_block_addresses) |address| {
                    assert(compaction.grid.free_set.is_released(address));
                }
            } else {
                compaction.grid.release(data_block_addresses);
                compaction.grid.release(&.{index_block_address});
            }
        }

        fn read_value_block_callback(grid_read: *Grid.Read, value_block: BlockPtrConst) void {
            const read: *ResourcePool.BlockRead = @fieldParentPtr("grid_read", grid_read);
            const compaction: *Compaction = read.parent(Compaction);
            const block = read.block;
            compaction.pool.?.reads.release(read);

            assert(block.stage == .read_value_block);
            stdx.copy_disjoint(.exact, u8, block.ptr, value_block);
            block.stage = .read_value_block_done;
            compaction.counters.in += Table.data_block_values_used(block.ptr).len;
            compaction.compaction_dispatch();
        }

        fn merge(compaction: *Compaction, cpu: *ResourcePool.CPU) void {
            assert(!compaction.quotas.bar_done());

            if (compaction.table_info_a.? == .immutable) {
                if (compaction.level_a_immutable_stage == .ready) {
                    compaction.level_a_immutable_stage = .merge;
                } else assert(compaction.level_a_immutable_stage == .exhausted);
            } else {
                if (compaction.level_a_value_block.head()) |block| {
                    assert(block.stage == .read_value_block_done);
                    block.stage = .merge;
                } else assert(compaction.level_b_value_block.head() != null);
            }

            if (compaction.level_b_value_block.head()) |block| {
                assert(block.stage == .read_value_block_done);
                block.stage = .merge;
            }

            assert(compaction.table_builder.state == .index_and_data_block);

            cpu.compaction = compaction;
            compaction.grid.on_next_tick(merge_callback, &cpu.next_tick);
        }

        fn merge_callback(next_tick: *Grid.NextTick) void {
            const cpu: *ResourcePool.CPU = @fieldParentPtr("next_tick", next_tick);
            const compaction: *Compaction = cpu.parent(Compaction);
            compaction.pool.?.cpus.release(cpu);
            assert(compaction.table_builder.state == .index_and_data_block);

            compaction.grid.trace.start(.compact_beat_merge, .{
                .tree = compaction.tree.config.name,
                .level_b = compaction.level_b,
            });

            const values_source_a, const values_source_b = compaction.merge_inputs();
            assert(values_source_a != null or values_source_b != null);

            const values_target = compaction.table_builder
                .data_block_values()[compaction.table_builder.value_count..];

            inline for ([_]?[]const Value{
                values_source_a,
                values_source_b,
                values_target,
            }) |values_maybe| {
                if (values_maybe) |values| {
                    assert(values.len > 0);
                    assert(values.len <= Table.data.value_count_max);
                }
            }

            // Do the actual merge from inputs to the output (table builder).
            const merge_result: MergeResult = if (values_source_a == null) blk: {
                const consumed = values_copy(values_target, values_source_b.?);
                break :blk .{
                    .consumed_a = 0,
                    .consumed_b = consumed,
                    .dropped = 0,
                    .produced = consumed,
                };
            } else if (values_source_b == null) blk: {
                if (compaction.drop_tombstones) {
                    const copy_result = values_copy_drop_tombstones(
                        values_target,
                        values_source_a.?,
                    );
                    break :blk .{
                        .consumed_a = copy_result.consumed,
                        .consumed_b = 0,
                        .dropped = copy_result.dropped,
                        .produced = copy_result.produced,
                    };
                } else {
                    const consumed = values_copy(values_target, values_source_a.?);
                    break :blk .{
                        .consumed_a = consumed,
                        .consumed_b = 0,
                        .dropped = 0,
                        .produced = consumed,
                    };
                }
            } else values_merge(
                values_target,
                values_source_a.?,
                values_source_b.?,
                compaction.drop_tombstones,
            );

            compaction.level_a_position.value += merge_result.consumed_a;
            compaction.level_b_position.value += merge_result.consumed_b;
            compaction.table_builder.value_count += merge_result.produced;

            if (compaction.table_info_a.? == .immutable) {
                assert(compaction.level_a_position.value <= Table.value_count_max);
            } else {
                assert(compaction.level_a_position.value <= Table.data.value_count_max);
            }
            assert(compaction.level_b_position.value <= Table.data.value_count_max);
            assert(compaction.table_builder.value_count <= Table.data.value_count_max);

            if (compaction.table_info_a.? == .immutable) {
                compaction.counters.in += merge_result.consumed_a;
            }
            compaction.counters.dropped += merge_result.dropped;

            compaction.quotas.done += merge_result.consumed_a + merge_result.consumed_b;
            assert(compaction.quotas.done <= compaction.quotas.bar);

            compaction.merge_advance_position();

            // NB: although all the work here is synchronous, we don't defer trace.stop precisely
            // to exclude compaction.dispatch call below.
            compaction.grid.trace.stop(.compact_beat_merge, .{
                .tree = compaction.tree.config.name,
                .level_b = compaction.level_b,
            });
            compaction.compaction_dispatch();
        }

        fn merge_inputs(compaction: *const Compaction) struct { ?[]const Value, ?[]const Value } {
            const level_a_values_used: ?[]const Value = values: {
                switch (compaction.table_info_a.?) {
                    .immutable => {
                        if (compaction.level_a_immutable_stage == .merge) {
                            break :values compaction.table_info_a.?.immutable;
                        } else {
                            assert(compaction.level_a_immutable_stage == .exhausted);
                            break :values null;
                        }
                    },
                    .disk => {
                        if (compaction.level_a_value_block.head()) |block| {
                            assert(block.stage == .merge);
                            break :values Table.data_block_values_used(block.ptr);
                        } else {
                            break :values null;
                        }
                    },
                }
            };

            const level_b_values_used: ?[]const Value = values: {
                if (compaction.level_b_value_block.head()) |block| {
                    assert(block.stage == .merge);
                    break :values Table.data_block_values_used(block.ptr);
                } else {
                    break :values null;
                }
            };
            assert(!(level_a_values_used == null and level_b_values_used == null));

            const level_a_values = if (level_a_values_used) |values_used| values: {
                const values_remaining = values_used[compaction.level_a_position.value..];
                // Only consume one block at a time so that a beat never outputs past its quota
                // by more than one value block.
                const limit = @min(
                    Table.data.value_count_max,
                    values_remaining.len,
                );
                break :values values_remaining[0..limit];
            } else null;

            const level_b_values = if (level_b_values_used) |values_used|
                values_used[compaction.level_b_position.value..]
            else
                null;

            return .{ level_a_values, level_b_values };
        }

        // merge_callback advances just position.values. Here, we implement the carry-flag logic,
        // advancing value_block and index_block. This is also the place where determine that the
        // beat's quota of work is done and begin to wind down the dispatch loop.
        fn merge_advance_position(compaction: *Compaction) void {
            if (compaction.table_info_a.? == .immutable) {
                if (compaction.level_a_immutable_stage == .merge) {
                    if (compaction.level_a_position.value ==
                        compaction.table_info_a.?.immutable.len)
                    {
                        compaction.level_a_position.value_block += 1;
                        assert(compaction.level_a_position.value_block == 1);
                        compaction.level_a_position.value = 0;
                        compaction.level_a_immutable_stage = .exhausted;
                    } else {
                        compaction.level_a_immutable_stage = .ready;
                    }
                } else {
                    assert(compaction.level_a_immutable_stage == .exhausted);
                }
            } else {
                if (compaction.level_a_value_block.head()) |value_block| {
                    assert(value_block.stage == .merge);
                    if (compaction.level_a_position.value ==
                        Table.data_block_values_used(value_block.ptr).len)
                    {
                        _ = compaction.level_a_value_block.pop();
                        compaction.level_a_position.value_block += 1;
                        compaction.level_a_position.value = 0;

                        if (value_block.last_block_in_the_table) {
                            compaction.level_a_position.index_block += 1;
                            assert(compaction.level_a_position.index_block == 1);
                            compaction.level_a_position.value_block = 0;
                        }

                        value_block.stage = .free;
                        value_block.last_block_in_the_table = false;
                        compaction.pool.?.block_release(value_block);
                    } else {
                        value_block.stage = .read_value_block_done;
                    }
                } else {
                    assert(compaction.level_a_position.value == 0); // Level A exhausted.
                }
            }

            if (compaction.level_b_value_block.head()) |value_block| {
                assert(value_block.stage == .merge);
                if (compaction.level_b_position.value ==
                    Table.data_block_values_used(value_block.ptr).len)
                {
                    _ = compaction.level_b_value_block.pop().?;
                    compaction.level_b_position.value_block += 1;
                    compaction.level_b_position.value = 0;

                    if (value_block.last_block_in_the_table) {
                        compaction.level_b_position.index_block += 1;
                        compaction.level_b_position.value_block = 0;
                    }

                    value_block.stage = .free;
                    value_block.last_block_in_the_table = false;
                    compaction.pool.?.block_release(value_block);
                } else {
                    value_block.stage = .read_value_block_done;
                }
            } else {
                assert(compaction.level_b_position.value == 0); // Level B exhausted.
            }

            if (compaction.quotas.beat_done()) {
                assert(
                    compaction.stage == .beat or
                        compaction.stage == .beat_quota_done,
                );
                compaction.stage = .beat_quota_done;

                if (compaction.table_builder.data_block_full() or compaction.quotas.bar_done()) {
                    compaction.stage = .beat_table_done;
                }
            }
        }

        fn write_value_block(
            compaction: *Compaction,
            write: *ResourcePool.BlockWrite,
            options: struct { address: u64 },
        ) void {
            const block = compaction.table_builder_value_block.?;
            assert(block.stage == .build_value_block);
            assert(compaction.table_builder.data_block == block.ptr);
            assert(!compaction.output_blocks.full());

            compaction.counters.out += compaction.table_builder.value_count;
            compaction.table_builder.data_block_finish(.{
                .cluster = compaction.grid.superblock.working.cluster,
                .release = compaction.grid.superblock.working.vsr_state.checkpoint.release,
                .address = options.address,
                .snapshot_min = snapshot_min_for_table_output(compaction.op_min),
                .tree_id = compaction.tree.config.id,
            });
            assert(compaction.table_builder.state == .index_block);
            compaction.table_builder_value_block = null;

            compaction.output_blocks.push_assume_capacity({});
            block.stage = .write_value_block;

            write.block = block;
            write.compaction = compaction;
            compaction.grid.create_block(
                write_block_callback,
                &write.grid_write,
                &write.block.ptr,
            );
        }

        fn write_index_block(
            compaction: *Compaction,
            write: *ResourcePool.BlockWrite,
            options: struct { address: u64 },
        ) void {
            const block = compaction.table_builder_index_block.?;
            assert(block.stage == .build_index_block);
            assert(compaction.table_builder.index_block == block.ptr);
            assert(!compaction.output_blocks.full());

            const table = compaction.table_builder.index_block_finish(.{
                .cluster = compaction.grid.superblock.working.cluster,
                .release = compaction.grid.superblock.working.vsr_state.checkpoint.release,
                .address = options.address,
                .snapshot_min = snapshot_min_for_table_output(compaction.op_min),
                .tree_id = compaction.tree.config.id,
            });
            assert(compaction.table_builder.state == .no_blocks);
            compaction.table_builder_index_block = null;

            compaction.manifest_entries.append_assume_capacity(.{
                .operation = .insert_to_level_b,
                .table = table,
            });

            compaction.output_blocks.push_assume_capacity({});
            block.stage = .write_index_block;

            write.block = block;
            write.compaction = compaction;
            compaction.grid.create_block(
                write_block_callback,
                &write.grid_write,
                &write.block.ptr,
            );
        }

        fn write_block_callback(grid_write: *Grid.Write) void {
            const write: *ResourcePool.BlockWrite = @fieldParentPtr("grid_write", grid_write);
            const compaction: *Compaction = write.parent(Compaction);
            const block = write.block;
            compaction.pool.?.writes.release(write);

            assert(block.stage == .write_value_block or block.stage == .write_index_block);
            block.stage = .free;
            compaction.pool.?.block_release(block);

            const popped = compaction.output_blocks.pop();
            assert(popped != null);

            compaction.compaction_dispatch();
        }

        // The three functions below are hot CPU loops doing the actual merging, TigerBeetle's data
        // plane. To reduce the probability of the optimizer getting confused over pointers, don't
        // use 'self' and instead specify all inputs and outputs explicitly. Its the caller's job to
        // apply control plane changes to the compaction state.
        //
        // TODO: Add micro benchmarks.

        fn values_copy(values_target: []Value, values_source: []const Value) u32 {
            assert(values_source.len > 0);
            assert(values_source.len <= Table.data.value_count_max);
            assert(values_target.len > 0);
            assert(values_target.len <= Table.data.value_count_max);

            const len: u32 = @intCast(@min(values_source.len, values_target.len));
            stdx.copy_disjoint(
                .exact,
                Value,
                values_target[0..len],
                values_source[0..len],
            );

            return len;
        }

        const CopyDropTombstonesResult = struct {
            consumed: u32,
            dropped: u32,
            produced: u32,
        };
        /// Copy values from values_source to values_target, dropping tombstones as we go.
        fn values_copy_drop_tombstones(
            values_target: []Value,
            values_source: []const Value,
        ) CopyDropTombstonesResult {
            assert(values_source.len > 0);
            assert(values_source.len <= Table.data.value_count_max);
            assert(values_target.len > 0);
            assert(values_target.len <= Table.data.value_count_max);

            var index_source: usize = 0;
            var index_target: usize = 0;
            // Merge as many values as possible.
            while (index_source < values_source.len and
                index_target < values_target.len)
            {
                const value_in = &values_source[index_source];
                index_source += 1;
                if (tombstone(value_in)) {
                    assert(Table.usage != .secondary_index);
                    continue;
                }
                values_target[index_target] = value_in.*;
                index_target += 1;
            }
            const copy_result: CopyDropTombstonesResult = .{
                .consumed = @intCast(index_source),
                .dropped = @intCast(index_source - index_target),
                .produced = @intCast(index_target),
            };
            assert(copy_result.consumed > 0);
            assert(copy_result.consumed <= values_source.len);
            assert(copy_result.dropped <= copy_result.consumed);
            assert(copy_result.produced <= values_target.len);
            assert(copy_result.produced == copy_result.consumed - copy_result.dropped);
            return copy_result;
        }

        const MergeResult = struct {
            consumed_a: u32,
            consumed_b: u32,
            dropped: u32,
            produced: u32,
        };

        /// Merge values from table_a and table_b, with table_a taking precedence. Tombstones may
        /// or may not be dropped depending on bar.drop_tombstones.
        fn values_merge(
            values_target: []Value,
            values_source_a: []const Value,
            values_source_b: []const Value,
            drop_tombstones: bool,
        ) MergeResult {
            assert(values_source_a.len > 0);
            assert(values_source_a.len <= Table.data.value_count_max);
            assert(values_source_b.len > 0);
            assert(values_source_b.len <= Table.data.value_count_max);
            assert(values_target.len > 0);
            assert(values_target.len <= Table.data.value_count_max);

            var index_source_a: usize = 0;
            var index_source_b: usize = 0;
            var index_target: usize = 0;

            while (index_source_a < values_source_a.len and
                index_source_b < values_source_b.len and
                index_target < values_target.len)
            {
                const value_a = &values_source_a[index_source_a];
                const value_b = &values_source_b[index_source_b];
                switch (std.math.order(key_from_value(value_a), key_from_value(value_b))) {
                    .lt => { // Pick value from level a.
                        index_source_a += 1;
                        if (drop_tombstones and tombstone(value_a)) {
                            assert(Table.usage != .secondary_index);
                            continue;
                        }
                        values_target[index_target] = value_a.*;
                        index_target += 1;
                    },
                    .gt => { // Pick value from level b.
                        index_source_b += 1;
                        values_target[index_target] = value_b.*;
                        index_target += 1;
                    },
                    .eq => { // Values have equal keys -- collapse them!
                        index_source_a += 1;
                        index_source_b += 1;

                        if (comptime Table.usage == .secondary_index) {
                            // Secondary index optimization --- cancel out put and remove.
                            assert(tombstone(value_a) != tombstone(value_b));
                        } else {
                            if (drop_tombstones and tombstone(value_a)) continue;
                            values_target[index_target] = value_a.*;
                            index_target += 1;
                        }
                    },
                }
            }

            const merge_result: MergeResult = .{
                .consumed_a = @intCast(index_source_a),
                .consumed_b = @intCast(index_source_b),
                .dropped = @intCast(index_source_a + index_source_b - index_target),
                .produced = @intCast(index_target),
            };
            assert(merge_result.consumed_a > 0 or merge_result.consumed_b > 0);
            assert(merge_result.consumed_a <= values_source_a.len);
            assert(merge_result.consumed_b <= values_source_b.len);
            assert(merge_result.dropped <= merge_result.consumed_a + merge_result.consumed_b);
            assert(merge_result.produced <= values_target.len);
            assert(merge_result.produced ==
                merge_result.consumed_a + merge_result.consumed_b - merge_result.dropped);
            return merge_result;
        }
    };
}

pub fn snapshot_max_for_table_input(op_min: u64) u64 {
    return snapshot_min_for_table_output(op_min) - 1;
}

pub fn snapshot_min_for_table_output(op_min: u64) u64 {
    assert(op_min > 0);
    assert(op_min % @divExact(constants.lsm_compaction_ops, 2) == 0);
    return op_min + @divExact(constants.lsm_compaction_ops, 2);
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
/// commits and compactions (with lsm_compaction_ops=8).
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
    assert(op >= half_bar_beat_count);
    return op - op % half_bar_beat_count;
}
