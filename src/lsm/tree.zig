//! An LSM tree.
const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const os = std.os;
const maybe = stdx.maybe;
const div_ceil = stdx.div_ceil;

const log = std.log.scoped(.tree);
const tracer = @import("../tracer.zig");

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const schema = @import("schema.zig");
const Snapshot = schema.Snapshot;

const CompositeKeyType = @import("composite_key.zig").CompositeKeyType;
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const GridType = @import("../vsr/grid.zig").GridType;
const BlockPtrConst = @import("../vsr/grid.zig").BlockPtrConst;

pub const ScopeCloseMode = enum { persist, discard };
const snapshot_min_for_table_output = @import("compaction.zig").snapshot_min_for_table_output;

const half_bar_beat_count = @divExact(constants.lsm_batch_multiple, 2);

// StateMachine:
//
// /// state machine will pass this on to all object stores
// /// Read I/O only
// pub fn read(batch, callback) void
//
// /// write the ops in batch to the memtable/objcache, previously called commit()
// pub fn write(batch) void
//
// /// Flush in memory state to disk, perform merges, etc
// /// Only function that triggers Write I/O in LSMs, as well as some Read
// /// Make as incremental as possible, don't block the main thread, avoid high latency/spikes
// pub fn flush(callback) void
//
// /// Write manifest info for all object stores into buffer
// pub fn encode_superblock(buffer) void
//
// /// Restore all in-memory state from the superblock data
// pub fn decode_superblock(buffer) void
//

/// The maximum number of tables for a single tree.
pub const table_count_max = table_count_max_for_tree(
    constants.lsm_growth_factor,
    constants.lsm_levels,
);

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

/// The maximum number of concurrent compactions (per tree).
pub const compactions_max = div_ceil(constants.lsm_levels, 2);

pub const TreeConfig = struct {
    /// Unique (stable) identifier, across all trees in the forest.
    id: u16,
    /// Human-readable tree name for logging.
    name: []const u8,
};

pub fn TreeType(comptime TreeTable: type, comptime Storage: type) type {
    const Key = TreeTable.Key;
    const Value = TreeTable.Value;
    const tombstone = TreeTable.tombstone;

    return struct {
        const Tree = @This();

        pub const Table = TreeTable;
        pub const TableMemory = @import("table_memory.zig").TableMemoryType(Table);
        pub const Manifest = @import("manifest.zig").ManifestType(Table, Storage);

        const Grid = GridType(Storage);
        const ManifestLog = @import("manifest_log.zig").ManifestLogType(Storage);
        const KeyRange = Manifest.KeyRange;

        const CompactionType = @import("compaction.zig").CompactionType;
        const Compaction = CompactionType(Table, Tree, Storage);

        pub const LookupMemoryResult = union(enum) {
            negative,
            positive: *const Value,
            possible: u8,
        };

        grid: *Grid,
        config: Config,
        options: Options,

        table_mutable: TableMemory,
        table_immutable: TableMemory,

        manifest: Manifest,

        compaction_phase: enum {
            idle,
            skipped,
            skipped_done,
            running,
            running_done,
        } = .idle,

        compaction_table_immutable: Compaction,

        /// The number of Compaction instances is divided by two as, at any given compaction tick,
        /// we're only compacting either even or odd levels but never both.
        /// Uses divFloor as the last level, even with odd lsm_levels, doesn't compact to anything.
        /// (e.g. floor(5/2) = 2 for levels 0->1, 2->3 when even and immut->0, 1->2, 3->4 when odd).
        /// This means, that for odd lsm_levels, the last CompactionTable is unused.
        compaction_table: [@divFloor(constants.lsm_levels, 2)]Compaction,

        /// While a compaction is running, this is the op of the last compact().
        /// While no compaction is running, this is the op of the last compact() to complete.
        /// (When recovering from a checkpoint, compaction_op starts at op_checkpoint).
        compaction_op: ?u64 = null,

        compaction_io_pending: usize = 0,
        compaction_callback: union(enum) {
            none,
            /// We're at the end of a half-bar.
            /// Call this callback when all current compactions finish.
            awaiting: *const fn (*Tree) void,
            /// We're at the end of some other beat.
            /// Call this on the next tick.
            next_tick: *const fn (*Tree) void,
        } = .none,
        compaction_next_tick: Grid.NextTick = undefined,

        tracer_slot: ?tracer.SpanStart = null,

        active_scope: ?TableMemory.ValueContext = null,

        /// The range of keys in this tree at snapshot_latest.
        key_range: ?KeyRange = null,

        /// (Constructed by the Forest.)
        pub const Config = TreeConfig;

        /// (Constructed by the StateMachine.)
        pub const Options = struct {
            // No options currently.
        };

        pub fn init(
            allocator: mem.Allocator,
            node_pool: *NodePool,
            grid: *Grid,
            config: Config,
            options: Options,
        ) !Tree {
            assert(grid.superblock.opened);
            assert(config.id != 0); // id=0 is reserved.
            assert(config.name.len > 0);

            var table_mutable = try TableMemory.init(allocator, .mutable, config.name);
            errdefer table_mutable.deinit(allocator);

            var table_immutable = try TableMemory.init(
                allocator,
                .{ .immutable = .{} },
                config.name,
            );
            errdefer table_immutable.deinit(allocator);

            var manifest = try Manifest.init(allocator, node_pool, config);
            errdefer manifest.deinit(allocator);

            var compaction_table_immutable = try Compaction.init(allocator, config);
            errdefer compaction_table_immutable.deinit(allocator);

            var compaction_table: [@divFloor(constants.lsm_levels, 2)]Compaction = undefined;
            {
                comptime var i: usize = 0;
                inline while (i < compaction_table.len) : (i += 1) {
                    errdefer for (compaction_table[0..i]) |*c| c.deinit(allocator);
                    compaction_table[i] = try Compaction.init(allocator, config);
                }
            }
            errdefer for (compaction_table) |*c| c.deinit(allocator);

            return Tree{
                .grid = grid,
                .config = config,
                .options = options,
                .table_mutable = table_mutable,
                .table_immutable = table_immutable,
                .manifest = manifest,
                .compaction_table_immutable = compaction_table_immutable,
                .compaction_table = compaction_table,
            };
        }

        pub fn deinit(tree: *Tree, allocator: mem.Allocator) void {
            assert(tree.tracer_slot == null);

            for (&tree.compaction_table) |*compaction| compaction.deinit(allocator);
            tree.compaction_table_immutable.deinit(allocator);

            // TODO Consider whether we should release blocks acquired from Grid.block_free_set.
            tree.manifest.deinit(allocator);
            tree.table_immutable.deinit(allocator);
            tree.table_mutable.deinit(allocator);
        }

        pub fn reset(tree: *Tree) void {
            tree.table_mutable.reset();
            tree.table_immutable.reset();
            tree.manifest.reset();

            tree.compaction_table_immutable.reset();
            for (&tree.compaction_table) |*compaction| compaction.reset();

            tree.* = .{
                .grid = tree.grid,
                .config = tree.config,
                .options = tree.options,
                .table_mutable = tree.table_mutable,
                .table_immutable = tree.table_immutable,
                .manifest = tree.manifest,
                .compaction_table_immutable = tree.compaction_table_immutable,
                .compaction_table = tree.compaction_table,
            };
        }

        /// Open a new scope. Within a scope, changes can be persisted
        /// or discarded. Only one scope can be active at a time.
        pub fn scope_open(tree: *Tree) void {
            assert(tree.active_scope == null);
            tree.active_scope = tree.table_mutable.value_context;
        }

        pub fn scope_close(tree: *Tree, mode: ScopeCloseMode) void {
            assert(tree.active_scope != null);
            assert(tree.active_scope.?.count <= tree.table_mutable.value_context.count);

            if (mode == .discard) {
                tree.table_mutable.value_context = tree.active_scope.?;
            }

            tree.active_scope = null;
        }

        pub fn put(tree: *Tree, value: *const Value) void {
            tree.table_mutable.put(value);
        }

        pub fn remove(tree: *Tree, value: *const Value) void {
            tree.table_mutable.put(&Table.tombstone_from_key(Table.key_from_value(value)));
        }

        pub fn key_range_update(tree: *Tree, key: Key) void {
            if (tree.key_range) |*key_range| {
                if (key < key_range.key_min) key_range.key_min = key;
                if (key > key_range.key_max) key_range.key_max = key;
            } else {
                tree.key_range = KeyRange{ .key_min = key, .key_max = key };
            }
        }

        /// Returns True if the given key may be present in the Tree, False if the key is
        /// guaranteed to not be present.
        ///
        /// Specifically, it checks whether the key exists within the Tree's key range.
        pub fn key_range_contains(tree: *const Tree, snapshot: Snapshot, key: Key) bool {
            if (snapshot.timestamp == Snapshot.latest.timestamp) {
                return tree.key_range != null and
                    tree.key_range.?.key_min <= key and
                    key <= tree.key_range.?.key_max;
            } else {
                return true;
            }
        }

        /// This function is intended to never be called by regular code. It only
        /// exists for fuzzing, due to the performance overhead it carries. Real
        /// code must rely on the Groove cache for lookups.
        /// The returned Value pointer is only valid synchronously.
        pub fn lookup_from_memory(tree: *Tree, key: Key) ?*const Value {
            comptime assert(constants.verify);

            tree.table_mutable.sort();
            return tree.table_mutable.get(key) orelse tree.table_immutable.get(key);
        }

        /// Returns:
        /// - .negative if the key does not exist in the Manifest.
        /// - .positive if the key exists in the Manifest, along with the associated value.
        /// - .possible if the key may exist in the Manifest but its existence cannot be
        ///  ascertained without IO, along with the level number at which IO must be performed.
        ///
        /// This function attempts to fetch the index & data blocks for the tables that
        /// could contain the key synchronously from the Grid cache. It then attempts to ascertain
        /// the existence of the key in the data block. If any of the blocks needed to
        /// ascertain the existence of the key are not in the Grid cache, it bails out.
        /// The returned `.positive` Value pointer is only valid synchronously.
        pub fn lookup_from_levels_cache(tree: *Tree, snapshot: Snapshot, key: Key) LookupMemoryResult {
            var iterator = tree.manifest.lookup(snapshot, key, 0);
            while (iterator.next()) |table| {
                const index_block = tree.grid.read_block_from_cache(
                    table.address,
                    table.checksum,
                    .{ .coherent = true },
                ) orelse {
                    // Index block not in cache. We cannot rule out existence without I/O,
                    // and therefore bail out.
                    return .{ .possible = iterator.level - 1 };
                };

                const key_blocks = Table.index_blocks_for_key(index_block, key) orelse continue;
                switch (tree.cached_data_block_search(
                    key_blocks.data_block_address,
                    key_blocks.data_block_checksum,
                    key,
                )) {
                    .negative => {},
                    // Key present in the data block.
                    .positive => |value| return .{ .positive = value },
                    // Data block was not found in the grid cache. We cannot rule out
                    // the existence of the key without I/O, and therefore bail out.
                    .block_not_in_cache => return .{ .possible = iterator.level - 1 },
                }
            }
            // Key not present in the Manifest.
            return .negative;
        }

        fn cached_data_block_search(
            tree: *Tree,
            address: u64,
            checksum: u128,
            key: Key,
        ) union(enum) {
            positive: *const Value,
            negative,
            block_not_in_cache,
        } {
            if (tree.grid.read_block_from_cache(
                address,
                checksum,
                .{ .coherent = true },
            )) |data_block| {
                if (Table.data_block_search(data_block, key)) |value| {
                    return .{ .positive = value };
                } else {
                    return .negative;
                }
            } else {
                return .block_not_in_cache;
            }
        }

        /// Call this function only after checking `lookup_from_levels_cache()`.
        /// The callback's Value pointer is only valid synchronously within the callback.
        pub fn lookup_from_levels_storage(tree: *Tree, parameters: struct {
            callback: *const fn (*LookupContext, ?*const Value) void,
            context: *LookupContext,
            snapshot: Snapshot,
            key: Key,
            level_min: u8,
        }) void {
            var index_block_count: u8 = 0;
            var index_block_addresses: [constants.lsm_levels]u64 = undefined;
            var index_block_checksums: [constants.lsm_levels]u128 = undefined;
            {
                var it = tree.manifest.lookup(
                    parameters.snapshot,
                    parameters.key,
                    parameters.level_min,
                );
                while (it.next()) |table| : (index_block_count += 1) {
                    assert(table.visible(parameters.snapshot));
                    assert(table.key_min <= parameters.key);
                    assert(parameters.key <= table.key_max);

                    index_block_addresses[index_block_count] = table.address;
                    index_block_checksums[index_block_count] = table.checksum;
                }
            }

            if (index_block_count == 0) {
                parameters.callback(parameters.context, null);
                return;
            }

            parameters.context.* = .{
                .tree = tree,
                .completion = undefined,
                .key = parameters.key,
                .index_block_count = index_block_count,
                .index_block_addresses = index_block_addresses,
                .index_block_checksums = index_block_checksums,
                .callback = parameters.callback,
            };

            parameters.context.read_index_block();
        }

        pub const LookupContext = struct {
            const Read = Grid.Read;

            tree: *Tree,
            completion: Read,

            key: Key,

            /// This value is an index into the index_block_addresses/checksums arrays.
            index_block: u8 = 0,
            index_block_count: u8,
            index_block_addresses: [constants.lsm_levels]u64,
            index_block_checksums: [constants.lsm_levels]u128,

            data_block: ?struct {
                address: u64,
                checksum: u128,
            } = null,

            callback: *const fn (*Tree.LookupContext, ?*const Value) void,

            fn read_index_block(context: *LookupContext) void {
                assert(context.data_block == null);
                assert(context.index_block < context.index_block_count);
                assert(context.index_block_count > 0);
                assert(context.index_block_count <= constants.lsm_levels);

                context.tree.grid.read_block(
                    .{ .from_local_or_global_storage = read_index_block_callback },
                    &context.completion,
                    context.index_block_addresses[context.index_block],
                    context.index_block_checksums[context.index_block],
                    .{ .cache_read = true, .cache_write = true },
                );
            }

            fn read_index_block_callback(completion: *Read, index_block: BlockPtrConst) void {
                const context = @fieldParentPtr(LookupContext, "completion", completion);
                assert(context.data_block == null);
                assert(context.index_block < context.index_block_count);
                assert(context.index_block_count > 0);
                assert(context.index_block_count <= constants.lsm_levels);
                assert(schema.TableIndex.metadata(index_block).tree_id == context.tree.config.id);

                const blocks = Table.index_blocks_for_key(index_block, context.key) orelse {
                    // The key is not present in this table, check the next level.
                    context.advance_to_next_level();
                    return;
                };

                context.data_block = .{
                    .address = blocks.data_block_address,
                    .checksum = blocks.data_block_checksum,
                };

                context.tree.grid.read_block(
                    .{ .from_local_or_global_storage = read_data_block_callback },
                    completion,
                    context.data_block.?.address,
                    context.data_block.?.checksum,
                    .{ .cache_read = true, .cache_write = true },
                );
            }

            fn read_data_block_callback(completion: *Read, data_block: BlockPtrConst) void {
                const context = @fieldParentPtr(LookupContext, "completion", completion);
                assert(context.data_block != null);
                assert(context.index_block < context.index_block_count);
                assert(context.index_block_count > 0);
                assert(context.index_block_count <= constants.lsm_levels);
                assert(schema.TableData.metadata(data_block).tree_id == context.tree.config.id);

                if (Table.data_block_search(data_block, context.key)) |value| {
                    context.callback(context, unwrap_tombstone(value));
                } else {
                    // The key is not present in this table, check the next level.
                    context.advance_to_next_level();
                }
            }

            fn advance_to_next_level(context: *LookupContext) void {
                assert(context.index_block < context.index_block_count);
                assert(context.index_block_count > 0);
                assert(context.index_block_count <= constants.lsm_levels);

                // Data block may be null if the key is not contained in the
                // index block's key range.
                maybe(context.data_block == null);

                context.index_block += 1;
                if (context.index_block == context.index_block_count) {
                    context.callback(context, null);
                    return;
                }
                assert(context.index_block < context.index_block_count);

                context.data_block = null;
                context.read_index_block();
            }
        };

        /// Returns null if the value is null or a tombstone, otherwise returns the value.
        /// We use tombstone values internally, but expose them as null to the user.
        /// This distinction enables us to cache a null result as a tombstone in our hash maps.
        pub inline fn unwrap_tombstone(value: ?*const Value) ?*const Value {
            return if (value == null or tombstone(value.?)) null else value.?;
        }

        pub fn open_commence(tree: *Tree, manifest_log: *ManifestLog) void {
            assert(tree.compaction_op == null);
            assert(tree.key_range == null);

            tree.manifest.open_commence(manifest_log);
        }

        pub fn open_table(
            tree: *Tree,
            table: *const schema.ManifestNode.TableInfo,
        ) void {
            assert(tree.compaction_op == null);
            assert(tree.key_range == null);

            const tree_table = Manifest.TreeTableInfo.decode(table);
            tree.manifest.levels[table.label.level].insert_table(
                tree.manifest.node_pool,
                &tree_table,
            );
        }

        pub fn open_complete(tree: *Tree) void {
            assert(tree.compaction_op == null);
            assert(tree.key_range == null);

            tree.compaction_op = tree.grid.superblock.working.vsr_state.checkpoint.commit_min;
            tree.key_range = tree.manifest.key_range();

            tree.manifest.verify(Snapshot.latest);
            assert(tree.compaction_op.? == 0 or
                (tree.compaction_op.? + 1) % constants.lsm_batch_multiple == 0);
            maybe(tree.key_range == null);
        }

        const CompactionTableContext = struct {
            compaction: *Compaction,
            index: u8,
            level_a: u8,
            level_b: u8,
        };

        const CompactionTableIterator = struct {
            tree: *Tree,
            index: u8 = 0,

            fn next(it: *CompactionTableIterator) ?CompactionTableContext {
                const compaction_beat = it.tree.compaction_op.? % constants.lsm_batch_multiple;
                const even_levels = compaction_beat < half_bar_beat_count;
                const level_a = (it.index * 2) + @intFromBool(!even_levels);
                const level_b = level_a + 1;

                if (level_a >= constants.lsm_levels - 1) return null;
                assert(level_b < constants.lsm_levels);

                defer it.index += 1;
                return CompactionTableContext{
                    .compaction = &it.tree.compaction_table[it.index],
                    .index = it.index,
                    .level_a = level_a,
                    .level_b = level_b,
                };
            }
        };

        /// Since concurrent compactions into and out of a level may contend for the same range:
        ///
        /// 1. compact level 0 to 1, level 2 to 3, level 4 to 5 etc., and then
        /// 2. compact the immutable table to level 0, level 1 to 2, level 3 to 4 etc.
        ///
        /// This order (even levels, then odd levels) is significant, since it reduces the number of
        /// level 0 tables that overlap with the immutable table, reducing write amplification.
        ///
        /// We therefore take the bar, during which all compactions run, and divide by two,
        /// running the compactions from even levels in the first half bar, and then the odd.
        ///
        /// Compactions start on the down beat of a half bar, using 0-based beats.
        /// For example, if there are 4 beats in a bar, start on beat 0 or beat 2.
        pub fn compact(tree: *Tree, callback: *const fn (*Tree) void, op: u64) void {
            assert(tree.compaction_phase == .idle);
            assert(tree.compaction_callback == .none);
            assert(op != 0);
            assert(op == tree.compaction_op.? + 1);
            assert(op > tree.grid.superblock.working.vsr_state.checkpoint.commit_min);

            tracer.start(
                &tree.tracer_slot,
                .{ .tree_compaction_beat = .{ .tree_name = tree.config.name } },
                @src(),
            );

            tree.compaction_op = op;

            if (op < constants.lsm_batch_multiple or
                tree.grid.superblock.working.vsr_state.op_compacted(op))
            {
                // Either:
                // - There is nothing to compact for the first measure.
                //   We skip the main compaction code path first compaction bar entirely because it
                //   is a special case — its first beat is 1, not 0.
                // - We recovered from a checkpoint, and must avoid replaying one bar of
                //   compactions that were applied before the checkpoint. Repeating these ops'
                //   compactions would actually perform different compactions than before,
                //   causing the storage state of the replica to diverge from the cluster.
                //   See also: compaction_op_min().
                // Immutable table preparation is handled by compact_end(), even when compaction
                // is skipped.
                tree.compaction_phase = .skipped;

                tree.compaction_callback = .{ .next_tick = callback };
                tree.grid.on_next_tick(compact_finish_next_tick, &tree.compaction_next_tick);
                return;
            }

            tree.compaction_phase = .running;

            //FIXME
            const op_min = compaction_op_min(tree.compaction_op.?);
            assert(op_min < Snapshot.latest.timestamp);
            assert(op_min % half_bar_beat_count == 0);

            const compaction_beat = tree.compaction_op.? % constants.lsm_batch_multiple;
            log.debug("{s}: compact: op={d} op_min={d} beat={d}/{d}", .{
                tree.config.name,
                tree.compaction_op.?,
                op_min,
                compaction_beat + 1,
                constants.lsm_batch_multiple,
            });

            const BeatKind = enum { half_bar_start, half_bar_middle, half_bar_end };
            const beat_kind = if (compaction_beat == 0 or
                compaction_beat == half_bar_beat_count)
                BeatKind.half_bar_start
            else if (compaction_beat == half_bar_beat_count - 1 or
                compaction_beat == constants.lsm_batch_multiple - 1)
                BeatKind.half_bar_end
            else
                BeatKind.half_bar_middle;

            switch (beat_kind) {
                .half_bar_start => {
                    if (constants.verify) {
                        tree.manifest.verify(
                            Snapshot{
                                //FIXME
                                .timestamp = tree.compaction_op.?,
                            },
                        );
                    }

                    // Corresponds to compact_finish_join_next_tick() (during half_bar_end).
                    tree.compaction_io_pending += 1;

                    // Maybe start compacting the immutable table.
                    const even_levels = compaction_beat < half_bar_beat_count;
                    if (even_levels) {
                        assert(tree.compaction_table_immutable.state == .idle);
                    } else {
                        tree.compact_start_table_immutable(op_min);
                    }

                    // Maybe start compacting the other levels.
                    var it = CompactionTableIterator{ .tree = tree };
                    while (it.next()) |context| {
                        tree.compact_start_table(op_min, context);
                    }

                    tree.compaction_callback = .{ .next_tick = callback };
                    tree.grid.on_next_tick(compact_finish_next_tick, &tree.compaction_next_tick);
                },
                .half_bar_middle => {
                    tree.compaction_callback = .{ .next_tick = callback };
                    tree.grid.on_next_tick(compact_finish_next_tick, &tree.compaction_next_tick);
                },
                .half_bar_end => {
                    // At the end of a half-bar, we have to wait for all compactions to finish.
                    tree.compaction_callback = .{ .awaiting = callback };
                    tree.grid.on_next_tick(
                        compact_finish_join_next_tick,
                        &tree.compaction_next_tick,
                    );
                },
            }
        }

        fn compact_start_table_immutable(tree: *Tree, op_min: u64) void {
            const compaction_beat = tree.compaction_op.? % constants.lsm_batch_multiple;
            assert(compaction_beat == half_bar_beat_count);

            // Do not start compaction if the immutable table does not require compaction.
            if (tree.table_immutable.mutability.immutable.flushed) return;

            const values = tree.table_immutable.values_used();
            const values_count = values.len;
            assert(values_count > 0);

            const level_b: u8 = 0;
            const range_b = tree.manifest.immutable_table_compaction_range(
                tree.table_immutable.key_min(),
                tree.table_immutable.key_max(),
            );

            // +1 to count the input table from level A.
            assert(range_b.tables.count() + 1 <= compaction_tables_input_max);
            assert(range_b.key_min <= tree.table_immutable.key_min());
            assert(tree.table_immutable.key_max() <= range_b.key_max);

            log.debug("{s}: compacting immutable table to level 0 " ++
                "(snapshot_min={d} compaction.op_min={d} table_count={d})", .{
                tree.config.name,
                tree.table_immutable.mutability.immutable.snapshot_min.timestamp,
                op_min,
                range_b.tables.count() + 1,
            });

            tree.compaction_io_pending += 1;
            tree.compaction_table_immutable.start(.{
                .grid = tree.grid,
                .tree = tree,
                .op_min = op_min,
                .table_info_a = .{ .immutable = values },
                .level_b = level_b,
                .range_b = range_b,
                .callback = compact_table_finish,
            });
        }

        fn compact_start_table(tree: *Tree, op_min: u64, context: CompactionTableContext) void {
            const compaction_beat = tree.compaction_op.? % half_bar_beat_count;
            assert(compaction_beat == 0);

            assert(context.level_a < constants.lsm_levels);
            assert(context.level_b < constants.lsm_levels);
            assert(context.level_a + 1 == context.level_b);

            // Do not start compaction if level A does not require compaction.
            const table_range = tree.manifest.compaction_table(context.level_a) orelse return;
            const table_a = table_range.table_a.table_info;
            const range_b = table_range.range_b;

            assert(range_b.tables.count() + 1 <= compaction_tables_input_max);
            assert(table_a.key_min <= table_a.key_max);
            assert(range_b.key_min <= table_a.key_min);
            assert(table_a.key_max <= range_b.key_max);

            log.debug("{s}: compacting {d} tables from level {d} to level {d}", .{
                tree.config.name,
                range_b.tables.count() + 1,
                context.level_a,
                context.level_b,
            });

            tree.compaction_io_pending += 1;
            context.compaction.start(.{
                .grid = tree.grid,
                .tree = tree,
                .op_min = op_min,
                .table_info_a = .{ .disk = table_range.table_a },
                .level_b = context.level_b,
                .range_b = range_b,
                .callback = compact_table_finish,
            });
        }

        fn compact_table_finish(compaction: *Compaction) void {
            const tree = compaction.context.tree;
            if (compaction.context.level_b == 0) {
                log.debug("{s}: compacted immutable table to level {d}", .{
                    tree.config.name,
                    compaction.context.level_b,
                });
            } else {
                log.debug("{s}: compacted {d} tables from level {d} to level {d}", .{
                    tree.config.name,
                    compaction.context.range_b.tables.count() + 1,
                    compaction.context.level_b - 1,
                    compaction.context.level_b,
                });
            }

            tree.compaction_io_pending -= 1;
            tree.compact_finish_join();
        }

        fn compact_finish_join_next_tick(next_tick: *Grid.NextTick) void {
            const tree = @fieldParentPtr(Tree, "compaction_next_tick", next_tick);
            tree.compaction_io_pending -= 1;
            tree.compact_finish_join();
        }

        /// This is called:
        /// * When a compaction finishes.
        /// * When we reach the end of a half-bar.
        /// But this function only does anything on the last call -
        //  when all compactions have finished AND we've reached the end of the half-bar.
        fn compact_finish_join(tree: *Tree) void {
            // If some compactions are still running, we're not finished.
            if (tree.compaction_io_pending > 0) return;

            // If we haven't yet reached the end of the half-bar, we're not finished.
            if (tree.compaction_callback != .awaiting) return;

            log.debug("{s}: finished all compactions", .{tree.config.name});
            tree.compact_finish();
        }

        fn compact_finish_next_tick(next_tick: *Grid.NextTick) void {
            const tree = @fieldParentPtr(Tree, "compaction_next_tick", next_tick);
            assert(tree.compaction_callback == .next_tick);

            switch (tree.compaction_phase) {
                .running => tree.compaction_phase = .running_done,
                .skipped => tree.compaction_phase = .skipped_done,
                else => unreachable,
            }

            tracer.end(
                &tree.tracer_slot,
                .{ .tree_compaction_beat = .{ .tree_name = tree.config.name } },
            );

            const callback = tree.compaction_callback.next_tick;
            tree.compaction_callback = .none;
            callback(tree);
        }

        fn compact_finish(tree: *Tree) void {
            assert(tree.compaction_phase == .running);
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback == .awaiting);

            tree.compaction_phase = .running_done;

            if (constants.verify) {
                tree.manifest.verify(Snapshot.latest);
            }

            tracer.end(
                &tree.tracer_slot,
                .{ .tree_compaction_beat = .{ .tree_name = tree.config.name } },
            );

            const callback = tree.compaction_callback.awaiting;
            tree.compaction_callback = .none;
            callback(tree);
        }

        pub fn compact_end(tree: *Tree) void {
            const state_old = tree.compaction_phase;
            tree.compaction_phase = .idle;
            const compaction_beat = tree.compaction_op.? % constants.lsm_batch_multiple;

            // We still need to swap our tables even if we skip compaction for the first bar. The
            // immutable table will be initially empty / 'flushed', so it's fine for it to be
            // discarded.
            if (state_old == .skipped_done and
                compaction_beat == constants.lsm_batch_multiple - 1)
            {
                tree.swap_mutable_and_immutable(
                    //FIXME
                    Snapshot{
                        .timestamp = snapshot_min_for_table_output(compaction_op_min(tree.compaction_op.?)),
                    },
                );
            }

            switch (state_old) {
                .running_done => {}, // Fall through.
                .skipped_done => return,
                else => unreachable,
            }

            // Only run at the end of each half-bar.
            const compacted_levels_odd = compaction_beat == constants.lsm_batch_multiple - 1;
            const compacted_levels_even = compaction_beat == half_bar_beat_count - 1;
            if (!compacted_levels_odd and !compacted_levels_even) return;

            // All compactions have finished for the current half-bar.
            // We couldn't remove the (invisible) input tables until now because prefetch()
            // needs a complete set of tables for lookups to avoid missing data.

            // Close the immutable table Compaction.
            // Also clear any tables made invisible by the compaction.
            const even_levels = compaction_beat < half_bar_beat_count;
            if (!even_levels) {
                switch (tree.compaction_table_immutable.state) {
                    // The compaction wasn't started for this half bar.
                    .idle => assert(tree.table_immutable.mutability.immutable.flushed),
                    .tables_writing_done => {
                        tree.compaction_table_immutable.apply_to_manifest();
                        tree.manifest.remove_invisible_tables(
                            tree.compaction_table_immutable.context.level_b,
                            &.{},
                            tree.compaction_table_immutable.context.range_b.key_min,
                            tree.compaction_table_immutable.context.range_b.key_max,
                        );

                        // Mark that the contents of our immutable table have been flushed,
                        // so it's safe to clear them.
                        tree.table_immutable.mutability.immutable.flushed = true;
                        tree.compaction_table_immutable.transition_to_idle();
                    },
                    else => unreachable,
                }
            }

            if (compaction_beat == constants.lsm_batch_multiple - 1) {
                tree.swap_mutable_and_immutable(
                    Snapshot{
                        //FIXME
                        .timestamp = snapshot_min_for_table_output(compaction_op_min(tree.compaction_op.?)),
                    },
                );
            }

            // Close all the other Compactions.
            // Also clear any tables made invisible by the compactions.
            var it = CompactionTableIterator{ .tree = tree };
            while (it.next()) |context| {
                switch (context.compaction.state) {
                    .idle => {}, // The compaction wasn't started for this half bar.
                    .tables_writing_done => {
                        context.compaction.apply_to_manifest();
                        tree.manifest.remove_invisible_tables(
                            context.compaction.context.level_b,
                            &.{},
                            context.compaction.context.range_b.key_min,
                            context.compaction.context.range_b.key_max,
                        );
                        if (context.compaction.context.level_b > 0) {
                            tree.manifest.remove_invisible_tables(
                                context.compaction.context.level_b - 1,
                                &.{},
                                context.compaction.context.range_b.key_min,
                                context.compaction.context.range_b.key_max,
                            );
                        }
                        context.compaction.transition_to_idle();
                    },
                    else => unreachable,
                }
            }

            assert(tree.compaction_table_immutable.state == .idle);
            it = CompactionTableIterator{ .tree = tree };
            while (it.next()) |context| {
                assert(context.compaction.state == .idle);
            }

            if (compacted_levels_odd) {
                // Assert all visible tables haven't overflowed their max per level.
                tree.manifest.assert_level_table_counts();
            }
        }

        /// Called after the last beat of a full compaction bar.
        fn swap_mutable_and_immutable(tree: *Tree, snapshot_min: Snapshot) void {
            assert(tree.table_mutable.mutability == .mutable);
            assert(tree.table_immutable.mutability == .immutable);
            assert(tree.table_immutable.mutability.immutable.flushed);
            assert(snapshot_min.timestamp > 0);
            assert(snapshot_min.timestamp < Snapshot.latest.timestamp);

            assert((tree.compaction_op.? + 1) % constants.lsm_batch_multiple == 0);

            // The immutable table must be visible to the next bar.
            // In addition, the immutable table is conceptually an output table of this compaction
            // bar, and now its snapshot_min matches the snapshot_min of the Compactions' output
            // tables.
            tree.table_mutable.make_immutable(snapshot_min);
            tree.table_immutable.make_mutable();
            std.mem.swap(TableMemory, &tree.table_mutable, &tree.table_immutable);

            assert(tree.table_mutable.count() == 0);
            assert(tree.table_mutable.mutability == .mutable);
            assert(tree.table_immutable.mutability == .immutable);
        }

        pub fn assert_between_bars(tree: *const Tree) void {
            // Assert no outstanding compact_tick() work.
            assert(tree.compaction_io_pending == 0);
            assert(tree.compaction_callback == .none);
            assert(tree.compaction_op.? > 0);

            // Assert that this is the last beat in the compaction bar.
            const compaction_beat = tree.compaction_op.? % constants.lsm_batch_multiple;
            const last_beat_in_bar = constants.lsm_batch_multiple - 1;
            assert(last_beat_in_bar == compaction_beat);

            // Assert no outstanding compactions.
            assert(tree.compaction_table_immutable.state == .idle);
            for (&tree.compaction_table) |*compaction| {
                assert(compaction.state == .idle);
            }

            // Assert all manifest levels haven't overflowed their table counts.
            tree.manifest.assert_level_table_counts();

            if (constants.verify) {
                tree.manifest.assert_no_invisible_tables(&.{});
            }
        }
    };
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
    return op - op % half_bar_beat_count;
}

/// The total number of tables that can be supported by the tree across so many levels.
pub fn table_count_max_for_tree(growth_factor: u32, levels_count: u32) u32 {
    assert(growth_factor >= 4);
    assert(growth_factor <= 16); // Limit excessive write amplification.
    assert(levels_count >= 2);
    assert(levels_count <= 10); // Limit excessive read amplification.
    assert(levels_count <= constants.lsm_levels);

    var count: u32 = 0;
    var level: u32 = 0;
    while (level < levels_count) : (level += 1) {
        count += table_count_max_for_level(growth_factor, level);
    }
    return count;
}

/// The total number of tables that can be supported by the level alone.
pub fn table_count_max_for_level(growth_factor: u32, level: u32) u32 {
    assert(level >= 0);
    assert(level < constants.lsm_levels);

    return math.pow(u32, growth_factor, level + 1);
}

test "table_count_max_for_level/tree" {
    const expectEqual = std.testing.expectEqual;

    try expectEqual(@as(u32, 8), table_count_max_for_level(8, 0));
    try expectEqual(@as(u32, 64), table_count_max_for_level(8, 1));
    try expectEqual(@as(u32, 512), table_count_max_for_level(8, 2));
    try expectEqual(@as(u32, 4096), table_count_max_for_level(8, 3));
    try expectEqual(@as(u32, 32768), table_count_max_for_level(8, 4));
    try expectEqual(@as(u32, 262144), table_count_max_for_level(8, 5));
    try expectEqual(@as(u32, 2097152), table_count_max_for_level(8, 6));

    try expectEqual(@as(u32, 8 + 64), table_count_max_for_tree(8, 2));
    try expectEqual(@as(u32, 72 + 512), table_count_max_for_tree(8, 3));
    try expectEqual(@as(u32, 584 + 4096), table_count_max_for_tree(8, 4));
    try expectEqual(@as(u32, 4680 + 32768), table_count_max_for_tree(8, 5));
    try expectEqual(@as(u32, 37448 + 262144), table_count_max_for_tree(8, 6));
    try expectEqual(@as(u32, 299592 + 2097152), table_count_max_for_tree(8, 7));
}

test "TreeType" {
    const CompositeKey = @import("composite_key.zig").CompositeKeyType(u64);
    const Table = @import("table.zig").TableType(
        CompositeKey.Key,
        CompositeKey,
        CompositeKey.key_from_value,
        CompositeKey.sentinel_key,
        CompositeKey.tombstone,
        CompositeKey.tombstone_from_key,
        constants.state_machine_config.lsm_batch_multiple * 1024,
        .secondary_index,
    );

    const Storage = @import("../storage.zig").Storage;
    std.testing.refAllDecls(TreeType(Table, Storage));
}
