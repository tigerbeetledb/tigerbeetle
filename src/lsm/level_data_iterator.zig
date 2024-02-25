const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;
const schema = @import("schema.zig");

const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const ManifestType = @import("manifest.zig").ManifestType;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const GridType = @import("../vsr/grid.zig").GridType;
const BlockPtr = @import("../vsr/grid.zig").BlockPtr;
const BlockPtrConst = @import("../vsr/grid.zig").BlockPtrConst;
const Direction = @import("../direction.zig").Direction;
const TableDataIteratorType = @import("table_data_iterator.zig").TableDataIteratorType;
const Snapshot = schema.Snapshot;

// Iterates over the data blocks in a level B table. References to the level B
// tables to be iterated over are passed via the `tables` field in the context,
// in the start() function.
// * Assumption: This iterator works under the assumption that the references to
// the tables are stable. For references fetched from the ManifestLevel, this
// assumption only holds true till move_table & insert_table are called as a part
// of apply_to_manifest. This is because move_table & insert_table are both operations
// that insert/remove tables from the ManifestLevel segmented arrays, which could cause
// rearrangements in the memory layout of the arrays. These rearrangements could
// cause our table references to get invalidated.
pub fn LevelTableValueBlockIteratorType(comptime Table: type, comptime Storage: type) type {
    return struct {
        const LevelTableValueBlockIterator = @This();

        const Grid = GridType(Storage);
        const Manifest = ManifestType(Table, Storage);
        const TableInfo = Manifest.TableInfo;
        const TableDataIterator = TableDataIteratorType(Storage);

        pub const Context = struct {
            grid: *Grid,
            level: u8,
            snapshot: Snapshot,
            index_block: BlockPtr,
            // `tables` contains TableInfo references from ManifestLevel.
            tables: union(enum) {
                compaction: []const Manifest.TableInfoReference,
                scan: ?*const Manifest.TreeTableInfo,
            },
            direction: Direction,

            fn tables_len(context: *const Context) usize {
                return switch (context.tables) {
                    .compaction => |slice| slice.len,
                    .scan => |table_info| @intFromBool(table_info != null),
                };
            }

            fn tables_get(context: *const Context, index: usize) *const Manifest.TreeTableInfo {
                return switch (context.tables) {
                    .compaction => |slice| slice[index].table_info,
                    .scan => |table_info| blk: {
                        assert(index == 0);
                        break :blk table_info.?;
                    },
                };
            }
        };

        pub const DataBlocksToLoad = union(enum) {
            none,
            all,
            range: struct { start: usize, end: usize },
        };
        pub const IndexCallback = *const fn (it: *LevelTableValueBlockIterator) DataBlocksToLoad;
        pub const DataCallback = *const fn (it: *LevelTableValueBlockIterator, data_block: ?BlockPtrConst) void;
        pub const Callback = struct {
            on_index: IndexCallback,
            on_data: DataCallback,
        };

        /// Passed by `start`.
        context: Context,

        /// Internal state.
        table_data_iterator: TableDataIterator,
        table_index: usize = 0,

        read: Grid.Read = undefined,
        next_tick: Grid.NextTick = undefined,

        callback: union(enum) {
            none,
            level_next: Callback,
            table_next: Callback,
        },

        pub fn init() LevelTableValueBlockIterator {
            var table_data_iterator = TableDataIterator.init();
            errdefer table_data_iterator.deinit();

            return LevelTableValueBlockIterator{
                .context = undefined,
                .table_data_iterator = table_data_iterator,
                .callback = .none,
            };
        }

        pub fn deinit(it: *LevelTableValueBlockIterator) void {
            it.table_data_iterator.deinit();
            it.* = undefined;
        }

        pub fn reset(it: *LevelTableValueBlockIterator) void {
            it.table_data_iterator.reset();
            it.* = .{
                .context = undefined,
                .table_data_iterator = it.table_data_iterator,
                .callback = .none,
            };
        }

        pub fn start(
            it: *LevelTableValueBlockIterator,
            context: Context,
        ) void {
            assert(it.callback == .none);
            assert(context.level < constants.lsm_levels);

            it.* = .{
                .context = context,
                .table_data_iterator = it.table_data_iterator,
                .callback = .none,
            };
            it.table_data_iterator.start(.{
                .grid = context.grid,
                .addresses = &.{},
                .checksums = &.{},
                .direction = context.direction,
            });
        }

        /// *Will* call `callback.on_data` once with the next data block,
        /// or with null if there are no more data blocks in the range.
        pub fn next(it: *LevelTableValueBlockIterator, callback: Callback) void {
            assert(it.callback == .none);
            // If this is the last table that we're iterating and it.table_data_iterator.empty()
            // is true, it.table_data_iterator.next takes care of calling callback.on_data with
            // a null data block.
            if (it.table_data_iterator.empty() and it.table_index < it.context.tables_len()) {
                // Refill `table_data_iterator` before calling `table_next`.
                const table_info = it.context.tables_get(it.table_index);
                it.callback = .{ .level_next = callback };
                it.context.grid.read_block(
                    .{ .from_local_or_global_storage = on_level_next },
                    &it.read,
                    table_info.address,
                    table_info.checksum,
                    .{ .cache_read = true, .cache_write = true },
                );
            } else {
                it.table_next(callback);
            }
        }

        fn on_level_next(
            read: *Grid.Read,
            index_block: BlockPtrConst,
        ) void {
            const it = @fieldParentPtr(LevelTableValueBlockIterator, "read", read);
            assert(it.table_data_iterator.empty());

            const callback = it.callback.level_next;
            it.callback = .none;
            // `index_block` is only valid for this callback, so copy it's contents.
            // TODO(jamii) This copy can be avoided if we bypass the cache.
            stdx.copy_disjoint(.exact, u8, it.context.index_block, index_block);

            const blocks_to_load = callback.on_index(it);

            const index_schema = schema.TableIndex.from(it.context.index_block);
            const data_addresses = index_schema.data_addresses_used(it.context.index_block);
            const data_checksums = index_schema.data_checksums_used(it.context.index_block);
            assert(data_addresses.len == data_checksums.len);

            it.table_index += 1;
            switch (blocks_to_load) {
                // In case of no data blocks to load, it's safe to call `on_data` synchronously
                // since it's already being called from a callback.
                .none => callback.on_data(it, null),
                inline .all, .range => |value, tag| {
                    it.table_data_iterator.start(.{
                        .grid = it.context.grid,
                        .addresses = if (tag == .all)
                            data_addresses
                        else
                            data_addresses[value.start..value.end],
                        .checksums = if (tag == .all)
                            data_checksums
                        else
                            data_checksums[value.start..value.end],
                        .direction = it.context.direction,
                    });
                    it.table_next(callback);
                },
            }
        }

        fn table_next(it: *LevelTableValueBlockIterator, callback: Callback) void {
            assert(it.callback == .none);
            it.callback = .{ .table_next = callback };
            it.table_data_iterator.next(on_table_next);
        }

        fn on_table_next(
            table_data_iterator: *TableDataIterator,
            data_block: ?BlockPtrConst,
        ) void {
            const it = @fieldParentPtr(
                LevelTableValueBlockIterator,
                "table_data_iterator",
                table_data_iterator,
            );
            const callback = it.callback.table_next;
            it.callback = .none;
            callback.on_data(it, data_block);
        }
    };
}
