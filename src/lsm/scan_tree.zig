const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const log = std.log.scoped(.scan);
const tracer = @import("../tracer.zig");

const stdx = @import("../stdx.zig");
const maybe = stdx.maybe;
const constants = @import("../constants.zig");
const lsm = @import("tree.zig");
const schema = @import("schema.zig");
const binary_search = @import("binary_search.zig");
const k_way_merge = @import("k_way_merge.zig");

const BinarySearchRange = binary_search.BinarySearchRange;
const Direction = @import("../direction.zig").Direction;
const GridType = @import("../vsr/grid.zig").GridType;
const BlockPtr = @import("../vsr/grid.zig").BlockPtr;
const BlockPtrConst = @import("../vsr/grid.zig").BlockPtrConst;
const TreeTableInfoType = @import("manifest.zig").TreeTableInfoType;
const ManifestType = @import("manifest.zig").ManifestType;
const ScanBuffer = @import("scan_buffer.zig").ScanBuffer;
const ScanState = @import("scan_state.zig").ScanState;
const LevelTableValueBlockIteratorType =
    @import("level_data_iterator.zig").LevelTableValueBlockIteratorType;
const Snapshot = schema.Snapshot;

/// Scans a range of keys over a Tree, in ascending or descending order.
/// At a high level, this is an ordered iterator over the values in a tree, at a particular
/// snapshot, within a given key range, merged across all levels (including the in-memory tables).
pub fn ScanTreeType(
    comptime Context: type,
    comptime Tree_: type,
    comptime Storage: type,
) type {
    return struct {
        const ScanTree = @This();

        pub const Callback = *const fn (context: Context, scan: *ScanTree) void;

        const Grid = GridType(Storage);

        const TableInfo = TreeTableInfoType(Table);
        const Manifest = ManifestType(Table, Storage);

        pub const Tree = Tree_;
        const Table = Tree.Table;
        const Key = Table.Key;
        const Value = Table.Value;
        const key_from_value = Table.key_from_value;

        const ScanTreeLevel = ScanTreeLevelType(ScanTree, Storage);

        /// KWayMerge stream identifier for each level of the LSM tree,
        /// plus the mutable and immutable tables.
        /// The `KWayMerge` API requires it to be a `u32`.
        const KWayMergeStreams = enum(u32) {
            const streams_count = constants.lsm_levels + 2;

            // Tables mutable and immutable are well-known indexes.
            table_mutable = constants.lsm_levels,
            table_immutable = constants.lsm_levels + 1,

            // The rest of the lsm levels are represented as a non-exhaustive enum.
            _,
        };

        /// KWayMergeIterator for merging results from all levels of the LSM tree.
        const KWayMergeIterator = T: {
            const stream = struct {
                fn peek(
                    scan: *ScanTree,
                    stream_index: u32,
                ) error{ Drained, Empty }!ScanTree.Key {
                    assert(stream_index < KWayMergeStreams.streams_count);

                    return switch (@as(KWayMergeStreams, @enumFromInt(stream_index))) {
                        .table_mutable => scan.merge_table_mutable_peek(),
                        .table_immutable => scan.merge_table_immutable_peek(),
                        _ => |index| scan.merge_level_peek(@intFromEnum(index)),
                    };
                }

                fn pop(scan: *ScanTree, stream_index: u32) ScanTree.Value {
                    assert(stream_index < KWayMergeStreams.streams_count);

                    return switch (@as(KWayMergeStreams, @enumFromInt(stream_index))) {
                        .table_mutable => scan.merge_table_mutable_pop(),
                        .table_immutable => scan.merge_table_immutable_pop(),
                        _ => |index| scan.merge_level_pop(@intFromEnum(index)),
                    };
                }

                // Precedence is: table_mutable > table_immutable > level 0 > level 1 > ...
                fn precedence(scan: *const ScanTree, a: u32, b: u32) bool {
                    _ = scan;
                    assert(a != b);
                    assert(a < KWayMergeStreams.streams_count);
                    assert(b < KWayMergeStreams.streams_count);

                    return switch (@as(KWayMergeStreams, @enumFromInt(a))) {
                        .table_mutable => true,
                        .table_immutable => @as(
                            KWayMergeStreams,
                            @enumFromInt(b),
                        ) != .table_mutable,
                        else => a < b and b < constants.lsm_levels,
                    };
                }
            };

            break :T k_way_merge.KWayMergeIteratorType(
                ScanTree,
                ScanTree.Key,
                ScanTree.Value,
                ScanTree.key_from_value,
                KWayMergeStreams.streams_count,
                stream.peek,
                stream.pop,
                stream.precedence,
            );
        };

        tree: *Tree,
        buffer: *const ScanBuffer,

        direction: Direction,
        key_min: Key,
        key_max: Key,
        snapshot: Snapshot,

        table_mutable_values: []const Value,
        table_immutable_values: []const Value,

        state: union(ScanState) {
            /// The scan has not been executed yet.
            /// All levels are still uninitialized.
            idle,

            /// The scan is at a valid position and ready to yield values, e.g. calling `next()`.
            /// All levels are either in the state `.buffered` or `.finished`.
            seeking,

            /// The scan needs to load data from the LSM levels, e.g. calling `read()`.
            /// At least one level is in the state `.fetching`.
            /// It's also possible for levels to be in the state `.buffered` and `.finished`.
            needs_data,

            /// The scan is attempting to load data from the LSM levels,
            /// e.g. in between calling `read()` and receiving the callback.
            /// Only levels in the state `.fetching` will load from storage.
            /// It's also possible for levels to be in the state `.buffered` and `.finished`.
            buffering: struct {
                context: Context,
                callback: Callback,
                pending_count: u32,
            },
        },
        levels: [constants.lsm_levels]ScanTreeLevel,

        merge_iterator: ?KWayMergeIterator,

        pub fn init(
            tree: *Tree,
            buffer: *const ScanBuffer,
            snapshot: Snapshot,
            key_min: Key,
            key_max: Key,
            direction: Direction,
        ) ScanTree {
            assert(key_min <= key_max);

            const table_mutable_values: []const Value = blk: {
                if (snapshot.timestamp != Snapshot.latest.timestamp) break :blk &.{};

                tree.table_mutable.sort();
                const values = tree.table_mutable.values_used();
                const range = binary_search.binary_search_values_range(
                    Key,
                    Value,
                    key_from_value,
                    values,
                    key_min,
                    key_max,
                );
                break :blk values[range.start..][0..range.count];
            };

            const table_immutable_values: []const Value = blk: {
                if (snapshot.timestamp <
                    tree.table_immutable.mutability.immutable.snapshot_min.timestamp) break :blk &.{};

                const values = tree.table_immutable.values_used();
                const range = binary_search.binary_search_values_range(
                    Key,
                    Value,
                    key_from_value,
                    values,
                    key_min,
                    key_max,
                );
                break :blk values[range.start..][0..range.count];
            };

            return .{
                .tree = tree,
                .buffer = buffer,
                .state = .idle,
                .snapshot = snapshot,
                .key_min = key_min,
                .key_max = key_max,
                .direction = direction,
                .table_mutable_values = table_mutable_values,
                .table_immutable_values = table_immutable_values,
                .levels = undefined,
                .merge_iterator = null,
            };
        }

        pub fn read(self: *ScanTree, context: Context, callback: Callback) void {
            assert(self.state == .idle or self.state == .needs_data);

            const state_before = self.state;
            self.state = .{
                .buffering = .{
                    .context = context,
                    .callback = callback,
                    .pending_count = 0,
                },
            };

            for (&self.levels, 0..) |*level, i| {
                if (state_before == .idle) {
                    // Initializing all levels for the first read.
                    level.* = ScanTreeLevel.init(
                        self,
                        self.buffer.levels[i],
                        @intCast(i),
                    );
                    level.table_next();
                }

                switch (level.values) {
                    .fetching => {
                        self.state.buffering.pending_count += 1;
                        level.fetch();
                    },
                    .buffered, .finished => assert(state_before == .needs_data),
                }
            }
        }

        /// Moves the iterator to the next position and returns its `Value` or `null` if the
        /// iterator has no more values to iterate.
        /// May return `error.ReadAgain` if a data block needs to be loaded, in this case
        /// call `read()` and resume the iteration after the read callback.
        pub fn next(self: *ScanTree) error{ReadAgain}!?Value {
            switch (self.state) {
                .idle => {
                    assert(self.merge_iterator == null);
                    return error.ReadAgain;
                },
                .seeking => return self.merge_iterator.?.pop() catch |err| switch (err) {
                    error.Drained => {
                        self.state = .needs_data;
                        return error.ReadAgain;
                    },
                },
                .needs_data => return error.ReadAgain,
                .buffering => unreachable,
            }
        }

        fn levels_read_complete(self: *ScanTree) void {
            assert(self.state == .buffering);
            assert(self.state.buffering.pending_count > 0);

            self.state.buffering.pending_count -= 1;
            if (self.state.buffering.pending_count == 0) self.read_complete();
        }

        /// The next data block for each level is available.
        fn read_complete(self: *ScanTree) void {
            assert(self.state == .buffering);
            assert(self.state.buffering.pending_count == 0);

            const context = self.state.buffering.context;
            const callback = self.state.buffering.callback;
            self.state = .seeking;

            if (self.merge_iterator == null) {
                self.merge_iterator = KWayMergeIterator.init(
                    self,
                    KWayMergeStreams.streams_count,
                    self.direction,
                );
            }

            callback(context, self);
        }

        fn merge_table_mutable_peek(self: *const ScanTree) error{ Drained, Empty }!Key {
            return self.table_memory_peek(self.table_mutable_values);
        }

        fn merge_table_immutable_peek(self: *const ScanTree) error{ Drained, Empty }!Key {
            return self.table_memory_peek(self.table_immutable_values);
        }

        fn merge_table_mutable_pop(self: *ScanTree) Value {
            return table_memory_pop(self, &self.table_mutable_values);
        }

        fn merge_table_immutable_pop(self: *ScanTree) Value {
            return table_memory_pop(self, &self.table_immutable_values);
        }

        inline fn table_memory_peek(
            self: *const ScanTree,
            values: []const Value,
        ) error{ Drained, Empty }!Key {
            assert(self.state == .seeking);

            if (values.len == 0) return error.Empty;

            const value: *const Value = switch (self.direction) {
                .ascending => &values[0],
                .descending => &values[values.len - 1],
            };

            const key = key_from_value(value);
            return key;
        }

        inline fn table_memory_pop(
            self: *ScanTree,
            field_reference: *[]const Value,
        ) Value {
            assert(self.state == .seeking);

            // The slice is re-sliced during pop,
            // updating the backing field at the end.
            var values = field_reference.*;
            defer field_reference.* = values;

            assert(values.len > 0);
            // Discarding duplicated entries from TableMemory, last entry wins:
            switch (self.direction) {
                .ascending => {
                    while (values.len > 1 and
                        key_from_value(&values[0]) ==
                        key_from_value(&values[1]))
                    {
                        values = values[1..];
                    }

                    const value_first = values[0];
                    values = values[1..];
                    return value_first;
                },
                .descending => {
                    const value_last = values[values.len - 1];
                    while (values.len > 1 and
                        key_from_value(&values[values.len - 1]) ==
                        key_from_value(&values[values.len - 2]))
                    {
                        values = values[0 .. values.len - 1];
                    }

                    values = values[0 .. values.len - 1];
                    return value_last;
                },
            }
        }

        fn merge_level_peek(self: *const ScanTree, level_index: u32) error{ Drained, Empty }!Key {
            assert(self.state == .seeking);
            assert(level_index < constants.lsm_levels);

            const level = &self.levels[level_index];
            return level.peek();
        }

        fn merge_level_pop(self: *ScanTree, level_index: u32) Value {
            assert(self.state == .seeking);
            assert(level_index < constants.lsm_levels);

            const level = &self.levels[level_index];
            return level.pop();
        }
    };
}

/// Scans over one level of the LSM Tree.
fn ScanTreeLevelType(comptime ScanTree: type, comptime Storage: type) type {
    return struct {
        const ScanTreeLevel = @This();
        const LevelTableValueBlockIterator = LevelTableValueBlockIteratorType(
            ScanTree.Table,
            Storage,
        );

        const TableInfo = ScanTree.TableInfo;
        const Manifest = ScanTree.Manifest;

        const Table = ScanTree.Table;
        const Key = Table.Key;
        const Value = Table.Value;
        const key_from_value = Table.key_from_value;

        scan: *ScanTree,
        level_index: u8,
        iterator: LevelTableValueBlockIterator,
        buffer: ScanBuffer.LevelBuffer,

        /// State over the manifest.
        manifest: union(enum) {
            begin,
            iterating: struct {
                fetch_pending: bool,
                key_exclusive_next: Key,
            },
            finished,
        },

        /// State over the values.
        values: union(enum) {
            fetching,
            buffered: []const Value,
            finished,
        },

        pub fn init(
            scan: *ScanTree,
            buffer: ScanBuffer.LevelBuffer,
            level_index: u8,
        ) ScanTreeLevel {
            assert(level_index < constants.lsm_levels);

            return .{
                .level_index = level_index,
                .scan = scan,
                .iterator = LevelTableValueBlockIterator.init(),
                .buffer = buffer,
                .manifest = .begin,
                .values = .fetching,
            };
        }

        /// Moves the level iterator to the next `table_info` that might contain the key range.
        pub fn table_next(self: *ScanTreeLevel) void {
            assert(self.manifest != .finished);
            assert(self.values == .fetching);
            assert(self.iterator.callback == .none);

            const scan: *ScanTree = self.scan;
            assert(scan.state == .buffering);

            const table_info: ?*const TableInfo = blk: {
                const manifest: *Manifest = &scan.tree.manifest;
                const key_exclusive_current: ?Key = switch (self.manifest) {
                    .begin => null,
                    .iterating => |state| key: {
                        assert(state.fetch_pending == false);
                        break :key state.key_exclusive_next;
                    },
                    .finished => unreachable,
                };

                if (manifest.next_table(.{
                    .level = self.level_index,
                    .snapshot = scan.snapshot,
                    .key_min = scan.key_min,
                    .key_max = scan.key_max,
                    .key_exclusive = key_exclusive_current,
                    .direction = scan.direction,
                })) |table_next_info| {
                    const key_exclusive_next = switch (scan.direction) {
                        .ascending => table_next_info.key_max,
                        .descending => table_next_info.key_min,
                    };

                    if (scan.key_max < key_exclusive_next) {
                        assert(scan.direction == .ascending);
                        self.manifest = .finished;
                    } else if (key_exclusive_next < scan.key_min) {
                        assert(scan.direction == .descending);
                        self.manifest = .finished;
                    } else {
                        self.manifest = .{ .iterating = .{
                            .key_exclusive_next = key_exclusive_next,
                            .fetch_pending = true,
                        } };
                    }

                    break :blk table_next_info;
                } else {
                    self.manifest = .finished;
                    break :blk null;
                }
            };

            assert(self.iterator.callback == .none);
            self.iterator.start(.{
                .grid = scan.tree.grid,
                .level = self.level_index,
                .snapshot = scan.snapshot,
                .index_block = self.buffer.index_block,
                .tables = .{ .scan = table_info },
                .direction = scan.direction,
            });
        }

        /// Fetches data from storage for the current `table_info`.
        pub fn fetch(self: *ScanTreeLevel) void {
            assert(self.values == .fetching);
            assert(self.scan.state == .buffering);
            assert(self.iterator.callback == .none);

            switch (self.manifest) {
                .begin => unreachable,
                .iterating => |*state| {
                    // Clearing the `fetch_pending` flag:
                    // `fetch()` can be called multiple times for the same `table_info`,
                    // `fetch pending` is true only the first time.
                    maybe(state.fetch_pending);
                    state.fetch_pending = false;
                },
                .finished => {},
            }

            self.iterator.next(.{
                .on_index = index_block_callback,
                .on_data = data_block_callback,
            });
        }

        pub fn peek(self: *const ScanTreeLevel) error{ Drained, Empty }!Key {
            assert(self.manifest != .begin);
            assert(self.scan.state == .seeking);

            switch (self.values) {
                .fetching => return error.Drained,
                .buffered => |values| {
                    assert(values.len > 0);
                    assert(@intFromPtr(values.ptr) >= @intFromPtr(self.buffer.data_block));
                    assert(@intFromPtr(values.ptr) <=
                        @intFromPtr(self.buffer.data_block) + self.buffer.data_block.len);

                    const value: *const Value = switch (self.scan.direction) {
                        .ascending => &values[0],
                        .descending => &values[values.len - 1],
                    };

                    const key = key_from_value(value);
                    return key;
                },
                .finished => return error.Empty,
            }
        }

        pub fn pop(self: *ScanTreeLevel) Value {
            assert(self.manifest != .begin);
            assert(self.values == .buffered);
            assert(self.scan.state == .seeking);

            var values = self.values.buffered;
            assert(values.len > 0);
            assert(@intFromPtr(values.ptr) >= @intFromPtr(self.buffer.data_block));
            assert(@intFromPtr(values.ptr) <=
                @intFromPtr(self.buffer.data_block) + self.buffer.data_block.len);

            // The buffer is re-sliced during pop,
            // updating the backing field at the end.
            defer if (values.len == 0) {
                self.values = .fetching;
            } else {
                self.values.buffered = values;
            };

            switch (self.scan.direction) {
                .ascending => {
                    const first_value = values[0];
                    values = values[1..];
                    return first_value;
                },
                .descending => {
                    const last_value = values[values.len - 1];
                    values = values[0 .. values.len - 1];
                    return last_value;
                },
            }
        }

        fn index_block_callback(
            iterator: *LevelTableValueBlockIterator,
        ) LevelTableValueBlockIterator.DataBlocksToLoad {
            const self: *ScanTreeLevel = @fieldParentPtr(ScanTreeLevel, "iterator", iterator);
            const scan: *const ScanTree = self.scan;

            assert(self.manifest != .begin);
            assert(self.values == .fetching);
            assert(scan.state == .buffering);
            assert(scan.state.buffering.pending_count > 0);

            const keys = Table.index_data_keys_used(iterator.context.index_block, .key_max);
            const indexes = binary_search.binary_search_keys_range_upsert_indexes(
                Key,
                keys,
                scan.key_min,
                scan.key_max,
            );

            if (indexes.start == keys.len) {
                // The key range was not found.
                return .none;
            } else {
                // Because we search `key_max` in the index block, if the search does not find an
                // exact match it returns the index of the next greatest key, which may contain
                // the key depending on the `key_min`.
                const end = end: {
                    const keys_min = Table.index_data_keys_used(
                        iterator.context.index_block,
                        .key_min,
                    );
                    break :end indexes.end + @intFromBool(
                        indexes.end < keys.len and keys_min[indexes.end] <= scan.key_max,
                    );
                };

                return if (indexes.start == end) .none else .{
                    .range = .{
                        .start = indexes.start,
                        .end = end,
                    },
                };
            }
        }

        fn data_block_callback(
            iterator: *LevelTableValueBlockIterator,
            data_block: ?BlockPtrConst,
        ) void {
            const self: *ScanTreeLevel = @fieldParentPtr(ScanTreeLevel, "iterator", iterator);
            const scan: *ScanTree = self.scan;

            assert(self.manifest != .begin);
            assert(self.values == .fetching);
            assert(scan.state == .buffering);
            assert(scan.state.buffering.pending_count > 0);

            if (data_block) |data| {
                stdx.copy_disjoint(.exact, u8, self.buffer.data_block, data);

                const values = Table.data_block_values_used(self.buffer.data_block);
                const range = binary_search.binary_search_values_range(
                    Key,
                    Value,
                    key_from_value,
                    values,
                    scan.key_min,
                    scan.key_max,
                );

                if (range.count > 0) {
                    self.values = .{ .buffered = values[range.start..][0..range.count] };
                } else {
                    // The `data_block` *might* contain the scan range,
                    // otherwise, it shouldn't have been returned by the iterator.
                    assert(key_from_value(&values[0]) < scan.key_min and
                        scan.key_max < key_from_value(&values[values.len - 1]));

                    // Keep loading from storage.
                    self.values = .fetching;
                }
            } else {
                switch (self.manifest) {
                    .begin => unreachable,
                    .iterating => self.table_next(),
                    .finished => self.values = .finished,
                }
            }

            switch (self.values) {
                .fetching => self.fetch(),
                .buffered, .finished => scan.levels_read_complete(),
            }
        }
    };
}
