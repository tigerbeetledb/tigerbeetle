const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const meta = std.meta;
const maybe = stdx.maybe;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const lsm = @import("tree.zig");
const binary_search = @import("binary_search.zig");

const Direction = @import("../direction.zig").Direction;
const SortedSegmentedArrayType = @import("segmented_array.zig").SortedSegmentedArrayType;

pub fn ManifestLevelType(
    comptime NodePool: type,
    comptime Key: type,
    comptime TableInfo: type,
    comptime table_count_max_tree: u32,
) type {
    comptime assert(@typeInfo(Key) == .Int or @typeInfo(Key) == .ComptimeInt);

    return struct {
        const ManifestLevel = @This();

        pub const Keys = SortedSegmentedArrayType(
            Key,
            NodePool,
            table_count_max_tree,
            Key,
            struct {
                inline fn key_from_value(value: *const Key) Key {
                    return value.*;
                }
            }.key_from_value,
            .{},
        );

        pub const Tables = SortedSegmentedArrayType(
            TableInfo,
            NodePool,
            table_count_max_tree,
            KeyMaxSnapshotMin.Int,
            struct {
                inline fn key_from_value(table_info: *const TableInfo) KeyMaxSnapshotMin.Int {
                    return KeyMaxSnapshotMin.key_from_value(.{
                        .key_max = table_info.key_max,
                        .snapshot_min = table_info.snapshot_min,
                    });
                }
            }.key_from_value,
            .{},
        );

        pub const KeyMaxSnapshotMin = packed struct(KeyMaxSnapshotMin.Int) {
            pub const Int = std.meta.Int(
                .unsigned,
                @bitSizeOf(u64) + @bitSizeOf(Key),
            );

            // The tables are ordered by (key_max,snapshot_min),
            // fields are declared from the least significant to the most significant:

            snapshot_min: u64,
            key_max: Key,

            pub inline fn key_from_value(value: KeyMaxSnapshotMin) Int {
                return @bitCast(value);
            }
        };

        // A direct reference to a TableInfo within the Tables array.
        pub const TableInfoReference = struct { table_info: *TableInfo, generation: u32 };

        pub const LeastOverlapTable = struct {
            table: TableInfoReference,
            range: OverlapRange,
        };

        pub const OverlapRange = struct {
            /// The minimum key across both levels.
            key_min: Key,
            /// The maximum key across both levels.
            key_max: Key,
            // References to tables in level B that intersect with the chosen table in level A.
            tables: stdx.BoundedArrayType(TableInfoReference, constants.lsm_growth_factor),
        };

        pub const LevelKeyRange = struct {
            key_range: ?KeyRange,

            /// Excludes the specified range from the level's key range, i.e. if the specified range
            /// contributes to the level's key_min/key_max, find a new key_min/key_max.
            ///
            /// This is achieved by querying the tables visible to snapshot_latest and updating
            /// level key_min/key_max to the key_min/key_max of the first table returned by the
            /// iterator. The query is guaranteed to only fetch non-snapshotted tables, since
            /// tables visible to old snapshots that users have retained would have
            /// snapshot_max set to a non math.maxInt(u64) value. Therefore, they wouldn't
            /// be visible to queries with snapshot_latest (math.maxInt(u64 - 1)).
            fn exclude(self: *LevelKeyRange, exclude_range: KeyRange) void {
                assert(self.key_range != null);

                var level: *ManifestLevel = @fieldParentPtr("key_range_latest", self);
                if (level.table_count_visible == 0) {
                    self.key_range = null;
                    return;
                }

                const snapshots = &[1]u64{lsm.snapshot_latest};
                if (exclude_range.key_max == self.key_range.?.key_max) {
                    var itr = level.iterator(.visible, snapshots, .descending, null);
                    const table: ?*const TableInfo = itr.next();
                    assert(table != null);
                    self.key_range.?.key_max = table.?.key_max;
                }
                if (exclude_range.key_min == self.key_range.?.key_min) {
                    var itr = level.iterator(.visible, snapshots, .ascending, null);
                    const table: ?*const TableInfo = itr.next();
                    assert(table != null);
                    self.key_range.?.key_min = table.?.key_min;
                }
                assert(self.key_range != null);
                assert(self.key_range.?.key_min <= self.key_range.?.key_max);
            }

            fn include(self: *LevelKeyRange, include_range: KeyRange) void {
                if (self.key_range) |*level_range| {
                    if (include_range.key_min < level_range.key_min) {
                        level_range.key_min = include_range.key_min;
                    }
                    if (include_range.key_max > level_range.key_max) {
                        level_range.key_max = include_range.key_max;
                    }
                } else {
                    self.key_range = include_range;
                }
                assert(self.key_range != null);
                assert(self.key_range.?.key_min <= self.key_range.?.key_max);
                assert(self.key_range.?.key_min <= include_range.key_min and
                    include_range.key_max <= self.key_range.?.key_max);
            }

            inline fn contains(self: *const LevelKeyRange, key: Key) bool {
                return (self.key_range != null) and
                    self.key_range.?.key_min <= key and
                    key <= self.key_range.?.key_max;
            }
        };

        // These two segmented arrays are parallel. That is, the absolute indexes of maximum key
        // and corresponding TableInfo are the same. However, the number of nodes, node index, and
        // relative index into the node differ as the elements per node are different.
        //
        // Ordered by ascending (maximum) key. Keys may repeat due to snapshots.
        keys: Keys,
        tables: Tables,

        /// The range of keys in this level covered by tables visible to snapshot_latest.
        key_range_latest: LevelKeyRange = .{ .key_range = null },

        /// The number of tables visible to snapshot_latest.
        /// Used to enforce table_count_max_tree_for_level().
        // TODO Track this in Manifest instead, since it knows both when tables are
        // added/updated/removed, and also knows the superblock's persisted snapshots.
        table_count_visible: u32 = 0,

        /// A monotonically increasing generation number that is used detect invalid internal
        /// TableInfo references.
        generation: u32 = 0,

        pub fn init(level: *ManifestLevel, allocator: mem.Allocator) !void {
            level.* = .{
                .keys = undefined,
                .tables = undefined,
            };

            level.keys = try Keys.init(allocator);
            errdefer level.keys.deinit(allocator, null);

            level.tables = try Tables.init(allocator);
            errdefer level.tables.deinit(allocator, null);
        }

        pub fn deinit(level: *ManifestLevel, allocator: mem.Allocator, node_pool: *NodePool) void {
            level.keys.deinit(allocator, node_pool);
            level.tables.deinit(allocator, node_pool);

            level.* = undefined;
        }

        pub fn reset(level: *ManifestLevel) void {
            level.keys.reset();
            level.tables.reset();

            level.* = .{
                .keys = level.keys,
                .tables = level.tables,
                .generation = level.generation + 1,
            };
        }

        /// Inserts the given table into the ManifestLevel.
        pub fn insert_table(
            level: *ManifestLevel,
            node_pool: *NodePool,
            table: *const TableInfo,
        ) void {
            if (constants.verify) {
                assert(!level.contains(table));
            }
            assert(level.keys.len() == level.tables.len());

            const absolute_index_keys = level.keys.insert_element(node_pool, table.key_max);
            assert(absolute_index_keys < level.keys.len());

            const absolute_index_tables = level.tables.insert_element(node_pool, table.*);
            assert(absolute_index_tables < level.tables.len());

            if (table.visible(lsm.snapshot_latest)) level.table_count_visible += 1;
            level.generation +%= 1;

            level.key_range_latest.include(KeyRange{
                .key_min = table.key_min,
                .key_max = table.key_max,
            });

            if (constants.verify) {
                assert(level.contains(table));

                // `keys` may have duplicate entries due to tables with the same key_max, but
                // different snapshots.
                maybe(absolute_index_keys != absolute_index_tables);

                var keys_iterator =
                    level.keys.iterator_from_index(absolute_index_tables, .ascending);
                var tables_iterator =
                    level.tables.iterator_from_index(absolute_index_keys, .ascending);

                assert(keys_iterator.next().?.* == table.key_max);
                assert(tables_iterator.next().?.key_max == table.key_max);
            }
            assert(level.keys.len() == level.tables.len());
        }

        /// Set snapshot_max for the given table in the ManifestLevel.
        /// * The table is mutable so that this function can update its snapshot.
        /// * Asserts that the table currently has snapshot_max of math.maxInt(u64).
        /// * Asserts that the table exists in the manifest.
        pub fn set_snapshot_max(
            level: *ManifestLevel,
            snapshot: u64,
            table_ref: TableInfoReference,
        ) void {
            var table = table_ref.table_info;

            assert(table_ref.generation == level.generation);
            if (constants.verify) {
                assert(level.contains(table));
            }
            assert(snapshot < lsm.snapshot_latest);
            assert(table.snapshot_max == math.maxInt(u64));
            assert(table.key_min <= table.key_max);

            table.snapshot_max = snapshot;
            level.table_count_visible -= 1;
            level.key_range_latest.exclude(KeyRange{
                .key_min = table.key_min,
                .key_max = table.key_max,
            });
        }

        /// Remove the given table.
        /// The `table` parameter must *not* be a pointer into the `tables`' SegmentedArray memory.
        pub fn remove_table(
            level: *ManifestLevel,
            node_pool: *NodePool,
            table: *const TableInfo,
        ) void {
            assert(level.keys.len() == level.tables.len());
            assert(table.key_min <= table.key_max);

            // Use `key_min` for both ends of the iterator; we are looking for a single table.
            const cursor_start = level.iterator_start(table.key_min, table.key_min, .ascending).?;

            var i = level.keys.absolute_index_for_cursor(cursor_start);
            var tables = level.tables.iterator_from_index(i, .ascending);
            const table_index_absolute = while (tables.next()) |level_table| : (i += 1) {
                // The `table` parameter should *not* be a pointer into the `tables` SegmentedArray
                // memory, since it will be invalidated by `tables.remove_elements()`.
                assert(level_table != table);

                if (level_table.equal(table)) break i;
                assert(level_table.checksum != table.checksum);
                assert(level_table.address != table.address);
            } else {
                @panic("ManifestLevel.remove_table: table not found");
            };

            level.generation +%= 1;
            level.keys.remove_elements(node_pool, table_index_absolute, 1);
            level.tables.remove_elements(node_pool, table_index_absolute, 1);
            assert(level.keys.len() == level.tables.len());

            if (table.visible(lsm.snapshot_latest)) {
                level.table_count_visible -= 1;

                level.key_range_latest.exclude(.{
                    .key_min = table.key_min,
                    .key_max = table.key_max,
                });
            }
        }

        /// Returns True if the given key may be present in the ManifestLevel,
        /// False if the key is guaranteed to not be present.
        ///
        /// Our key range keeps track of tables that are visible to snapshot_latest, so it cannot
        /// be relied upon for queries to older snapshots.
        pub fn key_range_contains(level: *const ManifestLevel, snapshot: u64, key: Key) bool {
            if (snapshot == lsm.snapshot_latest) {
                return level.key_range_latest.contains(key);
            } else {
                return true;
            }
        }

        pub const Visibility = enum {
            visible,
            invisible,
        };

        pub const KeyRange = struct {
            key_min: Key, // Inclusive.
            key_max: Key, // Inclusive.
        };

        pub fn iterator(
            level: *const ManifestLevel,
            visibility: Visibility,
            snapshots: []const u64,
            direction: Direction,
            key_range: ?KeyRange,
        ) Iterator {
            for (snapshots) |snapshot| {
                assert(snapshot <= lsm.snapshot_latest);
            }

            const inner = blk: {
                if (key_range) |range| {
                    assert(range.key_min <= range.key_max);

                    if (level.iterator_start(range.key_min, range.key_max, direction)) |start| {
                        break :blk level.tables.iterator_from_index(
                            level.keys.absolute_index_for_cursor(start),
                            direction,
                        );
                    } else {
                        break :blk Tables.Iterator{
                            .array = &level.tables,
                            .direction = direction,
                            .cursor = .{ .node = 0, .relative_index = 0 },
                            .done = true,
                        };
                    }
                } else {
                    switch (direction) {
                        .ascending => break :blk level.tables.iterator_from_index(0, direction),
                        .descending => {
                            break :blk level.tables.iterator_from_cursor(
                                level.tables.last(),
                                .descending,
                            );
                        },
                    }
                }
            };

            return .{
                .level = level,
                .inner = inner,
                .visibility = visibility,
                .snapshots = snapshots,
                .direction = direction,
                .key_range = key_range,
            };
        }

        pub const Iterator = struct {
            level: *const ManifestLevel,
            inner: Tables.Iterator,
            visibility: Visibility,
            snapshots: []const u64,
            direction: Direction,
            key_range: ?KeyRange,

            pub fn next(it: *Iterator) ?*TableInfo {
                while (it.inner.next()) |table| {
                    // We can't assert !it.inner.done as inner.next() may set done before returning.

                    // Skip tables that don't match the provided visibility interests.
                    switch (it.visibility) {
                        .invisible => blk: {
                            if (table.invisible(it.snapshots)) break :blk;
                            continue;
                        },
                        .visible => blk: {
                            for (it.snapshots) |snapshot| {
                                if (table.visible(snapshot)) break :blk;
                            }
                            continue;
                        },
                    }

                    // Filter the table using the key range if provided.
                    if (it.key_range) |key_range| {
                        switch (it.direction) {
                            .ascending => {
                                // Assert that the table is not out of bounds to the left.
                                //
                                // We can assert this as it is exactly the same key comparison when
                                // we binary search in iterator_start(), and since we move in
                                // ascending order this remains true beyond the first iteration.
                                assert(key_range.key_min <= table.key_max);

                                // Check if the table is out of bounds to the right.
                                if (table.key_min > key_range.key_max) {
                                    it.inner.done = true;
                                    return null;
                                }
                            },
                            .descending => {
                                // Check if the table is out of bounds to the right.
                                //
                                // Unlike in the ascending case, it is not guaranteed that
                                // table.key_min is less than or equal to key_range.key_max on the
                                // first iteration as the underlying SegmentedArray.search uses
                                // .upper_bound regardless of .direction.
                                if (table.key_min > key_range.key_max) {
                                    continue;
                                }

                                // Check if the table is out of bounds to the left.
                                if (table.key_max < key_range.key_min) {
                                    it.inner.done = true;
                                    return null;
                                }
                            },
                        }
                    }

                    return table;
                }

                assert(it.inner.done);
                return null;
            }
        };

        /// Returns the keys segmented array cursor at which iteration should be started.
        /// May return null if there is nothing to iterate because we know for sure that the key
        /// range is disjoint with the tables stored in this level.
        ///
        /// However, the cursor returned is not guaranteed to be in range for the query as only
        /// the key_max is stored in the index structures, not the key_min, and only the start
        /// bound for the given direction is checked here.
        fn iterator_start(
            level: ManifestLevel,
            key_min: Key,
            key_max: Key,
            direction: Direction,
        ) ?Keys.Cursor {
            assert(key_min <= key_max);
            assert(level.keys.len() == level.tables.len());

            if (level.keys.len() == 0) return null;

            // Ascending:  Find the first table where table.key_max ≥ iterator.key_min.
            // Descending: Find the first table where table.key_max ≥ iterator.key_max.
            const target = level.keys.search(switch (direction) {
                .ascending => key_min,
                .descending => key_max,
            });
            assert(target.node <= level.keys.node_count);

            if (level.keys.absolute_index_for_cursor(target) == level.keys.len()) {
                return switch (direction) {
                    // The key_min of the target range is greater than the key_max of the last
                    // table in the level and we are ascending, so this range matches no tables
                    // on this level.
                    .ascending => null,
                    // The key_max of the target range is greater than the key_max of the last
                    // table in the level and we are descending, so we need to start iteration
                    // at the last table in the level.
                    .descending => level.keys.last(),
                };
            } else {
                // Multiple tables in the level may share a key.
                // Scan to the edge so that the iterator will cover them all.
                return level.iterator_start_boundary(target, direction);
            }
        }

        /// This function exists because there may be tables in the level with the same
        /// key_max but non-overlapping snapshot visibility.
        ///
        /// Put differently, there may be several tables with different snapshots but the same
        /// `key_max`, and `iterator_start`'s binary search (`key_cursor`) may have landed in the
        /// middle of them.
        fn iterator_start_boundary(
            level: ManifestLevel,
            key_cursor: Keys.Cursor,
            direction: Direction,
        ) Keys.Cursor {
            var reverse = level.keys.iterator_from_cursor(key_cursor, direction.reverse());
            assert(meta.eql(reverse.cursor, key_cursor));

            // This cursor will always point to a key equal to start_key.
            var adjusted = reverse.cursor;
            const start_key = reverse.next().?.*;
            assert(start_key == level.keys.element_at_cursor(adjusted));

            var adjusted_next = reverse.cursor;
            while (reverse.next()) |k| {
                if (start_key != k.*) break;
                adjusted = adjusted_next;
                adjusted_next = reverse.cursor;
            } else {
                switch (direction) {
                    .ascending => assert(meta.eql(adjusted, level.keys.first())),
                    .descending => assert(meta.eql(adjusted, level.keys.last())),
                }
            }
            assert(start_key == level.keys.element_at_cursor(adjusted));

            return adjusted;
        }

        /// The function is only used for verification; it is not performance-critical.
        pub fn contains(level: ManifestLevel, table: *const TableInfo) bool {
            comptime assert(constants.verify);
            var level_tables = level.iterator(.visible, &.{
                table.snapshot_min,
            }, .ascending, KeyRange{
                .key_min = table.key_min,
                .key_max = table.key_max,
            });
            while (level_tables.next()) |level_table| {
                if (level_table.equal(table)) return true;
            }
            return false;
        }

        /// Given two levels (where A is the level on which this function
        /// is invoked and B is the other level), finds a table in Level A that
        /// overlaps with the least number of tables in Level B.
        ///
        /// * Exits early if it finds a table that doesn't overlap with any
        ///   tables in the second level.
        pub fn table_with_least_overlap(
            level_a: *const ManifestLevel,
            level_b: *const ManifestLevel,
            snapshot: u64,
            max_overlapping_tables: usize,
        ) ?LeastOverlapTable {
            assert(max_overlapping_tables <= constants.lsm_growth_factor);

            var optimal: ?LeastOverlapTable = null;
            const snapshots = [1]u64{snapshot};
            var iterations: usize = 0;
            var it = level_a.iterator(
                .visible,
                &snapshots,
                .ascending,
                null, // All visible tables in the level therefore no KeyRange filter.
            );

            while (it.next()) |table| {
                iterations += 1;

                const range = level_b.tables_overlapping_with_key_range(
                    table.key_min,
                    table.key_max,
                    snapshot,
                    max_overlapping_tables,
                ) orelse continue;
                assert(range.tables.count() <= max_overlapping_tables);

                if (optimal == null or range.tables.count() < optimal.?.range.tables.count()) {
                    optimal = LeastOverlapTable{
                        .table = TableInfoReference{
                            .table_info = table,
                            .generation = level_a.generation,
                        },
                        .range = range,
                    };
                }
                // If the table can be moved directly between levels then that is already optimal.
                if (optimal.?.range.tables.empty()) break;
            }
            assert(iterations > 0);
            assert(iterations == level_a.table_count_visible or
                optimal.?.range.tables.empty());

            return optimal.?;
        }

        /// Returns the next table in the range, after `key_exclusive` if provided.
        ///
        /// * The table returned is visible to `snapshot`.
        pub fn next_table(self: *const ManifestLevel, parameters: struct {
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            key_exclusive: ?Key,
            direction: Direction,
        }) ?TableInfoReference {
            const key_min = parameters.key_min;
            const key_max = parameters.key_max;
            const key_exclusive = parameters.key_exclusive;
            const direction = parameters.direction;
            const snapshot = parameters.snapshot;
            const snapshots = [_]u64{snapshot};

            assert(key_min <= key_max);

            if (key_exclusive == null) {
                var it = self.iterator(
                    .visible,
                    &snapshots,
                    direction,
                    KeyRange{ .key_min = key_min, .key_max = key_max },
                );
                if (it.next()) |table_info| {
                    return .{
                        .table_info = table_info,
                        .generation = self.generation,
                    };
                } else {
                    return null;
                }
            }

            assert(key_min <= key_exclusive.?);
            assert(key_exclusive.? <= key_max);

            const key_min_exclusive = if (direction == .ascending) key_exclusive.? else key_min;
            const key_max_exclusive = if (direction == .descending) key_exclusive.? else key_max;
            assert(key_min_exclusive <= key_max_exclusive);

            var it = self.iterator(
                .visible,
                &snapshots,
                direction,
                KeyRange{ .key_min = key_min_exclusive, .key_max = key_max_exclusive },
            );

            while (it.next()) |table| {
                assert(table.visible(snapshot));
                assert(table.key_min <= table.key_max);
                assert(key_min_exclusive <= table.key_max);
                assert(table.key_min <= key_max_exclusive);

                // These conditions are required to avoid iterating over the same
                // table twice. This is because the invoker sets key_exclusive to the
                // key_max or key_max of the previous table returned by this function,
                // based on the direction of iteration (ascending/descending).
                // key_exclusive is then set as KeyRange.key_min or KeyRange.key_max for the next
                // ManifestLevel query. This query would return the same table again,
                // so it needs to be skipped.
                const next = switch (direction) {
                    .ascending => table.key_min > key_exclusive.?,
                    .descending => table.key_max < key_exclusive.?,
                };
                if (next) {
                    return .{
                        .table_info = table,
                        .generation = self.generation,
                    };
                }
            }

            return null;
        }

        /// Returns the smallest visible range of tables in the given level
        /// that overlap with the given range: [key_min, key_max]
        ///
        /// Returns null if the number of tables that intersect with the range intersects more than
        /// max_overlapping_tables tables.
        ///
        /// The range keys are guaranteed to encompass all the relevant level A and level B tables:
        ///   range.key_min = min(a.key_min, b.key_min)
        ///   range.key_max = max(a.key_max, b.key_max)
        ///
        /// This last invariant is critical to ensuring that tombstones are dropped correctly.
        ///
        /// * Assumption: Currently, we only support a maximum of lsm_growth_factor
        ///   overlapping tables. This is because OverlapRange.tables is a
        ///   BoundedArray of size lsm_growth_factor. This works with our current
        ///   compaction strategy that is guaranteed to choose a table with that
        ///   intersects with <= lsm_growth_factor tables in the next level.
        pub fn tables_overlapping_with_key_range(
            level: *const ManifestLevel,
            key_min: Key,
            key_max: Key,
            snapshot: u64,
            max_overlapping_tables: usize,
        ) ?OverlapRange {
            assert(max_overlapping_tables <= constants.lsm_growth_factor);

            var range = OverlapRange{
                .key_min = key_min,
                .key_max = key_max,
                .tables = .{},
            };
            const snapshots = [1]u64{snapshot};
            var it = level.iterator(
                .visible,
                &snapshots,
                .ascending,
                KeyRange{ .key_min = range.key_min, .key_max = range.key_max },
            );

            while (it.next()) |table| {
                assert(table.visible(lsm.snapshot_latest));
                assert(table.key_min <= table.key_max);
                assert(range.key_min <= table.key_max);
                assert(table.key_min <= range.key_max);

                // The first iterated table.key_min/max may overlap range.key_min/max entirely.
                if (table.key_min < range.key_min) {
                    range.key_min = table.key_min;
                }

                // Thereafter, iterated tables may/may not extend the range in ascending order.
                if (table.key_max > range.key_max) {
                    range.key_max = table.key_max;
                }
                if (range.tables.count() < max_overlapping_tables) {
                    const table_info_reference = TableInfoReference{
                        .table_info = table,
                        .generation = level.generation,
                    };
                    range.tables.append_assume_capacity(table_info_reference);
                } else {
                    return null;
                }
            }
            assert(range.key_min <= range.key_max);
            assert(range.key_min <= key_min);
            assert(range.tables.count() <= max_overlapping_tables);
            assert(key_max <= range.key_max);

            return range;
        }
    };
}

pub fn TestContextType(
    comptime node_size: u32,
    comptime Key: type,
    comptime table_count_max_tree: u32,
) type {
    return struct {
        const TestContext = @This();

        const testing = std.testing;

        const log = false;

        const Value = packed struct {
            key: Key,
            tombstone: bool,
            padding: u63 = 0,

            comptime {
                assert(stdx.no_padding(Value));
                assert(@bitSizeOf(Value) == @sizeOf(Value) * 8);
            }
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

        const TableInfo = @import("manifest.zig").TreeTableInfoType(Table);
        const NodePoolType = @import("node_pool.zig").NodePoolType;

        const TestPool = NodePoolType(node_size, @alignOf(TableInfo));
        const TestLevel = ManifestLevelType(TestPool, Key, TableInfo, table_count_max_tree);
        const KeyRange = TestLevel.KeyRange;

        random: std.rand.Random,

        pool: TestPool,
        level: TestLevel,

        snapshot_max: u64 = 1,
        snapshots: stdx.BoundedArrayType(u64, 8) = .{},
        snapshot_tables: stdx.BoundedArrayType(std.ArrayList(TableInfo), 8) = .{},

        /// Contains only tables with snapshot_max == lsm.snapshot_latest
        reference: std.ArrayList(TableInfo),

        inserts: u64 = 0,
        removes: u64 = 0,

        fn init(context: *TestContext, random: std.rand.Random) !void {
            context.* = .{
                .random = random,

                .pool = undefined,
                .level = undefined,
                .reference = undefined,
            };

            try context.pool.init(
                testing.allocator,
                TestLevel.Keys.node_count_max + TestLevel.Tables.node_count_max,
            );
            errdefer context.pool.deinit(testing.allocator);

            try context.level.init(testing.allocator);
            errdefer context.level.deinit(testing.allocator, &context.pool);

            context.reference = std.ArrayList(TableInfo).init(testing.allocator);
            errdefer context.reference.deinit();
        }

        fn deinit(context: *TestContext) void {
            context.level.deinit(testing.allocator, &context.pool);
            context.pool.deinit(testing.allocator);

            for (context.snapshot_tables.slice()) |tables| tables.deinit();

            context.reference.deinit();
        }

        fn run(context: *TestContext) !void {
            if (log) std.debug.print("\n", .{});

            {
                var i: usize = 0;
                while (i < table_count_max_tree * 2) : (i += 1) {
                    switch (context.random.uintLessThanBiased(u32, 100)) {
                        0...59 => try context.insert_tables(),
                        60...69 => try context.create_snapshot(),
                        70...94 => try context.delete_tables(),
                        95...99 => try context.drop_snapshot(),
                        else => unreachable,
                    }
                }
            }

            {
                var i: usize = 0;
                while (i < table_count_max_tree * 2) : (i += 1) {
                    switch (context.random.uintLessThanBiased(u32, 100)) {
                        0...34 => try context.insert_tables(),
                        35...39 => try context.create_snapshot(),
                        40...89 => try context.delete_tables(),
                        90...99 => try context.drop_snapshot(),
                        else => unreachable,
                    }
                }
            }

            try context.remove_all();
        }

        fn insert_tables(context: *TestContext) !void {
            const count_free = table_count_max_tree - context.level.keys.len();

            if (count_free == 0) return;

            var buffer: [13]TableInfo = undefined;

            const count_max = @min(count_free, 13);
            const count = context.random.uintAtMostBiased(u32, count_max - 1) + 1;

            {
                var key: Key = context.random.uintAtMostBiased(Key, table_count_max_tree * 64);

                for (buffer[0..count]) |*table| {
                    table.* = context.random_greater_non_overlapping_table(key);
                    key = table.key_max;
                }
            }

            for (buffer[0..count]) |*table| {
                context.level.insert_table(&context.pool, table);
            }

            for (buffer[0..count]) |table| {
                const index = binary_search.binary_search_values_upsert_index(
                    Key,
                    TableInfo,
                    key_min_from_table,
                    context.reference.items,
                    table.key_max,
                    .{},
                );
                // Can't be equal as the tables may not overlap
                if (index < context.reference.items.len) {
                    assert(context.reference.items[index].key_min > table.key_max);
                }
                context.reference.insert(index, table) catch unreachable;
            }

            context.inserts += count;

            try context.verify();
        }

        fn random_greater_non_overlapping_table(context: *TestContext, key: Key) TableInfo {
            var new_key_min = key + context.random.uintLessThanBiased(Key, 31) + 1;

            assert(new_key_min > key);

            const i = binary_search.binary_search_values_upsert_index(
                Key,
                TableInfo,
                key_min_from_table,
                context.reference.items,
                new_key_min,
                .{},
            );

            if (i > 0) {
                if (new_key_min <= context.reference.items[i - 1].key_max) {
                    new_key_min = context.reference.items[i - 1].key_max + 1;
                }
            }

            const next_key_min = for (context.reference.items[i..]) |table| {
                switch (std.math.order(new_key_min, table.key_min)) {
                    .lt => break table.key_min,
                    .eq => new_key_min = table.key_max + 1,
                    .gt => unreachable,
                }
            } else math.maxInt(Key);

            const max_delta = @min(32, next_key_min - 1 - new_key_min);
            const new_key_max = new_key_min + context.random.uintAtMostBiased(Key, max_delta);

            return .{
                .checksum = context.random.int(u128),
                .address = context.random.int(u64),
                .snapshot_min = context.take_snapshot(),
                .key_min = new_key_min,
                .key_max = new_key_max,
                .value_count = context.random.int(u32),
            };
        }

        /// See Manifest.take_snapshot()
        fn take_snapshot(context: *TestContext) u64 {
            // A snapshot cannot be 0 as this is a reserved value in the superblock.
            assert(context.snapshot_max > 0);
            // The constant snapshot_latest must compare greater than any issued snapshot.
            // This also ensures that we are not about to overflow the u64 counter.
            assert(context.snapshot_max < lsm.snapshot_latest - 1);

            context.snapshot_max += 1;

            return context.snapshot_max;
        }

        fn create_snapshot(context: *TestContext) !void {
            if (context.snapshots.full()) return;

            context.snapshots.append_assume_capacity(context.take_snapshot());

            const tables = context.snapshot_tables.add_one_assume_capacity();
            tables.* = std.ArrayList(TableInfo).init(testing.allocator);
            try tables.insertSlice(0, context.reference.items);
        }

        fn drop_snapshot(context: *TestContext) !void {
            if (context.snapshots.empty()) return;

            const index = context.random.uintLessThanBiased(usize, context.snapshots.count());

            _ = context.snapshots.swap_remove(index);
            var tables = context.snapshot_tables.swap_remove(index);
            defer tables.deinit();

            // Use this memory as a scratch buffer since it's conveniently already allocated.
            tables.clearRetainingCapacity();

            const snapshots = context.snapshots.slice();

            // Ensure that iteration with a null key range in both directions is tested.
            if (context.random.boolean()) {
                var it = context.level.iterator(.invisible, snapshots, .ascending, null);
                while (it.next()) |table| try tables.append(table.*);
            } else {
                var it = context.level.iterator(.invisible, snapshots, .descending, null);
                while (it.next()) |table| try tables.append(table.*);
                mem.reverse(TableInfo, tables.items);
            }

            if (tables.items.len > 0) {
                for (tables.items) |*table| {
                    context.level.remove_table(&context.pool, table);
                }
            }
        }

        fn delete_tables(context: *TestContext) !void {
            const reference_len: u32 = @intCast(context.reference.items.len);
            if (reference_len == 0) return;

            const count_max = @min(reference_len, 13);
            const count = context.random.uintAtMostBiased(u32, count_max - 1) + 1;

            assert(context.reference.items.len <= table_count_max_tree);
            const index = context.random.uintAtMostBiased(u32, reference_len - count);

            const snapshot = context.take_snapshot();

            for (context.reference.items[index..][0..count]) |*table| {
                const cursor_start = context.level.iterator_start(
                    table.key_min,
                    table.key_min,
                    .ascending,
                ).?;
                const absolute_index = context.level.keys.absolute_index_for_cursor(cursor_start);

                var it = context.level.tables.iterator_from_index(absolute_index, .ascending);
                while (it.next()) |level_table| {
                    if (level_table.equal(table)) {
                        context.level.set_snapshot_max(snapshot, .{
                            .table_info = level_table,
                            .generation = context.level.generation,
                        });
                        table.snapshot_max = snapshot;
                        break;
                    }
                }
            }

            for (context.snapshot_tables.slice()) |tables| {
                for (tables.items) |*table| {
                    for (context.reference.items[index..][0..count]) |modified| {
                        if (table.address == modified.address) {
                            table.snapshot_max = snapshot;
                            assert(table.equal(&modified));
                        }
                    }
                }
            }

            {
                var to_remove = std.ArrayList(TableInfo).init(testing.allocator);
                defer to_remove.deinit();

                for (context.reference.items[index..][0..count]) |table| {
                    if (table.invisible(context.snapshots.slice())) {
                        try to_remove.append(table);
                    }
                }

                if (log) {
                    std.debug.print("Removing tables: ", .{});
                    for (to_remove.items) |t| {
                        std.debug.print("[{},{}], ", .{ t.key_min, t.key_max });
                    }
                    std.debug.print("\n", .{});
                    std.debug.print("\nactual: ", .{});
                    var it = context.level.iterator(
                        .invisible,
                        context.snapshots.slice(),
                        .ascending,
                        KeyRange{ .key_min = 0, .key_max = math.maxInt(Key) },
                    );
                    while (it.next()) |t| std.debug.print("[{},{}], ", .{ t.key_min, t.key_max });
                    std.debug.print("\n", .{});
                }

                if (to_remove.items.len > 0) {
                    for (to_remove.items) |*table| {
                        context.level.remove_table(&context.pool, table);
                    }
                }
            }

            context.reference.replaceRange(index, count, &[0]TableInfo{}) catch unreachable;

            context.removes += count;

            try context.verify();
        }

        fn remove_all(context: *TestContext) !void {
            while (context.snapshots.count() > 0) try context.drop_snapshot();
            while (context.reference.items.len > 0) try context.delete_tables();

            try testing.expectEqual(@as(u32, 0), context.level.keys.len());
            try testing.expectEqual(@as(u32, 0), context.level.tables.len());
            try testing.expect(context.inserts > 0);
            try testing.expect(context.inserts == context.removes);

            if (log) {
                std.debug.print("\ninserts: {}, removes: {}\n", .{
                    context.inserts,
                    context.removes,
                });
            }

            try context.verify();
        }

        fn verify(context: *TestContext) !void {
            try context.verify_snapshot(lsm.snapshot_latest, context.reference.items);

            for (context.snapshots.slice(), 0..) |snapshot, i| {
                try context.verify_snapshot(snapshot, context.snapshot_tables.get(i).items);
            }
        }

        fn verify_snapshot(
            context: *TestContext,
            snapshot: u64,
            reference: []const TableInfo,
        ) !void {
            if (log) {
                std.debug.print("\nsnapshot: {}\n", .{snapshot});
                std.debug.print("expect: ", .{});
                for (reference) |t| std.debug.print("[{},{}], ", .{ t.key_min, t.key_max });

                std.debug.print("\nactual: ", .{});
                var it = context.level.iterator(
                    .visible,
                    @as(*const [1]u64, &snapshot),
                    .ascending,
                    KeyRange{ .key_min = 0, .key_max = math.maxInt(Key) },
                );
                while (it.next()) |t| std.debug.print("[{},{}], ", .{ t.key_min, t.key_max });
                std.debug.print("\n", .{});
            }

            {
                var it = context.level.iterator(
                    .visible,
                    @as(*const [1]u64, &snapshot),
                    .ascending,
                    KeyRange{ .key_min = 0, .key_max = math.maxInt(Key) },
                );

                for (reference) |expect| {
                    const actual = it.next() orelse return error.TestUnexpectedResult;
                    try testing.expectEqual(expect, actual.*);
                }
                try testing.expectEqual(@as(?*const TableInfo, null), it.next());
            }

            {
                var it = context.level.iterator(
                    .visible,
                    @as(*const [1]u64, &snapshot),
                    .descending,
                    KeyRange{ .key_min = 0, .key_max = math.maxInt(Key) },
                );

                var i = reference.len;
                while (i > 0) {
                    i -= 1;

                    const expect = reference[i];
                    const actual = it.next() orelse return error.TestUnexpectedResult;
                    try testing.expectEqual(expect, actual.*);
                }
                try testing.expectEqual(@as(?*const TableInfo, null), it.next());
            }

            if (reference.len > 0) {
                const reference_len: u32 = @intCast(reference.len);
                const start = context.random.uintLessThanBiased(u32, reference_len);
                const end = context.random.uintLessThanBiased(u32, reference_len - start) + start;

                const key_min = reference[start].key_min;
                const key_max = reference[end].key_max;

                {
                    var it = context.level.iterator(
                        .visible,
                        @as(*const [1]u64, &snapshot),
                        .ascending,
                        KeyRange{ .key_min = key_min, .key_max = key_max },
                    );

                    for (reference[start .. end + 1]) |expect| {
                        const actual = it.next() orelse return error.TestUnexpectedResult;
                        try testing.expectEqual(expect, actual.*);
                    }
                    try testing.expectEqual(@as(?*const TableInfo, null), it.next());
                }

                {
                    var it = context.level.iterator(
                        .visible,
                        @as(*const [1]u64, &snapshot),
                        .descending,
                        KeyRange{ .key_min = key_min, .key_max = key_max },
                    );

                    var i = end + 1;
                    while (i > start) {
                        i -= 1;

                        const expect = reference[i];
                        const actual = it.next() orelse return error.TestUnexpectedResult;
                        try testing.expectEqual(expect, actual.*);
                    }
                    try testing.expectEqual(@as(?*const TableInfo, null), it.next());
                }
            }
        }

        inline fn key_min_from_table(table: *const TableInfo) Key {
            return table.key_min;
        }
    };
}

test "ManifestLevel" {
    const seed = 42;

    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    const Options = struct {
        key_type: type,
        node_size: u32,
        table_count_max_tree: u32,
    };

    inline for (.{
        Options{ .key_type = u64, .node_size = 256, .table_count_max_tree = 33 },
        Options{ .key_type = u64, .node_size = 256, .table_count_max_tree = 34 },
        Options{ .key_type = u64, .node_size = 256, .table_count_max_tree = 1024 },
        Options{ .key_type = u64, .node_size = 512, .table_count_max_tree = 1024 },
        Options{ .key_type = u64, .node_size = 1024, .table_count_max_tree = 1024 },
    }) |options| {
        const TestContext = TestContextType(
            options.node_size,
            options.key_type,
            options.table_count_max_tree,
        );

        var context: TestContext = undefined;
        try context.init(random);
        defer context.deinit();

        try context.run();
    }
}
