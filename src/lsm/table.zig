const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const binary_search = @import("binary_search.zig");

const stdx = @import("../stdx.zig");
const div_ceil = stdx.div_ceil;

const allocate_block = @import("../vsr/grid.zig").allocate_block;
const TreeTableInfoType = @import("manifest.zig").TreeTableInfoType;
const schema = @import("schema.zig");
const Snapshot = schema.Snapshot;

pub const TableUsage = enum {
    /// General purpose table.
    general,
    /// If your usage fits this pattern:
    /// * Only put keys which are not present.
    /// * Only remove keys which are present.
    /// * TableKey == TableValue (modulo padding, eg CompositeKey).
    /// Then we can unlock additional optimizations:
    /// * Immediately cancel out a tombstone and the corresponding insert, without waiting for the
    ///   tombstone to sink to the bottom of the LSM tree: absence of updates guarantees that
    ///   there are no otherwise visible values on lower level.
    /// * Immediately cancel out an insert and a tombstone for a "different" insert: as the values
    ///   are equal, it is correct to just resurrect an older value.
    secondary_index,
};

const address_size = @sizeOf(u64);
const checksum_size = @sizeOf(u256);

const block_size = constants.block_size;
const block_body_size = block_size - @sizeOf(vsr.Header);

const BlockPtr = *align(constants.sector_size) [block_size]u8;
const BlockPtrConst = *align(constants.sector_size) const [block_size]u8;

/// A table is a set of blocks:
///
/// * Index block (exactly 1)
/// * Data blocks (at least one, at most `data_block_count_max`) store the actual keys/values.
pub fn TableType(
    comptime TableKey: type,
    comptime TableValue: type,
    /// Returns the key for a value. For example, given `object` returns `object.id`.
    /// Since most objects contain an id, this avoids duplicating the key when storing the value.
    comptime table_key_from_value: fn (*const TableValue) callconv(.Inline) TableKey,
    /// Must compare greater than all other keys.
    comptime table_sentinel_key: TableKey,
    /// Returns whether a value is a tombstone value.
    comptime table_tombstone: fn (*const TableValue) callconv(.Inline) bool,
    /// Returns a tombstone value representation for a key.
    comptime table_tombstone_from_key: fn (TableKey) callconv(.Inline) TableValue,
    /// The maximum number of values per table.
    comptime table_value_count_max: usize,
    comptime table_usage: TableUsage,
) type {
    comptime assert(std.meta.trait.isIntegral(TableKey));

    return struct {
        const Table = @This();

        // Re-export all the generic arguments.
        pub const Key = TableKey;
        pub const Value = TableValue;
        pub const key_from_value = table_key_from_value;
        pub const sentinel_key = table_sentinel_key;
        pub const tombstone = table_tombstone;
        pub const tombstone_from_key = table_tombstone_from_key;
        pub const value_count_max = table_value_count_max;
        pub const usage = table_usage;

        // Export hashmap context for Key and Value
        pub const HashMapContextValue = struct {
            pub inline fn eql(_: HashMapContextValue, a: Value, b: Value) bool {
                return key_from_value(&a) == key_from_value(&b);
            }

            pub inline fn hash(_: HashMapContextValue, value: Value) u64 {
                return stdx.hash_inline(key_from_value(&value));
            }
        };

        pub const key_size = @sizeOf(Key);
        pub const value_size = @sizeOf(Value);

        comptime {
            assert(@alignOf(Key) == 8 or @alignOf(Key) == 16);
            // TODO(ifreund) What are our alignment expectations for Value?

            // There must be no padding in the Key/Value types to avoid buffer bleeds.
            assert(stdx.no_padding(Key));
            assert(stdx.no_padding(Value));

            // These impact our calculation of:
            // * the manifest log layout for alignment.
            assert(key_size >= 8);
            assert(key_size <= 32);
            assert(key_size == 8 or key_size == 16 or key_size == 24 or key_size == 32);
        }

        pub const layout = layout: {
            assert(block_size % constants.sector_size == 0);
            assert(math.isPowerOfTwo(block_size));

            // If the index is smaller than 16 keys then there are key sizes >= 4 such that
            // the total index size is not 64 byte cache line aligned.
            assert(@sizeOf(Key) >= 4);
            assert(@sizeOf(Key) % 4 == 0);

            const block_value_count_max = @divFloor(
                block_body_size,
                value_size,
            );

            // We need enough blocks to hold `value_count_max` values.
            const data_blocks = div_ceil(value_count_max, block_value_count_max);

            break :layout .{
                // The maximum number of values in a data block.
                .block_value_count_max = block_value_count_max,
                .data_block_count_max = data_blocks,
            };
        };

        const index_block_count = 1;
        pub const data_block_count_max = layout.data_block_count_max;
        pub const block_count_max = index_block_count + data_block_count_max;

        const index = schema.TableIndex.init(.{
            .key_size = key_size,
            .data_block_count_max = data_block_count_max,
        });

        pub const data = schema.TableData.init(.{
            .value_count_max = layout.block_value_count_max,
            .value_size = value_size,
        });

        const compile_log_layout = false;
        comptime {
            if (compile_log_layout) {
                @compileError(std.fmt.comptimePrint(
                    \\
                    \\
                    \\lsm parameters:
                    \\    value: {}
                    \\    value count max: {}
                    \\    key size: {}
                    \\    value size: {}
                    \\    block size: {}
                    \\layout:
                    \\    index block count: {}
                    \\    data block count max: {}
                    \\index:
                    \\    size: {}
                    \\    data_checksums_offset: {}
                    \\    data_checksums_size: {}
                    \\    keys_min_offset: {}
                    \\    keys_max_offset: {}
                    \\    keys_size: {}
                    \\    data_addresses_offset: {}
                    \\    data_addresses_size: {}
                    \\data:
                    \\    value_count_max: {}
                    \\    values_offset: {}
                    \\    values_size: {}
                    \\    padding_offset: {}
                    \\    padding_size: {}
                    \\
                ,
                    .{
                        Value,
                        value_count_max,
                        key_size,
                        value_size,
                        block_size,

                        index_block_count,
                        data_block_count_max,

                        index.size,
                        index.data_checksums_offset,
                        index.data_checksums_size,
                        index.keys_min_offset,
                        index.keys_max_offset,
                        index.keys_size,
                        index.data_addresses_offset,
                        index.data_addresses_size,

                        data.value_count_max,
                        data.values_offset,
                        data.values_size,
                        data.padding_offset,
                        data.padding_size,
                    },
                ));
            }
        }

        comptime {
            assert(index_block_count > 0);
            assert(data_block_count_max > 0);

            assert(index.size == @sizeOf(vsr.Header) +
                data_block_count_max * ((key_size * 2) + address_size + checksum_size));
            assert(index.size == index.data_addresses_offset + index.data_addresses_size);
            assert(index.size <= block_size);
            assert(index.keys_size > 0);
            assert(index.keys_size % key_size == 0);
            assert(@divExact(index.data_addresses_size, @sizeOf(u64)) == data_block_count_max);
            assert(@divExact(index.data_checksums_size, @sizeOf(u256)) == data_block_count_max);
            assert(block_size == index.padding_offset + index.padding_size);
            assert(block_size == index.size + index.padding_size);

            assert(data.value_count_max > 0);
            assert(@divExact(data.values_size, value_size) == data.value_count_max);
            assert(data.values_offset % constants.cache_line_size == 0);
            // You can have any size value you want, as long as it fits
            // neatly into the CPU cache lines :)
            assert((data.value_count_max * value_size) % constants.cache_line_size == 0);

            assert(data.padding_size >= 0);
            assert(block_size == @sizeOf(vsr.Header) + data.values_size + data.padding_size);
            assert(block_size == data.padding_offset + data.padding_size);

            // We expect no block padding at least for TigerBeetle's objects and indexes:
            if ((key_size == 8 and value_size == 128) or
                (key_size == 8 and value_size == 64) or
                (key_size == 16 and value_size == 16) or
                (key_size == 32 and value_size == 32))
            {
                assert(data.padding_size == 0);
            }
        }

        pub const Builder = struct {
            const TreeTableInfo = TreeTableInfoType(Table);

            key_min: Key = undefined, // Inclusive.
            key_max: Key = undefined, // Inclusive.

            index_block: BlockPtr,
            data_block: BlockPtr,

            data_block_count: u32 = 0,
            value_count: u32 = 0,
            value_count_total: u32 = 0, // Count across the entire table.

            pub fn init(allocator: mem.Allocator) !Builder {
                const index_block = try allocate_block(allocator);
                errdefer allocator.free(index_block);

                const data_block = try allocate_block(allocator);
                errdefer allocator.free(data_block);

                return Builder{
                    .index_block = index_block,
                    .data_block = data_block,
                };
            }

            pub fn deinit(builder: *Builder, allocator: mem.Allocator) void {
                allocator.free(builder.index_block);
                allocator.free(builder.data_block);

                builder.* = undefined;
            }

            pub fn reset(builder: *Builder) void {
                @memset(builder.index_block, 0);
                @memset(builder.data_block, 0);

                builder.* = .{
                    .index_block = builder.index_block,
                    .data_block = builder.data_block,
                };
            }

            pub fn data_block_values(builder: *Builder) []Value {
                return Table.data_block_values(builder.data_block);
            }

            pub fn data_block_empty(builder: *const Builder) bool {
                assert(builder.value_count <= data.value_count_max);
                return builder.value_count == 0;
            }

            pub fn data_block_full(builder: *const Builder) bool {
                assert(builder.value_count <= data.value_count_max);
                return builder.value_count == data.value_count_max;
            }

            const DataFinishOptions = struct {
                cluster: u128,
                address: u64,
                snapshot_min: Snapshot,
                tree_id: u16,
            };

            pub fn data_block_finish(builder: *Builder, options: DataFinishOptions) void {
                // For each block we write the sorted values,
                // complete the block header, and add the block's max key to the table index.

                assert(options.address > 0);
                assert(builder.value_count > 0);

                const block = builder.data_block;
                const header = mem.bytesAsValue(vsr.Header.Block, block[0..@sizeOf(vsr.Header)]);
                header.* = .{
                    .cluster = options.cluster,
                    .metadata_bytes = @bitCast(schema.TableData.Metadata{
                        .value_count_max = data.value_count_max,
                        .value_count = builder.value_count,
                        .value_size = value_size,
                        .tree_id = options.tree_id,
                    }),
                    .address = options.address,
                    .snapshot = options.snapshot_min,
                    .size = @sizeOf(vsr.Header) + builder.value_count * @sizeOf(Value),
                    .command = .block,
                    .block_type = .data,
                };

                header.set_checksum_body(block[@sizeOf(vsr.Header)..header.size]);
                header.set_checksum();

                const values = Table.data_block_values_used(block);
                { // Now that we have checksummed the block, sanity-check the result:

                    if (constants.verify) {
                        var a = &values[0];
                        for (values[1..]) |*b| {
                            assert(key_from_value(a) < key_from_value(b));
                            a = b;
                        }
                    }

                    assert(builder.value_count == values.len);
                    assert(block_size - header.size ==
                        (data.value_count_max - values.len) * @sizeOf(Value) + data.padding_size);
                }

                const key_min = key_from_value(&values[0]);
                const key_max = if (values.len == 1) key_min else blk: {
                    const key = key_from_value(&values[values.len - 1]);
                    assert(key_min < key);
                    break :blk key;
                };

                const current = builder.data_block_count;
                { // Update the index block:
                    index_data_keys(builder.index_block, .key_min)[current] = key_min;
                    index_data_keys(builder.index_block, .key_max)[current] = key_max;
                    index.data_addresses(builder.index_block)[current] = options.address;
                    index.data_checksums(builder.index_block)[current] =
                        .{ .value = header.checksum };
                }

                if (current == 0) builder.key_min = key_min;
                builder.key_max = key_max;

                if (current == 0 and values.len == 1) {
                    assert(builder.key_min == builder.key_max);
                } else {
                    assert(builder.key_min < builder.key_max);
                }
                assert(builder.key_max < sentinel_key);

                if (current > 0) {
                    const slice = index_data_keys(builder.index_block, .key_max);
                    const key_max_prev = slice[current - 1];
                    assert(key_max_prev < key_from_value(&values[0]));
                }

                builder.data_block_count += 1;
                builder.value_count_total += builder.value_count;
                builder.value_count = 0;
            }

            pub fn index_block_empty(builder: *const Builder) bool {
                assert(builder.data_block_count <= data_block_count_max);
                return builder.data_block_count == 0;
            }

            pub fn index_block_full(builder: *const Builder) bool {
                assert(builder.data_block_count <= data_block_count_max);
                return builder.data_block_count == data_block_count_max;
            }

            const IndexFinishOptions = struct {
                cluster: u128,
                address: u64,
                snapshot_min: Snapshot,
                tree_id: u16,
            };

            pub fn index_block_finish(
                builder: *Builder,
                options: IndexFinishOptions,
            ) TreeTableInfo {
                assert(options.address > 0);
                assert(builder.data_block_empty());
                assert(builder.data_block_count > 0);
                assert(builder.value_count == 0);

                const index_block = builder.index_block;
                const header =
                    mem.bytesAsValue(vsr.Header.Block, index_block[0..@sizeOf(vsr.Header)]);
                header.* = .{
                    .cluster = options.cluster,
                    .metadata_bytes = @bitCast(schema.TableIndex.Metadata{
                        .data_block_count = builder.data_block_count,
                        .data_block_count_max = index.data_block_count_max,
                        .tree_id = options.tree_id,
                        .key_size = index.key_size,
                    }),
                    .address = options.address,
                    .snapshot = options.snapshot_min,
                    .size = index.size,
                    .command = .block,
                    .block_type = .index,
                };

                for (index.padding(index_block)) |padding| {
                    @memset(index_block[padding.start..padding.end], 0);
                }

                header.set_checksum_body(index_block[@sizeOf(vsr.Header)..header.size]);
                header.set_checksum();

                const info: TreeTableInfo = .{
                    .checksum = header.checksum,
                    .address = options.address,
                    .snapshot_min = options.snapshot_min,
                    .key_min = builder.key_min,
                    .key_max = builder.key_max,
                    .value_count = builder.value_count_total,
                };

                assert(info.snapshot_max.timestamp == math.maxInt(u64));

                // Reset the builder to its initial state, leaving the buffers untouched.
                builder.* = .{
                    .key_min = undefined,
                    .key_max = undefined,
                    .index_block = builder.index_block,
                    .data_block = builder.data_block,
                };

                return info;
            }
        };

        pub inline fn index_data_keys(
            index_block: BlockPtr,
            comptime key: enum { key_min, key_max },
        ) []Key {
            const offset = comptime switch (key) {
                .key_min => index.keys_min_offset,
                .key_max => index.keys_max_offset,
            };
            return mem.bytesAsSlice(Key, index_block[offset..][0..index.keys_size]);
        }

        pub inline fn index_data_keys_used(
            index_block: BlockPtrConst,
            comptime key: enum { key_min, key_max },
        ) []const Key {
            const offset = comptime switch (key) {
                .key_min => index.keys_min_offset,
                .key_max => index.keys_max_offset,
            };
            const slice = mem.bytesAsSlice(Key, index_block[offset..][0..index.keys_size]);
            return slice[0..index.data_blocks_used(index_block)];
        }

        /// Returns the zero-based index of the data block that may contain the key
        /// or null if the key is not contained in the index block's key range.
        /// May be called on an index block only when the key is in range of the table.
        inline fn index_data_block_for_key(index_block: BlockPtrConst, key: Key) ?u32 {
            // Because we search key_max in the index block we can use the `upsert_index`
            // binary search here and avoid the extra comparison.
            // If the search finds an exact match, we want to return that data block.
            // If the search does not find an exact match it returns the index of the next
            // greatest key, which again is the index of the data block that may contain the key.
            const data_block_index = binary_search.binary_search_keys_upsert_index(
                Key,
                Table.index_data_keys_used(index_block, .key_max),
                key,
                .{},
            );
            assert(data_block_index < index.data_blocks_used(index_block));

            const key_min = Table.index_data_keys_used(index_block, .key_min)[data_block_index];
            return if (key < key_min) null else data_block_index;
        }

        pub const IndexBlocks = struct {
            data_block_address: u64,
            data_block_checksum: u128,
        };

        /// Returns all data stored in the index block relating to a given key
        /// or null if the key is not contained in the index block's keys range.
        /// May be called on an index block only when the key is in range of the table.
        pub inline fn index_blocks_for_key(index_block: BlockPtrConst, key: Key) ?IndexBlocks {
            return if (Table.index_data_block_for_key(index_block, key)) |i| .{
                .data_block_address = index.data_addresses_used(index_block)[i],
                .data_block_checksum = index.data_checksums_used(index_block)[i].value,
            } else null;
        }

        pub inline fn data_block_values(data_block: BlockPtr) []Value {
            return mem.bytesAsSlice(Value, data.block_values_bytes(data_block));
        }

        pub inline fn data_block_values_used(data_block: BlockPtrConst) []const Value {
            return mem.bytesAsSlice(Value, data.block_values_used_bytes(data_block));
        }

        pub inline fn block_address(block: BlockPtrConst) u64 {
            const header = schema.header_from_block(block);
            assert(header.address > 0);
            return header.address;
        }

        pub fn data_block_search(data_block: BlockPtrConst, key: Key) ?*const Value {
            const values = data_block_values_used(data_block);

            return binary_search.binary_search_values(
                Key,
                Value,
                key_from_value,
                values,
                key,
                .{},
            );
        }

        pub fn verify(
            comptime Storage: type,
            storage: *const Storage,
            index_address: u64,
            key_min: ?Key,
            key_max: ?Key,
        ) void {
            if (Storage != @import("../testing/storage.zig").Storage)
                // Too complicated to do async verification
                return;

            const index_block = storage.grid_block(index_address).?;
            const data_block_addresses = index.data_addresses_used(index_block);
            const data_block_checksums = index.data_checksums_used(index_block);

            for (
                data_block_addresses,
                data_block_checksums,
                0..,
            ) |data_block_address, data_block_checksum, data_block_index| {
                const data_block = storage.grid_block(data_block_address).?;
                const data_block_header = schema.header_from_block(data_block);
                assert(data_block_header.address == data_block_address);
                assert(data_block_header.checksum == data_block_checksum.value);

                const values = data_block_values_used(data_block);
                if (values.len > 0) {
                    if (data_block_index == 0) {
                        assert(key_min == null or
                            key_min.? == key_from_value(&values[0]));
                    }
                    if (data_block_index == data_block_addresses.len - 1) {
                        assert(key_max == null or
                            key_from_value(&values[values.len - 1]) == key_max.?);
                    }
                    var a = &values[0];
                    for (values[1..]) |*b| {
                        assert(key_from_value(a) < key_from_value(b));
                        a = b;
                    }
                }
            }
        }
    };
}

test "Table" {
    const CompositeKey = @import("composite_key.zig").CompositeKeyType(u128);

    const Table = TableType(
        CompositeKey.Key,
        CompositeKey,
        CompositeKey.key_from_value,
        CompositeKey.sentinel_key,
        CompositeKey.tombstone,
        CompositeKey.tombstone_from_key,
        1, // Doesn't matter for this test.
        .general,
    );

    std.testing.refAllDecls(Table.Builder);
}
