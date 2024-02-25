const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const binary_search = @import("binary_search.zig");
const schema = @import("schema.zig");

const Snapshot = schema.Snapshot;

pub fn TableMemoryType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const value_count_max = Table.value_count_max;
    const key_from_value = Table.key_from_value;

    return struct {
        const TableMemory = @This();

        pub const ValueContext = struct {
            count: usize = 0,
            sorted: bool = true,
        };

        const Mutability = union(enum) {
            mutable,
            immutable: struct {
                /// An empty table has nothing to flush
                flushed: bool = true,
                snapshot_min: Snapshot = .{ .timestamp = 0 },
            },
        };

        values: []Value,
        value_context: ValueContext,
        mutability: Mutability,
        name: []const u8,

        pub fn init(
            allocator: mem.Allocator,
            mutability: Mutability,
            name: []const u8,
        ) !TableMemory {
            const values = try allocator.alloc(Value, value_count_max);
            errdefer allocator.free(values);

            return TableMemory{
                .values = values,
                .value_context = .{},
                .mutability = mutability,
                .name = name,
            };
        }

        pub fn deinit(table: *TableMemory, allocator: mem.Allocator) void {
            allocator.free(table.values);
        }

        pub fn reset(table: *TableMemory) void {
            const mutability: Mutability = switch (table.mutability) {
                .immutable => .{ .immutable = .{} },
                .mutable => .mutable,
            };

            table.* = .{
                .values = table.values,
                .value_context = .{},
                .mutability = mutability,
                .name = table.name,
            };
        }

        pub fn count(table: *const TableMemory) usize {
            return table.value_context.count;
        }

        pub fn values_used(table: *const TableMemory) []Value {
            return table.values[0..table.count()];
        }

        pub fn put(table: *TableMemory, value: *const Value) void {
            assert(table.mutability == .mutable);
            assert(table.value_context.count < value_count_max);

            if (table.value_context.sorted) {
                table.value_context.sorted = table.value_context.count == 0 or
                    key_from_value(&table.values[table.value_context.count - 1]) <=
                    key_from_value(value);
            } else {
                assert(table.value_context.count > 0);
            }

            table.values[table.value_context.count] = value.*;
            table.value_context.count += 1;
        }

        /// This must be called on sorted tables.
        pub fn get(table: *TableMemory, key: Key) ?*const Value {
            assert(table.value_context.count <= value_count_max);
            assert(table.value_context.sorted);

            return binary_search.binary_search_values(
                Key,
                Value,
                key_from_value,
                table.values_used(),
                key,
                .{ .mode = .upper_bound },
            );
        }

        pub fn make_immutable(table: *TableMemory, snapshot_min: Snapshot) void {
            assert(table.mutability == .mutable);
            assert(table.value_context.count <= value_count_max);

            // Sort all the values. In future, this will be done incrementally, and use
            // k_way_merge, but for now the performance regression was too bad.
            table.sort();

            // If we have no values, then we can consider ourselves flushed right away.
            table.mutability = .{ .immutable = .{
                .flushed = table.value_context.count == 0,
                .snapshot_min = snapshot_min,
            } };
        }

        pub fn make_mutable(table: *TableMemory) void {
            assert(table.mutability == .immutable);
            assert(table.mutability.immutable.flushed == true);
            assert(table.value_context.count <= value_count_max);
            assert(table.value_context.sorted);

            table.* = .{
                .values = table.values,
                .value_context = .{},
                .mutability = .mutable,
                .name = table.name,
            };
        }

        pub fn sort(table: *TableMemory) void {
            if (!table.value_context.sorted) {
                std.mem.sort(
                    Value,
                    table.values_used(),
                    {},
                    sort_values_by_key_in_ascending_order,
                );
                table.value_context.sorted = true;
            }
        }

        fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
            return key_from_value(&a) < key_from_value(&b);
        }

        pub fn key_min(table: *const TableMemory) Key {
            const values = table.values_used();

            assert(values.len > 0);
            assert(table.mutability == .immutable);

            return key_from_value(&values[0]);
        }

        pub fn key_max(table: *const TableMemory) Key {
            const values = table.values_used();

            assert(values.len > 0);
            assert(table.mutability == .immutable);

            return key_from_value(&values[values.len - 1]);
        }
    };
}

const TestTable = struct {
    const Key = u32;
    const Value = struct { key: Key, value: u32, tombstone: bool };
    const value_count_max = 16;

    inline fn key_from_value(v: *const Value) u32 {
        return v.key;
    }

    inline fn tombstone_from_key(a: Key) Value {
        return Value{ .key = a, .value = 0, .tombstone = true };
    }
};

test "table_memory: unit" {
    const testing = std.testing;
    const TableMemory = TableMemoryType(TestTable);

    const allocator = testing.allocator;
    var table_memory = try TableMemory.init(allocator, .mutable, "test");
    defer table_memory.deinit(allocator);

    table_memory.put(&.{ .key = 1, .value = 1, .tombstone = false });
    table_memory.put(&.{ .key = 3, .value = 3, .tombstone = false });
    table_memory.put(&.{ .key = 5, .value = 5, .tombstone = false });

    assert(table_memory.count() == 3 and table_memory.value_context.count == 3);
    assert(table_memory.value_context.sorted);

    table_memory.put(&.{ .key = 0, .value = 0, .tombstone = false });
    table_memory.make_immutable(Snapshot{ .timestamp = 0 });

    assert(table_memory.count() == 4 and table_memory.value_context.count == 4);
    assert(table_memory.key_min() == 0);
    assert(table_memory.key_max() == 5);
    assert(table_memory.value_context.sorted);

    // "Flush" and make mutable again
    table_memory.mutability.immutable.flushed = true;

    table_memory.make_mutable();
    assert(table_memory.count() == 0 and table_memory.value_context.count == 0);
    assert(table_memory.value_context.sorted);
    assert(table_memory.mutability == .mutable);
}
