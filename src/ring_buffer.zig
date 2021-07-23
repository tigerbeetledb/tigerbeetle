const std = @import("std");
const assert = std.debug.assert;

/// A First In, First Out ring buffer holding at most `size` elements.
pub fn RingBuffer(comptime T: type, comptime size: usize) type {
    return struct {
        const Self = @This();

        buffer: [size]T = undefined,

        /// The index of the slot with the first item, if any.
        index: usize = 0,

        /// The number of items in the buffer.
        count: usize = 0,

        /// Add an element to the RingBuffer. Returns an error if the buffer
        /// is already full and the element could not be added.
        pub fn push(self: *Self, item: T) error{NoSpaceLeft}!void {
            if (self.full()) return error.NoSpaceLeft;
            self.buffer[(self.index + self.count) % self.buffer.len] = item;
            self.count += 1;
        }

        /// Return but do not remove the next item, if any.
        pub fn peek(self: *Self) ?*T {
            if (self.empty()) return null;
            return &self.buffer[self.index];
        }

        /// Remove and return the next item, if any.
        pub fn pop(self: *Self) ?T {
            if (self.empty()) return null;
            defer {
                self.index = (self.index + 1) % self.buffer.len;
                self.count -= 1;
            }
            return self.buffer[self.index];
        }

        /// Returns whether the ring buffer is completely full.
        pub fn full(self: *const Self) bool {
            return self.count == self.buffer.len;
        }

        /// Returns whether the ring buffer is completely empty.
        pub fn empty(self: *const Self) bool {
            return self.count == 0;
        }

        pub const Iterator = struct {
            ring: *Self,
            count: usize = 0,

            pub fn next(it: *Iterator) ?*T {
                assert(it.count <= it.ring.count);
                if (it.count == it.ring.count) return null;
                defer it.count += 1;
                return &it.ring.buffer[(it.ring.index + it.count) % it.ring.buffer.len];
            }
        };

        /// Returns an iterator to iterate through all `count` items in the ring buffer.
        pub fn iterator(self: *Self) Iterator {
            return .{ .ring = self };
        }
    };
}

const testing = std.testing;

test "push/peek/pop/full/empty" {
    var fifo = RingBuffer(u32, 3){};

    testing.expect(!fifo.full());
    testing.expect(fifo.empty());

    try fifo.push(1);
    testing.expectEqual(@as(u32, 1), fifo.peek().?.*);

    testing.expect(!fifo.full());
    testing.expect(!fifo.empty());

    try fifo.push(2);
    testing.expectEqual(@as(u32, 1), fifo.peek().?.*);

    try fifo.push(3);
    testing.expectError(error.NoSpaceLeft, fifo.push(4));

    testing.expect(fifo.full());
    testing.expect(!fifo.empty());

    testing.expectEqual(@as(u32, 1), fifo.peek().?.*);
    testing.expectEqual(@as(?u32, 1), fifo.pop());

    testing.expect(!fifo.full());
    testing.expect(!fifo.empty());

    testing.expectEqual(@as(?u32, 2), fifo.pop());
    testing.expectEqual(@as(?u32, 3), fifo.pop());
    testing.expectEqual(@as(?u32, null), fifo.pop());

    testing.expect(!fifo.full());
    testing.expect(fifo.empty());
}

fn test_iterator(comptime T: type, ring: *T, values: []const u32) void {
    const ring_index = ring.index;
    
    var loops: usize = 0;
    while (loops < 2) : (loops += 1) {
        var iterator = ring.iterator();
        var index: usize = 0;
        while (iterator.next()) |item| {
            testing.expectEqual(values[index], item.*);
            index += 1;
        }
        testing.expectEqual(values.len, index);
    }

    testing.expectEqual(ring_index, ring.index);
}

test "iterator" {
    const Ring = RingBuffer(u32, 2);

    var ring = Ring{};
    test_iterator(Ring, &ring, &[_]u32{});

    try ring.push(0);
    test_iterator(Ring, &ring, &[_]u32{0});

    try ring.push(1);
    test_iterator(Ring, &ring, &[_]u32{ 0, 1 });

    testing.expectEqual(@as(?u32, 0), ring.pop());
    test_iterator(Ring, &ring, &[_]u32{1});

    try ring.push(2);
    test_iterator(Ring, &ring, &[_]u32{ 1, 2 });

    testing.expectEqual(@as(?u32, 1), ring.pop());
    test_iterator(Ring, &ring, &[_]u32{2});

    try ring.push(3);
    test_iterator(Ring, &ring, &[_]u32{ 2, 3 });

    testing.expectEqual(@as(?u32, 2), ring.pop());
    test_iterator(Ring, &ring, &[_]u32{3});

    testing.expectEqual(@as(?u32, 3), ring.pop());
    test_iterator(Ring, &ring, &[_]u32{});
}
