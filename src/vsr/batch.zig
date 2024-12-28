const std = @import("std");
const testing = std.testing;

const stdx = @import("../stdx.zig");
const assert = std.debug.assert;
const maybe = stdx.maybe;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");

/// The trailer is a `[]u16` containing the number of elements in each batch.
/// To encode the trailer, `batch_count * @sizeOf(u16)` bytes are needed, but the total
/// space occupied by the trailer may be larger, as it must align with the element size.
/// In the case of operations where `element_size` is zero, the trailer must be aligned
/// to `@alignOf(u16)` (2 bytes).
pub fn batch_trailer_total_size(options: struct {
    element_size: usize,
    batch_count: u16,
}) usize {
    assert(options.batch_count > 0);
    maybe(options.element_size == 0);
    const alignment = @max(options.element_size, @alignOf(u16));
    return stdx.div_ceil(
        @as(usize, options.batch_count) * @sizeOf(u16),
        alignment,
    ) * alignment;
}

pub const BatchDecoder = struct {
    /// The batch element size.
    element_size: usize,
    /// The message body, excluding the trailer containing the batch metadata.
    payload: []const u8,
    /// The batching metadata, containing the number of events for each batch.
    batch_events: []const u16,

    pub fn init(
        /// The size of each element.
        element_size: usize,
        /// The message body used, including the trailer.
        body: []const u8,
        /// The number of batches encoded in this body.
        batch_count: u16,
    ) BatchDecoder {
        assert(batch_count > 0);
        assert(body.len > 0);
        assert(element_size == 0 or body.len % element_size == 0);

        const trailer_size = batch_trailer_total_size(.{
            .element_size = element_size,
            .batch_count = batch_count,
        });
        if (element_size == 0) {
            assert(body.len == trailer_size);
        } else {
            assert(body.len >= trailer_size);
        }

        const payload: []const u8 = @alignCast(body[0 .. body.len - trailer_size]);
        if (element_size == 0) {
            assert(payload.len == 0);
        } else {
            assert(payload.len % element_size == 0);
        }

        const batch_events: []const u16 = @alignCast(
            std.mem.bytesAsSlice(u16, body[body.len - trailer_size ..]),
        );
        assert(batch_events.len >= batch_count);

        const batch_events_used = batch_events[batch_events.len - batch_count ..];
        assert(batch_events_used.len == batch_count);

        if (constants.verify) {
            if (batch_events.len > batch_count) {
                // MaxInt is used as sentinel for the extra slots used for alignment.
                assert(std.mem.allEqual(
                    u16,
                    batch_events[0 .. batch_events.len - batch_count],
                    std.math.maxInt(u16),
                ));
            }

            if (element_size > 0) {
                var events_count_total: usize = 0;
                for (batch_events_used) |count| events_count_total += count;
                assert(payload.len == events_count_total * element_size);
            }
        }

        return .{
            .element_size = element_size,
            .payload = payload,
            .batch_events = batch_events_used,
        };
    }

    pub fn next(self: *BatchDecoder) ?[]const u8 {
        if (self.batch_events.len == 0) {
            assert(self.payload.len == 0);
            return null;
        }

        const batch_lenght = batch_lenght: {
            assert(self.batch_events.len > 0);
            maybe(self.payload.len == 0);

            // Batch metadata is written from the end of the message, so the last
            // element corresponds to the first batch.
            const batch_event_count = self.batch_events[self.batch_events.len - 1];
            assert(batch_event_count != std.math.maxInt(u16));
            maybe(batch_event_count == 0);

            const batch_lenght = batch_event_count * self.element_size;
            assert(batch_lenght <= self.payload.len);
            assert(batch_lenght % self.element_size == 0); // Must be aligned.

            break :batch_lenght batch_lenght;
        };

        defer {
            self.batch_events = self.batch_events[0 .. self.batch_events.len - 1];
            self.payload = if (self.payload.len > batch_lenght)
                self.payload[batch_lenght..]
            else
                &.{};
        }

        return self.payload[0..batch_lenght];
    }
};

pub const BatchEncoder = struct {
    element_size: usize,
    buffer: ?[]u8,
    bytes_written: usize,
    batch_count: u16,

    pub fn init(
        element_size: usize,
        buffer: []u8,
    ) BatchEncoder {
        assert(buffer.len > 0);
        assert(element_size == 0 or buffer.len % element_size == 0);

        return .{
            .element_size = element_size,
            .buffer = buffer,
            .bytes_written = 0,
            .batch_count = 0,
        };
    }

    pub fn writable(self: *const BatchEncoder) []u8 {
        assert(self.buffer != null);
        assert(self.bytes_written % self.element_size == 0);

        const trailer_size = batch_trailer_total_size(.{
            .element_size = self.element_size,
            .batch_count = self.batch_count + 1,
        });

        const buffer: []u8 = self.buffer.?;
        assert(buffer.len >= self.bytes_written + trailer_size);

        return buffer[self.bytes_written .. buffer.len - trailer_size];
    }

    pub fn can_add(self: *const BatchEncoder, bytes_written: usize) bool {
        assert(self.buffer != null);
        assert(self.bytes_written % self.element_size == 0);

        const trailer_size = batch_trailer_total_size(.{
            .element_size = self.element_size,
            .batch_count = self.batch_count + 1,
        });

        const buffer: []u8 = self.buffer.?;
        return buffer.len >= self.bytes_written + bytes_written + trailer_size;
    }

    pub fn add(self: *BatchEncoder, bytes_written: usize) void {
        assert(self.buffer != null);
        assert(bytes_written % self.element_size == 0);

        const buffer: []u8 = self.buffer.?;
        self.batch_count += 1;
        self.bytes_written += bytes_written;

        const trailer_size = batch_trailer_total_size(.{
            .element_size = self.element_size,
            .batch_count = self.batch_count,
        });
        assert(buffer.len >= self.bytes_written + trailer_size);

        const batch_events: []u16 = @alignCast(std.mem.bytesAsSlice(
            u16,
            buffer[buffer.len - trailer_size ..],
        ));
        assert(batch_events.len >= self.batch_count);

        // Batch metadata is written from the end of the message, so the last
        // element corresponds to the first batch.
        const batch_event_index = batch_events.len - self.batch_count;
        if (self.element_size > 0) {
            maybe(bytes_written == 0);
            batch_events[batch_event_index] = @intCast(@divExact(bytes_written, self.element_size));
        } else {
            assert(bytes_written == 0);
            batch_events[batch_event_index] = 0;
        }
    }

    pub fn finish(self: *BatchEncoder) void {
        assert(self.buffer != null);
        assert(self.batch_count > 0);
        assert(self.bytes_written % self.element_size == 0);
        assert(self.buffer.?.len > self.bytes_written);
        maybe(self.bytes_written == 0);

        const buffer = self.buffer.?;

        const trailer_size = batch_trailer_total_size(.{
            .element_size = self.element_size,
            .batch_count = self.batch_count,
        });
        assert(buffer.len >= self.bytes_written + trailer_size);

        const batch_events: []u16 = @alignCast(std.mem.bytesAsSlice(
            u16,
            buffer[self.bytes_written..][0..trailer_size],
        ));

        // Filling in the extra alignment bytes with `maxInt`.
        @memset(
            batch_events[0 .. batch_events.len - self.batch_count],
            std.math.maxInt(u16),
        );
        // While batches are being encoded, the trailer is written at the end of the buffer.
        // Once all batches are encoded, the trailer needs to be moved closer to the last
        // element written, along with sentinels to maintain alignment.
        const source: []u16 = @alignCast(std.mem.bytesAsSlice(
            u16,
            buffer[buffer.len - (@as(usize, self.batch_count) * @sizeOf(u16)) ..],
        ));
        const target: []u16 = batch_events[batch_events.len - self.batch_count ..];
        assert(@intFromPtr(target.ptr) <= @intFromPtr(source.ptr));
        assert(target.len == source.len);
        if (target.ptr != source.ptr) {
            stdx.copy_left(
                .exact,
                u16,
                target,
                source,
            );
        }

        // Update `bytes_written` to include the trailer and
        // set buffer to null to prevent misuse.
        self.buffer = null;
        self.bytes_written = self.bytes_written + trailer_size;
        assert(self.bytes_written % self.element_size == 0);
    }
};

const test_batch = struct {
    fn run(options: struct {
        random: std.rand.Random,
        element_size: usize,
        buffer: []u8,
        batch_count: u16,
        elements_per_batch: ?u32 = null,
    }) !usize {
        const allocator = testing.allocator;
        const expected = try allocator.alloc(u16, options.batch_count);
        defer allocator.free(expected);

        const trailer_size = batch_trailer_total_size(.{
            .element_size = options.element_size,
            .batch_count = options.batch_count,
        });

        // Cleaning the buffer first, so it can assert the bytes.
        @memset(options.buffer, std.math.maxInt(u8));

        var encoder = BatchEncoder.init(options.element_size, options.buffer);
        for (0..options.batch_count) |index| {
            const bytes_available = options.buffer.len - encoder.bytes_written - trailer_size;

            const bytes_written: usize = if (options.elements_per_batch) |elements_per_batch|
                elements_per_batch * options.element_size
            else random: {
                if (index == options.batch_count - 1) {
                    const batch_full = options.random.uintLessThanBiased(u8, 100) < 30;
                    if (batch_full) break :random bytes_available;
                }

                const batch_empty = options.random.uintLessThanBiased(u8, 100) < 30;
                if (batch_empty) break :random 0;

                break :random @divFloor(
                    options.random.intRangeAtMostBiased(usize, 0, bytes_available),
                    options.element_size,
                ) * options.element_size;
            };

            try testing.expect(encoder.can_add(bytes_written));

            const slice = encoder.writable();
            @memset(std.mem.bytesAsSlice(u16, slice[0..bytes_written]), @intCast(index));
            encoder.add(bytes_written);

            expected[index] = @intCast(@divExact(bytes_written, options.element_size));
        }
        encoder.finish();
        try testing.expect(encoder.batch_count == options.batch_count);

        var decoder = BatchDecoder.init(
            options.element_size,
            options.buffer[0..encoder.bytes_written],
            encoder.batch_count,
        );
        var batch_read_index: usize = 0;
        while (decoder.next()) |batch| : (batch_read_index += 1) {
            const event_count = @divExact(batch.len, options.element_size);
            try testing.expect(expected[batch_read_index] == event_count);
            try testing.expect(std.mem.allEqual(
                u16,
                @alignCast(std.mem.bytesAsSlice(u16, batch)),
                @intCast(batch_read_index),
            ));
        }
        try testing.expect(options.batch_count == batch_read_index);

        return encoder.bytes_written;
    }
};

test "batch: encode/decode" {
    var rng = std.rand.DefaultPrng.init(42);
    const buffer = try testing.allocator.alignedAlloc(
        u8,
        @alignOf(vsr.Header),
        constants.message_body_size_max,
    );
    defer testing.allocator.free(buffer);

    for (0..2048) |_| {
        const random = rng.random();

        // Element size ranging from 2^4 to 2^8:
        const element_size = std.math.pow(
            usize,
            2,
            random.intRangeAtMostBiased(usize, 4, 8),
        );

        // The maximum `batch_count` is a `u16`.
        // Depending on the element size, this limit can overflow. However, it is large enough
        // to support a 1MiB message body with batches containing a single 16-byte element each.
        const batch_count_max: u16 = @intCast(@divExact(buffer.len, element_size) - 1);
        const batch_count = random.intRangeAtMostBiased(u16, 1, batch_count_max);

        _ = try test_batch.run(.{
            .random = random,
            .element_size = element_size,
            .buffer = buffer,
            .batch_count = batch_count,
        });
    }
}

// The maximum number of batches, all with zero elements.
test "batch: maximum batches with no elements" {
    var rng = std.rand.DefaultPrng.init(42);
    const random = rng.random();

    const batch_count = std.math.maxInt(u16);
    const element_size = 128;
    const buffer_size = batch_trailer_total_size(.{
        .element_size = element_size,
        .batch_count = batch_count,
    });

    const buffer = try testing.allocator.alignedAlloc(
        u8,
        @alignOf(vsr.Header),
        buffer_size,
    );
    defer testing.allocator.free(buffer);
    const written_bytes = try test_batch.run(.{
        .random = random,
        .element_size = element_size,
        .buffer = buffer,
        .batch_count = batch_count,
        .elements_per_batch = 0,
    });
    try testing.expectEqual(buffer_size, written_bytes);
}

// The maximum number of batches, when each one has one single element.
test "batch: maximum batches with a single element" {
    var rng = std.rand.DefaultPrng.init(42);
    const random = rng.random();

    const element_size = 128;
    const buffer_size = (1024 * 1024) - @sizeOf(vsr.Header); // 1MiB message.
    const batch_count_max: u16 = @divExact(buffer_size, (element_size + @sizeOf(u16)));

    const buffer = try testing.allocator.alignedAlloc(u8, @alignOf(vsr.Header), buffer_size);
    defer testing.allocator.free(buffer);
    const written_bytes = try test_batch.run(.{
        .random = random,
        .element_size = element_size,
        .buffer = buffer,
        .batch_count = batch_count_max,
        .elements_per_batch = 1,
    });
    try testing.expectEqual(buffer_size, written_bytes);
}

// The maximum number of elements on a single batch.
test "batch: maximum elements on a single batch" {
    var rng = std.rand.DefaultPrng.init(42);
    const random = rng.random();

    const element_size = 128;
    const buffer_size = (1024 * 1024) - @sizeOf(vsr.Header); // 1MiB message.
    const batch_size_max = 8189; // maximum number of elements in a single-batch request.
    assert(batch_size_max == @divExact(buffer_size - element_size, element_size));

    const buffer = try testing.allocator.alignedAlloc(u8, @alignOf(vsr.Header), buffer_size);
    defer testing.allocator.free(buffer);
    const written_bytes = try test_batch.run(.{
        .random = random,
        .element_size = element_size,
        .buffer = buffer,
        .batch_count = 1,
        .elements_per_batch = batch_size_max,
    });
    try testing.expectEqual(buffer_size, written_bytes);
}
