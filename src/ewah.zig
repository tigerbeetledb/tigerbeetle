const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;
const stdx = @import("stdx.zig");
const div_ceil = stdx.div_ceil;
const disjoint_slices = stdx.disjoint_slices;
const maybe = stdx.maybe;

const constants = @import("constants.zig");

/// Encode or decode a bitset using Daniel Lemire's EWAH codec.
/// ("Histogram-Aware Sorting for Enhanced Word-Aligned Compression in Bitmap Indexes")
///
/// EWAH uses only two types of words, where the first type is a 64-bit verbatim ("literal") word.
/// The second type of word is a marker word:
/// * The first bit indicates which uniform word will follow.
/// * The next 31 bits are used to store the number of uniform words.
/// * The last 32 bits are used to store the number of literal words following the uniform words.
/// EWAH bitmaps begin with a marker word. A 'marker' looks like (assuming a 64-bit word):
///
///     [uniform_bit:u1][uniform_word_count:u31(LE)][literal_word_count:u32(LE)]
///
/// and is immediately followed by `literal_word_count` 64-bit literals.
/// When decoding a marker, the uniform words precede the literal words.
///
/// This encoding requires that the architecture is little-endian with 64-bit words.
pub fn ewah(comptime Word: type) type {
    const word_bits = @bitSizeOf(Word);

    return struct {
        const marker_uniform_word_count_max = (1 << ((word_bits / 2) - 1)) - 1;
        const marker_literal_word_count_max = (1 << (word_bits / 2)) - 1;

        pub const MarkerUniformCount = std.meta.Int(.unsigned, word_bits / 2 - 1); // Word=u64 → u31
        pub const MarkerLiteralCount = std.meta.Int(.unsigned, word_bits / 2); // Word=u64 → u32

        const Marker = packed struct(Word) {
            // Whether the uniform word is all 0s or all 1s.
            uniform_bit: u1,
            // 31-bit number of uniform words following the marker.
            uniform_word_count: MarkerUniformCount,
            // 32-bit number of literal words following the uniform words.
            literal_word_count: MarkerLiteralCount,
        };

        comptime {
            assert(@import("builtin").target.cpu.arch.endian() == std.builtin.Endian.little);
            assert(@typeInfo(Word).Int.signedness == .unsigned);
            assert(word_bits % 8 == 0); // A multiple of a byte, so that words can be cast to bytes.
            assert(@bitSizeOf(Marker) == word_bits);
            assert(@sizeOf(Marker) == @sizeOf(Word));

            assert(@bitSizeOf(MarkerUniformCount) % 2 == 1);
            assert(math.maxInt(MarkerUniformCount) == marker_uniform_word_count_max);

            assert(@bitSizeOf(MarkerLiteralCount) % 2 == 0);
            assert(math.maxInt(MarkerLiteralCount) == marker_literal_word_count_max);
        }

        inline fn marker_word(mark: Marker) Word {
            return @bitCast(mark);
        }

        pub const Decoder = struct {
            /// The number of bytes of the source buffer (the encoded data) that still need to be
            /// processed.
            source_size_remaining: usize,
            target_words: []Word,
            target_index: usize = 0,
            source_literal_words: usize = 0,

            /// Returns the number of *words* written to `target_words` by this invocation.
            // TODO Refactor to return an error when `source_chunk` is invalid,
            // so that we can test invalid encodings.
            pub fn decode_chunk(
                decoder: *Decoder,
                source_chunk: []align(@alignOf(Word)) const u8,
            ) usize {
                assert(source_chunk.len % @sizeOf(Word) == 0);

                decoder.source_size_remaining -= source_chunk.len;

                const source_words = mem.bytesAsSlice(Word, source_chunk);
                const target_words = decoder.target_words;
                assert(disjoint_slices(u8, Word, source_chunk, target_words));

                var source_index: usize = 0;
                var target_index: usize = decoder.target_index;
                defer decoder.target_index = target_index;

                if (decoder.source_literal_words > 0) {
                    const literal_word_count_chunk =
                        @min(decoder.source_literal_words, source_words.len);

                    stdx.copy_disjoint(
                        .exact,
                        Word,
                        target_words[target_index..][0..literal_word_count_chunk],
                        source_words[source_index..][0..literal_word_count_chunk],
                    );
                    source_index += literal_word_count_chunk;
                    target_index += literal_word_count_chunk;
                    decoder.source_literal_words -= literal_word_count_chunk;
                }

                while (source_index < source_words.len) {
                    assert(decoder.source_literal_words == 0);

                    const marker: *const Marker = @ptrCast(&source_words[source_index]);
                    source_index += 1;
                    @memset(
                        target_words[target_index..][0..marker.uniform_word_count],
                        if (marker.uniform_bit == 1) ~@as(Word, 0) else 0,
                    );
                    target_index += marker.uniform_word_count;

                    const literal_word_count_chunk =
                        @min(marker.literal_word_count, source_words.len - source_index);
                    stdx.copy_disjoint(
                        .exact,
                        Word,
                        target_words[target_index..][0..literal_word_count_chunk],
                        source_words[source_index..][0..literal_word_count_chunk],
                    );
                    source_index += literal_word_count_chunk;
                    target_index += literal_word_count_chunk;
                    decoder.source_literal_words =
                        marker.literal_word_count - literal_word_count_chunk;
                }
                assert(source_index <= source_words.len);
                assert(target_index <= target_words.len);

                return target_index - decoder.target_index;
            }

            pub fn done(decoder: *const Decoder) bool {
                assert(decoder.target_index <= decoder.target_words.len);

                if (decoder.source_size_remaining == 0) {
                    assert(decoder.source_literal_words == 0);
                    return true;
                } else {
                    maybe(decoder.source_literal_words == 0);
                    return false;
                }
            }
        };

        pub fn decode_chunks(target_words: []Word, source_size: usize) Decoder {
            return .{
                .target_words = target_words,
                .source_size_remaining = source_size,
            };
        }

        // (This is a helper for testing only.)
        /// Decodes the compressed bitset in `source` into `target_words`.
        /// Returns the number of *words* written to `target_words`.
        pub fn decode_all(source: []align(@alignOf(Word)) const u8, target_words: []Word) usize {
            comptime assert(constants.verify);
            assert(source.len % @sizeOf(Word) == 0);
            assert(disjoint_slices(u8, Word, source, target_words));

            var decoder = decode_chunks(target_words, source.len);
            return decoder.decode_chunk(source);
        }

        pub const Encoder = struct {
            source_words: []const Word,
            source_index: usize = 0,
            /// The number of literals left over from the previous encode() call that still need to
            /// be copied.
            literal_word_count: usize = 0,

            /// Returns the number of bytes written to `target_chunk` by this invocation.
            pub fn encode_chunk(encoder: *Encoder, target_chunk: []align(@alignOf(Word)) u8) usize {
                const source_words = encoder.source_words;
                assert(disjoint_slices(Word, u8, source_words, target_chunk));
                assert(encoder.source_index <= encoder.source_words.len);
                assert(encoder.literal_word_count <= encoder.source_words.len);

                const target_words = mem.bytesAsSlice(Word, target_chunk);
                @memset(target_words, 0);

                var target_index: usize = 0;
                var source_index: usize = encoder.source_index;

                if (encoder.literal_word_count > 0) {
                    maybe(encoder.source_index == 0);

                    const literal_word_count_chunk =
                        @min(encoder.literal_word_count, target_words.len);

                    stdx.copy_disjoint(
                        .exact,
                        Word,
                        target_words[target_index..][0..literal_word_count_chunk],
                        source_words[source_index..][0..literal_word_count_chunk],
                    );

                    source_index += literal_word_count_chunk;
                    target_index += literal_word_count_chunk;
                    encoder.literal_word_count -= literal_word_count_chunk;
                }

                while (source_index < source_words.len and target_index < target_words.len) {
                    assert(encoder.literal_word_count == 0);

                    const word = source_words[source_index];

                    const uniform_word_count = count: {
                        if (is_literal(word)) break :count 0;
                        // Measure run length.
                        const uniform_max = @min(
                            source_words.len - source_index,
                            marker_uniform_word_count_max,
                        );
                        for (source_words[source_index..][0..uniform_max], 0..) |w, i| {
                            if (w != word) break :count i;
                        }
                        break :count uniform_max;
                    };
                    source_index += uniform_word_count;
                    // For consistent encoding, set the run/uniform bit to 0 when there is no run.
                    const uniform_bit: u1 =
                        if (uniform_word_count == 0) 0 else @intCast(word & 1);

                    const literal_word_count = count: {
                        // Count sequential literals that immediately follow the run.
                        const literals_max = @min(
                            source_words.len - source_index,
                            marker_literal_word_count_max,
                        );
                        for (source_words[source_index..][0..literals_max], 0..) |w, i| {
                            if (!is_literal(w)) break :count i;
                        }
                        break :count literals_max;
                    };

                    target_words[target_index] = marker_word(.{
                        .uniform_bit = uniform_bit,
                        .uniform_word_count = @intCast(uniform_word_count),
                        .literal_word_count = @intCast(literal_word_count),
                    });
                    target_index += 1;

                    const literal_word_count_chunk =
                        @min(literal_word_count, target_words.len - target_index);
                    stdx.copy_disjoint(
                        .exact,
                        Word,
                        target_words[target_index..][0..literal_word_count_chunk],
                        source_words[source_index..][0..literal_word_count_chunk],
                    );
                    source_index += literal_word_count_chunk;
                    target_index += literal_word_count_chunk;

                    encoder.literal_word_count = literal_word_count - literal_word_count_chunk;
                }
                assert(source_index <= source_words.len);

                encoder.source_index = source_index;
                return target_index * @sizeOf(Word);
            }

            pub fn done(encoder: *const Encoder) bool {
                assert(encoder.source_index <= encoder.source_words.len);
                return encoder.source_index == encoder.source_words.len;
            }
        };

        pub fn encode_chunks(source_words: []const Word) Encoder {
            return .{ .source_words = source_words };
        }

        // (This is a helper for testing only.)
        // Returns the number of bytes written to `target`.
        pub fn encode_all(source_words: []const Word, target: []align(@alignOf(Word)) u8) usize {
            comptime assert(constants.verify);
            assert(target.len == encode_size_max(source_words.len));
            assert(disjoint_slices(Word, u8, source_words, target));

            var encoder = encode_chunks(source_words);
            defer assert(encoder.done());
            return encoder.encode_chunk(target);
        }

        /// Returns the maximum number of bytes required to encode `word_count` words.
        /// Assumes (pessimistically) that every word will be encoded as a literal.
        pub fn encode_size_max(word_count: usize) usize {
            const marker_count = div_ceil(word_count, marker_literal_word_count_max);
            return marker_count * @sizeOf(Marker) + word_count * @sizeOf(Word);
        }

        inline fn is_literal(word: Word) bool {
            return word != 0 and word != ~@as(Word, 0);
        }
    };
}

test "ewah encode→decode cycle" {
    const fuzz = @import("./ewah_fuzz.zig");
    var prng = std.rand.DefaultPrng.init(123);

    inline for (.{ u8, u16, u32, u64, usize }) |Word| {
        for ([_]usize{ 1, 2, 4, 5, 8, 16, 17, 32 }) |chunk_count| {
            var decoded: [4096]Word = undefined;

            const fuzz_options = .{
                .encode_chunk_words_count = @divFloor(decoded.len, chunk_count),
                .decode_chunk_words_count = @divFloor(decoded.len, chunk_count),
            };

            @memset(&decoded, 0);
            try fuzz.fuzz_encode_decode(Word, std.testing.allocator, &decoded, fuzz_options);

            @memset(&decoded, std.math.maxInt(Word));
            try fuzz.fuzz_encode_decode(Word, std.testing.allocator, &decoded, fuzz_options);

            prng.random().bytes(std.mem.asBytes(&decoded));
            try fuzz.fuzz_encode_decode(Word, std.testing.allocator, &decoded, fuzz_options);
        }
    }
}

test "ewah Word=u8" {
    try test_decode_with_word(u8);

    const codec = ewah(u8);
    for (0..math.maxInt(codec.MarkerUniformCount) + 1) |uniform_word_count| {
        try test_decode(u8, &.{
            codec.marker_word(.{
                .uniform_bit = 0,
                .uniform_word_count = @intCast(uniform_word_count),
                .literal_word_count = 3,
            }),
            12,
            34,
            56,
        });
    }

    try std.testing.expectEqual(codec.encode_size_max(0), 0);
    try std.testing.expectEqual(codec.encode_all(&.{}, &.{}), 0);
}

test "ewah Word=u16" {
    try test_decode_with_word(u16);
}

// decode → encode → decode
fn test_decode_with_word(comptime Word: type) !void {
    const codec = ewah(Word);

    // No set bits.
    try test_decode(Word, &.{});
    // Alternating runs, no literals.
    try test_decode(Word, &.{
        codec.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 2, .literal_word_count = 0 }),
        codec.marker_word(.{ .uniform_bit = 1, .uniform_word_count = 3, .literal_word_count = 0 }),
        codec.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 4, .literal_word_count = 0 }),
    });
    // Alternating runs, with literals.
    try test_decode(Word, &.{
        codec.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 2, .literal_word_count = 1 }),
        12,
        codec.marker_word(.{ .uniform_bit = 1, .uniform_word_count = 3, .literal_word_count = 1 }),
        34,
        codec.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 4, .literal_word_count = 1 }),
        56,
    });
    // Consecutive run marker overflow.
    try test_decode(Word, &.{
        codec.marker_word(.{
            .uniform_bit = 0,
            .uniform_word_count = math.maxInt(codec.MarkerUniformCount),
            .literal_word_count = 0,
        }),
        codec.marker_word(.{ .uniform_bit = 0, .uniform_word_count = 2, .literal_word_count = 0 }),
    });

    var encoding = std.ArrayList(Word).init(std.testing.allocator);
    defer encoding.deinit();

    {
        // Consecutive literal marker overflow.
        try encoding.append(codec.marker_word(.{
            .uniform_bit = 0,
            .uniform_word_count = 0,
            .literal_word_count = math.maxInt(codec.MarkerLiteralCount),
        }));
        var i: Word = 0;
        while (i < math.maxInt(codec.MarkerLiteralCount)) : (i += 1) try encoding.append(i + 1);
        try encoding.append(codec.marker_word(.{
            .uniform_bit = 0,
            .uniform_word_count = 0,
            .literal_word_count = 2,
        }));
        try encoding.append(i + 2);
        try encoding.append(i + 3);
        try test_decode(Word, encoding.items);
        encoding.items.len = 0;
    }
}

fn test_decode(comptime Word: type, encoded_expect_words: []const Word) !void {
    const encoded_expect = mem.sliceAsBytes(encoded_expect_words);
    const codec = ewah(Word);
    const decoded_expect_data = try std.testing.allocator.alloc(Word, 4 * math.maxInt(Word));
    defer std.testing.allocator.free(decoded_expect_data);

    const decoded_expect_length = codec.decode_all(encoded_expect, decoded_expect_data);
    const decoded_expect = decoded_expect_data[0..decoded_expect_length];
    const encoded_actual = try std.testing.allocator.alignedAlloc(
        u8,
        @alignOf(Word),
        codec.encode_size_max(decoded_expect.len),
    );
    defer std.testing.allocator.free(encoded_actual);

    const encoded_actual_length = codec.encode_all(decoded_expect, encoded_actual);
    try std.testing.expectEqual(encoded_expect.len, encoded_actual_length);
    try std.testing.expectEqualSlices(u8, encoded_expect, encoded_actual[0..encoded_actual_length]);

    const encoded_size_max = codec.encode_size_max(decoded_expect.len);
    try std.testing.expect(encoded_expect.len <= encoded_size_max);

    const decoded_actual = try std.testing.allocator.alloc(Word, decoded_expect.len);
    defer std.testing.allocator.free(decoded_actual);

    const decoded_actual_length = codec.decode_all(encoded_actual, decoded_actual);
    try std.testing.expectEqual(decoded_expect.len, decoded_actual_length);
    try std.testing.expectEqualSlices(Word, decoded_expect, decoded_actual);
}
