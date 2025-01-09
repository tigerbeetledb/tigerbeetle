const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;
const MaskInt = DynamicBitSetUnmanaged.MaskInt;

const constants = @import("../constants.zig");

const ewah = @import("../ewah.zig").ewah(FreeSet.Word);
const stdx = @import("../stdx.zig");

const div_ceil = stdx.div_ceil;
const maybe = stdx.maybe;

/// This is logically a range of addresses within the FreeSet, but its actual fields are block
/// indexes for ease of calculation.
///
/// A reservation covers a range of both free and acquired blocks — when it is first created,
/// it is guaranteed to cover exactly as many free blocks as were requested by `reserve()`.
pub const Reservation = struct {
    block_base: usize,
    block_count: usize,
    /// An identifier for each reservation cycle, to verify that old reservations are not reused.
    session: usize,
};

/// The 0 address is reserved for usage as a sentinel and will never be returned by acquire().
///
/// Concurrent callers must reserve free blocks before acquiring them to ensure that
/// acquisition order is deterministic despite concurrent jobs acquiring blocks in
/// nondeterministic order.
///
/// The reservation lifecycle is:
///
///   1. Reserve: In deterministic order, each job (e.g. compaction) calls `reserve()` to
///      reserve the upper bound of blocks that it may need to acquire to complete.
///   2. Acquire: The jobs run concurrently. Each job acquires blocks only from its respective
///      reservation (via `acquire()`).
///   3. Forfeit: When a job finishes, it calls `forfeit()` to drop its reservation.
///   4. Done: When all pending reservations are forfeited, the reserved (but unacquired) space
///      is reclaimed.
///
pub const FreeSet = struct {
    pub const Word = u64;
    pub const BitsetKinds = enum {
        blocks_acquired,
        blocks_released,
    };
    const BlocksReleasedPriorCheckpointDurability = std.AutoArrayHashMapUnmanaged(u64, void);

    // Free set is stored in the grid (see `CheckpointTrailer`) and is not available until the
    // relevant blocks are fetched from disk (or other replicas) and decoded.
    //
    // Without the free set, only blocks belonging to the free set might be read and no blocks can
    // be written.
    opened: bool = false,

    /// Whether the current checkpoint is durable.
    checkpoint_durable: bool = false,

    /// If a shard has any free blocks, the corresponding index bit is zero.
    /// If a shard has no free blocks, the corresponding index bit is one.
    index: DynamicBitSetUnmanaged,

    /// Set bits indicate acquired blocks; unset bits indicate free blocks.
    blocks_acquired: DynamicBitSetUnmanaged,

    /// Set bits indicate blocks released in the current checkpoint, to be freed when the next
    /// checkpoint becomes durable.
    blocks_released: DynamicBitSetUnmanaged,

    /// Temporarily holds blocks released prior durability of the current checkpoint, to be freed
    /// when the next checkpoint becomes durable. These blocks are moved to blocks_released once the
    /// current checkpoint becomes durable.
    blocks_released_prior_checkpoint_durability: BlocksReleasedPriorCheckpointDurability,

    /// The number of blocks that are reserved, counting both acquired and free blocks
    /// from the start of `blocks`.
    /// Alternatively, the index of the first non-reserved block in `blocks`.
    reservation_blocks: usize = 0,

    /// The number of active reservations.
    reservation_count: usize = 0,

    /// Verify that when the caller transitions from creating reservations to forfeiting them,
    /// all reservations must be forfeited before additional reservations are made.
    reservation_state: enum {
        reserving,
        forfeiting,
    } = .reserving,

    /// Verifies that reservations are not allocated from or forfeited when they should not be.
    reservation_session: usize = 1,

    // Each shard is 8 cache lines because the CPU line fill buffer can fetch 10 lines in parallel.
    // And 8 is fast for division when computing the shard of a block.
    // Since the shard is scanned sequentially, the prefetching amortizes the cost of the single
    // cache miss. It also reduces the size of the index.
    //
    // e.g. 10TiB disk ÷ 64KiB/block ÷ 512*8 blocks/shard ÷ 8 shards/byte = 5120B index
    const shard_cache_lines = 8;
    pub const shard_bits = shard_cache_lines * constants.cache_line_size * @bitSizeOf(u8);
    comptime {
        assert(shard_bits == 4096);
        assert(@bitSizeOf(MaskInt) == 64);
        // Ensure there are no wasted padding bits at the end of the index.
        assert(shard_bits % @bitSizeOf(MaskInt) == 0);
    }

    pub fn init(allocator: mem.Allocator, options: struct {
        blocks_count: usize,
        blocks_released_prior_checkpoint_durability_max: usize,
    }) !FreeSet {
        assert(options.blocks_count % shard_bits == 0);
        assert(options.blocks_count % @bitSizeOf(Word) == 0);

        // Every block bit is covered by exactly one index bit.
        const shards_count = @divExact(options.blocks_count, shard_bits);
        var index = try DynamicBitSetUnmanaged.initEmpty(allocator, shards_count);
        errdefer index.deinit(allocator);

        var blocks_acquired = try DynamicBitSetUnmanaged.initEmpty(allocator, options.blocks_count);
        errdefer blocks_acquired.deinit(allocator);

        var blocks_released = try DynamicBitSetUnmanaged.initEmpty(allocator, options.blocks_count);
        errdefer blocks_released.deinit(allocator);

        var released_prior_checkpoint_durability: BlocksReleasedPriorCheckpointDurability = .{};
        try released_prior_checkpoint_durability.ensureTotalCapacity(
            allocator,
            options.blocks_released_prior_checkpoint_durability_max,
        );
        errdefer released_prior_checkpoint_durability.deinit();

        assert(index.count() == 0);
        assert(blocks_acquired.count() == 0);
        assert(blocks_released.count() == 0);
        assert(released_prior_checkpoint_durability.count() == 0);

        return FreeSet{
            .index = index,
            .blocks_acquired = blocks_acquired,
            .blocks_released = blocks_released,
            .blocks_released_prior_checkpoint_durability = released_prior_checkpoint_durability,
        };
    }
    pub fn deinit(set: *FreeSet, allocator: mem.Allocator) void {
        set.index.deinit(allocator);
        set.blocks_acquired.deinit(allocator);
        set.blocks_released.deinit(allocator);
        set.blocks_released_prior_checkpoint_durability.deinit(allocator);
    }

    pub fn reset(set: *FreeSet) void {
        for ([_]*DynamicBitSetUnmanaged{
            &set.index,
            &set.blocks_acquired,
            &set.blocks_released,
        }) |bitset| {
            var it = bitset.iterator(.{});
            while (it.next()) |bit| bitset.unset(bit);
        }

        set.blocks_released_prior_checkpoint_durability.clearRetainingCapacity();

        set.* = .{
            .index = set.index,
            .blocks_acquired = set.blocks_acquired,
            .blocks_released = set.blocks_released,
            .blocks_released_prior_checkpoint_durability = set
                .blocks_released_prior_checkpoint_durability,
            .reservation_session = set.reservation_session +% 1,
        };

        assert(set.index.count() == 0);
        assert(set.blocks_acquired.count() == 0);
        assert(set.blocks_released.count() == 0);
        assert(set.blocks_released_prior_checkpoint_durability.count() == 0);

        assert(!set.opened);
    }

    /// Opens a free set. Needs two inputs:
    ///
    ///   - the `encoded` byte buffers with the ewah-encoded acquired and released bitsets,
    ///   - the list of block addresses used to store both the encoded bitsets in the grid.
    ///
    /// Block addresses themselves are not a part of the encoded bitset for acquired blocks,
    /// see CheckpointTrailer for details.
    pub fn open(set: *FreeSet, options: struct { encoded: struct {
        blocks_acquired: []const []align(@alignOf(Word)) const u8,
        blocks_released: []const []align(@alignOf(Word)) const u8,
    }, block_addresses: struct {
        blocks_acquired: []const u64,
        blocks_released: []const u64,
    } }) void {
        assert(!set.opened);
        assert((options.encoded.blocks_acquired.len == 0 and
            options.encoded.blocks_released.len == 0) ==
            (options.block_addresses.blocks_acquired.len == 0 and
            options.block_addresses.blocks_released.len == 0));
        set.decode_chunks(
            options.encoded.blocks_acquired,
            options.encoded.blocks_released,
        );
        set.mark_released(options.block_addresses.blocks_acquired);
        set.mark_released(options.block_addresses.blocks_released);
        set.opened = true;
    }

    // A shortcut to initialize and open an empty free set for tests.
    pub fn open_empty(allocator: mem.Allocator, blocks_count: usize) !FreeSet {
        var set = try init(allocator, .{
            .blocks_count = blocks_count,
            .blocks_released_prior_checkpoint_durability_max = 0,
        });
        set.open(.{
            .encoded = .{ .blocks_acquired = &.{}, .blocks_released = &.{} },
            .block_addresses = .{ .blocks_acquired = &.{}, .blocks_released = &.{} },
        });
        // Mark checkpoint as durable so tests use blocks_released for block releases.
        // blocks_released_prior_checkpoint_durable is required to ensure correctness across
        // multiple replicas, while tests check the following flows in a single process:
        // * Block acquisition-release
        // * Bitset encoding-decoding
        set.checkpoint_durable = true;

        assert(set.opened);
        assert(set.count_free() == blocks_count);
        assert(set.count_released() == 0);
        return set;
    }

    fn verify_index(set: *const FreeSet) void {
        for (0..set.index.bit_length) |shard| {
            assert((set.find_free_block_in_shard(shard) == null) == set.index.isSet(shard));
        }
    }

    /// Returns the number of active reservations.
    pub fn count_reservations(set: FreeSet) usize {
        assert(set.opened);
        return set.reservation_count;
    }

    /// Returns the number of free blocks.
    pub fn count_free(set: FreeSet) usize {
        assert(set.opened);
        return set.blocks_acquired.capacity() - set.blocks_acquired.count();
    }

    /// Returns the number of acquired blocks.
    pub fn count_acquired(set: FreeSet) usize {
        assert(set.opened);
        return set.blocks_acquired.count();
    }

    /// Returns the number of released blocks.
    pub fn count_released(set: FreeSet) usize {
        assert(set.opened);
        return set.blocks_released.count() +
            set.blocks_released_prior_checkpoint_durability.count();
    }

    /// Returns the address of the highest acquired block.
    pub fn highest_address_acquired(set: FreeSet) ?u64 {
        assert(set.opened);
        var it = set.blocks_acquired.iterator(.{
            .kind = .set,
            .direction = .reverse,
        });

        if (it.next()) |block| {
            const address = block + 1;
            return address;
        } else {
            // All blocks are free.
            assert(set.blocks_acquired.count() == 0);
            return null;
        }
    }

    /// Reserve `reserve_count` free blocks. The blocks are not acquired yet.
    ///
    /// Invariants:
    ///
    ///   - If a reservation is returned, it covers exactly `reserve_count` free blocks, along with
    ///     any interleaved already-acquired blocks.
    ///   - Active reservations are exclusive (i.e. disjoint).
    ///     (A reservation is active until `forfeit()` is called.)
    ///
    /// Returns null if there are not enough blocks free and vacant.
    /// Returns a reservation which can be used with `acquire()`:
    /// - The caller should consider the returned Reservation as opaque and immutable.
    /// - Each `reserve()` call which returns a non-null Reservation must correspond to exactly one
    ///   `forfeit()` call.
    pub fn reserve(set: *FreeSet, reserve_count: usize) ?Reservation {
        assert(set.opened);
        assert(set.reservation_state == .reserving);
        assert(reserve_count > 0);

        const shard_start = find_bit(
            set.index,
            @divFloor(set.reservation_blocks, shard_bits),
            set.index.bit_length,
            .unset,
        ) orelse return null;

        // The reservation may cover (and ignore) already-acquired blocks due to fragmentation.
        var block = @max(shard_start * shard_bits, set.reservation_blocks);
        var reserved: usize = 0;
        while (reserved < reserve_count) : (reserved += 1) {
            block = 1 + (find_bit(
                set.blocks_acquired,
                block,
                set.blocks_acquired.bit_length,
                .unset,
            ) orelse return null);
        }

        const block_base = set.reservation_blocks;
        const block_count = block - set.reservation_blocks;
        set.reservation_blocks += block_count;
        set.reservation_count += 1;

        return Reservation{
            .block_base = block_base,
            .block_count = block_count,
            .session = set.reservation_session,
        };
    }

    /// After invoking `forfeit()`, the reservation must never be used again.
    pub fn forfeit(set: *FreeSet, reservation: Reservation) void {
        assert(set.opened);
        assert(set.reservation_session == reservation.session);

        set.reservation_count -= 1;
        if (set.reservation_count == 0) {
            // All reservations have been dropped.
            set.reservation_blocks = 0;
            set.reservation_session +%= 1;
            set.reservation_state = .reserving;
        } else {
            set.reservation_state = .forfeiting;
        }
    }

    /// Marks a free block from the reservation as allocated, and returns the address.
    /// The reservation must not have been forfeited yet.
    /// The reservation must belong to the current cycle of reservations.
    ///
    /// Invariants:
    ///
    ///   - An acquired block cannot be acquired again until it has been released and the release
    ///     has been checkpointed.
    ///
    /// Returns null if no free block is available in the reservation.
    pub fn acquire(set: *FreeSet, reservation: Reservation) ?u64 {
        assert(set.opened);
        assert(set.reservation_count > 0);
        assert(reservation.block_count > 0);
        assert(reservation.block_base < set.reservation_blocks);
        assert(reservation.block_base + reservation.block_count <= set.reservation_blocks);
        assert(reservation.session == set.reservation_session);

        const shard_start = find_bit(
            set.index,
            @divFloor(reservation.block_base, shard_bits),
            div_ceil(reservation.block_base + reservation.block_count, shard_bits),
            .unset,
        ) orelse return null;
        assert(!set.index.isSet(shard_start));

        const reservation_start = @max(
            shard_start * shard_bits,
            reservation.block_base,
        );
        const reservation_end = reservation.block_base + reservation.block_count;
        const block = find_bit(
            set.blocks_acquired,
            reservation_start,
            reservation_end,
            .unset,
        ) orelse return null;
        assert(block >= reservation.block_base);
        assert(block <= reservation.block_base + reservation.block_count);
        assert(!set.blocks_acquired.isSet(block));
        assert(!set.blocks_released.isSet(block));
        assert(!set.blocks_released_prior_checkpoint_durability.contains(block));

        // Even if "shard_start" has free blocks, we might acquire our block from a later shard.
        // (This is possible because our reservation begins part-way through the shard.)
        const shard = @divFloor(block, shard_bits);
        maybe(shard == shard_start);
        assert(shard >= shard_start);

        set.blocks_acquired.set(block);
        // Update the index when every block in the shard is acquired.
        if (set.find_free_block_in_shard(shard) == null) set.index.set(shard);

        const address = block + 1;
        return address;
    }

    fn find_free_block_in_shard(set: FreeSet, shard: usize) ?usize {
        maybe(set.opened);
        const shard_start = shard * shard_bits;
        const shard_end = shard_start + shard_bits;
        assert(shard_start < set.blocks_acquired.bit_length);

        return find_bit(set.blocks_acquired, shard_start, shard_end, .unset);
    }

    pub fn is_free(set: FreeSet, address: u64) bool {
        if (set.opened) {
            const block = address - 1;
            return !set.blocks_acquired.isSet(block);
        } else {
            // When the free set is not open, conservatively assume that the block is acquired.
            //
            // This path is hit only when the replica opens the free set, reading its blocks from
            // the grid.
            return false;
        }
    }

    pub fn is_released(set: *const FreeSet, address: u64) bool {
        assert(set.opened);
        const block = address - 1;
        return set.blocks_released_prior_checkpoint_durability.contains(block) or
            set.blocks_released.isSet(block);
    }

    /// Returns `true` if the block at the given address would be freed when the current checkpoint
    /// becomes durable (when checkpoint_durable is set to `true`).
    ///
    /// Calling this function is only valid while the current checkpoint is not durable. During this
    /// period, blocks are marked as released in `blocks_released_prior_checkpoint_durability`;
    /// `blocks_released` remains unchanged and contains blocks released during the previous
    /// checkpoint interval.
    pub fn to_be_freed_at_checkpoint_durability(set: *const FreeSet, address: u64) bool {
        assert(set.opened);
        assert(!set.checkpoint_durable);
        maybe(set.blocks_released_prior_checkpoint_durability.count() > 0);
        const block = address - 1;
        return set.blocks_released.isSet(block);
    }

    /// Leave the address acquired for now, but free it when the next checkpoint becomes durable.
    /// This ensures that it will not be overwritten during the current checkpoint — the block may
    /// still be needed if we crash and recover from the current checkpoint.
    /// (TODO) If the block was created since the last checkpoint then it's safe to free
    ///        immediately. This may reduce space amplification, especially for smaller datasets.
    ///        (Note: This must be careful not to release while any reservations are held
    ///        to avoid making the reservation's acquire()s nondeterministic).
    pub fn release(set: *FreeSet, address: u64) void {
        assert(set.opened);

        const block = address - 1;
        assert(set.blocks_acquired.isSet(block));
        assert(!set.blocks_released.isSet(block));
        assert(!set.blocks_released_prior_checkpoint_durability.contains(block));

        // `blocks_released` remains unchanged while the current checkpoint is not durable,
        // since it contains blocks released in the previous checkpoint. These blocks must not be
        // freed till the current checkpoint is durable, so as to maintain the durability of these
        // blocks on a commit quorum of replicas.
        if (!set.checkpoint_durable) {
            set.blocks_released_prior_checkpoint_durability.putAssumeCapacity(block, {});
        } else {
            set.blocks_released.set(block);
        }
    }

    /// Mark the given addresses as allocated in the current checkpoint, but free in the next one.
    ///
    /// This is used only when reading a free set from the grid. On disk representation of the
    /// free set doesn't include the blocks storing the free set itself, and these blocks must be
    /// manually patched in after decoding. As the next checkpoint will have a completely different
    /// free set, the blocks can be simultaneously released.
    fn mark_released(set: *FreeSet, addresses: []const u64) void {
        assert(!set.opened);
        var address_previous: u64 = 0;
        for (addresses) |address| {
            assert(address > 0);

            // Assert that addresses are sorted and unique. Sortedness is not a requirement, but
            // a consequence of "first free" allocation algorithm.
            assert(address > address_previous);
            address_previous = address;

            const block = address - 1;

            assert(!set.blocks_acquired.isSet(block));
            assert(!set.blocks_released.isSet(block));
            assert(!set.blocks_released_prior_checkpoint_durability.contains(block));

            set.blocks_acquired.set(block);

            const shard = @divFloor(block, shard_bits);
            // Update the index when every block in the shard is acquired.
            if (set.find_free_block_in_shard(shard) == null) set.index.set(shard);

            if (set.checkpoint_durable) {
                set.blocks_released.set(block);
            } else {
                set.blocks_released_prior_checkpoint_durability.putAssumeCapacity(block, {});
            }
        }
    }

    /// Given the address, marks an acquired block as free.
    fn free(set: *FreeSet, address: u64) void {
        assert(set.opened);

        const block = address - 1;
        assert(set.blocks_acquired.isSet(block));
        assert(set.blocks_released.isSet(block));
        assert(set.reservation_count == 0);
        assert(set.reservation_blocks == 0);

        set.index.unset(@divFloor(block, shard_bits));
        set.blocks_acquired.unset(block);
        set.blocks_released.unset(block);
    }

    pub fn mark_checkpoint_not_durable(set: *FreeSet) void {
        assert(set.opened);
        assert(set.checkpoint_durable);
        assert(set.blocks_released_prior_checkpoint_durability.count() == 0);
        set.checkpoint_durable = false;
    }

    /// Marks the current checkpoint as durable and moves all released blocks from
    /// `blocks_released_prior_checkpoint_durability` → `blocks_released`.
    ///
    /// Must be called after all released blocks are freed at checkpoint durability, via
    /// `free_released_blocks`.
    pub fn mark_checkpoint_durable(set: *FreeSet) void {
        assert(set.opened);
        assert(!set.checkpoint_durable);
        assert(set.blocks_released.count() == 0);

        set.checkpoint_durable = true;

        // Block releases from the current checkpoint that were temporarily recorded in
        // blocks_released_prior_checkpoint_durability can now be moved to blocks_released.
        while (set.blocks_released_prior_checkpoint_durability.count() > 0) {
            const block = set.blocks_released_prior_checkpoint_durability.keys()[0];
            set.blocks_released_prior_checkpoint_durability.swapRemoveAt(0);
            set.blocks_released.set(block);
        }
        assert(set.blocks_released_prior_checkpoint_durability.count() == 0);

        // Index verification is O(blocks.bit_length) so do it only at checkpoint, which is
        // also linear.
        set.verify_index();
    }

    pub fn free_released_blocks(set: *FreeSet) void {
        assert(set.opened);
        var it = set.blocks_released.iterator(.{ .kind = .set });
        while (it.next()) |block| set.free(block + 1);

        assert(set.blocks_released.count() == 0);
    }

    /// Decodes the compressed bitset chunks in `source_chunks` into `target_bitset`.
    /// Panics if the `source_chunks` encoding is invalid.
    pub fn decode(
        set: *FreeSet,
        target_bitset: FreeSet.BitsetKinds,
        source_chunks: []const []align(@alignOf(Word)) const u8,
    ) void {
        var source_size: usize = 0;

        for (source_chunks) |source_chunk| source_size += source_chunk.len;

        var decoder = ewah.decode_chunks(switch (target_bitset) {
            .blocks_acquired => bit_set_masks(set.blocks_acquired),
            .blocks_released => bit_set_masks(set.blocks_released),
        }, source_size);

        var words_decoded: usize = 0;
        for (source_chunks) |source_chunk| {
            words_decoded += decoder.decode_chunk(source_chunk);
        }
        assert(words_decoded * @bitSizeOf(MaskInt) <= set.blocks_acquired.bit_length);
        assert(decoder.done());
    }

    pub fn decode_chunks(
        set: *FreeSet,
        source_chunks_blocks_acquired: []const []align(@alignOf(Word)) const u8,
        source_chunks_blocks_released: []const []align(@alignOf(Word)) const u8,
    ) void {
        // Verify that this FreeSet is entirely unallocated.
        assert(set.index.count() == 0);
        assert(set.blocks_acquired.count() == 0);
        assert(set.blocks_released.count() == 0);
        assert(set.blocks_released_prior_checkpoint_durability.count() == 0);

        assert(set.reservation_count == 0);
        assert(set.reservation_blocks == 0);

        set.decode(.blocks_acquired, source_chunks_blocks_acquired);
        set.decode(.blocks_released, source_chunks_blocks_released);

        for (0..set.index.bit_length) |shard| {
            if (set.find_free_block_in_shard(shard) == null) set.index.set(shard);
        }
    }

    /// Returns the maximum number of bytes that `blocks_count` blocks need to be encoded.
    pub fn encode_size_max(blocks_count: usize) usize {
        assert(blocks_count % shard_bits == 0);
        assert(blocks_count % @bitSizeOf(usize) == 0);

        return ewah.encode_size_max(@divExact(blocks_count, @bitSizeOf(Word)));
    }

    fn encode(
        set: *const FreeSet,
        source_bitset: FreeSet.BitsetKinds,
        target_chunks: []const []align(@alignOf(Word)) u8,
    ) usize {
        var encoder = switch (source_bitset) {
            .blocks_acquired => ewah.encode_chunks(bit_set_masks(set.blocks_acquired)),
            .blocks_released => ewah.encode_chunks(bit_set_masks(set.blocks_released)),
        };
        defer assert(encoder.done());

        var bytes_encoded_total: u64 = 0;
        for (target_chunks) |chunk| {
            const bytes_encoded =
                @as(u32, @intCast(encoder.encode_chunk(chunk)));
            assert(bytes_encoded > 0);

            bytes_encoded_total += bytes_encoded;

            if (encoder.done()) break;
        } else unreachable;

        return bytes_encoded_total;
    }

    pub fn encode_chunks(
        set: *const FreeSet,
        target_chunks_blocks_acquired: []const []align(@alignOf(Word)) u8,
        target_chunks_blocks_released: []const []align(@alignOf(Word)) u8,
    ) struct { encoded_size_blocks_acquired: u64, encoded_size_blocks_released: u64 } {
        assert(set.opened);
        assert(set.reservation_count == 0);
        assert(set.reservation_blocks == 0);

        return .{
            .encoded_size_blocks_acquired = set.encode(
                .blocks_acquired,
                target_chunks_blocks_acquired,
            ),
            .encoded_size_blocks_released = set.encode(
                .blocks_released,
                target_chunks_blocks_released,
            ),
        };
    }
};

fn bit_set_masks(bit_set: DynamicBitSetUnmanaged) []MaskInt {
    const len = div_ceil(bit_set.bit_length, @bitSizeOf(MaskInt));
    return bit_set.masks[0..len];
}

test "FreeSet block shard count" {
    if (constants.block_size != 64 * 1024) return;
    const blocks_in_tb = @divExact(1 << 40, constants.block_size);
    try test_block_shards_count(5120 * 8, 10 * blocks_in_tb);
    try test_block_shards_count(5120 * 8 - 1, 10 * blocks_in_tb - FreeSet.shard_bits);
    try test_block_shards_count(1, FreeSet.shard_bits); // Must be at least one index bit.
}

fn test_block_shards_count(expect_shards_count: usize, blocks_count: usize) !void {
    var set = try FreeSet.open_empty(std.testing.allocator, blocks_count);
    defer set.deinit(std.testing.allocator);

    try std.testing.expectEqual(expect_shards_count, set.index.bit_length);
}

test "FreeSet highest_address_acquired" {
    const expectEqual = std.testing.expectEqual;
    const blocks_count = FreeSet.shard_bits;
    var set = try FreeSet.open_empty(std.testing.allocator, blocks_count);
    defer set.deinit(std.testing.allocator);

    {
        const reservation = set.reserve(6).?;
        defer set.forfeit(reservation);

        try expectEqual(@as(?u64, null), set.highest_address_acquired());
        try expectEqual(@as(?u64, 1), set.acquire(reservation));
        try expectEqual(@as(?u64, 2), set.acquire(reservation));
        try expectEqual(@as(?u64, 3), set.acquire(reservation));
    }

    try expectEqual(@as(?u64, 3), set.highest_address_acquired());

    set.release(2);
    set.free(2);
    try expectEqual(@as(?u64, 3), set.highest_address_acquired());

    set.release(3);
    set.free(3);
    try expectEqual(@as(?u64, 1), set.highest_address_acquired());

    set.release(1);
    set.free(1);
    try expectEqual(@as(?u64, null), set.highest_address_acquired());

    {
        const reservation = set.reserve(6).?;
        defer set.forfeit(reservation);

        try expectEqual(@as(?u64, 1), set.acquire(reservation));
        try expectEqual(@as(?u64, 2), set.acquire(reservation));
        try expectEqual(@as(?u64, 3), set.acquire(reservation));
    }

    {
        set.release(3);
        try expectEqual(@as(?u64, 3), set.highest_address_acquired());

        set.free(3);
        try expectEqual(@as(?u64, 2), set.highest_address_acquired());
    }
}

test "FreeSet acquire/release" {
    try test_acquire_release(FreeSet.shard_bits);
    try test_acquire_release(2 * FreeSet.shard_bits);
    try test_acquire_release(63 * FreeSet.shard_bits);
    try test_acquire_release(64 * FreeSet.shard_bits);
    try test_acquire_release(65 * FreeSet.shard_bits);
}

fn test_acquire_release(blocks_count: usize) !void {
    const expectEqual = std.testing.expectEqual;
    // Acquire everything, then release, then acquire again.
    var set = try FreeSet.open_empty(std.testing.allocator, blocks_count);
    defer set.deinit(std.testing.allocator);

    var empty = try FreeSet.open_empty(std.testing.allocator, blocks_count);
    defer empty.deinit(std.testing.allocator);

    {
        const reservation = set.reserve(blocks_count).?;
        defer set.forfeit(reservation);

        var i: usize = 0;
        while (i < blocks_count) : (i += 1) {
            try expectEqual(@as(?u64, i + 1), set.acquire(reservation));
        }
        try expectEqual(@as(?u64, null), set.acquire(reservation));
    }

    try expectEqual(@as(u64, set.blocks_acquired.bit_length), set.count_acquired());
    try expectEqual(@as(u64, 0), set.count_free());

    {
        var i: usize = 0;
        while (i < blocks_count) : (i += 1) {
            set.release(@as(u64, i + 1));
            set.free(@as(u64, i + 1));
        }
        try expect_free_set_equal(empty, set);
    }

    try expectEqual(@as(u64, 0), set.count_acquired());
    try expectEqual(@as(u64, set.blocks_acquired.bit_length), set.count_free());

    {
        const reservation = set.reserve(blocks_count).?;
        defer set.forfeit(reservation);

        var i: usize = 0;
        while (i < blocks_count) : (i += 1) {
            try expectEqual(@as(?u64, i + 1), set.acquire(reservation));
        }
        try expectEqual(@as(?u64, null), set.acquire(reservation));
    }
}

test "FreeSet.reserve/acquire" {
    const blocks_count_total = 4096;
    var set = try FreeSet.open_empty(std.testing.allocator, blocks_count_total);
    defer set.deinit(std.testing.allocator);

    // At most `blocks_count_total` blocks are initially available for reservation.
    try std.testing.expectEqual(set.reserve(blocks_count_total + 1), null);
    const r1 = set.reserve(blocks_count_total - 1);
    const r2 = set.reserve(1);
    try std.testing.expectEqual(set.reserve(1), null);
    set.forfeit(r1.?);
    set.forfeit(r2.?);

    var address: usize = 1; // Start at 1 because addresses are >0.
    {
        const reservation = set.reserve(2).?;
        defer set.forfeit(reservation);

        try std.testing.expectEqual(set.acquire(reservation), address + 0);
        try std.testing.expectEqual(set.acquire(reservation), address + 1);
        try std.testing.expectEqual(set.acquire(reservation), null);
    }
    address += 2;

    {
        // Blocks are acquired from the target reservation.
        const reservation_1 = set.reserve(2).?;
        const reservation_2 = set.reserve(2).?;
        defer set.forfeit(reservation_1);
        defer set.forfeit(reservation_2);

        try std.testing.expectEqual(set.acquire(reservation_1), address + 0);
        try std.testing.expectEqual(set.acquire(reservation_2), address + 2);
        try std.testing.expectEqual(set.acquire(reservation_1), address + 1);
        try std.testing.expectEqual(set.acquire(reservation_1), null);
        try std.testing.expectEqual(set.acquire(reservation_2), address + 3);
        try std.testing.expectEqual(set.acquire(reservation_2), null);
    }
    address += 4;
}

test "FreeSet checkpoint" {
    const expectEqual = std.testing.expectEqual;
    const blocks_count = FreeSet.shard_bits;
    var set = try FreeSet.open_empty(std.testing.allocator, blocks_count);
    defer set.deinit(std.testing.allocator);

    var empty = try FreeSet.open_empty(std.testing.allocator, blocks_count);
    defer empty.deinit(std.testing.allocator);

    var full = try FreeSet.open_empty(std.testing.allocator, blocks_count);
    defer full.deinit(std.testing.allocator);

    {
        // Acquire all of `full`'s blocks.
        const reservation = full.reserve(blocks_count).?;
        defer full.forfeit(reservation);

        var i: usize = 0;
        while (i < full.blocks_acquired.bit_length) : (i += 1) {
            try expectEqual(@as(?u64, i + 1), full.acquire(reservation));
        }
    }

    {
        // Acquire & stage-release every block.
        const reservation = set.reserve(blocks_count).?;
        defer set.forfeit(reservation);

        var i: usize = 0;
        while (i < set.blocks_acquired.bit_length) : (i += 1) {
            try expectEqual(@as(?u64, i + 1), set.acquire(reservation));
            set.release(i + 1);

            // These count functions treat staged blocks as acquired.
            try expectEqual(@as(u64, i + 1), set.count_acquired());
            try expectEqual(@as(u64, set.blocks_acquired.bit_length - i - 1), set.count_free());
        }
        // All blocks are still acquired, though staged to release at the next checkpoint.
        try expectEqual(@as(?u64, null), set.acquire(reservation));
    }

    // Free all the blocks.
    set.free_released_blocks();
    try expect_free_set_equal(empty, set);
    try expectEqual(@as(usize, 0), set.blocks_released.count());

    {
        // Allocate & stage-release all blocks again.
        const reservation = set.reserve(blocks_count).?;
        defer set.forfeit(reservation);
        var i: usize = 0;
        while (i < set.blocks_acquired.bit_length) : (i += 1) {
            try expectEqual(@as(?u64, i + 1), set.acquire(reservation));
            set.release(i + 1);
        }
    }

    const set_encoded_blocks_acquired = try std.testing.allocator.alignedAlloc(
        u8,
        @alignOf(FreeSet.Word),
        FreeSet.encode_size_max(set.blocks_acquired.bit_length),
    );
    const set_encoded_blocks_released = try std.testing.allocator.alignedAlloc(
        u8,
        @alignOf(FreeSet.Word),
        FreeSet.encode_size_max(set.blocks_released.bit_length),
    );

    defer std.testing.allocator.free(set_encoded_blocks_acquired);
    defer std.testing.allocator.free(set_encoded_blocks_released);

    var set_decoded = try FreeSet.open_empty(std.testing.allocator, blocks_count);

    defer set_decoded.deinit(std.testing.allocator);

    {
        const free_set_encoded = set.encode_chunks(
            &.{set_encoded_blocks_acquired},
            &.{set_encoded_blocks_released},
        );

        set_decoded.decode_chunks(
            &.{set_encoded_blocks_acquired[0..free_set_encoded.encoded_size_blocks_acquired]},
            &.{set_encoded_blocks_released[0..free_set_encoded.encoded_size_blocks_released]},
        );
        try expect_free_set_equal(set, set_decoded);
    }

    {
        set_decoded.reset();
        const free_set_encoded = full.encode_chunks(
            &.{set_encoded_blocks_acquired},
            &.{set_encoded_blocks_released},
        );

        set_decoded.decode_chunks(
            &.{set_encoded_blocks_acquired[0..free_set_encoded.encoded_size_blocks_acquired]},
            &.{set_encoded_blocks_released[0..free_set_encoded.encoded_size_blocks_released]},
        );
        try expect_free_set_equal(full, set_decoded);
    }
}

test "FreeSet encode, decode, encode" {
    const shard_bits = FreeSet.shard_bits / @bitSizeOf(usize);
    // Uniform.
    try test_encode(&.{.{ .fill = .uniform_ones, .words = shard_bits }});
    try test_encode(&.{.{ .fill = .uniform_zeros, .words = shard_bits }});
    try test_encode(&.{.{ .fill = .literal, .words = shard_bits }});
    try test_encode(&.{.{ .fill = .uniform_ones, .words = std.math.maxInt(u16) + 1 }});

    // Mixed.
    try test_encode(&.{
        .{ .fill = .uniform_ones, .words = shard_bits / 4 },
        .{ .fill = .uniform_zeros, .words = shard_bits / 4 },
        .{ .fill = .literal, .words = shard_bits / 4 },
        .{ .fill = .uniform_ones, .words = shard_bits / 4 },
    });

    // Random.
    const seed = std.crypto.random.int(u64);
    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    const fills = [_]TestPatternFill{ .uniform_ones, .uniform_zeros, .literal };
    var t: usize = 0;
    while (t < 10) : (t += 1) {
        var patterns = std.ArrayList(TestPattern).init(std.testing.allocator);
        defer patterns.deinit();

        var i: usize = 0;
        while (i < shard_bits) : (i += 1) {
            try patterns.append(.{
                .fill = fills[random.uintLessThan(usize, fills.len)],
                .words = 1,
            });
        }
        try test_encode(patterns.items);
    }
}

const TestPattern = struct {
    fill: TestPatternFill,
    words: usize,
};

const TestPatternFill = enum { uniform_ones, uniform_zeros, literal };

fn test_encode(patterns: []const TestPattern) !void {
    const seed = std.crypto.random.int(u64);
    var prng = std.rand.DefaultPrng.init(seed);
    const random = prng.random();

    var blocks_count: usize = 0;
    for (patterns) |pattern| blocks_count += pattern.words * @bitSizeOf(usize);

    var decoded_expect = try FreeSet.open_empty(std.testing.allocator, blocks_count);
    defer decoded_expect.deinit(std.testing.allocator);

    {
        // The `index` will start out one-filled. Every pattern containing a zero will update the
        // corresponding index bit with a zero (probably multiple times) to ensure it ends up synced
        // with `blocks`.
        decoded_expect.index.toggleAll();
        assert(decoded_expect.index.count() == decoded_expect.index.capacity());

        // Fill the bitset according to the patterns.
        var blocks = bit_set_masks(decoded_expect.blocks_acquired);
        var blocks_offset: usize = 0;
        for (patterns) |pattern| {
            var i: usize = 0;
            while (i < pattern.words) : (i += 1) {
                blocks[blocks_offset] = switch (pattern.fill) {
                    .uniform_ones => ~@as(usize, 0),
                    .uniform_zeros => 0,
                    .literal => random.intRangeLessThan(usize, 1, std.math.maxInt(usize)),
                };
                const index_bit = blocks_offset * @bitSizeOf(usize) / FreeSet.shard_bits;
                if (pattern.fill != .uniform_ones) decoded_expect.index.unset(index_bit);
                blocks_offset += 1;
            }
        }
        assert(blocks_offset == blocks.len);
    }

    var encoded = try std.testing.allocator.alignedAlloc(
        u8,
        @alignOf(FreeSet.Word),
        FreeSet.encode_size_max(decoded_expect.blocks_acquired.bit_length),
    );
    defer std.testing.allocator.free(encoded);

    try std.testing.expectEqual(encoded.len % 8, 0);
    const encoded_length = decoded_expect.encode(.blocks_acquired, &.{encoded});

    var decoded_actual = try FreeSet.open_empty(std.testing.allocator, blocks_count);
    defer decoded_actual.deinit(std.testing.allocator);

    decoded_actual.decode_chunks(&.{encoded[0..encoded_length]}, &.{});
    try expect_free_set_equal(decoded_expect, decoded_actual);
}

fn expect_free_set_equal(a: FreeSet, b: FreeSet) !void {
    try expect_bit_set_equal(a.blocks_acquired, b.blocks_acquired);
    try expect_bit_set_equal(a.blocks_released, b.blocks_released);
    try expect_bit_set_equal(a.index, b.index);
}

fn expect_bit_set_equal(a: DynamicBitSetUnmanaged, b: DynamicBitSetUnmanaged) !void {
    try std.testing.expectEqual(a.bit_length, b.bit_length);
    const a_masks = bit_set_masks(a);
    const b_masks = bit_set_masks(b);
    for (a_masks, 0..) |aw, i| try std.testing.expectEqual(aw, b_masks[i]);
}

test "FreeSet decode small bitset into large bitset" {
    const shard_bits = FreeSet.shard_bits;
    var small_set = try FreeSet.open_empty(std.testing.allocator, shard_bits);
    defer small_set.deinit(std.testing.allocator);

    {
        // Set up a small bitset (with blocks_count==shard_bits) with no free blocks.
        const reservation = small_set.reserve(small_set.blocks_acquired.bit_length).?;
        defer small_set.forfeit(reservation);

        var i: usize = 0;
        while (i < small_set.blocks_acquired.bit_length) : (i += 1) {
            _ = small_set.acquire(reservation);
        }
    }

    var small_buffer = try std.testing.allocator.alignedAlloc(
        u8,
        @alignOf(usize),
        FreeSet.encode_size_max(small_set.blocks_acquired.bit_length),
    );
    defer std.testing.allocator.free(small_buffer);

    const small_buffer_written = small_set.encode(.blocks_acquired, &.{small_buffer});

    // Decode the serialized small bitset into a larger bitset (with blocks_count==2*shard_bits).
    var big_set = try FreeSet.open_empty(std.testing.allocator, 2 * shard_bits);
    defer big_set.deinit(std.testing.allocator);

    big_set.decode(.blocks_acquired, &.{small_buffer[0..small_buffer_written]});
    big_set.opened = true;

    var block: usize = 0;
    while (block < 2 * shard_bits) : (block += 1) {
        const address = block + 1;
        try std.testing.expectEqual(shard_bits <= block, big_set.is_free(address));
    }
}

test "FreeSet encode/decode manual" {
    const encoded_expect = mem.sliceAsBytes(&[_]usize{
        // Mask 1: run of 2 words of 0s, then 3 literals
        0 | (2 << 1) | (3 << 32),
        0b10101010_10101010_10101010_10101010_10101010_10101010_10101010_10101010, // literal 1
        0b01010101_01010101_01010101_01010101_01010101_01010101_01010101_01010101, // literal 2
        0b10101010_10101010_10101010_10101010_10101010_10101010_10101010_10101010, // literal 3
        // Mask 2: run of 59 words of 1s, then 0 literals
        //
        // 59 is chosen so that because the blocks_count must be a multiple of the shard size:
        // shard_bits = 4096 bits = 64 words × 64 bits/word = (2+3+59)*64
        1 | ((64 - 5) << 1),
    });
    const decoded_expect = [_]usize{
        0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000, // run 1
        0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
        0b10101010_10101010_10101010_10101010_10101010_10101010_10101010_10101010, // literal 1
        0b01010101_01010101_01010101_01010101_01010101_01010101_01010101_01010101, // literal 2
        0b10101010_10101010_10101010_10101010_10101010_10101010_10101010_10101010, // literal 3
    } ++ ([1]usize{~@as(usize, 0)} ** (64 - 5));
    const blocks_count = decoded_expect.len * @bitSizeOf(usize);

    // Test decode.
    var decoded_actual = try FreeSet.open_empty(std.testing.allocator, blocks_count);
    defer decoded_actual.deinit(std.testing.allocator);

    decoded_actual.decode(.blocks_acquired, &.{encoded_expect});
    decoded_actual.opened = true;
    try std.testing.expectEqual(
        decoded_expect.len,
        bit_set_masks(decoded_actual.blocks_acquired).len,
    );
    try std.testing.expectEqualSlices(
        usize,
        &decoded_expect,
        bit_set_masks(decoded_actual.blocks_acquired),
    );

    // Test encode.
    const encoded_actual = try std.testing.allocator.alignedAlloc(
        u8,
        @alignOf(usize),
        FreeSet.encode_size_max(decoded_actual.blocks_acquired.bit_length),
    );
    defer std.testing.allocator.free(encoded_actual);

    const encoded_actual_length = decoded_actual.encode(.blocks_acquired, &.{encoded_actual});
    try std.testing.expectEqual(encoded_expect.len, encoded_actual_length);
}

/// Returns the index of the first set/unset bit (relative to the start of the bitset) within
/// the range bit_min…bit_max (inclusive…exclusive).
fn find_bit(
    bit_set: DynamicBitSetUnmanaged,
    bit_min: usize,
    bit_max: usize,
    comptime bit_kind: std.bit_set.IteratorOptions.Type,
) ?usize {
    assert(bit_max >= bit_min);
    assert(bit_max <= bit_set.bit_length);

    const word_start = @divFloor(bit_min, @bitSizeOf(MaskInt)); // Inclusive.
    const word_offset = @mod(bit_min, @bitSizeOf(MaskInt));
    const word_end = div_ceil(bit_max, @bitSizeOf(MaskInt)); // Exclusive.
    const words_total = div_ceil(bit_set.bit_length, @bitSizeOf(MaskInt));
    if (word_end == word_start) return null;
    assert(word_end > word_start);

    // Only iterate over the subset of bits that were requested.
    var iterator = bit_set.iterator(.{ .kind = bit_kind });
    iterator.words_remain = bit_set.masks[word_start + 1 .. word_end];

    const mask = ~@as(MaskInt, 0);
    var word = bit_set.masks[word_start];
    if (bit_kind == .unset) word = ~word;
    iterator.bits_remain = word & std.math.shl(MaskInt, mask, word_offset);

    if (word_end != words_total) iterator.last_word_mask = mask;

    const b = bit_min - word_offset + (iterator.next() orelse return null);
    return if (b < bit_max) b else null;
}

test "find_bit" {
    var prng = std.rand.DefaultPrng.init(123);
    const random = prng.random();

    var bit_length: usize = 1;
    while (bit_length <= @bitSizeOf(std.DynamicBitSetUnmanaged.MaskInt) * 4) : (bit_length += 1) {
        var bit_set = try std.DynamicBitSetUnmanaged.initEmpty(std.testing.allocator, bit_length);
        defer bit_set.deinit(std.testing.allocator);

        const p = random.uintLessThan(usize, 100);
        var b: usize = 0;
        while (b < bit_length) : (b += 1) bit_set.setValue(b, p < random.uintLessThan(usize, 100));

        var i: usize = 0;
        while (i < 20) : (i += 1) try test_find_bit(random, bit_set, .set);
        while (i < 40) : (i += 1) try test_find_bit(random, bit_set, .unset);
    }
}

fn test_find_bit(
    random: std.rand.Random,
    bit_set: DynamicBitSetUnmanaged,
    comptime bit_kind: std.bit_set.IteratorOptions.Type,
) !void {
    const bit_min = random.uintLessThan(usize, bit_set.bit_length);
    const bit_max = random.uintLessThan(usize, bit_set.bit_length - bit_min) + bit_min;
    assert(bit_max >= bit_min);
    assert(bit_max <= bit_set.bit_length);

    const bit_actual = find_bit(bit_set, bit_min, bit_max, bit_kind);
    if (bit_actual) |bit| {
        assert(bit_set.isSet(bit) == (bit_kind == .set));
        assert(bit >= bit_min);
        assert(bit < bit_max);
    }

    var iterator = bit_set.iterator(.{ .kind = bit_kind });
    while (iterator.next()) |bit| {
        if (bit_min <= bit and bit < bit_max) {
            try std.testing.expectEqual(bit_actual, bit);
            break;
        }
    } else {
        try std.testing.expectEqual(bit_actual, null);
    }
}

test "FreeSet.acquire part-way through a shard" {
    var set = try FreeSet.open_empty(std.testing.allocator, FreeSet.shard_bits * 3);
    defer set.deinit(std.testing.allocator);

    const reservation_a = set.reserve(1).?;
    defer set.forfeit(reservation_a);

    const reservation_b = set.reserve(2 * FreeSet.shard_bits).?;
    defer set.forfeit(reservation_b);

    // Acquire all of reservation B.
    // At the end, the first shard still has a bit free (reserved by A).
    for (0..reservation_b.block_count) |i| {
        const address = set.acquire(reservation_b).?;
        try std.testing.expectEqual(address - 1, reservation_a.block_count + i);
        set.verify_index();
    }
    try std.testing.expectEqual(set.acquire(reservation_b), null);
}
