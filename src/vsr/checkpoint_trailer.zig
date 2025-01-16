const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const vsr = @import("../vsr.zig");
const stdx = @import("../stdx.zig");
const schema = @import("../lsm/schema.zig");
const GridType = @import("../vsr/grid.zig").GridType;
const BlockPtr = @import("../vsr/grid.zig").BlockPtr;
const BlockPtrConst = @import("../vsr/grid.zig").BlockPtrConst;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const constants = @import("../constants.zig");
const FreeSet = @import("./free_set.zig").FreeSet;
const BlockType = schema.BlockType;

/// CheckpointTrailer is the persistent representation of the free set and client sessions.
/// It defines the layout of the free set and client sessions as stored in the grid between
/// checkpoints.
///
/// - Free set is stored as a linked list of blocks containing EWAH-encoding of a bitset of acquired
///   blocks. The length of the linked list is proportional to the degree of fragmentation, rather
///   that to the size of the data file. The common case is a single block.
///
///   The blocks holding free set itself are marked as free in the on-disk encoding, because the
///   number of blocks required to store the compressed bitset becomes known only after encoding.
///   This might or might not be related to Russell's paradox.
///
/// - Client sessions is stored as a linked list of blocks containing reply headers and session
///   numbers.
///
/// Linked list is a FIFO. While the blocks are written in the direct order, they have to be read in
/// the reverse order.
pub fn CheckpointTrailerType(comptime Storage: type) type {
    const Grid = GridType(Storage);

    return struct {
        const CheckpointTrailer = @This();

        // Body of the block which holds encoded trailer data.
        // All chunks except for possibly the last one are full.
        const chunk_size_max = constants.block_size - @sizeOf(vsr.Header);

        // Chunk describes a slice of encoded trailer that goes into nth block on disk.
        const Chunk = struct {
            fn size(options: struct {
                block_index: u32,
                block_count: u32,
                trailer_size: u64,
            }) u32 {
                assert(options.block_count > 0);
                assert(options.block_count == stdx.div_ceil(options.trailer_size, chunk_size_max));
                assert(options.block_index < options.block_count);

                const last_block = options.block_index == options.block_count - 1;
                const chunk_size: u32 = if (last_block)
                    @intCast(options.trailer_size - (options.block_count - 1) * chunk_size_max)
                else
                    chunk_size_max;

                return chunk_size;
            }
        };

        // Reference to the grid is late-initialized in `open`, because the free set is part of
        // the grid, which doesn't have access to a stable grid pointer. It is set to null by
        // `reset`, to verify that the free set is not used before it is opened during sync.
        grid: ?*Grid = null,
        trailer_type: TrailerType,

        next_tick: Grid.NextTick = undefined,
        read: Grid.Read = undefined,
        write: Grid.Write = undefined,

        // TODO(Grid pool): Acquire blocks as-needed from the grid pool. The common-case number of
        // blocks needed is much less than the worst-case number of blocks.
        blocks: []BlockPtr,
        /// `encode_chunks()`/`decode_chunks()` return slices into this memory.
        block_bodies: [][]align(@sizeOf(u256)) u8,

        // SoA representation of block references holding the trailer itself.
        //
        // After the set is read from disk and decoded, these blocks are manually marked as
        // acquired.
        block_addresses: []u64,
        block_checksums: []u128,
        // The current block that is being read or written. It counts from 0 to block_count()
        // during checkpoint, and from block_count() to zero during open.
        block_index: u32 = 0,

        // Size of the encoded set in bytes.
        // (Does not include block headers.)
        size: u64 = 0,
        // The number of trailer bytes read or written during disk IO. Used to cross-check that we
        // haven't lost any bytes along the way.
        size_transferred: u64 = 0,

        // Checksum covering the entire encoded trailer.
        // (Does not include block headers.)
        checksum: u128 = 0,

        callback: union(enum) {
            none,
            open: *const fn (trailer: *CheckpointTrailer) void,
            checkpoint: *const fn (trailer: *CheckpointTrailer) void,
        } = .none,

        pub fn init(
            allocator: mem.Allocator,
            trailer_type: TrailerType,
            buffer_size: usize,
        ) !CheckpointTrailer {
            const block_count_max_ = block_count_max(buffer_size);
            const blocks = try allocator.alloc(BlockPtr, block_count_max_);
            errdefer allocator.free(blocks);

            for (blocks, 0..) |*block, i| {
                errdefer for (blocks[0..i]) |b| allocator.free(b);
                block.* = try allocate_block(allocator);
            }
            errdefer for (blocks) |block| allocator.free(block);

            const block_bodies = try allocator.alloc([]align(@sizeOf(u256)) u8, block_count_max_);
            errdefer allocator.free(block_bodies);
            @memset(block_bodies, undefined);

            const block_addresses = try allocator.alloc(u64, block_count_max_);
            errdefer allocator.free(block_addresses);

            const block_checksums = try allocator.alloc(u128, block_count_max_);
            errdefer allocator.free(block_checksums);

            return .{
                .trailer_type = trailer_type,
                .blocks = blocks,
                .block_bodies = block_bodies,
                .block_addresses = block_addresses,
                .block_checksums = block_checksums,
            };
        }

        pub fn deinit(trailer: *CheckpointTrailer, allocator: mem.Allocator) void {
            allocator.free(trailer.block_checksums);
            allocator.free(trailer.block_addresses);
            allocator.free(trailer.block_bodies);
            for (trailer.blocks) |block| allocator.free(block);
            allocator.free(trailer.blocks);
        }

        pub fn reset(trailer: *CheckpointTrailer) void {
            switch (trailer.callback) {
                .none, .open => {},
                // Checkpointing doesn't need to read blocks, so it's not cancellable.
                .checkpoint => unreachable,
            }
            trailer.* = .{
                .trailer_type = trailer.trailer_type,
                .blocks = trailer.blocks,
                .block_bodies = trailer.block_bodies,
                .block_addresses = trailer.block_addresses,
                .block_checksums = trailer.block_checksums,
            };
        }

        pub fn block_count(trailer: *const CheckpointTrailer) u32 {
            return block_count_max(trailer.size);
        }

        pub fn block_count_max(trailer_size: u64) u32 {
            return @intCast(stdx.div_ceil(trailer_size, chunk_size_max));
        }

        /// Each returned chunk has `chunk.len == chunk_size_max`.
        pub fn encode_chunks(trailer: *CheckpointTrailer) []const []align(@sizeOf(u256)) u8 {
            for (trailer.block_bodies, trailer.blocks) |*block_body, block| {
                block_body.* = block[@sizeOf(vsr.Header)..];

                assert(block_body.*.len == chunk_size_max);
            }
            return trailer.block_bodies;
        }

        pub fn decode_chunks(
            trailer: *const CheckpointTrailer,
        ) []const []align(@sizeOf(u256)) const u8 {
            const chunk_count: u32 = @intCast(stdx.div_ceil(trailer.size, chunk_size_max));
            for (
                trailer.block_bodies[0..chunk_count],
                trailer.blocks[0..chunk_count],
                0..,
            ) |*block_body, block, block_index| {
                const chunk_size = Chunk.size(.{
                    .block_index = @intCast(block_index),
                    .block_count = chunk_count,
                    .trailer_size = trailer.size,
                });

                block_body.* = block[@sizeOf(vsr.Header)..][0..chunk_size];
            }
            return trailer.block_bodies[0..chunk_count];
        }

        // These data are stored in the superblock header.
        pub fn checkpoint_reference(
            trailer: *const CheckpointTrailer,
        ) vsr.SuperBlockTrailerReference {
            assert(trailer.size == trailer.size_transferred);
            assert(trailer.callback == .none);

            const reference: vsr.SuperBlockTrailerReference = if (trailer.size == 0) .{
                .checksum = vsr.checksum(&.{}),
                .last_block_address = 0,
                .last_block_checksum = 0,
                .trailer_size = 0,
            } else .{
                .checksum = trailer.checksum,
                .last_block_address = trailer.block_addresses[trailer.block_count() - 1],
                .last_block_checksum = trailer.block_checksums[trailer.block_count() - 1],
                .trailer_size = trailer.size,
            };
            assert(reference.empty() == (trailer.size == 0));

            return reference;
        }

        pub fn open(
            trailer: *CheckpointTrailer,
            grid: *Grid,
            reference: vsr.SuperBlockTrailerReference,
            callback: *const fn (trailer: *CheckpointTrailer) void,
        ) void {
            assert(trailer.grid == null);
            trailer.grid = grid;

            assert(trailer.callback == .none);
            defer assert(trailer.callback == .open);

            assert(reference.trailer_size % trailer.trailer_type.item_size() == 0);
            assert(trailer.size == 0);
            assert(trailer.size_transferred == 0);
            assert(trailer.block_index == 0);

            trailer.size = reference.trailer_size;
            trailer.checksum = reference.checksum;
            trailer.callback = .{ .open = callback };

            // Start from the last block, as the linked list arranges data in the reverse order.
            trailer.block_index = trailer.block_count();

            if (trailer.size == 0) {
                assert(reference.last_block_address == 0);
                trailer.grid.?.on_next_tick(open_next_tick, &trailer.next_tick);
            } else {
                assert(reference.last_block_address != 0);
                trailer.open_read_next(reference.last_block_address, reference.last_block_checksum);
            }
        }

        fn open_next_tick(next_tick: *Grid.NextTick) void {
            const trailer: *CheckpointTrailer = @alignCast(@fieldParentPtr("next_tick", next_tick));
            assert(trailer.callback == .open);
            assert(trailer.size == 0);
            trailer.open_done();
        }

        fn open_read_next(trailer: *CheckpointTrailer, address: u64, checksum: u128) void {
            assert(trailer.callback == .open);
            assert(trailer.size > 0);
            assert((trailer.size_transferred == 0) ==
                (trailer.block_index == trailer.block_count()));
            assert(address != 0);

            assert(trailer.block_index <= trailer.block_count());
            assert(trailer.block_index > 0);
            trailer.block_index -= 1;

            trailer.block_addresses[trailer.block_index] = address;
            trailer.block_checksums[trailer.block_index] = checksum;
            for (trailer.block_index + 1..trailer.block_count()) |index| {
                assert(trailer.block_addresses[index] != address);
                assert(trailer.block_checksums[index] != checksum);
            }

            trailer.grid.?.read_block(
                .{ .from_local_or_global_storage = open_read_next_callback },
                &trailer.read,
                address,
                checksum,
                .{ .cache_read = true, .cache_write = false },
            );
        }

        fn open_read_next_callback(read: *Grid.Read, block: BlockPtrConst) void {
            const trailer: *CheckpointTrailer = @fieldParentPtr("read", read);
            assert(trailer.callback == .open);
            assert(trailer.size > 0);
            assert(trailer.block_index < trailer.block_count());

            const block_header = schema.header_from_block(block);
            assert(block_header.block_type == trailer.trailer_type.block_type());

            const chunk_size = Chunk.size(.{
                .block_index = trailer.block_index,
                .block_count = trailer.block_count(),
                .trailer_size = trailer.size,
            });

            stdx.copy_disjoint(
                .exact,
                u8,
                trailer.blocks[trailer.block_index][@sizeOf(vsr.Header)..][0..chunk_size],
                schema.TrailerNode.body(block),
            );
            trailer.size_transferred += chunk_size;

            if (schema.TrailerNode.previous(block)) |previous| {
                assert(trailer.block_index > 0);
                trailer.open_read_next(previous.address, previous.checksum);
            } else {
                assert(trailer.block_index == 0);
                trailer.open_done();
            }
        }

        fn open_done(trailer: *CheckpointTrailer) void {
            assert(trailer.callback == .open);
            defer assert(trailer.callback == .none);

            assert(trailer.block_index == 0);
            assert(trailer.size_transferred == trailer.size);

            var checksum_stream = vsr.ChecksumStream.init();
            for (trailer.decode_chunks()) |chunk| checksum_stream.add(chunk);
            assert(trailer.checksum == checksum_stream.checksum());

            const callback = trailer.callback.open;
            trailer.callback = .none;
            callback(trailer);
        }

        pub fn checkpoint(
            trailer: *CheckpointTrailer,
            callback: *const fn (trailer: *CheckpointTrailer) void,
        ) void {
            assert(trailer.callback == .none);
            defer assert(trailer.callback == .checkpoint);

            var checksum_stream = vsr.ChecksumStream.init();
            for (trailer.decode_chunks()) |chunk| checksum_stream.add(chunk);

            trailer.size_transferred = 0;
            trailer.checksum = checksum_stream.checksum();

            if (trailer.size > 0) {
                assert(trailer.grid.?.free_set.count_reservations() == 0);
                const reservation = trailer.grid.?.free_set.reserve(trailer.block_count()).?;
                defer trailer.grid.?.free_set.forfeit(reservation);

                for (
                    trailer.block_addresses[0..trailer.block_count()],
                    trailer.block_checksums[0..trailer.block_count()],
                ) |*address, *checksum| {
                    address.* = trailer.grid.?.free_set.acquire(reservation).?;
                    checksum.* = undefined;
                }
                // Reservation should be fully used up.
                assert(trailer.grid.?.free_set.acquire(reservation) == null);
            }

            trailer.block_index = 0;
            trailer.callback = .{ .checkpoint = callback };
            if (trailer.size == 0) {
                trailer.grid.?.on_next_tick(checkpoint_next_tick, &trailer.next_tick);
            } else {
                trailer.checkpoint_write_next();
            }
        }

        fn checkpoint_next_tick(next_tick: *Grid.NextTick) void {
            const trailer: *CheckpointTrailer = @alignCast(@fieldParentPtr("next_tick", next_tick));
            assert(trailer.callback == .checkpoint);
            assert(trailer.size == 0);
            assert(trailer.block_index == 0);
            trailer.checkpoint_done();
        }

        fn checkpoint_write_next(trailer: *CheckpointTrailer) void {
            assert(trailer.callback == .checkpoint);
            assert(trailer.size > 0);
            assert(trailer.block_index < trailer.block_count());
            assert((trailer.size_transferred == 0) == (trailer.block_index == 0));

            const chunk_size = Chunk.size(.{
                .block_index = trailer.block_index,
                .block_count = trailer.block_count(),
                .trailer_size = trailer.size,
            });

            const block_index = trailer.block_index;
            const block = &trailer.blocks[block_index];
            const metadata: schema.TrailerNode.Metadata = if (block_index == 0) .{
                .previous_trailer_block_checksum = 0,
                .previous_trailer_block_address = 0,
            } else .{
                .previous_trailer_block_checksum = trailer.block_checksums[block_index - 1],
                .previous_trailer_block_address = trailer.block_addresses[block_index - 1],
            };

            const header = mem.bytesAsValue(vsr.Header.Block, block.*[0..@sizeOf(vsr.Header)]);
            header.* = .{
                .cluster = trailer.grid.?.superblock.working.cluster,
                .metadata_bytes = @bitCast(metadata),
                .address = trailer.block_addresses[trailer.block_index],
                .snapshot = 0, // TODO(snapshots): Set this properly; it is useful for debugging.
                .size = @sizeOf(vsr.Header) + chunk_size,
                .command = .block,
                .release = trailer.grid.?.superblock.working.vsr_state.checkpoint.release,
                .block_type = trailer.trailer_type.block_type(),
            };
            trailer.size_transferred += chunk_size;
            header.set_checksum_body(block.*[@sizeOf(vsr.Header)..][0..chunk_size]);
            header.set_checksum();
            schema.TrailerNode.assert_valid_header(block.*);

            trailer.block_checksums[block_index] = header.checksum;
            // create_block swaps out the `blocks` BlockPtr, so our reference to it will be invalid.
            trailer.block_bodies[block_index] = undefined;
            trailer.grid.?.create_block(checkpoint_write_next_callback, &trailer.write, block);
        }

        fn checkpoint_write_next_callback(write: *Grid.Write) void {
            const trailer: *CheckpointTrailer = @fieldParentPtr("write", write);
            assert(trailer.callback == .checkpoint);

            trailer.block_index += 1;
            if (trailer.block_index == trailer.block_count()) {
                trailer.checkpoint_done();
            } else {
                trailer.checkpoint_write_next();
            }
        }

        fn checkpoint_done(trailer: *CheckpointTrailer) void {
            assert(trailer.callback == .checkpoint);
            defer assert(trailer.callback == .none);

            assert(trailer.block_index == trailer.block_count());
            assert(trailer.size_transferred == trailer.size);

            const callback = trailer.callback.checkpoint;
            trailer.callback = .none;
            callback(trailer);
        }
    };
}

pub const TrailerType = enum {
    free_set,
    client_sessions,

    fn block_type(trailer_type: TrailerType) schema.BlockType {
        return switch (trailer_type) {
            .free_set => .free_set,
            .client_sessions => .client_sessions,
        };
    }

    fn item_size(trailer_type: TrailerType) usize {
        return switch (trailer_type) {
            .free_set => @sizeOf(FreeSet.Word),
            .client_sessions => @sizeOf(vsr.Header) + @sizeOf(u64),
        };
    }
};
