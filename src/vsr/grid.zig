const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const mem = std.mem;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const schema = @import("../lsm/schema.zig");

const SuperBlockType = vsr.SuperBlockType;
const FIFOType = @import("../fifo.zig").FIFOType;
const IOPSType = @import("../iops.zig").IOPSType;
const SetAssociativeCacheType = @import("../lsm/set_associative_cache.zig").SetAssociativeCacheType;
const stdx = @import("../stdx.zig");
const GridBlocksMissing = @import("./grid_blocks_missing.zig").GridBlocksMissing;

const FreeSet = @import("./free_set.zig").FreeSet;

const log = stdx.log.scoped(.grid);

pub const BlockPtr = *align(constants.sector_size) [constants.block_size]u8;
pub const BlockPtrConst = *align(constants.sector_size) const [constants.block_size]u8;

// Leave this outside GridType so we can call it from modules that don't know about Storage.
pub fn allocate_block(
    allocator: mem.Allocator,
) error{OutOfMemory}!*align(constants.sector_size) [constants.block_size]u8 {
    const block = try allocator.alignedAlloc(u8, constants.sector_size, constants.block_size);
    @memset(block, 0);
    return block[0..constants.block_size];
}

/// The Grid provides access to on-disk blocks (blobs of `block_size` bytes).
/// Each block is identified by an "address" (`u64`, beginning at 1).
///
/// Recently/frequently-used blocks are transparently cached in memory.
pub fn GridType(comptime Storage: type) type {
    const block_size = constants.block_size;
    const SuperBlock = SuperBlockType(Storage);

    return struct {
        const Grid = @This();
        const CheckpointTrailer = vsr.CheckpointTrailerType(Storage);

        pub const read_iops_max = constants.grid_iops_read_max;
        pub const write_iops_max = constants.grid_iops_write_max;

        pub const RepairTable = GridBlocksMissing.RepairTable;
        pub const RepairTableResult = GridBlocksMissing.RepairTableResult;
        pub const Reservation = @import("./free_set.zig").Reservation;

        // Grid just reuses the Storage's NextTick abstraction for simplicity.
        pub const NextTick = Storage.NextTick;

        pub const Write = struct {
            callback: *const fn (*Grid.Write) void,
            address: u64,
            repair: bool,
            block: *BlockPtr,
            /// The current checkpoint when the write began.
            /// Verifies that the checkpoint does not advance during the (non-repair) write.
            checkpoint_id: u128,

            /// Link for the Grid.write_queue linked list.
            next: ?*Write = null,
        };

        const WriteIOP = struct {
            grid: *Grid,
            completion: Storage.Write,
            write: *Write,
        };

        const ReadBlockCallback = union(enum) {
            /// If the local read fails, report the error.
            from_local_storage: *const fn (*Grid.Read, ReadBlockResult) void,
            /// If the local read fails, this read will be added to a linked list, which Replica can
            /// then interrogate each tick(). The callback passed to this function won't be called
            /// until the block has been recovered.
            from_local_or_global_storage: *const fn (*Grid.Read, BlockPtrConst) void,
        };

        pub const Read = struct {
            callback: ReadBlockCallback,
            address: u64,
            checksum: u128,
            /// The current checkpoint when the read began.
            /// Used to verify that the checkpoint does not advance while the read is in progress.
            checkpoint_id: u128,
            checkpoint_durable: bool,

            /// When coherent=true:
            /// - the block (address+checksum) is part of the current checkpoint.
            /// - the read will complete before the next checkpoint occurs.
            /// - callback == .from_local_or_global_storage
            /// When coherent=false:
            /// - the block (address+checksum) is not necessarily part of the current checkpoint.
            /// - the read may complete after a future checkpoint.
            /// - callback == .from_local_storage
            coherent: bool,
            cache_read: bool,
            cache_write: bool,
            pending: ReadPending = .{},
            resolves: FIFOType(ReadPending) = .{ .name = null },

            grid: *Grid,
            next_tick: Grid.NextTick = undefined,

            /// Link for Grid.read_queue/Grid.read_global_queue linked lists.
            next: ?*Read = null,
        };

        /// Although we distinguish between the reasons why the block is invalid, we only use this
        /// info for logging, not logic.
        pub const ReadBlockResult = union(enum) {
            valid: BlockPtrConst,
            /// Checksum of block header is invalid.
            invalid_checksum,
            /// Checksum of block body is invalid.
            invalid_checksum_body,
            /// The block header is valid, but its `header.command` is not `block`.
            /// (This is possible due to misdirected IO).
            unexpected_command,
            /// The block is valid, but it is not the block we expected.
            unexpected_checksum,
        };

        const ReadPending = struct {
            /// Link for Read.resolves linked lists.
            next: ?*ReadPending = null,
        };

        const ReadIOP = struct {
            completion: Storage.Read,
            read: *Read,
        };

        const cache_interface = struct {
            inline fn address_from_address(address: *const u64) u64 {
                return address.*;
            }

            inline fn hash_address(address: u64) u64 {
                assert(address > 0);
                return stdx.hash_inline(address);
            }
        };

        const set_associative_cache_ways = 16;

        pub const Cache = SetAssociativeCacheType(
            u64,
            u64,
            cache_interface.address_from_address,
            cache_interface.hash_address,
            .{
                .ways = set_associative_cache_ways,
                // layout.cache_line_size isn't actually used to compute anything. Rather, it's
                // used by the SetAssociativeCache to assert() on sub-optimal values. In this case,
                // it's better to allow the user to be able to run with a much smaller grid cache
                // (256MiB vs 1GiB!) than trying to be completely optimal.
                .cache_line_size = 16,
                .value_alignment = @alignOf(u64),
            },
        );

        superblock: *SuperBlock,
        trace: *vsr.trace.Tracer,
        free_set: FreeSet,
        free_set_checkpoint_blocks_acquired: CheckpointTrailer,
        free_set_checkpoint_blocks_released: CheckpointTrailer,

        blocks_missing: GridBlocksMissing,

        cache: Cache,
        /// Each entry in cache has a corresponding block.
        cache_blocks: []BlockPtr,

        write_iops: IOPSType(WriteIOP, write_iops_max) = .{},
        write_queue: FIFOType(Write) = .{ .name = "grid_write" },

        // Each read_iops has a corresponding block.
        read_iop_blocks: [read_iops_max]BlockPtr,
        read_iops: IOPSType(ReadIOP, read_iops_max) = .{},
        read_queue: FIFOType(Read) = .{ .name = "grid_read" },

        // List of Read.pending's which are in `read_queue` but also waiting for a free `read_iops`.
        read_pending_queue: FIFOType(ReadPending) = .{ .name = "grid_read_pending" },
        /// List of `Read`s which are waiting for a block repair from another replica.
        /// (Reads in this queue have already failed locally).
        ///
        /// Invariants:
        /// - For each read, read.callback=from_local_or_global_storage.
        read_global_queue: FIFOType(Read) = .{ .name = "grid_read_global" },
        // True if there's a read that is resolving callbacks.
        // If so, the read cache must not be invalidated.
        read_resolving: bool = false,

        callback: union(enum) {
            none,
            open: *const fn (*Grid) void,
            checkpoint: *const fn (*Grid) void,
            checkpoint_durable: *const fn (*Grid) void,
            cancel: *const fn (*Grid) void,
        } = .none,

        canceling_tick_context: NextTick = undefined,

        pub fn init(allocator: mem.Allocator, options: struct {
            superblock: *SuperBlock,
            trace: *vsr.trace.Tracer,
            cache_blocks_count: u64 = Cache.value_count_max_multiple,
            missing_blocks_max: usize,
            missing_tables_max: usize,
            blocks_released_prior_checkpoint_durability_max: usize,
        }) !Grid {
            const blocks_count = vsr.block_count_max(options.superblock.storage_size_limit);
            var free_set = try FreeSet.init(allocator, .{
                .blocks_count = blocks_count,
                .blocks_released_prior_checkpoint_durability_max = options
                    .blocks_released_prior_checkpoint_durability_max,
            });
            errdefer free_set.deinit(allocator);

            const free_set_encoded_size_max = FreeSet.encode_size_max(blocks_count);
            var free_set_checkpoint_blocks_acquired =
                try CheckpointTrailer.init(allocator, .free_set, free_set_encoded_size_max);
            errdefer free_set_checkpoint_blocks_acquired.deinit(allocator);

            var free_set_checkpoint_blocks_released =
                try CheckpointTrailer.init(allocator, .free_set, free_set_encoded_size_max);
            errdefer free_set_checkpoint_blocks_released.deinit(allocator);

            var blocks_missing = try GridBlocksMissing.init(allocator, .{
                .blocks_max = options.missing_blocks_max,
                .tables_max = options.missing_tables_max,
            });
            errdefer blocks_missing.deinit(allocator);

            const cache_blocks = try allocator.alloc(BlockPtr, options.cache_blocks_count);
            errdefer allocator.free(cache_blocks);

            for (cache_blocks, 0..) |*cache_block, i| {
                errdefer for (cache_blocks[0..i]) |block| allocator.free(block);
                cache_block.* = try allocate_block(allocator);
            }
            errdefer for (cache_blocks) |block| allocator.free(block);

            var cache = try Cache.init(allocator, options.cache_blocks_count, .{ .name = "grid" });
            errdefer cache.deinit(allocator);

            var read_iop_blocks: [read_iops_max]BlockPtr = undefined;

            for (&read_iop_blocks, 0..) |*read_iop_block, i| {
                errdefer for (read_iop_blocks[0..i]) |block| allocator.free(block);
                read_iop_block.* = try allocate_block(allocator);
            }
            errdefer for (&read_iop_blocks) |block| allocator.free(block);

            return Grid{
                .superblock = options.superblock,
                .trace = options.trace,
                .free_set = free_set,
                .free_set_checkpoint_blocks_acquired = free_set_checkpoint_blocks_acquired,
                .free_set_checkpoint_blocks_released = free_set_checkpoint_blocks_released,
                .blocks_missing = blocks_missing,
                .cache = cache,
                .cache_blocks = cache_blocks,
                .read_iop_blocks = read_iop_blocks,
            };
        }

        pub fn deinit(grid: *Grid, allocator: mem.Allocator) void {
            for (&grid.read_iop_blocks) |block| allocator.free(block);

            for (grid.cache_blocks) |block| allocator.free(block);
            allocator.free(grid.cache_blocks);

            grid.cache.deinit(allocator);
            grid.blocks_missing.deinit(allocator);

            grid.free_set_checkpoint_blocks_acquired.deinit(allocator);
            grid.free_set_checkpoint_blocks_released.deinit(allocator);

            grid.free_set.deinit(allocator);

            grid.* = undefined;
        }

        pub fn open(grid: *Grid, callback: *const fn (*Grid) void) void {
            assert(grid.callback == .none);

            grid.callback = .{ .open = callback };
            grid.free_set_checkpoint_blocks_acquired.open(
                grid,
                grid.superblock.working.free_set_reference(.blocks_acquired),
                open_free_set_callback_blocks_acquired,
            );
            grid.free_set_checkpoint_blocks_released.open(
                grid,
                grid.superblock.working.free_set_reference(.blocks_released),
                open_free_set_callback_blocks_released,
            );
        }

        fn open_free_set_callback_blocks_acquired(trailer: *CheckpointTrailer) void {
            assert(trailer.callback == .none);
            const grid: *Grid = @fieldParentPtr("free_set_checkpoint_blocks_acquired", trailer);
            grid.open_free_set_callback();
        }

        fn open_free_set_callback_blocks_released(trailer: *CheckpointTrailer) void {
            assert(trailer.callback == .none);
            const grid: *Grid = @fieldParentPtr("free_set_checkpoint_blocks_released", trailer);
            grid.open_free_set_callback();
        }

        fn open_free_set_callback(grid: *Grid) void {
            assert(grid.free_set_checkpoint_blocks_acquired.callback == .none or
                grid.free_set_checkpoint_blocks_released.callback == .none);

            const callback = grid.callback.open;
            // May still be reading the CheckpointTrailer for `blocks_acquired`.
            if (grid.free_set_checkpoint_blocks_acquired.callback == .open) return;
            assert(grid.free_set_checkpoint_blocks_acquired.callback == .none);

            // May still be reading the CheckpointTrailer for `blocks_released`.
            if (grid.free_set_checkpoint_blocks_released.callback == .open) return;
            assert(grid.free_set_checkpoint_blocks_released.callback == .none);

            {
                assert(!grid.free_set.opened);
                defer assert(grid.free_set.opened);

                const block_count_encoded_blocks_acquired =
                    grid.free_set_checkpoint_blocks_acquired.block_count();
                const block_count_encoded_blocks_released =
                    grid.free_set_checkpoint_blocks_released.block_count();
                grid.free_set.open(.{
                    .encoded = .{
                        .blocks_acquired = grid.free_set_checkpoint_blocks_acquired.decode_chunks(),
                        .blocks_released = grid.free_set_checkpoint_blocks_released.decode_chunks(),
                    },
                    .free_set_block_addresses = .{
                        .blocks_acquired = grid.free_set_checkpoint_blocks_acquired
                            .block_addresses[0..block_count_encoded_blocks_acquired],
                        .blocks_released = grid.free_set_checkpoint_blocks_released
                            .block_addresses[0..block_count_encoded_blocks_released],
                    },
                });
                assert((grid.free_set.count_acquired() > 0) ==
                    (grid.free_set_checkpoint_blocks_acquired.size > 0));
                assert(grid.free_set.count_released() >=
                    (grid.free_set_checkpoint_blocks_acquired.block_count() +
                    grid.free_set_checkpoint_blocks_released.block_count()));

                assert(grid.free_set.count_reservations() == 0);
            }
            grid.callback = .none;
            callback(grid);
        }

        /// Checkpoint process is delicate:
        ///   1. Encode free set.
        ///   2. Derive the number of blocks required to store the encoding.
        ///   3. Allocate free set blocks for the encoding (in the old checkpoint).
        ///   4. Write the free set blocks to disk.
        ///   5. Mark the free set's own blocks as released (but not yet free).
        ///
        /// This function handles step 1, and calls CheckpointTrailer.checkpoint, which handles
        /// 2-4.
        /// The caller is responsible for calling Grid.mark_checkpoint_not_durable, which handles 5.
        pub fn checkpoint(grid: *Grid, callback: *const fn (*Grid) void) void {
            assert(grid.callback == .none);
            assert(grid.read_global_queue.empty());

            {
                assert(grid.free_set.count_reservations() == 0);

                const free_set_encoded = grid.free_set.encode_chunks(
                    grid.free_set_checkpoint_blocks_acquired.encode_chunks(),
                    grid.free_set_checkpoint_blocks_released.encode_chunks(),
                );

                grid.free_set_checkpoint_blocks_acquired.size =
                    free_set_encoded.encoded_size_blocks_acquired;
                grid.free_set_checkpoint_blocks_released.size =
                    free_set_encoded.encoded_size_blocks_released;

                assert(grid.free_set_checkpoint_blocks_acquired.size % @sizeOf(FreeSet.Word) == 0);
                assert(grid.free_set_checkpoint_blocks_released.size % @sizeOf(FreeSet.Word) == 0);
            }

            grid.callback = .{ .checkpoint = callback };
            grid.free_set_checkpoint_blocks_acquired
                .checkpoint(checkpoint_free_set_blocks_acquired_callback);
            grid.free_set_checkpoint_blocks_released
                .checkpoint(checkpoint_free_set_blocks_released_callback);
        }

        fn checkpoint_free_set_blocks_acquired_callback(trailer: *CheckpointTrailer) void {
            assert(trailer.callback == .none);
            const grid: *Grid = @fieldParentPtr("free_set_checkpoint_blocks_acquired", trailer);
            assert(grid.callback == .checkpoint);

            grid.checkpoint_join();
        }

        fn checkpoint_free_set_blocks_released_callback(trailer: *CheckpointTrailer) void {
            assert(trailer.callback == .none);
            const grid: *Grid = @fieldParentPtr("free_set_checkpoint_blocks_released", trailer);
            assert(grid.callback == .checkpoint);

            grid.checkpoint_join();
        }

        fn checkpoint_join(grid: *Grid) void {
            assert(grid.callback == .checkpoint);
            assert(grid.read_global_queue.empty());

            if (grid.free_set_checkpoint_blocks_acquired.callback == .checkpoint) {
                return; // Still writing free set `blocks_acquired` bitset.
            }
            assert(grid.free_set_checkpoint_blocks_acquired.callback == .none);

            if (grid.free_set_checkpoint_blocks_released.callback == .checkpoint) {
                return; // Still writing free set `blocks_released` bitset.
            }
            assert(grid.free_set_checkpoint_blocks_released.callback == .none);

            const callback = grid.callback.checkpoint;
            grid.callback = .none;
            callback(grid);
        }

        /// Mark the current checkpoint as not durable, then release the blocks acquired for the
        /// FreeSet checkpoints (to be freed when the *next* checkpoint becomes durable).
        ///
        /// The ordering is important here, if we were to release these blocks before the checkpoint
        /// is marked as not durable, they would erroneously be freed when the *current* checkpoint
        /// becomes durable.
        pub fn mark_checkpoint_not_durable(grid: *Grid) void {
            assert(grid.free_set.checkpoint_durable);
            defer assert(!grid.free_set.checkpoint_durable);

            grid.free_set.mark_checkpoint_not_durable();
            grid.release(grid.free_set_checkpoint_blocks_acquired
                .block_addresses[0..grid.free_set_checkpoint_blocks_acquired.block_count()]);
            grid.release(grid.free_set_checkpoint_blocks_released
                .block_addresses[0..grid.free_set_checkpoint_blocks_released.block_count()]);
        }

        /// Now that the checkpoint is durable on a commit quorum of replicas:
        /// 1. Await all pending repair-writes to blocks that are about to be freed.
        /// 2. Mark currently released blocks as free and eligible for acquisition.
        ///
        /// This function handles step 1.
        /// The caller is responsible for calling FreeSet.checkpoint which handles 2.
        pub fn checkpoint_durable(grid: *Grid, callback: *const fn (*Grid) void) void {
            assert(!grid.free_set.checkpoint_durable);
            grid.callback = .{ .checkpoint_durable = callback };

            grid.blocks_missing.checkpoint_durable_commence(&grid.free_set);
            if (grid.blocks_missing.checkpoint_durable.?.aborting == 0) {
                grid.checkpoint_durable_join();
            }
        }

        fn checkpoint_durable_join(grid: *Grid) void {
            assert(grid.callback == .checkpoint_durable);

            // We are still repairing some blocks released during the previous checkpoint interval.
            if (!grid.blocks_missing.checkpoint_durable_complete()) {
                assert(grid.write_iops.executing() > 0);
                return;
            }

            var write_queue = grid.write_queue.peek();
            while (write_queue) |write| : (write_queue = write.next) {
                assert(write.repair);
                assert(!grid.free_set.is_free(write.address));
                assert(!grid.free_set.to_be_freed_at_checkpoint_durability(write.address));
            }

            var write_iops = grid.write_iops.iterate();
            while (write_iops.next()) |iop| {
                assert(!grid.free_set.is_free(iop.write.address));
                assert(!grid.free_set.to_be_freed_at_checkpoint_durability(iop.write.address));
            }

            // Now that there are no writes to released blocks, we can safely mark them as free,
            // and also mark the checkpoint as durable.
            assert(!grid.free_set.checkpoint_durable);
            defer assert(grid.free_set.checkpoint_durable);
            grid.free_set.mark_checkpoint_durable();

            const callback = grid.callback.checkpoint_durable;
            grid.callback = .none;
            callback(grid);
        }

        pub fn cancel(grid: *Grid, callback: *const fn (*Grid) void) void {
            // grid.open() is cancellable the same way that read_block()/write_block() are.
            switch (grid.callback) {
                .none => {},
                .open => {},
                .checkpoint_durable => {},
                .checkpoint => unreachable,
                .cancel => unreachable,
            }

            grid.callback = .{ .cancel = callback };

            grid.blocks_missing.cancel();
            grid.read_queue.reset();
            grid.read_pending_queue.reset();
            grid.read_global_queue.reset();
            grid.write_queue.reset();
            grid.superblock.storage.reset_next_tick_lsm();
            grid.superblock.storage.on_next_tick(
                .vsr,
                cancel_tick_callback,
                &grid.canceling_tick_context,
            );
        }

        fn cancel_tick_callback(next_tick: *NextTick) void {
            const grid: *Grid = @alignCast(@fieldParentPtr("canceling_tick_context", next_tick));
            if (grid.callback != .cancel) return;

            assert(grid.read_queue.empty());
            assert(grid.read_pending_queue.empty());
            assert(grid.read_global_queue.empty());
            assert(grid.write_queue.empty());

            grid.cancel_join_callback();
        }

        fn cancel_join_callback(grid: *Grid) void {
            assert(grid.callback == .cancel);
            assert(grid.read_queue.empty());
            assert(grid.read_pending_queue.empty());
            assert(grid.read_global_queue.empty());
            assert(grid.write_queue.empty());

            if (grid.read_iops.executing() == 0 and
                grid.write_iops.executing() == 0)
            {
                const callback = grid.callback.cancel;
                grid.callback = .none;

                callback(grid);
            }
        }

        pub fn on_next_tick(
            grid: *Grid,
            callback: *const fn (*Grid.NextTick) void,
            next_tick: *Grid.NextTick,
        ) void {
            assert(grid.callback != .cancel);
            grid.superblock.storage.on_next_tick(.lsm, callback, next_tick);
        }

        /// Aborts if there are not enough free blocks to fill the reservation.
        /// Should a use case arise where a null return would be preferred, this can be split
        /// into panicking and non-panicking versions.
        pub fn reserve(grid: *Grid, blocks_count: usize) Reservation {
            assert(grid.callback == .none);
            return grid.free_set.reserve(blocks_count) orelse vsr.fatal(
                .storage_size_would_exceed_limit,
                "data file would become too large size={} + reservation={} > limit={}, " ++
                    "restart the replica increasing '--limit-storage'",
                .{
                    grid.superblock.working.vsr_state.checkpoint.storage_size,
                    blocks_count * constants.block_size,
                    grid.superblock.storage_size_limit,
                },
            );
        }

        /// Forfeit a reservation.
        pub fn forfeit(grid: *Grid, reservation: Reservation) void {
            assert(grid.callback == .none);
            return grid.free_set.forfeit(reservation);
        }

        /// Returns a just-allocated block.
        /// The caller is responsible for not acquiring more blocks than they reserved.
        pub fn acquire(grid: *Grid, reservation: Reservation) u64 {
            assert(grid.callback == .none);
            return grid.free_set.acquire(reservation).?;
        }

        /// This function should be used to release addresses, instead of release()
        /// on the free set directly, as this also demotes the address within the block cache.
        /// This reduces conflict misses in the block cache, by freeing ways soon after they are
        /// released.
        ///
        /// This does not remove the blocks from the cache — the blocks can be read until the next
        /// checkpoint.
        ///
        /// Asserts that the addresses are not currently being read from or written to.
        pub fn release(grid: *Grid, addresses: []const u64) void {
            assert(grid.callback == .none);
            for (addresses) |address| {
                assert(address > 0);

                // It's safe to release an address that is being read from, because the superblock
                // will not allow it to be overwritten before the end of the bar.
                assert(grid.writing(address, null) != .create);

                grid.cache.demote(address);
                grid.free_set.release(address);
            }
        }

        const Writing = enum { create, repair, not_writing };

        /// If the address is being written to by a non-repair, return `.create`.
        /// If the address is being written to by a repair, return `.repair`.
        /// Otherwise return `.not_writing`.
        ///
        /// Assert that the block pointer is not being used for any write if non-null.
        pub fn writing(grid: *Grid, address: u64, block: ?BlockPtrConst) Writing {
            assert(address > 0);

            var result = Writing.not_writing;
            {
                var it = grid.write_queue.peek();
                while (it) |queued_write| : (it = queued_write.next) {
                    assert(block != queued_write.block.*);
                    if (address == queued_write.address) {
                        assert(result == .not_writing);
                        result = if (queued_write.repair) .repair else .create;
                    }
                }
            }
            {
                var it = grid.write_iops.iterate();
                while (it.next()) |iop| {
                    assert(block != iop.write.block.*);
                    if (address == iop.write.address) {
                        assert(result == .not_writing);
                        result = if (iop.write.repair) .repair else .create;
                    }
                }
            }
            return result;
        }

        /// Assert that the address is not currently being read from (disregarding repairs).
        /// Assert that the block pointer is not being used for any read if non-null.
        fn assert_not_reading(grid: *Grid, address: u64, block: ?BlockPtrConst) void {
            assert(address > 0);

            for ([_]*const FIFOType(Read){
                &grid.read_queue,
                &grid.read_global_queue,
            }) |queue| {
                var it = queue.peek();
                while (it) |queued_read| : (it = queued_read.next) {
                    if (queued_read.coherent) {
                        assert(address != queued_read.address);
                    }
                }
            }
            {
                var it = grid.read_iops.iterate();
                while (it.next()) |iop| {
                    if (iop.read.coherent) {
                        assert(address != iop.read.address);
                    }
                    const iop_block = grid.read_iop_blocks[grid.read_iops.index(iop)];
                    assert(block != iop_block);
                }
            }
        }

        pub fn assert_only_repairing(grid: *Grid) void {
            assert(grid.callback != .cancel);
            assert(grid.read_global_queue.empty());

            var read_queue = grid.read_queue.peek();
            while (read_queue) |read| : (read_queue = read.next) {
                // Scrubber reads are independent from LSM operations.
                assert(!read.coherent);
            }

            var write_queue = grid.write_queue.peek();
            while (write_queue) |write| : (write_queue = write.next) {
                assert(write.repair);
                assert(!grid.free_set.is_free(write.address));
            }

            var write_iops = grid.write_iops.iterate();
            while (write_iops.next()) |iop| {
                assert(iop.write.repair);
                assert(!grid.free_set.is_free(iop.write.address));
            }
        }

        pub fn fulfill_block(grid: *Grid, block: BlockPtrConst) bool {
            assert(grid.callback != .cancel);

            const block_header = schema.header_from_block(block);
            assert(block_header.cluster == grid.superblock.working.cluster);
            assert(block_header.release.value <=
                grid.superblock.working.vsr_state.checkpoint.release.value);

            var reads_iterator = grid.read_global_queue.peek();
            while (reads_iterator) |read| : (reads_iterator = read.next) {
                if (read.checksum == block_header.checksum and
                    read.address == block_header.address)
                {
                    grid.read_global_queue.remove(read);
                    grid.read_block_resolve(read, .{ .valid = block });
                    return true;
                }
            }
            return false;
        }

        pub fn repair_block_waiting(grid: *Grid, address: u64, checksum: u128) bool {
            assert(grid.superblock.opened);
            assert(grid.callback != .cancel);
            return grid.blocks_missing.repair_waiting(address, checksum);
        }

        /// Write a block that should already exist but (maybe) doesn't because of:
        /// - a disk fault, or
        /// - the block was missed due to state sync.
        ///
        /// NOTE: This will consume `block` and replace it with a fresh block.
        pub fn repair_block(
            grid: *Grid,
            callback: *const fn (*Grid.Write) void,
            write: *Grid.Write,
            block: *BlockPtr,
        ) void {
            const block_header = schema.header_from_block(block.*);
            assert(grid.superblock.opened);
            assert(grid.callback != .cancel);
            assert(grid.writing(block_header.address, block.*) == .not_writing);
            assert(grid.blocks_missing.repair_waiting(block_header.address, block_header.checksum));
            assert(!grid.free_set.is_free(block_header.address));

            grid.blocks_missing.repair_commence(block_header.address, block_header.checksum);
            grid.write_block(callback, write, block, .repair);
        }

        /// Write a block for the first time.
        /// NOTE: This will consume `block` and replace it with a fresh block.
        pub fn create_block(
            grid: *Grid,
            callback: *const fn (*Grid.Write) void,
            write: *Grid.Write,
            block: *BlockPtr,
        ) void {
            const block_header = schema.header_from_block(block.*);
            assert(grid.superblock.opened);
            assert(grid.callback == .none or grid.callback == .checkpoint);
            assert((grid.callback == .checkpoint) == (block_header.block_type == .free_set));
            assert(grid.writing(block_header.address, block.*) == .not_writing);
            assert(!grid.blocks_missing.repair_waiting(
                block_header.address,
                block_header.checksum,
            ));
            assert(!grid.free_set.is_free(block_header.address));
            grid.assert_not_reading(block_header.address, block.*);

            grid.write_block(callback, write, block, .create);
        }

        /// NOTE: This will consume `block` and replace it with a fresh block.
        fn write_block(
            grid: *Grid,
            callback: *const fn (*Grid.Write) void,
            write: *Grid.Write,
            block: *BlockPtr,
            trigger: enum { create, repair },
        ) void {
            const header = schema.header_from_block(block.*);
            assert(header.cluster == grid.superblock.working.cluster);
            assert(header.release.value <=
                grid.superblock.working.vsr_state.checkpoint.release.value);

            assert(grid.superblock.opened);
            assert(grid.callback != .cancel);
            assert(grid.writing(header.address, block.*) == .not_writing);
            assert(!grid.free_set.is_free(header.address));
            grid.assert_coherent(header.address, header.checksum);

            if (constants.verify) {
                for (grid.cache_blocks) |cache_block| {
                    assert(cache_block != block.*);
                }
            }

            // Zero sector padding.
            @memset(block.*[header.size..vsr.sector_ceil(header.size)], 0);

            write.* = .{
                .callback = callback,
                .address = header.address,
                .repair = trigger == .repair,
                .block = block,
                .checkpoint_id = grid.superblock.working.checkpoint_id(),
            };

            const iop = grid.write_iops.acquire() orelse {
                grid.write_queue.push(write);
                return;
            };

            grid.write_block_with(iop, write);
        }

        fn write_block_with(grid: *Grid, iop: *WriteIOP, write: *Write) void {
            assert(!grid.free_set.is_free(write.address));

            grid.trace.start(.{ .grid_write = .{ .iop = grid.write_iops.index(iop) } }, .{});

            iop.* = .{
                .grid = grid,
                .completion = undefined,
                .write = write,
            };

            const write_header = schema.header_from_block(write.block.*);
            assert(write_header.size > @sizeOf(vsr.Header));
            assert(write_header.size <= constants.block_size);
            assert(stdx.zeroed(
                write.block.*[write_header.size..vsr.sector_ceil(write_header.size)],
            ));

            grid.superblock.storage.write_sectors(
                write_block_callback,
                &iop.completion,
                write.block.*[0..vsr.sector_ceil(write_header.size)],
                .grid,
                block_offset(write.address),
            );
        }

        fn write_block_callback(completion: *Storage.Write) void {
            const iop: *WriteIOP = @fieldParentPtr("completion", completion);

            // We must copy these values to the stack as they will be overwritten
            // when we release the iop and potentially start a queued write.
            const grid = iop.grid;
            const completed_write = iop.write;

            // We can only update the cache if the Grid is not resolving callbacks with a cache
            // block.
            assert(!grid.read_resolving);
            assert(!grid.free_set.is_free(completed_write.address));

            if (!completed_write.repair) {
                assert(grid.superblock.working.checkpoint_id() == completed_write.checkpoint_id);
            }

            // Insert the write block into the cache, and give the evicted block to the writer.
            const cache_index = grid.cache.upsert(&completed_write.address).index;
            const cache_block = &grid.cache_blocks[cache_index];
            std.mem.swap(BlockPtr, cache_block, completed_write.block);
            // This block content won't be used again. We could overwrite the entire thing, but that
            // would be more expensive.
            @memset(completed_write.block.*[0..@sizeOf(vsr.Header)], 0);

            const cache_block_header = schema.header_from_block(cache_block.*);
            assert(cache_block_header.address == completed_write.address);
            grid.assert_coherent(completed_write.address, cache_block_header.checksum);

            grid.trace.stop(.{ .grid_write = .{ .iop = grid.write_iops.index(iop) } }, .{});

            if (grid.callback == .cancel) {
                assert(grid.write_queue.empty());

                grid.write_iops.release(iop);
                grid.cancel_join_callback();
                return;
            }

            // Start a queued write if possible *before* calling the completed
            // write's callback. This ensures that if the callback calls
            // Grid.write_block() it doesn't preempt the queue.
            //
            // (Don't pop from the write queue until after the read-repairs are resolved.
            // Otherwise their resolution might complete grid cancellation, but the replica has
            // not released its own write iop (via callback).)
            if (grid.write_queue.pop()) |queued_write| {
                grid.write_block_with(iop, queued_write);
            } else {
                grid.write_iops.release(iop);
            }

            // Precede the write's callback, since the callback takes back ownership of the block.
            if (completed_write.repair) grid.blocks_missing.repair_complete(cache_block.*);
            // This call must come after (logically) releasing the IOP. Otherwise we risk tripping
            // assertions forbidding concurrent writes using the same block/address
            // if the callback calls write_block().
            completed_write.callback(completed_write);

            // We start awaiting pending repairs when the checkpoint becomes durable.
            if (grid.callback == .checkpoint_durable) grid.checkpoint_durable_join();
        }

        /// Fetch the block synchronously from cache, if possible.
        /// The returned block pointer is only valid until the next Grid write.
        pub fn read_block_from_cache(
            grid: *Grid,
            address: u64,
            checksum: u128,
            options: struct { coherent: bool },
        ) ?BlockPtrConst {
            assert(grid.superblock.opened);
            assert(grid.callback != .cancel);
            if (options.coherent) {
                assert(grid.writing(address, null) != .create);
                assert(!grid.free_set.is_free(address));
                grid.assert_coherent(address, checksum);
            }

            assert(address > 0);

            const cache_index = grid.cache.get_index(address) orelse return null;
            const cache_block = grid.cache_blocks[cache_index];

            const header = schema.header_from_block(cache_block);
            assert(header.address == address);
            assert(header.cluster == grid.superblock.working.cluster);
            assert(header.release.value <=
                grid.superblock.working.vsr_state.checkpoint.release.value);

            if (header.checksum == checksum) {
                if (constants.verify and
                    options.coherent and
                    grid.superblock.working.vsr_state.sync_op_max == 0)
                {
                    grid.verify_read(address, cache_block);
                }

                return cache_block;
            } else {
                if (options.coherent) {
                    assert(grid.superblock.working.vsr_state.sync_op_max > 0);
                }

                return null;
            }
        }

        pub fn read_block(
            grid: *Grid,
            callback: ReadBlockCallback,
            read: *Grid.Read,
            address: u64,
            checksum: u128,
            options: struct {
                cache_read: bool,
                cache_write: bool,
            },
        ) void {
            assert(grid.superblock.opened);
            assert(grid.callback != .cancel);
            assert(address > 0);

            switch (callback) {
                .from_local_storage => {
                    maybe(grid.callback == .checkpoint);
                    // We try to read the block even when it is free — if we recently released it,
                    // it might be found on disk anyway.
                    maybe(grid.free_set.is_free(address));
                    maybe(grid.writing(address, null) == .create);
                },
                .from_local_or_global_storage => {
                    assert(grid.callback != .checkpoint);
                    assert(!grid.free_set.is_free(address));
                    assert(grid.writing(address, null) != .create);
                    grid.assert_coherent(address, checksum);
                },
            }

            read.* = .{
                .callback = callback,
                .address = address,
                .checksum = checksum,
                .coherent = callback == .from_local_or_global_storage,
                .checkpoint_durable = grid.free_set.checkpoint_durable,
                .cache_read = options.cache_read,
                .cache_write = options.cache_write,
                .checkpoint_id = grid.superblock.working.checkpoint_id(),
                .grid = grid,
            };

            if (options.cache_read) {
                grid.on_next_tick(read_block_tick_callback, &read.next_tick);
            } else {
                read_block_tick_callback(&read.next_tick);
            }
        }

        fn read_block_tick_callback(next_tick: *Storage.NextTick) void {
            const read: *Grid.Read = @alignCast(@fieldParentPtr("next_tick", next_tick));
            const grid = read.grid;
            assert(grid.superblock.opened);
            assert(grid.callback != .cancel);
            if (read.coherent) {
                assert(!grid.free_set.is_free(read.address));
                assert(grid.writing(read.address, null) != .create);
            }

            assert(read.address > 0);

            // Check if a read is already processing/recovering and merge with it.
            for ([_]*const FIFOType(Read){
                &grid.read_queue,
                &grid.read_global_queue,
            }) |queue| {
                // Don't remote-repair repairs – the block may not belong in our current checkpoint.
                if (read.callback == .from_local_storage) {
                    if (queue == &grid.read_global_queue) continue;
                }

                var it = queue.peek();
                while (it) |queued_read| : (it = queued_read.next) {
                    if (queued_read.address == read.address) {
                        // TODO check all read options match
                        if (queued_read.checksum == read.checksum) {
                            queued_read.resolves.push(&read.pending);
                            return;
                        } else {
                            assert(!queued_read.coherent or !read.coherent);
                        }
                    }
                }
            }

            // When Read.cache_read is set, the caller of read_block() is responsible for calling
            // us via next_tick().
            if (read.cache_read) {
                if (grid.read_block_from_cache(
                    read.address,
                    read.checksum,
                    .{ .coherent = read.coherent },
                )) |cache_block| {
                    grid.read_block_resolve(read, .{ .valid = cache_block });
                    return;
                }
            }

            // Become the "root" read that's fetching the block for the given address. The fetch
            // happens asynchronously to avoid stack-overflow and nested cache invalidation.
            grid.read_queue.push(read);

            // Grab an IOP to resolve the block from storage.
            // Failure to do so means the read is queued to receive an IOP when one finishes.
            const iop = grid.read_iops.acquire() orelse {
                grid.read_pending_queue.push(&read.pending);
                return;
            };

            grid.read_block_with(iop, read);
        }

        fn read_block_with(grid: *Grid, iop: *Grid.ReadIOP, read: *Grid.Read) void {
            const address = read.address;
            assert(address > 0);

            // We can only update the cache if the Grid is not resolving callbacks with a cache
            // block.
            assert(!grid.read_resolving);

            grid.trace.start(.{ .grid_read = .{ .iop = grid.read_iops.index(iop) } }, .{});

            iop.* = .{
                .completion = undefined,
                .read = read,
            };
            const iop_block = grid.read_iop_blocks[grid.read_iops.index(iop)];

            grid.superblock.storage.read_sectors(
                read_block_callback,
                &iop.completion,
                iop_block,
                .grid,
                block_offset(address),
            );
        }

        fn read_block_callback(completion: *Storage.Read) void {
            const iop: *ReadIOP = @fieldParentPtr("completion", completion);
            const read = iop.read;
            const grid = read.grid;
            const iop_block = &grid.read_iop_blocks[grid.read_iops.index(iop)];

            grid.trace.stop(.{ .grid_read = .{ .iop = grid.read_iops.index(iop) } }, .{});

            if (grid.callback == .cancel) {
                grid.read_iops.release(iop);
                grid.cancel_join_callback();
                return;
            }

            // Insert the block into the cache, and give the evicted block to `iop`.
            const cache_index =
                if (read.cache_write) grid.cache.upsert(&read.address).index else null;
            const block = block: {
                if (read.cache_write) {
                    const cache_block = &grid.cache_blocks[cache_index.?];
                    std.mem.swap(BlockPtr, iop_block, cache_block);
                    // This block content won't be used again. We could overwrite the entire thing,
                    // but that would be more expensive.
                    @memset(iop_block.*[0..@sizeOf(vsr.Header)], 0);
                    break :block cache_block;
                } else {
                    break :block iop_block;
                }
            };

            // Handoff the iop to a pending read or release it before resolving the callbacks below.
            if (grid.read_pending_queue.pop()) |pending| {
                const queued_read: *Read = @alignCast(@fieldParentPtr("pending", pending));
                grid.read_block_with(iop, queued_read);
            } else {
                grid.read_iops.release(iop);
            }

            // Remove the "root" read so that the address is no longer actively reading / locked.
            grid.read_queue.remove(read);

            const result = read_block_validate(block.*, .{
                .address = read.address,
                .checksum = read.checksum,
            });

            if (result != .valid) {
                const header =
                    mem.bytesAsValue(vsr.Header.Block, block.*[0..@sizeOf(vsr.Header)]);
                log.warn(
                    "{}: {s}: expected address={} checksum={}, found address={} checksum={}",
                    .{
                        grid.superblock.replica_index.?,
                        @tagName(result),
                        read.address,
                        read.checksum,
                        header.address,
                        header.checksum,
                    },
                );

                if (read.cache_write) {
                    // Don't cache a corrupt or incorrect block.
                    const removed = grid.cache.remove(read.address);
                    assert(removed != null);
                }
            }

            grid.read_block_resolve(read, result);
        }

        fn read_block_validate(block: BlockPtrConst, expect: struct {
            address: u64,
            checksum: u128,
        }) ReadBlockResult {
            const header = mem.bytesAsValue(vsr.Header.Block, block[0..@sizeOf(vsr.Header)]);

            if (!header.valid_checksum()) return .invalid_checksum;
            if (header.command != .block) return .unexpected_command;

            assert(header.size >= @sizeOf(vsr.Header));
            assert(header.size <= constants.block_size);

            const block_body = block[@sizeOf(vsr.Header)..header.size];
            if (!header.valid_checksum_body(block_body)) {
                return .invalid_checksum_body;
            }

            if (header.checksum != expect.checksum) return .unexpected_checksum;

            if (constants.verify) {
                assert(stdx.zeroed(block[header.size..vsr.sector_ceil(header.size)]));
            }

            assert(header.address == expect.address);
            return .{ .valid = block };
        }

        fn read_block_resolve(grid: *Grid, read: *Grid.Read, result: ReadBlockResult) void {
            assert(grid.callback != .cancel);

            // Guard to make sure the cache cannot be updated by any read.callbacks() below.
            assert(!grid.read_resolving);
            grid.read_resolving = true;
            defer {
                assert(grid.read_resolving);
                grid.read_resolving = false;
            }

            if (read.coherent) {
                assert(!grid.free_set.is_free(read.address));
                assert(read.checkpoint_id == grid.superblock.working.checkpoint_id());
                grid.assert_coherent(read.address, read.checksum);
            }

            if (result == .valid) {
                const header = schema.header_from_block(result.valid);
                assert(header.cluster == grid.superblock.working.cluster);
                assert(header.release.value <=
                    grid.superblock.working.vsr_state.checkpoint.release.value);
                assert(header.address == read.address);
                assert(header.checksum == read.checksum);
            }

            var read_remote_resolves: FIFOType(ReadPending) = .{ .name = read.resolves.name };

            // Resolve all reads queued to the address with the block.
            while (read.resolves.pop()) |pending| {
                const pending_read: *Read = @alignCast(@fieldParentPtr("pending", pending));
                assert(pending_read.address == read.address);
                assert(pending_read.checksum == read.checksum);
                if (pending_read.coherent) {
                    assert(pending_read.checkpoint_id == grid.superblock.working.checkpoint_id());
                }

                switch (pending_read.callback) {
                    .from_local_storage => |callback| callback(pending_read, result),
                    .from_local_or_global_storage => |callback| {
                        if (result == .valid) {
                            callback(pending_read, result.valid);
                        } else {
                            read_remote_resolves.push(&pending_read.pending);
                        }
                    },
                }
            }

            // Then invoke the callback with the cache block (which should be valid for the duration
            // of the callback as any nested Grid calls cannot synchronously update the cache).
            switch (read.callback) {
                .from_local_storage => |callback| callback(read, result),
                .from_local_or_global_storage => |callback| {
                    if (result == .valid) {
                        callback(read, result.valid);
                    } else {
                        read_remote_resolves.push(&read.pending);
                    }
                },
            }

            // On the result of an invalid block, move the "root" read (and all others it
            // resolves) to recovery queue. Future reads on the same address will see the "root"
            // read in the recovery queue and enqueue to it.
            if (read_remote_resolves.pop()) |read_remote_head_pending| {
                const read_remote_head: *Read = @alignCast(
                    @fieldParentPtr("pending", read_remote_head_pending),
                );
                assert(read_remote_head.callback == .from_local_or_global_storage);
                assert(read_remote_head.coherent);

                log.debug("{}: read_block: fault: address={} checksum={}", .{
                    grid.superblock.replica_index.?,
                    read_remote_head.address,
                    read_remote_head.checksum,
                });

                read_remote_head.resolves = read_remote_resolves;
                grid.read_global_queue.push(read_remote_head);

                if (grid.blocks_missing.enqueue_blocks_available() > 0) {
                    grid.blocks_missing.enqueue_block(
                        read_remote_head.address,
                        read_remote_head.checksum,
                    );
                }
            }
        }

        pub fn next_batch_of_block_requests(grid: *Grid, requests: []vsr.BlockRequest) usize {
            assert(grid.callback != .cancel);
            assert(requests.len > 0);
            assert(requests.len == constants.grid_repair_reads_max);

            // Prioritize requests for blocks with stalled Grid reads, so that commit/compaction can
            // continue. We divide the buffer up between `read_global_queue` and
            // `blocks_missing.faulty_blocks` so that we always request blocks from both queues.
            // Surplus from `blocks_missing.faulty_blocks` may be used by `read_global_queue`.
            const request_faults_count_max = requests.len -
                @min(@divFloor(requests.len, 2), grid.blocks_missing.faulty_blocks.count());
            assert(request_faults_count_max > 0);
            assert(request_faults_count_max <= requests.len);
            assert(request_faults_count_max >= @divFloor(requests.len, 2));

            const request_faults_count = @min(
                grid.read_global_queue.count,
                request_faults_count_max,
            );
            // (Note that many – but not all – of these blocks are also in the GridBlocksMissing.
            // The `read_global_queue` is a FIFO, whereas the GridBlocksMissing has a fixed
            // capacity.)
            for (requests[0..request_faults_count]) |*request| {
                // Pop-push the FIFO to cycle the faulty queue so that successive requests
                // rotate through all stalled blocks (approximately) evenly.
                const read_fault = grid.read_global_queue.pop().?;
                grid.read_global_queue.push(read_fault);

                request.* = .{
                    .block_address = read_fault.address,
                    .block_checksum = read_fault.checksum,
                };
            }

            if (request_faults_count == requests.len) {
                return request_faults_count;
            } else {
                const request_repairs_count = grid.blocks_missing.next_batch_of_block_requests(
                    requests[request_faults_count..],
                );
                return request_faults_count + request_repairs_count;
            }
        }

        pub fn free_set_checkpoints_blocks_max(storage_size_limit: u64) usize {
            const block_count = vsr.block_count_max(storage_size_limit);
            const free_set_encoded_size = FreeSet.encode_size_max(block_count);
            return 2 * CheckpointTrailer.block_count_for_trailer_size(free_set_encoded_size);
        }

        fn block_offset(address: u64) u64 {
            assert(address > 0);

            return (address - 1) * block_size;
        }

        fn assert_coherent(grid: *const Grid, address: u64, checksum: u128) void {
            assert(!grid.free_set.is_free(address));

            const TestStorage = @import("../testing/storage.zig").Storage;
            if (Storage != TestStorage) return;

            if (grid.superblock.storage.options.grid_checker) |checker| {
                checker.assert_coherent(
                    grid.superblock.working.checkpoint_id(),
                    grid.free_set.checkpoint_durable,
                    address,
                    checksum,
                );

                checker.assert_coherent(
                    grid.superblock.staging.checkpoint_id(),
                    checkpoint_durable: {
                        if (grid.superblock.working.checkpoint_id() ==
                            grid.superblock.staging.checkpoint_id())
                        {
                            break :checkpoint_durable grid.free_set.checkpoint_durable;
                        } else {
                            // Checkpoint is currently being written to the superblock. Pass
                            // checkpoint_durable=False as we update free_set.checkpoint_durable
                            // only *after* the checkpoint is written to the superblock.
                            assert(grid.superblock.staging.parent_checkpoint_id() ==
                                grid.superblock.working.checkpoint_id());
                            assert(grid.free_set.checkpoint_durable);
                            break :checkpoint_durable false;
                        }
                    },
                    address,
                    checksum,
                );
            }
        }

        fn verify_read(grid: *Grid, address: u64, cached_block: BlockPtrConst) void {
            comptime assert(constants.verify);

            const TestStorage = @import("../testing/storage.zig").Storage;
            if (Storage != TestStorage) return;

            const actual_block = grid.superblock.storage.grid_block(address).?;
            const actual_header = schema.header_from_block(actual_block);
            const cached_header = schema.header_from_block(cached_block);
            assert(cached_header.checksum == actual_header.checksum);

            assert(std.mem.eql(
                u8,
                cached_block[0..cached_header.size],
                actual_block[0..actual_header.size],
            ));
        }
    };
}
