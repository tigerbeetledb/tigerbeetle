//! Scrub grid blocks.
//!
//! A "data scrubber" is a background task that gradually/incrementally reads the disk and validates
//! what it finds. Its purpose is to discover faults proactively – as early as possibly – rather
//! than waiting for them to be discovered by normal database operation (e.g. during compaction).
//!
//! The most common type of disk fault is a latent sector error:
//!
//! - A "latent sector error" is the temporary or permanent inability to access the data of a
//!   particular sector. That is, the disk as a whole continues to function, but a small section of
//!   data is unavailable.
//! - "Latent" refers to: the error is not discoverable until the sector is actually read.
//! - "An Analysis of Latent Sector Errors in Disk Drives" (2007) found that >60% of latent sector
//!   errors were discovered by a scrubber that cycles every 2 weeks.
//!
//! Finding and repairing errors proactively minimizes the risk of cluster data loss due to multiple
//! intersecting faults (analogous to a "double-fault") – a scenario where we fail to read a block,
//! and try to repair the block from another replica, only to discover that the copy of the block on
//! the remote replica's disk is *also* faulty.
//!
//! TODO Accelerate scrubbing rate (at runtime) if faults are detected frequently.
const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const log = std.log.scoped(.grid_scrubber);

const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const constants = @import("../constants.zig");
const schema = @import("../lsm/schema.zig");
const FIFOType = @import("../fifo.zig").FIFOType;
const IOPSType = @import("../iops.zig").IOPSType;

const allocate_block = @import("./grid.zig").allocate_block;
const GridType = @import("./grid.zig").GridType;
const BlockPtr = @import("./grid.zig").BlockPtr;
const ForestTableIteratorType = @import("../lsm/forest_table_iterator.zig").ForestTableIteratorType;
const TestStorage = @import("../testing/storage.zig").Storage;

pub fn GridScrubberType(comptime Forest: type) type {
    return struct {
        const GridScrubber = @This();
        const Grid = GridType(Forest.Storage);
        const WrappingForestTableIterator = WrappingForestTableIteratorType(Forest);
        const SuperBlock = vsr.SuperBlockType(Forest.Storage);
        const ManifestBlockIterator = ManifestBlockIteratorType(Forest.ManifestLog);
        const CheckpointTrailer = vsr.CheckpointTrailerType(Forest.Storage);

        pub const BlockId = struct {
            block_checksum: u128,
            block_address: u64,
            block_type: schema.BlockType,
        };

        const Read = struct {
            scrubber: *GridScrubber,
            read: Grid.Read = undefined,
            block_type: schema.BlockType,

            status: enum {
                /// If `read.done`: The scrub failed – the block must be repaired.
                /// If `!read.done`: The scrub is still in progress. (This is the initial state).
                repair,
                /// The scrub succeeded.
                /// Don't repair the block.
                ok,
                /// The scrub was aborted (the replica is about to state-sync).
                /// Don't repair the block.
                canceled,
                /// The block was freed by a checkpoint in the time that the read was in progress.
                /// Don't repair the block.
                ///
                /// (At checkpoint, the FreeSet frees blocks released during the preceding
                /// checkpoint. We can scrub released blocks, but not free blocks. Setting this flag
                /// ensures that GridScrubber doesn't require a read-barrier at checkpoint.)
                released,
            },
            /// Whether the read is ready to be released.
            done: bool,

            /// "next" belongs to the FIFOs.
            next: ?*Read = null,
        };

        superblock: *SuperBlock,
        forest: *Forest,
        client_sessions_checkpoint: *const CheckpointTrailer,

        reads: IOPSType(Read, constants.grid_scrubber_reads_max) = .{},

        /// A list of reads that are in progress.
        reads_busy: FIFOType(Read) = .{ .name = "grid_scrubber_reads_busy" },
        /// A list of reads that are ready to be released.
        reads_done: FIFOType(Read) = .{ .name = "grid_scrubber_reads_done" },

        /// Track the progress through the grid.
        ///
        /// Every full tour...
        /// - ...on an idle replica (i.e. not committing) scrubs every acquired block in the grid.
        /// - ...on a non-idle replica scrubs all blocks that survived the entire span of the tour
        ///   without moving to a different level, but may not scrub blocks that were added during
        ///   the tour or which moved.
        tour: union(enum) {
            init,
            done,
            table_index,
            table_data: struct {
                index_checksum: u128,
                index_address: u64,
                /// Points to `tour_index_block` once the index block has been read.
                index_block: ?BlockPtr = null,
                data_block_index: u32 = 0,
            },
            /// The manifest log tour iterates manifest blocks in reverse order.
            /// (To ensure that manifest compaction doesn't lead to missed blocks.)
            manifest_log: struct { iterator: ManifestBlockIterator = .init },
            free_set_blocks_acquired: struct { index: u32 = 0 },
            free_set_blocks_released: struct { index: u32 = 0 },
            client_sessions: struct { index: u32 = 0 },
        },

        /// When tour == .init, tour_tables == .{}
        /// When tour == .done, tour_tables.next() == null.
        tour_tables: ?WrappingForestTableIterator,
        /// The "offset" within the LSM from which scrubber table iteration cycles begin/end.
        /// This varies between replicas to minimize risk of data loss.
        tour_tables_origin: ?WrappingForestTableIterator.Origin,

        /// Contains a table index block when tour=table_data.
        tour_index_block: BlockPtr,

        /// These counters reset after every tour cycle.
        tour_blocks_scrubbed_count: u64,

        pub fn init(
            allocator: std.mem.Allocator,
            forest: *Forest,
            client_sessions_checkpoint: *const CheckpointTrailer,
        ) error{OutOfMemory}!GridScrubber {
            const tour_index_block = try allocate_block(allocator);
            errdefer allocator.free(tour_index_block);

            return .{
                .superblock = forest.grid.superblock,
                .forest = forest,
                .client_sessions_checkpoint = client_sessions_checkpoint,
                .tour = .init,
                .tour_tables = null,
                .tour_tables_origin = null,
                .tour_index_block = tour_index_block,
                .tour_blocks_scrubbed_count = 0,
            };
        }

        pub fn deinit(scrubber: *GridScrubber, allocator: std.mem.Allocator) void {
            allocator.free(scrubber.tour_index_block);

            scrubber.* = undefined;
        }

        pub fn open(scrubber: *GridScrubber, random: std.rand.Random) void {
            // Compute the tour origin exactly once.
            if (scrubber.tour_tables_origin != null) {
                return;
            }

            // Each replica's scrub origin is chosen independently.
            // This reduces the chance that the same block across multiple replicas can bitrot
            // without being discovered and repaired by a scrubber.
            //
            // To accomplish this, try to select an origin uniformly across all blocks:
            // - Bias towards levels with more tables.
            // - Bias towards trees with more blocks per table.
            // - (Though, for ease of implementation, the origin is always at the beginning of a
            //   tree's level, never in the middle.)
            assert(scrubber.tour == .init);

            scrubber.tour_tables_origin = .{
                .level = 0,
                .tree_id = Forest.tree_infos[0].tree_id,
            };

            var weights_sum: u64 = 0;
            for (0..constants.lsm_levels) |level| {
                inline for (Forest.tree_infos) |tree_info| {
                    const tree_id = comptime Forest.tree_id_cast(tree_info.tree_id);
                    const tree = scrubber.forest.tree_for_id_const(tree_id);
                    const levels = &tree.manifest.levels;
                    const tree_level_weight = @as(u64, levels[level].tables.len()) *
                        tree_info.Tree.Table.index.data_block_count_max;
                    if (tree_level_weight > 0) {
                        weights_sum += tree_level_weight;
                        if (random.uintLessThan(u64, weights_sum) < tree_level_weight) {
                            scrubber.tour_tables_origin = .{
                                .level = @intCast(level),
                                .tree_id = tree_info.tree_id,
                            };
                        }
                    }
                }
            }

            scrubber.tour_tables = WrappingForestTableIterator.init(scrubber.tour_tables_origin.?);

            log.debug("{}: open: tour_tables_origin.level={} tour_tables_origin.tree_id={}", .{
                scrubber.superblock.replica_index.?,
                scrubber.tour_tables_origin.?.level,
                scrubber.tour_tables_origin.?.tree_id,
            });
        }

        pub fn cancel(scrubber: *GridScrubber) void {
            for ([_]FIFOType(Read){ scrubber.reads_busy, scrubber.reads_done }) |reads_fifo| {
                var reads_iterator = reads_fifo.peek();
                while (reads_iterator) |read| : (reads_iterator = read.next) {
                    read.status = .canceled;
                }
            }

            if (scrubber.tour == .table_data) {
                // Skip scrubbing the table data; the table may not exist when state sync finishes.
                scrubber.tour = .table_index;
            }
        }

        /// Cancel queued reads to blocks that will be freed, now that the current checkpoint is
        /// durable. (The read still runs, but the results will be ignored.)
        pub fn checkpoint_durable(scrubber: *GridScrubber) void {
            assert(scrubber.superblock.opened);
            // GridScrubber.checkpoint_durable() is called immediately before
            // FreeSet.mark_checkpoint_durable(). All released blocks are about to be freed.
            assert(scrubber.forest.grid.callback == .none);

            for ([_]FIFOType(Read){ scrubber.reads_busy, scrubber.reads_done }) |reads_fifo| {
                var reads_iterator = reads_fifo.peek();
                while (reads_iterator) |read| : (reads_iterator = read.next) {
                    if (read.status == .repair) {
                        assert(!scrubber.forest.grid.free_set.is_free(read.read.address));
                        // Use `to_be_freed_at_checkpoint_durability` instead of `is_released`;
                        // the latter also contains the blocks that will be released when the
                        // *next* checkpoint becomes durable. We only need to abort scrubbing for
                        // blocks that are just about to be freed.
                        if (scrubber.forest.grid.free_set
                            .to_be_freed_at_checkpoint_durability(read.read.address))
                        {
                            read.status = .released;
                        }
                    }
                }
            }

            if (scrubber.tour == .table_data) {
                const index_address = scrubber.tour.table_data.index_address;
                assert(!scrubber.forest.grid.free_set.is_free(index_address));

                if (scrubber.forest.grid.free_set
                    .to_be_freed_at_checkpoint_durability(index_address))
                {
                    // Skip scrubbing the table data, since the table is about to be released.
                    scrubber.tour = .table_index;
                }
            }
        }

        /// Returns whether or not a new Read was started.
        pub fn read_next(scrubber: *GridScrubber) bool {
            assert(scrubber.superblock.opened);
            assert(scrubber.forest.grid.callback != .cancel);
            assert(scrubber.reads_busy.count + scrubber.reads_done.count ==
                scrubber.reads.executing());
            defer assert(scrubber.reads_busy.count + scrubber.reads_done.count ==
                scrubber.reads.executing());

            if (scrubber.reads.available() == 0) return false;
            const block_id = scrubber.tour_next() orelse return false;
            scrubber.tour_blocks_scrubbed_count += 1;

            const read = scrubber.reads.acquire().?;
            assert(!scrubber.reads_busy.contains(read));
            assert(!scrubber.reads_done.contains(read));

            log.debug("{}: read_next: address={} checksum={x:0>32} type={s}", .{
                scrubber.superblock.replica_index.?,
                block_id.block_address,
                block_id.block_checksum,
                @tagName(block_id.block_type),
            });

            read.* = .{
                .scrubber = scrubber,
                .block_type = block_id.block_type,
                .status = .repair,
                .done = false,
            };
            scrubber.reads_busy.push(read);

            scrubber.forest.grid.read_block(
                .{ .from_local_storage = read_next_callback },
                &read.read,
                block_id.block_address,
                block_id.block_checksum,
                .{ .cache_read = false, .cache_write = false },
            );
            return true;
        }

        fn read_next_callback(grid_read: *Grid.Read, result: Grid.ReadBlockResult) void {
            const read: *Read = @fieldParentPtr("read", grid_read);
            const scrubber = read.scrubber;
            assert(scrubber.reads_busy.contains(read));
            assert(!scrubber.reads_done.contains(read));
            assert(!read.done);
            maybe(read.status != .repair);

            log.debug("{}: read_next_callback: result={s} " ++
                "(address={} checksum={x:0>32} type={s} status={?})", .{
                scrubber.superblock.replica_index.?,
                @tagName(result),
                read.read.address,
                read.read.checksum,
                @tagName(read.block_type),
                read.status,
            });

            if (read.status == .repair and
                scrubber.tour == .table_data and
                scrubber.tour.table_data.index_block == null and
                scrubber.tour.table_data.index_checksum == read.read.checksum and
                scrubber.tour.table_data.index_address == read.read.address)
            {
                assert(scrubber.tour.table_data.data_block_index == 0);

                if (result == .valid) {
                    stdx.copy_disjoint(.inexact, u8, scrubber.tour_index_block, result.valid);
                    scrubber.tour.table_data.index_block = scrubber.tour_index_block;
                } else {
                    // The scrubber can't scrub the table data blocks until it has the corresponding
                    // index block. We will wait for the index block, and keep re-scrubbing it until
                    // it is repaired (or until the block is released by a checkpoint).
                    //
                    // (Alternatively, we could just skip past the table data blocks, and we will
                    // come across them again during the next cycle. But waiting for them makes for
                    // nicer invariants + tests.)
                    log.debug("{}: read_next_callback: waiting for index repair " ++
                        "(address={} checksum={x:0>32})", .{
                        scrubber.superblock.replica_index.?,
                        read.read.address,
                        read.read.checksum,
                    });
                }
            }

            if (result == .valid) {
                if (read.status == .repair) {
                    read.status = .ok;
                }
            }

            read.done = true;
            scrubber.reads_busy.remove(read);
            scrubber.reads_done.push(read);
        }

        pub fn read_fault(scrubber: *GridScrubber) ?BlockId {
            assert(scrubber.reads_busy.count + scrubber.reads_done.count ==
                scrubber.reads.executing());
            defer assert(scrubber.reads_busy.count + scrubber.reads_done.count ==
                scrubber.reads.executing());

            while (scrubber.reads_done.pop()) |read| {
                defer scrubber.reads.release(read);
                assert(read.done);

                if (read.status == .repair) {
                    return .{
                        .block_address = read.read.address,
                        .block_checksum = read.read.checksum,
                        .block_type = read.block_type,
                    };
                }
            }
            return null;
        }

        fn tour_next(scrubber: *GridScrubber) ?BlockId {
            assert(scrubber.superblock.opened);
            assert(scrubber.forest.manifest_log.opened);
            assert(scrubber.tour_tables_origin != null);

            const tour = &scrubber.tour;
            if (tour.* == .init) {
                tour.* = .table_index;
            }

            if (tour.* == .table_data) {
                const index_block = tour.table_data.index_block orelse {
                    // The table index is `null` if:
                    // - It was corrupt when we just scrubbed it.
                    // - Or `grid_scrubber_reads > 1`.
                    // Keep trying until either we find it, or a checkpoint removes it.
                    // (See read_next_callback() for more detail.)
                    return .{
                        .block_checksum = tour.table_data.index_checksum,
                        .block_address = tour.table_data.index_address,
                        .block_type = .index,
                    };
                };

                const index_schema = schema.TableIndex.from(index_block);
                const data_block_index = tour.table_data.data_block_index;
                if (data_block_index <
                    index_schema.data_blocks_used(scrubber.tour_index_block))
                {
                    tour.table_data.data_block_index += 1;

                    const data_block_addresses =
                        index_schema.data_addresses_used(scrubber.tour_index_block);
                    const data_block_checksums =
                        index_schema.data_checksums_used(scrubber.tour_index_block);
                    return .{
                        .block_checksum = data_block_checksums[data_block_index].value,
                        .block_address = data_block_addresses[data_block_index],
                        .block_type = .data,
                    };
                } else {
                    assert(data_block_index ==
                        index_schema.data_blocks_used(scrubber.tour_index_block));
                    tour.* = .table_index;
                }
            }

            if (tour.* == .table_index) {
                if (scrubber.tour_tables.?.next(scrubber.forest)) |table_info| {
                    if (Forest.Storage == TestStorage) {
                        scrubber.superblock.storage.verify_table(
                            table_info.address,
                            table_info.checksum,
                        );
                    }

                    tour.* = .{ .table_data = .{
                        .index_checksum = table_info.checksum,
                        .index_address = table_info.address,
                    } };

                    return .{
                        .block_checksum = table_info.checksum,
                        .block_address = table_info.address,
                        .block_type = .index,
                    };
                } else {
                    tour.* = .{ .manifest_log = .{} };
                }
            }

            if (tour.* == .manifest_log) {
                if (tour.manifest_log.iterator.next(
                    &scrubber.forest.manifest_log,
                )) |block_reference| {
                    return .{
                        .block_checksum = block_reference.checksum,
                        .block_address = block_reference.address,
                        .block_type = .manifest,
                    };
                } else {
                    tour.* = .{ .free_set_blocks_acquired = .{} };
                }
            }

            if (tour.* == .free_set_blocks_acquired) {
                const free_set_trailer = &scrubber.forest.grid.free_set_checkpoint_blocks_acquired;
                if (free_set_trailer.callback != .none) return null;
                if (tour.free_set_blocks_acquired.index < free_set_trailer.block_count()) {
                    const index = tour.free_set_blocks_acquired.index;
                    tour.free_set_blocks_acquired.index += 1;
                    return .{
                        .block_checksum = free_set_trailer.block_checksums[index],
                        .block_address = free_set_trailer.block_addresses[index],
                        .block_type = .free_set,
                    };
                } else {
                    // A checkpoint can reduce the number of trailer blocks while we are scrubbing
                    // the trailer.
                    maybe(tour.free_set_blocks_acquired.index > free_set_trailer.block_count());
                    tour.* = .{ .free_set_blocks_released = .{} };
                }
            }

            if (tour.* == .free_set_blocks_released) {
                const free_set_trailer = &scrubber.forest.grid.free_set_checkpoint_blocks_released;
                if (free_set_trailer.callback != .none) return null;
                if (tour.free_set_blocks_released.index < free_set_trailer.block_count()) {
                    const index = tour.free_set_blocks_released.index;
                    tour.free_set_blocks_released.index += 1;
                    return .{
                        .block_checksum = free_set_trailer.block_checksums[index],
                        .block_address = free_set_trailer.block_addresses[index],
                        .block_type = .free_set,
                    };
                } else {
                    // A checkpoint can reduce the number of trailer blocks while we are scrubbing
                    // the trailer.
                    maybe(tour.free_set_blocks_released.index > free_set_trailer.block_count());
                    tour.* = .{ .client_sessions = .{} };
                }
            }

            if (tour.* == .client_sessions) {
                const client_sessions = scrubber.client_sessions_checkpoint;
                if (client_sessions.callback != .none) return null;
                if (tour.client_sessions.index < client_sessions.block_count()) {
                    const index = tour.client_sessions.index;
                    tour.client_sessions.index += 1;
                    return .{
                        .block_checksum = client_sessions.block_checksums[index],
                        .block_address = client_sessions.block_addresses[index],
                        .block_type = .client_sessions,
                    };
                } else {
                    // A checkpoint can reduce the number of trailer blocks while we are scrubbing
                    // the trailer.
                    maybe(tour.client_sessions.index > client_sessions.block_count());
                    tour.* = .done;
                }
            }

            // Note that this is just the end of the tour.
            // (Some of the cycle's reads may still be in progress).
            log.debug("{}: tour_next: cycle done (toured_blocks={})", .{
                scrubber.superblock.replica_index.?,
                scrubber.tour_blocks_scrubbed_count,
            });

            // Wrap around to the next cycle.
            assert(tour.* == .done);
            tour.* = .init;
            scrubber.tour_tables = WrappingForestTableIterator.init(scrubber.tour_tables_origin.?);
            scrubber.tour_blocks_scrubbed_count = 0;

            return null;
        }
    };
}

fn WrappingForestTableIteratorType(comptime Forest: type) type {
    return struct {
        const WrappingForestTableIterator = @This();
        const ForestTableIterator = ForestTableIteratorType(Forest);

        origin: Origin,
        tables: ForestTableIterator,
        wrapped: bool,

        pub const Origin = struct {
            level: u6,
            tree_id: u16,
        };

        pub fn init(origin: Origin) WrappingForestTableIterator {
            return .{
                .origin = origin,
                .tables = .{
                    .level = origin.level,
                    .tree_id = origin.tree_id,
                },
                .wrapped = false,
            };
        }

        pub fn next(
            iterator: *WrappingForestTableIterator,
            forest: *const Forest,
        ) ?schema.ManifestNode.TableInfo {
            const table = iterator.tables.next(forest) orelse {
                if (iterator.wrapped) {
                    return null;
                } else {
                    iterator.wrapped = true;
                    iterator.tables = .{};
                    return iterator.tables.next(forest);
                }
            };

            if (iterator.wrapped and
                iterator.origin.level <= table.label.level and
                iterator.origin.tree_id <= table.tree_id)
            {
                return null;
            }
            return table;
        }
    };
}

/// Iterate over every manifest block address/checksum in the manifest log.
///
/// This iterator is stable across ManifestLog mutation – that is, it is guaranteed to iterate over
/// every manifest block that survives the entire iteration.
fn ManifestBlockIteratorType(comptime ManifestLog: type) type {
    return union(enum) {
        const ManifestBlockIterator = @This();

        init,
        done,
        state: struct {
            /// The last-known index (within the manifest blocks) of the address/checksum.
            index: u32,
            /// The address/checksum of the most-recently iterated manifest block.
            address: u64,
            checksum: u128,
        },

        fn next(
            iterator: *ManifestBlockIterator,
            manifest_log: *const ManifestLog,
        ) ?vsr.BlockReference {
            // Don't scrub the trailing `blocks_closed`; they are not yet flushed to disk.
            const log_block_count: u32 =
                @intCast(manifest_log.log_block_addresses.count - manifest_log.blocks_closed);

            const position: ?u32 = switch (iterator.*) {
                .done => null,
                .init => if (log_block_count == 0) null else log_block_count - 1,
                .state => |state| position: {
                    // `index` may be beyond the limit due to blocks removed by manifest compaction.
                    maybe(state.index >= log_block_count);

                    // The block that we most recently scrubbed may:
                    // - be in the same position, or
                    // - have shifted earlier in the list (due to manifest compaction), or
                    // - have been removed from the list (due to manifest compaction).
                    // Use the block's old position to find its current position.
                    var position: u32 = @min(state.index, log_block_count -| 1);
                    while (position > 0) : (position -= 1) {
                        if (manifest_log.log_block_addresses.get(position).? == state.address and
                            manifest_log.log_block_checksums.get(position).? == state.checksum)
                        {
                            break :position if (position == 0) null else position - 1;
                        }
                    } else {
                        break :position null;
                    }
                },
            };

            if (position) |index| {
                iterator.* = .{ .state = .{
                    .index = index,
                    .address = manifest_log.log_block_addresses.get(index).?,
                    .checksum = manifest_log.log_block_checksums.get(index).?,
                } };

                return .{
                    .address = iterator.state.address,
                    .checksum = iterator.state.checksum,
                };
            } else {
                iterator.* = .done;
                return null;
            }
        }
    };
}

// Model the probability that the cluster experiences data loss due to bitrot.
// Specifically, that *every* copy of *any* block is corrupted before the scrubber can repair it.
//
// Optimistic assumptions (see below):
// - Faults are independent between replicas. ¹
// - Faults are independent (i.e. uncorrelated) in space and time. ²
//
// Pessimistic assumptions:
// - There are only 3 (quorum_replication) copies of each sector.
// - Scrub randomization is ignored.
// - The simulated fault rate is much greater than a real disk's. (See `sector_faults_per_year`).
// - Reads, writes, and repairs due to other workloads (besides the scrubber) are not modelled.
// - All blocks are always full (512KiB).
//
// ¹: To mitigate the risk of correlated errors in production, replicas could use different SSD
// (hardware) models.
//
// ²: SSD faults are not independent (in either time or space).
// See, for example:
// - "An In-Depth Study of Correlated Failures in Production SSD-Based Data Centers"
//   (https://www.usenix.org/system/files/fast21-han.pdf)
// - "Flash Reliability in Production: The Expected and the Unexpected"
//   (https://www.usenix.org/system/files/conference/fast16/fast16-papers-schroeder.pdf)
// That being said, for the purposes of modelling scrubbing, it is a decent approximation because
// blocks are large relative to sectors. (Additionally, blocks that are written together are often
// scrubbed together).
test "GridScrubber cycle interval" {
    // Parameters:

    // The number of years that the test is "running". As the test runs longer, the probability that
    // the cluster will experience data loss increases.
    const test_duration_years = 20;

    // The number of days between scrubs of a particular sector.
    // Equivalently, the number of days to scrub the entire data file.
    const cycle_interval_days = 180;

    // The total size of the data file.
    // Note that since this parameter is separate from the faults/year rate, increasing
    // `storage_size` actually reduces the likelihood of data loss.
    const storage_size = 16 * (1024 * 1024 * 1024 * 1024);

    // The expected (average) number of sector faults per year.
    // I can't find any good, recent statistics for faults on SSDs.
    //
    // Most papers express the fault rate as "UBER" (uncorrectable bit errors per total bits read).
    // But "Flash Reliability in Production: The Expected and the Unexpected" §5.1 finds that
    // UBER's underlying assumption ­ that the uncorrectable errors is correlated to the number of
    // bytes read ­ is false. (That paper only shares "fraction of drives affected by an error",
    // which is too coarse for this model's purposes.)
    //
    // Instead, the parameter is chosen conservatively ­ greater than the "true" number by at least
    // an order of magnitude.
    const sector_faults_per_year = 10_000;

    // A block has multiple sectors. If any of a block's sectors are corrupt, then the block is
    // corrupt.
    //
    // Increasing this parameter increases the likelihood of eventual data loss.
    // (Intuitively, a single bitrot within 1GiB is more likely than a single bitrot within 1KiB.)
    const block_size = 512 * 1024;

    // The total number of copies of each sector.
    // The cluster is recoverable if a sector's number of faults is less than `replicas_total`.
    // Set to 3 rather than 6 since 3 is the quorum_replication.
    const replicas_total = 3;

    const sector_size = constants.sector_size;

    // Computation:

    const block_sectors = @divExact(block_size, sector_size);
    const storage_sectors = @divExact(storage_size, sector_size);
    const storage_blocks = @divExact(storage_size, block_size);
    const test_duration_days = test_duration_years * 365;
    const test_duration_cycles = stdx.div_ceil(test_duration_days, cycle_interval_days);
    const sector_faults_per_cycle =
        stdx.div_ceil(sector_faults_per_year * cycle_interval_days, 365);

    // P(a specific block is uncorrupted for an entire cycle)
    // If any of the block's sectors is corrupted, then the whole block is corrupted.
    const p_block_healthy_per_cycle = std.math.pow(
        f64,
        @as(f64, @floatFromInt(storage_sectors - block_sectors)) /
            @as(f64, @floatFromInt(storage_sectors)),
        @as(f64, @floatFromInt(sector_faults_per_cycle)),
    );

    const p_block_corrupt_per_cycle = 1.0 - p_block_healthy_per_cycle;
    // P(a specific block is corrupted on all replicas during a single cycle)
    const p_cluster_block_corrupt_per_cycle =
        std.math.pow(f64, p_block_corrupt_per_cycle, @as(f64, @floatFromInt(replicas_total)));
    // P(a specific block is uncorrupted on at least one replica during a single cycle)
    const p_cluster_block_healthy_per_cycle = 1.0 - p_cluster_block_corrupt_per_cycle;

    // P(a specific block is uncorrupted on at least one replica for all cycles)
    // Note that each cycle can be considered independently because we assume that if is at the end
    // of the cycle there is at least one healthy copy, then all of the corrupt copies are repaired.
    const p_cluster_block_healthy_per_span = std.math.pow(
        f64,
        p_cluster_block_healthy_per_cycle,
        @as(f64, @floatFromInt(test_duration_cycles)),
    );

    // P(each block is uncorrupted on at least one replica for all cycles)
    const p_cluster_blocks_healthy_per_span = std.math.pow(
        f64,
        p_cluster_block_healthy_per_span,
        @as(f64, @floatFromInt(storage_blocks)),
    );

    // P(at some point during all cycles, at least one block is corrupt across all replicas)
    // In other words, P(eventual data loss).
    const p_cluster_blocks_corrupt_per_span = 1.0 - p_cluster_blocks_healthy_per_span;

    const Snap = @import("../testing/snaptest.zig").Snap;
    const snap = Snap.snap;

    try snap(@src(),
        \\4.3582921528e-3
    ).diff_fmt("{e:.10}", .{p_cluster_blocks_corrupt_per_span});
}
