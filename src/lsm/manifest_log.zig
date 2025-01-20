//! Maintains a durable manifest log of the latest TableInfo's for every LSM tree's in-memory
//! manifest.
//!
//! Invariants:
//!
//! * Checkpointing the manifest log must flush all buffered log blocks.
//!
//! * Opening the manifest log must emit only the latest TableInfo's to be inserted.
//!
//! * The latest version of a table must never be dropped from the log through a compaction, unless
//!   the table was removed.
//!
//! * Removes that are recorded in a log block must also queue that log block for compaction.
//!
//! * Compaction must compact partially full blocks, even where it must rewrite all entries to the
//!   tail end of the log.
//!
//! * If a remove is dropped from the log, then all prior inserts/updates must already have been
//!   dropped.

const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const maybe = stdx.maybe;

const log = std.log.scoped(.manifest_log);

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const stdx = @import("../stdx.zig");

const SuperBlockType = vsr.SuperBlockType;
const GridType = @import("../vsr/grid.zig").GridType;
const BlockPtr = @import("../vsr/grid.zig").BlockPtr;
const BlockPtrConst = @import("../vsr/grid.zig").BlockPtrConst;
const allocate_block = @import("../vsr/grid.zig").allocate_block;
const compaction = @import("compaction.zig");
const RingBufferType = stdx.RingBufferType;
const schema = @import("schema.zig");
const TableInfo = schema.ManifestNode.TableInfo;
const BlockReference = vsr.BlockReference;

const block_builder_schema = schema.ManifestNode{
    .entry_count = schema.ManifestNode.entry_count_max,
};

pub fn ManifestLogType(comptime Storage: type) type {
    return struct {
        const ManifestLog = @This();

        const SuperBlock = SuperBlockType(Storage);
        const Grid = GridType(Storage);

        pub const Callback = *const fn (manifest_log: *ManifestLog) void;

        pub const OpenEvent = *const fn (manifest_log: *ManifestLog, table: *const TableInfo) void;

        const Write = struct {
            manifest_log: *ManifestLog,
            write: Grid.Write = undefined,
        };

        const TableExtents = std.AutoHashMapUnmanaged(u64, TableExtent);
        const TablesRemoved = std.AutoHashMapUnmanaged(u64, void);

        pub const TableExtent = struct {
            block: u64, // Manifest block address.
            entry: u32, // Index within the manifest block Label/TableInfo arrays.
        };

        superblock: *SuperBlock,
        grid: *Grid,
        pace: *const Pace,

        forest_table_count_max: u32,

        grid_reservation: ?Grid.Reservation = null,

        /// The number of blocks (remaining) to compact during the current half-bar.
        compact_blocks: ?u32 = null,

        /// This is a struct-of-arrays of `BlockReference`s.
        /// It includes:
        /// - blocks that are written
        /// - blocks that have closed, but not yet flushed
        /// - blocks that are being flushed
        ///
        /// Entries are ordered from oldest to newest.
        log_block_checksums: RingBufferType(u128, .slice),
        log_block_addresses: RingBufferType(u64, .slice),

        /// The head block accumulates a full block, to be written at the next flush.
        /// The remaining blocks must accommodate all further appends.
        blocks: RingBufferType(BlockPtr, .slice),

        /// The number of blocks that have been appended to, filled up, and then closed.
        blocks_closed: u8 = 0,

        /// The number of entries in the open block.
        ///
        /// Invariants:
        /// - When `entry_count = 0`, there is no open block.
        /// - `entry_count < entry_count_max`. When `entry_count` reaches the maximum, the open
        ///   block is closed, and `entry_count` resets to 0.
        entry_count: u32 = 0,

        opened: bool = false,
        open_event: OpenEvent = undefined,

        /// Set for the duration of `open` and `compact`.
        reading: bool = false,
        read: Grid.Read = undefined,
        read_callback: ?Callback = null,

        /// Set for the duration of `flush` and `checkpoint`.
        writing: bool = false,
        writes: []Write,
        writes_pending: usize = 0,
        write_callback: ?Callback = null,

        next_tick: Grid.NextTick = undefined,

        /// A map from table address to the manifest block and entry that is the latest extent
        /// version. Used to determine whether a table should be dropped in a compaction.
        table_extents: TableExtents,

        /// For a particular table in the manifest, the sequence of events is:
        ///
        ///   insert(0|1), update(0+), remove(0|1)
        ///
        /// During open(), manifest entries are processed in reverse-chronological order.
        ///
        /// This hash-set tracks tables that have been removed but whose corresponding "insert" has
        /// not yet been encountered. Given that the maximum number of tables in the forest at any
        /// given moment is `forest_table_count_max`, there are likewise at most
        /// `forest_table_count_max` "unpaired" removes to track.
        // TODO(Optimization) This memory (~35MiB) is only needed during open() – maybe borrow it
        // from the grid cache or node pool instead so that we don't pay for it during normal
        // operation.
        tables_removed: TablesRemoved,

        pub fn init(
            manifest_log: *ManifestLog,
            allocator: mem.Allocator,
            grid: *Grid,
            compaction_pace: *const Pace,
        ) !void {
            manifest_log.* = .{
                .superblock = grid.superblock,
                .grid = grid,
                .forest_table_count_max = compaction_pace.tables_max,
                .pace = compaction_pace,
                .log_block_checksums = undefined,
                .log_block_addresses = undefined,
                .blocks = undefined,
                .writes = undefined,
                .table_extents = undefined,
                .tables_removed = undefined,
            };

            inline for (std.meta.fields(Pace)) |pace_field| {
                log.debug("{?}: Manifest.Pace.{s} = {d}", .{
                    grid.superblock.replica_index,
                    pace_field.name,
                    @field(manifest_log.pace, pace_field.name),
                });
            }

            manifest_log.log_block_checksums =
                try RingBufferType(u128, .slice).init(allocator, manifest_log.pace.log_blocks_max);
            errdefer manifest_log.log_block_checksums.deinit(allocator);

            manifest_log.log_block_addresses =
                try RingBufferType(u64, .slice).init(allocator, manifest_log.pace.log_blocks_max);
            errdefer manifest_log.log_block_addresses.deinit(allocator);

            // The upper-bound of manifest blocks we must buffer.
            //
            // `blocks` must have sufficient capacity for:
            // - a leftover open block from the previous ops (+1 block)
            // - table updates copied from a half bar of manifest compactions
            // - table updates from a half bar of table compactions
            const half_bar_buffer_blocks_max = 1 + manifest_log.pace.half_bar_compact_blocks_max +
                manifest_log.pace.half_bar_append_blocks_max;
            assert(half_bar_buffer_blocks_max >= 3);

            // TODO RingBuffer for .slice should be extended to take care of alignment:
            manifest_log.blocks =
                try RingBufferType(BlockPtr, .slice).init(allocator, half_bar_buffer_blocks_max);
            errdefer manifest_log.blocks.deinit(allocator);

            for (manifest_log.blocks.buffer, 0..) |*block, i| {
                errdefer for (manifest_log.blocks.buffer[0..i]) |b| allocator.free(b);
                block.* = try allocate_block(allocator);
            }
            errdefer for (manifest_log.blocks.buffer) |b| allocator.free(b);

            manifest_log.writes = try allocator.alloc(Write, half_bar_buffer_blocks_max);
            errdefer allocator.free(manifest_log.writes);
            @memset(manifest_log.writes, undefined);

            manifest_log.table_extents = TableExtents{};
            try manifest_log.table_extents.ensureTotalCapacity(
                allocator,
                // Allocate space for one additional table, so that the code can still use
                // `getOrPutAssumeCapacity` while making it easier to check if the limit has been
                // exceeded to error with a friendly message.
                manifest_log.forest_table_count_max + 1,
            );
            errdefer manifest_log.table_extents.deinit(allocator);

            manifest_log.tables_removed = TablesRemoved{};
            try manifest_log.tables_removed.ensureTotalCapacity(
                allocator,
                manifest_log.forest_table_count_max,
            );
            errdefer manifest_log.tables_removed.deinit(allocator);
        }

        pub fn deinit(manifest_log: *ManifestLog, allocator: mem.Allocator) void {
            manifest_log.tables_removed.deinit(allocator);
            manifest_log.table_extents.deinit(allocator);
            allocator.free(manifest_log.writes);
            for (manifest_log.blocks.buffer) |block| allocator.free(block);
            manifest_log.blocks.deinit(allocator);
            manifest_log.log_block_addresses.deinit(allocator);
            manifest_log.log_block_checksums.deinit(allocator);
        }

        pub fn reset(manifest_log: *ManifestLog) void {
            assert(manifest_log.log_block_checksums.count ==
                manifest_log.log_block_addresses.count);

            manifest_log.grid.trace.reset(.compact_manifest);

            manifest_log.log_block_checksums.clear();
            manifest_log.log_block_addresses.clear();
            for (manifest_log.blocks.buffer) |block| @memset(block, 0);
            manifest_log.table_extents.clearRetainingCapacity();
            manifest_log.tables_removed.clearRetainingCapacity();

            manifest_log.* = .{
                .superblock = manifest_log.superblock,
                .grid = manifest_log.grid,
                .forest_table_count_max = manifest_log.pace.tables_max,
                .pace = manifest_log.pace,
                .log_block_checksums = manifest_log.log_block_checksums,
                .log_block_addresses = manifest_log.log_block_addresses,
                .blocks = .{ .buffer = manifest_log.blocks.buffer },
                .writes = manifest_log.writes,
                .table_extents = manifest_log.table_extents,
                .tables_removed = manifest_log.tables_removed,
            };
        }

        /// Opens the manifest log.
        /// Reads the manifest blocks in reverse order and passes extent table inserts to event().
        /// Therefore, only the latest version of a table will be emitted by event() for insertion
        /// into the in-memory manifest. Older versions of a table in older manifest blocks will not
        /// be emitted, as an optimization to not replay all table mutations.
        /// `ManifestLog.table_extents` is used to track the latest version of a table.
        // TODO(Optimization): Accumulate tables unordered, then sort all at once to splice into the
        // ManifestLevels' SegmentedArrays. (Constructing SegmentedArrays by repeated inserts is
        // expensive.)
        pub fn open(manifest_log: *ManifestLog, event: OpenEvent, callback: Callback) void {
            assert(!manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.read_callback == null);

            assert(manifest_log.log_block_checksums.count == 0);
            assert(manifest_log.log_block_addresses.count == 0);
            assert(manifest_log.blocks.count == 0);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.entry_count == 0);
            assert(manifest_log.table_extents.count() == 0);
            assert(manifest_log.tables_removed.count() == 0);

            manifest_log.open_event = event;
            manifest_log.reading = true;
            manifest_log.read_callback = callback;

            const references = manifest_log.superblock.working.manifest_references();
            assert(references.block_count <= manifest_log.log_block_checksums.buffer.len);

            if (references.empty()) {
                manifest_log.grid.on_next_tick(open_next_tick_callback, &manifest_log.next_tick);
            } else {
                manifest_log.open_read_block(.{
                    .checksum = references.newest_checksum,
                    .address = references.newest_address,
                });
            }
        }

        fn open_next_tick_callback(next_tick: *Grid.NextTick) void {
            const manifest_log: *ManifestLog = @alignCast(@fieldParentPtr("next_tick", next_tick));
            assert(!manifest_log.opened);
            assert(manifest_log.reading);
            assert(!manifest_log.writing);

            assert(manifest_log.log_block_checksums.count == 0);
            assert(manifest_log.log_block_addresses.count == 0);
            assert(manifest_log.table_extents.count() == 0);
            assert(manifest_log.tables_removed.count() == 0);
            assert(manifest_log.superblock.working.manifest_references().empty());

            manifest_log.open_done();
        }

        fn open_read_block(manifest_log: *ManifestLog, block_reference: BlockReference) void {
            assert(!manifest_log.opened);
            assert(manifest_log.reading);
            assert(manifest_log.read_callback != null);
            assert(!manifest_log.writing);
            assert(manifest_log.write_callback == null);
            assert(manifest_log.table_extents.count() <= manifest_log.forest_table_count_max);
            assert(manifest_log.tables_removed.count() <= manifest_log.forest_table_count_max);
            assert(manifest_log.log_block_checksums.count <
                manifest_log.log_block_checksums.buffer.len);
            assert(manifest_log.log_block_checksums.count ==
                manifest_log.log_block_addresses.count);
            assert(manifest_log.log_block_checksums.count <
                manifest_log.superblock.working.vsr_state.checkpoint.manifest_block_count);
            assert(manifest_log.blocks.count == 0);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.entry_count == 0);
            assert(block_reference.address > 0);

            if (constants.verify) {
                // The manifest block list has no cycles.
                var address_iterator = manifest_log.log_block_addresses.iterator();
                while (address_iterator.next()) |address| {
                    assert(address != block_reference.address);
                }
            }

            manifest_log.log_block_checksums.push_head_assume_capacity(block_reference.checksum);
            manifest_log.log_block_addresses.push_head_assume_capacity(block_reference.address);

            manifest_log.grid.read_block(
                .{ .from_local_or_global_storage = open_read_block_callback },
                &manifest_log.read,
                block_reference.address,
                block_reference.checksum,
                .{ .cache_read = true, .cache_write = true },
            );
        }

        fn open_read_block_callback(read: *Grid.Read, block: BlockPtrConst) void {
            const manifest_log: *ManifestLog = @fieldParentPtr("read", read);
            assert(!manifest_log.opened);
            assert(manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.log_block_addresses.count > 0);
            assert(manifest_log.log_block_checksums.count > 0);
            assert(!manifest_log.superblock.working.manifest_references().empty());

            const block_checksum = manifest_log.log_block_checksums.head().?;
            const block_address = manifest_log.log_block_addresses.head().?;
            verify_block(block, block_checksum, block_address);

            const block_schema = schema.ManifestNode.from(block);
            const tables_used = block_schema.tables_const(block);
            assert(block_schema.entry_count > 0);
            assert(block_schema.entry_count <= schema.ManifestNode.entry_count_max);

            var entry = block_schema.entry_count;
            while (entry > 0) {
                entry -= 1;

                const table = &tables_used[entry];
                assert(table.label.event != .reserved);
                assert(table.address > 0);

                if (table.label.event == .remove) {
                    const table_removed =
                        manifest_log.tables_removed.fetchPutAssumeCapacity(table.address, {});
                    assert(table_removed == null);
                } else {
                    if (manifest_log.tables_removed.get(table.address)) |_| {
                        if (table.label.event == .insert) {
                            assert(manifest_log.tables_removed.remove(table.address));
                        }
                    } else {
                        const extent =
                            manifest_log.table_extents.getOrPutAssumeCapacity(table.address);
                        if (!extent.found_existing) {
                            manifest_log.check_tables_count();
                            extent.value_ptr.* = .{ .block = block_address, .entry = entry };
                            manifest_log.open_event(manifest_log, table);
                        }
                    }
                }
            }

            log.debug("{}: opened: checksum={} address={} entries={}", .{
                manifest_log.superblock.replica_index.?,
                block_checksum,
                block_address,
                block_schema.entry_count,
            });

            const checkpoint_state = &manifest_log.superblock.working.vsr_state.checkpoint;
            if (checkpoint_state.manifest_oldest_address == block_address) {
                // When we find the oldest block, stop iterating the linked list – any more blocks
                // have already been compacted away.
                assert(checkpoint_state.manifest_oldest_checksum == block_checksum);

                manifest_log.open_done();
            } else {
                const block_reference_previous = schema.ManifestNode.previous(block).?;

                manifest_log.open_read_block(.{
                    .checksum = block_reference_previous.checksum,
                    .address = block_reference_previous.address,
                });
            }
        }

        fn open_done(manifest_log: *ManifestLog) void {
            assert(!manifest_log.opened);
            assert(manifest_log.reading);
            assert(manifest_log.read_callback != null);
            assert(!manifest_log.writing);
            assert(manifest_log.write_callback == null);
            assert(manifest_log.table_extents.count() <= manifest_log.forest_table_count_max);
            assert(manifest_log.tables_removed.count() <= manifest_log.forest_table_count_max);
            assert(manifest_log.log_block_checksums.count ==
                manifest_log.log_block_addresses.count);
            assert(manifest_log.log_block_checksums.count ==
                manifest_log.superblock.working.vsr_state.checkpoint.manifest_block_count);
            assert(manifest_log.blocks.count == 0);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.entry_count == 0);

            log.debug("{}: open_done: opened block_count={} table_count={}", .{
                manifest_log.superblock.replica_index.?,
                manifest_log.log_block_checksums.count,
                manifest_log.table_extents.count(),
            });

            const callback = manifest_log.read_callback.?;
            manifest_log.opened = true;
            manifest_log.open_event = undefined;
            manifest_log.reading = false;
            manifest_log.read_callback = null;

            callback(manifest_log);
        }

        /// Appends an insert/update/remove of a table to a level.
        ///
        /// A move is only recorded as an update, there is no remove from the previous level, since
        /// this is safer (no potential to get the event order wrong) and reduces fragmentation.
        pub fn append(manifest_log: *ManifestLog, table: *const TableInfo) void {
            maybe(manifest_log.opened);
            maybe(manifest_log.reading);
            assert(!manifest_log.writing);

            switch (table.label.event) {
                .reserved => unreachable,
                .insert => assert(manifest_log.table_extents.get(table.address) == null),
                // For updates + removes, the table must have previously been inserted into the log:
                .update => assert(manifest_log.table_extents.get(table.address) != null),
                .remove => assert(manifest_log.table_extents.get(table.address) != null),
            }

            manifest_log.append_internal(table);
        }

        /// The table extent must be updated immediately when appending, without delay.
        /// Otherwise, ManifestLog.compact() may append a stale version over the latest.
        ///
        /// append_internal() is used for both:
        /// - External appends, e.g. events created due to table compaction.
        /// - Internal appends, e.g. events recycled by manifest compaction.
        fn append_internal(manifest_log: *ManifestLog, table: *const TableInfo) void {
            assert(manifest_log.opened);
            assert(!manifest_log.writing);
            maybe(manifest_log.reading);
            assert(manifest_log.grid_reservation != null);
            assert(table.label.level < constants.lsm_levels);
            assert(table.address > 0);
            assert(table.snapshot_min > 0);
            assert(table.snapshot_max > table.snapshot_min);

            if (manifest_log.entry_count == 0) {
                assert(manifest_log.blocks.count == manifest_log.blocks_closed);
                manifest_log.acquire_block();
            } else if (manifest_log.entry_count > 0) {
                assert(manifest_log.blocks.count > 0);
            }

            assert(manifest_log.entry_count < schema.ManifestNode.entry_count_max);
            assert(manifest_log.blocks.count - manifest_log.blocks_closed == 1);

            log.debug(
                "{}: {s}: level={} tree={} checksum={} address={} snapshot={}..{}",
                .{
                    manifest_log.superblock.replica_index.?,
                    @tagName(table.label.event),
                    table.label.level,
                    table.tree_id,
                    table.checksum,
                    table.address,
                    table.snapshot_min,
                    table.snapshot_max,
                },
            );

            const block: BlockPtr = manifest_log.blocks.tail().?;
            const entry = manifest_log.entry_count;
            block_builder_schema.tables(block)[entry] = table.*;

            const block_header =
                mem.bytesAsValue(vsr.Header.Block, block[0..@sizeOf(vsr.Header)]);
            const block_address = block_header.address;

            switch (table.label.event) {
                .reserved => unreachable,
                .insert,
                .update,
                => {
                    const extent = manifest_log.table_extents.getOrPutAssumeCapacity(table.address);
                    if (!extent.found_existing) {
                        assert(table.label.event == .insert);

                        // When inserting, check that the insertion didn't cause the number of
                        // tables to exceed `forest_table_count_max`.
                        manifest_log.check_tables_count();
                    } else {
                        maybe(table.label.event == .insert); // (Compaction.)
                    }
                    extent.value_ptr.* = .{ .block = block_address, .entry = entry };
                },
                .remove => assert(manifest_log.table_extents.remove(table.address)),
            }

            manifest_log.entry_count += 1;
            if (manifest_log.entry_count == schema.ManifestNode.entry_count_max) {
                manifest_log.close_block();
                assert(manifest_log.entry_count == 0);
            }
        }

        fn check_tables_count(manifest_log: *ManifestLog) void {
            const tables_count = manifest_log.table_extents.count();
            if (tables_count > manifest_log.forest_table_count_max) {
                vsr.fatal(
                    .forest_tables_count_would_exceed_limit,
                    "forest_tables_count would exceed limit " ++
                        "(tables_count={} tables_max={}) - " ++
                        "please contact the team directly who will be able to assist",
                    .{ tables_count, manifest_log.forest_table_count_max },
                );
            }
        }

        fn flush(manifest_log: *ManifestLog, callback: Callback) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.write_callback == null);

            log.debug("{}: flush: writing {} block(s)", .{
                manifest_log.superblock.replica_index.?,
                manifest_log.blocks_closed,
            });

            manifest_log.writing = true;
            manifest_log.write_callback = callback;

            for (0..manifest_log.blocks_closed) |_| manifest_log.write_block();
            assert(manifest_log.blocks_closed == manifest_log.writes_pending);

            if (manifest_log.writes_pending == 0) {
                manifest_log.grid.on_next_tick(flush_next_tick_callback, &manifest_log.next_tick);
            }
        }

        fn flush_next_tick_callback(next_tick: *Grid.NextTick) void {
            const manifest_log: *ManifestLog = @alignCast(@fieldParentPtr("next_tick", next_tick));
            assert(manifest_log.writing);

            manifest_log.flush_done();
        }

        fn flush_done(manifest_log: *ManifestLog) void {
            assert(manifest_log.writing);
            assert(manifest_log.write_callback != null);
            assert(manifest_log.blocks_closed == 0);

            const callback = manifest_log.write_callback.?;
            manifest_log.write_callback = null;
            manifest_log.writing = false;
            callback(manifest_log);
        }

        fn write_block(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            assert(manifest_log.writing);
            assert(manifest_log.blocks_closed > 0);
            assert(manifest_log.blocks_closed <= manifest_log.blocks.count);
            assert(manifest_log.writes_pending < manifest_log.blocks_closed);

            const block_index = manifest_log.writes_pending;
            const block = manifest_log.blocks.get_ptr(block_index).?;
            verify_block(block.*, null, null);

            const block_schema = schema.ManifestNode.from(block.*);
            assert(block_schema.entry_count > 0);

            const header = schema.header_from_block(block.*);
            assert(header.address > 0);

            if (block_index == manifest_log.blocks_closed - 1) {
                // This might be the last block of a checkpoint, which can be a partial block.
                assert(block_schema.entry_count <= schema.ManifestNode.entry_count_max);
            } else {
                assert(block_schema.entry_count == schema.ManifestNode.entry_count_max);
            }

            log.debug("{}: write_block: checksum={} address={} entries={}", .{
                manifest_log.superblock.replica_index.?,
                header.checksum,
                header.address,
                block_schema.entry_count,
            });

            const write = &manifest_log.writes[block_index];
            write.* = .{ .manifest_log = manifest_log };

            manifest_log.writes_pending += 1;
            manifest_log.grid.create_block(write_block_callback, &write.write, block);
        }

        fn write_block_callback(grid_write: *Grid.Write) void {
            const write: *Write = @fieldParentPtr("write", grid_write);
            const manifest_log = write.manifest_log;
            assert(manifest_log.opened);
            assert(manifest_log.writing);
            assert(manifest_log.blocks_closed <= manifest_log.blocks.count);

            manifest_log.writes_pending -= 1;

            if (manifest_log.writes_pending == 0) {
                for (0..manifest_log.blocks_closed) |_| manifest_log.blocks.advance_head();
                manifest_log.blocks_closed = 0;

                if (manifest_log.blocks.count == 0) {
                    assert(manifest_log.entry_count == 0);
                } else {
                    assert(manifest_log.blocks.count == 1);
                    assert(manifest_log.entry_count < schema.ManifestNode.entry_count_max);
                }

                manifest_log.flush_done();
            }
        }

        /// `compact` does not close a partial block; that is only necessary during `checkpoint`.
        ///
        /// The (production) block size is large, so the number of blocks compacted per half-bar is
        /// relatively small (e.g. ~4). We read them in sequence rather than parallel to spread the
        /// work more evenly across the half-bar's beats.
        // TODO Make sure block reservation cannot fail — before compaction begins verify that
        // enough free blocks are available for all reservations.
        pub fn compact(manifest_log: *ManifestLog, callback: Callback, op: u64) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.read_callback == null);
            assert(manifest_log.write_callback == null);
            assert(manifest_log.grid_reservation == null);
            assert(manifest_log.blocks.count ==
                manifest_log.blocks_closed + @intFromBool(manifest_log.entry_count > 0));
            assert(manifest_log.compact_blocks == null);

            // TODO: Currently manifest compaction is hardcoded to run on the last beat of each
            // half-bar.
            // This is because otherwise it would mess with our grid reserve / forfeit ordering,
            // since we now reserve / forfeit per beat.
            assert((op + 1) % @divExact(constants.lsm_compaction_ops, 2) == 0);

            manifest_log.grid.trace.start(.compact_manifest, .{});

            if (op < constants.lsm_compaction_ops or
                manifest_log.superblock.working.vsr_state.op_compacted(op))
            {
                manifest_log.read_callback = callback;
                manifest_log.grid.on_next_tick(compact_tick_callback, &manifest_log.next_tick);
                return;
            }

            manifest_log.compact_blocks = @min(
                manifest_log.pace.half_bar_compact_blocks(.{
                    .log_blocks_count = @intCast(manifest_log.log_block_checksums.count),
                    .tables_count = manifest_log.table_extents.count(),
                }),
                // Never compact closed blocks. (They haven't even been written yet.)
                manifest_log.log_block_checksums.count - manifest_log.blocks_closed,
            );
            assert(manifest_log.compact_blocks.? <= manifest_log.pace.half_bar_compact_blocks_max);

            manifest_log.grid_reservation = manifest_log.grid.reserve(
                manifest_log.compact_blocks.? +
                    manifest_log.pace.half_bar_append_blocks_max,
            );

            manifest_log.read_callback = callback;
            manifest_log.flush(compact_next_block);
        }

        fn compact_tick_callback(next_tick: *Grid.NextTick) void {
            const manifest_log: *ManifestLog = @alignCast(@fieldParentPtr("next_tick", next_tick));
            assert(manifest_log.write_callback == null);
            assert(manifest_log.grid_reservation == null);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.blocks.count == 0);
            assert(manifest_log.entry_count == 0);
            assert(manifest_log.compact_blocks == null);

            manifest_log.grid.trace.stop(.compact_manifest, .{});

            const callback = manifest_log.read_callback.?;
            manifest_log.read_callback = null;
            callback(manifest_log);
        }

        fn compact_next_block(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.read_callback != null);
            assert(manifest_log.grid_reservation != null);

            const compact_blocks = manifest_log.compact_blocks.?;
            if (compact_blocks == 0) {
                manifest_log.compact_done_callback();
            } else {
                const oldest_checksum = manifest_log.log_block_checksums.head().?;
                const oldest_address = manifest_log.log_block_addresses.head().?;
                assert(oldest_address > 0);

                manifest_log.compact_blocks.? -= 1;
                manifest_log.reading = true;
                manifest_log.grid.read_block(
                    .{ .from_local_or_global_storage = compact_read_block_callback },
                    &manifest_log.read,
                    oldest_address,
                    oldest_checksum,
                    .{ .cache_read = true, .cache_write = true },
                );
            }
        }

        fn compact_read_block_callback(read: *Grid.Read, block: BlockPtrConst) void {
            const manifest_log: *ManifestLog = @fieldParentPtr("read", read);
            assert(manifest_log.opened);
            assert(manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.read_callback != null);
            assert(manifest_log.grid_reservation != null);

            const oldest_checksum = manifest_log.log_block_checksums.pop().?;
            const oldest_address = manifest_log.log_block_addresses.pop().?;
            verify_block(block, oldest_checksum, oldest_address);

            const block_schema = schema.ManifestNode.from(block);
            assert(block_schema.entry_count > 0);
            assert(block_schema.entry_count <= schema.ManifestNode.entry_count_max);

            var frees: u32 = 0;
            for (
                block_schema.tables_const(block),
                0..block_schema.entry_count,
            ) |*table, entry_index| {
                const entry: u32 = @intCast(entry_index);
                switch (table.label.event) {
                    .reserved => unreachable,
                    // Append the table, updating the table extent:
                    .insert,
                    .update,
                    => {
                        // Update the extent if the table is the latest version.
                        // We must iterate entries in forward order to drop the extent here.
                        // Otherwise, stale versions earlier in the block may reappear.
                        if (std.meta.eql(
                            manifest_log.table_extents.get(table.address),
                            .{ .block = oldest_address, .entry = entry },
                        )) {
                            // Append the table, updating the table extent:
                            manifest_log.append_internal(table);
                        } else {
                            // Either:
                            // - This is not the latest insert for this table, so it can be dropped.
                            // - The table was removed some time after this insert.
                            frees += 1;
                        }
                    },
                    // Since we compact oldest blocks first, we know that we have already
                    // compacted all inserts that were eclipsed by this remove, so this remove
                    // can now be safely dropped.
                    .remove => frees += 1,
                }
            }

            log.debug("{}: compacted: checksum={} address={} free={}/{}", .{
                manifest_log.superblock.replica_index.?,
                oldest_checksum,
                oldest_address,
                frees,
                block_schema.entry_count,
            });

            // Blocks are compacted in sequence – not skipped, even if no entries will be freed.
            // (That should be rare though, since blocks are large.)
            // This is necessary to update the block's "previous block" pointer in the header.
            maybe(frees == 0);
            assert(manifest_log.blocks_closed <= manifest_log.pace.half_bar_compact_blocks_max);

            manifest_log.grid.release(&.{oldest_address});
            manifest_log.reading = false;

            manifest_log.compact_next_block();
        }

        fn compact_done_callback(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.blocks_closed <= manifest_log.pace.half_bar_compact_blocks_max);
            assert(manifest_log.read_callback != null);
            assert(manifest_log.grid_reservation != null);
            assert(manifest_log.compact_blocks.? == 0);

            manifest_log.grid.trace.stop(.compact_manifest, .{});

            const callback = manifest_log.read_callback.?;
            manifest_log.read_callback = null;
            manifest_log.compact_blocks = null;

            callback(manifest_log);
        }

        pub fn compact_end(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.read_callback == null);
            assert(manifest_log.write_callback == null);

            if (manifest_log.grid_reservation) |grid_reservation| {
                manifest_log.grid.forfeit(grid_reservation);
                manifest_log.grid_reservation = null;
            } else {
                // Compaction was skipped for this half-bar.
                assert(manifest_log.entry_count == 0);
                assert(manifest_log.blocks.count == 0);
                assert(manifest_log.blocks_closed == 0);
            }
        }

        pub fn checkpoint(manifest_log: *ManifestLog, callback: Callback) void {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.write_callback == null);
            assert(manifest_log.grid_reservation == null);

            if (manifest_log.entry_count > 0) {
                manifest_log.close_block();
                assert(manifest_log.entry_count == 0);
                assert(manifest_log.blocks_closed > 0);
            }
            assert(manifest_log.blocks_closed == manifest_log.blocks.count);

            manifest_log.flush(callback);
        }

        pub fn checkpoint_references(
            manifest_log: *const ManifestLog,
        ) vsr.SuperBlockManifestReferences {
            assert(manifest_log.opened);
            assert(!manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.write_callback == null);
            assert(manifest_log.grid_reservation == null);
            assert(manifest_log.log_block_checksums.count ==
                manifest_log.log_block_addresses.count);
            assert(manifest_log.blocks.count == 0);
            assert(manifest_log.blocks_closed == 0);
            assert(manifest_log.entry_count == 0);

            if (manifest_log.log_block_addresses.count == 0) {
                return std.mem.zeroes(vsr.SuperBlockManifestReferences);
            } else {
                return .{
                    .oldest_checksum = manifest_log.log_block_checksums.head().?,
                    .oldest_address = manifest_log.log_block_addresses.head().?,
                    .newest_checksum = manifest_log.log_block_checksums.tail().?,
                    .newest_address = manifest_log.log_block_addresses.tail().?,
                    .block_count = @intCast(manifest_log.log_block_addresses.count),
                };
            }
        }

        fn acquire_block(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            maybe(manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.entry_count == 0);
            assert(manifest_log.log_block_checksums.count ==
                manifest_log.log_block_addresses.count);
            assert(manifest_log.blocks.count == manifest_log.blocks_closed);
            assert(!manifest_log.blocks.full());

            manifest_log.blocks.advance_tail();

            const block: BlockPtr = manifest_log.blocks.tail().?;
            // The ManifestLog acquires block addresses eagerly here, rather than deferring until
            // close_block(). This is because the open block's address must be inserted into
            // `table_extents` at the same time the entry is appended to the open block.
            const block_address = manifest_log.grid.acquire(manifest_log.grid_reservation.?);

            const header = mem.bytesAsValue(vsr.Header.Block, block[0..@sizeOf(vsr.Header)]);
            header.* = .{
                .cluster = manifest_log.superblock.working.cluster,
                .address = block_address,
                .snapshot = 0, // TODO(snapshots): Set this properly; it is useful for debugging.
                .size = undefined,
                .command = .block,
                .release = manifest_log.superblock.working.vsr_state.checkpoint.release,
                .metadata_bytes = undefined, // Set by close_block().
                .block_type = .manifest,
            };
        }

        fn close_block(manifest_log: *ManifestLog) void {
            assert(manifest_log.opened);
            maybe(manifest_log.reading);
            assert(!manifest_log.writing);
            assert(manifest_log.blocks.count == manifest_log.blocks_closed + 1);
            assert(manifest_log.log_block_checksums.count <
                manifest_log.log_block_checksums.buffer.len);

            const block: BlockPtr = manifest_log.blocks.tail().?;
            const entry_count = manifest_log.entry_count;
            assert(entry_count > 0);
            assert(entry_count <= schema.ManifestNode.entry_count_max);

            const block_schema = schema.ManifestNode{ .entry_count = entry_count };
            const header = mem.bytesAsValue(vsr.Header.Block, block[0..@sizeOf(vsr.Header)]);
            assert(header.cluster == manifest_log.superblock.working.cluster);
            assert(header.command == .block);
            assert(header.address > 0);
            header.size = block_schema.size();

            const newest_checksum = manifest_log.log_block_checksums.tail() orelse 0;
            const newest_address = manifest_log.log_block_addresses.tail() orelse 0;
            header.metadata_bytes = @bitCast(schema.ManifestNode.Metadata{
                .previous_manifest_block_checksum = newest_checksum,
                .previous_manifest_block_address = newest_address,
                .entry_count = entry_count,
            });

            // Zero padding:
            @memset(block[header.size..], 0);

            header.set_checksum_body(block[@sizeOf(vsr.Header)..header.size]);
            header.set_checksum();
            verify_block(block, null, null);

            manifest_log.log_block_checksums.push_assume_capacity(header.checksum);
            manifest_log.log_block_addresses.push_assume_capacity(header.address);

            log.debug("{}: close_block: checksum={} address={} entries={}/{}", .{
                manifest_log.superblock.replica_index.?,
                header.checksum,
                header.address,
                entry_count,
                schema.ManifestNode.entry_count_max,
            });

            manifest_log.blocks_closed += 1;
            manifest_log.entry_count = 0;
            assert(manifest_log.blocks.count == manifest_log.blocks_closed);
        }

        fn verify_block(block: BlockPtrConst, checksum: ?u128, address: ?u64) void {
            if (constants.verify) {
                const frame = std.mem.bytesAsValue(vsr.Header, block[0..@sizeOf(vsr.Header)]);
                assert(frame.valid_checksum());
                assert(frame.valid_checksum_body(block[@sizeOf(vsr.Header)..frame.size]));
            }

            const header = schema.header_from_block(block);
            assert(header.block_type == .manifest);

            assert(address == null or header.address == address.?);
            assert(checksum == null or header.checksum == checksum.?);

            const block_schema = schema.ManifestNode.from(block);
            assert(block_schema.entry_count > 0);
            assert(block_schema.entry_count <= schema.ManifestNode.entry_count_max);
        }
    };
}

/// The goals of manifest log compaction are (in no particular order):
///
/// 1. Free enough manifest blocks such that there are always enough free slots in the manifest
///    log checksums/addresses ring buffers to accommodate the appends by table compaction.
/// 2. Shrink the manifest log: A smaller manifest means that fewer blocks need to be replayed
///    during recovery, or repaired during state sync.
/// 3. Don't shrink the manifest too much: The more manifest compaction work is deferred, the more
///    "efficient" compaction is. Put another way: deferring manifest compaction means that more
///    entries are freed per block compacted.
/// 4. Spread compaction work evenly between half-bars, to avoid latency spikes.
///
/// To address goal 1, we must (on average) "remove" as many blocks from the manifest log as we add.
/// But when we compact a block, only a subset of its entries can be freed/dropped – the remainder
/// must be re-appended to the manifest log.
///
/// The upper-bound number of manifest blocks is related to the rate at which we compact blocks.
/// Put simply, the more compaction work we do, the smaller the upper bound.
///
///
/// To reason about this relation mathematically, and compute the upper-bound number of manifest
/// blocks in terms of the compaction rate:
///
/// - Let `A` be the maximum number of manifest blocks that may be created by any single half-bar
///   due to appends via table compaction. (In other words, `A` does not count manifest compaction.)
/// - Let `T` be the minimum number of manifest blocks to hold `table_count_max` tables (inserts).
/// - Let `C` be the maximum number of manifest blocks to compact (i.e. read) during any half-bar.
///   - In the worst case, compacting a block frees no entries.
///   - (Then `C` is also the worst-case number of manifest blocks *written* due to manifest
///     compaction during each half-bar.)
///
/// Suppose that at a certain point in time `t₀`, there are `M₀` manifest blocks total.
///
/// If we compact at least `C` manifest blocks for each of `⌈M₀/C⌉` half-bars, then any of the
/// initial `M₀` manifest blocks that required compaction at time `t₀` have been compacted.
/// In the worst case (where all of those `M₀` blocks were full of live entries) we now have as
/// many as `M₁ = min(M₀,T) + A×⌈M₀/C⌉` manifest blocks:
///
///   - `min(M₀,T)`: After compacting the original `M₀` blocks, we may produce as many as `M₀`
///     blocks (if no entries were freed). But if there are more than `T` blocks then some *must* be
///     dropped, since `T` is the upper-bound of a fully-compacted manifest.
///   - `⌈M₀/C⌉` is the number of half-bars that it takes to compact the initial `M₀` manifest
///     blocks.
///   - `A×⌈M₀/C⌉` is the maximum number of manifest blocks produced by table compaction while
///     compacting the original `M₀` manifest blocks.
///
/// If we cycle again, starting with `M₁` manifest blocks this time, then at the end of the cycle
/// there are at most `M₂ = min(M₁,T) + A×⌈M₁/C⌉` manifest blocks.
///
/// To generalize, at the beginning of any cycle `c`, the maximum number of manifest blocks
/// (`MC(c)`) is:
///
///   MC(c) = min(T, MC(c-1)) + A×⌈MC(c-1)/C⌉
///
///
/// However, *within* a cycle the manifest block count may "burst" temporarily beyond this limit.
/// We compact chronologically. If the blocks early in the manifest have no/few free entries, we
/// must still compact them anyway, shifting their entries from the prefix of the log to its suffix.
/// During that time, the table-compact appends still occur, so the net manifest log size grows.
///
/// The lower-bound for the number of blocks freed (`F(k)`) in terms of the number of blocks
/// compacted (`k`) is:
///
///   F(k) ≥ max(0, k - (T + 1))
///
/// In other words:
/// - After compacting `T` or fewer blocks, we may not have freed any whole blocks.
/// - After compacting `T+1` blocks, we must have freed at least 1 whole block.
/// - After compacting `T+2` blocks, we must have freed at least 2 whole blocks.
/// - Etc.
///
/// Then the upper-bound number of manifest blocks (`MB(b)`) at any half-bar boundary (`b`) is:
///
///   MB(b) = min(T, MB(b-1)) + A×⌈M(b-1)/C⌉ + A×⌈(T+1)/C⌉
///
/// As `b` approaches infinity, this recurrence relation converges (iff `C > A`) to the absolute
/// upper-bound number of manifest blocks.
///
/// As `C` increases (relative to `A`), the manifest block upper-bound decreases, but the amount of
/// compaction work performed increases.
///
/// If, for any half-bar that the manifest log contains at least `MC(∞)` blocks we compact at least
/// `C` blocks, then the total size of the manifest log will never exceed `MB(∞)` blocks.
///
/// NOTE: Both the algorithm above and the implementation below make several simplifications:
///
///   - The calculation is performed at the granularity of blocks, not entries. In particular, this
///     means that "A" might in truth be fractional, but we would round up. For example, if "A" is
///     2.1, for the purposes of the upper-bound it is 3. Because `C` is computed (below) as
///     "A + compact_extra_blocks", the result is that we perform more compaction (relative to
///     appends) than the block-granular constants indicate.
///     As a result, we overestimate the upper-bound (or, equivalently, perform compaction more
///     quickly than strictly necessary).
///   - The calculation does *not* consider the "padding" appends in to a partial block written
///     during a checkpoint. This oversight is masked because "A" is overestimated (see previous
///     bullet).
///
pub const Pace = struct {
    /// "A":
    /// The maximum number of manifest blocks appended during a single half-bar by table appends.
    ///
    /// This counts:
    /// - Input tables are updated in the manifest (snapshot_max is reduced).
    /// - Input tables are removed from the manifest (if not held by a persistent snapshot).
    /// - Output tables are inserted into the manifest.
    /// This does *not* count:
    /// - Manifest log compaction.
    /// - Releasing persistent snapshots.
    half_bar_append_blocks_max: u32,

    /// "C":
    /// The maximum number of manifest blocks to compact (i.e. read) during a single half-bar.
    half_bar_compact_blocks_max: u32,

    /// "T":
    /// The maximum number of blocks in a fully-compacted manifest.
    /// (Exposed by the struct only for the purpose of logging.)
    log_blocks_full_max: u64,

    /// "limit of MC(c) as c approaches ∞"
    log_blocks_cycle_max: u64,
    /// "limit of MB(b) as b approaches ∞"
    log_blocks_max: u64,

    tables_max: u32,

    comptime {
        const log_pace = false;
        if (log_pace) {
            const pace = Pace.init(.{
                .tree_count = 24,
                .tables_max = 2_300_000,
                .compact_extra_blocks = constants.lsm_manifest_compact_extra_blocks,
            });

            for (std.meta.fields(Pace)) |pace_field| {
                @compileLog(std.fmt.comptimePrint("ManifestLog.Pace.{s} = {d}", .{
                    pace_field.name,
                    @field(pace, pace_field.name),
                }));
            }
        }
    }

    pub fn init(options: struct {
        tree_count: u32,
        tables_max: u32,
        compact_extra_blocks: u32,
    }) Pace {
        assert(options.tree_count > 0);
        assert(options.tables_max > 0);
        assert(options.tables_max > options.tree_count);
        assert(options.compact_extra_blocks > 0);

        const block_entries_max = schema.ManifestNode.entry_count_max;

        const half_bar_append_entries_max = options.tree_count *
            stdx.div_ceil(constants.lsm_levels, 2) * // Maximum number of compactions/half-bar.
            (compaction.compaction_tables_input_max + // Update snapshot_max.
            compaction.compaction_tables_input_max + // Remove.
            compaction.compaction_tables_output_max); // Insert.

        // "A":
        const half_bar_append_blocks_max =
            stdx.div_ceil(half_bar_append_entries_max, block_entries_max);

        const half_bar_compact_blocks_extra = options.compact_extra_blocks;
        assert(half_bar_compact_blocks_extra > 0);

        // "C":
        const half_bar_compact_blocks_max =
            half_bar_append_blocks_max + half_bar_compact_blocks_extra;
        assert(half_bar_compact_blocks_max > half_bar_append_blocks_max);

        // "T":
        const log_blocks_full_max = stdx.div_ceil(options.tables_max, block_entries_max);
        assert(log_blocks_full_max > 0);

        // "limit of MC(c) as c approaches ∞":
        // Working out this recurrence relation's limit with a closed-form solution is complicated.
        // Just compute the limit iteratively instead. (1024 is an arbitrary safety counter.)
        var log_blocks_before: u32 = 0;
        const log_blocks_cycle_max = for (0..1024) |_| {
            const log_blocks_after =
                log_blocks_full_max +
                half_bar_append_blocks_max *
                stdx.div_ceil(log_blocks_before, half_bar_compact_blocks_max);

            if (log_blocks_before == log_blocks_after) {
                break log_blocks_after;
            }
            log_blocks_before = log_blocks_after;
        } else {
            // If the value does not converge within the given number of steps,
            // constants.lsm_manifest_compact_blocks_extra should probably be raised.
            @panic("ManifestLog.Pace.log_blocks_cycle_max: no convergence");
        };

        const log_blocks_burst_max = half_bar_append_blocks_max *
            stdx.div_ceil(log_blocks_full_max + 1, half_bar_compact_blocks_max);

        // "limit of MB(b) as b approaches ∞":
        const log_blocks_max = log_blocks_cycle_max + log_blocks_burst_max;

        assert(log_blocks_cycle_max > log_blocks_full_max);
        assert(log_blocks_cycle_max < log_blocks_max);

        return .{
            .half_bar_append_blocks_max = half_bar_append_blocks_max,
            .half_bar_compact_blocks_max = half_bar_compact_blocks_max,
            .log_blocks_full_max = log_blocks_full_max,
            .log_blocks_max = log_blocks_max,
            .log_blocks_cycle_max = log_blocks_cycle_max,
            .tables_max = options.tables_max,
        };
    }

    fn half_bar_compact_blocks(pace: Pace, options: struct {
        /// The number of manifest blocks that *currently* exist.
        log_blocks_count: u32,
        /// The number of live tables.
        tables_count: u32,
    }) u32 {
        assert(options.tables_count <= pace.tables_max);

        // Pretend we have an extra half_bar_append_blocks_max blocks so that we always switch to
        // the maximum compaction rate before we exceed the cycle-max.
        if (pace.log_blocks_cycle_max <=
            options.log_blocks_count + pace.half_bar_append_blocks_max)
        {
            return pace.half_bar_compact_blocks_max;
        }

        // We have enough free manifest blocks that we could go a whole "cycle" without
        // compacting any. It doesn't strictly matter how much compaction we do in this case, so
        // just try to pace the work evenly, maintaining a constant load factor with respect to
        // the cycle-max.

        // Our "target" block count extrapolates a log block count from our table count and the
        // log's maximum load factor.
        const log_blocks_target = @max(1, @divFloor(
            pace.log_blocks_cycle_max * options.tables_count,
            pace.tables_max,
        ));

        return @min(
            pace.half_bar_compact_blocks_max,
            @divFloor(
                pace.half_bar_compact_blocks_max * options.log_blocks_count,
                log_blocks_target,
            ),
        );
    }
};
