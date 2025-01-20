//! Track corrupt/missing grid blocks.
//!
//! - The GridBlocksMissing is LSM-aware: it can repair entire tables.
//! - The GridBlocksMissing is shared by all Trees.
//! - The GridBlocksMissing is "coherent" – that is, all of the blocks in the queue belong in the
//!   replica's current checkpoint:
//!   - The GridBlocksMissing will not repair freed blocks.
//!   - The GridBlocksMissing will repair released blocks, until they are freed at the checkpoint.
//! - GridBlocksMissing.enqueue_table() is called immediately after superblock sync.
//! - GridBlocksMissing.enqueue_block() is called by the grid when non-repair reads encounter
//!   corrupt blocks.
const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const schema = @import("../lsm/schema.zig");
const vsr = @import("../vsr.zig");

const FIFOType = @import("../fifo.zig").FIFOType;
const BlockPtrConst = *align(constants.sector_size) const [constants.block_size]u8;

pub const GridBlocksMissing = struct {
    const TableDataBlocksSet = std.StaticBitSet(constants.lsm_table_data_blocks_max);

    /// A block is removed from the collection when:
    /// - the block's write completes, or
    /// - the block is released and the release is checkpointed, or
    /// - the grid is canceled.
    ///
    /// The map is keyed by block address.
    const FaultyBlocks = std.AutoArrayHashMapUnmanaged(u64, FaultyBlock);

    const FaultyBlock = struct {
        checksum: u128,
        progress: FaultProgress,
        /// Transitions:
        /// - Initial state is `waiting`.
        /// - `waiting → writing` when the block arrives and begins to repair.
        /// - `writing → aborting` when checkpoint becomes durable and the (writing) block is to be
        ///    freed.
        state: enum { waiting, writing, aborting } = .waiting,
    };

    const FaultProgress = union(enum) {
        /// Repair a single block.
        block,
        /// Repair the table and all of its content. Awaiting table index block.
        table_index: TableIndex,
        /// Repair the table and all of its content. Awaiting table data blocks.
        table_data: TableData,

        const TableIndex = struct { table: *RepairTable };
        const TableData = struct { table: *RepairTable, index: u32 };
    };

    pub const RepairTable = struct {
        index_address: u64,
        index_checksum: u128,
        /// Invariants:
        /// - data_blocks_received.count < table_blocks_total
        /// TODO(Congestion control): This bitset is currently used only for extra validation.
        /// Eventually we should request tables using this + EWAH encoding, instead of
        /// block-by-block.
        data_blocks_received: TableDataBlocksSet = TableDataBlocksSet.initEmpty(),
        /// This count includes the index block.
        /// Invariants:
        /// - table_blocks_written ≤ table_blocks_total
        table_blocks_written: u32 = 0,
        /// When null, the table is awaiting an index block.
        /// When non-null, the table is awaiting data blocks.
        /// This count includes the index block.
        table_blocks_total: ?u32 = null,
        /// "next" belongs to the `faulty_tables`/`faulty_tables_free` FIFOs.
        next: ?*RepairTable = null,
    };

    pub const Options = struct {
        /// Lower-bound for the limit of concurrent enqueue_block()'s available.
        blocks_max: usize,
        /// Maximum number of concurrent enqueue_table()'s.
        tables_max: usize,
    };

    options: Options,

    /// Invariants:
    /// - For every block address in faulty_blocks, ¬free_set.is_free(address).
    faulty_blocks: FaultyBlocks,
    /// Index within `faulty_blocks`, used to cycle through block-repair requests.
    ///
    /// Invariants:
    /// - faulty_blocks.count() > 0 implies faulty_blocks_repair_index < faulty_blocks.count()
    /// - faulty_blocks.count() = 0 implies faulty_blocks_repair_index = faulty_blocks.count()
    faulty_blocks_repair_index: usize = 0,

    /// Invariants:
    /// - enqueued_blocks_table + enqueued_blocks_single = faulty_blocks.count()
    /// - enqueued_blocks_table ≤ options.tables_max * lsm_table_content_blocks_max
    enqueued_blocks_single: usize = 0,
    enqueued_blocks_table: usize = 0,

    /// Invariants:
    /// - For every index address in faulty_tables: ¬free_set.is_free(address).
    faulty_tables: FIFOType(RepairTable) = .{ .name = "grid_missing_blocks_tables" },
    faulty_tables_free: FIFOType(RepairTable) = .{ .name = "grid_missing_blocks_tables_free" },

    checkpoint_durable: ?struct {
        /// The number of faulty_blocks with state=aborting.
        aborting: usize,
    } = null,

    pub fn init(
        allocator: std.mem.Allocator,
        options: Options,
    ) error{OutOfMemory}!GridBlocksMissing {
        var faulty_blocks = FaultyBlocks{};
        errdefer faulty_blocks.deinit(allocator);

        try faulty_blocks.ensureTotalCapacity(
            allocator,
            options.blocks_max + options.tables_max * constants.lsm_table_data_blocks_max,
        );

        return GridBlocksMissing{
            .options = options,
            .faulty_blocks = faulty_blocks,
        };
    }

    pub fn deinit(queue: *GridBlocksMissing, allocator: std.mem.Allocator) void {
        queue.faulty_blocks.deinit(allocator);

        queue.* = undefined;
    }

    /// When the queue wants more blocks than fit in a single request message, successive calls
    /// to this function cycle through the pending BlockRequests.
    pub fn next_batch_of_block_requests(
        queue: *GridBlocksMissing,
        requests: []vsr.BlockRequest,
    ) usize {
        assert(requests.len > 0);

        const faults_total = queue.faulty_blocks.count();
        if (faults_total == 0) return 0;
        assert(queue.faulty_blocks_repair_index < faults_total);

        const fault_addresses = queue.faulty_blocks.entries.items(.key);
        const fault_data = queue.faulty_blocks.entries.items(.value);

        var requests_count: usize = 0;
        var fault_offset: usize = 0;
        while (fault_offset < faults_total) : (fault_offset += 1) {
            const fault_index =
                (queue.faulty_blocks_repair_index + fault_offset) % faults_total;

            switch (fault_data[fault_index].state) {
                .waiting => {
                    requests[requests_count] = .{
                        .block_address = fault_addresses[fault_index],
                        .block_checksum = fault_data[fault_index].checksum,
                    };
                    requests_count += 1;

                    if (requests_count == requests.len) break;
                },
                .writing => {},
                .aborting => assert(queue.checkpoint_durable.?.aborting > 0),
            }
        }

        // +1 so we start from the next faulty block in the subsequent function invocation, thereby
        // cycling through the faulty blocks in the queue.
        queue.faulty_blocks_repair_index =
            (queue.faulty_blocks_repair_index + fault_offset + 1) % faults_total;

        assert(requests_count <= requests.len);
        assert(requests_count <= faults_total);
        return requests_count;
    }

    pub fn reclaim_table(queue: *GridBlocksMissing) ?*RepairTable {
        return queue.faulty_tables_free.pop();
    }

    /// Count the number of *non-table* block repairs available.
    pub fn enqueue_blocks_available(queue: *const GridBlocksMissing) usize {
        assert(queue.faulty_tables.count <= queue.options.tables_max);
        assert(queue.faulty_blocks.count() ==
            queue.enqueued_blocks_single + queue.enqueued_blocks_table);
        assert(queue.enqueued_blocks_table <=
            queue.options.tables_max * constants.lsm_table_data_blocks_max);

        const faulty_blocks_free =
            queue.faulty_blocks.capacity() -
            queue.enqueued_blocks_single -
            queue.options.tables_max * constants.lsm_table_data_blocks_max;
        return faulty_blocks_free;
    }

    /// Queue a faulty block to request from the cluster and repair.
    pub fn enqueue_block(queue: *GridBlocksMissing, address: u64, checksum: u128) void {
        assert(queue.enqueue_blocks_available() > 0);
        assert(queue.faulty_tables.count <= queue.options.tables_max);
        assert(queue.faulty_blocks.count() ==
            queue.enqueued_blocks_single + queue.enqueued_blocks_table);

        const enqueue = queue.enqueue_faulty_block(address, checksum, .block);
        assert(enqueue == .insert or enqueue == .duplicate);
    }

    pub fn enqueue_table(
        queue: *GridBlocksMissing,
        table: *RepairTable,
        address: u64,
        checksum: u128,
    ) enum { insert, duplicate } {
        assert(queue.faulty_tables.count < queue.options.tables_max);
        assert(queue.faulty_blocks.count() ==
            queue.enqueued_blocks_single + queue.enqueued_blocks_table);

        var tables = queue.faulty_tables.peek();
        while (tables) |queue_table| : (tables = queue_table.next) {
            assert(queue_table != table);

            if (queue_table.index_address == address) {
                // The ForestTableIterator does not repeat tables *except* when the table was first
                // encountered at level L, and then it was re-encountered having moved to a deeper
                // level (L+1, etc).
                assert(queue_table.index_checksum == checksum);
                return .duplicate;
            }
        }

        table.* = .{
            .index_address = address,
            .index_checksum = checksum,
        };
        queue.faulty_tables.push(table);

        const enqueue =
            queue.enqueue_faulty_block(address, checksum, .{ .table_index = .{ .table = table } });
        assert(enqueue == .insert or enqueue == .replace);

        return .insert;
    }

    fn enqueue_faulty_block(
        queue: *GridBlocksMissing,
        address: u64,
        checksum: u128,
        progress: FaultProgress,
    ) union(enum) {
        insert,
        replace: *FaultyBlock,
        duplicate,
    } {
        assert(queue.faulty_tables.count <= queue.options.tables_max);
        assert(queue.faulty_blocks.count() ==
            queue.enqueued_blocks_single + queue.enqueued_blocks_table);

        defer {
            assert(queue.faulty_blocks.count() ==
                queue.enqueued_blocks_single + queue.enqueued_blocks_table);
        }

        const fault_result = queue.faulty_blocks.getOrPutAssumeCapacity(address);
        if (fault_result.found_existing) {
            const fault = fault_result.value_ptr;
            assert(fault.checksum == checksum);
            assert(fault.state != .aborting);

            switch (progress) {
                .block => return .duplicate,
                .table_index,
                .table_data,
                => {
                    // The data block may already have been queued by either the scrubber or a
                    // commit/compaction grid read.
                    assert(fault.progress == .block);

                    queue.enqueued_blocks_single -= 1;
                    queue.enqueued_blocks_table += 1;
                    fault.progress = progress;
                    return .{ .replace = fault };
                },
            }
        } else {
            switch (progress) {
                .block => queue.enqueued_blocks_single += 1,
                .table_index => queue.enqueued_blocks_table += 1,
                .table_data => queue.enqueued_blocks_table += 1,
            }

            fault_result.value_ptr.* = .{
                .checksum = checksum,
                .progress = progress,
            };
            return .insert;
        }
    }

    pub fn repair_waiting(queue: *const GridBlocksMissing, address: u64, checksum: u128) bool {
        const fault_index = queue.faulty_blocks.getIndex(address) orelse return false;
        const fault = &queue.faulty_blocks.entries.items(.value)[fault_index];
        return fault.checksum == checksum and fault.state == .waiting;
    }

    pub fn repair_commence(queue: *const GridBlocksMissing, address: u64, checksum: u128) void {
        assert(queue.repair_waiting(address, checksum));

        const fault_index = queue.faulty_blocks.getIndex(address).?;
        const fault = &queue.faulty_blocks.entries.items(.value)[fault_index];
        assert(fault.checksum == checksum);
        assert(fault.state == .waiting);

        if (fault.progress == .table_data) {
            const progress = &fault.progress.table_data;
            assert(progress.table.table_blocks_written < progress.table.table_blocks_total.?);
            assert(!progress.table.data_blocks_received.isSet(progress.index));

            progress.table.data_blocks_received.set(progress.index);
        }

        fault.state = .writing;
    }

    pub fn repair_complete(queue: *GridBlocksMissing, block: BlockPtrConst) void {
        const block_header = schema.header_from_block(block);
        const fault_index = queue.faulty_blocks.getIndex(block_header.address).?;
        const fault_address = queue.faulty_blocks.entries.items(.key)[fault_index];
        const fault: FaultyBlock = queue.faulty_blocks.entries.items(.value)[fault_index];
        assert(fault_address == block_header.address);
        assert(fault.checksum == block_header.checksum);
        assert(fault.state == .aborting or fault.state == .writing);

        queue.release_fault(fault_index);

        if (fault.state == .aborting) {
            queue.checkpoint_durable.?.aborting -= 1;
            return;
        }

        switch (fault.progress) {
            .block => {},
            .table_index => |progress| {
                assert(progress.table.data_blocks_received.count() == 0);

                // The reason that the data blocks are queued here (when the write ends) rather
                // than when the write begins is so that a `enqueue_block()` can be converted to a
                // `enqueue_table()` after the former's write is already in progress.
                queue.enqueue_table_data(fault.progress.table_index.table, block);
            },
            .table_data => |progress| {
                assert(progress.table.data_blocks_received.isSet(progress.index));
            },
        }

        if (switch (fault.progress) {
            .block => null,
            .table_index => |progress| progress.table,
            .table_data => |progress| progress.table,
        }) |table| {
            assert(table.table_blocks_total != null); // We already received the index block.
            assert(table.table_blocks_written < table.table_blocks_total.?);
            assert(table.data_blocks_received.count() <= table.table_blocks_total.? - 1);

            table.table_blocks_written += 1;
            if (table.table_blocks_written == table.table_blocks_total.?) {
                queue.faulty_tables.remove(table);
                queue.faulty_tables_free.push(table);
            }
        }
    }

    fn enqueue_table_data(
        queue: *GridBlocksMissing,
        table: *RepairTable,
        index_block_data: BlockPtrConst,
    ) void {
        assert(queue.faulty_blocks.count() ==
            queue.enqueued_blocks_single + queue.enqueued_blocks_table);
        assert(table.table_blocks_total == null);
        assert(table.table_blocks_written == 0);
        assert(table.data_blocks_received.count() == 0);

        const index_schema = schema.TableIndex.from(index_block_data);
        const index_block_header = schema.header_from_block(index_block_data);
        assert(index_block_header.address == table.index_address);
        assert(index_block_header.checksum == table.index_checksum);
        assert(index_block_header.block_type == .index);

        table.table_blocks_total = index_schema.data_blocks_used(index_block_data) + 1;

        for (
            index_schema.data_addresses_used(index_block_data),
            index_schema.data_checksums_used(index_block_data),
            0..,
        ) |address, checksum, index| {
            const enqueue = queue.enqueue_faulty_block(
                address,
                checksum.value,
                .{ .table_data = .{ .table = table, .index = @intCast(index) } },
            );

            if (enqueue == .replace) {
                if (enqueue.replace.state == .writing) {
                    table.data_blocks_received.set(index);
                }
            } else {
                assert(enqueue == .insert);
            }
        }
    }

    fn release_fault(queue: *GridBlocksMissing, fault_index: usize) void {
        assert(queue.faulty_blocks_repair_index < queue.faulty_blocks.count());

        switch (queue.faulty_blocks.entries.items(.value)[fault_index].progress) {
            .block => queue.enqueued_blocks_single -= 1,
            .table_index => queue.enqueued_blocks_table -= 1,
            .table_data => queue.enqueued_blocks_table -= 1,
        }

        queue.faulty_blocks.swapRemoveAt(fault_index);

        if (queue.faulty_blocks_repair_index == queue.faulty_blocks.count()) {
            queue.faulty_blocks_repair_index = 0;
        }
    }

    pub fn cancel(queue: *GridBlocksMissing) void {
        queue.faulty_blocks.clearRetainingCapacity();
        while (queue.faulty_tables.pop()) |table| {
            queue.faulty_tables_free.push(table);
        }

        queue.* = .{
            .options = queue.options,
            .faulty_blocks = queue.faulty_blocks,
            .faulty_tables_free = queue.faulty_tables_free,
        };
    }

    /// Aborts queued repairs to blocks to be freed, now that the current checkpoint is durable.
    pub fn checkpoint_durable_commence(
        queue: *GridBlocksMissing,
        free_set: *const vsr.FreeSet,
    ) void {
        assert(queue.checkpoint_durable == null);
        assert(queue.faulty_blocks.count() ==
            queue.enqueued_blocks_single + queue.enqueued_blocks_table);

        var aborting: usize = 0;

        var faulty_blocks = queue.faulty_blocks.iterator();
        while (faulty_blocks.next()) |fault_entry| {
            const fault_address = fault_entry.key_ptr.*;
            assert(!free_set.is_free(fault_address));
            assert(fault_entry.value_ptr.state != .aborting);
            // Use `to_be_freed_at_checkpoint_durability` instead of `is_released`; the latter also
            // contains the blocks that will be released when the *next* checkpoint becomes durable.
            if (free_set.to_be_freed_at_checkpoint_durability(fault_address)) {
                switch (fault_entry.value_ptr.state) {
                    .waiting => {
                        faulty_blocks.index -= 1;
                        faulty_blocks.len -= 1;
                        queue.release_fault(faulty_blocks.index);
                    },
                    .writing => {
                        fault_entry.value_ptr.state = .aborting;
                        aborting += 1;
                    },
                    .aborting => unreachable,
                }
            }
        }

        var tables: FIFOType(RepairTable) = .{ .name = queue.faulty_tables.name };
        while (queue.faulty_tables.pop()) |table| {
            assert(!free_set.is_free(table.index_address));

            if (free_set.to_be_freed_at_checkpoint_durability(table.index_address)) {
                queue.faulty_tables_free.push(table);
            } else {
                tables.push(table);
            }
        }
        queue.faulty_tables = tables;

        queue.checkpoint_durable = .{ .aborting = aborting };
    }

    /// Returns `true` when the `state == aborting` faults for blocks to be freed at checkpoint
    /// durability have finished. (All other writes can safely complete after checkpoint durability)
    pub fn checkpoint_durable_complete(queue: *GridBlocksMissing) bool {
        assert(queue.checkpoint_durable != null);
        assert(queue.faulty_blocks.count() ==
            queue.enqueued_blocks_single + queue.enqueued_blocks_table);

        if (queue.checkpoint_durable.?.aborting == 0) {
            queue.checkpoint_durable = null;

            var faulty_blocks = queue.faulty_blocks.iterator();
            while (faulty_blocks.next()) |fault_entry| {
                assert(fault_entry.value_ptr.state != .aborting);
            }

            return true;
        } else {
            return false;
        }
    }
};
