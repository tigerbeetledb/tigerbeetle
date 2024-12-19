//! In-memory storage, with simulated faults and latency.
//!
//!
//! Fault Injection
//!
//! Storage injects faults that a fully-connected cluster can (i.e. should be able to) recover from.
//! Each zone can tolerate a different pattern of faults.
//!
//! - superblock:
//!   - One read/write fault is permitted per area (section, free set, …).
//!   - An additional fault is permitted at the target of a pending write during a crash.
//!
//! - wal_headers, wal_prepares:
//!   - Read/write faults are distributed between replicas according to ClusterFaultAtlas, to ensure
//!     that at least one replica will have a valid copy to help others repair.
//!     (See: generate_faulty_wal_areas()).
//!   - When a replica crashes, it may fault the WAL outside of ClusterFaultAtlas.
//!   - When replica_count=1, its WAL can only be corrupted by a crash, never a read/write.
//!     (When replica_count=1, there are no other replicas to assist with repair).
//!
//! - grid:
//!   - Similarly to prepares and headers, ClusterFaultAtlas ensures that at least one replica will
//!     have a block.
//!   - When replica_count≤2, grid faults are disabled.
//!
const std = @import("std");
const assert = std.debug.assert;
const panic = std.debug.panic;
const math = std.math;
const mem = std.mem;

const FIFOType = @import("../fifo.zig").FIFOType;
const IOPSType = @import("../iops.zig").IOPSType;
const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const superblock = @import("../vsr/superblock.zig");
const FreeSet = @import("../vsr/free_set.zig").FreeSet;
const schema = @import("../lsm/schema.zig");
const stdx = @import("../stdx.zig");
const maybe = stdx.maybe;
const PriorityQueue = std.PriorityQueue;
const fuzz = @import("./fuzz.zig");
const hash_log = @import("./hash_log.zig");
const GridChecker = @import("./cluster/grid_checker.zig").GridChecker;

const log = std.log.scoped(.storage);

// TODOs:
// less than a majority of replicas may have corruption
// have an option to enable/disable the following corruption types:
// bitrot
// misdirected read/write
// corrupt sector
// latent sector error
// - emulate by zeroing sector, as this is how we handle this in the real Storage implementation
// - likely that surrounding sectors also corrupt
// - likely that stuff written at the same time is also corrupt even if written to a far away sector
pub const Storage = struct {
    /// Options for fault injection during fuzz testing
    pub const Options = struct {
        /// Seed for the storage PRNG.
        seed: u64 = 0,

        /// Required when `fault_atlas` is set.
        replica_index: ?u8 = null,

        /// Minimum number of ticks it may take to read data.
        read_latency_min: u64,
        /// Average number of ticks it may take to read data. Must be >= read_latency_min.
        read_latency_mean: u64,
        /// Minimum number of ticks it may take to write data.
        write_latency_min: u64,
        /// Average number of ticks it may take to write data. Must be >= write_latency_min.
        write_latency_mean: u64,

        /// Chance out of 100 that a read will corrupt a sector, if the target memory is within
        /// a faulty area of this replica.
        read_fault_probability: u8 = 0,
        /// Chance out of 100 that a write will corrupt a sector, if the target memory is within
        /// a faulty area of this replica.
        write_fault_probability: u8 = 0,
        /// Chance out of 100 that a write will misdirect to the wrong sector, if the target memory
        /// is within a faulty area of this replica.
        write_misdirect_probability: u8 = 0,
        /// Chance out of 100 that a crash will corrupt a sector of a pending write's target,
        /// if the target memory is within a faulty area of this replica.
        crash_fault_probability: u8 = 0,

        /// Enable/disable automatic read/write faults.
        /// Does not impact crash faults or manual faults.
        fault_atlas: ?*const ClusterFaultAtlas = null,

        /// Accessed by the Grid for extra verification of grid coherence.
        grid_checker: ?*GridChecker = null,
    };

    /// See usage in Journal.write_sectors() for details.
    /// TODO: allow testing in both modes.
    pub const synchronicity: enum {
        always_synchronous,
        always_asynchronous,
    } = .always_asynchronous;

    pub const Read = struct {
        callback: *const fn (read: *Storage.Read) void,
        buffer: []u8,
        zone: vsr.Zone,
        /// Relative offset within the zone.
        offset: u64,
        /// Tick at which this read is considered "completed" and the callback should be called.
        done_at_tick: u64,
        stack_trace: StackTrace,

        fn less_than(context: void, a: *Read, b: *Read) math.Order {
            _ = context;

            return math.order(a.done_at_tick, b.done_at_tick);
        }
    };

    pub const Write = struct {
        callback: *const fn (write: *Storage.Write) void,
        buffer: []const u8,
        zone: vsr.Zone,
        /// Relative offset within the zone.
        offset: u64,
        /// Tick at which this write is considered "completed" and the callback should be called.
        done_at_tick: u64,
        stack_trace: StackTrace,

        fn less_than(context: void, a: *Write, b: *Write) math.Order {
            _ = context;

            return math.order(a.done_at_tick, b.done_at_tick);
        }
    };

    pub const NextTick = struct {
        next: ?*NextTick = null,
        source: NextTickSource,
        callback: *const fn (next_tick: *NextTick) void,
    };

    pub const NextTickSource = enum { lsm, vsr };

    /// See `Storage.overlays`.
    const overlays_count_max = 2;

    allocator: mem.Allocator,

    size: u64,
    options: Options,
    prng: std.rand.DefaultPrng,

    memory: []align(constants.sector_size) u8,
    /// Set bits correspond to sectors that have ever been written to.
    memory_written: std.DynamicBitSetUnmanaged,
    /// Set bits correspond to faulty sectors. The underlying sectors of `memory` is left clean.
    faults: std.DynamicBitSetUnmanaged,

    /// Overlays take precedence over the data in `memory`.
    ///
    /// Each misdirected write creates two overlays.
    /// When a misdirected write is triggered:
    /// - The intended target is overlaid with its old data.
    /// - The intended target's `memory` is set to the `write.buffer` data.
    /// - The mistaken target is overlaid with the `write.buffer` data.
    /// - The mistaken target's `memory` is left untouched.
    ///
    /// The reason for all of this is:
    /// - By keeping `memory` pristine, we can trivially disable both sides of the misdirected-write
    ///   fault by flipping the `faulty` flag.
    /// - By tracking the overlays separately, they can be repaired separately.
    ///
    /// Other notes:
    /// - We allow for (at most) one misdirect fault per Storage for the time being, for simplicity
    ///   and because double-faults are not covered by our fault model. This will hopefully match
    ///   physical disks – misdirected faults are an order of magnitude less frequent than bit rot,
    ///   which in turn is an order of magnitude less frequent than LSEs.
    /// - In order to keep things interesting:
    ///   - misdirections are always within the same zone,
    ///   - the entire write is misdirected (rather than only some of the sectors), and
    ///   - the misdirected write lands on a convenient offset.
    ///   Thanks to rigorous checksums, misdirections that break these rules just manifest as
    ///   corruptions, and corruption is already well-tested (see `faults`). The goal here is to
    ///   test how TigerBeetle handles well-formed but incorrectly-located data.
    overlays: IOPSType(struct { zone: vsr.Zone, offset: u64, size: u32 }, overlays_count_max) = .{},
    overlay_buffers: [overlays_count_max][]align(constants.sector_size) u8,

    /// Whether to enable faults (when false, this supersedes `faulty_wal_areas` &c).
    /// This is used to disable faults during the replica's first startup.
    faulty: bool = true,

    reads: PriorityQueue(*Storage.Read, void, Storage.Read.less_than),
    writes: PriorityQueue(*Storage.Write, void, Storage.Write.less_than),

    ticks: u64 = 0,
    next_tick_queue: FIFOType(NextTick) = .{ .name = "storage_next_tick" },

    pub fn init(allocator: mem.Allocator, size: u64, options: Storage.Options) !Storage {
        assert(size <= constants.storage_size_limit_max);
        assert(options.write_latency_mean >= options.write_latency_min);
        assert(options.read_latency_mean >= options.read_latency_min);
        assert(options.fault_atlas == null or options.replica_index != null);

        assert(options.read_fault_probability <= 100);
        assert(options.write_fault_probability <= 100);
        assert(options.write_misdirect_probability <= 100);
        assert(options.crash_fault_probability <= 100);

        const prng = std.rand.DefaultPrng.init(options.seed);
        const sector_count = @divExact(size, constants.sector_size);
        const memory = try allocator.alignedAlloc(u8, constants.sector_size, size);
        errdefer allocator.free(memory);

        var memory_written = try std.DynamicBitSetUnmanaged.initEmpty(allocator, sector_count);
        errdefer memory_written.deinit(allocator);

        var faults = try std.DynamicBitSetUnmanaged.initEmpty(allocator, sector_count);
        errdefer faults.deinit(allocator);

        var overlay_buffers_allocated: u32 = 0;
        var overlay_buffers: [overlays_count_max][]align(constants.sector_size) u8 = undefined;
        errdefer for (overlay_buffers) |b| allocator.free(b);

        for (&overlay_buffers) |*overlay_buffer| {
            overlay_buffer.* =
                try allocator.alignedAlloc(u8, constants.sector_size, constants.message_size_max);
            overlay_buffers_allocated += 1;
        }

        var reads = PriorityQueue(*Storage.Read, void, Storage.Read.less_than).init(allocator, {});
        errdefer reads.deinit();
        try reads.ensureTotalCapacity(constants.iops_read_max);

        var writes =
            PriorityQueue(*Storage.Write, void, Storage.Write.less_than).init(allocator, {});
        errdefer writes.deinit();
        try writes.ensureTotalCapacity(constants.iops_write_max);

        return Storage{
            .allocator = allocator,
            .size = size,
            .options = options,
            .prng = prng,
            .memory = memory,
            .memory_written = memory_written,
            .faults = faults,
            .overlay_buffers = overlay_buffers,
            .reads = reads,
            .writes = writes,
        };
    }

    pub fn deinit(storage: *Storage, allocator: mem.Allocator) void {
        for (storage.overlay_buffers) |b| allocator.free(b);
        allocator.free(storage.memory);
        storage.memory_written.deinit(allocator);
        storage.faults.deinit(allocator);
        storage.reads.deinit();
        storage.writes.deinit();
    }

    /// Cancel any currently in-progress reads/writes.
    /// Corrupt the target sectors of any in-progress writes.
    pub fn reset(storage: *Storage) void {
        log.debug("Reset: {} pending reads, {} pending writes, {} pending next_ticks", .{
            storage.reads.count(),
            storage.writes.count(),
            storage.next_tick_queue.count,
        });
        while (storage.writes.peek()) |_| {
            const write = storage.writes.remove();
            if (!storage.x_in_100(storage.options.crash_fault_probability)) continue;

            // Randomly corrupt one of the faulty sectors the operation targeted.
            // TODO: inject more realistic and varied storage faults as described above.
            const sectors = SectorRange.from_zone(write.zone, write.offset, write.buffer.len);
            storage.fault_sector(write.zone, sectors.random(storage.prng.random()));
        }
        assert(storage.writes.items.len == 0);

        storage.reads.items.len = 0;
        storage.next_tick_queue.reset();
    }

    /// Compile-time upper bound on the size of a grid of a testing Storage.
    pub const grid_blocks_max =
        grid_blocks_for_storage_size(constants.storage_size_limit_max);

    /// Runtime bound on the size of the grid of a testing Storage.
    pub fn grid_blocks(storage: *const Storage) u64 {
        return grid_blocks_for_storage_size(storage.size);
    }

    /// How many grid blocks fit in the Storage of the specified size.
    fn grid_blocks_for_storage_size(size: u64) u64 {
        assert(size <= constants.storage_size_limit_max);
        const free_set_shard_count = @divFloor(
            size - superblock.data_file_size_min,
            constants.block_size * FreeSet.shard_bits,
        );
        return free_set_shard_count * FreeSet.shard_bits;
    }

    /// Returns the number of bytes that have been written to, assuming that (the simulated)
    /// `fallocate()` creates a sparse file.
    pub fn size_used(storage: *const Storage) usize {
        return storage.memory_written.count() * constants.sector_size;
    }

    /// Copy state from `origin` to `storage`:
    ///
    /// - ticks
    /// - memory
    /// - occupied memory
    /// - faulty sectors
    /// - reads in-progress
    /// - writes in-progress
    ///
    /// Both instances must have an identical size.
    pub fn copy(storage: *Storage, origin: *const Storage) void {
        assert(storage.size == origin.size);

        storage.ticks = origin.ticks;

        var it = origin.memory_written.iterator(.{});
        while (it.next()) |sector| {
            stdx.copy_disjoint(
                .exact,
                u8,
                storage.memory[sector * constants.sector_size ..][0..constants.sector_size],
                origin.memory[sector * constants.sector_size ..][0..constants.sector_size],
            );
        }
        storage.memory_written.toggleSet(storage.memory_written);
        storage.memory_written.toggleSet(origin.memory_written);
        storage.faults.toggleSet(storage.faults);
        storage.faults.toggleSet(origin.faults);

        storage.reads.items.len = 0;
        for (origin.reads.items) |read| {
            storage.reads.add(read) catch unreachable;
        }

        storage.writes.items.len = 0;
        for (origin.writes.items) |write| {
            storage.writes.add(write) catch unreachable;
        }
    }

    pub fn tick(storage: *Storage) void {
        storage.ticks += 1;

        while (storage.reads.peek()) |read| {
            if (read.done_at_tick > storage.ticks) break;
            _ = storage.reads.remove();
            storage.read_sectors_finish(read);
        }

        while (storage.writes.peek()) |write| {
            if (write.done_at_tick > storage.ticks) break;
            _ = storage.writes.remove();
            storage.write_sectors_finish(write);
        }

        // Process the queues in a single loop, since their callbacks may append to each other.
        while (storage.next_tick_queue.pop()) |next_tick| {
            next_tick.callback(next_tick);
        }
    }

    pub fn on_next_tick(
        storage: *Storage,
        source: NextTickSource,
        callback: *const fn (next_tick: *Storage.NextTick) void,
        next_tick: *Storage.NextTick,
    ) void {
        next_tick.* = .{
            .source = source,
            .callback = callback,
        };

        storage.next_tick_queue.push(next_tick);
    }

    pub fn reset_next_tick_lsm(storage: *Storage) void {
        var next_tick_iterator = storage.next_tick_queue;
        storage.next_tick_queue.reset();

        while (next_tick_iterator.pop()) |next_tick| {
            if (next_tick.source != .lsm) storage.next_tick_queue.push(next_tick);
        }
    }

    /// * Verifies that the read fits within the target sector.
    /// * Verifies that the read targets sectors that have been written to.
    pub fn read_sectors(
        storage: *Storage,
        callback: *const fn (read: *Storage.Read) void,
        read: *Storage.Read,
        buffer: []u8,
        zone: vsr.Zone,
        offset_in_zone: u64,
    ) void {
        zone.verify_iop(buffer, offset_in_zone);
        assert(zone != .grid_padding);
        hash_log.emit_autohash(.{ buffer, zone, offset_in_zone }, .DeepRecursive);

        switch (zone) {
            .superblock,
            .wal_headers,
            .wal_prepares,
            => {
                var sectors = SectorRange.from_zone(zone, offset_in_zone, buffer.len);
                while (sectors.next()) |sector| assert(storage.memory_written.isSet(sector));
            },
            .grid_padding => unreachable,
            .client_replies, .grid => {
                // ClientReplies/Grid repairs can read blocks that have not ever been written.
                // (The former case is possible if we sync to a new superblock and someone requests
                // a client reply that we haven't repaired yet.)
            },
        }

        read.* = .{
            .callback = callback,
            .buffer = buffer,
            .zone = zone,
            .offset = offset_in_zone,
            .done_at_tick = storage.ticks + storage.read_latency(),
            .stack_trace = StackTrace.capture(),
        };

        // We ensure the capacity is sufficient for constants.iops_read_max in init()
        storage.reads.add(read) catch unreachable;
    }

    fn read_sectors_finish(storage: *Storage, read: *Storage.Read) void {
        hash_log.emit_autohash(.{ read.buffer, read.zone, read.offset }, .DeepRecursive);

        const offset_in_storage = read.zone.offset(read.offset);
        stdx.copy_disjoint(
            .exact,
            u8,
            read.buffer,
            storage.memory[offset_in_storage..][0..read.buffer.len],
        );

        if (storage.x_in_100(storage.options.read_fault_probability)) {
            if (storage.pick_faulty_sector(read.zone, read.offset, read.buffer.len)) |sector| {
                storage.fault_sector(read.zone, sector);
            }
        }

        const faulty = storage.faulty and faulty: {
            if (read.zone != .wal_prepares) break :faulty true;

            // Don't corrupt a WAL prepare if the corresponding WAL header write was misdirected, to
            // avoid a double-fault which the journal interprets as a torn prepare.
            const header_slot = @divExact(read.offset, constants.message_size_max);
            const header_offset = vsr.sector_floor(header_slot * @sizeOf(vsr.Header));

            var overlays_iterator = storage.overlays.iterate();
            while (overlays_iterator.next()) |overlay| {
                if (overlay.zone == .wal_headers and overlay.offset == header_offset) {
                    break :faulty false;
                }
            }
            break :faulty true;
        };

        // Fill faulty or uninitialized sectors with random data.
        var sectors = SectorRange.from_zone(read.zone, read.offset, read.buffer.len);
        const sectors_min = sectors.min;
        while (sectors.next()) |sector| {
            const faulty_sector = faulty and storage.faults.isSet(sector);
            const uninit = !storage.memory_written.isSet(sector);
            if (faulty_sector or uninit) {
                const sector_offset = (sector - sectors_min) * constants.sector_size;
                const sector_bytes = read.buffer[sector_offset..][0..constants.sector_size];
                storage.prng.random().bytes(sector_bytes);
            }
        }

        // Apply misdirected data.
        if (storage.faulty) {
            var overlays_iterator = storage.overlays.iterate();
            while (overlays_iterator.next()) |overlay| {
                if (overlay.zone == read.zone and
                    overlay.offset == read.offset)
                {
                    log.debug("{}: read_sectors_finish: apply misdirect " ++
                        "zone={s} offset={} size={}", .{
                        storage.options.replica_index.?,
                        @tagName(overlay.zone),
                        overlay.offset,
                        overlay.size,
                    });

                    const overlay_buffer = storage.overlay_buffers[storage.overlays.index(overlay)];
                    const overlay_target = overlay_buffer[0..@min(overlay.size, read.buffer.len)];
                    stdx.copy_disjoint(.inexact, u8, read.buffer, overlay_target);
                }
            }
        }

        read.callback(read);
    }

    pub fn write_sectors(
        storage: *Storage,
        callback: *const fn (write: *Storage.Write) void,
        write: *Storage.Write,
        buffer: []const u8,
        zone: vsr.Zone,
        offset_in_zone: u64,
    ) void {
        zone.verify_iop(buffer, offset_in_zone);
        maybe(zone == .grid_padding); // Padding is zeroed during format.
        hash_log.emit_autohash(.{ buffer, zone, offset_in_zone }, .DeepRecursive);

        // Verify that there are no concurrent overlapping writes.
        var iterator = storage.writes.iterator();
        while (iterator.next()) |other| {
            if (other.zone != zone) continue;
            assert(offset_in_zone + buffer.len <= other.offset or
                other.offset + other.buffer.len <= offset_in_zone);
        }

        write.* = .{
            .callback = callback,
            .buffer = buffer,
            .zone = zone,
            .offset = offset_in_zone,
            .done_at_tick = storage.ticks + storage.write_latency(),
            .stack_trace = StackTrace.capture(),
        };

        // We ensure the capacity is sufficient for constants.iops_write_max in init()
        storage.writes.add(write) catch unreachable;
    }

    fn write_sectors_finish(storage: *Storage, write: *Storage.Write) void {
        assert(storage.overlays.total() >= 2);

        hash_log.emit_autohash(.{ write.buffer, write.zone, write.offset }, .DeepRecursive);

        // Clean up old misdirects if they are overwritten.
        var overlays_iterator = storage.overlays.iterate();
        while (overlays_iterator.next()) |overlay| {
            if (overlay.zone == write.zone and
                overlay.offset == write.offset)
            {
                storage.overlays.release(overlay);
            }
        }

        // Apply a new misdirect.
        const misdirect = storage.overlays.available() >= 2 and
            storage.pick_faulty_sector(write.zone, write.offset, write.buffer.len) != null and
            storage.x_in_100(storage.options.write_misdirect_probability);
        const misdirect_offset = if (misdirect) storage.pick_faulty_chunk_offset(write) else null;
        if (misdirect_offset) |mistaken_offset| {
            const overlay_mistaken = storage.overlays.acquire().?;
            const overlay_intended = storage.overlays.acquire().?;

            const overlay_mistaken_index = storage.overlays.index(overlay_mistaken);
            const overlay_intended_index = storage.overlays.index(overlay_intended);

            log.debug("{}: write_sectors_finish: misdirect zone={s} offset={}->{} size={}", .{
                storage.options.replica_index.?,
                @tagName(write.zone),
                write.offset,
                mistaken_offset,
                write.buffer.len,
            });

            const overlay_size: u32 = @intCast(write.buffer.len);
            overlay_mistaken.* =
                .{ .zone = write.zone, .offset = mistaken_offset, .size = overlay_size };
            overlay_intended.* =
                .{ .zone = write.zone, .offset = write.offset, .size = overlay_size };

            const overlay_mistaken_buffer = storage.overlay_buffers[overlay_mistaken_index];
            const overlay_intended_buffer = storage.overlay_buffers[overlay_intended_index];
            const target_intended_buffer =
                storage.memory[write.zone.offset(write.offset)..][0..write.buffer.len];

            stdx.copy_disjoint(.inexact, u8, overlay_mistaken_buffer, write.buffer);
            stdx.copy_disjoint(.inexact, u8, overlay_intended_buffer, target_intended_buffer);
        }

        const offset_in_storage = write.zone.offset(write.offset);
        stdx.copy_disjoint(
            .exact,
            u8,
            storage.memory[offset_in_storage..][0..write.buffer.len],
            write.buffer,
        );

        var sectors = SectorRange.from_zone(write.zone, write.offset, write.buffer.len);
        while (sectors.next()) |sector| {
            storage.faults.unset(sector);
            storage.memory_written.set(sector);
        }

        if (storage.x_in_100(storage.options.write_fault_probability)) {
            if (storage.pick_faulty_sector(write.zone, write.offset, write.buffer.len)) |sector| {
                storage.fault_sector(write.zone, sector);
            }
        }

        write.callback(write);
    }

    fn read_latency(storage: *Storage) u64 {
        return storage.latency(storage.options.read_latency_min, storage.options.read_latency_mean);
    }

    fn write_latency(storage: *Storage) u64 {
        return storage.latency(
            storage.options.write_latency_min,
            storage.options.write_latency_mean,
        );
    }

    fn latency(storage: *Storage, min: u64, mean: u64) u64 {
        return min + fuzz.random_int_exponential(storage.prng.random(), u64, mean - min);
    }

    /// Return true with probability x/100.
    fn x_in_100(storage: *Storage, x: u8) bool {
        assert(x <= 100);
        return x > storage.prng.random().uintLessThan(u8, 100);
    }

    fn pick_faulty_sector(
        storage: *Storage,
        zone: vsr.Zone,
        offset_in_zone: u64,
        size: u64,
    ) ?usize {
        const atlas = storage.options.fault_atlas orelse return null;
        return atlas.faulty_sector(
            storage.prng.random(),
            storage.options.replica_index.?,
            zone,
            offset_in_zone,
            size,
        );
    }

    fn pick_faulty_chunk_offset(storage: *Storage, write: *const Write) ?u64 {
        const atlas = storage.options.fault_atlas orelse return null;
        return atlas.faulty_chunk_offset(
            storage.prng.random(),
            storage.options.replica_index.?,
            write.zone,
            write.buffer.len,
        );
    }

    fn fault_sector(storage: *Storage, zone: vsr.Zone, sector: usize) void {
        storage.faults.set(sector);
        if (storage.options.replica_index) |replica_index| {
            const offset = sector * constants.sector_size - zone.offset(0);
            switch (zone) {
                .superblock => {
                    log.debug(
                        "{}: corrupting sector at zone={} offset={}",
                        .{ replica_index, zone, offset },
                    );
                },
                .wal_prepares, .client_replies => {
                    comptime assert(constants.message_size_max % constants.sector_size == 0);
                    const slot = @divFloor(offset, constants.message_size_max);
                    log.debug(
                        "{}: corrupting sector at zone={} offset={} slot={}",
                        .{ replica_index, zone, offset, slot },
                    );
                },
                .wal_headers => {
                    comptime assert(constants.sector_size % @sizeOf(vsr.Header) == 0);
                    const slot_min = @divFloor(offset, @sizeOf(vsr.Header));
                    const slot_max = slot_min +
                        @divExact(constants.sector_size, @sizeOf(vsr.Header));
                    log.debug(
                        "{}: corrupting sector at zone={} offset={} slots={}...{}",
                        .{ replica_index, zone, offset, slot_min, slot_max },
                    );
                },
                .grid_padding => unreachable,
                .grid => {
                    comptime assert(constants.block_size % @sizeOf(vsr.Header) == 0);
                    const address = @divFloor(offset, constants.block_size) + 1;
                    log.debug(
                        "{}: corrupting sector at zone={} offset={} address={}",
                        .{ replica_index, zone, offset, address },
                    );
                },
            }
        }
    }

    pub fn area_memory(
        storage: *const Storage,
        area: Area,
    ) []align(constants.sector_size) const u8 {
        const sectors = area.sectors();
        const area_min = sectors.min * constants.sector_size;
        const area_max = sectors.max * constants.sector_size;
        return @alignCast(storage.memory[area_min..area_max]);
    }

    /// Returns whether any sector in the area is corrupt.
    pub fn area_faulty(storage: *const Storage, area: Area) bool {
        const sectors = area.sectors();
        var sector = sectors.min;
        var faulty: bool = false;
        while (sector < sectors.max) : (sector += 1) {
            faulty = faulty or storage.faults.isSet(sector);
        }

        var misdirected: bool = false;
        var overlays = storage.overlays.iterate_const();
        while (overlays.next()) |overlay| {
            misdirected = misdirected or
                (overlay.zone == area and overlay.offset == area.offset_in_zone());
        }
        return faulty or misdirected;
    }

    pub fn superblock_header(
        storage: *const Storage,
        copy_: u8,
    ) *const superblock.SuperBlockHeader {
        const offset =
            vsr.Zone.superblock.offset(@as(usize, copy_) * superblock.superblock_copy_size);
        const bytes = storage.memory[offset..][0..@sizeOf(superblock.SuperBlockHeader)];
        return @alignCast(mem.bytesAsValue(superblock.SuperBlockHeader, bytes));
    }

    pub fn wal_headers(storage: *const Storage) []const vsr.Header.Prepare {
        const offset = vsr.Zone.wal_headers.offset(0);
        const size = vsr.Zone.wal_headers.size().?;
        return @alignCast(mem.bytesAsSlice(
            vsr.Header.Prepare,
            storage.memory[offset..][0..size],
        ));
    }

    fn MessageRawType(comptime command: vsr.Command) type {
        return extern struct {
            const MessageRaw = @This();
            header: vsr.Header.Type(command),
            body: [constants.message_size_max - @sizeOf(vsr.Header)]u8,

            comptime {
                assert(@sizeOf(MessageRaw) == constants.message_size_max);
                assert(stdx.no_padding(MessageRaw));
            }
        };
    }

    pub fn wal_prepares(storage: *const Storage) []const MessageRawType(.prepare) {
        const offset = vsr.Zone.wal_prepares.offset(0);
        const size = vsr.Zone.wal_prepares.size().?;
        return @alignCast(mem.bytesAsSlice(
            MessageRawType(.prepare),
            storage.memory[offset..][0..size],
        ));
    }

    pub fn client_replies(storage: *const Storage) []const MessageRawType(.reply) {
        const offset = vsr.Zone.client_replies.offset(0);
        const size = vsr.Zone.client_replies.size().?;
        return @alignCast(mem.bytesAsSlice(
            MessageRawType(.reply),
            storage.memory[offset..][0..size],
        ));
    }

    pub fn grid_block(
        storage: *const Storage,
        address: u64,
    ) ?*align(constants.sector_size) const [constants.block_size]u8 {
        assert(address > 0);

        const block_offset = vsr.Zone.grid.offset((address - 1) * constants.block_size);
        if (storage.memory_written.isSet(@divExact(block_offset, constants.sector_size))) {
            const block_buffer = storage.memory[block_offset..][0..constants.block_size];
            const block_header = schema.header_from_block(@alignCast(block_buffer));
            assert(block_header.address == address);

            return @alignCast(block_buffer);
        } else {
            return null;
        }
    }

    pub fn log_pending_io(storage: *const Storage) void {
        for (storage.reads.items) |read| {
            log.debug("Pending read: {} {}\n{}", .{ read.offset, read.zone, read.stack_trace });
        }
        for (storage.writes.items) |write| {
            log.debug("Pending write: {} {}\n{}", .{ write.offset, write.zone, write.stack_trace });
        }
    }

    pub fn assert_no_pending_reads(storage: *const Storage, zone: vsr.Zone) void {
        var assert_failed = false;

        for (storage.reads.items) |read| {
            if (read.zone == zone) {
                log.err("Pending read: {} {}\n{}", .{ read.offset, read.zone, read.stack_trace });
                assert_failed = true;
            }
        }

        if (assert_failed) {
            panic("Pending reads in zone: {}", .{zone});
        }
    }

    pub fn assert_no_pending_writes(storage: *const Storage, zone: vsr.Zone) void {
        var assert_failed = false;

        const writes = storage.writes;
        for (writes.items) |write| {
            if (write.zone == zone) {
                log.err("Pending write: {} {}\n{}", .{
                    write.offset,
                    write.zone,
                    write.stack_trace,
                });
                assert_failed = true;
            }
        }

        if (assert_failed) {
            panic("Pending writes in zone: {}", .{zone});
        }
    }

    /// Verify that the storage:
    /// - contains the given index block
    /// - contains every data block referenced by the index block
    pub fn verify_table(storage: *const Storage, index_address: u64, index_checksum: u128) void {
        assert(index_address > 0);

        const index_block = storage.grid_block(index_address).?;
        const index_schema = schema.TableIndex.from(index_block);
        const index_block_header = schema.header_from_block(index_block);
        assert(index_block_header.address == index_address);
        assert(index_block_header.checksum == index_checksum);
        assert(index_block_header.block_type == .index);

        for (
            index_schema.data_addresses_used(index_block),
            index_schema.data_checksums_used(index_block),
        ) |address, checksum| {
            const data_block = storage.grid_block(address).?;
            const data_block_header = schema.header_from_block(data_block);

            assert(data_block_header.address == address);
            assert(data_block_header.checksum == checksum.value);
            assert(data_block_header.block_type == .data);
        }
    }

    pub fn transition_to_liveness_mode(storage: *Storage) void {
        storage.options.read_fault_probability = 0;
        storage.options.write_fault_probability = 0;
        storage.options.write_misdirect_probability = 0;
        storage.options.crash_fault_probability = 0;
    }
};

pub const Area = union(vsr.Zone) {
    superblock: struct { copy: u8 },
    wal_headers: struct { sector: usize },
    wal_prepares: struct { slot: usize },
    client_replies: struct { slot: usize },
    grid_padding,
    grid: struct { address: u64 },

    fn offset_in_zone(area: Area) u64 {
        return switch (area) {
            .superblock => |data| vsr.superblock.superblock_copy_size * @as(u64, data.copy),
            .wal_headers => |data| constants.sector_size * data.sector,
            .wal_prepares => |data| constants.message_size_max * data.slot,
            .client_replies => |data| constants.message_size_max * data.slot,
            .grid_padding => unreachable,
            .grid => |data| constants.block_size * (data.address - 1),
        };
    }

    fn sectors(area: Area) SectorRange {
        return SectorRange.from_zone(area, area.offset_in_zone(), switch (area) {
            .superblock => vsr.superblock.superblock_copy_size,
            .wal_headers => constants.sector_size,
            .wal_prepares => constants.message_size_max,
            .client_replies => constants.message_size_max,
            .grid_padding => unreachable,
            .grid => constants.block_size,
        });
    }
};

const SectorRange = struct {
    min: usize, // inclusive sector index
    max: usize, // exclusive sector index

    fn from_zone(
        zone: vsr.Zone,
        offset_in_zone: u64,
        size: usize,
    ) SectorRange {
        return from_offset(zone.offset(offset_in_zone), size);
    }

    fn from_offset(offset_in_storage: u64, size: usize) SectorRange {
        return .{
            .min = @divExact(offset_in_storage, constants.sector_size),
            .max = @divExact(offset_in_storage + size, constants.sector_size),
        };
    }

    fn random(range: SectorRange, rand: std.rand.Random) usize {
        return range.min + rand.uintLessThan(usize, range.max - range.min);
    }

    fn next(range: *SectorRange) ?usize {
        if (range.min == range.max) return null;
        defer range.min += 1;
        return range.min;
    }

    fn intersect(a: SectorRange, b: SectorRange) ?SectorRange {
        if (a.max <= b.min) return null;
        if (b.max <= a.min) return null;
        return SectorRange{
            .min = @max(a.min, b.min),
            .max = @min(a.max, b.max),
        };
    }
};

/// To ensure the cluster can recover, each header/prepare/block must be valid (not faulty) at
/// a majority of replicas.
///
/// We can't allow WAL storage faults for the same message in a majority of
/// the replicas as that would make recovery impossible. Instead, we only
/// allow faults in certain areas which differ between replicas.
pub const ClusterFaultAtlas = struct {
    pub const Options = struct {
        faulty_superblock: bool,
        faulty_wal_headers: bool,
        faulty_wal_prepares: bool,
        faulty_client_replies: bool,
        faulty_grid: bool,
    };

    const ReplicaSet = std.StaticBitSet(constants.replicas_max);
    const headers_per_sector = @divExact(constants.sector_size, @sizeOf(vsr.Header));
    const members_max = constants.members_max;

    faulty_wal_header_sectors: [members_max]std.DynamicBitSetUnmanaged,
    faulty_client_reply_slots: [members_max]std.DynamicBitSetUnmanaged,
    /// Bit 0 corresponds to address 1.
    faulty_grid_blocks: [members_max]std.DynamicBitSetUnmanaged,

    pub fn init(
        allocator: std.mem.Allocator,
        replica_count: u8,
        random: std.rand.Random,
        options: Options,
    ) !ClusterFaultAtlas {
        if (replica_count == 1) {
            // If there is only one replica in the cluster, WAL/Grid faults are not recoverable.
            maybe(options.faulty_superblock);
            assert(!options.faulty_wal_headers);
            assert(!options.faulty_wal_prepares);
            assert(!options.faulty_client_replies);
            assert(!options.faulty_grid);
        }

        // Currently these faulty areas are coupled together, so they should match.
        assert(options.faulty_wal_headers == options.faulty_wal_prepares);

        const fault_bitset_sizes = [3]u32{
            @divExact(constants.journal_size_headers, constants.sector_size), // WAL headers.
            constants.clients_max, // Client replies.
            Storage.grid_blocks_max, // Grid.
        };

        var fault_bitsets_allocated: u32 = 0;
        var fault_bitsets: [3 * members_max]std.DynamicBitSetUnmanaged = undefined;
        errdefer for (fault_bitsets[0..fault_bitsets_allocated]) |*b| b.deinit(allocator);

        for (&fault_bitsets, 0..) |*fault_bitset, i| {
            const fault_bitset_size = fault_bitset_sizes[@divFloor(i, members_max)];
            fault_bitset.* = try std.DynamicBitSetUnmanaged.initEmpty(allocator, fault_bitset_size);
            fault_bitsets_allocated += 1;
        }

        var atlas = ClusterFaultAtlas{
            .faulty_wal_header_sectors = fault_bitsets[0 * members_max ..][0..members_max].*,
            .faulty_client_reply_slots = fault_bitsets[1 * members_max ..][0..members_max].*,
            .faulty_grid_blocks = fault_bitsets[2 * members_max ..][0..members_max].*,
        };

        const quorums = vsr.quorums(replica_count);
        const faults_max = quorums.replication - 1;
        assert(faults_max < replica_count);
        assert(faults_max < quorums.replication);
        assert(faults_max < quorums.view_change);
        assert(faults_max > 0 or replica_count == 1);

        for ([_]struct { bool, *[members_max]std.DynamicBitSetUnmanaged }{
            .{ options.faulty_wal_headers, &atlas.faulty_wal_header_sectors },
            .{ options.faulty_client_replies, &atlas.faulty_client_reply_slots },
            .{ options.faulty_grid, &atlas.faulty_grid_blocks },
        }) |zone| {
            const faulty = zone.@"0";
            const chunks = zone.@"1";
            if (!faulty) continue;

            for (0..chunks[0].bit_length) |chunk| {
                var replicas = ReplicaSet.initEmpty();
                while (replicas.count() < faults_max) {
                    const replica_index = random.uintLessThan(u8, replica_count);
                    if (chunks[replica_index].count() + 1 <
                        chunks[replica_index].capacity())
                    {
                        chunks[replica_index].set(chunk);
                        replicas.set(replica_index);
                    } else {
                        // Never corrupt all chunks of a particular replica.
                        // (For the WAL, this can cause error.WALInvalid).
                    }
                }
            }
        }

        return atlas;
    }

    pub fn deinit(atlas: *ClusterFaultAtlas, allocator: std.mem.Allocator) void {
        for (&atlas.faulty_grid_blocks) |*b| b.deinit(allocator);
        for (&atlas.faulty_client_reply_slots) |*b| b.deinit(allocator);
        for (&atlas.faulty_wal_header_sectors) |*b| b.deinit(allocator);
    }

    fn zone_chunks(atlas: *const ClusterFaultAtlas, zone: vsr.Zone) ?struct {
        chunk_size: u32,
        faulty: *const [members_max]std.DynamicBitSetUnmanaged,
    } {
        return switch (zone) {
            // Don't inject additional read/write/misdirect faults into superblock headers.
            // This prevents the quorum from being lost like so:
            // - copy₀: B (ok)
            // - copy₁: B (torn write)
            // - copy₂: A (corrupt)
            // - copy₃: A (ok)
            // TODO Use hash-chaining to safely load copy₀, so that we can inject a superblock
            // fault.
            .superblock => null,
            // We assert that the padding is never read, so there's no need to fault it.
            .grid_padding => unreachable,

            .wal_headers => .{
                .chunk_size = constants.sector_size,
                .faulty = &atlas.faulty_wal_header_sectors,
            },
            .wal_prepares => .{
                .chunk_size = constants.message_size_max * headers_per_sector,
                .faulty = &atlas.faulty_wal_header_sectors,
            },
            .client_replies => .{
                .chunk_size = constants.message_size_max,
                .faulty = &atlas.faulty_client_reply_slots,
            },
            .grid => .{
                .chunk_size = constants.block_size,
                .faulty = &atlas.faulty_grid_blocks,
            },
        };
    }

    /// Given a write of `size` bytes to the given zone, find an interesting offset within the same
    /// zone to target. (If we want to drop the latter condition, an alternate implementation
    /// strategy is: on random writes, perform the write successfully, but save the target
    /// zone/offset/size. Then on a future random write, misdirect to a compatible saved location.)
    fn faulty_chunk_offset(
        atlas: *const ClusterFaultAtlas,
        random: std.Random,
        replica_index: u8,
        zone: vsr.Zone,
        size: u64,
    ) ?u64 {
        const chunks = atlas.zone_chunks(zone) orelse return null;

        if (chunks.chunk_size < size) {
            // When formatting the WAL, we may write many chunks simultaneously (to avoid a storm of
            // tiny writes).
            assert(zone == .wal_headers or zone == .wal_prepares);
            assert(size % constants.sector_size == 0);
            return null;
        }

        const chunks_faulty = &chunks.faulty[replica_index];
        const chunk_count = chunks_faulty.bit_length;
        const chunk_start = random.uintLessThan(usize, chunk_count);
        for (0..chunk_count) |i| {
            const chunk_index = (chunk_start + i) % chunk_count;
            if (chunks_faulty.isSet(chunk_index)) {
                // The chunk size of zone=wal_prepares is a multiple of the message_size_max, but
                // misdirects in the wal_prepares zone always land on the first message of a chunk.
                return chunk_index * chunks.chunk_size;
            }
        }
        return null;
    }

    fn faulty_sector(
        atlas: *const ClusterFaultAtlas,
        random: std.Random,
        replica_index: u8,
        zone: vsr.Zone,
        offset_in_zone: u64,
        size: u64,
    ) ?usize {
        const chunks = atlas.zone_chunks(zone) orelse return null;

        var fault_start: ?usize = null;
        var fault_count: usize = 0;

        var chunk: usize = @divFloor(offset_in_zone, chunks.chunk_size);
        while (chunk * chunks.chunk_size < offset_in_zone + size) : (chunk += 1) {
            if (chunks.faulty[replica_index].isSet(chunk)) {
                if (fault_start == null) fault_start = chunk;
                fault_count += 1;
            } else {
                if (fault_start != null) break;
            }
        }

        if (fault_start) |start| {
            return SectorRange.from_zone(
                zone,
                chunks.chunk_size * start,
                chunks.chunk_size * fault_count,
            ).intersect(SectorRange.from_zone(zone, offset_in_zone, size)).?.random(random);
        } else {
            return null;
        }
    }
};

const StackTrace = struct {
    addresses: [64]usize,
    index: usize,

    fn capture() StackTrace {
        var addresses: [64]usize = undefined;
        var stack_trace = std.builtin.StackTrace{
            .instruction_addresses = &addresses,
            .index = 0,
        };
        std.debug.captureStackTrace(null, &stack_trace);
        return StackTrace{ .addresses = addresses, .index = stack_trace.index };
    }

    pub fn format(
        self: StackTrace,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        var addresses = self.addresses;
        const stack_trace = std.builtin.StackTrace{
            .instruction_addresses = &addresses,
            .index = self.index,
        };
        try writer.print("{}", .{stack_trace});
    }
};
