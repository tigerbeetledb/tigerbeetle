//! Fuzz FreeSet reserve/acquire/release flow.
//!
//! This fuzzer does *not* cover FreeSet encoding/decoding.
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.fuzz_vsr_free_set);

const FreeSet = @import("./free_set.zig").FreeSet;
const Reservation = @import("./free_set.zig").Reservation;
const fuzz = @import("../testing/fuzz.zig");

pub fn main(args: fuzz.FuzzArgs) !void {
    const allocator = fuzz.allocator;

    var prng = std.rand.DefaultPrng.init(args.seed);

    const blocks_count = FreeSet.shard_bits * (1 + prng.random().uintLessThan(usize, 10));
    const events_count = @min(
        args.events_max orelse @as(usize, 2_000_000),
        fuzz.random_int_exponential(prng.random(), usize, blocks_count * 100),
    );
    const events = try generate_events(allocator, prng.random(), .{
        .blocks_count = blocks_count,
        .events_count = events_count,
    });
    defer allocator.free(events);

    try run_fuzz(allocator, prng.random(), blocks_count, events);
}

fn run_fuzz(
    allocator: std.mem.Allocator,
    random: std.rand.Random,
    blocks_count: usize,
    events: []const FreeSetEvent,
) !void {
    var free_set = try FreeSet.open_empty(allocator, blocks_count);
    defer free_set.deinit(allocator);

    var free_set_model = try FreeSetModel.init(allocator, blocks_count);
    defer free_set_model.deinit(allocator);

    var active_reservations = std.ArrayList(Reservation).init(allocator);
    defer active_reservations.deinit();

    var active_addresses = std.ArrayList(u64).init(allocator);
    defer active_addresses.deinit();

    for (events) |event| {
        log.debug("event={}", .{event});
        switch (event) {
            .reserve => |reserve| {
                const reservation_actual = free_set.reserve(reserve.blocks);
                const reservation_expect = free_set_model.reserve(reserve.blocks);
                assert(std.meta.eql(reservation_expect, reservation_actual));

                if (reservation_expect) |reservation| {
                    try active_reservations.append(reservation);
                }
            },
            .forfeit => {
                random.shuffle(Reservation, active_reservations.items);
                for (active_reservations.items) |reservation| {
                    free_set.forfeit(reservation);
                    free_set_model.forfeit(reservation);
                }
                active_reservations.clearRetainingCapacity();
            },
            .acquire => |data| {
                if (active_reservations.items.len == 0) continue;
                const reservation = active_reservations.items[
                    data.reservation % active_reservations.items.len
                ];
                const address_actual = free_set.acquire(reservation);
                const address_expect = free_set_model.acquire(reservation);
                assert(std.meta.eql(address_expect, address_actual));
                if (address_expect) |address| {
                    try active_addresses.append(address);
                }
            },
            .release => |data| {
                if (active_addresses.items.len == 0) continue;

                const address_index = data.address % active_addresses.items.len;
                const address = active_addresses.swapRemove(address_index);
                free_set.release(address);
                free_set_model.release(address);
            },
            .checkpoint => {
                random.shuffle(Reservation, active_reservations.items);
                for (active_reservations.items) |reservation| {
                    free_set.forfeit(reservation);
                    free_set_model.forfeit(reservation);
                }
                active_reservations.clearRetainingCapacity();

                free_set.free_released_blocks();
                free_set_model.checkpoint();
            },
        }

        assert(free_set_model.count_reservations() == free_set.count_reservations());
        assert(free_set_model.count_free() == free_set.count_free());
        assert(free_set_model.count_acquired() == free_set.count_acquired());
        assert(std.meta.eql(
            free_set_model.highest_address_acquired(),
            free_set.highest_address_acquired(),
        ));
    }
}

const FreeSetEventType = std.meta.Tag(FreeSetEvent);
const FreeSetEvent = union(enum) {
    reserve: struct { blocks: usize },
    forfeit: void,
    acquire: struct { reservation: usize },
    release: struct { address: usize },
    checkpoint: void,
};

fn generate_events(allocator: std.mem.Allocator, random: std.rand.Random, options: struct {
    blocks_count: usize,
    events_count: usize,
}) ![]const FreeSetEvent {
    const event_distribution = fuzz.DistributionType(FreeSetEventType){
        .reserve = 1 + random.float(f64) * 100,
        .forfeit = 1,
        .acquire = random.float(f64) * 1000,
        .release = if (random.boolean()) 0 else 500 * random.float(f64),
        .checkpoint = random.floatExp(f64) * 10,
    };

    const events = try allocator.alloc(FreeSetEvent, options.events_count);
    errdefer allocator.free(events);

    log.info("event_distribution = {:.2}", .{event_distribution});
    log.info("event_count = {d}", .{events.len});

    const reservation_blocks_mean = 1 +
        random.uintLessThan(usize, @divFloor(options.blocks_count, 20));
    for (events) |*event| {
        event.* = switch (fuzz.random_enum(random, FreeSetEventType, event_distribution)) {
            .reserve => FreeSetEvent{ .reserve = .{
                .blocks = 1 + fuzz.random_int_exponential(random, usize, reservation_blocks_mean),
            } },
            .forfeit => FreeSetEvent{ .forfeit = {} },
            .acquire => FreeSetEvent{ .acquire = .{ .reservation = random.int(usize) } },
            .release => FreeSetEvent{ .release = .{
                .address = random.int(usize),
            } },
            .checkpoint => FreeSetEvent{ .checkpoint = {} },
        };
    }
    return events;
}

const FreeSetModel = struct {
    /// Set bits indicate acquired blocks.
    blocks_acquired: std.DynamicBitSetUnmanaged,

    /// Set bits indicate blocks that will be released at the next checkpoint.
    blocks_released: std.DynamicBitSetUnmanaged,

    /// Set bits indicate blocks that are currently reserved and not yet forfeited.
    blocks_reserved: std.DynamicBitSetUnmanaged,

    reservation_count: usize = 0,
    reservation_session: usize = 1,

    fn init(allocator: std.mem.Allocator, blocks_count: usize) !FreeSetModel {
        var blocks_acquired = try std.DynamicBitSetUnmanaged.initEmpty(allocator, blocks_count);
        errdefer blocks_acquired.deinit(allocator);

        var blocks_released = try std.DynamicBitSetUnmanaged.initEmpty(allocator, blocks_count);
        errdefer blocks_released.deinit(allocator);

        var blocks_reserved = try std.DynamicBitSetUnmanaged.initEmpty(allocator, blocks_count);
        errdefer blocks_reserved.deinit(allocator);

        return FreeSetModel{
            .blocks_acquired = blocks_acquired,
            .blocks_released = blocks_released,
            .blocks_reserved = blocks_reserved,
        };
    }

    fn deinit(set: *FreeSetModel, allocator: std.mem.Allocator) void {
        set.blocks_acquired.deinit(allocator);
        set.blocks_released.deinit(allocator);
        set.blocks_reserved.deinit(allocator);
    }

    pub fn count_reservations(set: FreeSetModel) usize {
        return set.reservation_count;
    }

    pub fn count_free(set: FreeSetModel) usize {
        return set.blocks_acquired.capacity() - set.blocks_acquired.count();
    }

    pub fn count_acquired(set: FreeSetModel) usize {
        return set.blocks_acquired.count();
    }

    pub fn highest_address_acquired(set: FreeSetModel) ?u64 {
        var it = set.blocks_acquired.iterator(.{
            .direction = .reverse,
        });
        const block = it.next() orelse return null;
        return block + 1;
    }

    pub fn reserve(set: *FreeSetModel, reserve_count: usize) ?Reservation {
        assert(reserve_count > 0);

        var blocks_found_free: usize = 0;
        var iterator = set.blocks_acquired.iterator(.{ .kind = .unset });
        const blocks_reserved_count = set.blocks_reserved.count();
        while (iterator.next()) |block| {
            if (block < blocks_reserved_count) {
                assert(set.blocks_reserved.isSet(block));
                continue;
            }

            blocks_found_free += 1;
            if (blocks_found_free == reserve_count) {
                const block_base = blocks_reserved_count;
                const block_count = block + 1 - block_base;

                var i: usize = 0;
                while (i < block_count) : (i += 1) set.blocks_reserved.set(block_base + i);

                set.reservation_count += 1;
                return Reservation{
                    .block_base = block_base,
                    .block_count = block_count,
                    .session = set.reservation_session,
                };
            }
        }
        return null;
    }

    pub fn forfeit(set: *FreeSetModel, reservation: Reservation) void {
        set.assert_reservation_active(reservation);
        set.reservation_count -= 1;

        var i: usize = 0;
        while (i < reservation.block_count) : (i += 1) {
            set.blocks_reserved.unset(reservation.block_base + i);
        }

        if (set.reservation_count == 0) {
            set.reservation_session +%= 1;
            assert(set.blocks_reserved.count() == 0);
        }
    }

    pub fn acquire(set: *FreeSetModel, reservation: Reservation) ?u64 {
        assert(reservation.block_count > 0);
        assert(reservation.block_base < set.blocks_acquired.capacity());
        assert(reservation.session == set.reservation_session);
        set.assert_reservation_active(reservation);

        var iterator = set.blocks_acquired.iterator(.{ .kind = .unset });
        while (iterator.next()) |block| {
            if (block >= reservation.block_base and
                block < reservation.block_base + reservation.block_count)
            {
                assert(!set.blocks_acquired.isSet(block));
                set.blocks_acquired.set(block);

                const address = block + 1;
                return address;
            }
        }
        return null;
    }

    pub fn is_free(set: *FreeSetModel, address: u64) bool {
        return !set.blocks_acquired.isSet(address - 1);
    }

    pub fn release(set: *FreeSetModel, address: u64) void {
        const block = address - 1;
        set.blocks_released.set(block);
    }

    pub fn checkpoint(set: *FreeSetModel) void {
        assert(set.blocks_reserved.count() == 0);

        var iterator = set.blocks_released.iterator(.{});
        while (iterator.next()) |block| {
            assert(set.blocks_released.isSet(block));
            assert(set.blocks_acquired.isSet(block));

            set.blocks_released.unset(block);
            set.blocks_acquired.unset(block);
        }
        assert(set.blocks_released.count() == 0);
    }

    fn assert_reservation_active(set: FreeSetModel, reservation: Reservation) void {
        assert(set.reservation_count > 0);
        assert(set.reservation_session == reservation.session);

        var i: usize = 0;
        while (i < reservation.block_count) : (i += 1) {
            assert(set.blocks_reserved.isSet(reservation.block_base + i));
        }
    }
};
