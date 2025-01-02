const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("vsr.zig");
const constants = @import("constants.zig");
const IO = @import("testing/io.zig").IO;
const Storage = @import("storage.zig").StorageType(IO);
const fuzz = @import("testing/fuzz.zig");

pub fn main(args: fuzz.FuzzArgs) !void {
    const zones: []const vsr.Zone = &.{
        .superblock,
        .wal_headers,
        .wal_prepares,
        .client_replies,
    };

    const sector_size = constants.sector_size;
    const sector_count = 64;
    const storage_size = sector_count * sector_size;
    const iterations = args.events_max orelse 10_000;

    var prng = std.rand.DefaultPrng.init(args.seed);
    const random = prng.random();

    for (0..iterations) |_| {
        var fault_map = std.bit_set.ArrayBitSet(u8, sector_count).initEmpty();

        const failed_sector_cluster_count = random.intRangeAtMost(usize, 1, 10);
        const failed_sector_cluster_minimum_length = random.intRangeAtMost(usize, 1, 3);
        const failed_sector_cluster_maximum_length =
            failed_sector_cluster_minimum_length + random.intRangeAtMost(usize, 1, 3);

        for (0..failed_sector_cluster_count) |_| {
            const start = random.intRangeLessThan(
                usize,
                0,
                sector_count - failed_sector_cluster_maximum_length,
            );
            const end = start + random.intRangeAtMost(
                usize,
                failed_sector_cluster_minimum_length,
                failed_sector_cluster_maximum_length,
            );

            fault_map.setRangeValue(.{ .start = start, .end = @min(end, sector_count) }, true);
        }

        var storage_data_written: [storage_size]u8 align(sector_size) = undefined;
        @memset(&storage_data_written, 0);

        for (0..sector_count) |sector| {
            if (!fault_map.isSet(sector)) {
                prng.fill(
                    storage_data_written[sector * sector_size ..][0..sector_size],
                );
            }
        }

        var storage_data_stored: [storage_size]u8 align(sector_size) = undefined;
        @memset(&storage_data_stored, 0);
        var storage_data_read: [storage_size]u8 align(sector_size) = undefined;
        @memset(&storage_data_read, 0);

        var io = IO.init(&.{
            .{
                .buffer = &storage_data_stored,
                .fault_map = &fault_map.masks,
            },
        }, .{
            .seed = args.seed,
            .larger_than_logical_sector_read_fault_probability = 10,
        });

        var storage = try Storage.init(&io, 0);

        var write_completion: Storage.Write = undefined;

        for (zones) |zone| {
            storage.write_sectors(
                struct {
                    fn callback(completion: *Storage.Write) void {
                        _ = completion;
                    }
                }.callback,
                &write_completion,
                storage_data_written[zone.start()..][0..zone.size().?],
                zone,
                0,
                .{},
            );

            storage.tick();
        }

        for (zones) |zone| {
            const ReadDetail = struct {
                offset_in_zone: u64,
                read_length: u64,
            };

            var read_details: [32]ReadDetail = undefined;

            const zone_sector_count: u64 = @divExact(zone.size().?, sector_size);
            assert(zone_sector_count <= read_details.len);

            var index: u64 = 0;
            var read_detail_length: usize = 0;

            while (index < zone_sector_count) : (read_detail_length += 1) {
                const n_sectors = random.intRangeAtMost(
                    u64,
                    1,
                    @min(4, zone_sector_count - index),
                );

                read_details[read_detail_length] = .{
                    .offset_in_zone = index * sector_size,
                    .read_length = n_sectors * sector_size,
                };

                index += n_sectors;
            }

            random.shuffle(ReadDetail, read_details[0..read_detail_length]);

            for (read_details[0..read_detail_length]) |read_detail| {
                const sector_offset = read_detail.offset_in_zone;
                const read_length = read_detail.read_length;
                const read_buffer =
                    storage_data_read[zone.start() + sector_offset ..][0..read_length];

                var read_completion: Storage.Read = undefined;
                storage.read_sectors(
                    struct {
                        fn callback(completion: *Storage.Read) void {
                            _ = completion;
                        }
                    }.callback,
                    &read_completion,
                    read_buffer,
                    zone,
                    sector_offset,
                );

                storage.tick();
            }
        }

        for (zones) |zone| {
            const start = zone.start();
            const end = start + zone.size().?;

            try std.testing.expectEqualSlices(
                u8,
                storage_data_written[start..end],
                storage_data_stored[start..end],
            );
            try std.testing.expectEqualSlices(
                u8,
                storage_data_stored[start..end],
                storage_data_read[start..end],
            );
        }
    }
}
