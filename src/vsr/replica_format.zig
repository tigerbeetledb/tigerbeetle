const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const Header = vsr.Header;
const format_wal_headers = @import("./journal.zig").format_wal_headers;
const format_wal_prepares = @import("./journal.zig").format_wal_prepares;

// TODO Parallelize formatting IO.

/// Initialize the TigerBeetle replica's data file.
pub fn format(
    comptime Storage: type,
    allocator: std.mem.Allocator,
    options: vsr.SuperBlockType(Storage).FormatOptions,
    storage: *Storage,
    superblock: *vsr.SuperBlockType(Storage),
) !void {
    const ReplicaFormat = ReplicaFormatType(Storage);
    var replica_format = ReplicaFormat{};

    try replica_format.format_wal(allocator, options.cluster, storage);
    assert(!replica_format.formatting);

    try replica_format.format_replies(allocator, storage);
    assert(!replica_format.formatting);

    try replica_format.format_grid_padding(allocator, storage);
    assert(!replica_format.formatting);

    superblock.format(
        ReplicaFormat.format_superblock_callback,
        &replica_format.superblock_context,
        options,
    );

    replica_format.formatting = true;
    while (replica_format.formatting) storage.tick();

    storage.flush_sectors(ReplicaFormat.flush_sectors_callback, &replica_format.flush);
    replica_format.formatting = true;
    while (replica_format.formatting) storage.tick();
}

fn ReplicaFormatType(comptime Storage: type) type {
    const SuperBlock = vsr.SuperBlockType(Storage);
    return struct {
        const ReplicaFormat = @This();

        formatting: bool = false,
        superblock_context: SuperBlock.Context = undefined,
        write: Storage.Write = undefined,
        flush: Storage.Flush = undefined,

        fn format_wal(
            self: *ReplicaFormat,
            allocator: std.mem.Allocator,
            cluster: u128,
            storage: *Storage,
        ) !void {
            assert(!self.formatting);

            const header_zeroes = [_]u8{0} ** @sizeOf(Header);
            const wal_write_size_max = 4 * 1024 * 1024;
            assert(wal_write_size_max % constants.sector_size == 0);

            // Direct I/O requires the buffer to be sector-aligned.
            var wal_buffer = try allocator.alignedAlloc(
                u8,
                constants.sector_size,
                wal_write_size_max,
            );
            defer allocator.free(wal_buffer);

            // The logical offset *within the Zone*.
            // Even though the prepare zone follows the redundant header zone, write the prepares
            // first. This allows the test Storage to check the invariant "never write the redundant
            // header before the prepare".
            var wal_offset: u64 = 0;
            while (wal_offset < constants.journal_size_prepares) {
                const size = format_wal_prepares(cluster, wal_offset, wal_buffer);
                assert(size > 0);

                for (std.mem.bytesAsSlice(Header.Prepare, wal_buffer[0..size])) |*header| {
                    if (std.mem.eql(u8, std.mem.asBytes(header), &header_zeroes)) {
                        // This is the (empty) body of a reserved or root Prepare.
                    } else {
                        // This is a Prepare's header.
                        assert(header.valid_checksum());

                        if (header.op == 0) {
                            assert(header.operation == .root);
                        } else {
                            assert(header.operation == .reserved);
                        }
                    }
                }

                storage.write_sectors(
                    write_sectors_callback,
                    &self.write,
                    wal_buffer[0..size],
                    .wal_prepares,
                    wal_offset,
                    .{ .dsync = false, .format = true },
                );
                self.formatting = true;
                while (self.formatting) storage.tick();
                wal_offset += size;
            }
            // There are no prepares left to write.
            assert(format_wal_prepares(cluster, wal_offset, wal_buffer) == 0);

            wal_offset = 0;
            while (wal_offset < constants.journal_size_headers) {
                const size = format_wal_headers(cluster, wal_offset, wal_buffer);
                assert(size > 0);

                for (std.mem.bytesAsSlice(Header.Prepare, wal_buffer[0..size])) |*header| {
                    assert(header.valid_checksum());

                    if (header.op == 0) {
                        assert(header.operation == .root);
                    } else {
                        assert(header.operation == .reserved);
                    }
                }

                storage.write_sectors(
                    write_sectors_callback,
                    &self.write,
                    wal_buffer[0..size],
                    .wal_headers,
                    wal_offset,
                    .{ .dsync = false, .format = true },
                );
                self.formatting = true;
                while (self.formatting) storage.tick();
                wal_offset += size;
            }
            // There are no headers left to write.
            assert(format_wal_headers(cluster, wal_offset, wal_buffer) == 0);
        }

        fn format_replies(
            self: *ReplicaFormat,
            allocator: std.mem.Allocator,
            storage: *Storage,
        ) !void {
            assert(!self.formatting);

            // Direct I/O requires the buffer to be sector-aligned.
            const message_buffer =
                try allocator.alignedAlloc(u8, constants.sector_size, constants.message_size_max);
            defer allocator.free(message_buffer);
            @memset(message_buffer, 0);

            for (0..constants.clients_max) |slot| {
                storage.write_sectors(
                    write_sectors_callback,
                    &self.write,
                    message_buffer,
                    .client_replies,
                    slot * constants.message_size_max,
                    .{ .dsync = false, .format = true },
                );
                self.formatting = true;
                while (self.formatting) storage.tick();
            }
        }

        fn format_grid_padding(
            self: *ReplicaFormat,
            allocator: std.mem.Allocator,
            storage: *Storage,
        ) !void {
            assert(!self.formatting);

            const padding_size = vsr.Zone.size(.grid_padding).?;
            assert(padding_size < constants.block_size);

            if (padding_size > 0) {
                // Direct I/O requires the buffer to be sector-aligned.
                const padding_buffer = try allocator.alignedAlloc(
                    u8,
                    constants.sector_size,
                    vsr.Zone.size(.grid_padding).?,
                );
                defer allocator.free(padding_buffer);
                @memset(padding_buffer, 0);

                storage.write_sectors(
                    write_sectors_callback,
                    &self.write,
                    padding_buffer,
                    .grid_padding,
                    0,
                    .{ .dsync = false, .format = true },
                );
                self.formatting = true;
                while (self.formatting) storage.tick();
            }
        }

        fn write_sectors_callback(write: *Storage.Write) void {
            const self: *ReplicaFormat = @alignCast(@fieldParentPtr("write", write));
            assert(self.formatting);
            self.formatting = false;
        }

        fn format_superblock_callback(superblock_context: *SuperBlock.Context) void {
            const self: *ReplicaFormat =
                @alignCast(@fieldParentPtr("superblock_context", superblock_context));
            assert(self.formatting);
            self.formatting = false;
        }

        fn flush_sectors_callback(flush: *Storage.Flush) void {
            const self: *ReplicaFormat = @alignCast(@fieldParentPtr("flush", flush));
            assert(self.formatting);
            self.formatting = false;
        }
    };
}

test "format" {
    const data_file_size_min = @import("./superblock.zig").data_file_size_min;
    const Storage = @import("../testing/storage.zig").Storage;
    const SuperBlock = vsr.SuperBlockType(Storage);
    const allocator = std.testing.allocator;
    const cluster = 0;
    const replica = 1;
    const replica_count = 1;

    var storage = try Storage.init(
        allocator,
        data_file_size_min,
        .{
            .read_latency_min = 0,
            .read_latency_mean = 0,
            .write_latency_min = 0,
            .write_latency_mean = 0,
        },
    );
    defer storage.deinit(allocator);

    var superblock = try SuperBlock.init(allocator, .{
        .storage = &storage,
        .storage_size_limit = data_file_size_min,
    });
    defer superblock.deinit(allocator);

    try format(Storage, allocator, .{
        .cluster = cluster,
        .release = vsr.Release.minimum,
        .replica = replica,
        .replica_count = replica_count,
    }, &storage, &superblock);

    // Verify the superblock headers.
    var copy: u8 = 0;
    while (copy < constants.superblock_copies) : (copy += 1) {
        const superblock_header = storage.superblock_header(copy);

        try std.testing.expectEqual(superblock_header.copy, copy);
        try std.testing.expectEqual(superblock_header.cluster, cluster);
        try std.testing.expectEqual(superblock_header.sequence, 1);
        try std.testing.expectEqual(
            superblock_header.vsr_state.checkpoint.storage_size,
            storage.size,
        );
        try std.testing.expectEqual(superblock_header.vsr_state.checkpoint.header.op, 0);
        try std.testing.expectEqual(superblock_header.vsr_state.commit_max, 0);
        try std.testing.expectEqual(superblock_header.vsr_state.view, 0);
        try std.testing.expectEqual(superblock_header.vsr_state.log_view, 0);
        try std.testing.expectEqual(
            superblock_header.vsr_state.replica_id,
            superblock_header.vsr_state.members[replica],
        );
        try std.testing.expectEqual(superblock_header.vsr_state.replica_count, replica_count);
    }

    // Verify the WAL headers and prepares zones.
    for (storage.wal_headers(), storage.wal_prepares(), 0..) |header, *message, slot| {
        try std.testing.expect(std.meta.eql(header, message.header));

        try std.testing.expect(header.valid_checksum());
        try std.testing.expect(header.valid_checksum_body(&[0]u8{}));
        try std.testing.expectEqual(header.invalid(), null);
        try std.testing.expectEqual(header.cluster, cluster);
        try std.testing.expectEqual(header.op, slot);
        try std.testing.expectEqual(header.size, @sizeOf(vsr.Header));
        try std.testing.expectEqual(header.command, .prepare);
        if (slot == 0) {
            try std.testing.expectEqual(header.operation, .root);
        } else {
            try std.testing.expectEqual(header.operation, .reserved);
        }
    }

    // Verify client replies.
    try std.testing.expectEqual(storage.client_replies().len, constants.clients_max);
    try std.testing.expect(stdx.zeroed(
        storage.memory[vsr.Zone.client_replies.offset(0)..][0..vsr.Zone.client_replies.size().?],
    ));

    // Verify grid padding.
    const padding_size = vsr.Zone.grid_padding.size().?;
    if (padding_size > 0) {
        try std.testing.expect(stdx.zeroed(
            storage.memory[vsr.Zone.grid_padding.offset(0)..][0..vsr.Zone.grid_padding.size().?],
        ));
    }
}
