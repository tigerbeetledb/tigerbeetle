const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const FIFOType = @import("../tb_client.zig").vsr.fifo.FIFOType;

// When referenced from unit_test.zig, there is no vsr import module so use path.
const vsr = if (@import("root") == @This()) @import("vsr") else @import("../../../vsr.zig");
const stdx = vsr.stdx;

pub const Packet = extern struct {
    next: ?*Packet,
    user_data: ?*anyopaque,
    data: ?*anyopaque,
    data_size: u32,
    operation: u8,
    status: Status,
    reserved: [2]u8 = [_]u8{0} ** 2,

    comptime {
        assert(@sizeOf(Packet) == 32);
        assert(@alignOf(Packet) == 8);
    }

    pub const Status = enum(u8) {
        ok,
        too_much_data,
        client_evicted,
        client_release_too_low,
        client_release_too_high,
        client_shutdown,
        invalid_operation,
        invalid_data_size,
    };

    /// Thread-safe FIFO.
    pub const SubmissionQueue = struct {
        fifo: FIFOType(Packet) = .{
            .name = null,
            .verify_push = builtin.is_test,
        },
        mutex: std.Thread.Mutex = .{},

        pub fn push(self: *SubmissionQueue, packet: *Packet) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.fifo.push(packet);
        }

        pub fn pop(self: *SubmissionQueue) ?*Packet {
            self.mutex.lock();
            defer self.mutex.unlock();

            return self.fifo.pop();
        }

        pub fn empty(self: *SubmissionQueue) bool {
            self.mutex.lock();
            defer self.mutex.unlock();

            return self.fifo.count == 0;
        }
    };

    pub fn events(packet: *const Packet) []const u8 {
        if (packet.data_size == 0) {
            // It may be an empty array (null pointer)
            // or a buffer with no elements (valid pointer and size == 0).
            stdx.maybe(packet.data == null);
            return &[0]u8{};
        }

        const data: [*]const u8 = @ptrCast(packet.data.?);
        return data[0..packet.data_size];
    }
};
