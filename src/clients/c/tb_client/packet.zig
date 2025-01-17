const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const FIFOType = @import("../tb_client.zig").vsr.fifo.FIFOType;

pub const Packet = extern struct {
    next: ?*Packet,
    user_data: ?*anyopaque,
    operation: u8,
    status: Status,
    data_size: u32,
    data: ?*anyopaque,
    batch_next: ?*Packet,
    batch_tail: ?*Packet,
    batch_size: u32,
    batch_allowed: bool,
    reserved: [7]u8 = [_]u8{0} ** 7,

    comptime {
        assert(@sizeOf(Packet) == 64);
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
};
