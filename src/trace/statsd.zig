const std = @import("std");

const IO = @import("io.zig").IO;
const FIFOType = @import("fifo.zig").FIFOType;

const max_packet_size = 1400;
const max_packet_count = 256;

const RingBufferType = @import("../ring_buffer.zig").RingBufferType;

const BufferCompletion = struct {
    buffer: [max_packet_size]u8,
    completion: IO.Completion = undefined,
};

const BufferCompletionRing = RingBufferType(BufferCompletion, .{
    .slice,
});

pub const StatsD = struct {
    socket: std.posix.socket_t,
    io: *IO,
    buffer_completions: BufferCompletionRing,

    /// Creates a statsd instance, which will send UDP packets via the IO instance provided.
    pub fn init(allocator: std.mem.Allocator, io: *IO, address: std.net.Address) !StatsD {
        const socket = try io.open_socket(
            address.any.family,
            std.posix.SOCK.DGRAM,
            std.posix.IPPROTO.UDP,
        );
        errdefer io.close_socket(socket);

        const buffer_completions = BufferCompletionRing.init(allocator, max_packet_count);

        // 'Connect' the UDP socket, so we can just send() to it normally.
        try std.posix.connect(socket, &address.any, address.getOsSockLen());

        return .{
            .socket = socket,
            .io = io,
            .buffer_completions = buffer_completions,
        };
    }

    pub fn deinit(self: *StatsD, allocator: std.mem.Allocator) void {
        self.io.close_socket(self.socket);
        self.buffer_completions.deinit(allocator);
    }

    fn send_callback(
        self: *StatsD,
        completion: *IO.Completion,
        result: IO.SendError!usize,
    ) void {
        _ = result catch {};
        const buffer_completion: *BufferCompletion = @fieldParentPtr("completion", completion);
        self.buffer_completions.push_assume_capacity(buffer_completion);
    }
};
