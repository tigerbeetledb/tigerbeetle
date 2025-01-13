const std = @import("std");
const stdx = @import("../stdx.zig");

const IO = @import("../io.zig").IO;
const FIFOType = @import("../fifo.zig").FIFOType;

const max_packet_size = 1400;
const max_packet_count = 256;

const RingBufferType = @import("../ring_buffer.zig").RingBufferType;
const EventTiming = @import("event.zig").EventTiming;
const EventMetric = @import("event.zig").EventMetric;
const EventTimingAggregate = @import("event.zig").EventTimingAggregate;
const EventMetricAggregate = @import("event.zig").EventMetricAggregate;

const BufferCompletion = struct {
    buffer: [max_packet_size]u8,
    completion: IO.Completion = undefined,
};

// fixme: hmm pop push and pointers
const BufferCompletionRing = RingBufferType(*BufferCompletion, .{ .array = max_packet_count });

pub const StatsD = struct {
    socket: std.posix.socket_t,
    io: *IO,
    buffer_completions: BufferCompletionRing,
    buffer_completions_buffer: []BufferCompletion,

    /// Creates a statsd instance, which will send UDP packets via the IO instance provided.
    pub fn init(allocator: std.mem.Allocator, io: *IO, address: std.net.Address) !StatsD {
        const socket = try io.open_socket(
            address.any.family,
            std.posix.SOCK.DGRAM,
            std.posix.IPPROTO.UDP,
        );
        errdefer io.close_socket(socket);

        var buffer_completions = BufferCompletionRing.init();
        const buffer_completions_buffer = try allocator.alloc(BufferCompletion, max_packet_count);
        for (buffer_completions_buffer) |*buffer_completion| {
            buffer_completions.push_assume_capacity(buffer_completion);
        }

        // 'Connect' the UDP socket, so we can just send() to it normally.
        // FIXME: In io.zig, connrefused etc should be handled better!
        try std.posix.connect(socket, &address.any, address.getOsSockLen());

        return .{
            .socket = socket,
            .io = io,
            .buffer_completions = buffer_completions,
            .buffer_completions_buffer = buffer_completions_buffer,
        };
    }

    pub fn deinit(self: *StatsD, allocator: std.mem.Allocator) void {
        self.io.close_socket(self.socket);
        allocator.free(self.buffer_completions_buffer);
        self.buffer_completions.deinit(allocator);
    }

    pub fn emit_timing_and_reset(self: *StatsD, events_aggregate: []?EventTimingAggregate) !void {
        // At some point, would it be more efficient to use a hashmap here...?
        var buffer_completion = self.buffer_completions.pop() orelse return error.NoSpaceLeft;
        var index: usize = 0;

        // FIXME: Comptime lenght limits, must be under a packet...
        var single_buffer: [max_packet_size]u8 = undefined;
        std.debug.assert(single_buffer.len <= self.buffer_completions_buffer[0].buffer.len);

        for (events_aggregate, 0..) |maybe_event_aggregate, i| {
            if (maybe_event_aggregate) |event_timing| {
                const timing = event_timing.timing;
                const field_name = switch (event_timing.event) {
                    inline else => |_, tag| @tagName(tag),
                };
                const event_timing_tag_formatter = EventStatsdTagFormatter(EventTiming){
                    .event = event_timing.event,
                };

                inline for (.{ .min, .avg, .max, .sum, .count }) |aggregation| {
                    const value = switch (aggregation) {
                        .min => timing.duration_min_us,
                        .avg => @divFloor(timing.duration_sum_us, timing.count),
                        .max => timing.duration_max_us,
                        .sum => timing.duration_sum_us,
                        .count => timing.count,
                        else => unreachable,
                    };
                    const single_metric = try std.fmt.bufPrint(
                        &single_buffer,
                        "tigerbeetle.{s}_us.{s}:{}|g|#{s}\n",
                        .{ field_name, @tagName(aggregation), value, event_timing_tag_formatter },
                    );

                    // Might need a new buffer, if this one is full.
                    if (single_metric.len > buffer_completion.buffer[index..].len) {
                        self.io.send(
                            *StatsD,
                            self,
                            StatsD.send_callback,
                            &buffer_completion.completion,
                            self.socket,
                            buffer_completion.buffer[0..index],
                        );

                        buffer_completion = self.buffer_completions.pop() orelse return error.NoSpaceLeft;

                        index = 0;
                        std.debug.assert(buffer_completion.buffer[index..].len > single_metric.len);
                    }

                    stdx.copy_disjoint(.inexact, u8, buffer_completion.buffer[index..], single_metric);
                    index += single_metric.len;
                }

                events_aggregate[i] = null;
            }
        }

        // Send the final packet, if needed.
        if (index > 0) {
            self.io.send(
                *StatsD,
                self,
                StatsD.send_callback,
                &buffer_completion.completion,
                self.socket,
                buffer_completion.buffer[0..index],
            );
        }
    }

    pub fn emit_metric_and_reset(self: *StatsD, events_metric: []?EventMetricAggregate) !void {
        // At some point, would it be more efficient to use a hashmap here...?
        var buffer_completion = self.buffer_completions.pop() orelse return error.NoSpaceLeft;
        var index: usize = 0;

        // FIXME: Comptime lenght limits, must be under a packet...
        var single_buffer: [max_packet_size]u8 = undefined;
        std.debug.assert(single_buffer.len <= self.buffer_completions_buffer[0].buffer.len);

        for (events_metric, 0..) |maybe_event_aggregate, i| {
            if (maybe_event_aggregate) |event_metric| {
                const value = event_metric.value;
                const field_name = switch (event_metric.event) {
                    inline else => |_, tag| @tagName(tag),
                };
                const event_metric_tag_formatter = EventStatsdTagFormatter(EventMetric){
                    .event = event_metric.event,
                };

                const single_metric = try std.fmt.bufPrint(
                    &single_buffer,
                    "tigerbeetle.{s}:{}|g|#{s}\n",
                    .{ field_name, value, event_metric_tag_formatter },
                );

                // Might need a new buffer, if this one is full.
                if (single_metric.len > buffer_completion.buffer[index..].len) {
                    self.io.send(
                        *StatsD,
                        self,
                        StatsD.send_callback,
                        &buffer_completion.completion,
                        self.socket,
                        buffer_completion.buffer[0..index],
                    );

                    buffer_completion = self.buffer_completions.pop() orelse return error.NoSpaceLeft;

                    index = 0;
                    std.debug.assert(buffer_completion.buffer[index..].len > single_metric.len);
                }

                stdx.copy_disjoint(.inexact, u8, buffer_completion.buffer[index..], single_metric);
                index += single_metric.len;

                events_metric[i] = null;
            }
        }

        // Send the final packet, if needed.
        if (index > 0) {
            self.io.send(
                *StatsD,
                self,
                StatsD.send_callback,
                &buffer_completion.completion,
                self.socket,
                buffer_completion.buffer[0..index],
            );
        }
    }
    fn send_callback(
        self: *StatsD,
        completion: *IO.Completion,
        result: IO.SendError!usize,
    ) void {
        _ = result catch |e| {
            std.log.warn("error sending metric: {}", .{e});
        };
        const buffer_completion: *BufferCompletion = @fieldParentPtr("completion", completion);
        self.buffer_completions.push_assume_capacity(buffer_completion);
    }
};

fn EventStatsdTagFormatter(EventType: type) type {
    return struct {
        event: EventType,

        pub fn format(
            formatter: *const @This(),
            comptime fmt: []const u8,
            options: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            _ = fmt;
            _ = options;

            switch (formatter.event) {
                inline else => |data| {
                    if (@TypeOf(data) == void) {
                        return;
                    }

                    const fields = std.meta.fields(@TypeOf(data));
                    inline for (fields, 0..) |data_field, i| {
                        std.debug.assert(data_field.type == bool or
                            @typeInfo(data_field.type) == .Int or
                            @typeInfo(data_field.type) == .Enum or
                            @typeInfo(data_field.type) == .Union);

                        const data_field_value = @field(data, data_field.name);
                        try writer.writeAll(data_field.name);
                        try writer.writeByte(':');

                        if (@typeInfo(data_field.type) == .Enum or
                            @typeInfo(data_field.type) == .Union)
                        {
                            try writer.print("{s}", .{@tagName(data_field_value)});
                        } else {
                            try writer.print("{}", .{data_field_value});
                        }

                        if (i != fields.len - 1) {
                            try writer.writeByte(',');
                        }
                    }
                },
            }
        }
    };
}
