const std = @import("std");

const IO = @import("../io.zig").IO;
const StatsD = @import("statsd.zig").StatsD;
const Event = @import("event.zig").Event;
const EventTiming = @import("event.zig").EventTiming;
const EventMetric = @import("event.zig").EventMetric;
const EventTimingAggregate = @import("event.zig").EventTimingAggregate;
const EventMetricAggregate = @import("event.zig").EventMetricAggregate;

pub const Metrics = struct {
    events_timing: []?EventTimingAggregate,
    events_metric: []?EventMetricAggregate,

    statsd: StatsD,

    pub fn init(allocator: std.mem.Allocator, io: *IO) !Metrics {
        const events_timing = try allocator.alloc(?EventTimingAggregate, EventTiming.stack_count);
        errdefer allocator.free(events_timing);

        @memset(events_timing, null);

        const events_metric = try allocator.alloc(?EventMetricAggregate, EventMetric.stack_count);
        errdefer allocator.free(events_metric);

        @memset(events_metric, null);

        const statsd = try StatsD.init(allocator, io, try std.net.Address.resolveIp("127.0.0.1", 8125));
        errdefer statsd.deinit(allocator);

        return .{
            .events_timing = events_timing,
            .events_metric = events_metric,
            .statsd = statsd,
        };
    }

    pub fn deinit(self: *Metrics, allocator: std.mem.Allocator) void {
        allocator.free(self.events_metric);
        allocator.free(self.events_timing);
    }

    /// Gauges work on a last-set wins. Multiple calls to .gauge() followed by an emit will result
    /// in the last value being submitted.
    pub fn gauge(self: *Metrics, event_metric: EventMetric, value: u64) void {
        const timing_stack = event_metric.stack();
        self.events_metric[timing_stack] = .{
            .event = event_metric,
            .value = value,
        };
    }

    // Timing works by storing the min, max, sum and count of each value provided. The avg is
    // calculated from sum and count at emit time. More advanced streaming quantile calculation is
    // not done.
    pub fn timing(self: *Metrics, event_timing: EventTiming, duration_us: u64) void {
        const timing_stack = event_timing.stack();

        if (self.events_timing[timing_stack] == null) {
            self.events_timing[timing_stack] = .{
                .event = event_timing,
                .values = .{
                    .duration_min_us = duration_us,
                    .duration_max_us = duration_us,
                    .duration_sum_us = duration_us,
                    .count = 1,
                },
            };
        } else {
            const timing_existing = self.events_timing[timing_stack].?.values;
            // Certain high cardinality data (eg, op) _can_ differ.
            // Maybe assert and gate on constants.verify
            //maybe(self.events_timing[timing_stack].?.event_timing == event_timing);

            self.events_timing[timing_stack].?.values = .{
                .duration_min_us = @min(timing_existing.duration_min_us, duration_us),
                .duration_max_us = @max(timing_existing.duration_max_us, duration_us),
                .duration_sum_us = timing_existing.duration_sum_us + duration_us,
                .count = timing_existing.count + 1,
            };
        }
    }

    pub fn emit(self: *Metrics) !void {
        try self.statsd.emit_timing_and_reset(self.events_timing);
        try self.statsd.emit_metric_and_reset(self.events_metric);
    }
};
