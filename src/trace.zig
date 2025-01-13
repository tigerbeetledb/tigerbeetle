//! Log IO/CPU event spans for analysis/visualization.
//!
//! Example:
//!
//!     $ ./tigerbeetle start --experimental --trace=trace.json
//!
//! or:
//!
//!     $ ./tigerbeetle benchmark --trace=trace.json
//!
//! The trace JSON output is compatible with:
//! - https://ui.perfetto.dev/
//! - https://gravitymoth.com/spall/spall.html
//! - chrome://tracing/
//!
//! Example integrations:
//!
//!     // Trace a synchronous event.
//!     // The second argument is a `anytype` struct, corresponding to the struct argument to
//!     // `log.debug()`.
//!     tree.grid.trace.start(.compact_mutable, .{ .tree = tree.config.name });
//!     defer tree.grid.trace.stop(.compact_mutable, .{ .tree = tree.config.name });
//!
//! Note that only one of each Event can be running at a time:
//!
//!     // good
//!     trace.start(.foo, .{});
//!     trace.stop(.foo, .{});
//!     trace.start(.bar, .{});
//!     trace.stop(.bar, .{});
//!
//!     // good
//!     trace.start(.foo, .{});
//!     trace.start(.bar, .{});
//!     trace.stop(.foo, .{});
//!     trace.stop(.bar, .{});
//!
//!     // bad
//!     trace.start(.foo, .{});
//!     trace.start(.foo, .{});
//!
//!     // bad
//!     trace.stop(.foo, .{});
//!     trace.start(.foo, .{});
//!
//! If an event is is cancelled rather than properly stopped, use .reset():
//! - Reset is safe to call regardless of whether the event is currently started.
//! - For events with multiple instances (e.g. IO reads and writes), .reset() will
//!   cancel all running traces of the same event.
//!
//!     // good
//!     trace.start(.foo, .{});
//!     trace.reset(.foo);
//!     trace.start(.foo, .{});
//!     trace.stop(.foo, .{});
//!
//! Notes:
//! - When enabled, traces are written to stdout (as opposed to logs, which are written to stderr).
//! - The JSON output is a "[" followed by a comma-separated list of JSON objects. The JSON array is
//!   never closed with a "]", but Chrome, Spall, and Perfetto all handle this.
//! - Event pairing (start/stop) is asserted at runtime.
//! - `trace.start()/.stop()/.reset()` will `log.debug()` regardless of whether tracing is enabled.
//!
//! The JSON output looks like:
//!
//!     {
//!         // Process id:
//!         // The replica index is encoded as the "process id" of trace events, so events from
//!         // multiple replicas of a cluster can be unified to visualize them on the same timeline.
//!         "pid": 0,
//!
//!         // Thread id:
//!         "tid": 0,
//!
//!         // Category.
//!         "cat": "replica_commit",
//!
//!         // Phase.
//!         "ph": "B",
//!
//!         // Timestamp:
//!         // Microseconds since program start.
//!         "ts": 934327,
//!
//!         // Event name:
//!         // Includes the event name and a *low cardinality subset* of the second argument to
//!         // `trace.start()`. (Low-cardinality part so that tools like Perfetto can distinguish
//!         // events usefully.)
//!         "name": "replica_commit stage='next_pipeline'",
//!
//!         // Extra event arguments. (Encoded from the second argument to `trace.start()`).
//!         "args": {
//!             "stage": "next_pipeline",
//!             "op": 1
//!         },
//!     },
//!
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.trace);

const constants = @import("constants.zig");
const IO = @import("io.zig").IO;

const Metrics = @import("trace/metric.zig").Metrics;
const Event = @import("trace/event.zig").Event;
const EventTracing = @import("trace/event.zig").EventTracing;
const EventTiming = @import("trace/event.zig").EventTiming;
const EventAggregate = @import("trace/event.zig").EventAggregate;

const trace_span_size_max = 1024;

pub const Tracer = struct {
    replica_index: u8,
    options: Options,
    buffer: []u8,

    events_started: [EventTracing.stack_count]?u64 = .{null} ** EventTracing.stack_count,
    metrics: Metrics,

    time_start: std.time.Instant,
    timer: std.time.Timer,

    pub const Options = struct {
        /// The tracer still validates start/stop state even when writer=null.
        writer: ?std.io.AnyWriter = null,

        // For metrics: FIXME
        io: ?*IO = null,
        // FIXME: Add statsd address etc. Or move it up and make anytype? PRobs too much for now.
    };

    pub fn init(allocator: std.mem.Allocator, replica_index: u8, options: Options) !Tracer {
        if (options.writer) |writer| {
            try writer.writeAll("[\n");
        }

        const buffer = try allocator.alloc(u8, trace_span_size_max);
        errdefer allocator.free(buffer);

        const metrics = try Metrics.init(allocator, options.io.?);
        errdefer metrics.deinit(allocator);

        return .{
            .replica_index = replica_index,
            .options = options,
            .buffer = buffer,

            .metrics = metrics,

            .time_start = std.time.Instant.now() catch @panic("std.time.Instant.now() unsupported"),
            .timer = std.time.Timer.start() catch @panic("std.time.Timer.start() unsupported"),
        };
    }

    pub fn deinit(tracer: *Tracer, allocator: std.mem.Allocator) void {
        allocator.free(tracer.buffer);
        tracer.metrics.deinit(allocator);
        tracer.* = undefined;
    }

    pub fn start(tracer: *Tracer, event: Event) void {
        const event_tracing = event.event_tracing();
        const event_timing = event.event_timing();
        const stack = event_tracing.stack();

        assert(tracer.events_started[stack] == null);
        tracer.events_started[stack] = tracer.timer.read();

        log.info(
            "{}: {s}({}): start: {}",
            .{ tracer.replica_index, @tagName(event), event_tracing, event_timing },
        );

        const writer = tracer.options.writer orelse return;
        const time_now = std.time.Instant.now() catch unreachable;
        const time_elapsed_ns = time_now.since(tracer.time_start);
        const time_elapsed_us = @divFloor(time_elapsed_ns, std.time.ns_per_us);

        var buffer_stream = std.io.fixedBufferStream(tracer.buffer);

        // String tid's would be much more useful.
        // They are supported by both Chrome and Perfetto, but rejected by Spall.
        buffer_stream.writer().print("{{" ++
            "\"pid\":{[process_id]}," ++
            "\"tid\":{[thread_id]}," ++
            "\"ph\":\"{[event]c}\"," ++
            "\"ts\":{[timestamp]}," ++
            "\"cat\":\"{[category]s}\"," ++
            "\"name\":\"{[category]s} {[event_tracing]s} {[event_timing]}\"," ++
            "\"args\":{[args]s}" ++
            "}},\n", .{
            .process_id = tracer.replica_index,
            .thread_id = event_tracing.stack(),
            .category = @tagName(event),
            .event = 'B',
            .timestamp = time_elapsed_us,
            .event_tracing = event_tracing,
            .event_timing = event_timing,
            .args = std.json.Formatter(Event){ .value = event, .options = .{} },
        }) catch unreachable;

        writer.writeAll(buffer_stream.getWritten()) catch |err| {
            std.debug.panic("Tracer.start: {}\n", .{err});
        };
    }

    pub fn stop(tracer: *Tracer, event: Event) void {
        const us_log_threshold_ns = 5 * std.time.ns_per_ms;

        const event_tracing = event.event_tracing();
        const event_timing = event.event_timing();
        const stack = event_tracing.stack();

        const event_start_ns = tracer.events_started[stack].?;
        const event_end_ns = tracer.timer.read();
        const duration_ns = event_end_ns - event_start_ns;
        const duration_us = @divFloor(duration_ns, std.time.ns_per_us);

        assert(tracer.events_started[stack] != null);
        tracer.events_started[stack] = null;

        // Double leading space to align with 'start: '.
        log.info("{}: {s}({}): stop:  {} (duration={}{s})", .{
            tracer.replica_index,
            @tagName(event),
            event_tracing,
            event_timing,
            if (duration_ns < us_log_threshold_ns)
                duration_us
            else
                @divFloor(duration_ns, std.time.ns_per_ms),
            if (duration_ns < us_log_threshold_ns) "us" else "ms",
        });

        tracer.metrics.timing(event_timing, duration_us);

        tracer.write_stop(stack);
    }

    pub fn reset(tracer: *Tracer, event_tag: Event.EventTag) void {
        const stack_base = EventTracing.stack_bases.get(event_tag);
        const cardinality = EventTracing.stack_limits.get(event_tag);
        for (stack_base..stack_base + cardinality) |stack| {
            if (tracer.events_started[stack]) |_| {
                log.debug("{}: {s}: reset", .{ tracer.replica_index, @tagName(event_tag) });

                tracer.events_started[stack] = null;
                tracer.write_stop(@intCast(stack));
            }
        }

        // FIXME: Reset metrics too.
    }

    fn write_stop(tracer: *Tracer, stack: u32) void {
        const writer = tracer.options.writer orelse return;
        const time_now = std.time.Instant.now() catch unreachable;
        const time_elapsed_ns = time_now.since(tracer.time_start);
        const time_elapsed_us = @divFloor(time_elapsed_ns, std.time.ns_per_us);

        var buffer_stream = std.io.fixedBufferStream(tracer.buffer);

        buffer_stream.writer().print(
            "{{" ++
                "\"pid\":{[process_id]}," ++
                "\"tid\":{[thread_id]}," ++
                "\"ph\":\"{[event]c}\"," ++
                "\"ts\":{[timestamp]}" ++
                "}},\n",
            .{
                .process_id = tracer.replica_index,
                .thread_id = stack,
                .event = 'E',
                .timestamp = time_elapsed_us,
            },
        ) catch unreachable;

        writer.writeAll(buffer_stream.getWritten()) catch |err| {
            std.debug.panic("Tracer.stop: {}\n", .{err});
        };
    }

    pub fn emit(tracer: *Tracer) void {
        tracer.start(.metrics_emit);
        tracer.metrics.emit() catch |e| {
            log.warn("error while emitting metrics: {}", .{e});
        };
        tracer.stop(.metrics_emit);
    }
};

test "trace json" {
    const Snap = @import("testing/snaptest.zig").Snap;
    const snap = Snap.snap;

    var trace_buffer = std.ArrayList(u8).init(std.testing.allocator);
    defer trace_buffer.deinit();

    var trace = try Tracer.init(std.testing.allocator, 0, .{
        .writer = trace_buffer.writer().any(),
    });
    defer trace.deinit(std.testing.allocator);

    trace.start(.{ .replica_commit = .{ .stage = .idle, .op = 123 } });
    trace.start(.{ .compact_beat = .{ .tree = @enumFromInt(1), .level_b = 1 } });
    trace.stop(.{ .compact_beat = .{ .tree = @enumFromInt(1), .level_b = 1 } });
    trace.stop(.{ .replica_commit = .{ .stage = .idle, .op = 456 } });

    try snap(@src(),
        \\[
        \\{"pid":0,"tid":0,"ph":"B","ts":<snap:ignore>,"cat":"replica_commit","name":"replica_commit","args":{"foo":123}},
        \\{"pid":0,"tid":4,"ph":"B","ts":<snap:ignore>,"cat":"compact_beat","name":"compact_beat","args":{}},
        \\{"pid":0,"tid":4,"ph":"E","ts":<snap:ignore>},
        \\{"pid":0,"tid":0,"ph":"E","ts":<snap:ignore>},
        \\
    ).diff(trace_buffer.items);
}
