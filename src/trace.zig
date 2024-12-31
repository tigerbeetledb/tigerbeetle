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
const sync = @import("vsr/sync.zig");

const trace_span_size_max = 1024;

pub const Event = union(enum) {
    replica_commit,
    replica_aof_write,
    replica_sync_stage,
    replica_sync_table: struct { index: usize },

    compact_beat,
    compact_beat_merge,
    compact_manifest,
    compact_mutable,
    compact_mutable_suffix,

    lookup,
    lookup_worker: struct { index: u8 },

    scan_tree: struct { index: u8 },
    scan_tree_level: struct { index: u8, level: u8 },

    grid_read: struct { iop: usize },
    grid_write: struct { iop: usize },

    pub fn format(
        event: *const Event,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        try writer.writeAll(@tagName(event.*));
        switch (event.*) {
            inline else => |data| {
                if (@TypeOf(data) != void) {
                    try writer.print(":{}", .{struct_format(data, .dense)});
                }
            },
        }
    }

    const EventTag = std.meta.Tag(Event);

    const event_stack_cardinality = std.enums.EnumArray(EventTag, u32).init(.{
        .replica_commit = 1,
        .replica_aof_write = 1,
        .replica_sync_stage = 1,
        .replica_sync_table = constants.grid_missing_tables_max,
        .compact_beat = 1,
        .compact_beat_merge = 1,
        .compact_manifest = 1,
        .compact_mutable = 1,
        .compact_mutable_suffix = 1,
        .lookup = 1,
        .lookup_worker = constants.grid_iops_read_max,
        .scan_tree = constants.lsm_scans_max,
        .scan_tree_level = constants.lsm_scans_max * @as(u32, constants.lsm_levels),
        .grid_read = constants.grid_iops_read_max,
        .grid_write = constants.grid_iops_write_max,
    });

    const stack_count = count: {
        var count: u32 = 0;
        for (std.enums.values(EventTag)) |event_type| {
            count += event_stack_cardinality.get(event_type);
        }
        break :count count;
    };

    const event_stack_base = array: {
        var array = std.enums.EnumArray(EventTag, u32).initDefault(0, .{});
        var next: u32 = 0;
        for (std.enums.values(EventTag)) |event_type| {
            array.set(event_type, next);
            next += event_stack_cardinality.get(event_type);
        }
        break :array array;
    };

    // Stack is a u32 since it must be losslessly encoded as a JSON integer.
    fn stack(event: *const Event) u32 {
        switch (event.*) {
            inline .replica_sync_table,
            .lookup_worker,
            => |data| {
                assert(data.index < event_stack_cardinality.get(event.*));
                const stack_base = event_stack_base.get(event.*);
                return stack_base + @as(u32, @intCast(data.index));
            },
            .scan_tree => |data| {
                assert(data.index < constants.lsm_scans_max);
                // This event has "nested" sub-events, so its offset is calculated
                // with padding to accommodate `scan_tree_level` events in between.
                const stack_base = event_stack_base.get(event.*);
                const scan_tree_offset = (constants.lsm_levels + 1) * data.index;
                return stack_base + scan_tree_offset;
            },
            .scan_tree_level => |data| {
                assert(data.index < constants.lsm_scans_max);
                assert(data.level < constants.lsm_levels);
                // This is a "nested" event, so its offset is calculated
                // relative to the parent `scan_tree`'s offset.
                const stack_base = event_stack_base.get(.scan_tree);
                const scan_tree_offset = (constants.lsm_levels + 1) * data.index;
                const scan_tree_level_offset = data.level + 1;
                return stack_base + scan_tree_offset + scan_tree_level_offset;
            },
            inline .grid_read, .grid_write => |data| {
                assert(data.iop < event_stack_cardinality.get(event.*));
                const stack_base = event_stack_base.get(event.*);
                return stack_base + @as(u32, @intCast(data.iop));
            },
            inline else => |data, event_tag| {
                comptime assert(@TypeOf(data) == void);
                comptime assert(event_stack_cardinality.get(event_tag) == 1);
                return comptime event_stack_base.get(event_tag);
            },
        }
    }
};

pub const Tracer = struct {
    replica_index: u8,
    options: Options,
    buffer: []u8,

    events_started: [Event.stack_count]?u64 = .{null} ** Event.stack_count,
    time_start: std.time.Instant,
    timer: std.time.Timer,

    pub const Options = struct {
        /// The tracer still validates start/stop state even when writer=null.
        writer: ?std.io.AnyWriter = null,
    };

    pub fn init(allocator: std.mem.Allocator, replica_index: u8, options: Options) !Tracer {
        if (options.writer) |writer| {
            try writer.writeAll("[\n");
        }

        const buffer = try allocator.alloc(u8, trace_span_size_max);
        errdefer allocator.free(buffer);

        return .{
            .replica_index = replica_index,
            .options = options,
            .buffer = buffer,
            .time_start = std.time.Instant.now() catch @panic("std.time.Instant.now() unsupported"),
            .timer = std.time.Timer.start() catch @panic("std.time.Timer.start() unsupported"),
        };
    }

    pub fn deinit(tracer: *Tracer, allocator: std.mem.Allocator) void {
        allocator.free(tracer.buffer);
        tracer.* = undefined;
    }

    pub fn start(tracer: *Tracer, event: Event, data: anytype) void {
        comptime assert(@typeInfo(@TypeOf(data)) == .Struct);

        const stack = event.stack();
        assert(tracer.events_started[stack] == null);
        tracer.events_started[stack] = tracer.timer.read();

        log.debug(
            "{}: {}: start:{}",
            .{ tracer.replica_index, event, struct_format(data, .dense) },
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
            "\"name\":\"{[name]s}{[data]}\"," ++
            "\"args\":{[args]s}" ++
            "}},\n", .{
            .process_id = tracer.replica_index,
            .thread_id = event.stack(),
            .category = @tagName(event),
            .event = 'B',
            .timestamp = time_elapsed_us,
            .name = event,
            .data = struct_format(data, .sparse),
            .args = if (comptime @TypeOf(data) == @TypeOf(.{}))
                "{}" // Serialize `.{}` as an empty object, not as an empty array.
            else
                std.json.Formatter(@TypeOf(data)){ .value = data, .options = .{} },
        }) catch unreachable;

        writer.writeAll(buffer_stream.getWritten()) catch |err| {
            std.debug.panic("Tracer.start: {}\n", .{err});
        };
    }

    pub fn stop(tracer: *Tracer, event: Event, data: anytype) void {
        comptime assert(@typeInfo(@TypeOf(data)) == .Struct);

        const stack = event.stack();
        const event_start_ns = tracer.events_started[stack].?;
        const event_end_ns = tracer.timer.read();

        assert(tracer.events_started[stack] != null);
        tracer.events_started[stack] = null;

        log.debug("{}: {}: stop:{} (duration={}ms)", .{
            tracer.replica_index,
            event,
            struct_format(data, .dense),
            @divFloor(event_end_ns - event_start_ns, std.time.ns_per_ms),
        });

        tracer.write_stop(stack, data);
    }

    pub fn reset(tracer: *Tracer, event_tag: Event.EventTag) void {
        const stack_base = Event.event_stack_base.get(event_tag);
        const cardinality = Event.event_stack_cardinality.get(event_tag);
        for (stack_base..stack_base + cardinality) |stack| {
            if (tracer.events_started[stack]) |_| {
                log.debug("{}: {s}: reset", .{ tracer.replica_index, @tagName(event_tag) });

                tracer.events_started[stack] = null;
                tracer.write_stop(@intCast(stack), .{});
            }
        }
    }

    fn write_stop(tracer: *Tracer, stack: u32, data: anytype) void {
        comptime assert(@typeInfo(@TypeOf(data)) == .Struct);

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
};

const DataFormatterCardinality = enum { dense, sparse };

fn StructFormatterType(comptime Data: type, comptime cardinality: DataFormatterCardinality) type {
    assert(@typeInfo(Data) == .Struct);

    return struct {
        data: Data,

        pub fn format(
            formatter: *const @This(),
            comptime fmt: []const u8,
            options: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            _ = fmt;
            _ = options;

            inline for (std.meta.fields(Data)) |data_field| {
                if (cardinality == .sparse) {
                    if (data_field.type != bool and
                        data_field.type != u8 and
                        data_field.type != []const u8 and
                        data_field.type != [:0]const u8 and
                        @typeInfo(data_field.type) != .Enum and
                        @typeInfo(data_field.type) != .Union)
                    {
                        continue;
                    }
                }

                const data_field_value = @field(formatter.data, data_field.name);
                try writer.writeByte(' ');
                try writer.writeAll(data_field.name);
                try writer.writeByte('=');

                if (data_field.type == []const u8 or
                    data_field.type == [:0]const u8)
                {
                    // This is an arbitrary limit, to ensure that the logging isn't too noisy.
                    // (Logged strings should be low-cardinality as well, but we can't explicitly
                    // check that.)
                    const string_length_max = 256;
                    assert(data_field_value.len <= string_length_max);

                    // Since the string is not properly escaped before printing, assert that it
                    // doesn't contain any special characters that would mess with the JSON.
                    if (constants.verify) {
                        for (data_field_value) |char| {
                            assert(char != '\n');
                            assert(char != '\\');
                            assert(char != '"');
                        }
                    }

                    try writer.print("{s}", .{data_field_value});
                } else if (@typeInfo(data_field.type) == .Enum or
                    @typeInfo(data_field.type) == .Union)
                {
                    try writer.print("{s}", .{@tagName(data_field_value)});
                } else {
                    try writer.print("{}", .{data_field_value});
                }
            }
        }
    };
}

fn struct_format(
    data: anytype,
    comptime cardinality: DataFormatterCardinality,
) StructFormatterType(@TypeOf(data), cardinality) {
    return StructFormatterType(@TypeOf(data), cardinality){ .data = data };
}

test "trace json" {
    const Snap = @import("testing/snaptest.zig").Snap;
    const snap = Snap.snap;

    var trace_buffer = std.ArrayList(u8).init(std.testing.allocator);
    defer trace_buffer.deinit();

    var trace = try Tracer.init(std.testing.allocator, 0, .{
        .writer = trace_buffer.writer().any(),
    });
    defer trace.deinit(std.testing.allocator);

    trace.start(.replica_commit, .{ .foo = 123 });
    trace.start(.compact_beat, .{});
    trace.stop(.compact_beat, .{});
    trace.stop(.replica_commit, .{ .bar = 456 });

    try snap(@src(),
        \\[
        \\{"pid":0,"tid":0,"ph":"B","ts":<snap:ignore>,"cat":"replica_commit","name":"replica_commit","args":{"foo":123}},
        \\{"pid":0,"tid":4,"ph":"B","ts":<snap:ignore>,"cat":"compact_beat","name":"compact_beat","args":{}},
        \\{"pid":0,"tid":4,"ph":"E","ts":<snap:ignore>},
        \\{"pid":0,"tid":0,"ph":"E","ts":<snap:ignore>},
        \\
    ).diff(trace_buffer.items);
}
