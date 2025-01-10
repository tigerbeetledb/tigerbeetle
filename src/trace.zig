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

const trace_span_size_max = 1024;

// FIXME hack for now
const CommitStage = @import("vsr/replica.zig").CommitStage;
const TreeEnum = @import("state_machine.zig").TreeEnum;

pub const Event = union(enum) {
    replica_commit: struct { stage: CommitStage, op: ?usize = null },
    replica_aof_write: struct { op: usize },
    replica_sync_table: struct { index: usize },

    compact_beat: struct { tree: TreeEnum, level_b: u8 },
    compact_beat_merge: struct { tree: TreeEnum, level_b: u8 },
    compact_manifest,
    compact_mutable: struct { tree: TreeEnum },
    compact_mutable_suffix: struct { tree: TreeEnum },

    lookup: struct { tree: TreeEnum },
    lookup_worker: struct { index: u8, tree: TreeEnum },

    scan_tree: struct { index: u8, tree: TreeEnum },
    scan_tree_level: struct { index: u8, tree: TreeEnum, level: u8 },

    grid_read: struct { iop: usize },
    grid_write: struct { iop: usize },

    const EventTag = std.meta.Tag(Event);

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

    fn data_concurrency(event: *const Event) EventConcurrency {
        return switch (event.*) {
            .replica_commit => .replica_commit,
            .replica_aof_write => .replica_aof_write,
            .replica_sync_table => |event_unwrapped| .{ .replica_sync_table = .{ .index = event_unwrapped.index } },
            .compact_beat => .compact_beat,
            .compact_beat_merge => .compact_beat_merge,
            .compact_manifest => .compact_manifest,
            .compact_mutable => .compact_mutable,
            .compact_mutable_suffix => .compact_mutable_suffix,
            .lookup => .lookup,
            .lookup_worker => |event_unwrapped| .{ .lookup_worker = .{ .index = event_unwrapped.index } },
            .scan_tree => |event_unwrapped| .{ .scan_tree = .{ .index = event_unwrapped.index } },
            .scan_tree_level => |event_unwrapped| .{ .scan_tree_level = .{ .index = event_unwrapped.index, .level = event_unwrapped.level } },
            .grid_read => |event_unwrapped| .{ .grid_read = .{ .iop = event_unwrapped.iop } },
            .grid_write => |event_unwrapped| .{ .grid_write = .{ .iop = event_unwrapped.iop } },
        };
    }

    fn data_cardinality(event: *const Event) EventCardinality {
        return switch (event.*) {
            .replica_commit => |event_unwrapped| .{ .replica_commit = .{ .stage = event_unwrapped.stage } },
            .replica_aof_write => .replica_aof_write,
            .replica_sync_table => .replica_sync_table,
            .compact_beat => |event_unwrapped| .{ .compact_beat = .{ .tree = event_unwrapped.tree, .level_b = event_unwrapped.level_b } },
            .compact_beat_merge => |event_unwrapped| .{ .compact_beat_merge = .{ .tree = event_unwrapped.tree, .level_b = event_unwrapped.level_b } },
            .compact_manifest => .compact_manifest,
            .compact_mutable => |event_unwrapped| .{ .compact_mutable = .{ .tree = event_unwrapped.tree } },
            .compact_mutable_suffix => |event_unwrapped| .{ .compact_mutable_suffix = .{ .tree = event_unwrapped.tree } },
            .lookup => |event_unwrapped| .{ .lookup = .{ .tree = event_unwrapped.tree } },
            .lookup_worker => |event_unwrapped| .{ .lookup_worker = .{ .tree = event_unwrapped.tree } },
            .scan_tree => |event_unwrapped| .{ .scan_tree = .{ .tree = event_unwrapped.tree } },
            .scan_tree_level => |event_unwrapped| .{ .scan_tree_level = .{ .tree = event_unwrapped.tree, .level = event_unwrapped.level } },
            .grid_read => .grid_read,
            .grid_write => .grid_write,
        };
    }
};

/// There's a difference between tracing and aggregate timing. When doing tracing, the code needs
/// to worry about the static allocation required for _concurrent_ traces. That is, there might be
/// multiple `scan_tree`s, with different `index`es happening at once.
///
/// When timing, this is flipped on its head: the timing code doesn't need space for concurrency
/// because it is called once, when an event has finished, and internally aggregates.
///
/// Rather, it needs space for the cardinality of the tags you'd like to emit. In the case of
/// `scan_tree`s, this would be the tree it's scanning over, rather than the index of the scan. Eg:
///
/// timing.scan_tree.transfers_id_avg=1234us vs timing.scan_tree.index_0_avg=1234us
pub const EventConcurrency = union(Event.EventTag) {
    replica_commit,
    replica_aof_write,
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

    const stack_limits = std.enums.EnumArray(Event.EventTag, u32).init(.{
        .replica_commit = 1,
        .replica_aof_write = 1,
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

    const stack_bases = array: {
        var array = std.enums.EnumArray(Event.EventTag, u32).initDefault(0, .{});
        var next: u32 = 0;
        for (std.enums.values(Event.EventTag)) |event_type| {
            array.set(event_type, next);
            next += stack_limits.get(event_type);
        }
        break :array array;
    };

    const stack_count = count: {
        var count: u32 = 0;
        for (std.enums.values(Event.EventTag)) |event_type| {
            count += stack_limits.get(event_type);
        }
        break :count count;
    };

    // Stack is a u32 since it must be losslessly encoded as a JSON integer.
    fn stack(event: *const EventConcurrency) u32 {
        switch (event.*) {
            inline .replica_sync_table,
            .lookup_worker,
            => |data| {
                assert(data.index < stack_limits.get(event.*));
                const stack_base = stack_bases.get(event.*);
                return stack_base + @as(u32, @intCast(data.index));
            },
            .scan_tree => |data| {
                assert(data.index < constants.lsm_scans_max);
                // This event has "nested" sub-events, so its offset is calculated
                // with padding to accommodate `scan_tree_level` events in between.
                const stack_base = stack_bases.get(event.*);
                const scan_tree_offset = (constants.lsm_levels + 1) * data.index;
                return stack_base + scan_tree_offset;
            },
            .scan_tree_level => |data| {
                assert(data.index < constants.lsm_scans_max);
                assert(data.level < constants.lsm_levels);
                // This is a "nested" event, so its offset is calculated
                // relative to the parent `scan_tree`'s offset.
                const stack_base = stack_bases.get(.scan_tree);
                const scan_tree_offset = (constants.lsm_levels + 1) * data.index;
                const scan_tree_level_offset = data.level + 1;
                return stack_base + scan_tree_offset + scan_tree_level_offset;
            },
            inline .grid_read, .grid_write => |data| {
                assert(data.iop < stack_limits.get(event.*));
                const stack_base = stack_bases.get(event.*);
                return stack_base + @as(u32, @intCast(data.iop));
            },
            inline else => |data, event_tag| {
                comptime assert(@TypeOf(data) == void);
                comptime assert(stack_limits.get(event_tag) == 1);
                return comptime stack_bases.get(event_tag);
            },
        }
    }

    pub fn format(
        event: *const EventConcurrency,
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
};

pub const EventCardinality = union(Event.EventTag) {
    replica_commit: struct { stage: CommitStage },
    replica_aof_write,
    replica_sync_table,

    compact_beat: struct { tree: TreeEnum, level_b: u8 },
    compact_beat_merge: struct { tree: TreeEnum, level_b: u8 },
    compact_manifest,
    compact_mutable: struct { tree: TreeEnum },
    compact_mutable_suffix: struct { tree: TreeEnum },

    lookup: struct { tree: TreeEnum },
    lookup_worker: struct { tree: TreeEnum },

    scan_tree: struct { tree: TreeEnum },
    scan_tree_level: struct { tree: TreeEnum, level: u8 },

    grid_read,
    grid_write,

    pub fn format(
        event: *const EventCardinality,
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
};

pub const Tracer = struct {
    replica_index: u8,
    options: Options,
    buffer: []u8,

    events_started: [EventConcurrency.stack_count]?u64 = .{null} ** EventConcurrency.stack_count,
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

    pub fn start(tracer: *Tracer, event: Event) void {
        const data_concurrency = event.data_concurrency();
        const data_cardinality = event.data_cardinality();
        const stack = data_concurrency.stack();

        assert(tracer.events_started[stack] == null);
        tracer.events_started[stack] = tracer.timer.read();

        log.info(
            "{}: {}: start:{}",
            .{ tracer.replica_index, data_concurrency, data_cardinality },
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
            .thread_id = data_concurrency.stack(),
            .category = @tagName(event),
            .event = 'B',
            .timestamp = time_elapsed_us,
            .name = data_concurrency,
            .data = struct_format(data_cardinality, .sparse),
            .args = std.json.Formatter(@TypeOf(data_cardinality)){ .value = data_cardinality, .options = .{} },
        }) catch unreachable;

        writer.writeAll(buffer_stream.getWritten()) catch |err| {
            std.debug.panic("Tracer.start: {}\n", .{err});
        };
    }

    pub fn stop(tracer: *Tracer, event: Event) void {
        const data_concurrency = event.data_concurrency();
        const data_cardinality = event.data_cardinality();
        const stack = data_concurrency.stack();

        const event_start_ns = tracer.events_started[stack].?;
        const event_end_ns = tracer.timer.read();

        assert(tracer.events_started[stack] != null);
        tracer.events_started[stack] = null;

        log.debug("{}: {}: stop:{} (duration={}ms)", .{
            tracer.replica_index,
            data_concurrency,
            struct_format(data_cardinality, .dense),
            @divFloor(event_end_ns - event_start_ns, std.time.ns_per_ms),
        });

        tracer.write_stop(stack);
    }

    pub fn reset(tracer: *Tracer, event_tag: Event.EventTag) void {
        const stack_base = EventConcurrency.stack_bases.get(event_tag);
        const cardinality = EventConcurrency.stack_limits.get(event_tag);
        for (stack_base..stack_base + cardinality) |stack| {
            if (tracer.events_started[stack]) |_| {
                log.debug("{}: {s}: reset", .{ tracer.replica_index, @tagName(event_tag) });

                tracer.events_started[stack] = null;
                tracer.write_stop(@intCast(stack));
            }
        }
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
};

const DataFormatterCardinality = enum { dense, sparse };

fn StructFormatterType(comptime Data: type, comptime cardinality: DataFormatterCardinality) type {
    // assert(@typeInfo(Data) == .Struct);

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
