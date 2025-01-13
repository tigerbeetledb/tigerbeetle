const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");

// FIXME hack for now
const CommitStage = @import("../vsr/replica.zig").CommitStage;
const TreeEnum = @import("../state_machine.zig").TreeEnum;

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

    metrics_emit: void,

    pub const EventTag = std.meta.Tag(Event);

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
                    try writer.print(":{}", .{struct_format(data)});
                }
            },
        }
    }

    pub fn event_tracing(event: *const Event) EventTracing {
        return switch (event.*) {
            .replica_sync_table => |event_unwrapped| .{ .replica_sync_table = .{ .index = event_unwrapped.index } },
            .lookup_worker => |event_unwrapped| .{ .lookup_worker = .{ .index = event_unwrapped.index } },
            .scan_tree => |event_unwrapped| .{ .scan_tree = .{ .index = event_unwrapped.index } },
            .scan_tree_level => |event_unwrapped| .{ .scan_tree_level = .{ .index = event_unwrapped.index, .level = event_unwrapped.level } },
            .grid_read => |event_unwrapped| .{ .grid_read = .{ .iop = event_unwrapped.iop } },
            .grid_write => |event_unwrapped| .{ .grid_write = .{ .iop = event_unwrapped.iop } },
            inline else => |_, tag| tag,
        };
    }

    pub fn event_timing(event: *const Event) EventTiming {
        return switch (event.*) {
            .replica_commit => |event_unwrapped| .{ .replica_commit = .{ .stage = event_unwrapped.stage } },
            .compact_beat => |event_unwrapped| .{ .compact_beat = .{ .tree = event_unwrapped.tree, .level_b = event_unwrapped.level_b } },
            .compact_beat_merge => |event_unwrapped| .{ .compact_beat_merge = .{ .tree = event_unwrapped.tree, .level_b = event_unwrapped.level_b } },
            .compact_mutable => |event_unwrapped| .{ .compact_mutable = .{ .tree = event_unwrapped.tree } },
            .compact_mutable_suffix => |event_unwrapped| .{ .compact_mutable_suffix = .{ .tree = event_unwrapped.tree } },
            .lookup => |event_unwrapped| .{ .lookup = .{ .tree = event_unwrapped.tree } },
            .lookup_worker => |event_unwrapped| .{ .lookup_worker = .{ .tree = event_unwrapped.tree } },
            .scan_tree => |event_unwrapped| .{ .scan_tree = .{ .tree = event_unwrapped.tree } },
            .scan_tree_level => |event_unwrapped| .{ .scan_tree_level = .{ .tree = event_unwrapped.tree, .level = event_unwrapped.level } },
            inline else => |_, tag| tag,
        };
    }
};

/// There's a difference between tracing and aggregate timing. When doing tracing, the code needs
/// to worry about the static allocation required for _concurrent_ traces. That is, there might be
/// multiple `scan_tree`s, with different `index`es happening at once.
///
/// When timing, this is flipped on its head: the timing code doesn't need space for concurrency
/// because it is called once, when an event has finished, and internally aggregates. The
/// aggregiation is needed because there can be an unknown number of calls between flush intervals,
/// compared to tracing which is emitted as it happens.
///
/// Rather, it needs space for the cardinality of the tags you'd like to emit. In the case of
/// `scan_tree`s, this would be the tree it's scanning over, rather than the index of the scan. Eg:
///
/// timing.scan_tree.transfers_id_avg=1234us vs timing.scan_tree.index_0_avg=1234us
pub const EventTracing = union(Event.EventTag) {
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

    metrics_emit: void,

    pub const stack_limits = std.enums.EnumArray(Event.EventTag, u32).init(.{
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
        .metrics_emit = 1,
    });

    pub const stack_bases = array: {
        var array = std.enums.EnumArray(Event.EventTag, u32).initDefault(0, .{});
        var next: u32 = 0;
        for (std.enums.values(Event.EventTag)) |event_type| {
            array.set(event_type, next);
            next += stack_limits.get(event_type);
        }
        break :array array;
    };

    pub const stack_count = count: {
        var count: u32 = 0;
        for (std.enums.values(Event.EventTag)) |event_type| {
            count += stack_limits.get(event_type);
        }
        break :count count;
    };

    // Stack is a u32 since it must be losslessly encoded as a JSON integer.
    pub fn stack(event: *const EventTracing) u32 {
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
        event: *const EventTracing,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        switch (event.*) {
            inline else => |data| {
                if (@TypeOf(data) != void) {
                    try writer.print("{}", .{struct_format(data)});
                }
            },
        }
    }
};

pub const EventTiming = union(Event.EventTag) {
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

    metrics_emit: void,

    // FIXME: Exhaustively test these with a test. Easy enough!
    pub const stack_limits = std.enums.EnumArray(Event.EventTag, u32).init(.{
        .replica_commit = std.meta.fields(CommitStage).len,
        .replica_aof_write = 1,
        .replica_sync_table = 1,
        .compact_beat = (std.meta.fields(TreeEnum).len + 1) * @as(u32, constants.lsm_levels),
        .compact_beat_merge = (std.meta.fields(TreeEnum).len + 1) * @as(u32, constants.lsm_levels),
        .compact_manifest = 1,
        .compact_mutable = (std.meta.fields(TreeEnum).len + 1),
        .compact_mutable_suffix = (std.meta.fields(TreeEnum).len + 1),
        .lookup = (std.meta.fields(TreeEnum).len + 1),
        .lookup_worker = (std.meta.fields(TreeEnum).len + 1),
        .scan_tree = (std.meta.fields(TreeEnum).len + 1),
        .scan_tree_level = (std.meta.fields(TreeEnum).len + 1) * @as(u32, constants.lsm_levels),
        .grid_read = 1,
        .grid_write = 1,
        .metrics_emit = 1,
    });

    pub const stack_bases = array: {
        var array = std.enums.EnumArray(Event.EventTag, u32).initDefault(0, .{});
        var next: u32 = 0;
        for (std.enums.values(Event.EventTag)) |event_type| {
            array.set(event_type, next);
            next += stack_limits.get(event_type);
        }
        break :array array;
    };

    pub const stack_count = count: {
        var count: u32 = 0;
        for (std.enums.values(Event.EventTag)) |event_type| {
            count += stack_limits.get(event_type);
        }
        break :count count;
    };

    pub fn stack(event: *const EventTiming) u32 {
        switch (event.*) {
            // Single payload: CommitStage
            inline .replica_commit => |data| {
                // FIXME:Can we assert this at comptime?
                // FIXME: The enum logic is actually broken. We kinda want to remap the enum from 0 -> limit.... At the moment
                // hack around it with +1 for trees etc.
                const stage = @intFromEnum(data.stage);
                assert(stage < stack_limits.get(event.*));

                return stack_bases.get(event.*) + @as(u32, @intCast(stage));
            },
            // Single payload: TreeEnum
            inline .compact_mutable, .compact_mutable_suffix, .lookup, .lookup_worker, .scan_tree => |data| {
                const tree_id = @intFromEnum(data.tree);
                assert(tree_id < stack_limits.get(event.*));

                return stack_bases.get(event.*) + @as(u32, @intCast(tree_id));
            },
            // Double payload: TreeEnum + level_b
            inline .compact_beat, .compact_beat_merge => |data| {
                const tree_id = @intFromEnum(data.tree);
                const level_b = data.level_b;
                const offset = tree_id * constants.lsm_levels + level_b;
                assert(offset < stack_limits.get(event.*));

                return stack_bases.get(event.*) + @as(u32, @intCast(offset));
            },
            // Double payload: TreeEnum + level
            inline .scan_tree_level => |data| {
                const tree_id = @intFromEnum(data.tree);
                const level = data.level;
                const offset = tree_id * constants.lsm_levels + level;
                assert(offset < stack_limits.get(event.*));

                return stack_bases.get(event.*) + @as(u32, @intCast(offset));
            },
            inline else => |data, event_tag| {
                comptime assert(@TypeOf(data) == void);
                comptime assert(stack_limits.get(event_tag) == 1);

                return comptime stack_bases.get(event_tag);
            },
        }
    }

    pub fn format(
        event: *const EventTiming,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        switch (event.*) {
            inline else => |data| {
                if (@TypeOf(data) != void) {
                    try writer.print("{}", .{struct_format(data)});
                }
            },
        }
    }
};

pub const EventMetric = union(enum) {
    const EventTag = std.meta.Tag(EventMetric);

    table_count_visible: struct { tree: TreeEnum, level: u8 },
    table_count_visible_max: struct { tree: TreeEnum, level: u8 },

    // FIXME: Exhaustively test these with a test. Easy enough!
    pub const stack_limits = std.enums.EnumArray(EventTag, u32).init(.{
        .table_count_visible = (std.meta.fields(TreeEnum).len + 1) * @as(u32, constants.lsm_levels),
        .table_count_visible_max = (std.meta.fields(TreeEnum).len + 1) * @as(u32, constants.lsm_levels),
    });

    pub const stack_bases = array: {
        var array = std.enums.EnumArray(EventTag, u32).initDefault(0, .{});
        var next: u32 = 0;
        for (std.enums.values(EventTag)) |event_type| {
            array.set(event_type, next);
            next += stack_limits.get(event_type);
        }
        break :array array;
    };

    pub const stack_count = count: {
        var count: u32 = 0;
        for (std.enums.values(EventTag)) |event_type| {
            count += stack_limits.get(event_type);
        }
        break :count count;
    };

    pub fn stack(event: *const EventMetric) u32 {
        switch (event.*) {
            // Double payload: TreeEnum + level
            inline .table_count_visible, .table_count_visible_max => |data| {
                const tree_id = @intFromEnum(data.tree);
                const level = data.level;
                const offset = tree_id * constants.lsm_levels + level;
                assert(offset < stack_limits.get(event.*));

                return stack_bases.get(event.*) + @as(u32, @intCast(offset));
            },
            // inline else => |data, event_tag| {
            //     comptime assert(@TypeOf(data) == void);
            //     comptime assert(stack_limits.get(event_tag) == 1);

            //     return comptime stack_bases.get(event_tag);
            // },
        }
    }
};

fn StructFormatterType(comptime Data: type) type {
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

            const fields = std.meta.fields(Data);
            inline for (fields, 0..) |data_field, i| {
                assert(data_field.type == bool or
                    @typeInfo(data_field.type) == .Int or
                    @typeInfo(data_field.type) == .Enum or
                    @typeInfo(data_field.type) == .Union);

                const data_field_value = @field(formatter.data, data_field.name);
                try writer.writeAll(data_field.name);
                try writer.writeByte('=');

                if (@typeInfo(data_field.type) == .Enum or
                    @typeInfo(data_field.type) == .Union)
                {
                    try writer.print("{s}", .{@tagName(data_field_value)});
                } else {
                    try writer.print("{}", .{data_field_value});
                }

                if (i != fields.len - 1) {
                    try writer.writeByte(' ');
                }
            }
        }
    };
}

pub fn struct_format(
    data: anytype,
) StructFormatterType(@TypeOf(data)) {
    return StructFormatterType(@TypeOf(data)){ .data = data };
}

pub const EventTimingAggregate = struct {
    event: EventTiming,
    timing: struct { // FIXME: -> value
        duration_min_us: u64,
        duration_max_us: u64,
        duration_sum_us: u64,

        count: u64,
    },
};

pub const EventMetricAggregate = struct {
    event: EventMetric,
    value: u64,
};
