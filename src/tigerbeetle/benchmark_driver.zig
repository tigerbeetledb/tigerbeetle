//! Driver script behind `tigerbeetle benchmark` command.
//!
//! During benchmarking, there are three entities to keep track of:
//! - the "load" process generating requests,
//! - the cluster of `tigerbeetle`s processing requests,
//! - the orchestrating script coordinating the two.
//!
//! This here is the orchestrator. If no `--addresses` is passed on the command line, it spins up a
//! temporary single-node `tigerbeetle` cluster. Otherwise, an existing cluster is re-used for the
//! benchmarking.
//!
//! The cluster address is then passed onto `benchmark_load.zig`, which deals with both offering
//! the load and measuring response latencies and throughput. The load runs in-process.

const std = @import("std");
const ChildProcess = std.process.Child;

const vsr = @import("vsr");
const cli = @import("./cli.zig");
const benchmark_load = @import("./benchmark_load.zig");

const log = std.log;

pub fn main(allocator: std.mem.Allocator, args: *const cli.Command.Benchmark) !void {
    // Note: we intentionally don't use a temporary directory for this data file, and instead just
    // put it into CWD, as performance of TigerBeetle very much depends on a specific file system.
    const data_file = args.file orelse data_file: {
        var random_bytes: [4]u8 = undefined;
        std.crypto.random.bytes(&random_bytes);
        const random_suffix: [8]u8 = std.fmt.bytesToHex(random_bytes, .lower);
        break :data_file "0_0-" ++ random_suffix ++ ".tigerbeetle.benchmark";
    };

    var data_file_created = false;
    defer {
        if (data_file_created and args.file == null) {
            std.fs.cwd().deleteFile(data_file) catch {};
        }
    }

    var tigerbeetle_process: ?TigerBeetleProcess = null;
    defer if (tigerbeetle_process) |*p| {
        _ = p.deinit();
    };

    var maybe_stat_empty: ?std.fs.File.Stat = null;
    if (args.addresses == null) {
        const me = try std.fs.selfExePathAlloc(allocator);
        defer allocator.free(me);

        try format(allocator, .{ .tigerbeetle = me, .data_file = data_file });
        data_file_created = true;
        maybe_stat_empty = try std.fs.cwd().statFile(data_file);

        tigerbeetle_process = try start(allocator, .{
            .tigerbeetle = me,
            .data_file = data_file,
            .args = args,
        });
    } else {
        // Arguments forwarded to the replica cannot be used with a cluster started by the user.
        inline for (.{
            "cache_accounts",
            "cache_transfers",
            "cache_transfers_pending",
            "cache_grid",
            "trace",
            "file",
        }) |arg_name| {
            if (@field(args, arg_name) != null) {
                vsr.fatal(.cli, "--" ++ arg_name ++ ": incompatible with --addresses", .{});
            }
        }
    }

    const addresses = if (args.addresses) |*addresses|
        addresses.const_slice()
    else
        &.{tigerbeetle_process.?.address};
    try benchmark_load.main(allocator, addresses, args);

    if (tigerbeetle_process) |*p| {
        const rusage = p.deinit();
        tigerbeetle_process = null;

        if (rusage.getMaxRss()) |max_rss_bytes| {
            std.io.getStdOut().writer().print("\nrss = {} bytes\n", .{max_rss_bytes}) catch {};
        }
    }

    if (data_file_created) {
        const stat = try std.fs.cwd().statFile(data_file);
        if (maybe_stat_empty) |stat_empty| {
            try std.io.getStdOut().writer().print("\ndatafile empty = {} bytes\n", .{
                stat_empty.size,
            });
        }
        try std.io.getStdOut().writer().print("datafile = {} bytes\n", .{stat.size});
    }
}

fn format(allocator: std.mem.Allocator, options: struct {
    tigerbeetle: []const u8,
    data_file: []const u8,
}) !void {
    const format_result = try ChildProcess.run(.{
        .allocator = allocator,
        .argv = &.{
            options.tigerbeetle,
            "format",
            "--cluster=0",
            "--replica=0",
            "--replica-count=1",
            options.data_file,
        },
    });
    defer {
        allocator.free(format_result.stdout);
        allocator.free(format_result.stderr);
    }
    errdefer log.err("stderr: {s}", .{format_result.stderr});

    switch (format_result.term) {
        .Exited => |code| if (code != 0) return error.BadFormat,
        else => return error.BadFormat,
    }
}

const TigerBeetleProcess = struct {
    child: std.process.Child,
    address: std.net.Address,

    fn deinit(self: *TigerBeetleProcess) std.process.Child.ResourceUsageStatistics {
        // Although we could just kill the child here, let's exercise the "normal" termination logic
        // through stdin closure, such that, from the perspective of the child, there's no
        // difference between the parent process exiting normally or just crashing.
        self.child.stdin.?.close();
        self.child.stdin = null;
        _ = self.child.wait() catch {};

        defer self.* = undefined;
        return self.child.resource_usage_statistics;
    }
};

fn start(allocator: std.mem.Allocator, options: struct {
    tigerbeetle: []const u8,
    data_file: []const u8,
    args: *const cli.Command.Benchmark,
}) !TigerBeetleProcess {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var start_args = std.ArrayListUnmanaged([]const u8){};
    try start_args.append(arena.allocator(), options.tigerbeetle);
    try start_args.append(arena.allocator(), "start");
    try start_args.append(arena.allocator(), "--addresses=0");

    // Forward the cache options to the tigerbeetle process:
    const forward_args = &.{
        .{ options.args.cache_accounts, "cache-accounts" },
        .{ options.args.cache_transfers, "cache-transfers" },
        .{ options.args.cache_transfers_pending, "cache-transfers-pending" },
        .{ options.args.cache_grid, "cache-grid" },
        .{ options.args.trace, "trace" },
    };

    inline for (forward_args) |forward_arg| {
        if (forward_arg[0]) |arg_value| {
            try start_args.append(
                arena.allocator(),
                try std.fmt.allocPrint(arena.allocator(), "--{s}={s}", .{
                    forward_arg[1],
                    arg_value,
                }),
            );
        }
    }

    // Some of the forwarded arguments require the "--experimental" flag.
    const experimental: bool = inline for (forward_args) |forward_arg| {
        if (forward_arg[0] != null) break true;
    } else false;
    if (experimental) {
        try start_args.append(arena.allocator(), "--experimental");
    }

    try start_args.append(arena.allocator(), options.data_file);
    var child = std.process.Child.init(start_args.items, allocator);

    child.request_resource_usage_statistics = true;
    child.stdin_behavior = .Pipe;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Inherit;
    try child.spawn();
    errdefer {
        _ = child.kill() catch {};
    }

    const port = port: {
        errdefer log.err("failed to read port number from tigerbeetle process", .{});
        var port_buf: [std.fmt.count("{}\n", .{std.math.maxInt(u16)})]u8 = undefined;
        const port_buf_len = try child.stdout.?.readAll(&port_buf);
        break :port try std.fmt.parseInt(u16, port_buf[0 .. port_buf_len - 1], 10);
    };

    const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, port);

    return .{ .child = child, .address = address };
}
