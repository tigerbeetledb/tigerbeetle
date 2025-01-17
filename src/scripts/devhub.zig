//! Runs a set of macro-benchmarks whose result is displayed at <https://tigerbeetle.github.io>.
//!
//! Specifically:
//!
//! - This script is run by the CI infrastructure on every merge to main.
//! - It runs a set of "benchmarks", where a "benchmark" can be anything (eg, measuring the size of
//!   the binary).
//! - The results of all measurements are serialized as a single JSON object, `Run`.
//! - The key part: this JSON is then stored in a "distributed database" for our visualization
//!   front-end to pick up. This "database" is just a newline-delimited JSON file in a git repo
const std = @import("std");

const stdx = @import("../stdx.zig");
const Shell = @import("../shell.zig");
const changelog = @import("./changelog.zig");
const Release = @import("../multiversioning.zig").Release;

const log = std.log;

pub const CLIArgs = struct {
    sha: []const u8,
};

pub fn main(shell: *Shell, _: std.mem.Allocator, cli_args: CLIArgs) !void {
    try devhub_metrics(shell, cli_args);
    try devhub_coverage(shell);
}

fn devhub_coverage(shell: *Shell) !void {
    const kcov_version = shell.exec_stdout("kcov --version", .{}) catch {
        return error.NoKcov;
    };
    log.info("kcov version {s}", .{kcov_version});

    try shell.exec_zig("build test:unit:build", .{});
    try shell.exec_zig("build vopr:build", .{});
    try shell.exec_zig("build fuzz:build", .{});

    // Put results into src/devhub, as that folder is deployed as GitHub pages.
    try shell.project_root.deleteTree("./src/devhub/coverage");
    try shell.project_root.makePath("./src/devhub/coverage");

    const kcov: []const []const u8 = &.{ "kcov", "--include-path=./src", "./src/devhub/coverage" };
    inline for (.{
        "{kcov} ./zig-out/bin/test",
        "{kcov} ./zig-out/bin/fuzz --events-max=500000 lsm_tree 92",
        "{kcov} ./zig-out/bin/fuzz --events-max=500000 lsm_forest 92",
        "{kcov} ./zig-out/bin/vopr 92",
    }) |command| {
        try shell.exec(command, .{ .kcov = kcov });
    }

    var coverage_dir = try shell.cwd.openDir("./src/devhub/coverage", .{ .iterate = true });
    defer coverage_dir.close();

    // kcov adds some symlinks to the output, which prevents upload to GitHub actions from working.
    var it = coverage_dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind == .sym_link) {
            try coverage_dir.deleteFile(entry.name);
        }
    }
}

fn devhub_metrics(shell: *Shell, cli_args: CLIArgs) !void {
    const commit_timestamp_str =
        try shell.exec_stdout("git show -s --format=%ct {sha}", .{ .sha = cli_args.sha });
    const commit_timestamp = try std.fmt.parseInt(u64, commit_timestamp_str, 10);

    // Only build the TigerBeetle binary to test build speed and build size. Throw it away once
    // done, and use a release build from `zig-out/dist/` to run the benchmark.
    var timer = try std.time.Timer.start();
    try shell.exec_zig("build -Drelease install", .{});
    const build_time_ms = timer.lap() / std.time.ns_per_ms;
    const executable_size_bytes = (try shell.cwd.statFile("tigerbeetle")).size;
    try shell.project_root.deleteFile("tigerbeetle");

    // When doing a release, the latest release in the changelog on main will be newer than the
    // latest release on GitHub. In this case, don't pass in --no-changelog - as doing that causes
    // the release code to try and look for a version which doesn't yet exist!
    const no_changelog_flag = blk: {
        const changelog_text = try shell.project_root.readFileAlloc(
            shell.arena.allocator(),
            "CHANGELOG.md",
            1024 * 1024,
        );
        var changelog_iteratator = changelog.ChangelogIterator.init(changelog_text);

        const last_release_changelog = changelog_iteratator.next_changelog().?.release orelse
            break :blk true;
        const last_release_published = try Release.parse(try shell.exec_stdout(
            "gh release list --json tagName --jq {query} --limit 1",
            .{ .query = ".[].tagName" },
        ));

        if (Release.less_than({}, last_release_published, last_release_changelog)) {
            break :blk false;
        } else {
            break :blk true;
        }
    };

    if (no_changelog_flag) {
        try shell.exec_zig(
            \\build scripts -- release --build --no-changelog --sha={sha}
            \\    --language=zig
        , .{ .sha = cli_args.sha });
    } else {
        try shell.exec_zig(
            \\build scripts -- release --build --sha={sha}
            \\    --language=zig
        , .{ .sha = cli_args.sha });
    }
    try shell.project_root.deleteFile("tigerbeetle");
    try shell.exec("unzip zig-out/dist/tigerbeetle/tigerbeetle-x86_64-linux.zip", .{});

    const benchmark_result = try shell.exec_stdout(
        "./tigerbeetle benchmark --validate --checksum-performance",
        .{},
    );
    const tps = try get_measurement(benchmark_result, "load accepted", "tx/s");
    const batch_p100_ms = try get_measurement(benchmark_result, "batch latency p100", "ms");
    const query_p100_ms = try get_measurement(benchmark_result, "query latency p100", "ms");
    const rss_bytes = try get_measurement(benchmark_result, "rss", "bytes");
    const datafile_bytes = try get_measurement(benchmark_result, "datafile", "bytes");
    const datafile_empty_bytes = try get_measurement(benchmark_result, "datafile empty", "bytes");
    const checksum_message_size_max_us = try get_measurement(
        benchmark_result,
        "checksum message size max",
        "us",
    );
    const format_time_ms = blk: {
        timer.reset();

        try shell.exec(
            "./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 datafile",
            .{},
        );

        break :blk timer.read() / std.time.ns_per_ms;
    };
    const startup_time_ms = blk: {
        timer.reset();

        var process = try shell.spawn(
            .{
                .stdin_behavior = .Pipe,
                .stdout_behavior = .Pipe,
                .stderr_behavior = .Inherit,
            },
            "./tigerbeetle start --addresses=0 --cache-grid=8GiB datafile",
            .{},
        );
        errdefer _ = process.kill() catch unreachable;

        var port_buf: [std.fmt.count("{}\n", .{std.math.maxInt(u16)})]u8 = undefined;
        _ = try process.stdout.?.readAll(&port_buf);

        break :blk timer.read() / std.time.ns_per_ms;
    };

    const ci_pipeline_duration_s = blk: {
        const times_gh = try shell.exec_stdout("gh run list -c {sha} -e merge_group " ++
            "--json startedAt,updatedAt -L 1 --template {template}", .{
            .sha = cli_args.sha,
            .template = "{{range .}}{{.startedAt}} {{.updatedAt}}{{end}}",
        });
        const iso8601_started_at, const iso8601_updated_at =
            (stdx.cut(times_gh, " ") orelse break :blk null).unpack();

        const epoch_started_at = try shell.iso8601_to_timestamp_seconds(iso8601_started_at);
        const epoch_updated_at = try shell.iso8601_to_timestamp_seconds(iso8601_updated_at);

        break :blk epoch_updated_at - epoch_started_at;
    };

    const batch = MetricBatch{
        .timestamp = commit_timestamp,
        .attributes = .{
            .git_repo = "https://github.com/tigerbeetle/tigerbeetle",
            .git_commit = cli_args.sha,
            .branch = "main",
        },
        .metrics = &[_]Metric{
            .{ .name = "ci pipeline duration", .value = ci_pipeline_duration_s.?, .unit = "s" },
            .{ .name = "executable size", .value = executable_size_bytes, .unit = "bytes" },
            .{ .name = "TPS", .value = tps, .unit = "count" },
            .{ .name = "batch p100", .value = batch_p100_ms, .unit = "ms" },
            .{ .name = "query p100", .value = query_p100_ms, .unit = "ms" },
            .{ .name = "RSS", .value = rss_bytes, .unit = "bytes" },
            .{ .name = "datafile", .value = datafile_bytes, .unit = "bytes" },
            .{ .name = "datafile empty", .value = datafile_empty_bytes, .unit = "bytes" },
            .{
                .name = "checksum(message_size_max)",
                .value = checksum_message_size_max_us,
                .unit = "us",
            },
            .{ .name = "build time", .value = build_time_ms, .unit = "ms" },
            .{ .name = "format time", .value = format_time_ms, .unit = "ms" },
            .{ .name = "startup time - 8GiB grid cache", .value = startup_time_ms, .unit = "ms" },
        },
    };

    try upload_run(shell, &batch);

    upload_nyrkio(shell, &batch) catch |err| {
        log.err("failed to upload Nyrkiö metrics: {}", .{err});
    };
}

fn get_measurement(
    benchmark_stdout: []const u8,
    comptime label: []const u8,
    comptime unit: []const u8,
) !u64 {
    errdefer {
        std.log.err("can't extract '" ++ label ++ "' measurement", .{});
    }

    var cut = stdx.cut(benchmark_stdout, label ++ " = ") orelse return error.BadMeasurement;
    cut = stdx.cut(cut.suffix, " " ++ unit) orelse return error.BadMeasurement;

    return try std.fmt.parseInt(u64, cut.prefix, 10);
}

fn upload_run(shell: *Shell, batch: *const MetricBatch) !void {
    const token = try shell.env_get("DEVHUBDB_PAT");
    try shell.exec(
        \\git clone --depth 1
        \\  https://oauth2:{token}@github.com/tigerbeetle/devhubdb.git
        \\  devhubdb
    , .{
        .token = token,
    });

    try shell.pushd("./devhubdb");
    defer shell.popd();

    for (0..32) |_| {
        try shell.exec("git fetch origin", .{});
        try shell.exec("git reset --hard origin/main", .{});

        {
            const file = try shell.cwd.openFile("./devhub/data.json", .{
                .mode = .write_only,
            });
            defer file.close();

            try file.seekFromEnd(0);
            try std.json.stringify(batch, .{}, file.writer());
            try file.writeAll("\n");
        }

        try shell.exec("git add ./devhub/data.json", .{});
        try shell.git_env_setup(.{ .use_hostname = false });
        try shell.exec("git commit -m 📈", .{});
        if (shell.exec("git push", .{})) {
            log.info("metrics uploaded", .{});
            break;
        } else |_| {
            log.info("conflict, retrying", .{});
        }
    } else {
        log.err("can't push new data to devhub", .{});
        return error.CanNotPush;
    }
}

const Metric = struct {
    name: []const u8,
    unit: []const u8,
    value: u64,
};

const MetricBatch = struct {
    timestamp: u64,
    metrics: []const Metric,
    attributes: struct {
        git_repo: []const u8,
        branch: []const u8,
        git_commit: []const u8,
    },
};

fn upload_nyrkio(shell: *Shell, batch: *const MetricBatch) !void {
    const url = "https://nyrkio.com/api/v0/result/devhub";
    const token = try shell.env_get("NYRKIO_TOKEN");
    const payload = try std.json.stringifyAlloc(
        shell.arena.allocator(),
        [_]*const MetricBatch{batch}, // Nyrkiö needs an _array_ of batches.
        .{},
    );
    try shell.http_post(url, payload, .{
        .content_type = .json,
        .authorization = try shell.fmt("Bearer {s}", .{token}),
    });
}
