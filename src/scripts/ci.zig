//! Various CI checks that go beyond `zig build test`. Notably, at the moment this script includes:
//!
//! - Testing all language clients.
//! - Building and link-checking docs.

const std = @import("std");
const builtin = @import("builtin");
const log = std.log;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const Shell = @import("../shell.zig");

const client_readmes = @import("./client_readmes.zig");

pub const Language = std.meta.FieldEnum(@TypeOf(LanguageCI));
const LanguageCI = .{
    .dotnet = @import("../clients/dotnet/ci.zig"),
    .go = @import("../clients/go/ci.zig"),
    .java = @import("../clients/java/ci.zig"),
    .node = @import("../clients/node/ci.zig"),
    .python = @import("../clients/python/ci.zig"),
};

pub const CLIArgs = struct {
    language: ?Language = null,
    validate_release: bool = false,
};

pub fn main(shell: *Shell, gpa: std.mem.Allocator, cli_args: CLIArgs) !void {
    if (cli_args.validate_release) {
        try validate_release(shell, gpa, cli_args.language);
    } else {
        try generate_readmes(shell, gpa, cli_args.language);
        try run_tests(shell, gpa, cli_args.language);
    }
}

fn generate_readmes(shell: *Shell, gpa: std.mem.Allocator, language_requested: ?Language) !void {
    inline for (comptime std.enums.values(Language)) |language| {
        if (language_requested == language or language_requested == null) {
            try shell.pushd("./src/clients/" ++ @tagName(language));
            defer shell.popd();

            try client_readmes.test_freshness(shell, gpa, language);
        }
    }
}

fn run_tests(shell: *Shell, gpa: std.mem.Allocator, language_requested: ?Language) !void {
    inline for (comptime std.enums.values(Language)) |language| {
        if (language_requested == language or language_requested == null) {
            const ci = @field(LanguageCI, @tagName(language));
            var section = try shell.open_section(@tagName(language) ++ " ci");
            defer section.close();

            {
                try shell.pushd("./src/clients/" ++ @tagName(language));
                defer shell.popd();

                try ci.tests(shell, gpa);
            }
        }
    }
}

fn validate_release(shell: *Shell, gpa: std.mem.Allocator, language_requested: ?Language) !void {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    try shell.pushd_dir(tmp_dir.dir);
    defer shell.popd();

    const release_info = try shell.exec_stdout(
        "gh release --repo tigerbeetle/tigerbeetle list --limit 1",
        .{},
    );
    const tag = stdx.cut(release_info, "\t").?.prefix;
    log.info("validating release {s}", .{tag});

    try shell.exec(
        "gh release --repo tigerbeetle/tigerbeetle download {tag}",
        .{ .tag = tag },
    );

    if (builtin.os.tag != .linux) {
        log.warn("skip release verification for platforms other than Linux", .{});
    }

    // Note: when updating the list of artifacts, don't forget to check for any external links.
    //
    // At minimum, `installation.md` requires an update.
    const artifacts = [_][]const u8{
        "tigerbeetle-aarch64-linux-debug.zip",
        "tigerbeetle-aarch64-linux.zip",
        "tigerbeetle-universal-macos-debug.zip",
        "tigerbeetle-universal-macos.zip",
        "tigerbeetle-x86_64-linux-debug.zip",
        "tigerbeetle-x86_64-linux.zip",
        "tigerbeetle-x86_64-windows-debug.zip",
        "tigerbeetle-x86_64-windows.zip",
    };
    for (artifacts) |artifact| {
        assert(shell.file_exists(artifact));
    }

    // Enable this once deterministic zip generation has been merged in and released.
    // const raw_run_number = stdx.cut(stdx.cut(tag, ".").?.suffix, ".").?.suffix;

    // // The +188 comes from how release.zig calculates the version number.
    // const run_number = try std.fmt.allocPrint(
    //     shell.arena.allocator(),
    //     "{}",
    //     .{try std.fmt.parseInt(u32, raw_run_number, 10) + 188},
    // );

    // const sha = try shell.exec_stdout("git rev-parse HEAD", .{});
    // try shell.exec_zig("build scripts -- release --build  --run-number={run_number} " ++
    //     "--sha={sha} --language=zig", .{
    //     .run_number = run_number,
    //     .sha = sha,
    // });
    // for (artifacts) |artifact| {
    //     // Zig only guarantees release builds to be deterministic.
    //     if (std.mem.indexOf(u8, artifact, "-debug.zip") != null) continue;

    //     // TODO(Zig): Determinism is broken on Windows:
    //     // https://github.com/ziglang/zig/issues/9432
    //     if (std.mem.indexOf(u8, artifact, "-windows.zip") != null) continue;

    //     const checksum_downloaded = try shell.exec_stdout("sha256sum {artifact}", .{
    //         .artifact = artifact,
    //     });

    //     shell.popd();
    //     const checksum_built = try shell.exec_stdout("sha256sum dist/tigerbeetle/{artifact}", .{
    //         .artifact = artifact,
    //     });
    //     try shell.pushd_dir(tmp_dir.dir);

    //     // Slice the output to suppress the names.
    //     if (!std.mem.eql(u8, checksum_downloaded[0..64], checksum_built[0..64])) {
    //         std.debug.panic("checksum mismatch - {s}: downloaded {s}, built {s}", .{
    //             artifact,
    //             checksum_downloaded[0..64],
    //             checksum_built[0..64],
    //         });
    //     }
    // }

    try shell.exec("unzip tigerbeetle-x86_64-linux.zip", .{});
    const version = try shell.exec_stdout("./tigerbeetle version --verbose", .{});
    assert(std.mem.indexOf(u8, version, tag) != null);
    assert(std.mem.indexOf(u8, version, "ReleaseSafe") != null);

    const tigerbeetle_absolute_path = try shell.cwd.realpathAlloc(gpa, "tigerbeetle");
    defer gpa.free(tigerbeetle_absolute_path);

    inline for (comptime std.enums.values(Language)) |language| {
        if (language_requested == language or language_requested == null) {
            const ci = @field(LanguageCI, @tagName(language));
            try ci.validate_release(shell, gpa, .{
                .tigerbeetle = tigerbeetle_absolute_path,
                .version = tag,
            });
        }
    }

    const docker_version = try shell.exec_stdout(
        \\docker run ghcr.io/tigerbeetle/tigerbeetle:{version} version --verbose
    , .{ .version = tag });
    assert(std.mem.indexOf(u8, docker_version, tag) != null);
    assert(std.mem.indexOf(u8, docker_version, "ReleaseSafe") != null);
}
