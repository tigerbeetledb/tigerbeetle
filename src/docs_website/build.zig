const std = @import("std");
const Website = @import("src/website.zig").Website;
const assets = @import("src/assets.zig");
const docs = @import("src/docs.zig");

pub fn build(b: *std.Build) !void {
    const url_prefix: []const u8 = b.option(
        []const u8,
        "url_prefix",
        "Prefix links with this string",
    ) orelse "";

    const git_commit = b.option(
        []const u8,
        "git-commit",
        "The git commit revision of the source code.",
    ) orelse std.mem.trimRight(u8, b.run(&.{ "git", "rev-parse", "--verify", "HEAD" }), "\n");

    const pandoc_bin = get_pandoc_bin(b) orelse return;

    const website = Website.init(b, url_prefix, pandoc_bin);

    const file_checker_exe = b.addExecutable(.{
        .name = "file_checker",
        .root_source_file = b.path("src/file_checker.zig"),
        .target = b.graph.host,
    });
    const file_checker_run = b.addRunArtifact(file_checker_exe);
    file_checker_run.addArg("zig-out");

    const install_assets = assets.install(b, .{ .source = "assets", .target = "." });
    file_checker_run.step.dependOn(&install_assets.step);

    const docs_dir = try docs.build(b, website);
    const install_docs = b.addInstallDirectory(.{
        .source_dir = docs_dir,
        .install_dir = .prefix,
        .install_subdir = ".",
    });
    file_checker_run.step.dependOn(&install_docs.step);

    const service_worker_writer_exe = b.addExecutable(.{
        .name = "service_worker_writer",
        .root_source_file = b.path("src/service_worker_writer.zig"),
        .target = b.graph.host,
    });
    const service_worker_writer_run = b.addRunArtifact(service_worker_writer_exe);
    service_worker_writer_run.addArgs(&.{ url_prefix, git_commit, "zig-out" });
    service_worker_writer_run.step.dependOn(&file_checker_run.step);

    b.getInstallStep().dependOn(&service_worker_writer_run.step);
}

fn get_pandoc_bin(b: *std.Build) ?std.Build.LazyPath {
    const host = b.graph.host.result;
    const name = switch (host.os.tag) {
        .linux => switch (host.cpu.arch) {
            .x86_64 => "pandoc_linux_amd64",
            else => @panic("unsupported cpu arch"),
        },
        .macos => switch (host.cpu.arch) {
            .aarch64 => "pandoc_macos_arm64",
            else => @panic("unsupported cpu arch"),
        },
        else => @panic("unsupported os"),
    };
    if (b.lazyDependency(name, .{})) |dep| {
        return dep.path("bin/pandoc");
    } else {
        return null;
    }
}
