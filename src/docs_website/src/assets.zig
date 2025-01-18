const std = @import("std");
const Build = std.Build;

pub const supported_file_types = [_][]const u8{
    "avif",
    "css",
    "gif",
    "html",
    "jpg",
    "js",
    "json",
    "png",
    "svg",
    "ttf",
    "webp",
    "woff2",
    "xml",
};

pub fn install(b: *Build, options: struct {
    source: []const u8,
    target: []const u8,
}) *Build.Step.InstallDir {
    return b.addInstallDirectory(.{
        .source_dir = b.path(options.source),
        .install_dir = .prefix,
        .install_subdir = options.target,
        .include_extensions = &supported_file_types,
    });
}
