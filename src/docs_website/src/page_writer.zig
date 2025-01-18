const std = @import("std");
const assert = std.debug.assert;
const Website = @import("website.zig").Website;
const Html = @import("html.zig").Html;

const page_template = @embedFile("html/page.html");
const script_template = @embedFile("html/page-script.js");

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    const allocator = arena.allocator();
    const args = try std.process.argsAlloc(allocator);
    assert(args.len == 7);
    const title = args[1];
    const author = args[2];
    const url_prefix = args[3];
    const nav = args[4];
    const source_file_path = args[5];
    const target_file_path = args[6];

    var script = try Html.create(allocator);
    try script.write(script_template, .{ .url_prefix = url_prefix });

    var script_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(script.string(), &script_hash, .{});
    const b64_encoder = std.base64.standard.Encoder;
    var script_hash_b64_buf: [b64_encoder.calcSize(script_hash.len)]u8 = undefined;
    const script_hash_b64 = b64_encoder.encode(&script_hash_b64_buf, &script_hash);

    const content = try std.fs.cwd().readFileAlloc(
        allocator,
        source_file_path,
        Website.file_size_max,
    );
    var html = try Html.create(allocator);
    try html.write(page_template, .{
        .page_script_hash = script_hash_b64,
        .title = title,
        .author = author,
        .url_prefix = url_prefix,
        .nav = nav,
        .content = content,
        .page_script = script,
    });
    try std.fs.cwd().writeFile(.{ .sub_path = target_file_path, .data = html.string() });
}
