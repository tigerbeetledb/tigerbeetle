const std = @import("std");
const log = std.log.scoped(.template);

pub const Html = @This();

arena: std.mem.Allocator,
buffer: std.ArrayList(u8),
writer: std.ArrayList(u8).Writer,

pub fn create(arena: std.mem.Allocator) !*Html {
    var html = try arena.create(Html);
    html.* = .{
        .arena = arena,
        .buffer = std.ArrayList(u8).init(arena),
        .writer = undefined,
    };
    html.writer = html.buffer.writer();
    return html;
}

/// Replaces the variables in `$snake_case` matching the names of the fields in the
/// `replacement` struct.
///
/// We picked the $dollar_name format over a {curly_braces} format to avoid potential ambiguities
/// with JavaScript function syntax in the template.
pub fn write(html: *Html, template: []const u8, replacements: anytype) !void {
    const ReplacementsType = @TypeOf(replacements);
    const replacements_type_info = @typeInfo(ReplacementsType);
    if (replacements_type_info != .Struct) @compileError("expected struct");

    var unused = std.StringHashMap(void).init(html.arena);
    inline for (replacements_type_info.Struct.fields) |field| {
        try unused.put(field.name, {});
    }

    var it = std.mem.tokenizeScalar(u8, template, '$');
    if (template[0] != '$') if (it.next()) |prefix| try html.writer.writeAll(prefix);
    while (it.next()) |chunk| {
        const identifier_len = for (chunk, 0..) |c, index| {
            switch (c) {
                'a'...'z', '_' => {},
                else => break index,
            }
        } else chunk.len;

        const identifier = chunk[0..identifier_len];
        const found = inline for (replacements_type_info.Struct.fields) |field| {
            if (std.mem.eql(u8, field.name, identifier)) {
                try html.writer.writeAll(switch (field.type) {
                    *Html => @field(replacements, field.name).string(),
                    else => @field(replacements, field.name),
                });
                _ = unused.remove(field.name);
                break true;
            }
        } else false;
        if (!found) {
            log.err("Html.write: identifier '{s}' not found in replacements", .{identifier});
            return error.IdentifierNotFound;
        }

        try html.writer.writeAll(chunk[identifier_len..]);
    }

    if (unused.count() > 0) {
        var unused_it = unused.keyIterator();
        while (unused_it.next()) |unused_identifier| {
            log.err("Html.write: identifier '{s}' not found in template", .{unused_identifier.*});
        }
        return error.UnusedIdentifiers;
    }
}

pub fn child(self: Html) !*Html {
    return try Html.create(self.arena);
}

pub fn string(self: Html) []const u8 {
    return self.buffer.items;
}

/// Converts a string in markdown format to an HTML string.
/// Currently handles only backtick (`) conversion to <code>.
pub fn from_md(self: *Html, string_md: []const u8) ![]const u8 {
    if (string_md[0] == '`') {
        var buffer = std.ArrayList(u8).init(self.arena);
        // try buffer.writer().print("<code>{s}</code>", .{std.mem.trim(u8, string_md, "`")});
        try buffer.writer().print("{s}", .{std.mem.trim(u8, string_md, "`")});
        return buffer.items;
    } else {
        return string_md;
    }
}

pub fn redirect(arena: std.mem.Allocator, url: []const u8) ![]const u8 {
    const template = @embedFile("html/redirect.html");
    var html = try Html.create(arena);
    try html.write(template, .{ .url = url });
    return html.string();
}
