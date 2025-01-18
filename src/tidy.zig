//! Checks for various non-functional properties of the code itself.

const std = @import("std");
const assert = std.debug.assert;
const fs = std.fs;
const mem = std.mem;

const stdx = @import("./stdx.zig");
const Shell = @import("./shell.zig");

const UsedDeclarations = std.StringHashMapUnmanaged(struct {
    count: u32,
    offset: u32,
});

test "tidy" {
    const allocator = std.testing.allocator;

    const shell = try Shell.create(allocator);
    defer shell.destroy();

    const paths = try list_file_paths(shell);

    const buffer_size = 1024 * 1024;
    const buffer = try allocator.alloc(u8, buffer_size);
    defer allocator.free(buffer);

    var dead_files_detector = DeadFilesDetector.init(allocator);
    defer dead_files_detector.deinit();

    var dead_declarations: UsedDeclarations = .{};
    defer dead_declarations.deinit(allocator);

    try dead_declarations.ensureTotalCapacity(allocator, identifiers_per_file_max);

    var function_line_count_longest: usize = 0;

    // NB: all checks are intentionally implemented in a streaming fashion, such that we only need
    // to read the files once.
    for (paths) |path| {
        const bytes_read = (try std.fs.cwd().readFile(path, buffer)).len;
        if (bytes_read == buffer.len - 1) return error.FileTooLong;
        buffer[bytes_read] = 0;

        const source_file = SourceFile{ .path = path, .text = buffer[0..bytes_read :0] };

        if (tidy_control_characters(source_file)) |control_character| {
            std.debug.print(
                "{s} error: contains control character: code={} symbol='{c}'\n",
                .{ source_file.path, control_character, control_character },
            );
            return error.BannedControlCharacter;
        }

        if (mem.endsWith(u8, source_file.path, ".zig")) {
            if (tidy_banned(source_file.text)) |ban_reason| {
                std.debug.print(
                    "{s}: error: banned, {s}\n",
                    .{ source_file.path, ban_reason },
                );
                return error.Banned;
            }

            if (try tidy_long_line(source_file)) |line_index| {
                std.debug.print(
                    "{s}:{d} error: line exceeds 100 columns\n",
                    .{ source_file.path, line_index + 1 },
                );
                return error.LineTooLong;
            }

            var tree = try std.zig.Ast.parse(allocator, source_file.text, .zig);
            defer tree.deinit(allocator);

            if (tidy_dead_declarations(&tree, &dead_declarations)) |name| {
                std.debug.print("{s}: error: '{s}' is dead code\n", .{ source_file.path, name });
            }

            function_line_count_longest = @max(
                function_line_count_longest,
                (try tidy_long_functions(source_file, &tree)).function_line_count_longest,
            );

            if (tidy_generic_functions(source_file)) |function| {
                std.debug.print(
                    "{s}:{d} error: '{s}' should end with the 'Type' suffix\n",
                    .{
                        source_file.path,
                        function.line,
                        function.name,
                    },
                );
                return error.GenericFunctionWithoutType;
            }

            try dead_files_detector.visit(source_file);
        }

        if (mem.endsWith(u8, source_file.path, ".md")) {
            tidy_markdown_title(source_file.text) catch |err| {
                std.debug.print(
                    "{s} error: invalid markdown headings, {}\n",
                    .{ source_file.path, err },
                );
                return err;
            };
        }
    }

    try dead_files_detector.finish();

    if (function_line_count_longest < function_line_count_max) {
        std.debug.print("error: `function_line_count_max` must be updated to {d}\n", .{
            function_line_count_longest,
        });
        return error.LineCountOutdated;
    }
}

const SourceFile = struct { path: []const u8, text: [:0]const u8 };

fn tidy_banned(source: []const u8) ?[]const u8 {
    // Note: must avoid banning ourselves!
    if (std.mem.indexOf(u8, source, "std." ++ "BoundedArray") != null) {
        return "use stdx.BoundedArrayType instead of std version";
    }

    if (std.mem.indexOf(u8, source, "trait." ++ "hasUniqueRepresentation") != null) {
        return "use stdx.has_unique_representation instead of std version";
    }

    if (std.mem.indexOf(u8, source, "mem." ++ "copy(") != null) {
        return "use stdx.copy_disjoint instead of std version";
    }

    if (std.mem.indexOf(u8, source, "mem." ++ "copyForwards(") != null) {
        return "use stdx.copy_left instead of std version";
    }

    if (std.mem.indexOf(u8, source, "mem." ++ "copyBackwards(") != null) {
        return "use stdx.copy_right instead of std version";
    }

    if (std.mem.indexOf(u8, source, "posix." ++ "unexpectedErrno(") != null) {
        return "use stdx.unexpected_errno instead of std version";
    }

    // Ban "fixme" comments. This allows using fixme as reminders with teeth --- when working on
    // larger pull requests, it is often helpful to leave fixme comments as a reminder to oneself.
    // This tidy rule ensures that the reminder is acted upon before code gets into main. That is:
    // - use fixme for issues to be fixed in the same pull request,
    // - use todo as general-purpose long-term remainders without enforcement.
    if (std.mem.indexOf(u8, source, "FIX" ++ "ME") != null) {
        return "FIX" ++ "ME comments must be addressed before getting to main";
    }

    if (std.mem.indexOf(u8, source, "dbg(") != null) {
        if (std.mem.indexOf(u8, source, "fn dbg(") == null) {
            return "dbg" ++ "() must be removed before getting to main";
        }
    }

    if (std.mem.indexOf(u8, source, "Self = " ++ "@This()") != null) {
        return "use a type name instead of Self";
    }

    if (std.mem.indexOf(u8, source, "!" ++ "comptime") != null) {
        return "use ! inside comptime";
    }

    return null;
}

fn tidy_long_line(file: SourceFile) !?u32 {
    if (std.mem.endsWith(u8, file.path, "low_level_hash_vectors.zig")) return null;
    var line_iterator = mem.split(u8, file.text, "\n");
    var line_index: u32 = 0;
    while (line_iterator.next()) |line| : (line_index += 1) {
        const line_length = try std.unicode.utf8CountCodepoints(line);
        if (line_length > 100) {
            if (has_link(line)) continue;

            // Journal recovery table
            if (std.mem.indexOf(u8, line, "Case.init(") != null) continue;

            // For multiline strings, we care that the _result_ fits 100 characters,
            // but we don't mind indentation in the source.
            if (parse_multiline_string(line)) |string_value| {
                const string_value_length = try std.unicode.utf8CountCodepoints(string_value);
                if (string_value_length <= 100) continue;

                if (std.mem.startsWith(u8, string_value, " account A") or
                    std.mem.startsWith(u8, string_value, " transfer T") or
                    std.mem.startsWith(u8, string_value, " transfer   "))
                {
                    // Table tests from state_machine.zig. They are intentionally wide.
                    continue;
                }

                // vsr.zig's Checkpoint ops diagram.
                if (std.mem.startsWith(u8, string_value, "OPS: ")) continue;

                // trace.zig's JSON snapshot test.
                if (std.mem.startsWith(u8, string_value, "{\"pid\":0,\"tid\":0,\"ph\":")) continue;
            }

            return line_index;
        }
    }
    return null;
}

fn tidy_control_characters(file: SourceFile) ?u8 {
    const binary_file_extensions: []const []const u8 = &.{ ".ico", ".png", ".webp" };
    for (binary_file_extensions) |extension| {
        if (std.mem.endsWith(u8, file.path, extension)) return null;
    }

    if (mem.indexOfScalar(u8, file.text, '\r') != null) {
        if (std.mem.endsWith(u8, file.path, ".bat")) return null;
        return '\r';
    }

    // Learning the best from UNIX, Visual Studio, like make, insists on tabs.
    if (std.mem.endsWith(u8, file.path, ".sln")) return null;
    // Go code uses tabs.
    if (std.mem.endsWith(u8, file.path, ".go") or
        (std.mem.endsWith(u8, file.path, ".md") and mem.indexOf(u8, file.text, "```go") != null))
    {
        return null;
    }

    if (mem.indexOfScalar(u8, file.text, '\t') != null) {
        return '\t';
    }
    return null;
}

const identifiers_per_file_max = 100_000;
/// Detects unused constants and functions.
///
/// This is a one-side heuristic: there might be false negatives, but no false positives.
///
/// Current algorithm:
/// - Two passes.
/// - Pass 1: count how many times each identifier is mentioned in the file.
/// - Pass 2: warn about any unique identifier which is a non-public declaration.
///
/// At the moment, this is implemented using only the lexer, without looking at the AST, as that
/// seemed simpler.
fn tidy_dead_declarations(
    tree: *const std.zig.Ast,
    used: *UsedDeclarations,
) ?[]const u8 {
    assert(used.count() == 0);
    defer used.clearRetainingCapacity();

    var identifier_start: ?std.zig.Ast.ByteOffset = 0;
    inline for (.{ .fill, .check }) |phase| {
        next_token: for (
            tree.tokens.items(.tag),
            tree.tokens.items(.start),
            0..,
        ) |tag, start, index| {
            const identifier_start_previous = identifier_start;
            identifier_start = switch (tag) {
                .identifier => start,
                else => null,
            };

            const start_previous = identifier_start_previous orelse continue :next_token;
            const token_text = std.mem.trim(
                u8,
                tree.source[start_previous..start],
                &std.ascii.whitespace,
            );

            switch (phase) {
                .fill => {
                    const gop = used.getOrPutAssumeCapacity(token_text);
                    if (used.count() >= identifiers_per_file_max) @panic("file to large");

                    if (gop.found_existing) {
                        const same_line_occurrence = mem.indexOfScalar(
                            u8,
                            tree.source[gop.value_ptr.offset..start],
                            '\n',
                        ) == null;
                        // Count occurrences on a single line as one, as a special case for imports:
                        // const foo = std.foo;
                        if (same_line_occurrence) continue :next_token;
                    }

                    if (!gop.found_existing) gop.value_ptr.* = .{ .count = 0, .offset = 0 };
                    gop.value_ptr.count += 1;
                    gop.value_ptr.offset = start;

                    continue :next_token;
                },
                .check => {
                    const usages = used.get(token_text).?.count;
                    assert(usages >= 1);
                    if (usages > 1) continue :next_token;
                },
                else => comptime unreachable,
            }

            assert(phase == .check);
            assert(used.get(token_text).?.count == 1);
            var declaration_keyword = false;
            for (0..3) |context_offset| {
                const context_tag = if (index - context_offset < 2)
                    .eof
                else
                    tree.tokens.get(index - context_offset - 2).tag;
                if (!declaration_keyword) {
                    switch (context_tag) {
                        .keyword_fn, .keyword_const => declaration_keyword = true,
                        // Not a declaration.
                        else => continue :next_token,
                    }
                } else {
                    switch (context_tag) {
                        .keyword_inline => {},
                        // Public declaration can be used in a different file.
                        .keyword_pub, .keyword_export => continue :next_token,
                        // []const u8 or *const u8, not a declaration.
                        .r_bracket, .asterisk => continue :next_token,
                        // Non public declarations, never used.
                        else => return token_text,
                    }
                }
            }
        }
    }

    return null;
}

/// As we trim our functions, make sure to update this constant; tidy will error if you do not.
const function_line_count_max = 414; // fn check in state_machine.zig

fn tidy_long_functions(
    file: SourceFile,
    tree: *const std.zig.Ast,
) !struct {
    function_line_count_longest: usize,
} {
    if (std.mem.endsWith(u8, file.path, "client_readmes.zig")) {
        // This file is essentially a template to generate a markdown file, so it
        // intentionally has giant functions.
        return .{ .function_line_count_longest = 0 };
    }

    const Function = struct {
        fn_decl_line: usize,
        first_token_location: std.zig.Ast.Location,
        last_token_location: std.zig.Ast.Location,
        /// Functions that are not "innermost," meaning that they have other functions
        /// inside of them (such as functions that return `type`s) are not checked as
        /// it is normal for them to be very lengthy.
        is_innermost: bool,

        fn is_parent_of(a: @This(), b: @This()) bool {
            return a.first_token_location.line_start < b.first_token_location.line_start and
                a.last_token_location.line_end > b.last_token_location.line_end;
        }

        fn get_and_check_line_count(
            function: @This(),
            file_of_function: SourceFile,
        ) usize {
            const function_line_count =
                function.last_token_location.line -
                function.first_token_location.line;

            if (function_line_count > function_line_count_max) {
                std.debug.print(
                    "{s}:{d} error: above function line count max with {d} lines\n",
                    .{
                        file_of_function.path,
                        function.fn_decl_line + 1,
                        function_line_count,
                    },
                );
            }

            return function_line_count;
        }
    };

    var function_stack = stdx.BoundedArrayType(Function, 32).from_slice(&.{}) catch unreachable;

    const tags = tree.nodes.items(.tag);
    const datas = tree.nodes.items(.data);

    var function_line_count_longest: usize = 0;

    for (tags, datas, 0..) |tag, data, function_decl_node| {
        if (tag != .fn_decl) continue;

        const function_body_node = data.rhs;

        const function_decl_first_token = tree.firstToken(@intCast(function_decl_node));
        const function_body_first_token = tree.firstToken(@intCast(function_body_node));
        const function_body_last_token = tree.lastToken(@intCast(function_body_node));

        const innermost_function = .{
            .fn_decl_line = tree.tokenLocation(0, function_decl_first_token).line,
            .first_token_location = tree.tokenLocation(0, function_body_first_token),
            .last_token_location = tree.tokenLocation(0, function_body_last_token),
            .is_innermost = true,
        };

        while (function_stack.count() > 0) {
            const last_function = function_stack.get(function_stack.count() - 1);

            if (!last_function.is_parent_of(innermost_function)) {
                if (last_function.is_innermost) {
                    const line_count = last_function.get_and_check_line_count(file);
                    function_line_count_longest = @max(function_line_count_longest, line_count);
                }
                _ = function_stack.pop();
            } else {
                break;
            }
        }

        if (function_stack.count() > 0) {
            const last_function = &function_stack.slice()[function_stack.count() - 1];

            assert(last_function.is_parent_of(innermost_function));
            last_function.is_innermost = false;
        }

        function_stack.append_assume_capacity(innermost_function);
    }

    if (function_stack.count() > 0) {
        const last_function = function_stack.get(function_stack.count() - 1);

        if (last_function.is_innermost) {
            const line_count = last_function.get_and_check_line_count(file);
            function_line_count_longest = @max(function_line_count_longest, line_count);
        }
    }

    return .{
        .function_line_count_longest = function_line_count_longest,
    };
}

/// All functions using the `CamelCase` naming convention return a type,
/// so we enforce that the function name also ends with the `Type` suffix.
fn tidy_generic_functions(
    file: SourceFile,
) ?struct {
    line: usize,
    name: []const u8,
} {
    var line_count: u32 = 0;
    var it = std.mem.split(u8, file.text, "\n");
    while (it.next()) |line| {
        line_count += 1;
        // Zig fmt enforces that the pattern `fn Foo(` is not split across multiple lines.
        const function_name = function_name: {
            const fn_prefix = "fn ";
            const index = std.mem.indexOf(u8, line, fn_prefix) orelse continue;
            // Not all `fn ` tokens are functions, some may be `callback_fn` for example.
            // They should appear at the beginning of a line or after a whitespace.
            if (index == 0 or std.ascii.isWhitespace(line[index - 1])) {
                const begin = index + fn_prefix.len;
                const end = std.mem.indexOf(u8, line[begin..], "(") orelse continue;
                if (end == 0) continue;

                assert(begin + end < line.len);
                break :function_name line[begin..][0..end];
            }
            continue;
        };

        // Skipping naming convetions that requires upper-case functions.
        if (std.mem.startsWith(u8, function_name, "JNI_")) continue;

        if (std.ascii.isUpper(function_name[0])) {
            if (!std.mem.endsWith(u8, function_name, "Type")) {
                return .{
                    .line = line_count,
                    .name = function_name,
                };
            }
        }
    }

    return null;
}

/// Checks that each markdown document has exactly one h1.
///
/// There are two schools of thought regarding largest (`# War and Peace`)
/// headings in markdown. One school says that they are _section_ titles, so
/// you could have multiple #'s in the document. But another option is to
/// say that a single # signifies document _title_, and there should be only
/// one in a document.
///
/// We use markdown to create HTML, so # turns into h1. MDN recommends that
/// there's only a single h1 in a page:
///
/// <https://developer.mozilla.org/en-US/docs/Web/HTML/Element/Heading_Elements#avoid_using_multiple_h1_elements_on_one_page>
///
/// For this reason,  we follow the second convention.
fn tidy_markdown_title(text: []const u8) !void {
    var fenced_block = false; // Avoid interpreting `# ` shell comments as titles.
    var heading_count: u32 = 0;
    var line_count: u32 = 0;
    var it = std.mem.split(u8, text, "\n");
    while (it.next()) |line| {
        line_count += 1;
        if (mem.startsWith(u8, line, "```")) fenced_block = !fenced_block;
        if (!fenced_block and mem.startsWith(u8, line, "# ")) heading_count += 1;
    }
    assert(!fenced_block);
    switch (heading_count) {
        0 => if (line_count > 2) return error.MissingTitle, // No need for a title for a short note.
        1 => {},
        else => return error.DuplicateTitle,
    }
}

// Zig's lazy compilation model makes it too easy to forget to include a file into the build --- if
// nothing imports a file, compiler just doesn't see it and can't flag it as unused.
//
// DeadFilesDetector implements heuristic detection of unused files, by "grepping" for import
// statements and flagging file which are never imported. This gives false negatives for unreachable
// cycles of files, as well as for identically-named files, but it should be good enough in
// practice.
const DeadFilesDetector = struct {
    const FileName = [64]u8;
    const FileState = struct { import_count: u32, definition_count: u32 };
    const FileMap = std.AutoArrayHashMap(FileName, FileState);

    files: FileMap,

    fn init(allocator: std.mem.Allocator) DeadFilesDetector {
        return .{ .files = FileMap.init(allocator) };
    }

    fn deinit(detector: *DeadFilesDetector) void {
        detector.files.deinit();
    }

    fn visit(detector: *DeadFilesDetector, file: SourceFile) !void {
        (try detector.file_state(file.path)).definition_count += 1;

        var text: []const u8 = file.text;
        for (0..1024) |_| {
            const cut = stdx.cut(text, "@import(\"") orelse break;
            text = cut.suffix;
            const import_path = stdx.cut(text, "\")").?.prefix;
            if (std.mem.endsWith(u8, import_path, ".zig")) {
                (try detector.file_state(import_path)).import_count += 1;
            }
        } else {
            std.debug.panic("file with more than 1024 imports: {s}", .{file.path});
        }
    }

    fn finish(detector: *DeadFilesDetector) !void {
        defer detector.files.clearRetainingCapacity();

        for (detector.files.keys(), detector.files.values()) |name, state| {
            assert(state.definition_count > 0);
            if (state.import_count == 0 and !is_entry_point(name)) {
                std.debug.print("file never imported: {s}\n", .{name});
                return error.DeadFile;
            }
        }
    }

    fn file_state(detector: *DeadFilesDetector, path: []const u8) !*FileState {
        const gop = try detector.files.getOrPut(path_to_name(path));
        if (!gop.found_existing) gop.value_ptr.* = .{ .import_count = 0, .definition_count = 0 };
        return gop.value_ptr;
    }

    fn path_to_name(path: []const u8) FileName {
        assert(std.mem.endsWith(u8, path, ".zig"));
        const basename = std.fs.path.basename(path);
        var file_name: FileName = .{0} ** 64;
        assert(basename.len <= file_name.len);
        stdx.copy_disjoint(.inexact, u8, &file_name, basename);
        return file_name;
    }

    fn is_entry_point(file: FileName) bool {
        const entry_points: []const []const u8 = &.{
            "fuzz_tests.zig",
            "integration_tests.zig",
            "jni_tests.zig",
            "main.zig",
            "node.zig",
            "vopr.zig",
            "tb_client_header.zig",
            "unit_tests.zig",
            "scripts.zig",
            "tb_client_exports.zig",
            "dotnet_bindings.zig",
            "go_bindings.zig",
            "node_bindings.zig",
            "java_bindings.zig",
            "python_bindings.zig",
            "build.zig",
            "build_multiversion.zig",
            "vortex.zig",
            "file_checker.zig",
            "page_writer.zig",
            "search_index_writer.zig",
            "service_worker_writer.zig",
        };
        for (entry_points) |entry_point| {
            if (std.mem.startsWith(u8, &file, entry_point)) return true;
        }
        return false;
    }
};

test "tidy changelog" {
    const allocator = std.testing.allocator;

    const changelog_size_max = 1024 * 1024;
    const changelog = try fs.cwd().readFileAlloc(allocator, "CHANGELOG.md", changelog_size_max);
    defer allocator.free(changelog);

    var line_iterator = mem.split(u8, changelog, "\n");
    var line_index: usize = 0;
    while (line_iterator.next()) |line| : (line_index += 1) {
        if (std.mem.endsWith(u8, line, " ")) {
            std.debug.print("CHANGELOG.md:{d} trailing whitespace", .{line_index + 1});
            return error.TrailingWhitespace;
        }
        const line_length = try std.unicode.utf8CountCodepoints(line);
        if (line_length > 100 and !has_link(line)) {
            std.debug.print("CHANGELOG.md:{d} line exceeds 100 columns\n", .{line_index + 1});
            return error.LineTooLong;
        }
    }
}

test "tidy no large blobs" {
    const allocator = std.testing.allocator;
    const shell = try Shell.create(allocator);
    defer shell.destroy();

    // Run `git rev-list | git cat-file` to find large blobs. This is better than looking at the
    // files in the working tree, because it catches the cases where a large file is "removed" by
    // reverting the commit.
    //
    // Zig's std doesn't provide a cross platform abstraction for piping two commands together, so
    // we begrudgingly pass the data through this intermediary process.
    const shallow = try shell.exec_stdout("git rev-parse --is-shallow-repository", .{});
    if (!std.mem.eql(u8, shallow, "false")) {
        return error.ShallowRepository;
    }

    const MiB = 1024 * 1024;
    const rev_list = try shell.exec_stdout("git rev-list --objects HEAD", .{});
    const objects = try shell.exec_stdout_options(
        .{ .stdin_slice = rev_list },
        "git cat-file --batch-check={format}",
        .{ .format = "%(objecttype) %(objectsize) %(rest)" },
    );

    var has_large_blobs = false;
    var lines = std.mem.split(u8, objects, "\n");
    while (lines.next()) |line| {
        // Parsing lines like
        //     blob 1032 client/package.json
        const blob = stdx.cut_prefix(line, "blob ") orelse continue;

        const cut = stdx.cut(blob, " ").?;
        const size = try std.fmt.parseInt(u64, cut.prefix, 10);
        const path = cut.suffix;

        if (std.mem.eql(u8, path, "src/vsr/replica.zig")) continue; // :-)
        if (std.mem.eql(u8, path, "src/state_machine.zig")) continue; // :-|
        if (std.mem.eql(u8, path, "src/docs_website/package-lock.json")) continue; // :-(
        if (size > @divExact(MiB, 4)) {
            has_large_blobs = true;
            std.debug.print("{s}\n", .{line});
        }
    }
    if (has_large_blobs) return error.HasLargeBlobs;
}

// Sanity check for "unexpected" files in the repository.
test "tidy extensions" {
    const allowed_extensions = std.StaticStringMap(void).initComptime(.{
        .{".bat"}, .{".c"},   .{".cs"},    .{".csproj"}, .{".css"}, .{".go"},
        .{".h"},   .{".hcl"}, .{".html"},  .{".java"},   .{".js"},  .{".json"},
        .{".md"},  .{".mod"}, .{".props"}, .{".ps1"},    .{".py"},  .{".service"},
        .{".sln"}, .{".sum"}, .{".svg"},   .{".ts"},     .{".txt"}, .{".xml"},
        .{".yml"}, .{".zig"}, .{".zon"},
    });

    const exceptions = std.StaticStringMap(void).initComptime(.{
        .{".editorconfig"},
        .{".gitignore"},
        .{"exclude-pmd.properties"},
        .{"favicon.png"},
        .{"notfound-light.webp"},
        .{"notfound-dark.webp"},
        .{"preview.webp"},
        .{"LICENSE"},
        .{"module-info.test"},
        .{"anchor-links.lua"},
        .{"markdown-links.lua"},
        .{"table-wrapper.lua"},
        .{"zig/download.sh"},
        .{"src/scripts/cfo_supervisor.sh"},
        .{".github/ci/docs_check.sh"},
        .{".github/ci/test_aof.sh"},
        .{"src/clients/python/pyproject.toml"},
    });

    const allocator = std.testing.allocator;
    const shell = try Shell.create(allocator);
    defer shell.destroy();

    const paths = try list_file_paths(shell);

    for (exceptions.keys()) |exception| {
        for (paths) |path| {
            const basename = std.fs.path.basename(path);
            if (std.mem.eql(u8, exception, basename) or std.mem.eql(u8, exception, path)) {
                break;
            }
        } else {
            std.debug.panic("exception (or basename) doesn't exist: {s} ({s})", .{
                exception,
                std.fs.path.basename(exception),
            });
        }
    }

    var bad_extension = false;
    for (paths) |path| {
        if (path.len == 0) continue;
        const extension = std.fs.path.extension(path);
        if (!allowed_extensions.has(extension)) {
            const basename = std.fs.path.basename(path);
            if (!exceptions.has(basename) and !exceptions.has(path)) {
                std.debug.print("bad extension: {s}\n", .{path});
                bad_extension = true;
            }
        }
    }
    if (bad_extension) return error.BadExtension;
}

/// Heuristically checks if a `line` contains an URL.
fn has_link(line: []const u8) bool {
    return std.mem.indexOf(u8, line, "https://") != null;
}

/// If a line is a `\\` string literal, extract its value.
fn parse_multiline_string(line: []const u8) ?[]const u8 {
    const cut = stdx.cut(line, "\\\\") orelse return null;
    for (cut.prefix) |c| if (c != ' ') return null;
    return cut.suffix;
}

/// Lists all files in the repository.
fn list_file_paths(shell: *Shell) ![]const []const u8 {
    var result = std.ArrayList([]const u8).init(shell.arena.allocator());

    const files = try shell.exec_stdout("git ls-files -z", .{});
    assert(files[files.len - 1] == 0);
    var lines = std.mem.splitScalar(u8, files[0 .. files.len - 1], 0);
    while (lines.next()) |line| {
        assert(line.len > 0);
        try result.append(line);
    }

    return result.items;
}
