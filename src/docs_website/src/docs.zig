const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const LazyPath = std.Build.LazyPath;
const log = std.log.scoped(.docs);
const Website = @import("website.zig").Website;
const assets = @import("assets.zig");
const Html = @import("html.zig").Html;

const enforce_readme_md = false;

const base_path = "../../docs";

const SearchIndexEntry = struct {
    page_path: []const u8,
    html_path: LazyPath,
};
const SearchIndex = std.ArrayList(SearchIndexEntry);

pub fn build(
    b: *std.Build,
    website: Website,
) !LazyPath {
    const arena = b.allocator;
    const docs = b.addWriteFiles();

    var search_index = SearchIndex.init(arena);

    const root_menu = try create_root_menu(arena);
    try root_menu.install(b, website, root_menu, docs, &search_index);

    const search_index_writer_exe = b.addExecutable(.{
        .name = "search_index_writer",
        .root_source_file = b.path("src/search_index_writer.zig"),
        .target = b.graph.host,
    });
    const run_search_index_writer = b.addRunArtifact(search_index_writer_exe);
    const search_index_output = run_search_index_writer.addOutputFileArg("search-index.json");
    _ = docs.addCopyFile(search_index_output, "search-index.json");
    for (search_index.items) |entry| {
        run_search_index_writer.addArg(entry.page_path);
        run_search_index_writer.addFileArg(entry.html_path);
    }

    return docs.getDirectory();
}

fn create_root_menu(arena: std.mem.Allocator) !Menu {
    var items = std.ArrayList(Menu.Item).init(arena);
    try items.append(.{ .page = try Page.init(arena, base_path ++ "/quick-start.md") });
    try items.append(.{ .menu = try create_coding_menu(arena) });
    try items.append(.{ .menu = try create_operating_menu(arena) });
    try items.append(.{ .menu = try create_clients_menu(arena) });
    try items.append(.{ .menu = try create_reference_menu(arena) });
    try items.append(.{ .menu = try create_about_menu(arena) });

    return .{
        .title = "TigerBeetle Docs",
        .index = try Page.init(arena, base_path ++ "/README.md"),
        .items = items.items,
    };
}

fn create_coding_menu(arena: std.mem.Allocator) !Menu {
    const page_names = .{
        "system-architecture.md",
        "data-modeling.md",
        "financial-accounting.md",
        "two-phase-transfers.md",
        "reliable-transaction-submission.md",
        "time.md",
    };
    var items = std.ArrayList(Menu.Item).init(arena);
    inline for (page_names) |page_name| {
        try items.append(.{ .page = try Page.init(arena, base_path ++ "/coding/" ++ page_name) });
    }
    try items.append(.{ .menu = try create_recipes_menu(arena) });

    return .{
        .title = "Coding",
        .index = try Page.init(arena, base_path ++ "/coding/README.md"),
        .items = items.items,
    };
}

fn create_recipes_menu(arena: std.mem.Allocator) !Menu {
    const page_names = .{
        "currency-exchange.md",
        "multi-debit-credit-transfers.md",
        "close-account.md",
        "balance-conditional-transfers.md",
        "balance-bounds.md",
        "correcting-transfers.md",
        "rate-limiting.md",
        "balance-invariant-transfers.md",
    };
    var items = std.ArrayList(Menu.Item).init(arena);
    inline for (page_names) |page_name| {
        try items.append(.{
            .page = try Page.init(arena, base_path ++ "/coding/recipes/" ++ page_name),
        });
    }

    return .{
        .title = "Recipes",
        .index = null,
        .items = items.items,
    };
}

fn create_operating_menu(arena: std.mem.Allocator) !Menu {
    const page_names = .{
        "deploy.md",
        "hardware.md",
        "linux.md",
        "docker.md",
        "managed-service.md",
        "upgrading.md",
    };
    var items = std.ArrayList(Menu.Item).init(arena);
    inline for (page_names) |page_name| {
        try items.append(.{
            .page = try Page.init(arena, base_path ++ "/operating/" ++ page_name),
        });
    }

    return .{
        .title = "Operating",
        .index = null,
        .items = items.items,
    };
}

fn create_clients_menu(arena: std.mem.Allocator) !Menu {
    var pages = std.ArrayList(Menu.Item).init(arena);

    const clients = &.{ "go", "java", "dotnet", "node", "python" };
    const titles = &.{ "Go", "Java", ".NET", "Node.js", "Python" };
    inline for (clients, titles) |client, title| {
        try pages.append(.{ .page = .{
            .path_source = "../clients/" ++ client ++ "/README.md",
            .path_target = "clients/" ++ client,
            .title = title,
        } });
    }

    return .{
        .title = "Client Libraries",
        .index = null,
        .items = pages.items,
    };
}

fn create_reference_menu(arena: std.mem.Allocator) !Menu {
    const path = base_path ++ "/reference/";
    var items = std.ArrayList(Menu.Item).init(arena);
    try items.append(.{ .page = try Page.init(arena, path ++ "account.md") });
    try items.append(.{ .page = try Page.init(arena, path ++ "transfer.md") });
    try items.append(.{ .page = try Page.init(arena, path ++ "account-balance.md") });
    try items.append(.{ .page = try Page.init(arena, path ++ "account-filter.md") });
    try items.append(.{ .page = try Page.init(arena, path ++ "query-filter.md") });
    try items.append(.{ .menu = try create_requests_menu(arena) });
    try items.append(.{ .page = try Page.init(arena, path ++ "sessions.md") });

    return .{
        .title = "Reference",
        .index = null,
        .items = items.items,
    };
}

fn create_requests_menu(arena: std.mem.Allocator) !Menu {
    const path = base_path ++ "/reference/requests/";
    var items = std.ArrayList(Menu.Item).init(arena);
    try items.append(.{ .page = try Page.init(arena, path ++ "create_accounts.md") });
    try items.append(.{ .page = try Page.init(arena, path ++ "create_transfers.md") });
    try items.append(.{ .page = try Page.init(arena, path ++ "get_account_balances.md") });
    try items.append(.{ .page = try Page.init(arena, path ++ "get_account_transfers.md") });
    try items.append(.{ .page = try Page.init(arena, path ++ "lookup_accounts.md") });
    try items.append(.{ .page = try Page.init(arena, path ++ "lookup_transfers.md") });
    try items.append(.{ .page = try Page.init(arena, path ++ "query_accounts.md") });
    try items.append(.{ .page = try Page.init(arena, path ++ "query_transfers.md") });

    return .{
        .title = "Requests",
        .index = null,
        .items = items.items,
    };
}

fn create_about_menu(arena: std.mem.Allocator) !Menu {
    var items = std.ArrayList(Menu.Item).init(arena);
    try items.append(.{ .page = try Page.init(arena, base_path ++ "/about/oltp.md") });
    try items.append(.{ .page = try Page.init(arena, base_path ++ "/about/performance.md") });
    try items.append(.{ .page = try Page.init(arena, base_path ++ "/about/safety.md") });
    try items.append(.{ .page = try Page.init(arena, base_path ++ "/about/vopr.md") });
    try items.append(.{ .page = try Page.init(arena, base_path ++ "/about/architecture.md") });
    try items.append(.{ .page = try Page.init(arena, base_path ++ "/about/production-ready.md") });
    try items.append(.{ .page = try Page.init(arena, base_path ++ "/about/zig.md") });
    // (internals intentionally hidden)

    return .{
        .title = "About",
        .index = try Page.init(arena, base_path ++ "/about/README.md"),
        .items = items.items,
    };
}

const Menu = struct {
    const Item = union(Type) {
        const Type = enum {
            menu,
            page,
        };

        menu: Menu,
        page: Page,
    };

    title: []const u8,
    index: ?Page,
    items: []const Item,

    fn asc(context: void, lhs: Menu, rhs: Menu) bool {
        _ = context;
        assert(!std.mem.eql(u8, lhs.title, rhs.title));
        return std.mem.lessThan(u8, lhs.title, rhs.title);
    }

    fn contains_page(self: Menu, target: Page) bool {
        for (self.items) |item| {
            switch (item) {
                .menu => |menu| if (menu.contains_page(target)) return true,
                .page => |page| if (page.eql(target)) return true,
            }
        }
        return false;
    }

    fn write_links(
        self: Menu,
        website: Website,
        html: *Html,
        target_page: Page,
    ) !void {
        try html.write("<ol>", .{});
        for (self.items) |item| {
            switch (item) {
                .menu => |menu| {
                    try html.write("<li><details", .{});
                    if (menu.contains_page(target_page)) try html.write(" open", .{});
                    try html.write("><summary class=\"item\">", .{});
                    if (menu.index) |page| {
                        try html.write(
                            \\<a href="$url_prefix/$url/">$title</a>
                        , .{
                            .url_prefix = website.url_prefix,
                            .url = page.path_target,
                            // Fabio: index page titles are too long
                            .title = try html.from_md(menu.title),
                        });
                    } else {
                        try html.write("$title", .{
                            .title = try html.from_md(menu.title),
                        });
                    }
                    try html.write("</summary>", .{});
                    try menu.write_links(website, html, target_page);
                    try html.write("</details></li>", .{});
                },
                .page => |page| {
                    try html.write(
                        \\<li class="item"><a href="$url_prefix/$url/"$target_page>$title</a></li>
                    , .{
                        .url_prefix = website.url_prefix,
                        .url = page.path_target,
                        .target_page = if (page.eql(target_page)) " class=\"target\"" else "",
                        .title = try html.from_md(page.title),
                    });
                },
            }
        }
        try html.write("</ol>", .{});
    }

    fn install(
        self: Menu,
        b: *std.Build,
        website: Website,
        root_menu: Menu,
        docs: *std.Build.Step.WriteFile,
        search_index: *SearchIndex,
    ) !void {
        if (self.index) |page| {
            try page.install(b, website, root_menu, docs, search_index);
        }
        for (self.items) |item| {
            switch (item) {
                .menu => |menu| try menu.install(b, website, root_menu, docs, search_index),
                .page => |page| try page.install(b, website, root_menu, docs, search_index),
            }
        }
    }
};

const Page = struct {
    path_source: []const u8,
    path_target: []const u8,

    // Parsed from Markdown content.
    title: []const u8,

    fn init(arena: Allocator, path_source: []const u8) !Page {
        assert(std.mem.endsWith(u8, path_source, ".md"));

        var path_target = path_source[base_path.len + 1 ..];
        if (std.mem.eql(u8, path_target, "README.md")) {
            path_target = ".";
        } else if (std.mem.endsWith(u8, path_target, "/README.md")) {
            path_target = path_target[0 .. path_target.len - "/README.md".len];
        } else {
            path_target = path_target[0 .. path_target.len - ".md".len];
        }

        var post: Page = .{
            .path_source = path_source,
            .path_target = path_target,
            .title = undefined,
        };
        try post.load(arena);

        return post;
    }

    fn load(self: *Page, arena: Allocator) !void {
        errdefer log.err("error while loading '{s}'", .{self.path_source});

        const source = try std.fs.cwd().readFileAlloc(
            arena,
            self.path_source,
            Website.file_size_max,
        );
        var line_it = std.mem.tokenizeScalar(u8, source, '\n');

        const title_line = line_it.next().?;
        if (title_line.len < 3 or !std.mem.eql(u8, title_line[0..2], "# ")) {
            return error.TitleInvalid;
        }
        self.title = title_line[2..];
    }

    fn eql(lhs: Page, rhs: Page) bool {
        return std.mem.eql(u8, lhs.path_source, rhs.path_source);
    }

    fn asc(context: void, lhs: Page, rhs: Page) bool {
        _ = context;
        assert(!std.mem.eql(u8, lhs.title, rhs.title));
        return std.mem.lessThan(u8, lhs.title, rhs.title);
    }

    fn install(
        self: Page,
        b: *std.Build,
        website: Website,
        root_menu: Menu,
        docs: *std.Build.Step.WriteFile,
        search_index: *SearchIndex,
    ) !void {
        const pandoc_step = std.Build.Step.Run.create(b, "run pandoc");
        pandoc_step.addFileArg(website.pandoc_bin);
        pandoc_step.addArgs(&.{ "--from", "gfm", "--to", "html5" });
        pandoc_step.addArg("--lua-filter");
        pandoc_step.addFileArg(b.path("pandoc/markdown-links.lua"));
        pandoc_step.addArg("--lua-filter");
        pandoc_step.addFileArg(b.path("pandoc/anchor-links.lua"));
        pandoc_step.addArg("--lua-filter");
        pandoc_step.addFileArg(b.path("pandoc/table-wrapper.lua"));
        pandoc_step.addArg("--output");
        const pandoc_out = pandoc_step.addOutputFileArg("pandoc-out.html");
        pandoc_step.addFileArg(b.path(self.path_source));

        try search_index.append(.{ .page_path = self.path_target, .html_path = pandoc_out });

        const title_suffix = "TigerBeetle Docs";
        const page_title = blk: {
            if (std.mem.eql(u8, self.title, title_suffix)) {
                break :blk self.title;
            }
            break :blk try std.mem.join(b.allocator, " | ", &.{ self.title, title_suffix });
        };

        const nav_html = try Html.create(b.allocator);
        try root_menu.write_links(website, nav_html, self);

        const page_path = website.write_page(.{
            .title = page_title,
            .nav = nav_html.string(),
            .content = pandoc_out,
        });
        _ = docs.addCopyFile(page_path, b.pathJoin(&.{ self.path_target, "index.html" }));

        // If it exists, copy the page's asset directory.
        const page_dir = self.path_source[0 .. self.path_source.len - ".md".len];
        if (try path_exists(b.pathFromRoot(page_dir))) {
            _ = docs.addCopyDirectory(b.path(page_dir), self.path_target, .{
                .include_extensions = &assets.supported_file_types,
            });
        }
    }
};

fn make_title(arena: Allocator, input: []const u8) ![]const u8 {
    const output = try arena.dupe(u8, input);
    var needs_upper = true;
    for (output) |*c| {
        if (needs_upper) {
            c.* = std.ascii.toUpper(c.*);
            needs_upper = false;
        }
        switch (c.*) {
            ' ' => needs_upper = true,
            else => {},
        }
    }
    return output;
}

fn path_exists(path: []const u8) !bool {
    std.fs.cwd().access(path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}
