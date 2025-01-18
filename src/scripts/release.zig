//! Orchestrates building and publishing a distribution of tigerbeetle --- a collection of (source
//! and binary) artifacts which constitutes a release and which we upload to various registries.
//!
//! Concretely, the artifacts are:
//!
//! - TigerBeetle binary build for all supported architectures
//! - TigerBeetle clients build for all supported languages
//!
//! This is implemented as a standalone zig script, rather as a step in build.zig, because this is
//! a "meta" build system --- we need to orchestrate `zig build`, `go build`, `npm publish` and
//! friends, and treat them as peers.
//!
//! Note on verbosity: to ease debugging, try to keep the output to O(1) lines per command. The idea
//! here is that, if something goes wrong, you can see _what_ goes wrong and easily copy-paste
//! specific commands to your local terminal, but, at the same time, you don't want to sift through
//! megabytes of info-level noise first.

const builtin = @import("builtin");
const std = @import("std");
const log = std.log;
const assert = std.debug.assert;

const Shell = @import("../shell.zig");
const multiversioning = @import("../multiversioning.zig");
const changelog = @import("./changelog.zig");

const multiversion_binary_size_max = multiversioning.multiversion_binary_size_max;

const Language = enum { dotnet, go, java, node, python, zig, docker };
const LanguageSet = std.enums.EnumSet(Language);
pub const CLIArgs = struct {
    sha: []const u8,
    language: ?Language = null,
    build: bool = false,
    publish: bool = false,
    // Set if there's no changelog entry for the current code. That is, if the top changelog
    // entry describes a past release, and not the release we are creating here.
    //
    // This flag is used to test the release process on the main branch.
    no_changelog: bool = false,
};

const VersionInfo = struct {
    // release_triple is a comptime parameter of the VSR used for the upgrade protocol.
    release_triple: []const u8,
    release_triple_client_min: []const u8,
    // tag is the symbolic name of the release used as a git tag and a version for client libraries.
    // Normally, the tag and the release_triple match, but it is possible to have different tags
    // with matching release_triples, for hot-fixes.
    tag: []const u8,
    // The git tag/GitHub release to download to include in a multiversion binary.
    tag_multiversion: []const u8,
    sha: []const u8,
};

pub fn main(shell: *Shell, gpa: std.mem.Allocator, cli_args: CLIArgs) !void {
    _ = gpa;

    const languages = if (cli_args.language) |language|
        LanguageSet.initOne(language)
    else
        LanguageSet.initFull();

    const changelog_text = try shell.project_root.readFileAlloc(
        shell.arena.allocator(),
        "CHANGELOG.md",
        1024 * 1024,
    );
    var changelog_iteratator = changelog.ChangelogIterator.init(changelog_text);
    const release, const release_multiversion, const changelog_body = blk: {
        if (cli_args.no_changelog) {
            var last_release = changelog_iteratator.next_changelog().?;
            while (last_release.release == null) {
                last_release = changelog_iteratator.next_changelog().?;
            }

            break :blk .{
                multiversioning.Release.from(.{
                    .major = last_release.release.?.triple().major,
                    .minor = last_release.release.?.triple().minor,
                    .patch = last_release.release.?.triple().patch + 1,
                }),
                last_release.release.?,
                "",
            };
        } else {
            const changelog_current = changelog_iteratator.next_changelog().?;
            if (changelog_current.release == null) {
                @panic("The last changelog entry must have a release version.");
            }
            const changelog_previous = while (changelog_iteratator.next_changelog()) |entry| {
                // The release number can be null if it was tagged as "unreleased".
                if (entry.release == null) continue;
                break entry;
            } else unreachable;
            break :blk .{
                changelog_current.release.?,
                changelog_previous.release.?,
                changelog_current.text_body,
            };
        }
    };
    assert(multiversioning.Release.less_than({}, release_multiversion, release));

    // Ensure we're building a version newer than the first multiversion release. That was
    // bootstrapped with code to do a custom build of the release before that (see git history)
    // whereas now past binaries are downloaded and the multiversion parts extracted.
    const first_multiversion_release = "0.15.4";
    assert(release.value >
        (try multiversioning.Release.parse(first_multiversion_release)).value);

    // The minimum client version allowed to connect. This has implications for backwards
    // compatibility and the upgrade path for replicas and clients. If there's no overlap
    // between a replica version and minimum client version - eg, replica 0.15.4 requires
    // client 0.15.4 - it means that upgrading requires coordination with clients, which
    // will be very inconvenient for operators.
    const release_triple_client_min = .{
        .major = 0,
        .minor = 15,
        .patch = 3,
    };

    const version_info = VersionInfo{
        .release_triple = try shell.fmt(
            "{[major]}.{[minor]}.{[patch]}",
            release.triple(),
        ),
        .release_triple_client_min = try shell.fmt(
            "{[major]}.{[minor]}.{[patch]}",
            release_triple_client_min,
        ),
        .tag = try shell.fmt(
            "{[major]}.{[minor]}.{[patch]}",
            release.triple(),
        ),
        .tag_multiversion = try shell.fmt(
            "{[major]}.{[minor]}.{[patch]}",
            release_multiversion.triple(),
        ),
        .sha = cli_args.sha,
    };

    // Typically GitHub tag matches the release triple in the binary exactly. For exceptional
    // hot-fix releases, the tag can be different. To make a hot-fix release, set the tag manually
    // here and remove the assert.
    assert(std.mem.eql(u8, version_info.release_triple, version_info.tag));

    log.info("release={s} sha={s}", .{ version_info.release_triple, version_info.sha });
    if (!std.mem.eql(u8, version_info.release_triple, version_info.tag)) {
        log.warn("tag != release, tag={s}", .{version_info.tag});
    }

    if (cli_args.build) {
        try build(shell, languages, version_info);
    }

    if (cli_args.publish) {
        assert(!cli_args.no_changelog);
        try publish(shell, languages, changelog_body, version_info);
    }
}

fn build(shell: *Shell, languages: LanguageSet, info: VersionInfo) !void {
    var section = try shell.open_section("build all");
    defer section.close();

    try shell.project_root.deleteTree("zig-out/dist");
    var dist_dir = try shell.project_root.makeOpenPath("zig-out/dist", .{});
    defer dist_dir.close();

    log.info("building TigerBeetle distribution into {s}", .{
        try dist_dir.realpathAlloc(shell.arena.allocator(), "."),
    });

    if (languages.contains(.zig)) {
        var dist_dir_tigerbeetle = try dist_dir.makeOpenPath("tigerbeetle", .{});
        defer dist_dir_tigerbeetle.close();

        try build_tigerbeetle(shell, info, dist_dir_tigerbeetle);
    }

    if (languages.contains(.dotnet)) {
        var dist_dir_dotnet = try dist_dir.makeOpenPath("dotnet", .{});
        defer dist_dir_dotnet.close();

        try build_dotnet(shell, info, dist_dir_dotnet);
    }

    if (languages.contains(.go)) {
        var dist_dir_go = try dist_dir.makeOpenPath("go", .{});
        defer dist_dir_go.close();

        try build_go(shell, info, dist_dir_go);
    }

    if (languages.contains(.java)) {
        var dist_dir_java = try dist_dir.makeOpenPath("java", .{});
        defer dist_dir_java.close();

        try build_java(shell, info, dist_dir_java);
    }

    if (languages.contains(.node)) {
        var dist_dir_node = try dist_dir.makeOpenPath("node", .{});
        defer dist_dir_node.close();

        try build_node(shell, info, dist_dir_node);
    }

    if (languages.contains(.python)) {
        var dist_dir_python = try dist_dir.makeOpenPath("python", .{});
        defer dist_dir_python.close();

        try build_python(shell, info, dist_dir_python);
    }
}

fn build_tigerbeetle(shell: *Shell, info: VersionInfo, dist_dir: std.fs.Dir) !void {
    var section = try shell.open_section("build tigerbeetle");
    defer section.close();

    // We shell out to `zip` for creating archives, so we need an absolute path here.
    const dist_dir_path = try dist_dir.realpathAlloc(shell.arena.allocator(), ".");

    const targets = .{
        "x86_64-linux",
        "x86_64-windows",
        "aarch64-linux",
        "aarch64-macos", // Will build a universal binary.
    };

    const sha_date = try shell.exec_stdout("git show --no-patch --no-notes --pretty=%cI {sha}", .{
        .sha = info.sha,
    });

    // Build tigerbeetle binary for all OS/CPU combinations we support and copy the result to
    // `dist`.
    inline for (.{ true, false }) |debug| {
        inline for (targets) |target| {
            try shell.exec_zig(
                \\build
                \\    -Dtarget={target}
                \\    -Drelease={release}
                \\    -Dgit-commit={commit}
                \\    -Dconfig-release={release_triple}
                \\    -Dconfig-release-client-min={release_triple_client_min}
                \\    -Dmultiversion={tag_multiversion}
            , .{
                .target = target,
                .release = if (debug) "false" else "true",
                .commit = info.sha,
                .release_triple = info.release_triple,
                .release_triple_client_min = info.release_triple_client_min,
                .tag_multiversion = info.tag_multiversion,
            });

            const windows = comptime std.mem.eql(u8, target, "x86_64-windows");
            const macos = comptime std.mem.eql(u8, target, "aarch64-macos");

            const exe_name = "tigerbeetle" ++ if (windows) ".exe" else "";
            const zip_name = "tigerbeetle-" ++
                (if (macos) "universal-macos" else target) ++
                (if (debug) "-debug" else "") ++
                ".zip";

            if ((std.mem.eql(u8, target, "x86_64-linux") and builtin.target.os.tag == .linux) or
                (macos and builtin.target.os.tag == .macos) or
                (windows and builtin.target.os.tag == .windows))
            {
                const output = try shell.exec_stdout("./{exe_name} version --verbose", .{
                    .exe_name = exe_name,
                });
                assert(std.mem.indexOf(u8, output, "process.verify=true") != null);
                const build_mode = if (debug)
                    "build.mode=builtin.OptimizeMode.Debug"
                else
                    "build.mode=builtin.OptimizeMode.ReleaseSafe";
                assert(std.mem.indexOf(u8, output, build_mode) != null);
            }

            try shell.exec("touch -d {sha_date} {exe_name}", .{
                .sha_date = sha_date,
                .exe_name = exe_name,
            });
            try shell.exec("zip -9 {zip_path} {exe_name}", .{
                .zip_path = try shell.fmt("{s}/{s}", .{ dist_dir_path, zip_name }),
                .exe_name = exe_name,
            });
        }
    }
}

fn build_dotnet(shell: *Shell, info: VersionInfo, dist_dir: std.fs.Dir) !void {
    var section = try shell.open_section("build dotnet");
    defer section.close();

    try shell.pushd("./src/clients/dotnet");
    defer shell.popd();

    const dotnet_version = shell.exec_stdout("dotnet --version", .{}) catch {
        return error.NoDotnet;
    };
    log.info("dotnet version {s}", .{dotnet_version});

    try shell.exec_zig(
        \\build clients:dotnet -Drelease -Dconfig-release={release_triple}
        \\ -Dconfig-release-client-min={release_triple_client_min}
    , .{
        .release_triple = info.release_triple,
        .release_triple_client_min = info.release_triple_client_min,
    });
    try shell.exec(
        \\dotnet pack TigerBeetle --configuration Release
        \\/p:AssemblyVersion={tag} /p:Version={tag}
    , .{ .tag = info.tag });

    try Shell.copy_path(
        shell.cwd,
        try shell.fmt("TigerBeetle/bin/Release/tigerbeetle.{s}.nupkg", .{info.tag}),
        dist_dir,
        try shell.fmt("tigerbeetle.{s}.nupkg", .{info.tag}),
    );
}

fn build_go(shell: *Shell, info: VersionInfo, dist_dir: std.fs.Dir) !void {
    var section = try shell.open_section("build go");
    defer section.close();

    try shell.pushd("./src/clients/go");
    defer shell.popd();

    try shell.exec_zig(
        \\build clients:go -Drelease -Dconfig-release={release_triple}
        \\ -Dconfig-release-client-min={release_triple_client_min}
    , .{
        .release_triple = info.release_triple,
        .release_triple_client_min = info.release_triple_client_min,
    });

    const files = try shell.exec_stdout("git ls-files", .{});
    var files_lines = std.mem.tokenize(u8, files, "\n");
    var copied_count: u32 = 0;
    while (files_lines.next()) |file| {
        assert(file.len > 3);
        try Shell.copy_path(shell.cwd, file, dist_dir, file);
        copied_count += 1;
    }
    assert(copied_count >= 10);

    const native_files = try shell.find(.{ .where = &.{"."}, .extensions = &.{ ".a", ".lib" } });
    copied_count = 0;
    for (native_files) |native_file| {
        try Shell.copy_path(shell.cwd, native_file, dist_dir, native_file);
        copied_count += 1;
    }
    // 5 = 3 + 2
    //     3 = x86_64 for mac, windows and linux
    //         2 = aarch64 for mac and linux
    assert(copied_count == 5);

    const readme = try shell.fmt(
        \\# tigerbeetle-go
        \\This repo has been automatically generated from
        \\[tigerbeetle/tigerbeetle@{[sha]s}](https://github.com/tigerbeetle/tigerbeetle/commit/{[sha]s})
        \\to keep binary blobs out of the monorepo.
        \\
        \\Please see
        \\<https://github.com/tigerbeetle/tigerbeetle/tree/main/src/clients/go>
        \\for documentation and contributions.
    , .{ .sha = info.sha });
    try dist_dir.writeFile(.{ .sub_path = "README.md", .data = readme });
}

fn build_java(shell: *Shell, info: VersionInfo, dist_dir: std.fs.Dir) !void {
    var section = try shell.open_section("build java");
    defer section.close();

    try shell.pushd("./src/clients/java");
    defer shell.popd();

    const java_version = shell.exec_stdout("java --version", .{}) catch {
        return error.NoJava;
    };
    log.info("java version {s}", .{java_version});

    try shell.exec_zig(
        \\build clients:java -Drelease -Dconfig-release={release_triple}
        \\ -Dconfig-release-client-min={release_triple_client_min}
    , .{
        .release_triple = info.release_triple,
        .release_triple_client_min = info.release_triple_client_min,
    });

    try backup_create(shell.cwd, "pom.xml");
    defer backup_restore(shell.cwd, "pom.xml");

    try shell.exec(
        \\mvn --batch-mode --quiet --file pom.xml
        \\versions:set -DnewVersion={tag}
    , .{ .tag = info.tag });

    try shell.exec(
        \\mvn --batch-mode --quiet --file pom.xml
        \\  -Dmaven.test.skip -Djacoco.skip
        \\  package
    , .{});

    try Shell.copy_path(
        shell.cwd,
        try shell.fmt("target/tigerbeetle-java-{s}.jar", .{info.tag}),
        dist_dir,
        try shell.fmt("tigerbeetle-java-{s}.jar", .{info.tag}),
    );
}

fn build_node(shell: *Shell, info: VersionInfo, dist_dir: std.fs.Dir) !void {
    var section = try shell.open_section("build node");
    defer section.close();

    try shell.pushd("./src/clients/node");
    defer shell.popd();

    const node_version = shell.exec_stdout("node --version", .{}) catch {
        return error.NoNode;
    };
    log.info("node version {s}", .{node_version});

    try shell.exec_zig(
        \\build clients:node -Drelease -Dconfig-release={release_triple}
        \\ -Dconfig-release-client-min={release_triple_client_min}
    , .{
        .release_triple = info.release_triple,
        .release_triple_client_min = info.release_triple_client_min,
    });

    try backup_create(shell.cwd, "package.json");
    defer backup_restore(shell.cwd, "package.json");

    try backup_create(shell.cwd, "package-lock.json");
    defer backup_restore(shell.cwd, "package-lock.json");

    try shell.exec(
        "npm version --no-git-tag-version {tag}",
        .{ .tag = info.tag },
    );
    try shell.exec("npm install", .{});
    try shell.exec("npm pack --quiet", .{});

    try Shell.copy_path(
        shell.cwd,
        try shell.fmt("tigerbeetle-node-{s}.tgz", .{info.tag}),
        dist_dir,
        try shell.fmt("tigerbeetle-node-{s}.tgz", .{info.tag}),
    );
}

fn build_python(shell: *Shell, info: VersionInfo, dist_dir: std.fs.Dir) !void {
    var section = try shell.open_section("build python");
    defer section.close();

    try shell.pushd("./src/clients/python");
    defer shell.popd();

    const python_version = shell.exec_stdout("python3 --version", .{}) catch {
        return error.NoPython;
    };
    log.info("{s}", .{python_version});

    try shell.exec_zig(
        \\build clients:python -Drelease -Dconfig-release={release_triple}
        \\ -Dconfig-release-client-min={release_triple_client_min}
    , .{
        .release_triple = info.release_triple,
        .release_triple_client_min = info.release_triple_client_min,
    });

    try backup_create(shell.cwd, "pyproject.toml");
    defer backup_restore(shell.cwd, "pyproject.toml");

    const pyproject = try shell.cwd.readFileAlloc(
        shell.arena.allocator(),
        "pyproject.toml",
        1024 * 1024,
    );
    const version_line = try shell.fmt(
        "version = \"{s}\"",
        .{info.tag},
    );
    const pyproject_updated = try std.mem.replaceOwned(
        u8,
        shell.arena.allocator(),
        pyproject,
        "version = \"0.0.1\"",
        version_line,
    );
    assert(std.mem.indexOf(u8, pyproject_updated, version_line) != null);

    try shell.cwd.writeFile(.{
        .sub_path = "pyproject.toml",
        .data = pyproject_updated,
    });

    try shell.exec("python3 -m build .", .{});

    try Shell.copy_path(
        shell.cwd,
        try shell.fmt("dist/tigerbeetle-{s}-py3-none-any.whl", .{info.tag}),
        dist_dir,
        try shell.fmt("tigerbeetle-{s}-py3-none-any.whl", .{info.tag}),
    );
}

fn publish(
    shell: *Shell,
    languages: LanguageSet,
    changelog_body: []const u8,
    info: VersionInfo,
) !void {
    var section = try shell.open_section("publish all");
    defer section.close();

    { // Sanity check that the new release doesn't exist but the multiversion does.
        var tag_multiversion_exists = false;
        var tag_exists = false;
        const tags_exiting = try shell.exec_stdout(
            "gh release list --json tagName --jq {query}",
            .{ .query = ".[].tagName" },
        );
        var it = std.mem.split(u8, tags_exiting, "\n");
        while (it.next()) |tag_existing| {
            assert(std.mem.trim(u8, tag_existing, " \t\n\r").len == tag_existing.len);
            if (std.mem.eql(u8, tag_existing, info.release_triple)) {
                tag_exists = true;
            }
            if (std.mem.eql(u8, tag_existing, info.tag_multiversion)) {
                tag_multiversion_exists = true;
            }
        }
        assert(!tag_exists);
        assert(tag_multiversion_exists);
    }

    assert(try shell.dir_exists("zig-out/dist"));

    if (languages.contains(.zig)) {
        _ = try shell.env_get("GITHUB_TOKEN");
        const gh_version = shell.exec_stdout("gh --version", .{}) catch {
            return error.NoGh;
        };
        log.info("gh version {s}", .{gh_version});

        const release_included_min = blk: {
            shell.project_root.deleteFile("tigerbeetle") catch {};
            defer shell.project_root.deleteFile("tigerbeetle") catch {};

            try shell.exec("unzip ./zig-out/dist/tigerbeetle/tigerbeetle-x86_64-linux.zip", .{});
            const past_binary_contents = try shell.cwd.readFileAllocOptions(
                shell.arena.allocator(),
                "tigerbeetle",
                multiversion_binary_size_max,
                null,
                8,
                null,
            );

            const parsed_offsets = try multiversioning.parse_elf(past_binary_contents);
            const header_bytes =
                past_binary_contents[parsed_offsets.x86_64.?.header_offset..][0..@sizeOf(
                multiversioning.MultiversionHeader,
            )];

            const header = try multiversioning.MultiversionHeader.init_from_bytes(header_bytes);
            const release_min = header.past.releases[0];
            const release_max = header.past.releases[header.past.count - 1];
            assert(release_min < release_max);

            break :blk multiversioning.Release{ .value = release_min };
        };

        const notes = try shell.fmt(
            \\# {[tag]s}
            \\
            \\### Supported upgrade versions
            \\
            \\Oldest supported client version: {[release_triple_client_min]s}
            \\Oldest upgradable replica version: {[release_included_min]s}
            \\
            \\## Server
            \\
            \\* Binary: Download the zip for your OS and architecture from this page and unzip.
            \\* Docker: `docker pull ghcr.io/tigerbeetle/tigerbeetle:{[tag]s}`
            \\* Docker (debug image): `docker pull ghcr.io/tigerbeetle/tigerbeetle:{[tag]s}-debug`
            \\
            \\## Clients
            \\
            \\**NOTE**: Because of package manager caching, it may take a few
            \\minutes after the release for this version to appear in the package
            \\manager.
            \\
            \\You cannot run a newer client against an older cluster: clients are only compatible
            \\with replicas from their own release *or newer*, subject to the newer release's
            \\`Oldest supported client version`.
            \\
            \\* .NET: `dotnet add package tigerbeetle --version {[tag]s}`
            \\* Go: `go mod edit -require github.com/tigerbeetle/tigerbeetle-go@v{[tag]s}`
            \\* Java: Update the version of `com.tigerbeetle.tigerbeetle-java` in `pom.xml`
            \\  to `{[tag]s}`.
            \\* Node.js: `npm install --save-exact tigerbeetle-node@{[tag]s}`
            \\* Python: `pip install tigerbeetle=={[tag]s}`
            \\
            \\## Changelog
            \\
            \\{[changelog]s}
        , .{
            .tag = info.tag,
            .release_triple_client_min = info.release_triple_client_min,
            .release_included_min = release_included_min,
            .changelog = changelog_body,
        });

        try shell.exec(
            \\gh release create --draft
            \\  --target {sha}
            \\  --notes {notes}
            \\  {tag}
        , .{
            .sha = info.sha,
            .notes = notes,
            .tag = info.tag,
        });

        // Here and elsewhere for publishing we explicitly spell out the files we are uploading
        // instead of using a for loop to double-check the logic in `build`.
        const artifacts: []const []const u8 = &.{
            "zig-out/dist/tigerbeetle/tigerbeetle-aarch64-linux-debug.zip",
            "zig-out/dist/tigerbeetle/tigerbeetle-aarch64-linux.zip",
            "zig-out/dist/tigerbeetle/tigerbeetle-universal-macos-debug.zip",
            "zig-out/dist/tigerbeetle/tigerbeetle-universal-macos.zip",
            "zig-out/dist/tigerbeetle/tigerbeetle-x86_64-linux-debug.zip",
            "zig-out/dist/tigerbeetle/tigerbeetle-x86_64-linux.zip",
            "zig-out/dist/tigerbeetle/tigerbeetle-x86_64-windows-debug.zip",
            "zig-out/dist/tigerbeetle/tigerbeetle-x86_64-windows.zip",
        };
        try shell.exec("gh release upload {tag} {artifacts}", .{
            .tag = info.tag,
            .artifacts = artifacts,
        });
    }

    if (languages.contains(.docker)) try publish_docker(shell, info);
    if (languages.contains(.dotnet)) try publish_dotnet(shell, info);
    if (languages.contains(.go)) try publish_go(shell, info);
    if (languages.contains(.java)) try publish_java(shell, info);
    if (languages.contains(.node)) try publish_node(shell, info);
    if (languages.contains(.python)) try publish_python(shell, info);

    // Our docs are build with node, so publish the docs together with the node package, but do it
    // last so that if docs fail everything else is still released.
    if (languages.contains(.node)) try publish_docs(shell, info);

    if (languages.contains(.zig)) {
        try shell.exec(
            \\gh release edit --draft=false --latest=true
            \\  {tag}
        , .{ .tag = info.tag });
    }
}

fn publish_dotnet(shell: *Shell, info: VersionInfo) !void {
    var section = try shell.open_section("publish dotnet");
    defer section.close();

    assert(try shell.dir_exists("zig-out/dist/dotnet"));

    const nuget_key = try shell.env_get("NUGET_KEY");
    try shell.exec(
        \\dotnet nuget push
        \\    --api-key {nuget_key}
        \\    --source https://api.nuget.org/v3/index.json
        \\    {package}
    , .{
        .nuget_key = nuget_key,
        .package = try shell.fmt("zig-out/dist/dotnet/tigerbeetle.{s}.nupkg", .{
            info.tag,
        }),
    });
}

fn publish_go(shell: *Shell, info: VersionInfo) !void {
    var section = try shell.open_section("publish go");
    defer section.close();

    assert(try shell.dir_exists("zig-out/dist/go"));

    const token = try shell.env_get("TIGERBEETLE_GO_PAT");
    try shell.exec(
        \\git clone --no-checkout --depth 1
        \\  https://oauth2:{token}@github.com/tigerbeetle/tigerbeetle-go.git tigerbeetle-go
    , .{ .token = token });
    defer {
        shell.project_root.deleteTree("tigerbeetle-go") catch {};
    }

    const dist_files = try shell.find(.{ .where = &.{"zig-out/dist/go"} });
    assert(dist_files.len > 10);
    for (dist_files) |file| {
        try Shell.copy_path(
            shell.project_root,
            file,
            shell.project_root,
            try std.mem.replaceOwned(
                u8,
                shell.arena.allocator(),
                file,
                "zig-out/dist/go",
                "tigerbeetle-go",
            ),
        );
    }

    try shell.pushd("./tigerbeetle-go");
    defer shell.popd();

    try shell.exec("git add .", .{});
    // Native libraries are ignored in this repository, but we want to push them to the
    // tigerbeetle-go one!
    try shell.exec("git add --force pkg/native", .{});

    try shell.git_env_setup(.{ .use_hostname = false });
    try shell.exec("git commit --message {message}", .{
        .message = try shell.fmt(
            "Autogenerated commit from tigerbeetle/tigerbeetle@{s}",
            .{info.sha},
        ),
    });

    try shell.exec("git tag tigerbeetle-{sha}", .{ .sha = info.sha });
    try shell.exec("git tag v{tag}", .{ .tag = info.tag });

    try shell.exec("git push origin main", .{});
    try shell.exec("git push origin tigerbeetle-{sha}", .{ .sha = info.sha });
    try shell.exec("git push origin v{tag}", .{ .tag = info.tag });
}

fn publish_java(shell: *Shell, info: VersionInfo) !void {
    var section = try shell.open_section("publish java");
    defer section.close();

    assert(try shell.dir_exists("zig-out/dist/java"));

    // These variables don't have a special meaning in maven, and instead are a part of
    // settings.xml generated by GitHub actions.
    _ = try shell.env_get("MAVEN_USERNAME");
    _ = try shell.env_get("MAVEN_CENTRAL_TOKEN");
    _ = try shell.env_get("MAVEN_GPG_PASSPHRASE");

    // TODO: Maven uniquely doesn't support uploading pre-build package, so here we just rebuild
    // from source and upload a _different_ artifact. This is wrong.
    //
    // As far as I can tell, there isn't a great solution here. See, for example:
    //
    // <https://users.maven.apache.narkive.com/jQ3WocgT/mvn-deploy-without-rebuilding>
    //
    // I think what we should do here is for `build` to deploy to the local repo, and then use
    //
    // <https://gist.github.com/rishabh9/183cc0c4c3ada4f8df94d65fcd73a502>
    //
    // to move the contents of that local repo to maven central. But this is todo, just rebuild now.
    try backup_create(shell.project_root, "src/clients/java/pom.xml");
    defer backup_restore(shell.project_root, "src/clients/java/pom.xml");

    try shell.exec(
        \\mvn --batch-mode --quiet --file src/clients/java/pom.xml
        \\  versions:set -DnewVersion={tag}
    , .{ .tag = info.tag });

    try shell.exec(
        \\mvn --batch-mode --quiet --file src/clients/java/pom.xml
        \\  -Dmaven.test.skip -Djacoco.skip
        \\  deploy
    , .{});
}

fn publish_node(shell: *Shell, info: VersionInfo) !void {
    var section = try shell.open_section("publish node");
    defer section.close();

    assert(try shell.dir_exists("zig-out/dist/node"));

    // `NODE_AUTH_TOKEN` env var doesn't have a special meaning in npm. It does have special meaning
    // in GitHub Actions, which adds a literal
    //
    //    //registry.npmjs.org/:_authToken=${NODE_AUTH_TOKEN}
    //
    // to the .npmrc file (that is, node config file itself supports env variables).
    _ = try shell.env_get("NODE_AUTH_TOKEN");
    try shell.exec("npm publish {package}", .{
        .package = try shell.fmt("zig-out/dist/node/tigerbeetle-node-{s}.tgz", .{
            info.tag,
        }),
    });
}

fn publish_python(shell: *Shell, info: VersionInfo) !void {
    var section = try shell.open_section("publish python");
    defer section.close();

    assert(try shell.dir_exists("zig-out/dist/python"));

    _ = try shell.env_get("TWINE_USERNAME");
    _ = try shell.env_get("TWINE_PASSWORD");

    try shell.exec("python3 -m twine upload {package}", .{
        .package = try shell.fmt("zig-out/dist/python/tigerbeetle-{s}-py3-none-any.whl", .{
            info.tag,
        }),
    });
}

// Docker is not required and not recommended for running TigerBeetle. A container is published
// just for convenience of consumers expecting one!
fn publish_docker(shell: *Shell, info: VersionInfo) !void {
    var section = try shell.open_section("publish docker");
    defer section.close();

    assert(try shell.dir_exists("zig-out/dist/tigerbeetle"));

    try shell.exec(
        \\docker login --username tigerbeetle --password {password} ghcr.io
    , .{
        .password = try shell.env_get("GITHUB_TOKEN"),
    });

    try shell.exec(
        \\docker buildx create --use
    , .{});

    for ([_]bool{ true, false }) |debug| {
        const triples = [_][]const u8{ "aarch64-linux", "x86_64-linux" };
        const docker_arches = [_][]const u8{ "arm64", "amd64" };
        for (triples, docker_arches) |triple, docker_arch| {
            // We need to unzip binaries from dist. For simplicity, don't bother with a temporary
            // directory.
            shell.project_root.deleteFile("tigerbeetle") catch {};
            try shell.exec("unzip ./zig-out/dist/tigerbeetle/tigerbeetle-{triple}{debug}.zip", .{
                .triple = triple,
                .debug = if (debug) "-debug" else "",
            });
            try shell.project_root.rename(
                "tigerbeetle",
                try shell.fmt("tigerbeetle-{s}", .{docker_arch}),
            );
        }
        // Build docker container by copying pre-build executable inside.
        //
        // TigerBeetle doesn't install its own signal handlers, and PID 1 doesn't have a default
        // SIGTERM signal handler. (See https://github.com/krallin/tini#why-tini). Using "tini" as
        // PID 1 ensures that signals work as expected, so e.g. "docker stop" will not hang.
        try shell.exec_options(
            .{
                .stdin_slice =
                \\FROM alpine:3.19
                \\RUN apk add --no-cache tini
                \\ARG TARGETARCH
                \\COPY tigerbeetle-${TARGETARCH} /tigerbeetle
                \\ENTRYPOINT ["tini", "--", "/tigerbeetle"]
                ,
            },
            \\docker buildx build
            \\   --file - .
            \\   --platform linux/amd64,linux/arm64
            \\   --tag ghcr.io/tigerbeetle/tigerbeetle:{tag}{debug}
            \\   {tag_latest}
            \\   --push
        ,
            .{
                .tag = info.tag,
                .debug = if (debug) "-debug" else "",
                .tag_latest = @as(
                    []const []const u8,
                    if (debug) &.{} else &.{ "--tag", "ghcr.io/tigerbeetle/tigerbeetle:latest" },
                ),
            },
        );

        // Sadly, there isn't an easy way to locally build & test a multiplatform image without
        // pushing it out to the registry first. As docker testing isn't covered under not rocket
        // science rule, let's do a best effort after-the-fact testing here.
        const version_verbose = try shell.exec_stdout(
            \\docker run ghcr.io/tigerbeetle/tigerbeetle:{tag}{debug} version --verbose
        , .{
            .tag = info.tag,
            .debug = if (debug) "-debug" else "",
        });
        const mode = if (debug) "Debug" else "ReleaseSafe";
        assert(std.mem.indexOf(u8, version_verbose, mode) != null);
        assert(std.mem.indexOf(u8, version_verbose, info.release_triple) != null);
    }
}

fn publish_docs(shell: *Shell, info: VersionInfo) !void {
    var section = try shell.open_section("publish docs");
    defer section.close();

    {
        try shell.pushd("./src/docs_website");
        defer shell.popd();

        try shell.exec_zig("build", .{});
    }

    const token = try shell.env_get("TIGERBEETLE_DOCS_PAT");
    try shell.exec(
        \\git clone --no-checkout --depth 1
        \\  https://oauth2:{token}@github.com/tigerbeetle/docs.git tigerbeetle-docs
    , .{ .token = token });
    defer {
        shell.project_root.deleteTree("tigerbeetle-docs") catch {};
    }

    const docs_files = try shell.find(.{ .where = &.{"src/docs_website/zig-out"} });
    assert(docs_files.len > 10);
    for (docs_files) |file| {
        try Shell.copy_path(
            shell.project_root,
            file,
            shell.project_root,
            try std.mem.replaceOwned(
                u8,
                shell.arena.allocator(),
                file,
                "src/docs_website/zig-out",
                "tigerbeetle-docs/",
            ),
        );
    }

    try shell.pushd("./tigerbeetle-docs");
    defer shell.popd();

    try shell.exec("git add .", .{});
    try shell.env.put("GIT_AUTHOR_NAME", "TigerBeetle Bot");
    try shell.env.put("GIT_AUTHOR_EMAIL", "bot@tigerbeetle.com");
    try shell.env.put("GIT_COMMITTER_NAME", "TigerBeetle Bot");
    try shell.env.put("GIT_COMMITTER_EMAIL", "bot@tigerbeetle.com");
    // We want to push a commit even if there are no changes to the docs, to make sure
    // that the latest commit message on the docs repo points to the latest tigerbeetle
    // release.
    try shell.exec("git commit --allow-empty --message {message}", .{
        .message = try shell.fmt(
            "Autogenerated commit from tigerbeetle/tigerbeetle@{s}",
            .{info.sha},
        ),
    });

    try shell.exec("git push origin main", .{});
}

fn backup_create(dir: std.fs.Dir, comptime file: []const u8) !void {
    try Shell.copy_path(dir, file, dir, file ++ ".backup");
}

fn backup_restore(dir: std.fs.Dir, comptime file: []const u8) void {
    dir.deleteFile(file) catch {};
    Shell.copy_path(dir, file ++ ".backup", dir, file) catch {};
    dir.deleteFile(file ++ ".backup") catch {};
}
