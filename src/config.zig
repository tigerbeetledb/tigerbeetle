//! Raw configuration values.
//!
//! Code which needs these values should use `constants.zig` instead.
//! Configuration values are set from a combination of:
//! - default values
//! - `root.tigerbeetle_config`
//! - `@import("tigerbeetle_options")`

const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;

const root = @import("root");

const BuildOptions = struct {
    config_verify: bool,
    hash_log_mode: HashLogMode,
    git_commit: ?[40]u8,
    release: ?[]const u8,
    release_client_min: ?[]const u8,
    config_aof_recovery: bool,
};

// Allow setting build-time config either via `build.zig` `Options`, or via a struct in the root
// file.
const build_options: BuildOptions = blk: {
    if (@hasDecl(root, "vsr_options")) {
        break :blk root.vsr_options;
    } else {
        const vsr_options = @import("vsr_options");
        // Zig's `addOptions` reuses the type, but redeclares it — identical structurally,
        // but a different type from a nominal typing perspective.
        var result: BuildOptions = undefined;
        for (std.meta.fields(BuildOptions)) |field| {
            @field(result, field.name) = launder_type(
                field.type,
                @field(vsr_options, field.name),
            );
        }
        break :blk result;
    }
};

fn launder_type(comptime T: type, comptime value: anytype) T {
    if (T == bool or
        T == []const u8 or
        T == ?[]const u8 or
        T == ?[40]u8)
    {
        return value;
    }
    if (@typeInfo(T) == .Enum) {
        assert(@typeInfo(@TypeOf(value)) == .Enum);
        return @field(T, @tagName(value));
    }
    unreachable;
}

const vsr = @import("vsr.zig");
const sector_size = @import("constants.zig").sector_size;

pub const Config = struct {
    pub const Cluster = ConfigCluster;
    pub const Process = ConfigProcess;

    cluster: ConfigCluster,
    process: ConfigProcess,

    /// Returns true if the configuration is intended for "production".
    /// Intended solely for extra sanity-checks: all meaningful decisions should be driven by
    /// specific fields of the config.
    pub fn is_production(config: *const Config) bool {
        return config.cluster.journal_slot_count > ConfigCluster.journal_slot_count_min;
    }
};

/// Configurations which are tunable per-replica (or per-client).
/// - Replica configs need not equal each other.
/// - Client configs need not equal each other.
/// - Client configs need not equal replica configs.
/// - Replica configs can change between restarts.
///
/// Fields are documented within constants.zig.
// TODO: Some of these could be runtime parameters (e.g. grid_scrubber_cycle_ms).
const ConfigProcess = struct {
    log_level: std.log.Level = .info,
    hash_log_mode: HashLogMode = .none,
    verify: bool,
    release: vsr.Release = vsr.Release.minimum,
    release_client_min: vsr.Release = vsr.Release.minimum,
    git_commit: ?[40]u8 = null,
    port: u16 = 3001,
    address: []const u8 = "127.0.0.1",
    storage_size_limit_default: u64 = 16 * 1024 * 1024 * 1024 * 1024,
    storage_size_limit_max: u64 = 64 * 1024 * 1024 * 1024 * 1024,
    memory_size_max_default: u64 = 1024 * 1024 * 1024,
    cache_accounts_size_default: usize,
    cache_transfers_size_default: usize,
    cache_transfers_pending_size_default: usize,
    cache_account_balances_size_default: usize,
    client_request_queue_max: usize = 2,
    lsm_manifest_node_size: usize = 16 * 1024,
    connection_delay_min_ms: u64 = 50,
    connection_delay_max_ms: u64 = 1000,
    tcp_backlog: u31 = 64,
    tcp_rcvbuf: c_int = 4 * 1024 * 1024,
    tcp_keepalive: bool = true,
    tcp_keepidle: c_int = 5,
    tcp_keepintvl: c_int = 4,
    tcp_keepcnt: c_int = 3,
    tcp_nodelay: bool = true,
    direct_io: bool,
    journal_iops_read_max: usize = 8,
    journal_iops_write_max: usize = 8,
    client_replies_iops_read_max: usize = 1,
    client_replies_iops_write_max: usize = 2,
    client_request_completion_warn_ms: u64 = 200,
    tick_ms: u63 = 10,
    rtt_ms: u64 = 300,
    rtt_max_ms: u64 = 1000 * 60,
    rtt_multiple: u8 = 2,
    backoff_min_ms: u64 = 100,
    backoff_max_ms: u64 = 10000,
    clock_offset_tolerance_max_ms: u64 = 10000,
    clock_epoch_max_ms: u64 = 60000,
    clock_synchronization_window_min_ms: u64 = 2000,
    clock_synchronization_window_max_ms: u64 = 20000,
    grid_iops_read_max: u64 = 16,
    grid_iops_write_max: u64 = 16,
    grid_cache_size_default: u64 = 1024 * 1024 * 1024,
    grid_repair_request_max: usize = 4,
    grid_repair_reads_max: usize = 4,
    grid_missing_blocks_max: usize = 30,
    grid_missing_tables_max: usize = 6,
    grid_scrubber_reads_max: usize = 1,
    grid_scrubber_cycle_ms: usize = std.time.ms_per_day * 180,
    grid_scrubber_interval_ms_min: usize = std.time.ms_per_s / 20,
    grid_scrubber_interval_ms_max: usize = std.time.ms_per_s * 10,
    aof_recovery: bool = false,
    multiversion_binary_platform_size_max: u64 = 64 * 1024 * 1024,
    multiversion_poll_interval_ms: u64 = 1000,
};

/// Configurations which are tunable per-cluster.
/// - All replicas within a cluster must have the same configuration.
/// - Replicas must reuse the same configuration when the binary is upgraded — they do not change
///   over the cluster lifetime.
/// - The storage formats generated by different ConfigClusters are incompatible.
///
/// Fields are documented within constants.zig.
const ConfigCluster = struct {
    cache_line_size: comptime_int = 64,
    clients_max: usize,
    pipeline_prepare_queue_max: usize = 8,
    view_change_headers_suffix_max: usize = 8 + 1,
    quorum_replication_max: u8 = 3,
    journal_slot_count: usize = 1024,
    message_size_max: usize = 1 * 1024 * 1024,
    superblock_copies: comptime_int = 4,
    block_size: comptime_int = 512 * 1024,
    lsm_levels: u6 = 7,
    lsm_growth_factor: u32 = 8,
    lsm_compaction_ops: comptime_int = 32,
    lsm_snapshots_max: usize = 32,
    lsm_manifest_compact_extra_blocks: comptime_int = 1,
    lsm_table_coalescing_threshold_percent: comptime_int = 50,
    vsr_releases_max: usize = 64,

    /// Minimal value.
    // TODO(batiati): Maybe this constant should be derived from `grid_iops_read_max`,
    // since each scan can read from `lsm_levels` in parallel.
    lsm_scans_max: comptime_int = 6,

    /// The WAL requires at least two sectors of redundant headers — otherwise we could lose them
    /// all to a single torn write. A replica needs at least one valid redundant header to
    /// determine an (untrusted) maximum op in recover_torn_prepare(), without which it cannot
    /// truncate a torn prepare.
    pub const journal_slot_count_min = 2 * @divExact(sector_size, @sizeOf(vsr.Header));

    pub const clients_max_min = 1;

    /// The smallest possible message_size_max (for use in the simulator to improve performance).
    /// The message body must have room for pipeline_prepare_queue_max headers in the DVC.
    pub fn message_size_max_min(clients_max: usize) usize {
        return @max(
            sector_size,
            std.mem.alignForward(
                usize,
                @sizeOf(vsr.Header) + clients_max * @sizeOf(vsr.Header),
                sector_size,
            ),
        );
    }

    /// Fingerprint of the cluster-wide configuration.
    /// It is used to assert that all cluster members share the same config.
    pub fn checksum(comptime config: ConfigCluster) u128 {
        @setEvalBranchQuota(10_000);
        comptime var config_bytes: []const u8 = &.{};
        comptime for (std.meta.fields(ConfigCluster)) |field| {
            const value = @field(config, field.name);
            const value_64 = @as(u64, value);
            assert(builtin.target.cpu.arch.endian() == .little);
            config_bytes = config_bytes ++ std.mem.asBytes(&value_64);
        };
        return vsr.checksum(config_bytes);
    }
};

pub const ConfigBase = enum {
    production,
    test_min,
    default,
};

pub const HashLogMode = enum {
    none,
    create,
    check,
};

pub const configs = struct {
    /// A good default config for production.
    pub const default_production = Config{
        .process = .{
            .direct_io = true,
            .cache_accounts_size_default = @sizeOf(vsr.tigerbeetle.Account) * 1024 * 1024,
            .cache_transfers_size_default = 0,
            .cache_transfers_pending_size_default = 0,
            .cache_account_balances_size_default = 0,
            .verify = true,
        },
        .cluster = .{
            .clients_max = 64,
        },
    };

    /// Minimal test configuration — small WAL, small grid block size, etc.
    /// Not suitable for production, but good for testing code that would be otherwise hard to
    /// reach.
    pub const test_min = Config{
        .process = .{
            .storage_size_limit_default = 1 * 1024 * 1024 * 1024,
            .storage_size_limit_max = 1 * 1024 * 1024 * 1024,
            .direct_io = false,
            .cache_accounts_size_default = @sizeOf(vsr.tigerbeetle.Account) * 256,
            .cache_transfers_size_default = 0,
            .cache_transfers_pending_size_default = 0,
            .cache_account_balances_size_default = 0,
            .journal_iops_read_max = 3,
            .journal_iops_write_max = 2,
            .grid_repair_request_max = 4,
            .grid_repair_reads_max = 4,
            .grid_missing_blocks_max = 3,
            .grid_missing_tables_max = 2,
            .grid_scrubber_reads_max = 2,
            .grid_scrubber_cycle_ms = std.time.ms_per_hour,
            .verify = true,
        },
        .cluster = .{
            .clients_max = 4 + 3,
            .pipeline_prepare_queue_max = 4,
            .view_change_headers_suffix_max = 4 + 1,
            .journal_slot_count = Config.Cluster.journal_slot_count_min,
            .message_size_max = Config.Cluster.message_size_max_min(4),

            .block_size = sector_size,
            .lsm_compaction_ops = 4,
            .lsm_growth_factor = 4,
            // (This is higher than the production default value because the block size is smaller.)
            .lsm_manifest_compact_extra_blocks = 5,
            // (We need to fuzz more scans merge than in production.)
            .lsm_scans_max = 12,
        },
    };

    pub const current = current: {
        var base = if (@hasDecl(root, "tigerbeetle_config"))
            root.tigerbeetle_config
        else if (builtin.is_test)
            test_min
        else
            default_production;

        if (build_options.release == null and build_options.release_client_min != null) {
            @compileError("must set release if setting release_client_min");
        }

        if (build_options.release_client_min == null and build_options.release != null) {
            @compileError("must set release_client_min if setting release");
        }

        const release = if (build_options.release) |release|
            vsr.Release.from(vsr.ReleaseTriple.parse(release) catch {
                @compileError("invalid release version");
            })
        else
            vsr.Release.minimum;

        const release_client_min = if (build_options.release_client_min) |release_client_min|
            vsr.Release.from(vsr.ReleaseTriple.parse(release_client_min) catch {
                @compileError("invalid release_client_min version");
            })
        else
            vsr.Release.minimum;

        // TODO Use additional build options to overwrite other fields.
        base.process.hash_log_mode = build_options.hash_log_mode;
        base.process.release = release;
        base.process.release_client_min = release_client_min;
        base.process.git_commit = build_options.git_commit;
        base.process.aof_recovery = build_options.config_aof_recovery;
        base.process.verify = build_options.config_verify;

        assert(base.process.release.value >= base.process.release_client_min.value);

        break :current base;
    };
};
