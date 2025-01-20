//! Constants are the configuration that the code actually imports — they include:
//! - all of the configuration values (flattened)
//! - derived configuration values,

const std = @import("std");
const assert = std.debug.assert;
const vsr = @import("vsr.zig");
const Config = @import("config.zig").Config;
const stdx = @import("stdx.zig");

pub const config = @import("config.zig").configs.current;

pub const semver = std.SemanticVersion{
    .major = config.process.release.triple().major,
    .minor = config.process.release.triple().minor,
    .patch = config.process.release.triple().patch,
    .pre = null,
    .build = if (config.process.git_commit) |sha_full| sha_full[0..7] else null,
};

// Which mode to use for ./testing/hash_log.zig.
pub const hash_log_mode = config.process.hash_log_mode;

/// The maximum number of replicas allowed in a cluster.
pub const replicas_max = 6;
/// The maximum number of standbys allowed in a cluster.
pub const standbys_max = 6;
/// The maximum number of cluster members (either standbys or active replicas).
pub const members_max = replicas_max + standbys_max;

/// All operations <vsr_operations_reserved are reserved for the control protocol.
/// All operations ≥vsr_operations_reserved are available for the state machine.
pub const vsr_operations_reserved: u8 = 128;

comptime {
    assert(vsr_operations_reserved <= std.math.maxInt(u8));
}

/// The checkpoint interval is chosen to be the highest possible value that satisfies the
/// constraints described below.
pub const vsr_checkpoint_ops = journal_slot_count -
    lsm_compaction_ops -
    lsm_compaction_ops * stdx.div_ceil(pipeline_prepare_queue_max * 2, lsm_compaction_ops);

comptime {
    // Invariant: to guarantee durability, a log entry from a previous checkpoint can be overwritten
    // only when there is a quorum of replicas at the next checkpoint.
    //
    // This assert guarantees that when a prepare gets bumped from the log, there is a prepare
    // _committed_ on top of the next checkpoint, which in turn guarantees the existence of a
    // checkpoint quorum.
    //
    // More specifically, the checkpoint interval must be less than the WAL length by (at least) the
    // sum of:
    // - `lsm_compaction_ops`: Ensure that the final batch of entries immediately preceding a
    //   checkpoint trigger is not overwritten by the following checkpoint's entries. This final
    //   batch's updates were not persisted as part of the former checkpoint – they are only in
    //   memory until they are compacted by the *next* batch of commits (i.e. the first batch of
    //   the following checkpoint).
    // - `2 * pipeline_prepare_queue_max` (rounded up to the nearest lsm_compaction_ops multiple):
    //    This margin ensures that the entries prepared immediately following a checkpoint's prepare
    //    max never overwrite an entry from the previous WAL wrap until a quorum of replicas has
    //    reached that checkpoint. The first pipeline_prepare_queue_max is the maximum number of
    //    entries a replica can prepare after a checkpoint trigger, so checkpointing doesn't stall
    //    normal processing (referred to as the checkpoint's prepare_max). The second
    //    pipeline_prepare_queue_max ensures entries prepared after a checkpoint's prepare_max don't
    //    overwrite entries from the previous WAL wrap. By the time we start preparing entries after
    //    the second pipeline_prepare_queue_max, a quorum of replicas is guaranteed to have already
    //    reached the former checkpoint.
    assert(vsr_checkpoint_ops + lsm_compaction_ops + pipeline_prepare_queue_max * 2 <=
        journal_slot_count);
    assert(vsr_checkpoint_ops >= pipeline_prepare_queue_max);
    assert(vsr_checkpoint_ops >= lsm_compaction_ops);
    assert(vsr_checkpoint_ops % lsm_compaction_ops == 0);
}

pub const vsr_repair_message_budget_max = replicas_max * 2;
pub const vsr_repair_message_budget_refill = @divFloor(vsr_repair_message_budget_max, 2);

comptime {
    assert(vsr_repair_message_budget_max > 0);
    assert(vsr_repair_message_budget_refill > 0);
}

/// The maximum number of clients allowed per cluster, where each client has a unique 128-bit ID.
/// This impacts the amount of memory allocated at initialization by the server.
/// This determines the size of the VR client table used to cache replies to clients by client ID.
/// Each client has one entry in the VR client table to store the latest `message_size_max` reply.
pub const clients_max = config.cluster.clients_max;

comptime {
    assert(clients_max >= Config.Cluster.clients_max_min);
}

/// The maximum number of release versions (upgrade candidates) that can be advertised by a replica
/// in each ping message body.
pub const vsr_releases_max = config.cluster.vsr_releases_max;

/// The maximum cumulative size of a final TigerBeetle output binary - including potential past
/// releases and metadata.
pub fn multiversion_binary_platform_size_max(options: struct { macos: bool, debug: bool }) u64 {
    // {Linux, Windows} get the base value. macOS gets 2x since it has universal binaries. All cases
    // get a further 2x in debug.
    var size_max = config.process.multiversion_binary_platform_size_max;
    if (options.macos) size_max *= 2;
    if (options.debug) size_max *= 2;

    return size_max;
}

/// The maximum size, like above, but for any platform.
pub const multiversion_binary_size_max =
    config.process.multiversion_binary_platform_size_max * 2 * 2;
comptime {
    assert(multiversion_binary_platform_size_max(.{
        .macos = true,
        .debug = true,
    }) <= multiversion_binary_size_max);
}

pub const multiversion_poll_interval_ms = config.process.multiversion_poll_interval_ms;

comptime {
    assert(vsr_releases_max >= 2);
    assert(vsr_releases_max * @sizeOf(vsr.Release) <= message_body_size_max);
    // The number of releases is encoded into ping headers as a u16.
    assert(vsr_releases_max <= std.math.maxInt(u16));
}

/// The maximum number of nodes required to form a quorum for replication.
/// Majority quorums are only required across view change and replication phases (not within).
/// As per Flexible Paxos, provided `quorum_replication + quorum_view_change > replicas`:
/// 1. you may increase `quorum_view_change` above a majority, so that
/// 2. you can decrease `quorum_replication` below a majority, to optimize the common case.
/// This improves latency by reducing the number of nodes required for synchronous replication.
/// This reduces redundancy only in the short term, asynchronous replication will still continue.
/// The size of the replication quorum is limited to the minimum of this value and ⌈replicas/2⌉.
/// The size of the view change quorum will then be automatically inferred from quorum_replication.
pub const quorum_replication_max = config.cluster.quorum_replication_max;

/// The default server port to listen on if not specified in `--addresses`:
pub const port = config.process.port;

/// The default network interface address to listen on if not specified in `--addresses`:
/// WARNING: Binding to all interfaces with "0.0.0.0" is dangerous and opens the server to anyone.
/// Bind to the "127.0.0.1" loopback address to accept local connections as a safe default only.
pub const address = config.process.address;

comptime {
    // vsr.parse_address assumes that config.address/config.port are valid.
    _ = std.net.Address.parseIp4(address, 0) catch unreachable;
    _ = @as(u16, port);
}

/// The default maximum amount of memory to use.
pub const memory_size_max_default = config.process.memory_size_max_default;

/// At a high level, priority for object caching is (in descending order):
///
/// 1. Accounts.
///   - 2 lookups per created transfer
///   - high temporal locality
///   - positive expected result
/// 2. Posted transfers.
///   - high temporal locality
///   - positive expected result
/// 3. Transfers. Generally don't cache these because of:
///   - low temporal locality
///   - negative expected result
///
/// The default size of the accounts in-memory cache:
/// This impacts the amount of memory allocated at initialization by the server.
pub const cache_accounts_size_default = config.process.cache_accounts_size_default;

/// The default size of the transfers in-memory cache:
/// This impacts the amount of memory allocated at initialization by the server.
/// We allocate more capacity than the number of transfers for a safe hash table load factor.
pub const cache_transfers_size_default = config.process.cache_transfers_size_default;

/// The default size of the two-phase transfers in-memory cache:
/// This impacts the amount of memory allocated at initialization by the server.
pub const cache_transfers_pending_size_default =
    config.process.cache_transfers_pending_size_default;

/// The default size of historical balances in-memory cache:
/// This impacts the amount of memory allocated at initialization by the server.
pub const cache_account_balances_size_default = config.process.cache_account_balances_size_default;

/// The size of the client replies zone.
pub const client_replies_size = clients_max * message_size_max;

comptime {
    assert(client_replies_size > 0);
    assert(client_replies_size % sector_size == 0);
}

/// The maximum number of batch entries in the journal file:
/// A batch entry may contain many transfers, so this is not a limit on the number of transfers.
/// We need this limit to allocate space for copies of batch headers at the start of the journal.
/// These header copies enable us to disentangle corruption from crashes and recover accordingly.
pub const journal_slot_count = config.cluster.journal_slot_count;

/// The maximum size of the WAL zone:
/// This is pre-allocated and zeroed for performance when initialized.
/// Writes within this file never extend the filesystem inode size reducing the cost of fdatasync().
/// This enables static allocation of disk space so that appends cannot fail with ENOSPC.
/// This also enables us to detect filesystem inode corruption that would change the journal size.
pub const journal_size = journal_size_headers + journal_size_prepares;
pub const journal_size_headers = journal_slot_count * @sizeOf(vsr.Header);
pub const journal_size_prepares = journal_slot_count * message_size_max;

comptime {
    // For the given WAL (lsm_compaction_ops=4):
    //
    //   A    B    C    D    E
    //   |····|····|····|····|
    //
    // - ("|" delineates bars, where a bar is a multiple of prepare batches.)
    // - ("·" is a prepare in the WAL.)
    // - The Replica triggers a checkpoint at "E".
    // - The entries between "A" and "D" are on-disk in level 0.
    // - The entries between "D" and "E" are in-memory in the immutable table.
    // - So the checkpoint only includes "A…D".
    //
    // The journal must have at least two bars to ensure at least one is checkpointed.
    assert(journal_slot_count >= Config.Cluster.journal_slot_count_min);
    assert(journal_slot_count >= lsm_compaction_ops * 2);
    assert(journal_slot_count % lsm_compaction_ops == 0);
    // The journal must have at least two pipelines of messages to ensure that a new, fully-repaired
    // primary has enough headers for a complete SV message, even if the view-change just truncated
    // another pipeline of messages. (See op_repair_min()).
    assert(journal_slot_count >= pipeline_prepare_queue_max * 2);

    assert(journal_size == journal_size_headers + journal_size_prepares);
}

/// The maximum size of a message in bytes:
/// This is also the limit of all inflight data across multiple pipelined requests per connection.
/// We may have one request of up to 2 MiB inflight or 2 pipelined requests of up to 1 MiB inflight.
/// This impacts sequential disk write throughput, the larger the buffer the better.
/// 2 MiB is 16,384 transfers, and a reasonable choice for sequential disk write throughput.
/// However, this impacts bufferbloat and head-of-line blocking latency for pipelined requests.
/// For a 1 Gbps NIC = 125 MiB/s throughput: 2 MiB / 125 * 1000ms = 16ms for the next request.
/// This impacts the amount of memory allocated at initialization by the server.
pub const message_size_max = config.cluster.message_size_max;
pub const message_body_size_max = message_size_max - @sizeOf(vsr.Header);

comptime {
    // The WAL format requires messages to be a multiple of the sector size.
    assert(message_size_max % sector_size == 0);
    assert(message_size_max >= @sizeOf(vsr.Header));
    assert(message_size_max >= sector_size);
    assert(message_size_max >= Config.Cluster.message_size_max_min(clients_max));

    // Ensure that DVC/SV messages can fit all necessary headers.
    assert(message_body_size_max >= view_change_headers_max * @sizeOf(vsr.Header));

    assert(message_body_size_max >= @sizeOf(vsr.ReconfigurationRequest));
    assert(message_body_size_max >= @sizeOf(vsr.BlockRequest));
    assert(message_body_size_max >= @sizeOf(vsr.CheckpointState));
}

/// The maximum number of Viewstamped Replication prepare messages that can be inflight at a time.
/// This is immutable once assigned per cluster, as replicas need to know how many operations might
/// possibly be uncommitted during a view change, and this must be constant for all replicas.
pub const pipeline_prepare_queue_max: u32 = config.cluster.pipeline_prepare_queue_max;

/// The maximum number of Viewstamped Replication request messages that can be queued at a primary,
/// waiting to prepare.
pub const pipeline_request_queue_max: u32 = clients_max -| pipeline_prepare_queue_max;

comptime {
    // A prepare-queue capacity larger than clients_max is wasted.
    assert(pipeline_prepare_queue_max <= clients_max);
    // A total queue capacity larger than clients_max is wasted.
    assert(pipeline_prepare_queue_max + pipeline_request_queue_max <= clients_max);
    assert(pipeline_prepare_queue_max > 0);
    assert(pipeline_request_queue_max >= 0);

    // A DVC message uses the `header.context` (u128) field as a bitset to mark whether it has
    // prepared the corresponding header's message.
    assert(pipeline_prepare_queue_max + 1 <= @bitSizeOf(u128));
}

/// Maximum number of headers from the WAL suffix to include in an SV message.
/// Must at least cover the full pipeline.
/// Increasing this reduces likelihood that backups will need to repair their suffix's headers.
///
/// CRITICAL:
/// - We must provide enough headers to cover all uncommitted headers so that the new
///   primary (if we are in a view change) can decide whether to discard uncommitted headers
///   that cannot be repaired because they are gaps. See DVCQuorum for more detail.
/// - +1 to leave room for commit_max, in case a backup converts the SV to a DVC.
pub const view_change_headers_suffix_max = config.cluster.view_change_headers_suffix_max;

/// The number of prepare headers to include in the body of a DVC/SV.
///
/// start_view:
///
/// - We must include all uncommitted headers.
/// - +1 We must include the highest cluster-committed header (in case the SV is converted to a DVC
///   by the backup). (This is part of view_change_headers_suffix_max).
/// - +2: We must provide the header corresponding to each checkpoint-trigger in the intact
///   suffix of our journal.
///   - These help a lagging replica catch up when its `op < commit_max`.
///   - There are at most two of these in the journal.
///     (There are 2 immediately after we checkpoint, until we prepare enough to overwrite one).
///
/// do_view_change:
///
/// - We must include all uncommitted headers.
/// - +1 We must include the highest cluster-committed header, so that the new primary still has a
///   head op if it truncates the entire pipeline.
pub const view_change_headers_max = view_change_headers_suffix_max + 2;

comptime {
    assert(view_change_headers_suffix_max >= pipeline_prepare_queue_max + 1);

    assert(view_change_headers_max > 0);
    assert(view_change_headers_max >= pipeline_prepare_queue_max + 3);
    assert(view_change_headers_max <= journal_slot_count);
    assert(view_change_headers_max <= @divFloor(
        message_body_size_max - @sizeOf(vsr.CheckpointState),
        @sizeOf(vsr.Header),
    ));
    assert(view_change_headers_max > view_change_headers_suffix_max);
}

/// The maximum number of headers to include with a response to a command=request_headers message.
pub const request_headers_max = @min(
    @divFloor(message_body_size_max, @sizeOf(vsr.Header)),
    64,
);

comptime {
    assert(request_headers_max > 0);
}

/// The maximum number of block addresses/checksums requested by a single command=request_blocks.
pub const grid_repair_request_max = config.process.grid_repair_request_max;

/// The number of grid reads allocated to handle incoming command=request_blocks messages.
pub const grid_repair_reads_max = config.process.grid_repair_reads_max;

/// Immediately after state sync we want access to all of the grid's write bandwidth to rapidly sync
/// table blocks.
pub const grid_repair_writes_max = grid_iops_write_max;

/// The default sizing of the grid cache. It's expected for operators to override this on the CLI.
pub const grid_cache_size_default = config.process.grid_cache_size_default;

/// The maximum capacity (in *single* blocks – not counting syncing tables) of the
/// GridBlocksMissing.
///
/// As this increases:
/// - GridBlocksMissing allocates more memory.
/// - The "period" of GridBlocksMissing's requests increases.
///   This makes the repair protocol more tolerant of network latency.
/// - (Repair protocol is used to repair manifest log blocks immediately after state sync).
pub const grid_missing_blocks_max = config.process.grid_missing_blocks_max;

/// The number of tables that can be synced simultaneously.
/// "Table" in this context is the number of table index blocks to hold in memory while syncing
/// their content.
///
/// As this increases:
/// - GridBlocksMissing allocates more memory (~2 blocks for each).
/// - Syncing is more efficient, as more blocks can be fetched concurrently.
pub const grid_missing_tables_max = config.process.grid_missing_tables_max;

comptime {
    assert(grid_repair_request_max > 0);
    assert(grid_repair_request_max <= @divFloor(message_body_size_max, @sizeOf(vsr.BlockRequest)));
    assert(grid_repair_request_max <= grid_repair_reads_max);

    assert(grid_repair_reads_max > 0);
    assert(grid_repair_writes_max > 0);
    assert(grid_repair_writes_max <=
        grid_missing_blocks_max + grid_missing_tables_max * lsm_table_data_blocks_max);

    assert(grid_missing_blocks_max > 0);
    assert(grid_missing_tables_max > 0);
}

/// The maximum number of concurrent scrubber reads.
///
/// Unless the scrubber cycle is extremely short and the data file very large there is no need to
/// set this higher than 1.
pub const grid_scrubber_reads_max = config.process.grid_scrubber_reads_max;

/// `grid_scrubber_cycle_ms` is the (approximate, target) total milliseconds per scrub of each
/// replica's entire grid. Scrubbing work is spread evenly across this duration.
///
/// Napkin math for the "worst case" scrubber read overhead as a function of cycle duration
/// (assuming a fully-loaded data file – maximum size and 100% acquired):
///
///   storage_size_limit          = 64TiB
///   grid_scrubber_cycle_seconds = 180 days * 24 hr/day * 60 min/hr * 60 s/min (2 cycle/year)
///   read_bytes_per_second       = storage_size_limit / grid_scrubber_cycle_seconds ≈ 4.32 MiB/s
///
pub const grid_scrubber_cycle_ticks = config.process.grid_scrubber_cycle_ms / tick_ms;

/// Accelerate/throttle scrubber reads if they are less/more frequent than this range.
/// (This is to keep the timeouts from being too extreme when the grid is tiny or huge.)
pub const grid_scrubber_interval_ticks_min = config.process.grid_scrubber_interval_ms_min / tick_ms;
pub const grid_scrubber_interval_ticks_max = config.process.grid_scrubber_interval_ms_max / tick_ms;

comptime {
    assert(grid_scrubber_reads_max > 0);
    assert(grid_scrubber_reads_max <= grid_iops_read_max);
    assert(grid_scrubber_cycle_ticks > 0);
    assert(grid_scrubber_cycle_ticks > @divFloor(std.time.ms_per_min, tick_ms)); // Sanity-check.
    assert(grid_scrubber_interval_ticks_min > 0);
    assert(grid_scrubber_interval_ticks_min <= grid_scrubber_interval_ticks_max);
    assert(grid_scrubber_interval_ticks_max > 0);
}

/// The minimum and maximum amount of time in milliseconds to wait before initiating a connection.
/// Exponential backoff and jitter are applied within this range.
pub const connection_delay_min_ms = config.process.connection_delay_min_ms;
pub const connection_delay_max_ms = config.process.connection_delay_max_ms;

/// The maximum number of outgoing messages that may be queued on a replica connection.
pub const connection_send_queue_max_replica = @max(@min(clients_max, 4), 2);

/// The maximum number of outgoing messages that may be queued on a client connection.
/// The client has one in-flight request, and occasionally a ping.
pub const connection_send_queue_max_client = 2;

/// The maximum number of outgoing requests that may be queued on a client (including the in-flight
/// request).
pub const client_request_queue_max = config.process.client_request_queue_max;

/// The maximum number of connections in the kernel's complete connection queue pending an accept():
/// If the backlog argument is greater than the value in `/proc/sys/net/core/somaxconn`, then it is
/// silently truncated to that value. Since Linux 5.4, the default in this file is 4096.
pub const tcp_backlog = config.process.tcp_backlog;

/// The maximum size of a kernel socket receive buffer in bytes (or 0 to use the system default):
/// This sets SO_RCVBUF as an alternative to the auto-tuning range in /proc/sys/net/ipv4/tcp_rmem.
/// The value is limited by /proc/sys/net/core/rmem_max, unless the CAP_NET_ADMIN privilege exists.
/// The kernel doubles this value to allow space for packet bookkeeping overhead.
/// The receive buffer should ideally exceed the Bandwidth-Delay Product for maximum throughput.
/// At the same time, be careful going beyond 4 MiB as the kernel may merge many small TCP packets,
/// causing considerable latency spikes for large buffer sizes:
/// https://blog.cloudflare.com/the-story-of-one-latency-spike/
pub const tcp_rcvbuf = config.process.tcp_rcvbuf;

/// The maximum size of a kernel socket send buffer in bytes (or 0 to use the system default):
/// This sets SO_SNDBUF as an alternative to the auto-tuning range in /proc/sys/net/ipv4/tcp_wmem.
/// The value is limited by /proc/sys/net/core/wmem_max, unless the CAP_NET_ADMIN privilege exists.
/// The kernel doubles this value to allow space for packet bookkeeping overhead.
pub const tcp_sndbuf_replica = connection_send_queue_max_replica * message_size_max;
pub const tcp_sndbuf_client = connection_send_queue_max_client * message_size_max;

comptime {
    // Avoid latency issues from setting sndbuf too high:
    assert(tcp_sndbuf_replica <= 16 * 1024 * 1024);
    assert(tcp_sndbuf_client <= 16 * 1024 * 1024);
}

/// Whether to enable TCP keepalive:
pub const tcp_keepalive = config.process.tcp_keepalive;

/// The time (in seconds) the connection needs to be idle before sending TCP keepalive probes:
/// Probes are not sent when the send buffer has data or the congestion window size is zero,
/// for these cases we also need tcp_user_timeout_ms below.
pub const tcp_keepidle = config.process.tcp_keepidle;

/// The time (in seconds) between individual keepalive probes:
pub const tcp_keepintvl = config.process.tcp_keepintvl;

/// The maximum number of keepalive probes to send before dropping the connection:
pub const tcp_keepcnt = config.process.tcp_keepcnt;

/// The time (in milliseconds) to timeout an idle connection or unacknowledged send:
/// This timer rides on the granularity of the keepalive or retransmission timers.
/// For example, if keepalive will only send a probe after 10s then this becomes the lower bound
/// for tcp_user_timeout_ms to fire, even if tcp_user_timeout_ms is 2s. Nevertheless, this would
/// timeout the connection at 10s rather than wait for tcp_keepcnt probes to be sent. At the same
/// time, if tcp_user_timeout_ms is larger than the max keepalive time then tcp_keepcnt will be
/// ignored and more keepalive probes will be sent until tcp_user_timeout_ms fires.
/// For a thorough overview of how these settings interact:
/// https://blog.cloudflare.com/when-tcp-sockets-refuse-to-die/
pub const tcp_user_timeout_ms = (tcp_keepidle + tcp_keepintvl * tcp_keepcnt) * 1000;

/// Whether to disable Nagle's algorithm to eliminate send buffering delays:
pub const tcp_nodelay = config.process.tcp_nodelay;

/// Size of a CPU cache line in bytes
pub const cache_line_size = config.cluster.cache_line_size;

/// The minimum size of an aligned kernel page and an Advanced Format disk sector:
/// This is necessary for direct I/O without the kernel having to fix unaligned pages with a copy.
/// The new Advanced Format sector size is backwards compatible with the old 512 byte sector size.
/// This should therefore never be less than 4 KiB to be future-proof when server disks are swapped.
pub const sector_size = 4096;

/// Whether to perform direct I/O to the underlying disk device:
/// This enables several performance optimizations:
/// * A memory copy to the kernel's page cache can be eliminated for reduced CPU utilization.
/// * I/O can be issued immediately to the disk device without buffering delay for improved latency.
/// This also enables several safety features:
/// * Disk data can be scrubbed to repair latent sector errors and checksum errors proactively.
/// * Fsync failures can be recovered from correctly.
/// WARNING: Disabling direct I/O is unsafe; the page cache cannot be trusted after an fsync error,
/// even after an application panic, since the kernel will mark dirty pages as clean, even
/// when they were never written to disk.
pub const direct_io = config.process.direct_io;

// TODO Add in the upper-bound that the Superblock will use.
pub const iops_read_max = journal_iops_read_max + client_replies_iops_read_max + grid_iops_read_max;
pub const iops_write_max = journal_iops_write_max + client_replies_iops_write_max +
    grid_iops_write_max;

/// The maximum number of concurrent WAL read I/O operations to allow at once.
pub const journal_iops_read_max = config.process.journal_iops_read_max;
/// The maximum number of concurrent WAL write I/O operations to allow at once.
/// Ideally this is at least as high as pipeline_prepare_queue_max, but it is safe to be lower.
pub const journal_iops_write_max = config.process.journal_iops_write_max;

/// The maximum number of concurrent reads to the client-replies zone.
/// Client replies are read when the client misses their original reply and retries a request.
pub const client_replies_iops_read_max = config.process.client_replies_iops_read_max;
/// The maximum number of concurrent writes to the client-replies zone.
/// Client replies are written after every commit.
pub const client_replies_iops_write_max = config.process.client_replies_iops_write_max;
/// The amount of time (in milliseconds) within which a client must receive a response from the
/// cluster, after which it emits a warning log (for alerting/metrics).
pub const client_request_completion_warn_ms = config.process.client_request_completion_warn_ms;

/// The maximum number of concurrent grid read I/O operations to allow at once.
pub const grid_iops_read_max = config.process.grid_iops_read_max;
/// The maximum number of concurrent grid write I/O operations to allow at once.
pub const grid_iops_write_max = config.process.grid_iops_write_max;

comptime {
    assert(journal_iops_read_max > 0);
    assert(journal_iops_write_max > 0);
    assert(client_replies_iops_read_max > 0);
    assert(client_replies_iops_write_max > 0);
    assert(client_replies_iops_write_max <= clients_max);
    assert(grid_iops_read_max > 0);
    assert(grid_iops_write_max > 0);
}

/// The number of redundant copies of the superblock in the superblock storage zone.
/// This must be either { 4, 6, 8 }, i.e. an even number, for more efficient flexible quorums.
///
/// The superblock contains local state for the replica and therefore cannot be replicated remotely.
/// Loss of the superblock would represent loss of the replica and so it must be protected.
///
/// This can mean checkpointing latencies in the rare extreme worst-case of at most 264ms, although
/// this would require EWAH compression of our block free set to have zero effective compression.
/// In practice, checkpointing latency should be an order of magnitude better due to compression,
/// because our block free set will fill holes when allocating.
///
/// The superblock only needs to be checkpointed every now and then, before the WAL wraps around,
/// or when a view change needs to take place to elect a new primary.
pub const superblock_copies = config.cluster.superblock_copies;

comptime {
    assert(superblock_copies % 2 == 0);
    assert(superblock_copies >= 4);
    assert(superblock_copies <= 8);
}

/// The default maximum size of a local data file. This can be override, up to
/// storage_size_limit_max, by a CLI flag.
pub const storage_size_limit_default = config.process.storage_size_limit_default;

/// The maximum size of a local data file.
/// This should not be much larger than several TiB to limit:
/// * blast radius and recovery time when a whole replica is lost,
/// * replicated storage overhead, since all data files are mirrored, and
/// * the static memory allocation required for tracking LSM forest metadata in memory.
///
/// This is a "firm" limit --- while it is a compile-time constant, it does not affect data file
/// layout and can be safely changed for an existing cluster.
pub const storage_size_limit_max = config.process.storage_size_limit_max;

comptime {
    assert(storage_size_limit_max >= storage_size_limit_default);
}

/// The unit of read/write access to LSM manifest and LSM table blocks in the block storage zone.
///
/// - A lower block size increases the memory overhead of table metadata, due to smaller/more
///   tables.
/// - A higher block size increases space amplification due to partially-filled blocks.
pub const block_size = config.cluster.block_size;

comptime {
    assert(block_size % sector_size == 0);
    assert(block_size > @sizeOf(vsr.Header));
    // Blocks are sent over the network as messages during grid repair and state sync.
    assert(block_size <= message_size_max);
}

/// The number of levels in an LSM tree.
/// A higher number of levels increases read amplification, as well as total storage capacity.
pub const lsm_levels = config.cluster.lsm_levels;

comptime {
    // ManifestLog serializes the level as a u6.
    assert(lsm_levels > 0);
    assert(lsm_levels <= std.math.maxInt(u6));
}

/// The number of tables at level i (0 ≤ i < lsm_levels) is `pow(lsm_growth_factor, i+1)`.
/// A higher growth factor increases write amplification (by increasing the number of tables in
/// level B that overlap a table in level A in a compaction), but decreases read amplification (by
/// reducing the height of the tree and thus the number of levels that must be probed). Since read
/// amplification can be optimized more easily (with caching), we target a growth
/// factor of 8 for lower write amplification rather than the more typical growth factor of 10.
pub const lsm_growth_factor = config.cluster.lsm_growth_factor;

comptime {
    assert(lsm_growth_factor > 1);
}

/// Size of nodes used by the LSM tree manifest implementation.
/// TODO Double-check this with our "LSM Manifest" spreadsheet.
pub const lsm_manifest_node_size = config.process.lsm_manifest_node_size;

/// The number of manifest blocks to compact *beyond the minimum*, per half-bar.
///
/// In the worst case, we still compact entries faster than we produce them (by a margin of
/// "extra" blocks). This is necessary to ensure that the manifest has a bounded number of entries.
/// (Or in other words, that Pace's recurrence relation converges.)
///
/// This specific choice of value is somewhat arbitrary, but yields a decent balance between
/// "compaction work performed" and "total manifest size".
///
/// As this value increases, the manifest must perform more compaction work, but the manifest
/// upper-bound shrinks (and therefore manifest recovery time decreases).
///
/// See ManifestLog.Pace for more detail.
pub const lsm_manifest_compact_extra_blocks = config.cluster.lsm_manifest_compact_extra_blocks;

comptime {
    assert(lsm_manifest_compact_extra_blocks > 0);
}

/// Number of prepares accumulated in the in-memory table before flushing to disk.
///
/// This is a batch of batches. Each prepare can contain at most 8_190 transfers. With
/// lsm_compaction_ops=32, 32 prepares are processed to fill the in-memory table with 262_080
/// transfers. During processing of the next 32 prepares, this in-memory table is flushed to disk.
/// Simultaneously, compaction is run to free up enough space to flush the in-memory table from the
/// next batch of lsm_compaction_ops prepares.
///
/// Together with message_body_size_max, lsm_compaction_ops determines the size a table on disk.
pub const lsm_compaction_ops = config.cluster.lsm_compaction_ops;

comptime {
    // The LSM tree uses half-measures to balance compaction.
    assert(lsm_compaction_ops % 2 == 0);
}

// Limits for the number of value blocks that a single compaction can queue up for IO and for the
// number of IO operations themselves. The number of index blocks is always one per level.
// This is a comptime upper bound. The actual number of concurrency is also limited by the
// runtime-known number of free blocks.
//
// For simplicity for now, size IOPS to always be available.
pub const lsm_compaction_queue_read_max = 8;
pub const lsm_compaction_queue_write_max = 8;
pub const lsm_compaction_iops_read_max = lsm_compaction_queue_read_max + 2; // + two index blocks.
pub const lsm_compaction_iops_write_max = lsm_compaction_queue_write_max + 1; // + one index block.

pub const lsm_snapshots_max = config.cluster.lsm_snapshots_max;

/// The maximum number of blocks that can possibly be referenced by any table index block.
///
/// - This is a very conservative (upper-bound) calculation that doesn't rely on the StateMachine's
///   tree configuration. (To prevent Grid from depending on StateMachine).
/// - This counts data blocks, but does not count the index block itself.
pub const lsm_table_data_blocks_max = table_blocks_max: {
    const checksum_size = @sizeOf(u256);
    const address_size = @sizeOf(u64);
    break :table_blocks_max @divFloor(
        block_size - @sizeOf(vsr.Header),
        (checksum_size + address_size),
    );
};

/// The default size in bytes of the NodePool used for the LSM forest's manifests.
pub const lsm_manifest_memory_size_default = lsm_manifest_memory: {
    // TODO Tune this better.
    const lsm_forest_node_count: u32 = 8192;
    break :lsm_manifest_memory lsm_forest_node_count * lsm_manifest_node_size;
};

/// The maximum size in bytes of the NodePool used for the LSM forest's manifests.
pub const lsm_manifest_memory_size_max =
    @divFloor(std.math.maxInt(u32), lsm_manifest_memory_size_multiplier) *
    lsm_manifest_memory_size_multiplier;

/// The minimum size in bytes of the NodePool used for the LSM forest's manifests.
pub const lsm_manifest_memory_size_min = lsm_manifest_memory_size_multiplier;

/// The lsm memory size must be a multiple of this value.
///
/// While technically this could be equal to lsm_manifest_node_size, we set it
/// to 1MiB so it is a more obvious increment for users.
pub const lsm_manifest_memory_size_multiplier = lsm_manifest_memory_multiplier: {
    const lsm_manifest_memory_multiplier = 64 * lsm_manifest_node_size;
    assert(lsm_manifest_memory_multiplier == 1024 * 1024);
    break :lsm_manifest_memory_multiplier lsm_manifest_memory_multiplier;
};

/// The LSM will attempt to coalesce a table if it is less full than this threshold.
pub const lsm_table_coalescing_threshold_percent =
    config.cluster.lsm_table_coalescing_threshold_percent;

comptime {
    assert(lsm_table_coalescing_threshold_percent > 0); // Ensure that coalescing is possible.
    assert(lsm_table_coalescing_threshold_percent < 100); // Don't coalesce full tables.
}

/// The number of milliseconds between each replica tick, the basic unit of time in TigerBeetle.
/// Used to regulate heartbeats, retries and timeouts, all specified as multiples of a tick.
pub const tick_ms = config.process.tick_ms;

/// The conservative round-trip time at startup when there is no network knowledge.
/// Adjusted dynamically thereafter for RTT-sensitive timeouts according to network congestion.
/// This should be set higher rather than lower to avoid flooding the network at startup.
pub const rtt_ticks = config.process.rtt_ms / tick_ms;

/// Maximum RTT, to prevent too-long timeouts.
pub const rtt_max_ticks = config.process.rtt_max_ms / tick_ms;

/// The multiple of round-trip time for RTT-sensitive timeouts.
pub const rtt_multiple = 2;

/// The min/max bounds of exponential backoff (and jitter) to add to RTT-sensitive timeouts.
pub const backoff_min_ticks = config.process.backoff_min_ms / tick_ms;
pub const backoff_max_ticks = config.process.backoff_max_ms / tick_ms;

/// The maximum skew between two clocks to allow when considering them to be in agreement.
/// The principle is that no two clocks tick exactly alike but some clocks more or less agree.
/// The maximum skew across the cluster as a whole is this value times the total number of clocks.
/// The cluster will be unavailable if the majority of clocks are all further than this value apart.
/// Decreasing this reduces the probability of reaching agreement on synchronized time.
/// Increasing this reduces the accuracy of synchronized time.
pub const clock_offset_tolerance_max_ms = config.process.clock_offset_tolerance_max_ms;

/// The amount of time before the clock's synchronized epoch is expired.
/// If the epoch is expired before it can be replaced with a new synchronized epoch, then this most
/// likely indicates either a network partition or else too many clock faults across the cluster.
/// A new synchronized epoch will be installed as soon as these conditions resolve.
pub const clock_epoch_max_ms = config.process.clock_epoch_max_ms;

/// The amount of time to wait for enough accurate samples before synchronizing the clock.
/// The more samples we can take per remote clock source, the more accurate our estimation becomes.
/// This impacts cluster startup time as the primary must first wait for synchronization to
/// complete.
pub const clock_synchronization_window_min_ms = config.process.clock_synchronization_window_min_ms;

/// The amount of time without agreement before the clock window is expired and a new window opened.
/// This happens where some samples have been collected but not enough to reach agreement.
/// The quality of samples degrades as they age so at some point we throw them away and start over.
/// This eliminates the impact of gradual clock drift on our clock offset (clock skew) measurements.
/// If a window expires because of this then it is likely that the clock epoch will also be expired.
pub const clock_synchronization_window_max_ms = config.process.clock_synchronization_window_max_ms;

pub const StateMachineConfig = struct {
    release: vsr.Release,
    message_body_size_max: comptime_int,
    lsm_compaction_ops: comptime_int,
    vsr_operations_reserved: u8,
};

pub const state_machine_config = StateMachineConfig{
    .release = config.process.release,
    .message_body_size_max = message_body_size_max,
    .lsm_compaction_ops = lsm_compaction_ops,
    .vsr_operations_reserved = vsr_operations_reserved,
};

/// TigerBeetle uses asserts proactively, unless they severely degrade performance. For production,
/// 5% slow down might be deemed critical, tests tolerate slowdowns up to 5x. Tests should be
/// reasonably fast to make deterministic simulation effective. `constants.verify` disambiguate the
/// two cases.
///
/// In the control plane (eg, vsr proper) assert unconditionally. Due to batching, control plane
/// overhead is negligible. It is acceptable to spend O(N) time to verify O(1) computation.
///
/// In the data plane (eg, lsm tree), finer grained judgement is required. Do an unconditional O(1)
/// assert before an O(N) loop (e.g, a bounds check). Inside the loop, it might or might not be
/// feasible to add an extra assert per iteration. In the latter case, guard the assert with `if
/// (constants.verify)`, but prefer an unconditional assert unless benchmarks prove it to be costly.
///
/// In the data plane, never use O(N) asserts for O(1) computations --- due to do randomized testing
/// the overall coverage is proportional to the number of tests run. Slow thorough assertions
/// decrease the overall test coverage.
///
/// Specific data structures might use a comptime parameter, to enable extra costly verification
/// only during unit tests of the data structure.
pub const verify = config.process.verify;

/// Place us in a special recovery state, where we accept timestamps passed in to us. Used to
/// replay our AOF.
pub const aof_recovery = config.process.aof_recovery;

/// The maximum number of bytes to use for compaction blocks.
pub const compaction_block_memory_size_max = std.math.maxInt(u32) * block_size;

/// Maximum number of tree scans that can be performed by a single query.
/// NOTE: Each condition in a query is a scan, for example `WHERE a=0 AND b=1` needs 2 scans.
pub const lsm_scans_max = config.cluster.lsm_scans_max;

/// Processing more than this amount of messages in a single event loop turn issues a warning.
pub const bus_message_burst_warn_min = 8;
