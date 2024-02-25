const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;
const maybe = stdx.maybe;
const SourceLocation = std.builtin.SourceLocation;

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");

const StaticAllocator = @import("../static_allocator.zig");
const allocate_block = @import("grid.zig").allocate_block;
const GridType = @import("grid.zig").GridType;
const BlockPtr = @import("grid.zig").BlockPtr;
const IOPS = @import("../iops.zig").IOPS;
const MessagePool = @import("../message_pool.zig").MessagePool;
const Message = @import("../message_pool.zig").MessagePool.Message;
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const ForestTableIteratorType =
    @import("../lsm/forest_table_iterator.zig").ForestTableIteratorType;
const TestStorage = @import("../testing/storage.zig").Storage;

const vsr = @import("../vsr.zig");
const Header = vsr.Header;
const Timeout = vsr.Timeout;
const Command = vsr.Command;
const Version = vsr.Version;
const VSRState = vsr.VSRState;
const SyncStage = vsr.SyncStage;
const SyncTarget = vsr.SyncTarget;
const ClientSessions = vsr.ClientSessions;
const Snapshot = @import("../lsm/schema.zig").Snapshot;
const log = stdx.log.scoped(.replica);
const tracer = @import("../tracer.zig");

pub const Status = enum {
    normal,
    view_change,
    /// Replicas start with `.recovering` status. Normally, replica immediately
    /// transitions to a different status. The exception is a single-node cluster,
    /// where the replica stays in `.recovering` state until it commits all entries
    /// from its journal.
    recovering,
    /// Replica transitions from `.recovering` to `.recovering_head` at startup
    /// if it finds its persistent state corrupted. In this case, replica can
    /// not participate in consensus, as it might have forgotten some of the
    /// messages it has sent or received before. Instead, it waits for a SV
    /// message to get into a consistent state.
    recovering_head,
};

const CommitStage = union(enum) {
    /// Not committing.
    idle,
    /// About to start committing.
    next,
    next_journal,
    next_pipeline,
    prefetch_state_machine,
    /// Ensure that the ClientReplies has at least one Write available.
    setup_client_replies,
    compact_state_machine,
    checkpoint_data: CheckpointDataProgress,
    checkpoint_superblock,
    /// A commit just finished. Clean up before proceeding to the next.
    cleanup,
};

const CheckpointData = enum {
    state_machine,
    client_replies,
    client_sessions,
    grid,
};

const CheckpointDataProgress = std.enums.EnumSet(CheckpointData);

pub const ReplicaEvent = union(enum) {
    message_sent: *const Message,
    state_machine_opened,
    /// Called immediately after a prepare is committed by the state machine.
    committed,
    /// Called immediately after a compaction.
    compaction_completed,
    /// Called immediately before a checkpoint.
    checkpoint_commenced,
    /// Called immediately after a checkpoint.
    /// Note: The replica may checkpoint without calling this function:
    /// 1. Begin checkpoint.
    /// 2. Write 2/4 SuperBlock copies.
    /// 3. Crash.
    /// 4. Recover in the new checkpoint (but op_checkpoint wasn't called).
    checkpoint_completed,
    sync_stage_changed,
};

const Nonce = u128;

const Prepare = struct {
    /// The current prepare message (used to cross-check prepare_ok messages, and for resending).
    message: *Message.Prepare,

    /// Unique prepare_ok messages for the same view, op number and checksum from ALL replicas.
    ok_from_all_replicas: QuorumCounter = quorum_counter_null,

    /// Whether a quorum of prepare_ok messages has been received for this prepare.
    ok_quorum_received: bool = false,
};

const Request = struct {
    message: *Message.Request,
    realtime: i64,
};

const DVCQuorumMessages = [constants.replicas_max]?*Message.DoViewChange;
const dvc_quorum_messages_null = [_]?*Message.DoViewChange{null} ** constants.replicas_max;

const QuorumCounter = std.StaticBitSet(constants.replicas_max);
const quorum_counter_null = QuorumCounter.initEmpty();

pub fn ReplicaType(
    comptime StateMachine: type,
    comptime MessageBus: type,
    comptime Storage: type,
    comptime Time: type,
    comptime AOF: type,
) type {
    const Grid = GridType(Storage);

    return struct {
        const Self = @This();

        pub const SuperBlock = vsr.SuperBlockType(Storage);
        const CheckpointTrailer = vsr.CheckpointTrailerType(Storage);
        const Journal = vsr.JournalType(Self, Storage);
        const ClientReplies = vsr.ClientRepliesType(Storage);
        const Clock = vsr.ClockType(Time);
        const ForestTableIterator = ForestTableIteratorType(StateMachine.Forest);

        const BlockRead = struct {
            read: Grid.Read,
            replica: *Self,
            destination: u8,
            message: *Message.Block,
        };

        const BlockWrite = struct {
            write: Grid.Write = undefined,
            replica: *Self,
        };

        const RepairTable = struct {
            replica: *Self,
            table: Grid.RepairTable,
        };

        /// We use this allocator during open/init and then disable it.
        /// An accidental dynamic allocation after open/init will cause an assertion failure.
        static_allocator: StaticAllocator,

        /// The number of the cluster to which this replica belongs:
        cluster: u128,

        /// The number of replicas in the cluster:
        replica_count: u8,

        /// The number of standbys in the cluster.
        standby_count: u8,

        /// Total amount of nodes (replicas and standbys) in the cluster.
        ///
        /// Invariant: node_count = replica_count + standby_count
        node_count: u8,

        /// The index of this replica's address in the configuration array held by the MessageBus.
        /// If replica >= replica_count, this is a standby.
        ///
        /// Invariant: replica < node_count
        replica: u8,

        /// The minimum number of replicas required to form a replication quorum:
        quorum_replication: u8,

        /// The minimum number of replicas required to form a view change quorum:
        quorum_view_change: u8,

        /// The minimum number of replicas required to nack an uncommitted pipeline prepare
        /// header/message.
        quorum_nack_prepare: u8,

        /// More than half of replica_count.
        quorum_majority: u8,

        /// A globally unique integer generated by a crypto rng during replica process startup.
        /// Presently, it is used to detect outdated start view messages in recovering head status.
        nonce: Nonce,

        time: Time,

        /// A distributed fault-tolerant clock for lower and upper bounds on the primary's wall clock:
        clock: Clock,

        /// The persistent log of hash-chained journal entries:
        journal: Journal,

        /// ClientSessions records for each client the latest session and the latest committed reply.
        /// This is modified between checkpoints, and is persisted on checkpoint and sync.
        client_sessions: ClientSessions,

        client_sessions_checkpoint: CheckpointTrailer,

        /// The persistent log of the latest reply per active client.
        client_replies: ClientReplies,

        /// An abstraction to send messages from the replica to another replica or client.
        /// The message bus will also deliver messages to this replica by calling `on_message_from_bus()`.
        message_bus: MessageBus,

        /// For executing service up-calls after an operation has been committed:
        state_machine: StateMachine,

        /// Set to true once StateMachine.open() completes.
        /// When false, the replica must not commit/compact/checkpoint.
        state_machine_opened: bool = false,

        /// Durably store VSR state, the "root" of the LSM tree, and other replica metadata.
        superblock: SuperBlock,

        /// Context for SuperBlock.open(), .checkpoint(), and .sync().
        superblock_context: SuperBlock.Context = undefined,
        /// Context for SuperBlock.view_change().
        superblock_context_view_change: SuperBlock.Context = undefined,

        grid: Grid,
        grid_reads: IOPS(BlockRead, constants.grid_repair_reads_max) = .{},
        grid_repair_tables: IOPS(RepairTable, constants.grid_missing_tables_max) = .{},
        grid_repair_writes: IOPS(BlockWrite, constants.grid_repair_writes_max) = .{},
        grid_repair_write_blocks: [constants.grid_repair_writes_max]BlockPtr,

        opened: bool,

        syncing: SyncStage = .idle,
        /// The latest discovered checkpoint that would be safe to sync to.
        /// Kept up-to-date during every status, while syncing or healthy.
        sync_target_max: ?SyncTarget = null,
        /// Invariants:
        /// - If syncing≠idle then sync_tables=null.
        sync_tables: ?ForestTableIterator = null,

        /// The current view.
        /// Initialized from the superblock's VSRState.
        ///
        /// Invariants:
        /// * `replica.view = replica.log_view` when status=normal
        /// * `replica.view ≥ replica.log_view`
        /// * `replica.view ≥ replica.view_durable`
        /// * `replica.view = 0` when replica_count=1.
        view: u32,

        /// The latest view where
        /// - the replica was a primary and acquired a DVC quorum, or
        /// - the replica was a backup and processed a SV message.
        /// i.e. the latest view in which this replica changed its head message.
        ///
        /// Initialized from the superblock's VSRState.
        ///
        /// Invariants (see `view` for others):
        /// * `replica.log_view ≥ replica.log_view_durable`
        /// * `replica.log_view = 0` when replica_count=1.
        log_view: u32,

        /// The current status, either normal, view_change, or recovering:
        status: Status = .recovering,

        /// The op number assigned to the most recently prepared operation.
        /// This op is sometimes referred to as the replica's "head" or "head op".
        ///
        /// Invariants (not applicable during status=recovering|recovering_head):
        /// * `replica.op` exists in the Journal.
        /// * `replica.op ≥ replica.op_checkpoint`.
        /// * `replica.op ≥ replica.commit_min`.
        /// * `replica.op - replica.commit_min    ≤ journal_slot_count`
        /// * `replica.op - replica.op_checkpoint ≤ journal_slot_count`
        ///   It is safe to overwrite `op_checkpoint` itself.
        /// * `replica.op ≤ replica.op_prepare_max`:
        ///   Don't wrap the WAL until we are sure that the overwritten entry will not be required
        ///   for recovery.
        op: u64,

        /// The op number of the latest committed and executed operation (according to the replica).
        /// The replica may have to wait for repairs to complete before commit_min reaches commit_max.
        ///
        /// Invariants (not applicable during status=recovering):
        /// * `replica.commit_min` exists in the Journal OR `replica.commit_min == op_checkpoint`.
        /// * `replica.commit_min ≤ replica.op`
        /// * `replica.commit_min ≥ replica.op_checkpoint`.
        /// * never decreases while the replica is alive
        commit_min: u64,

        /// The op number of the latest committed operation (according to the cluster).
        /// This is the commit number in terms of the VRR paper.
        ///
        /// - When syncing=idle and status≠recovering_head,
        ///   this is the latest commit *within our view*.
        /// - When syncing≠idle or status=recovering_head,
        ///   this is max(latest commit within our view, sync_target.op).
        ///
        /// Invariants:
        /// * `replica.commit_max ≥ replica.commit_min`.
        /// * `replica.commit_max ≥ replica.op -| constants.pipeline_prepare_queue_max`.
        /// * never decreases.
        /// Invariants (status=normal primary):
        /// * `replica.commit_max = replica.commit_min`.
        /// * `replica.commit_max = replica.op - pipeline.queue.prepare_queue.count`.
        commit_max: u64,

        /// Guards against concurrent commits, and tracks the commit progress.
        commit_stage: CommitStage = .idle,

        /// Whether we are reading a prepare from storage to construct the pipeline.
        pipeline_repairing: bool = false,

        /// The pipeline is a queue for a replica which is the primary and in status=normal.
        /// At all other times the pipeline is a cache.
        pipeline: union(enum) {
            /// The primary's pipeline of inflight prepares waiting to commit in FIFO order,
            /// with a tail of pending requests which have not begun to prepare.
            /// This allows us to pipeline without the complexity of out-of-order commits.
            queue: PipelineQueue,
            /// Prepares in the cache may be committed or uncommitted, and may not belong to the
            /// current view.
            cache: PipelineCache,
        },

        /// When "log_view < view": The DVC headers.
        /// When "log_view = view": The SV headers. (Just as a cache,
        //  since they are regenerated for every request_start_view).
        ///
        /// Invariants:
        /// - view_headers.len     > 0
        /// - view_headers[0].view ≤ self.log_view
        view_headers: vsr.Headers.ViewChangeArray,

        /// In some cases, a replica may send a message to itself. We do not submit these messages
        /// to the message bus but rather queue them here for guaranteed immediate delivery, which
        /// we require and assert in our protocol implementation.
        loopback_queue: ?*Message = null,

        /// The last timestamp received on a commit heartbeat.
        /// The timestamp originates from the primary's monotonic clock. It is used to discard
        /// delayed or duplicate heartbeat messages.
        /// (status=normal backup)
        heartbeat_timestamp: u64 = 0,

        /// While set, don't send commit heartbeats.
        /// Used when the primary believes that it is partitioned and needs to step down.
        /// In particular, guards against a deadlock in the case where small messages (e.g.
        /// heartbeats, pings/pongs) succeed, but large messages (e.g. prepares) fail.
        /// (status=normal primary, pipeline has prepare with !ok_quorum_received)
        primary_abdicating: bool = false,

        /// Unique start_view_change messages for the same view from ALL replicas (including ourself).
        start_view_change_from_all_replicas: QuorumCounter = quorum_counter_null,

        /// Unique do_view_change messages for the same view from ALL replicas (including ourself).
        do_view_change_from_all_replicas: DVCQuorumMessages = dvc_quorum_messages_null,

        /// Whether the primary has received a quorum of do_view_change messages for the view change:
        /// Determines whether the primary may effect repairs according to the CTRL protocol.
        do_view_change_quorum: bool = false,

        /// The number of ticks before a primary or backup broadcasts a ping to other replicas.
        /// TODO Explain why we need this (MessageBus handshaking, leapfrogging faulty replicas,
        /// deciding whether starting a view change would be detrimental under some network partitions).
        /// (always running)
        ping_timeout: Timeout,

        /// The number of ticks without enough prepare_ok's before the primary resends a prepare.
        /// (status=normal primary, pipeline has prepare with !ok_quorum_received)
        prepare_timeout: Timeout,

        /// The number of ticks waiting for a prepare_ok.
        /// When triggered, set primary_abdicating=true, which pauses outgoing commit heartbeats.
        /// (status=normal primary, pipeline has prepare with !ok_quorum_received)
        primary_abdicate_timeout: Timeout,

        /// The number of ticks before the primary sends a commit heartbeat:
        /// The primary always sends a commit heartbeat irrespective of when it last sent a prepare.
        /// This improves liveness when prepare messages cannot be replicated fully due to partitions.
        /// (status=normal primary)
        commit_message_timeout: Timeout,

        /// The number of ticks without a heartbeat.
        /// Reset any time the backup receives a heartbeat from the primary.
        /// Triggers SVC messages. If an SVC quorum is achieved, we will kick off a view-change.
        /// (status=normal backup)
        normal_heartbeat_timeout: Timeout,

        /// The number of ticks before resetting the SVC quorum.
        /// (status=normal|view-change, SVC quorum contains message from ANY OTHER replica)
        start_view_change_window_timeout: Timeout,

        /// The number of ticks before resending a `start_view_change` message.
        /// (status=normal|view-change)
        start_view_change_message_timeout: Timeout,

        /// The number of ticks before a view change is timed out.
        /// When triggered, begin sending SVC messages (to attempt to increment the view and try a
        /// different primary) — but keep trying DVCs as well.
        /// (status=view-change)
        view_change_status_timeout: Timeout,

        /// The number of ticks before resending a `do_view_change` message:
        /// (status=view-change)
        do_view_change_message_timeout: Timeout,

        /// The number of ticks before resending a `request_start_view` message.
        /// (status=view-change backup)
        request_start_view_message_timeout: Timeout,

        /// The number of ticks before repairing missing/disconnected headers and/or dirty entries:
        /// (status=normal or (status=view-change and primary))
        repair_timeout: Timeout,

        /// The number of ticks before triggering state sync.
        /// This allows the replica to attempt WAL/grid repair before falling back, even if it
        /// is lagging behind the primary, to try to avoid unnecessary state sync.
        ///
        /// Reset anytime that commit work progresses.
        /// (status=normal backup)
        repair_sync_timeout: Timeout,

        /// The number of ticks before sending a command=request_blocks.
        /// (always running)
        grid_repair_message_timeout: Timeout,

        /// The number of ticks before sending a sync message (message types depend on syncing).
        /// (syncing≠idle)
        sync_message_timeout: Timeout,

        /// Used to calculate exponential backoff with random jitter.
        /// Seeded with the replica's index number.
        prng: std.rand.DefaultPrng,

        /// Used by `Cluster` in the simulator.
        test_context: ?*anyopaque = null,
        /// Simulator hooks.
        event_callback: ?*const fn (replica: *const Self, event: ReplicaEvent) void = null,

        /// The prepare message being committed.
        commit_prepare: ?*Message.Prepare = null,

        tracer_slot_commit: ?tracer.SpanStart = null,
        tracer_slot_checkpoint: ?tracer.SpanStart = null,

        aof: *AOF,

        const OpenOptions = struct {
            node_count: u8,
            storage_size_limit: u64,
            storage: *Storage,
            message_pool: *MessagePool,
            nonce: Nonce,
            time: Time,
            aof: *AOF,
            state_machine_options: StateMachine.Options,
            message_bus_options: MessageBus.Options,
            grid_cache_blocks_count: u32 = Grid.Cache.value_count_max_multiple,
        };

        /// Initializes and opens the provided replica using the options.
        pub fn open(self: *Self, parent_allocator: std.mem.Allocator, options: OpenOptions) !void {
            assert(options.storage_size_limit <= constants.storage_size_limit_max);
            assert(options.storage_size_limit % constants.sector_size == 0);
            assert(options.nonce != 0);

            self.static_allocator = StaticAllocator.init(parent_allocator);
            const allocator = self.static_allocator.allocator();

            self.superblock = try SuperBlock.init(
                allocator,
                .{
                    .storage = options.storage,
                    .storage_size_limit = options.storage_size_limit,
                },
            );

            // Once initialized, the replica is in charge of calling superblock.deinit().
            var initialized = false;
            errdefer if (!initialized) self.superblock.deinit(allocator);

            // Open the superblock:
            self.opened = false;
            self.superblock.open(superblock_open_callback, &self.superblock_context);
            while (!self.opened) self.superblock.storage.tick();
            self.superblock.working.vsr_state.assert_internally_consistent();

            const replica_id = self.superblock.working.vsr_state.replica_id;
            const replica: u8 = for (self.superblock.working.vsr_state.members, 0..) |member, index| {
                if (member == replica_id) break @intCast(index);
            } else unreachable;
            const replica_count = self.superblock.working.vsr_state.replica_count;
            if (replica >= options.node_count or replica_count > options.node_count) {
                log.err("{}: open: no address for replica (replica_count={} node_count={})", .{
                    replica,
                    replica_count,
                    options.node_count,
                });
                return error.NoAddress;
            }

            // Initialize the replica:
            try self.init(allocator, .{
                .cluster = self.superblock.working.cluster,
                .replica_index = replica,
                .replica_count = replica_count,
                .standby_count = options.node_count - replica_count,
                .storage = options.storage,
                .aof = options.aof,
                .nonce = options.nonce,
                .time = options.time,
                .message_pool = options.message_pool,
                .state_machine_options = options.state_machine_options,
                .message_bus_options = options.message_bus_options,
                .grid_cache_blocks_count = options.grid_cache_blocks_count,
            });

            // Disable all dynamic allocation from this point onwards.
            self.static_allocator.transition_from_init_to_static();

            initialized = true;
            errdefer self.deinit(allocator);

            self.opened = false;
            self.journal.recover(journal_recover_callback);
            while (!self.opened) self.superblock.storage.tick();

            // Abort if all slots are faulty, since something is very wrong.
            if (self.journal.faulty.count == constants.journal_slot_count) return error.WALInvalid;

            const vsr_headers = self.superblock.working.vsr_headers();
            // If we were a lagging backup that installed an SV but didn't finish fast-forwarding,
            // the vsr_headers head op may be part of the checkpoint after this one.
            maybe(vsr_headers.slice[0].op > self.op_prepare_max());

            // Given on-disk state, try to recover the head op after a restart.
            //
            // If the replica crashed in status == .normal (view == log_view), the head is generally
            // the last record in WAL. As a special case, during  the first open the last (and the
            // only) record in WAL is the root prepare.
            //
            // Otherwise, the head is recovered from the superblock. When transitioninig to a
            // view_change, replicas encode the current head into vsr_headers.
            //
            // It is a possibility that the head can't be recovered from the local data.
            // In this case, the replica transitions to .recovering_head and waits for a .start_view
            // message from a primary to reset its head.
            var op_head: ?u64 = null;

            if (self.log_view == self.view) {
                for (self.journal.headers) |*header| {
                    assert(header.command == .prepare);
                    if (header.operation != .reserved) {
                        assert(header.op <= self.op_prepare_max());
                        assert(header.view <= self.log_view);

                        if (op_head == null or op_head.? < header.op) op_head = header.op;
                    }
                }
            } else {
                // Fall-through to choose op-head from vsr_headers.
                //
                // "Highest op from log_view in WAL" is not the correct choice for op-head when
                // recovering with a durable DVC (though we still resort to this if there are no
                // usable headers in the vsr_headers). It is possible that we started the view and
                // finished some repair before updating our view_durable.
                //
                // To avoid special-casing this all over, we pretend this higher op doesn't
                // exist. This is safe because we never prepared any ops in the view we joined just
                // before the crash.
                assert(self.log_view < self.view);
                maybe(self.journal.op_maximum() > vsr_headers.slice[0].op);
            }

            // Try to use vsr_headers to update our head op and its header.
            // To avoid the following scenario, don't load headers prior to the head:
            // 1. Replica A prepares[/commits] op X.
            // 2. Replica A crashes.
            // 3. Prepare X is corrupted in the WAL.
            // 4. Replica A recovers. During `Replica.open()`, Replica A loads the header
            //    for op `X - journal_slot_count` (same slot, prior wrap) from vsr_headers
            //    into the journal.
            // 5. Replica A participates in a view-change, but nacks[/does not include] op X.
            // 6. Checkpoint X is truncated.
            for (vsr_headers.slice) |*vsr_header| {
                if (vsr.Headers.dvc_header_type(vsr_header) == .valid and
                    vsr_header.op <= self.op_prepare_max() and
                    (op_head == null or op_head.? <= vsr_header.op))
                {
                    op_head = vsr_header.op;

                    if (!self.journal.has(vsr_header)) {
                        self.journal.set_header_as_dirty(vsr_header);
                    }
                    break;
                }
            } else {
                // This case can only occur if we loaded an SV for its hook header, then converted
                // that SV to a DVC (dropping the hooks; see start_view_into_do_view_change()),
                // but never finished the view change.
                if (op_head == null) {
                    assert(self.view > self.log_view);
                    op_head = self.journal.op_maximum();
                }
            }
            assert(op_head.? <= self.op_prepare_max());

            self.op = op_head.?;
            self.commit_max = @max(
                self.commit_max,
                self.op -| constants.pipeline_prepare_queue_max,
            );

            const header_head = self.journal.header_with_op(self.op).?;
            assert(header_head.view <= self.superblock.working.vsr_state.log_view);

            if (self.solo()) {
                if (self.journal.faulty.count > 0) return error.WALCorrupt;
                assert(self.op_head_certain());

                // Solo replicas must increment their view after recovery.
                // Otherwise, two different versions of an op could exist within a single view
                // (the former version truncated as a torn write).
                //
                // on_request() will ignore incoming requests until the view_durable_update()
                // completes.
                self.log_view += 1;
                self.view += 1;
                self.primary_update_view_headers();
                self.view_durable_update();

                if (self.commit_min == self.op) {
                    self.transition_to_normal_from_recovering_status();
                }
            } else {
                // Even if op_head_certain() returns false, a DVC always has a certain head op.
                if ((self.log_view < self.view and self.op_checkpoint() <= self.op) or
                    (self.log_view == self.view and self.op_head_certain()))
                {
                    if (self.log_view == self.view) {
                        if (self.primary_index(self.view) == self.replica) {
                            self.transition_to_view_change_status(self.view + 1);
                        } else {
                            self.transition_to_normal_from_recovering_status();
                        }
                    } else {
                        assert(self.view > self.log_view);
                        self.transition_to_view_change_status(self.view);
                    }
                } else {
                    self.transition_to_recovering_head();
                }
            }

            maybe(self.status == .normal);
            maybe(self.status == .view_change);
            maybe(self.status == .recovering_head);
            if (self.status == .recovering) assert(self.solo());

            // Asynchronously open the free set and then the (Forest inside) StateMachine so that we
            // can repair grid blocks if necessary:
            self.grid.open(grid_open_callback);
        }

        fn superblock_open_callback(superblock_context: *SuperBlock.Context) void {
            const self = @fieldParentPtr(Self, "superblock_context", superblock_context);
            assert(!self.opened);
            self.opened = true;
        }

        fn journal_recover_callback(journal: *Journal) void {
            const self = @fieldParentPtr(Self, "journal", journal);
            assert(!self.opened);
            self.opened = true;
        }

        fn grid_open_callback(grid: *Grid) void {
            const self = @fieldParentPtr(Self, "grid", grid);
            assert(!self.state_machine_opened);
            assert(self.commit_stage == .idle);
            assert(self.syncing == .idle);
            assert(self.sync_tables == null);
            assert(self.grid_repair_tables.executing() == 0);
            assert(self.grid.free_set.count_released() == self.grid.free_set_checkpoint.block_count());
            assert(std.meta.eql(
                grid.free_set_checkpoint.checkpoint_reference(),
                self.superblock.working.free_set_reference(),
            ));

            // TODO This can probably be performed concurrently to StateMachine.open().
            self.client_sessions_checkpoint.open(
                &self.grid,
                self.superblock.working.client_sessions_reference(),
                client_sessions_open_callback,
            );
        }

        fn client_sessions_open_callback(client_sessions_checkpoint: *CheckpointTrailer) void {
            const self = @fieldParentPtr(Self, "client_sessions_checkpoint", client_sessions_checkpoint);
            assert(!self.state_machine_opened);
            assert(self.commit_stage == .idle);
            assert(self.syncing == .idle);
            assert(self.sync_tables == null);
            assert(self.grid_repair_tables.executing() == 0);
            assert(self.client_sessions.entries_free.count() == constants.clients_max);
            assert(std.meta.eql(
                self.client_sessions_checkpoint.checkpoint_reference(),
                self.superblock.working.client_sessions_reference(),
            ));

            {
                const checkpoint = &self.client_sessions_checkpoint;
                var address_previous: u64 = 0;
                for (checkpoint.block_addresses[0..checkpoint.block_count()]) |address| {
                    assert(address > 0);
                    assert(address > address_previous);
                    address_previous = address;
                    self.grid.release(address);
                }
            }

            const trailer_size = self.client_sessions_checkpoint.size;
            const trailer_chunks = self.client_sessions_checkpoint.decode_chunks();

            if (self.superblock.working.client_sessions_reference().empty()) {
                assert(trailer_chunks.len == 0);
                assert(trailer_size == 0);
            } else {
                assert(trailer_chunks.len == 1);
                assert(trailer_size == ClientSessions.encode_size);
                assert(trailer_size == trailer_chunks[0].len);
                self.client_sessions.decode(trailer_chunks[0]);
            }

            if (self.superblock.working.vsr_state.sync_op_max > 0) {
                maybe(self.client_replies.writing.count() > 0);
                for (0..constants.clients_max) |entry_slot| {
                    if (self.client_sessions.entries_free.isSet(entry_slot)) {
                        assert(!self.client_replies.faulty.isSet(entry_slot));
                    } else {
                        const entry = &self.client_sessions.entries[entry_slot];
                        if (entry.header.op >= self.superblock.working.vsr_state.sync_op_min and
                            entry.header.op <= self.superblock.working.vsr_state.sync_op_max)
                        {
                            const entry_faulty = entry.header.size > @sizeOf(Header);
                            self.client_replies.faulty.setValue(entry_slot, entry_faulty);
                        }
                    }
                }
            }

            self.state_machine.open(state_machine_open_callback);
        }

        fn state_machine_open_callback(state_machine: *StateMachine) void {
            const self = @fieldParentPtr(Self, "state_machine", state_machine);
            assert(self.grid.free_set.opened);
            assert(!self.state_machine_opened);
            assert(self.commit_stage == .idle);
            assert(self.syncing == .idle);
            assert(self.sync_tables == null);
            assert(self.grid_repair_tables.executing() == 0);

            log.debug("{}: state_machine_open_callback: sync_ops={}..{}", .{
                self.replica,
                self.superblock.working.vsr_state.sync_op_min,
                self.superblock.working.vsr_state.sync_op_max,
            });

            self.state_machine_opened = true;
            if (self.event_callback) |hook| hook(self, .state_machine_opened);

            if (self.superblock.working.vsr_state.sync_op_max > 0) {
                self.sync_content();
            }

            if (self.solo()) {
                if (self.commit_min < self.op) {
                    self.advance_commit_max(self.op, @src());
                    self.commit_journal();

                    // Recovery will complete when commit_journal finishes.
                    assert(self.status == .recovering);
                } else {
                    assert(self.status == .normal);
                }
            } else {
                if (self.status == .normal and self.primary()) {
                    if (self.pipeline.queue.prepare_queue.count > 0) {
                        self.commit_pipeline();
                    }
                } else {
                    if (self.status != .recovering_head) {
                        self.commit_journal();
                    }
                }
            }
        }

        const Options = struct {
            cluster: u128,
            replica_count: u8,
            standby_count: u8,
            replica_index: u8,
            nonce: Nonce,
            time: Time,
            storage: *Storage,
            aof: *AOF,
            message_pool: *MessagePool,
            message_bus_options: MessageBus.Options,
            state_machine_options: StateMachine.Options,
            grid_cache_blocks_count: u32,
        };

        /// NOTE: self.superblock must be initialized and opened prior to this call.
        fn init(self: *Self, allocator: Allocator, options: Options) !void {
            assert(options.nonce != 0);

            const replica_count = options.replica_count;
            const standby_count = options.standby_count;
            const node_count = replica_count + standby_count;
            assert(replica_count > 0);
            assert(replica_count <= constants.replicas_max);
            assert(standby_count <= constants.standbys_max);
            assert(node_count <= constants.members_max);

            const replica_index = options.replica_index;
            assert(replica_index < node_count);

            assert(self.opened);
            assert(self.superblock.opened);
            self.superblock.working.vsr_state.assert_internally_consistent();

            const quorums = vsr.quorums(replica_count);
            const quorum_replication = quorums.replication;
            const quorum_view_change = quorums.view_change;
            const quorum_nack_prepare = quorums.nack_prepare;
            const quorum_majority = quorums.majority;
            assert(quorum_replication <= replica_count);
            assert(quorum_view_change <= replica_count);
            assert(quorum_nack_prepare <= replica_count);
            assert(quorum_majority <= replica_count);

            if (replica_count <= 2) {
                assert(quorum_replication == replica_count);
                assert(quorum_view_change == replica_count);
            } else {
                assert(quorum_replication < replica_count);
                assert(quorum_view_change < replica_count);
            }

            // Flexible quorums are safe if these two quorums intersect so that this relation holds:
            assert(quorum_replication + quorum_view_change > replica_count);

            self.time = options.time;

            // The clock is special-cased for standbys. We want to balance two concerns:
            //   - standby clock should never affect cluster time,
            //   - standby should have up-to-date clock, such that it can quickly join the cluster
            //     (or be denied joining if its clock is broken).
            //
            // To do this:
            //   - an active replica clock tracks only other active replicas,
            //   - a standby clock tracks active replicas and the standby itself.
            self.clock = try if (replica_index < replica_count) Clock.init(
                allocator,
                replica_count,
                replica_index,
                &self.time,
            ) else Clock.init(
                allocator,
                replica_count + 1,
                replica_count,
                &self.time,
            );
            errdefer self.clock.deinit(allocator);

            self.journal = try Journal.init(allocator, options.storage, replica_index);
            errdefer self.journal.deinit(allocator);

            var client_sessions = try ClientSessions.init(allocator);
            errdefer client_sessions.deinit(allocator);

            var client_sessions_checkpoint = try CheckpointTrailer.init(
                allocator,
                .client_sessions,
                ClientSessions.encode_size,
            );
            errdefer client_sessions_checkpoint.deinit(allocator);

            var client_replies = ClientReplies.init(.{
                .storage = options.storage,
                .message_pool = options.message_pool,
                .replica_index = replica_index,
            });
            errdefer client_replies.deinit();

            self.message_bus = try MessageBus.init(
                allocator,
                options.cluster,
                .{ .replica = options.replica_index },
                options.message_pool,
                Self.on_message_from_bus,
                options.message_bus_options,
            );
            errdefer self.message_bus.deinit(allocator);

            self.grid = try Grid.init(allocator, .{
                .superblock = &self.superblock,
                .cache_blocks_count = options.grid_cache_blocks_count,
                .missing_blocks_max = constants.grid_missing_blocks_max,
                .missing_tables_max = constants.grid_missing_tables_max,
            });
            errdefer self.grid.deinit(allocator);

            for (&self.grid_repair_write_blocks, 0..) |*block, i| {
                errdefer for (self.grid_repair_write_blocks[0..i]) |b| allocator.free(b);
                block.* = try allocate_block(allocator);
            }
            errdefer for (self.grid_repair_write_blocks) |b| allocator.free(b);

            self.state_machine = try StateMachine.init(
                allocator,
                &self.grid,
                options.state_machine_options,
            );
            errdefer self.state_machine.deinit(allocator);

            self.* = Self{
                .static_allocator = self.static_allocator,
                .cluster = options.cluster,
                .replica_count = replica_count,
                .standby_count = standby_count,
                .node_count = node_count,
                .replica = replica_index,
                .quorum_replication = quorum_replication,
                .quorum_view_change = quorum_view_change,
                .quorum_nack_prepare = quorum_nack_prepare,
                .quorum_majority = quorum_majority,
                .nonce = options.nonce,
                // Copy the (already-initialized) time back, to avoid regressing the monotonic
                // clock guard.
                .time = self.time,
                .clock = self.clock,
                .journal = self.journal,
                .client_sessions = client_sessions,
                .client_sessions_checkpoint = client_sessions_checkpoint,
                .client_replies = client_replies,
                .message_bus = self.message_bus,
                .state_machine = self.state_machine,
                .superblock = self.superblock,
                .grid = self.grid,
                .grid_repair_write_blocks = self.grid_repair_write_blocks,
                .opened = self.opened,
                .view = self.superblock.working.vsr_state.view,
                .log_view = self.superblock.working.vsr_state.log_view,
                .op = undefined,
                .commit_min = self.superblock.working.vsr_state.checkpoint.commit_min,
                .commit_max = self.superblock.working.vsr_state.commit_max,
                .pipeline = .{ .cache = .{} },
                .view_headers = vsr.Headers.ViewChangeArray.init_from_slice(
                    self.superblock.working.vsr_headers().command,
                    self.superblock.working.vsr_headers().slice,
                ),
                .ping_timeout = Timeout{
                    .name = "ping_timeout",
                    .id = replica_index,
                    .after = 100,
                },
                .prepare_timeout = Timeout{
                    .name = "prepare_timeout",
                    .id = replica_index,
                    .after = 50,
                },
                .primary_abdicate_timeout = Timeout{
                    .name = "primary_abdicate_timeout",
                    .id = replica_index,
                    .after = 1000,
                },
                .commit_message_timeout = Timeout{
                    .name = "commit_message_timeout",
                    .id = replica_index,
                    .after = 50,
                },
                .normal_heartbeat_timeout = Timeout{
                    .name = "normal_heartbeat_timeout",
                    .id = replica_index,
                    .after = 500,
                },
                .start_view_change_window_timeout = Timeout{
                    .name = "start_view_change_window_timeout",
                    .id = replica_index,
                    .after = 500,
                },
                .start_view_change_message_timeout = Timeout{
                    .name = "start_view_change_message_timeout",
                    .id = replica_index,
                    .after = 50,
                },
                .view_change_status_timeout = Timeout{
                    .name = "view_change_status_timeout",
                    .id = replica_index,
                    .after = 500,
                },
                .do_view_change_message_timeout = Timeout{
                    .name = "do_view_change_message_timeout",
                    .id = replica_index,
                    .after = 50,
                },
                .request_start_view_message_timeout = Timeout{
                    .name = "request_start_view_message_timeout",
                    .id = replica_index,
                    .after = 100,
                },
                .repair_timeout = Timeout{
                    .name = "repair_timeout",
                    .id = replica_index,
                    .after = 50,
                },
                .repair_sync_timeout = Timeout{
                    .name = "repair_sync_timeout",
                    .id = replica_index,
                    .after = 500,
                },
                .grid_repair_message_timeout = Timeout{
                    .name = "grid_repair_message_timeout",
                    .id = replica_index,
                    .after = 50,
                },
                .sync_message_timeout = Timeout{
                    .name = "sync_message_timeout",
                    .id = replica_index,
                    .after = 50,
                },
                .prng = std.rand.DefaultPrng.init(replica_index),

                .aof = options.aof,
            };

            log.debug("{}: init: replica_count={} quorum_view_change={} quorum_replication={}", .{
                self.replica,
                self.replica_count,
                self.quorum_view_change,
                self.quorum_replication,
            });
            assert(self.status == .recovering);
        }

        /// Free all memory and unref all messages held by the replica.
        /// This does not deinitialize the Storage or Time.
        pub fn deinit(self: *Self, allocator: Allocator) void {
            assert(self.tracer_slot_checkpoint == null);
            assert(self.tracer_slot_commit == null);

            self.static_allocator.transition_from_static_to_deinit();

            self.client_replies.deinit();
            self.client_sessions_checkpoint.deinit(allocator);
            self.client_sessions.deinit(allocator);
            self.journal.deinit(allocator);
            self.clock.deinit(allocator);
            self.state_machine.deinit(allocator);
            self.superblock.deinit(allocator);
            self.grid.deinit(allocator);
            defer self.message_bus.deinit(allocator);

            switch (self.pipeline) {
                inline else => |*pipeline| pipeline.deinit(self.message_bus.pool),
            }

            if (self.loopback_queue) |loopback_message| {
                assert(loopback_message.next == null);
                self.message_bus.unref(loopback_message);
                self.loopback_queue = null;
            }

            if (self.commit_prepare) |message| {
                assert(self.commit_stage != .idle);
                self.message_bus.unref(message);
                self.commit_prepare = null;
            }

            var grid_reads = self.grid_reads.iterate();
            while (grid_reads.next()) |read| self.message_bus.unref(read.message);

            for (self.grid_repair_write_blocks) |block| allocator.free(block);

            for (self.do_view_change_from_all_replicas) |message| {
                if (message) |m| self.message_bus.unref(m);
            }
        }

        /// Time is measured in logical ticks that are incremented on every call to tick().
        /// This eliminates a dependency on the system time and enables deterministic testing.
        pub fn tick(self: *Self) void {
            assert(self.opened);
            // Ensure that all asynchronous IO callbacks flushed the loopback queue as needed.
            // If an IO callback queues a loopback message without flushing the queue then this will
            // delay the delivery of messages (e.g. a prepare_ok from the primary to itself) and
            // decrease throughput significantly.
            assert(self.loopback_queue == null);

            // TODO Replica owns Time; should it tick() here instead of Clock?
            self.clock.tick();
            self.message_bus.tick();

            self.ping_timeout.tick();
            self.prepare_timeout.tick();
            self.primary_abdicate_timeout.tick();
            self.commit_message_timeout.tick();
            self.normal_heartbeat_timeout.tick();
            self.start_view_change_window_timeout.tick();
            self.start_view_change_message_timeout.tick();
            self.view_change_status_timeout.tick();
            self.do_view_change_message_timeout.tick();
            self.request_start_view_message_timeout.tick();
            self.repair_timeout.tick();
            self.repair_sync_timeout.tick();
            self.grid_repair_message_timeout.tick();
            self.sync_message_timeout.tick();

            if (self.ping_timeout.fired()) self.on_ping_timeout();
            if (self.prepare_timeout.fired()) self.on_prepare_timeout();
            if (self.primary_abdicate_timeout.fired()) self.on_primary_abdicate_timeout();
            if (self.commit_message_timeout.fired()) self.on_commit_message_timeout();
            if (self.normal_heartbeat_timeout.fired()) self.on_normal_heartbeat_timeout();
            if (self.start_view_change_window_timeout.fired()) self.on_start_view_change_window_timeout();
            if (self.start_view_change_message_timeout.fired()) self.on_start_view_change_message_timeout();
            if (self.view_change_status_timeout.fired()) self.on_view_change_status_timeout();
            if (self.do_view_change_message_timeout.fired()) self.on_do_view_change_message_timeout();
            if (self.request_start_view_message_timeout.fired()) self.on_request_start_view_message_timeout();
            if (self.repair_timeout.fired()) self.on_repair_timeout();
            if (self.repair_sync_timeout.fired()) self.on_repair_sync_timeout();
            if (self.grid_repair_message_timeout.fired()) self.on_grid_repair_message_timeout();
            if (self.sync_message_timeout.fired()) self.on_sync_message_timeout();

            // None of the on_timeout() functions above should send a message to this replica.
            assert(self.loopback_queue == null);
        }

        /// Called by the MessageBus to deliver a message to the replica.
        fn on_message_from_bus(message_bus: *MessageBus, message: *Message) void {
            const self = @fieldParentPtr(Self, "message_bus", message_bus);
            self.on_message(message);
        }

        pub fn on_message(self: *Self, message: *Message) void {
            assert(self.opened);
            assert(self.loopback_queue == null);
            assert(message.references > 0);

            // Switch on the header type so that we don't log opaque bytes for the per-command data.
            switch (message.header.into_any()) {
                inline else => |header| {
                    log.debug("{}: on_message: view={} status={} {}", .{
                        self.replica,
                        self.view,
                        self.status,
                        header,
                    });
                },
            }

            if (message.header.invalid()) |reason| {
                log.err("{}: on_message: invalid (command={}, {s})", .{
                    self.replica,
                    message.header.command,
                    reason,
                });
                return;
            }

            // No client or replica should ever send a .reserved message.
            assert(message.header.command != .reserved);

            if (message.header.cluster != self.cluster) {
                log.warn("{}: on_message: wrong cluster (cluster must be {} not {})", .{
                    self.replica,
                    self.cluster,
                    message.header.cluster,
                });
                return;
            }

            self.jump_view(message.header);
            self.jump_sync_target(message.header);

            assert(message.header.replica < self.node_count);
            switch (message.into_any()) {
                .ping => |m| self.on_ping(m),
                .pong => |m| self.on_pong(m),
                .ping_client => |m| self.on_ping_client(m),
                .request => |m| self.on_request(m),
                .prepare => |m| self.on_prepare(m),
                .prepare_ok => |m| self.on_prepare_ok(m),
                .reply => |m| self.on_reply(m),
                .commit => |m| self.on_commit(m),
                .start_view_change => |m| self.on_start_view_change(m),
                .do_view_change => |m| self.on_do_view_change(m),
                .start_view => |m| self.on_start_view(m),
                .request_start_view => |m| self.on_request_start_view(m),
                .request_prepare => |m| self.on_request_prepare(m),
                .request_headers => |m| self.on_request_headers(m),
                .request_reply => |m| self.on_request_reply(m),
                .headers => |m| self.on_headers(m),
                .request_blocks => |m| self.on_request_blocks(m),
                .block => |m| self.on_block(m),
                .request_sync_checkpoint => |m| self.on_request_sync_checkpoint(m),
                .sync_checkpoint => |m| self.on_sync_checkpoint(m),
                // A replica should never handle misdirected messages intended for a client:
                .pong_client, .eviction => {
                    log.warn("{}: on_message: misdirected message ({s})", .{
                        self.replica,
                        @tagName(message.header.command),
                    });
                    return;
                },
                .reserved => unreachable,
            }

            if (self.loopback_queue) |loopback_message| {
                log.err("{}: on_message: on_{s}() queued a {s} loopback message with no flush", .{
                    self.replica,
                    @tagName(message.header.command),
                    @tagName(loopback_message.header.command),
                });
            }

            // Any message handlers that loopback must take responsibility for the flush.
            assert(self.loopback_queue == null);
        }

        /// Pings are used by replicas to synchronise cluster time and to probe for network connectivity.
        fn on_ping(self: *Self, message: *const Message.Ping) void {
            assert(message.header.command == .ping);
            if (self.status != .normal and self.status != .view_change) return;

            assert(self.status == .normal or self.status == .view_change);

            if (message.header.replica == self.replica) {
                log.warn("{}: on_ping: misdirected message (self)", .{self.replica});
                return;
            }

            // TODO Drop pings that were not addressed to us.

            self.send_header_to_replica(message.header.replica, @bitCast(Header.Pong{
                .command = .pong,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view_durable(), // Don't drop pongs while the view is being updated.
                // Copy the ping's monotonic timestamp to our pong and add our wall clock sample:
                .ping_timestamp_monotonic = message.header.ping_timestamp_monotonic,
                .pong_timestamp_wall = @bitCast(self.clock.realtime()),
            }));
        }

        fn on_pong(self: *Self, message: *const Message.Pong) void {
            assert(message.header.command == .pong);
            if (message.header.replica == self.replica) {
                log.warn("{}: on_pong: misdirected message (self)", .{self.replica});
                return;
            }

            // Ignore clocks of standbys.
            if (message.header.replica >= self.replica_count) return;

            const m0 = message.header.ping_timestamp_monotonic;
            const t1: i64 = @bitCast(message.header.pong_timestamp_wall);
            const m2 = self.clock.monotonic();

            self.clock.learn(message.header.replica, m0, t1, m2);
        }

        /// Pings are used by clients to learn about the current view.
        fn on_ping_client(self: *Self, message: *const Message.PingClient) void {
            assert(message.header.command == .ping_client);
            assert(message.header.client != 0);

            if (self.ignore_ping_client(message)) return;

            self.send_header_to_client(message.header.client, @bitCast(Header.PongClient{
                .command = .pong_client,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
            }));
        }

        /// When there is free space in the pipeline's prepare queue:
        ///   The primary advances op-number, adds the request to the end of the log, and updates the
        ///   information for this client in the client-table to contain the new request number, s.
        ///   Then it sends a ⟨PREPARE v, m, n, k⟩ message to the other replicas, where v is the
        ///   current view-number, m is the message it received from the client, n is the op-number
        ///   it assigned to the request, and k is the commit-number.
        /// Otherwise, when there is room in the pipeline's request queue:
        ///   The request is queued, and will be dequeued & prepared when the pipeline head commits.
        /// Otherwise, drop the request.
        fn on_request(self: *Self, message: *Message.Request) void {
            if (self.ignore_request_message(message)) return;

            assert(self.status == .normal);
            assert(self.primary());
            assert(self.syncing == .idle);
            assert(self.commit_min == self.commit_max);
            assert(self.commit_max + self.pipeline.queue.prepare_queue.count == self.op);

            assert(message.header.command == .request);
            assert(message.header.operation != .reserved);
            assert(message.header.operation != .root);
            assert(message.header.view <= self.view); // The client's view may be behind ours.

            const realtime = self.clock.realtime_synchronized() orelse {
                log.err("{}: on_request: dropping (clock not synchronized)", .{self.replica});
                return;
            };

            const request = .{
                .message = message.ref(),
                .realtime = realtime,
            };

            if (self.pipeline.queue.prepare_queue.full()) {
                self.pipeline.queue.push_request(request);
            } else {
                self.primary_pipeline_prepare(request);
            }
        }

        /// Replication is simple, with a single code path for the primary and backups.
        ///
        /// The primary starts by sending a prepare message to itself.
        ///
        /// Each replica (including the primary) then forwards this prepare message to the next
        /// replica in the configuration, in parallel to writing to its own journal, closing the
        /// circle until the next replica is back to the primary, in which case the replica does not
        /// forward.
        ///
        /// This keeps the primary's outgoing bandwidth limited (one-for-one) to incoming bandwidth,
        /// since the primary need only replicate to the next replica. Otherwise, the primary would
        /// need to replicate to multiple backups, dividing available bandwidth.
        ///
        /// This does not impact latency, since with Flexible Paxos we need only one remote
        /// prepare_ok. It is ideal if this synchronous replication to one remote replica is to the
        /// next replica, since that is the replica next in line to be primary, which will need to
        /// be up-to-date before it can start the next view.
        ///
        /// At the same time, asynchronous replication keeps going, so that if our local disk is
        /// slow, then any latency spike will be masked by more remote prepare_ok messages as they
        /// come in. This gives automatic tail latency tolerance for storage latency spikes.
        ///
        /// The remaining problem then is tail latency tolerance for network latency spikes.
        /// If the next replica is down or partitioned, then the primary's prepare timeout will fire,
        /// and the primary will resend but to another replica, until it receives enough prepare_ok's.
        fn on_prepare(self: *Self, message: *Message.Prepare) void {
            assert(message.header.command == .prepare);
            assert(message.header.replica < self.replica_count);
            assert(message.header.operation != .reserved);

            if (self.is_repair(message)) {
                log.debug("{}: on_prepare: ignoring (repair)", .{self.replica});
                self.on_repair(message);
                return;
            }

            if (self.status != .normal) {
                log.debug("{}: on_prepare: ignoring ({})", .{ self.replica, self.status });
                return;
            }

            if (message.header.view < self.view) {
                log.debug("{}: on_prepare: ignoring (older view)", .{self.replica});
                return;
            }

            if (message.header.view > self.view) {
                log.debug("{}: on_prepare: ignoring (newer view)", .{self.replica});
                return;
            }

            assert(self.status == .normal);
            assert(message.header.view == self.view);
            assert(self.primary() or self.backup());
            assert(message.header.replica == self.primary_index(message.header.view));
            assert(message.header.op > self.op_checkpoint());
            assert(message.header.op > self.op);
            assert(message.header.op > self.commit_min);

            if (self.backup()) {
                self.advance_commit_max(message.header.commit, @src());
                assert(self.commit_max >= message.header.commit);
            }
            defer if (self.backup()) self.commit_journal();

            // Verify that the new request will fit in the WAL.
            if (message.header.op > self.op_prepare_max()) {
                log.debug("{}: on_prepare: ignoring op={} (too far ahead, prepare_max={})", .{
                    self.replica,
                    message.header.op,
                    self.op_prepare_max(),
                });
                // When we are the primary, `on_request` enforces this invariant.
                assert(self.backup());
                return;
            }

            if (message.header.checkpoint_id != self.superblock.working.checkpoint_id() and
                message.header.checkpoint_id != self.superblock.working.vsr_state.checkpoint.parent_checkpoint_id)
            {
                // Panic on encountering a prepare which does not match an expected checkpoint id.
                //
                // If this branch is hit, there is a storage determinism problem. At this point in
                // the code it is not possible to distinguish whether the problem is with this
                // replica, the prepare's replica, or both independently.
                log.err("{}: on_prepare: checkpoint diverged " ++
                    "(op={} expect={x:0>32} received={x:0>32} from={})", .{
                    self.replica,
                    message.header.op,
                    self.superblock.working.checkpoint_id(),
                    message.header.checkpoint_id,
                    message.header.replica,
                });

                assert(self.backup());
                @panic("checkpoint diverged");
            }

            if (message.header.op > self.op + 1) {
                log.debug("{}: on_prepare: newer op", .{self.replica});
                self.jump_to_newer_op_in_normal_status(message.header);
                // "`replica.op` exists" invariant is temporarily broken.
                assert(self.journal.header_with_op(message.header.op - 1) == null);
            }

            if (self.journal.previous_entry(message.header)) |previous| {
                // Any previous entry may be a whole journal's worth of ops behind due to wrapping.
                // We therefore do not do any further op or checksum assertions beyond this:
                self.panic_if_hash_chain_would_break_in_the_same_view(previous, message.header);
            }

            // We must advance our op and set the header as dirty before replicating and journalling.
            // The primary needs this before its journal is outrun by any prepare_ok quorum:
            log.debug("{}: on_prepare: advancing: op={}..{} checksum={}..{}", .{
                self.replica,
                self.op,
                message.header.op,
                message.header.parent,
                message.header.checksum,
            });
            assert(message.header.op == self.op + 1);
            assert(message.header.op <= self.op_prepare_max());
            assert(message.header.op - self.op_repair_min() < constants.journal_slot_count);
            self.op = message.header.op;
            self.journal.set_header_as_dirty(message.header);

            self.replicate(message);
            self.append(message);
        }

        fn on_prepare_ok(self: *Self, message: *Message.PrepareOk) void {
            assert(message.header.command == .prepare_ok);
            if (self.ignore_prepare_ok(message)) return;

            assert(self.status == .normal);
            assert(message.header.view == self.view);
            assert(self.primary());
            assert(self.syncing == .idle);

            const prepare = self.pipeline.queue.prepare_by_prepare_ok(message) orelse {
                // This can be normal, for example, if an old prepare_ok is replayed.
                log.debug("{}: on_prepare_ok: not preparing ok={} checksum={}", .{
                    self.replica,
                    message.header.op,
                    message.header.prepare_checksum,
                });
                return;
            };

            assert(prepare.message.header.checksum == message.header.prepare_checksum);
            assert(prepare.message.header.op >= self.commit_max + 1);
            assert(prepare.message.header.op <= self.commit_max +
                self.pipeline.queue.prepare_queue.count);
            assert(prepare.message.header.op <= self.op);

            assert(prepare.message.header.checkpoint_id == message.header.checkpoint_id);
            assert(prepare.message.header.checkpoint_id ==
                self.checkpoint_id_for_op(prepare.message.header.op).?);

            // Wait until we have a quorum of prepare_ok messages (including ourself):
            const threshold = self.quorum_replication;

            if (!prepare.ok_from_all_replicas.isSet(message.header.replica)) {
                self.primary_abdicating = false;
                if (!prepare.ok_quorum_received) {
                    self.primary_abdicate_timeout.reset();
                }
            }

            const count = self.count_message_and_receive_quorum_exactly_once(
                &prepare.ok_from_all_replicas,
                message,
                threshold,
            ) orelse return;

            const prepare_pending = self.primary_pipeline_pending().?;
            assert(count == threshold);
            assert(!prepare.ok_quorum_received);
            prepare.ok_quorum_received = true;

            log.debug("{}: on_prepare_ok: quorum received, context={}", .{
                self.replica,
                prepare.message.header.checksum,
            });

            assert(self.prepare_timeout.ticking);
            assert(self.primary_abdicate_timeout.ticking);
            assert(!self.primary_abdicating);
            if (self.primary_pipeline_pending()) |_| {
                if (prepare_pending == prepare) self.prepare_timeout.reset();
            } else {
                self.prepare_timeout.stop();
                self.primary_abdicate_timeout.stop();
            }

            self.commit_pipeline();
        }

        fn on_reply(self: *Self, message: *Message.Reply) void {
            assert(message.header.command == .reply);
            assert(message.header.replica < self.replica_count);

            const entry = self.client_sessions.get(message.header.client) orelse {
                log.debug("{}: on_reply: ignoring, client not in table (client={} request={})", .{
                    self.replica,
                    message.header.client,
                    message.header.request,
                });
                return;
            };

            if (message.header.checksum != entry.header.checksum) {
                log.debug("{}: on_reply: ignoring, reply not in table (client={} request={})", .{
                    self.replica,
                    message.header.client,
                    message.header.request,
                });
                return;
            }

            const slot = self.client_sessions.get_slot_for_header(message.header).?;
            if (!self.client_replies.faulty.isSet(slot.index)) {
                log.debug("{}: on_reply: ignoring, reply is clean (client={} request={})", .{
                    self.replica,
                    message.header.client,
                    message.header.request,
                });
                return;
            }

            if (!self.client_replies.ready_sync()) {
                log.debug("{}: on_reply: ignoring, busy (client={} request={})", .{
                    self.replica,
                    message.header.client,
                    message.header.request,
                });
                return;
            }

            log.debug("{}: on_reply: repairing reply (client={} request={})", .{
                self.replica,
                message.header.client,
                message.header.request,
            });

            self.client_replies.write_reply(slot, message, .repair);
        }

        /// Known issue:
        /// TODO The primary should stand down if it sees too many retries in on_prepare_timeout().
        /// It's possible for the network to be one-way partitioned so that backups don't see the
        /// primary as down, but neither can the primary hear from the backups.
        fn on_commit(self: *Self, message: *const Message.Commit) void {
            assert(message.header.command == .commit);
            assert(message.header.replica < self.replica_count);

            if (self.status != .normal) {
                log.debug("{}: on_commit: ignoring ({})", .{ self.replica, self.status });
                return;
            }

            if (message.header.view < self.view) {
                log.debug("{}: on_commit: ignoring (older view)", .{self.replica});
                return;
            }

            if (message.header.view > self.view) {
                log.debug("{}: on_commit: ignoring (newer view)", .{self.replica});
                return;
            }

            if (self.primary()) {
                log.warn("{}: on_commit: misdirected message (primary)", .{self.replica});
                return;
            }

            assert(self.status == .normal);
            assert(self.backup());
            assert(message.header.view == self.view);
            assert(message.header.replica == self.primary_index(message.header.view));

            // Old/duplicate heartbeats don't count.
            if (self.heartbeat_timestamp < message.header.timestamp_monotonic) {
                self.heartbeat_timestamp = message.header.timestamp_monotonic;
                self.normal_heartbeat_timeout.reset();
                if (!self.standby()) {
                    self.start_view_change_from_all_replicas.unset(self.replica);
                }
            }

            // We may not always have the latest commit entry but if we do our checksum must match:
            if (self.journal.header_with_op(message.header.commit)) |commit_entry| {
                if (commit_entry.checksum == message.header.commit_checksum) {
                    log.debug("{}: on_commit: checksum verified", .{self.replica});
                } else if (self.valid_hash_chain(@src())) {
                    @panic("commit checksum verification failed");
                } else {
                    // We may still be repairing after receiving the start_view message.
                    log.debug("{}: on_commit: skipping checksum verification", .{self.replica});
                }
            }

            self.advance_commit_max(message.header.commit, @src());
            self.commit_journal();
        }

        fn on_repair(self: *Self, message: *Message.Prepare) void {
            assert(message.header.command == .prepare);

            if (self.status != .normal and self.status != .view_change) {
                log.debug("{}: on_repair: ignoring ({})", .{ self.replica, self.status });
                return;
            }

            if (message.header.view > self.view) {
                log.debug("{}: on_repair: ignoring (newer view)", .{self.replica});
                return;
            }

            if (self.status == .view_change and message.header.view == self.view) {
                log.debug("{}: on_repair: ignoring (view started)", .{self.replica});
                return;
            }

            if (self.status == .view_change and self.primary_index(self.view) != self.replica) {
                log.debug("{}: on_repair: ignoring (view change, backup)", .{self.replica});
                return;
            }

            if (self.status == .view_change and !self.do_view_change_quorum) {
                log.debug("{}: on_repair: ignoring (view change, waiting for quorum)", .{
                    self.replica,
                });
                return;
            }

            if (message.header.op > self.op) {
                assert(message.header.view < self.view);
                log.debug("{}: on_repair: ignoring (would advance self.op)", .{self.replica});
                return;
            }

            assert(self.status == .normal or self.status == .view_change);
            assert(self.repairs_allowed());
            assert(message.header.view <= self.view);
            assert(message.header.op <= self.op); // Repairs may never advance `self.op`.

            if (self.journal.has_clean(message.header)) {
                log.debug("{}: on_repair: ignoring (duplicate)", .{self.replica});

                self.send_prepare_ok(message.header);
                defer self.flush_loopback_queue();
                return;
            }

            if (self.repair_header(message.header)) {
                assert(self.journal.has_dirty(message.header));

                log.debug("{}: on_repair: repairing journal", .{self.replica});
                self.write_prepare(message, .repair);
            }
        }

        fn on_start_view_change(self: *Self, message: *Message.StartViewChange) void {
            assert(message.header.command == .start_view_change);
            if (self.ignore_start_view_change_message(message)) return;

            assert(!self.solo());
            assert(self.status == .normal or self.status == .view_change);
            assert(message.header.view == self.view);

            // Wait until we have a view-change quorum of messages (possibly including ourself).
            // This ensures that we do not start a view-change while normal request processing
            // is possible.
            const threshold = self.quorum_view_change;

            self.start_view_change_from_all_replicas.set(message.header.replica);

            if (self.replica != message.header.replica and
                !self.start_view_change_window_timeout.ticking)
            {
                self.start_view_change_window_timeout.start();
            }

            const count = self.start_view_change_from_all_replicas.count();
            assert(count <= threshold);

            if (count < threshold) {
                log.debug("{}: on_start_view_change: view={} waiting for quorum " ++
                    "({}/{}; replicas={b:0>6})", .{
                    self.replica,
                    self.view,
                    count,
                    threshold,
                    self.start_view_change_from_all_replicas.mask,
                });
                return;
            }
            log.debug("{}: on_start_view_change: view={} quorum received (replicas={b:0>6})", .{
                self.replica,
                self.view,
                self.start_view_change_from_all_replicas.mask,
            });

            self.transition_to_view_change_status(self.view + 1);
            assert(self.start_view_change_from_all_replicas.count() == 0);
        }

        /// DVC serves two purposes:
        ///
        /// When the new primary receives a quorum of do_view_change messages from different replicas
        /// (including itself), it sets its view number to that in the messages and selects as the
        /// new log the one contained in the message with the largest v′; if several messages have
        /// the same v′ it selects the one among them with the largest n. It sets its op number to
        /// that of the topmost entry in the new log, sets its commit number to the largest such
        /// number it received in the do_view_change messages, changes its status to normal, and
        /// informs the other replicas of the completion of the view change by sending
        /// ⟨start_view v, l, n, k⟩ messages to the other replicas, where l is the new log, n is the
        /// op number, and k is the commit number.
        ///
        /// When a new backup receives a do_view_change message for a new view, it transitions to
        /// that new view in view-change status and begins to broadcast its own DVC.
        fn on_do_view_change(self: *Self, message: *Message.DoViewChange) void {
            assert(message.header.command == .do_view_change);
            if (self.ignore_view_change_message(message.base_const())) return;

            assert(!self.solo());
            assert(self.status == .view_change);
            assert(self.syncing == .idle);
            assert(!self.do_view_change_quorum);
            assert(self.primary_index(self.view) == self.replica);
            assert(message.header.view == self.view);
            DVCQuorum.verify_message(message);

            self.primary_receive_do_view_change(message);

            // Wait until we have a quorum of messages (including ourself):
            assert(self.do_view_change_from_all_replicas[self.replica] != null);
            assert(self.do_view_change_from_all_replicas[self.replica].?.header.checkpoint_op <=
                self.op_checkpoint());
            DVCQuorum.verify(self.do_view_change_from_all_replicas);

            // Store in a var so that `.complete_valid` can capture a mutable pointer in switch.
            var headers = DVCQuorum.quorum_headers(
                self.do_view_change_from_all_replicas,
                .{
                    .quorum_nack_prepare = self.quorum_nack_prepare,
                    .quorum_view_change = self.quorum_view_change,
                    .replica_count = self.replica_count,
                },
            );
            const op_head = switch (headers) {
                .awaiting_quorum => {
                    log.debug("{}: on_do_view_change: view={} waiting for quorum", .{
                        self.replica,
                        self.view,
                    });
                    return;
                },
                .awaiting_repair => {
                    log.warn("{}: on_do_view_change: view={} quorum received, awaiting repair", .{
                        self.replica,
                        self.view,
                    });
                    self.primary_log_do_view_change_quorum("on_do_view_change");
                    return;
                },
                .complete_invalid => {
                    log.err("{}: on_do_view_change: view={} quorum received, deadlocked", .{
                        self.replica,
                        self.view,
                    });
                    self.primary_log_do_view_change_quorum("on_do_view_change");
                    return;
                },
                .complete_valid => |*quorum_headers| quorum_headers.next().?.op,
            };

            log.debug("{}: on_do_view_change: view={} quorum received", .{
                self.replica,
                self.view,
            });
            self.primary_log_do_view_change_quorum("on_do_view_change");

            const op_checkpoint_max =
                DVCQuorum.op_checkpoint_max(self.do_view_change_from_all_replicas);
            if (op_checkpoint_max > self.op_checkpoint() and op_head > self.op_prepare_max()) {
                // When:
                // 1. the cluster is at a checkpoint ahead of the local checkpoint,
                // 2. AND the quorum's head op is part of a future checkpoint,
                //    abdicate as primary, jumping to a new view.
                //
                // - If 1 and ¬2, then the ops in the next checkpoint are uncommitted and our
                //   current checkpoint is sufficient.
                // - If 2 and ¬1, then the ops past the checkpoint's op-prepare-max must be
                //   uncommitted because no replica in the DVC quorum even prepared them.
                //   (They are present anyway because the origin of the DVC is reusing its SV
                //   headers as a DVC since it has not completed repair.)
                //
                // This serves a few purposes:
                // 1. Availability: We pick a primary to minimize the number of WAL repairs, to
                //    minimize the likelihood of a repair-deadlock.
                // 2. Optimization: The cluster does not need to wait for a lagging replicas before
                //    prepares/commits can resume.
                // 3. Simplify repair: A new primary never needs to fast-forward to a new checkpoint.

                // As an optimization, jump directly to a view where the primary will have the
                // cluster's latest checkpoint.
                var v: u32 = 1;
                const next_view = while (v < self.replica_count) : (v += 1) {
                    const next_view = self.view + v;
                    const next_primary = self.primary_index(next_view);
                    assert(next_primary != self.replica);

                    if (self.do_view_change_from_all_replicas[next_primary]) |dvc| {
                        assert(dvc.header.replica == next_primary);

                        const dvc_checkpoint = dvc.header.checkpoint_op;
                        if (dvc_checkpoint == op_checkpoint_max) break next_view;
                    }
                } else unreachable;

                log.debug("{}: on_do_view_change: lagging primary; forfeiting " ++
                    "(view={}..{} checkpoint={}..{})", .{
                    self.replica,
                    self.view,
                    next_view,
                    self.op_checkpoint(),
                    op_checkpoint_max,
                });
                self.transition_to_view_change_status(next_view);
            } else {
                assert(!self.do_view_change_quorum);
                self.do_view_change_quorum = true;

                self.primary_set_log_from_do_view_change_messages();
                // We aren't status=normal yet, but our headers from our prior log_view may have
                // been replaced. If we participate in another DVC (before reaching status=normal,
                // which would update our log_view), we must disambiguate our (new) headers from the
                // headers of any other replica with the same log_view so that the next primary can
                // identify an unambiguous set of canonical headers.
                self.log_view = self.view;

                assert(self.op == op_head);
                assert(self.op >= self.commit_max);
                assert(self.state_machine.prepare_timestamp >=
                    self.journal.header_with_op(self.op).?.timestamp);

                // Start repairs according to the CTRL protocol:
                assert(!self.repair_timeout.ticking);
                self.repair_timeout.start();
                self.repair();
            }
        }

        /// When other replicas receive the start_view message, they replace their log with the one
        /// in the message, set their op number to that of the latest entry in the log, set their
        /// view number to the view number in the message, change their status to normal, and update
        /// the information in their client table. If there are non-committed operations in the log,
        /// they send a ⟨prepare_ok v, n, i⟩ message to the primary; here n is the op-number. Then
        /// they execute all operations known to be committed that they haven’t executed previously,
        /// advance their commit number, and update the information in their client table.
        fn on_start_view(self: *Self, message: *const Message.StartView) void {
            assert(message.header.command == .start_view);
            if (self.ignore_view_change_message(message.base_const())) return;

            assert(self.status == .view_change or
                self.status == .normal or
                self.status == .recovering_head);
            assert(message.header.view >= self.view);
            assert(message.header.replica != self.replica);
            assert(message.header.replica == self.primary_index(message.header.view));
            assert(message.header.commit >= message.header.checkpoint_op);
            assert(message.header.commit - message.header.checkpoint_op <=
                constants.vsr_checkpoint_interval + constants.lsm_batch_multiple);
            assert(message.header.op >= message.header.commit);
            assert(message.header.op - message.header.commit <=
                constants.pipeline_prepare_queue_max);

            if (message.header.view == self.log_view and message.header.op < self.op) {
                // We were already in this view prior to receiving the SV.
                assert(self.status == .normal or self.status == .recovering_head);

                log.debug("{}: on_start_view view={} (ignoring, old message)", .{
                    self.replica,
                    self.log_view,
                });
                return;
            }

            if (self.status == .recovering_head) {
                self.view = message.header.view;
                maybe(self.view == self.log_view);
            } else {
                if (self.view < message.header.view) {
                    self.transition_to_view_change_status(message.header.view);
                }

                if (self.status == .normal) {
                    assert(self.backup());
                    assert(self.view == self.log_view);
                }
            }
            assert(self.view == message.header.view);

            const view_headers = message_body_as_view_headers(message.base_const());
            assert(view_headers.command == .start_view);
            assert(view_headers.slice[0].op == message.header.op);

            for (view_headers.slice) |*header| {
                assert(header.commit <= message.header.commit);

                if (header.op <= self.op_prepare_max()) {
                    if (self.log_view < self.view or
                        (self.log_view == self.view and header.op >= self.op))
                    {
                        self.set_op_and_commit_max(header.op, message.header.commit, @src());
                        assert(self.op == header.op);
                        assert(self.commit_max >= message.header.commit);
                        break;
                    }
                }
            } else {
                // This replica is too far behind, i.e. the new `self.op` is too far ahead of
                // the last checkpoint. If we wrap now, we overwrite un-checkpointed transfers
                // in the WAL, precluding recovery.
                if (self.syncing == .idle) {
                    log.warn("{}: on_start_view: start sync; lagging behind cluster " ++
                        "(op_prepare_max={} quorum_head={})", .{
                        self.replica,
                        self.op_prepare_max(),
                        view_headers.slice[0].op,
                    });
                    self.sync_start_from_committing();
                }
                return;
            }

            for (view_headers.slice) |*header| {
                if (header.op <= self.op_prepare_max()) {
                    self.replace_header(header);
                }
            }

            if (self.status != .normal) {
                self.view_headers.replace(.start_view, view_headers.slice);
                assert(self.view_headers.array.get(0).view <= self.view);
                assert(self.view_headers.array.get(0).op == message.header.op);
                maybe(self.view_headers.array.get(0).op > self.op_prepare_max());
                assert(self.view_headers.array.get(self.view_headers.array.count() - 1).op <=
                    self.op_prepare_max());
            }

            switch (self.status) {
                .view_change => {
                    self.transition_to_normal_from_view_change_status(message.header.view);
                    self.send_prepare_oks_after_view_change();
                    self.commit_journal();
                },
                .recovering_head => {
                    self.transition_to_normal_from_recovering_head_status(message.header.view);
                    self.commit_journal();
                },
                .normal => {},
                .recovering => unreachable,
            }

            assert(self.status == .normal);
            assert(message.header.view == self.log_view);
            assert(message.header.view == self.view);
            assert(self.backup());

            self.repair();
        }

        fn on_request_start_view(
            self: *Self,
            message: *const Message.RequestStartView,
        ) void {
            assert(message.header.command == .request_start_view);
            if (self.ignore_repair_message(message.base_const())) return;

            assert(self.status == .normal);
            assert(self.view == self.log_view);
            assert(message.header.view == self.view);
            assert(message.header.replica != self.replica);
            assert(self.primary());

            const start_view = self.create_start_view_message(message.header.nonce);
            defer self.message_bus.unref(start_view);

            assert(start_view.references == 1);
            assert(start_view.header.command == .start_view);
            assert(start_view.header.view == self.view);
            assert(start_view.header.op == self.op);
            assert(start_view.header.commit == self.commit_max);

            self.send_message_to_replica(message.header.replica, start_view);
        }

        /// If the requested prepare has been guaranteed by this replica:
        /// * Read the prepare from storage, and forward it to the replica that requested it.
        /// * Otherwise send no reply — it isn't safe to nack.
        /// If the requested prepare has *not* been guaranteed by this replica, then send a nack.
        ///
        /// A prepare is considered "guaranteed" by a replica if that replica has acknowledged it
        /// to the cluster. The cluster sees the replica as an underwriter of a guaranteed
        /// prepare. If a guaranteed prepare is found to by faulty, the replica must repair it
        /// to restore durability.
        fn on_request_prepare(self: *Self, message: *const Message.RequestPrepare) void {
            assert(message.header.command == .request_prepare);
            if (self.ignore_repair_message(message.base_const())) return;

            assert(self.node_count > 1);
            maybe(self.status == .recovering_head);
            assert(message.header.replica != self.replica);

            const op = message.header.prepare_op;
            const slot = self.journal.slot_for_op(op);
            const checksum = message.header.prepare_checksum;

            // Try to serve the message directly from the pipeline.
            // This saves us from going to disk. And we don't need to worry that the WAL's copy
            // of an uncommitted prepare is lost/corrupted.
            if (self.pipeline_prepare_by_op_and_checksum(op, checksum)) |prepare| {
                log.debug("{}: on_request_prepare: op={} checksum={} reply from pipeline", .{
                    self.replica,
                    op,
                    checksum,
                });
                self.send_message_to_replica(message.header.replica, prepare);
                return;
            }

            if (self.journal.prepare_inhabited[slot.index]) {
                const prepare_checksum = self.journal.prepare_checksums[slot.index];
                // Consult `journal.prepare_checksums` (rather than `journal.headers`):
                // the former may have the prepare we want — even if journal recovery marked the
                // slot as faulty and left the in-memory header as reserved.
                if (checksum == prepare_checksum) {
                    log.debug("{}: on_request_prepare: op={} checksum={} reading", .{
                        self.replica,
                        op,
                        checksum,
                    });

                    // Improve availability by calling `read_prepare_with_op_and_checksum` instead
                    // of `read_prepare` — even if `journal.headers` contains the target message.
                    // The latter skips the read when the target prepare is present but dirty (e.g.
                    // it was recovered with decision=fix).
                    // TODO Do not reissue the read if we are already reading in order to send to
                    // this particular destination replica.
                    self.journal.read_prepare_with_op_and_checksum(
                        on_request_prepare_read,
                        op,
                        prepare_checksum,
                        message.header.replica,
                    );
                    return;
                }
            }

            log.debug("{}: on_request_prepare: op={} checksum={} missing", .{
                self.replica,
                op,
                checksum,
            });
        }

        fn on_request_prepare_read(
            self: *Self,
            prepare: ?*Message.Prepare,
            destination_replica: ?u8,
        ) void {
            const message = prepare orelse {
                log.debug("{}: on_request_prepare_read: prepare=null", .{self.replica});
                return;
            };
            assert(message.header.command == .prepare);

            log.debug("{}: on_request_prepare_read: op={} checksum={} sending to replica={}", .{
                self.replica,
                message.header.op,
                message.header.checksum,
                destination_replica.?,
            });

            assert(destination_replica.? != self.replica);
            self.send_message_to_replica(destination_replica.?, message);
        }

        fn on_request_headers(self: *Self, message: *const Message.RequestHeaders) void {
            assert(message.header.command == .request_headers);
            if (self.ignore_repair_message(message.base_const())) return;

            maybe(self.status == .recovering_head);
            assert(message.header.replica != self.replica);

            const response = self.message_bus.get_message(.headers);
            defer self.message_bus.unref(response);

            response.header.* = .{
                .command = .headers,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
            };

            const op_min = message.header.op_min;
            const op_max = message.header.op_max;
            assert(op_max >= op_min);

            // We must add 1 because op_max and op_min are both inclusive:
            const count_max: usize = @min(constants.request_headers_max, op_max - op_min + 1);
            assert(count_max * @sizeOf(vsr.Header) <= constants.message_body_size_max);

            const count = self.journal.copy_latest_headers_between(
                op_min,
                op_max,
                std.mem.bytesAsSlice(
                    Header.Prepare,
                    response.buffer[@sizeOf(Header)..][0 .. @sizeOf(Header) * count_max],
                ),
            );
            assert(count <= count_max);

            if (count == 0) {
                log.debug("{}: on_request_headers: ignoring (op={}..{}, no headers)", .{
                    self.replica,
                    op_min,
                    op_max,
                });
                return;
            }

            response.header.size = @intCast(@sizeOf(Header) * (1 + count));
            response.header.set_checksum_body(response.body());
            response.header.set_checksum();

            // Assert that the headers are valid.
            _ = message_body_as_prepare_headers(response.base_const());

            self.send_message_to_replica(message.header.replica, response);
        }

        fn on_request_reply(self: *Self, message: *const Message.RequestReply) void {
            assert(message.header.command == .request_reply);
            assert(message.header.reply_client != 0);

            if (self.ignore_repair_message(message.base_const())) return;
            assert(message.header.replica != self.replica);

            const entry = self.client_sessions.get(message.header.reply_client) orelse {
                log.debug("{}: on_request_reply: ignoring, client not in table", .{self.replica});
                return;
            };
            assert(entry.header.client == message.header.reply_client);

            if (entry.header.checksum != message.header.reply_checksum) {
                log.debug("{}: on_request_reply: ignoring, reply not in table " ++
                    "(requested={} stored={})", .{
                    self.replica,
                    message.header.reply_checksum,
                    entry.header.checksum,
                });
                return;
            }
            assert(entry.header.size != @sizeOf(Header));
            assert(entry.header.op == message.header.reply_op);

            const slot = self.client_sessions.get_slot_for_header(&entry.header).?;
            if (self.client_replies.read_reply_sync(slot, entry)) |reply| {
                on_request_reply_read_callback(
                    &self.client_replies,
                    &entry.header,
                    reply,
                    message.header.replica,
                );
            } else {
                self.client_replies.read_reply(
                    slot,
                    entry,
                    on_request_reply_read_callback,
                    message.header.replica,
                ) catch |err| {
                    assert(err == error.Busy);

                    log.debug("{}: on_request_reply: ignoring, client_replies busy", .{
                        self.replica,
                    });
                };
            }
        }

        fn on_request_reply_read_callback(
            client_replies: *ClientReplies,
            reply_header: *const Header.Reply,
            reply_: ?*Message.Reply,
            destination_replica: ?u8,
        ) void {
            const self = @fieldParentPtr(Self, "client_replies", client_replies);
            const reply = reply_ orelse {
                log.debug("{}: on_request_reply: reply not found for replica={} (checksum={})", .{
                    self.replica,
                    destination_replica.?,
                    reply_header.checksum,
                });

                if (self.client_sessions.get_slot_for_header(reply_header)) |slot| {
                    self.client_replies.faulty.set(slot.index);
                }
                return;
            };

            assert(reply.header.command == .reply);
            assert(reply.header.checksum == reply_header.checksum);

            log.debug("{}: on_request_reply: sending reply to replica={} (checksum={})", .{
                self.replica,
                destination_replica.?,
                reply_header.checksum,
            });

            self.send_message_to_replica(destination_replica.?, reply);
        }

        fn on_headers(self: *Self, message: *const Message.Headers) void {
            assert(message.header.command == .headers);
            if (self.ignore_repair_message(message.base_const())) return;

            assert(self.status == .normal or self.status == .view_change);
            maybe(message.header.view == self.view);
            assert(message.header.replica != self.replica);

            // We expect at least one header in the body, or otherwise no response to our request.
            assert(message.header.size > @sizeOf(Header));

            var op_min: ?u64 = null;
            var op_max: ?u64 = null;
            for (message_body_as_prepare_headers(message.base_const())) |*h| {
                if (op_min == null or h.op < op_min.?) op_min = h.op;
                if (op_max == null or h.op > op_max.?) op_max = h.op;
                _ = self.repair_header(h);
            }
            assert(op_max.? >= op_min.?);

            self.repair();
        }

        fn on_request_blocks(self: *Self, message: *const Message.RequestBlocks) void {
            assert(message.header.command == .request_blocks);

            if (message.header.replica == self.replica) {
                log.warn("{}: on_request_blocks: ignoring; misdirected message (self)", .{self.replica});
                return;
            }

            if (self.standby()) {
                log.warn("{}: on_request_blocks: ignoring; misdirected message (standby)", .{self.replica});
                return;
            }

            if (self.grid.callback == .cancel) {
                log.debug("{}: on_request_blocks: ignoring; canceling grid", .{self.replica});
                return;
            }

            // TODO Rate limit replicas that keep requesting the same blocks (maybe via checksum_body?)
            // to avoid unnecessary work in the presence of an asymmetric partition.
            const requests = std.mem.bytesAsSlice(vsr.BlockRequest, message.body());
            assert(requests.len > 0);

            next_request: for (requests, 0..) |*request, i| {
                assert(stdx.zeroed(&request.reserved));

                var reads = self.grid_reads.iterate();
                while (reads.next()) |read| {
                    if (read.read.address == request.block_address and
                        read.read.checksum == request.block_checksum and
                        read.destination == message.header.replica)
                    {
                        log.debug("{}: on_request_blocks: ignoring block request;" ++
                            " already reading (destination={} address={} checksum={})", .{
                            self.replica,
                            message.header.replica,
                            request.block_address,
                            request.block_checksum,
                        });
                        continue :next_request;
                    }
                }

                const read = self.grid_reads.acquire() orelse {
                    log.debug("{}: on_request_blocks: ignoring remaining blocks; busy " ++
                        "(replica={} ignored={}/{})", .{
                        self.replica,
                        message.header.replica,
                        requests.len - i,
                        requests.len,
                    });
                    return;
                };

                log.debug("{}: on_request_blocks: reading block " ++
                    "(replica={} address={} checksum={})", .{
                    self.replica,
                    message.header.replica,
                    request.block_address,
                    request.block_checksum,
                });

                const reply = self.message_bus.get_message(.block);
                defer self.message_bus.unref(reply);

                read.* = .{
                    .replica = self,
                    .destination = message.header.replica,
                    .read = undefined,
                    .message = reply.ref(),
                };

                self.grid.read_block(
                    .{ .from_local_storage = on_request_blocks_read_block },
                    &read.read,
                    request.block_address,
                    request.block_checksum,
                    .{ .cache_read = true, .cache_write = false },
                );
            }
        }

        fn on_request_blocks_read_block(
            grid_read: *Grid.Read,
            result: Grid.ReadBlockResult,
        ) void {
            const read = @fieldParentPtr(BlockRead, "read", grid_read);
            const self = read.replica;
            defer {
                self.message_bus.unref(read.message);
                self.grid_reads.release(read);
            }

            assert(read.destination != self.replica);

            if (result != .valid) {
                log.debug("{}: on_request_blocks: error: {s}: " ++
                    "(destination={} address={} checksum={})", .{
                    self.replica,
                    @tagName(result),
                    read.destination,
                    grid_read.address,
                    grid_read.checksum,
                });
                return;
            }

            log.debug("{}: on_request_blocks: success: (destination={} address={} checksum={})", .{
                self.replica,
                read.destination,
                grid_read.address,
                grid_read.checksum,
            });

            stdx.copy_disjoint(.inexact, u8, read.message.buffer, result.valid);

            assert(read.message.header.command == .block);
            assert(read.message.header.address == grid_read.address);
            assert(read.message.header.checksum == grid_read.checksum);
            assert(read.message.header.size <= constants.block_size);

            self.send_message_to_replica(read.destination, read.message);
        }

        fn on_block(self: *Self, message: *const Message.Block) void {
            maybe(self.state_machine_opened);
            assert(message.header.command == .block);
            assert(message.header.size <= constants.block_size);
            assert(message.header.address > 0);

            if (self.grid.callback == .cancel) {
                assert(self.grid.read_global_queue.count == 0);

                log.debug("{}: on_block: ignoring; grid is canceling (address={} checksum={})", .{
                    self.replica,
                    message.header.address,
                    message.header.checksum,
                });
                return;
            }

            const block = message.buffer[0..constants.block_size];

            const grid_fulfill = self.grid.fulfill_block(block);
            if (grid_fulfill) {
                assert(!self.grid.free_set.is_free(message.header.address));

                log.debug("{}: on_block: fulfilled address={} checksum={}", .{
                    self.replica,
                    message.header.address,
                    message.header.checksum,
                });
            }

            const grid_repair =
                self.grid.repair_block_waiting(message.header.address, message.header.checksum);
            if (grid_repair) {
                assert(!self.grid.free_set.is_free(message.header.address));

                if (self.grid_repair_writes.acquire()) |write| {
                    const write_index = self.grid_repair_writes.index(write);
                    const write_block = &self.grid_repair_write_blocks[write_index];

                    log.debug("{}: on_block: repairing address={} checksum={}", .{
                        self.replica,
                        message.header.address,
                        message.header.checksum,
                    });

                    stdx.copy_disjoint(
                        .inexact,
                        u8,
                        write_block.*,
                        message.buffer[0..message.header.size],
                    );

                    write.* = .{ .replica = self };
                    self.grid.repair_block(grid_repair_block_callback, &write.write, write_block);
                } else {
                    log.debug("{}: on_block: ignoring; no write available " ++
                        "(address={} checksum={})", .{
                        self.replica,
                        message.header.address,
                        message.header.checksum,
                    });
                }
            }

            if (!grid_fulfill and !grid_repair) {
                log.debug("{}: on_block: ignoring; block not needed (address={} checksum={})", .{
                    self.replica,
                    message.header.address,
                    message.header.checksum,
                });
            }
        }

        fn grid_repair_block_callback(grid_write: *Grid.Write) void {
            const write = @fieldParentPtr(BlockWrite, "write", grid_write);
            const self = write.replica;
            defer self.grid_repair_writes.release(write);

            log.debug("{}: on_block: repair done address={}", .{
                self.replica,
                grid_write.address,
            });

            self.sync_reclaim_tables();
        }

        fn on_request_sync_checkpoint(
            self: *Self,
            message: *Message.RequestSyncCheckpoint,
        ) void {
            assert(message.header.command == .request_sync_checkpoint);
            if (self.ignore_request_sync_checkpoint_message(message)) return;

            assert(message.header.checkpoint_op ==
                self.superblock.staging.vsr_state.checkpoint.commit_min);
            assert(message.header.checkpoint_id == self.superblock.staging.checkpoint_id());

            self.send_sync_checkpoint(.{ .replica = message.header.replica });
        }

        fn on_sync_checkpoint(self: *Self, message: *const Message.SyncCheckpoint) void {
            assert(message.header.replica < self.replica_count);
            assert(message.header.command == .sync_checkpoint);
            if (self.ignore_sync_checkpoint_message(message)) return;

            const stage: *const SyncStage.RequestingCheckpoint =
                &self.syncing.requesting_checkpoint;
            assert(stage.target.checkpoint_id == message.header.checkpoint_id);
            assert(stage.target.checkpoint_id == message.header.checksum_body);

            log.debug("{}: on_sync_checkpoint: checkpoint_op={} checkpoint_id={x:0>32}", .{
                self.replica,
                stage.target.checkpoint_op,
                stage.target.checkpoint_id,
            });

            self.sync_requesting_checkpoint_callback(std.mem.bytesAsValue(
                vsr.CheckpointState,
                message.body()[0..@sizeOf(vsr.CheckpointState)],
            ));
        }

        fn on_ping_timeout(self: *Self) void {
            self.ping_timeout.reset();

            var ping = Header.Ping{
                .command = .ping,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view_durable(), // Don't drop pings while the view is being updated.
                .checkpoint_id = self.superblock.working.checkpoint_id(),
                .checkpoint_op = self.op_checkpoint(),
                .ping_timestamp_monotonic = self.clock.monotonic(),
            };
            assert(ping.view <= self.view);

            self.send_header_to_other_replicas_and_standbys(ping.frame_const().*);
        }

        fn on_prepare_timeout(self: *Self) void {
            // We will decide below whether to reset or backoff the timeout.
            assert(self.status == .normal);
            assert(self.primary());
            assert(self.prepare_timeout.ticking);

            const prepare = self.primary_pipeline_pending().?;

            if (self.solo()) {
                // Replica=1 doesn't write prepares concurrently to avoid gaps in its WAL.
                assert(self.journal.writes.executing() <= 1);
                assert(self.journal.writes.executing() == 1 or
                    self.commit_stage != .idle or
                    self.client_replies.writes.executing() > 0);

                self.prepare_timeout.reset();
                return;
            }

            // The list of remote replicas yet to send a prepare_ok:
            var waiting: [constants.replicas_max]u8 = undefined;
            var waiting_len: usize = 0;
            var ok_from_all_replicas_iterator = prepare.ok_from_all_replicas.iterator(.{
                .kind = .unset,
            });
            while (ok_from_all_replicas_iterator.next()) |replica| {
                // Ensure we don't wait for replicas that don't exist.
                // The bits between `replica_count` and `replicas_max` are always unset,
                // since they don't actually represent replicas.
                if (replica == self.replica_count) {
                    assert(self.replica_count < constants.replicas_max);
                    break;
                }
                assert(replica < self.replica_count);

                if (replica != self.replica) {
                    waiting[waiting_len] = @intCast(replica);
                    waiting_len += 1;
                }
            } else {
                assert(self.replica_count == constants.replicas_max);
            }

            if (waiting_len == 0) {
                assert(self.quorum_replication == self.replica_count);
                assert(!prepare.ok_from_all_replicas.isSet(self.replica));
                assert(prepare.ok_from_all_replicas.count() == self.replica_count - 1);
                assert(prepare.message.header.op <= self.op);

                self.prepare_timeout.reset();
                log.debug("{}: on_prepare_timeout: waiting for journal", .{self.replica});

                // We may be slow and waiting for the write to complete.
                //
                // We may even have maxed out our IO depth and been unable to initiate the write,
                // which can happen if `constants.pipeline_prepare_queue_max` exceeds
                // `constants.journal_iops_write_max`. This can lead to deadlock for a cluster of
                // one or two (if we do not retry here), since there is no other way for the primary
                // to repair the dirty op because no other replica has it.
                //
                // Retry the write through `on_repair()` which will work out which is which.
                // We do expect that the op would have been run through `on_prepare()` already.
                self.on_repair(prepare.message);
                return;
            }

            self.prepare_timeout.backoff(self.prng.random());

            assert(waiting_len <= self.replica_count);
            for (waiting[0..waiting_len]) |replica| {
                assert(replica < self.replica_count);

                log.debug("{}: on_prepare_timeout: waiting for replica {}", .{
                    self.replica,
                    replica,
                });
            }

            // Cycle through the list to reach live replicas and get around partitions:
            // We do not assert `prepare_timeout.attempts > 0` since the counter may wrap back to 0.
            const replica = waiting[self.prepare_timeout.attempts % waiting_len];
            assert(replica != self.replica);

            log.debug("{}: on_prepare_timeout: replicating to replica {}", .{
                self.replica,
                replica,
            });
            self.send_message_to_replica(replica, prepare.message);
        }

        fn on_primary_abdicate_timeout(self: *Self) void {
            assert(self.status == .normal);
            assert(self.primary());
            assert(self.primary_pipeline_pending() != null);
            self.primary_abdicate_timeout.reset();
            if (self.solo()) return;

            log.debug("{}: on_primary_abdicate_timeout: abdicating (view={})", .{
                self.replica,
                self.view,
            });
            self.primary_abdicating = true;
        }

        fn on_commit_message_timeout(self: *Self) void {
            self.commit_message_timeout.reset();

            assert(self.status == .normal);
            assert(self.primary());
            assert(self.commit_min == self.commit_max);

            if (self.primary_abdicating) {
                assert(self.primary_abdicate_timeout.ticking);
                assert(self.pipeline.queue.prepare_queue.count > 0);
                assert(self.primary_pipeline_pending() != null);

                log.debug("{}: on_commit_message_timeout: primary abdicating (view={})", .{
                    self.replica,
                    self.view,
                });
                return;
            }

            const latest_committed_entry = checksum: {
                if (self.commit_max == self.superblock.working.vsr_state.checkpoint.commit_min) {
                    break :checksum self.superblock.working.vsr_state.checkpoint.commit_min_checksum;
                } else {
                    break :checksum self.journal.header_with_op(self.commit_max).?.checksum;
                }
            };

            self.send_header_to_other_replicas_and_standbys(@bitCast(Header.Commit{
                .command = .commit,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
                .commit = self.commit_max,
                .commit_checksum = latest_committed_entry,
                .timestamp_monotonic = self.clock.monotonic(),
                .checkpoint_op = self.superblock.working.vsr_state.checkpoint.commit_min,
                .checkpoint_id = self.superblock.working.checkpoint_id(),
            }));
        }

        fn on_normal_heartbeat_timeout(self: *Self) void {
            assert(self.status == .normal);
            assert(self.backup());
            self.normal_heartbeat_timeout.reset();

            if (self.solo()) return;

            log.debug("{}: on_normal_heartbeat_timeout: heartbeat lost (view={})", .{
                self.replica,
                self.view,
            });
            self.send_start_view_change();
        }

        fn on_start_view_change_window_timeout(self: *Self) void {
            assert(self.status == .normal or self.status == .view_change);
            assert(self.start_view_change_from_all_replicas.count() > 0);
            assert(!self.solo());
            self.start_view_change_window_timeout.stop();

            if (self.standby()) return;

            // Don't reset our own SVC; it will be reset if/when we receive a heartbeat.
            const svc = self.start_view_change_from_all_replicas.isSet(self.replica);
            self.reset_quorum_start_view_change();
            if (svc) self.start_view_change_from_all_replicas.set(self.replica);
        }

        fn on_start_view_change_message_timeout(self: *Self) void {
            assert(self.status == .normal or self.status == .view_change);
            self.start_view_change_message_timeout.reset();

            if (self.solo()) return;
            if (self.standby()) return;

            if (self.start_view_change_from_all_replicas.isSet(self.replica)) {
                self.send_start_view_change();
            }
        }

        fn on_view_change_status_timeout(self: *Self) void {
            assert(self.status == .view_change);
            assert(!self.solo());
            self.view_change_status_timeout.reset();

            self.send_start_view_change();
        }

        fn on_do_view_change_message_timeout(self: *Self) void {
            assert(self.status == .view_change);
            assert(!self.solo());
            self.do_view_change_message_timeout.reset();

            if (self.primary_index(self.view) == self.replica and self.do_view_change_quorum) {
                // A primary in status=view_change with a complete DVC quorum must be repairing —
                // it does not need to signal other replicas.
                assert(self.view == self.log_view);
            } else {
                assert(self.view > self.log_view);
                self.send_do_view_change();
            }
        }

        fn on_request_start_view_message_timeout(self: *Self) void {
            assert(self.status == .view_change);
            assert(self.primary_index(self.view) != self.replica);
            self.request_start_view_message_timeout.reset();

            log.debug("{}: on_request_start_view_message_timeout: view={}", .{
                self.replica,
                self.view,
            });
            self.send_header_to_replica(
                self.primary_index(self.view),
                @bitCast(Header.RequestStartView{
                    .command = .request_start_view,
                    .cluster = self.cluster,
                    .replica = self.replica,
                    .view = self.view,
                    .nonce = self.nonce,
                }),
            );
        }

        fn on_repair_timeout(self: *Self) void {
            assert(self.status == .normal or self.status == .view_change);
            self.repair();
        }

        fn on_repair_sync_timeout(self: *Self) void {
            assert(!self.solo());
            assert(self.status == .normal);
            assert(self.backup());
            assert(self.repair_sync_timeout.ticking);

            self.repair_sync_timeout.reset();

            if (self.syncing != .idle) return;
            // May as well wait for an in-progress checkpoint to complete —
            // we would need to wait for it before sync starts anyhow, and the newer
            // checkpoint might sidestep the need for sync anyhow.
            if (self.commit_stage == .checkpoint_superblock) return;
            if (self.commit_stage == .checkpoint_data) return;

            // TODO Test connectivity to cluster to rule out a network partition.

            // Even if we want to transition to sync, there is no point if we don't have
            // a target yet (or if our latest target would not advance our state).
            if (self.sync_target_max == null) return;
            if (self.sync_target_max.?.checkpoint_op <= self.op_checkpoint()) return;

            {
                const commit_next = self.commit_min + 1;
                const commit_next_slot = self.journal.slot_with_op(commit_next);

                // "stuck" is not actually certain, merely likely (see below).
                const stuck_header = !self.valid_hash_chain(@src());

                const stuck_prepare =
                    (commit_next_slot == null or self.journal.dirty.bit(commit_next_slot.?));

                const stuck_grid = !self.grid.read_global_queue.empty();

                if (!stuck_header and !stuck_prepare and !stuck_grid) return;
            }

            // At this point:
            // - we know that the cluster committed atop of a future a checkpoint, and
            // - we have not made any progress committing for some interval.
            // It is possible that we could in fact repair and progress anyway, but this is not
            // guaranteed: the rest of the cluster no longer repairs the relevant prepares.
            // We might be stuck waiting for a prepare or block which will never arrive.

            log.warn("{}: on_repair_sync_timeout: start sync; lagging behind cluster " ++
                "(op_head={} commit_min={} commit_max={} commit_stage={s})", .{
                self.replica,
                self.op,
                self.commit_min,
                self.commit_max,
                @tagName(self.commit_stage),
            });

            // It is possible that we trigger state sync even though we could have repaired.
            // For example, if a block we are trying to repair keeps getting dropped by the network.
            self.sync_start_from_committing();
        }

        fn on_grid_repair_message_timeout(self: *Self) void {
            assert(self.grid_repair_message_timeout.ticking);
            maybe(self.state_machine_opened);

            self.grid_repair_message_timeout.reset();
            if (self.grid.callback != .cancel) {
                self.send_request_blocks();
            }
        }

        fn on_sync_message_timeout(self: *Self) void {
            assert(!self.solo());
            assert(self.syncing != .idle);
            assert(self.sync_message_timeout.ticking);

            self.sync_message_timeout.reset();

            switch (self.syncing) {
                .idle => unreachable,
                .canceling_commit,
                .canceling_grid,
                .requesting_target,
                .updating_superblock,
                => return,

                .requesting_checkpoint => self.send_request_sync_checkpoint(),
            }
        }

        fn primary_receive_do_view_change(
            self: *Self,
            message: *Message.DoViewChange,
        ) void {
            assert(!self.solo());
            assert(self.status == .view_change);
            assert(self.primary_index(self.view) == self.replica);
            assert(self.do_view_change_from_all_replicas.len == constants.replicas_max);

            assert(message.header.command == .do_view_change);
            assert(message.header.cluster == self.cluster);
            assert(message.header.replica < self.replica_count);
            assert(message.header.view == self.view);

            const command: []const u8 = @tagName(message.header.command);

            if (self.do_view_change_from_all_replicas[message.header.replica]) |m| {
                // Assert that this is a duplicate message and not a different message:
                assert(m.header.command == message.header.command);
                assert(m.header.replica == message.header.replica);
                assert(m.header.view == message.header.view);
                assert(m.header.op == message.header.op);
                assert(m.header.checksum_body == message.header.checksum_body);

                // Replicas don't resend `do_view_change` messages to themselves.
                assert(message.header.replica != self.replica);
                // A replica may resend a `do_view_change` with a different checkpoint or commit
                // if it was checkpointing/committing originally.
                // Keep the one with the highest checkpoint, then commit.
                // This is *not* necessary for correctness.
                if (m.header.checkpoint_op < message.header.checkpoint_op or
                    (m.header.checkpoint_op == message.header.checkpoint_op and
                    m.header.commit_min < message.header.commit_min))
                {
                    log.debug("{}: on_{s}: replacing " ++
                        "(newer message replica={} checkpoint={}..{} commit={}..{})", .{
                        self.replica,
                        command,
                        message.header.replica,
                        m.header.checkpoint_op,
                        message.header.checkpoint_op,
                        m.header.commit_min,
                        message.header.commit_min,
                    });
                    // TODO(Buggify): skip updating the DVC, since it isn't required for correctness.
                    self.message_bus.unref(m);
                    self.do_view_change_from_all_replicas[message.header.replica] = message.ref();
                } else if (m.header.checkpoint_op != message.header.checkpoint_op or
                    m.header.commit_min != message.header.commit_min or
                    m.header.nack_bitset != message.header.nack_bitset or
                    m.header.present_bitset != message.header.present_bitset)
                {
                    log.debug("{}: on_{s}: ignoring (older message replica={})", .{
                        self.replica,
                        command,
                        message.header.replica,
                    });
                } else {
                    assert(m.header.checksum == message.header.checksum);
                }

                log.debug("{}: on_{s}: ignoring (duplicate message replica={})", .{
                    self.replica,
                    command,
                    message.header.replica,
                });
            } else {
                // Record the first receipt of this message:
                assert(self.do_view_change_from_all_replicas[message.header.replica] == null);
                self.do_view_change_from_all_replicas[message.header.replica] = message.ref();
            }
        }

        fn count_message_and_receive_quorum_exactly_once(
            self: *Self,
            counter: *QuorumCounter,
            message: *Message.PrepareOk,
            threshold: u32,
        ) ?usize {
            assert(threshold >= 1);
            assert(threshold <= self.replica_count);

            assert(QuorumCounter.bit_length == constants.replicas_max);
            assert(message.header.cluster == self.cluster);
            assert(message.header.replica < self.replica_count);
            assert(message.header.view == self.view);

            switch (message.header.command) {
                .prepare_ok => {
                    if (self.replica_count <= 2) assert(threshold == self.replica_count);

                    assert(self.status == .normal);
                    assert(self.primary());
                },
                else => unreachable,
            }

            const command: []const u8 = @tagName(message.header.command);

            // Do not allow duplicate messages to trigger multiple passes through a state transition:
            if (counter.isSet(message.header.replica)) {
                log.debug("{}: on_{s}: ignoring (duplicate message replica={})", .{
                    self.replica,
                    command,
                    message.header.replica,
                });
                return null;
            }

            // Record the first receipt of this message:
            counter.set(message.header.replica);
            assert(counter.isSet(message.header.replica));

            // Count the number of unique messages now received:
            const count = counter.count();
            log.debug("{}: on_{s}: {} message(s)", .{ self.replica, command, count });
            assert(count <= self.replica_count);

            // Wait until we have exactly `threshold` messages for quorum:
            if (count < threshold) {
                log.debug("{}: on_{s}: waiting for quorum", .{ self.replica, command });
                return null;
            }

            // This is not the first time we have had quorum, the state transition has already happened:
            if (count > threshold) {
                log.debug("{}: on_{s}: ignoring (quorum received already)", .{
                    self.replica,
                    command,
                });
                return null;
            }

            assert(count == threshold);
            return count;
        }

        /// Caller must ensure that:
        /// - op=commit is indeed committed by the cluster,
        /// - local WAL doesn't contain truncated prepares from finished views.
        fn advance_commit_max(self: *Self, commit: u64, source: SourceLocation) void {
            defer {
                assert(self.commit_max >= commit);
                assert(self.commit_max >= self.commit_min);
                assert(self.commit_max >= self.op -| constants.pipeline_prepare_queue_max);
                if (self.status == .normal and self.primary()) {
                    assert(self.commit_max == self.commit_min);
                    assert(self.commit_max == self.op - self.pipeline.queue.prepare_queue.count);
                }
            }

            if (commit > self.commit_max) {
                log.debug("{}: {s}: advancing commit_max={}..{}", .{
                    self.replica,
                    source.fn_name,
                    self.commit_max,
                    commit,
                });
                self.commit_max = commit;
            }
        }

        fn append(self: *Self, message: *Message.Prepare) void {
            assert(self.status == .normal);
            assert(message.header.command == .prepare);
            assert(message.header.operation != .reserved);
            assert(message.header.view == self.view);
            assert(message.header.op == self.op);
            assert(message.header.op <= self.op_prepare_max());

            if (self.solo() and self.pipeline.queue.prepare_queue.count > 1) {
                // In a cluster-of-one, the prepares must always be written to the WAL sequentially
                // (never concurrently). This ensures that there will be no gaps in the WAL during
                // crash recovery.
                log.debug("{}: append: serializing append op={}", .{
                    self.replica,
                    message.header.op,
                });
            } else {
                log.debug("{}: append: appending to journal op={}", .{
                    self.replica,
                    message.header.op,
                });
                self.write_prepare(message, .append);
            }
        }

        /// Returns whether `b` succeeds `a` by having a newer view or same view and newer op.
        fn ascending_viewstamps(
            a: *const Header.Prepare,
            b: *const Header.Prepare,
        ) bool {
            assert(a.command == .prepare);
            assert(b.command == .prepare);
            assert(a.operation != .reserved);
            assert(b.operation != .reserved);

            if (a.view < b.view) {
                // We do not assert b.op >= a.op, ops may be reordered during a view change.
                return true;
            } else if (a.view > b.view) {
                // We do not assert b.op <= a.op, ops may be reordered during a view change.
                return false;
            } else if (a.op < b.op) {
                assert(a.view == b.view);
                return true;
            } else if (a.op > b.op) {
                assert(a.view == b.view);
                return false;
            } else {
                unreachable;
            }
        }

        /// Choose a different replica each time if possible (excluding ourself).
        ///
        /// Currently this picks the target replica at random instead of doing something like
        /// round-robin in order to avoid a resonance.
        fn choose_any_other_replica(self: *Self) u8 {
            assert(!self.solo());
            comptime assert(constants.members_max * 2 < std.math.maxInt(u8));

            // Carefully select any replica if we are a standby,
            // and any different replica if we are active.
            const pool = if (self.standby()) self.replica_count else self.replica_count - 1;
            const shift = self.prng.random().intRangeAtMost(u8, 1, pool);
            const other_replica = @mod(self.replica + shift, self.replica_count);
            assert(other_replica != self.replica);
            return other_replica;
        }

        fn commit_dispatch(self: *Self, stage_new: CommitStage) void {
            assert(self.commit_min <= self.commit_max);
            assert(self.commit_min <= self.op);

            if (self.syncing == .canceling_commit) {
                return self.sync_cancel_commit_callback();
            }
            assert(self.syncing == .idle);

            const stage_old = self.commit_stage;
            assert(stage_old != @as(std.meta.Tag(CommitStage), stage_new));

            self.commit_stage = switch (stage_new) {
                .next => if (self.status == .normal and self.primary())
                    CommitStage.next_pipeline
                else
                    CommitStage.next_journal,
                else => stage_new,
            };

            // Reset the repair-sync timeout anytime that a commit makes progress.
            if (self.commit_stage != .next_journal and
                self.commit_stage != .next_pipeline and
                self.commit_stage != .idle)
            {
                if (self.repair_sync_timeout.ticking) self.repair_sync_timeout.reset();
            }

            log.debug("{}: commit_dispatch: {s}..{s} (commit_min={})", .{
                self.replica,
                @tagName(stage_old),
                @tagName(self.commit_stage),
                self.commit_min,
            });

            switch (self.commit_stage) {
                .next => unreachable,
                .next_journal => self.commit_journal_next(),
                .next_pipeline => self.commit_pipeline_next(),
                .prefetch_state_machine => self.commit_op_prefetch(),
                .setup_client_replies => {
                    if (self.client_replies.ready_sync()) {
                        commit_op_client_replies_ready_callback(&self.client_replies);
                    } else {
                        self.client_replies.ready(commit_op_client_replies_ready_callback);
                    }
                },
                .compact_state_machine => self.state_machine.compact(
                    commit_op_compact_callback,
                    self.commit_prepare.?.header.op,
                ),
                .checkpoint_data => {
                    // For encoding/decoding simplicity, require that the entire ClientSessions fits
                    // in a single block.
                    const chunks = self.client_sessions_checkpoint.encode_chunks();
                    assert(chunks.len == 1);

                    self.client_sessions_checkpoint.size = self.client_sessions.encode(chunks[0]);
                    assert(self.client_sessions_checkpoint.size == ClientSessions.encode_size);

                    self.state_machine.checkpoint(commit_op_checkpoint_state_machine_callback);
                    self.client_sessions_checkpoint.checkpoint(commit_op_checkpoint_client_sessions_callback);
                    self.client_replies.checkpoint(commit_op_checkpoint_client_replies_callback);
                    // The grid checkpoint must begin after the manifest/trailers have acquired all
                    // their blocks, since it encodes the free set:
                    self.grid.checkpoint(commit_op_checkpoint_grid_callback);
                },
                .checkpoint_superblock => self.commit_op_checkpoint_superblock(),
                .cleanup => self.commit_op_cleanup(),
                .idle => assert(self.commit_prepare == null),
            }
        }

        /// Commit ops up to commit_max (inclusive).
        fn commit_journal(self: *Self) void {
            assert(self.status == .normal or self.status == .view_change or
                (self.status == .recovering and self.solo()));
            assert(!(self.status == .normal and self.primary()));
            assert(self.commit_min <= self.commit_max);
            assert(self.commit_min <= self.op);
            maybe(self.commit_max > self.op);

            // We have already committed this far:
            if (self.commit_max == self.commit_min) return;

            if (!self.state_machine_opened) {
                assert(self.commit_stage == .idle);
                return;
            }

            if (self.syncing != .idle) return;

            // Guard against multiple concurrent invocations of commit_journal()/commit_pipeline():
            if (self.commit_stage != .idle) {
                log.debug("{}: commit_journal: already committing ({s}; commit_min={})", .{
                    self.replica,
                    @tagName(self.commit_stage),
                    self.commit_min,
                });
                return;
            }

            // We check the hash chain before we read each op, rather than once upfront, because
            // it's possible for `commit_max` to change while we read asynchronously, after we
            // validate the hash chain.
            //
            // We therefore cannot keep committing until we reach `commit_max`. We need to verify
            // the hash chain before each read. Once verified (before the read) we can commit in the
            // callback after the read, but if we see a change we need to stop committing any
            // further ops, because `commit_max` may have been bumped and may refer to a different
            // op.

            assert(self.commit_stage == .idle);
            self.commit_dispatch(.next_journal);
        }

        fn commit_journal_next(self: *Self) void {
            assert(self.commit_stage == .next_journal);
            assert(self.commit_prepare == null);
            assert(self.status == .normal or self.status == .view_change or
                (self.status == .recovering and self.solo()));
            assert(!(self.status == .normal and self.primary()));
            assert(self.pipeline == .cache);
            assert(self.commit_min <= self.commit_max);
            assert(self.commit_min <= self.op);
            maybe(self.commit_max <= self.op);

            if (!self.valid_hash_chain(@src())) {
                assert(!self.solo());
                self.commit_dispatch(.idle);
                return;
            }

            // We may receive commit numbers for ops we do not yet have (`commit_max > self.op`):
            // Even a naive state sync may fail to correct for this.
            if (self.commit_min < self.commit_max and self.commit_min < self.op) {
                const op = self.commit_min + 1;
                const header = self.journal.header_with_op(op).?;

                if (self.pipeline.cache.prepare_by_op_and_checksum(op, header.checksum)) |prepare| {
                    log.debug("{}: commit_journal_next: cached prepare op={} checksum={}", .{
                        self.replica,
                        op,
                        header.checksum,
                    });
                    self.commit_journal_next_callback(prepare, null);
                } else {
                    self.journal.read_prepare(
                        commit_journal_next_callback,
                        op,
                        header.checksum,
                        null,
                    );
                }
            } else {
                self.commit_dispatch(.idle);
                // This is an optimization to expedite the view change before the `repair_timeout`:
                if (self.status == .view_change and self.repairs_allowed()) self.repair();

                if (self.status == .recovering) {
                    assert(self.solo());
                    assert(self.commit_min == self.commit_max);
                    assert(self.commit_min == self.op);
                    self.transition_to_normal_from_recovering_status();
                } else {
                    // We expect that a cluster-of-one only calls commit_journal() in recovering status.
                    assert(!self.solo());
                }
            }
        }

        fn commit_journal_next_callback(
            self: *Self,
            prepare: ?*Message.Prepare,
            destination_replica: ?u8,
        ) void {
            assert(self.status == .normal or self.status == .view_change or
                (self.status == .recovering and self.solo()));
            assert(self.commit_stage == .next_journal);
            assert(self.commit_prepare == null);
            assert(destination_replica == null);

            if (prepare == null) {
                self.commit_dispatch(.idle);
                log.debug("{}: commit_journal_next_callback: prepare == null", .{self.replica});
                if (self.solo()) @panic("cannot recover corrupt prepare");
                return;
            }

            switch (self.status) {
                .normal => {},
                .view_change => {
                    if (self.primary_index(self.view) != self.replica) {
                        self.commit_dispatch(.idle);
                        log.debug("{}: commit_journal_next_callback: no longer primary view={}", .{
                            self.replica,
                            self.view,
                        });
                        assert(!self.solo());
                        return;
                    }
                    // Only the primary may commit during a view change before starting the new view.
                    // Fall through if this is indeed the case.
                },
                .recovering => {
                    assert(self.solo());
                    assert(self.primary_index(self.view) == self.replica);
                },
                .recovering_head => unreachable,
            }

            const op = self.commit_min + 1;
            assert(prepare.?.header.op == op);

            self.commit_prepare = prepare.?.ref();
            self.commit_dispatch(.prefetch_state_machine);
        }

        /// Commits, frees and pops as many prepares at the head of the pipeline as have quorum.
        /// Can be called only when the replica is the primary.
        /// Can be called only when the pipeline has at least one prepare.
        fn commit_pipeline(self: *Self) void {
            assert(self.status == .normal);
            assert(self.primary());
            assert(self.pipeline.queue.prepare_queue.count > 0);
            assert(self.syncing == .idle);

            if (!self.state_machine_opened) {
                assert(self.commit_stage == .idle);
                return;
            }

            // Guard against multiple concurrent invocations of commit_journal()/commit_pipeline():
            if (self.commit_stage != .idle) {
                log.debug("{}: commit_pipeline: already committing ({s}; commit_min={})", .{
                    self.replica,
                    @tagName(self.commit_stage),
                    self.commit_min,
                });
                return;
            }

            self.commit_dispatch(.next_pipeline);
        }

        fn commit_pipeline_next(self: *Self) void {
            assert(self.commit_stage == .next_pipeline);
            assert(self.status == .normal);
            assert(self.primary());
            assert(self.syncing == .idle);

            if (self.pipeline.queue.prepare_queue.head_ptr()) |prepare| {
                assert(self.commit_min == self.commit_max);
                assert(self.commit_min + 1 == prepare.message.header.op);
                assert(self.commit_min + self.pipeline.queue.prepare_queue.count == self.op);
                assert(self.journal.has(prepare.message.header));

                if (!prepare.ok_quorum_received) {
                    // Eventually handled by on_prepare_timeout().
                    log.debug("{}: commit_pipeline_next: waiting for quorum", .{self.replica});
                    self.commit_dispatch(.idle);
                    return;
                }

                const count = prepare.ok_from_all_replicas.count();
                assert(count >= self.quorum_replication);
                assert(count <= self.replica_count);

                self.commit_prepare = prepare.message.ref();
                self.commit_dispatch(.prefetch_state_machine);
            } else {
                self.commit_dispatch(.idle);
            }
        }

        /// Begin the commit path that is common between `commit_pipeline` and `commit_journal`:
        ///
        /// 1. Prefetch.
        /// 2. Commit_op: Update the state machine and the replica's commit_min/commit_max.
        /// 3. Compact.
        /// 4. Checkpoint: (Only called when `commit_min == op_checkpoint_next_trigger`).
        /// 5. Done. Go to step 1 to repeat for the next op.
        fn commit_op_prefetch(self: *Self) void {
            assert(self.state_machine_opened);
            assert(self.status == .normal or self.status == .view_change or
                (self.status == .recovering and self.solo()));
            assert(self.commit_stage == .prefetch_state_machine);
            assert(self.commit_prepare.?.header.command == .prepare);
            assert(self.commit_prepare.?.header.operation != .root);
            assert(self.commit_prepare.?.header.operation != .reserved);
            assert(self.commit_prepare.?.header.op == self.commit_min + 1);
            assert(self.commit_prepare.?.header.op <= self.op);

            const prepare = self.commit_prepare.?;

            tracer.start(
                &self.tracer_slot_commit,
                .{ .commit = .{ .op = prepare.header.op } },
                @src(),
            );

            if (prepare.header.operation.vsr_reserved()) {
                // NOTE: this inline callback is fine because the next stage of committing,
                // `.setup_client_replies`, is always async.
                commit_op_prefetch_callback(&self.state_machine);
            } else {
                self.state_machine.prefetch(
                    commit_op_prefetch_callback,
                    prepare.header.op,
                    prepare.header.operation.cast(StateMachine),
                    prepare.body(),
                );
            }
        }

        fn commit_op_prefetch_callback(state_machine: *StateMachine) void {
            const self = @fieldParentPtr(Self, "state_machine", state_machine);
            assert(self.commit_stage == .prefetch_state_machine);
            assert(self.commit_prepare != null);
            assert(self.commit_prepare.?.header.op == self.commit_min + 1);

            // Ensure that ClientReplies has at least one Write available.
            maybe(self.client_replies.writes.available() == 0);
            self.commit_dispatch(.setup_client_replies);
        }

        fn commit_op_client_replies_ready_callback(client_replies: *ClientReplies) void {
            const self = @fieldParentPtr(Self, "client_replies", client_replies);
            assert(self.commit_stage == .setup_client_replies);
            assert(self.commit_prepare != null);
            assert(self.commit_prepare.?.header.op == self.commit_min + 1);
            assert(self.client_replies.writes.available() > 0);

            self.commit_op(self.commit_prepare.?);
            assert(self.commit_min == self.commit_prepare.?.header.op);
            assert(self.commit_min <= self.commit_max);

            if (self.status == .normal and self.primary()) {
                if (self.pipeline.queue.pop_request()) |request| {
                    // Start preparing the next request in the queue (if any).
                    self.primary_pipeline_prepare(request);
                }

                assert(self.commit_min == self.commit_max);

                if (self.pipeline.queue.prepare_queue.head_ptr()) |next| {
                    assert(next.message.header.op == self.commit_min + 1);
                    assert(next.message.header.op == self.commit_prepare.?.header.op + 1);

                    if (self.solo()) {
                        // Write the next message in the queue.
                        // A cluster-of-one writes prepares sequentially to avoid gaps in the
                        // WAL caused by reordered writes.
                        log.debug("{}: append: appending to journal op={}", .{
                            self.replica,
                            next.message.header.op,
                        });
                        self.write_prepare(next.message, .append);
                    }
                }
            }

            self.commit_dispatch(.compact_state_machine);
        }

        fn commit_op_compact_callback(state_machine: *StateMachine) void {
            const self = @fieldParentPtr(Self, "state_machine", state_machine);
            assert(self.commit_stage == .compact_state_machine);
            assert(self.op_checkpoint() == self.superblock.staging.vsr_state.checkpoint.commit_min);
            assert(self.op_checkpoint() == self.superblock.working.vsr_state.checkpoint.commit_min);

            if (self.event_callback) |hook| hook(self, .compaction_completed);

            const op = self.commit_prepare.?.header.op;
            assert(op == self.commit_min);
            assert(op <= self.op_checkpoint_next_trigger());

            if (op == self.op_checkpoint_next_trigger()) {
                assert(op <= self.op);
                assert((op + 1) % constants.lsm_batch_multiple == 0);
                log.debug("{}: commit_op_compact_callback: checkpoint start " ++
                    "(op={} current_checkpoint={} next_checkpoint={})", .{
                    self.replica,
                    self.op,
                    self.op_checkpoint(),
                    self.op_checkpoint_next(),
                });
                tracer.start(
                    &self.tracer_slot_checkpoint,
                    .checkpoint,
                    @src(),
                );
                if (self.event_callback) |hook| hook(self, .checkpoint_commenced);

                // TODO(Compaction pacing) Move this out of the conditional once there is no IO
                // between beats.
                self.grid.assert_only_repairing();

                self.commit_dispatch(.{ .checkpoint_data = CheckpointDataProgress.initEmpty() });
            } else {
                self.commit_dispatch(.cleanup);
            }
        }

        fn commit_op_checkpoint_state_machine_callback(state_machine: *StateMachine) void {
            const self = @fieldParentPtr(Self, "state_machine", state_machine);
            assert(self.commit_stage == .checkpoint_data);
            self.commit_op_checkpoint_data_callback(.state_machine);
        }

        fn commit_op_checkpoint_client_replies_callback(client_replies: *ClientReplies) void {
            const self = @fieldParentPtr(Self, "client_replies", client_replies);
            assert(self.commit_stage == .checkpoint_data);
            self.commit_op_checkpoint_data_callback(.client_replies);
        }

        fn commit_op_checkpoint_client_sessions_callback(client_sessions_checkpoint: *CheckpointTrailer) void {
            const self = @fieldParentPtr(Self, "client_sessions_checkpoint", client_sessions_checkpoint);
            assert(self.commit_stage == .checkpoint_data);
            self.commit_op_checkpoint_data_callback(.client_sessions);
        }

        fn commit_op_checkpoint_grid_callback(grid: *Grid) void {
            const self = @fieldParentPtr(Self, "grid", grid);
            assert(self.commit_stage == .checkpoint_data);
            assert(self.commit_prepare.?.header.op <= self.op);
            assert(self.commit_prepare.?.header.op == self.commit_min);
            assert(self.grid.free_set.opened);
            assert(self.grid.free_set.count_released() ==
                self.grid.free_set_checkpoint.block_count());

            self.commit_op_checkpoint_data_callback(.grid);
        }

        fn commit_op_checkpoint_data_callback(
            self: *Self,
            checkpoint_data: CheckpointData,
        ) void {
            assert(self.commit_stage == .checkpoint_data);
            assert(self.commit_prepare.?.header.op <= self.op);
            assert(self.commit_prepare.?.header.op == self.commit_min);
            assert(self.commit_prepare.?.header.op == self.op_checkpoint_next_trigger());

            assert(!self.commit_stage.checkpoint_data.contains(checkpoint_data));
            self.commit_stage.checkpoint_data.insert(checkpoint_data);

            if (self.commit_stage.checkpoint_data.count() == CheckpointDataProgress.len) {
                self.grid.assert_only_repairing();

                {
                    const checkpoint = &self.client_sessions_checkpoint;
                    var address_previous: u64 = 0;
                    for (checkpoint.block_addresses[0..checkpoint.block_count()]) |address| {
                        assert(address > 0);
                        assert(address > address_previous);
                        address_previous = address;
                        self.grid.release(address);
                    }
                }
                assert(self.grid.free_set.count_released() ==
                    self.grid.free_set_checkpoint.block_count() +
                    self.client_sessions_checkpoint.block_count());

                self.commit_dispatch(.checkpoint_superblock);
            }
        }

        fn commit_op_checkpoint_superblock(self: *Self) void {
            assert(self.grid.free_set.opened);
            assert(self.state_machine_opened);
            assert(self.commit_stage == .checkpoint_superblock);
            assert(self.commit_prepare.?.header.op <= self.op);
            assert(self.commit_prepare.?.header.op == self.commit_min);
            assert(self.commit_prepare.?.header.op == self.op_checkpoint_next_trigger());
            assert(self.op_checkpoint_next_trigger() <= self.commit_max);
            self.grid.assert_only_repairing();

            // For the given WAL (journal_slot_count=8, lsm_batch_multiple=2, op=commit_min=7):
            //
            //   A  B  C  D  E
            //   |01|23|45|67|
            //
            // The checkpoint is triggered at "E".
            // At this point, ops 6 and 7 are in the in-memory immutable table.
            // They will only be compacted to disk in the next bar.
            // Therefore, only ops "A..D" are committed to disk.
            // Thus, the SuperBlock's `commit_min` is set to 7-2=5.
            const vsr_state_commit_min = self.op_checkpoint_next();

            const vsr_state_sync_op: struct { min: u64, max: u64 } = sync: {
                if (self.sync_content_done()) {
                    assert(self.sync_tables == null);
                    assert(self.grid_repair_tables.executing() == 0);

                    break :sync .{ .min = 0, .max = 0 };
                } else {
                    break :sync .{
                        .min = self.superblock.staging.vsr_state.sync_op_min,
                        .max = self.superblock.staging.vsr_state.sync_op_max,
                    };
                }
            };

            const storage_size: u64 = storage_size: {
                var storage_size = vsr.superblock.data_file_size_min;
                if (self.grid.free_set.highest_address_acquired()) |address| {
                    assert(address > 0);
                    assert(self.grid.free_set_checkpoint.size > 0);
                    storage_size += address * constants.block_size;
                } else {
                    assert(self.grid.free_set_checkpoint.size == 0);
                    assert(self.grid.free_set.count_released() == 0);
                }
                break :storage_size storage_size;
            };

            self.superblock.checkpoint(
                commit_op_checkpoint_superblock_callback,
                &self.superblock_context,
                .{
                    .commit_min_checksum = self.journal.header_with_op(vsr_state_commit_min).?.checksum,
                    .commit_min = vsr_state_commit_min,
                    .commit_max = self.commit_max,
                    .sync_op_min = vsr_state_sync_op.min,
                    .sync_op_max = vsr_state_sync_op.max,
                    .manifest_references = self.state_machine.forest.manifest_log.checkpoint_references(),
                    .free_set_reference = self.grid.free_set_checkpoint.checkpoint_reference(),
                    .client_sessions_reference = self.client_sessions_checkpoint.checkpoint_reference(),
                    .storage_size = storage_size,
                },
            );
        }

        fn commit_op_checkpoint_superblock_callback(superblock_context: *SuperBlock.Context) void {
            const self = @fieldParentPtr(Self, "superblock_context", superblock_context);
            assert(self.commit_stage == .checkpoint_superblock);
            assert(self.commit_prepare.?.header.op <= self.op);
            assert(self.commit_prepare.?.header.op == self.commit_min);

            assert(self.op_checkpoint() == self.commit_min - constants.lsm_batch_multiple);
            assert(self.op_checkpoint() == self.superblock.staging.vsr_state.checkpoint.commit_min);
            assert(self.op_checkpoint() == self.superblock.working.vsr_state.checkpoint.commit_min);
            self.grid.assert_only_repairing();

            log.debug("{}: commit_op_compact_callback: checkpoint done (op={} new_checkpoint={})", .{
                self.replica,
                self.op,
                self.op_checkpoint(),
            });
            tracer.end(
                &self.tracer_slot_checkpoint,
                .checkpoint,
            );

            if (self.event_callback) |hook| hook(self, .checkpoint_completed);
            self.commit_dispatch(.cleanup);
        }

        fn commit_op_cleanup(self: *Self) void {
            assert(self.commit_stage == .cleanup);
            assert(self.commit_prepare.?.header.op == self.commit_min);
            assert(self.commit_prepare.?.header.op < self.op_checkpoint_next_trigger());

            const op = self.commit_prepare.?.header.op;

            self.message_bus.unref(self.commit_prepare.?);
            self.commit_prepare = null;

            tracer.end(
                &self.tracer_slot_commit,
                .{ .commit = .{ .op = op } },
            );

            self.commit_dispatch(.next);
        }

        fn commit_op(self: *Self, prepare: *const Message.Prepare) void {
            // TODO Can we add more checks around allowing commit_op() during a view change?
            assert(self.commit_stage == .setup_client_replies);
            assert(self.commit_prepare.? == prepare);
            assert(self.status == .normal or self.status == .view_change or
                (self.status == .recovering and self.solo()));
            assert(self.client_replies.writes.available() > 0);
            assert(prepare.header.command == .prepare);
            assert(prepare.header.operation != .root);
            assert(prepare.header.operation != .reserved);
            assert(prepare.header.op == self.commit_min + 1);
            assert(prepare.header.op <= self.op);

            // If we are a backup committing through `commit_journal()` then a view change may
            // have happened since we last checked in `commit_journal_next()`. However, this would
            // relate to subsequent ops, since by now we have already verified the hash chain for
            // this commit.

            assert(self.journal.has(prepare.header));
            if (self.op_checkpoint() == self.commit_min) {
                // op_checkpoint's slot may have been overwritten in the WAL — but we can
                // always use the VSRState to anchor the hash chain.
                assert(prepare.header.parent ==
                    self.superblock.working.vsr_state.checkpoint.commit_min_checksum);
            } else {
                assert(prepare.header.parent ==
                    self.journal.header_with_op(self.commit_min).?.checksum);
            }

            log.debug("{}: commit_op: executing view={} primary={} op={} checksum={} ({s})", .{
                self.replica,
                self.view,
                self.primary_index(self.view) == self.replica,
                prepare.header.op,
                prepare.header.checksum,
                prepare.header.operation.tag_name(StateMachine),
            });

            const reply = self.message_bus.get_message(.reply);
            defer self.message_bus.unref(reply);

            log.debug("{}: commit_op: commit_timestamp={} prepare.header.timestamp={}", .{
                self.replica,
                self.state_machine.commit_timestamp,
                prepare.header.timestamp,
            });
            assert(self.state_machine.commit_timestamp < prepare.header.timestamp or constants.aof_recovery);

            // Synchronously record this request in our AOF. This can be used for disaster recovery
            // in the case of catastrophic storage failure. Internally, write() will only return
            // once the data has been written to disk with O_DIRECT and O_SYNC.
            //
            // We run this here, instead of in state_machine, so we can have full access to the VSR
            // header information. This way we can just log the Prepare in its entirety.
            //
            // A minor detail, but this is not a WAL. Hence the name being AOF - since it's similar
            // to how Redis's Append Only File works. It's also technically possible for a request
            // to be recorded by the AOF, with the client not having received a response
            // (eg, a panic right after writing to the AOF before sending the response) but we
            // consider this harmless due to our requirement for unique Account / Transfer IDs.
            //
            // It should be impossible for a client to receive a response without the request
            // being logged by at least one replica.
            if (AOF != void) {
                self.aof.write(prepare, .{
                    .replica = self.replica,
                    .primary = self.primary_index(self.view),
                }) catch @panic("aof failure");
            }

            const reply_body_size = switch (prepare.header.operation) {
                .reserved, .root => unreachable,
                .register => 0,
                .reconfigure => self.commit_reconfiguration(
                    prepare,
                    reply.buffer[@sizeOf(Header)..],
                ),
                else => self.state_machine.commit(
                    prepare.header.client,
                    prepare.header.op,
                    prepare.header.timestamp,
                    prepare.header.operation.cast(StateMachine),
                    prepare.buffer[@sizeOf(Header)..prepare.header.size],
                    reply.buffer[@sizeOf(Header)..],
                ),
            };

            assert(self.state_machine.commit_timestamp <= prepare.header.timestamp or constants.aof_recovery);
            self.state_machine.commit_timestamp = prepare.header.timestamp;

            if (self.status == .normal and self.primary()) {
                const pipeline_prepare = self.pipeline.queue.pop_prepare().?;
                defer self.message_bus.unref(pipeline_prepare.message);

                assert(pipeline_prepare.message == prepare);
                assert(pipeline_prepare.message.header.command == .prepare);
                assert(pipeline_prepare.message.header.checksum == self.commit_prepare.?.header.checksum);
                assert(pipeline_prepare.message.header.op == self.commit_min + 1);
                assert(pipeline_prepare.message.header.op == self.commit_max + 1);
                assert(pipeline_prepare.ok_quorum_received);
            }

            self.commit_min += 1;
            assert(self.commit_min == prepare.header.op);
            self.advance_commit_max(self.commit_min, @src());

            if (self.event_callback) |hook| hook(self, .committed);

            reply.header.* = .{
                .command = .reply,
                .operation = prepare.header.operation,
                .request_checksum = prepare.header.request_checksum,
                .client = prepare.header.client,
                .request = prepare.header.request,
                .cluster = prepare.header.cluster,
                .replica = prepare.header.replica,
                .view = prepare.header.view,
                .op = prepare.header.op,
                .timestamp = prepare.header.timestamp,
                .commit = prepare.header.op,
                .size = @sizeOf(Header) + @as(u32, @intCast(reply_body_size)),
            };
            assert(reply.header.epoch == 0);

            reply.header.set_checksum_body(reply.body());
            // See `send_reply_message_to_client` for why we compute the checksum twice.
            reply.header.context = reply.header.calculate_checksum();
            reply.header.set_checksum();

            if (self.superblock.working.vsr_state.op_compacted(prepare.header.op)) {
                // We are recovering from a checkpoint. Prior to the crash, the client table was
                // updated with entries for one bar beyond the op_checkpoint.
                assert(self.op_checkpoint() ==
                    self.superblock.working.vsr_state.checkpoint.commit_min);
                if (self.client_sessions.get(prepare.header.client)) |entry| {
                    assert(entry.header.command == .reply);
                    assert(entry.header.op >= prepare.header.op);
                } else {
                    assert(self.client_sessions.count() == self.client_sessions.capacity());
                }

                log.debug("{}: commit_op: skip client table update: prepare.op={} checkpoint={}", .{
                    self.replica,
                    prepare.header.op,
                    self.op_checkpoint(),
                });
            } else {
                if (reply.header.operation == .register) {
                    self.client_table_entry_create(reply);
                } else {
                    self.client_table_entry_update(reply);
                }
            }

            if (self.primary_index(self.view) == self.replica) {
                log.debug("{}: commit_op: replying to client: {}", .{ self.replica, reply.header });
                self.send_reply_message_to_client(reply);
            }
        }

        // The actual "execution" was handled by the primary when the request was prepared.
        // Primary makes use of local information to decide whether reconfiguration should be accepted.
        // Here, we just copy over the result.
        fn commit_reconfiguration(
            self: *Self,
            prepare: *const Message.Prepare,
            output_buffer: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            assert(self.commit_stage == .setup_client_replies);
            assert(self.commit_prepare.? == prepare);
            assert(prepare.header.command == .prepare);
            assert(prepare.header.operation == .reconfigure);
            assert(prepare.header.size == @sizeOf(vsr.Header) + @sizeOf(vsr.ReconfigurationRequest));
            assert(prepare.header.op == self.commit_min + 1);
            assert(prepare.header.op <= self.op);

            const reconfiguration_request = std.mem.bytesAsValue(
                vsr.ReconfigurationRequest,
                prepare.body()[0..@sizeOf(vsr.ReconfigurationRequest)],
            );
            assert(reconfiguration_request.result != .reserved);

            const result = std.mem.bytesAsValue(
                vsr.ReconfigurationResult,
                output_buffer[0..@sizeOf(vsr.ReconfigurationResult)],
            );

            result.* = reconfiguration_request.result;
            return @sizeOf(vsr.ReconfigurationResult);
        }

        /// Creates an entry in the client table when registering a new client session.
        /// Asserts that the new session does not yet exist.
        /// Evicts another entry deterministically, if necessary, to make space for the insert.
        fn client_table_entry_create(self: *Self, reply: *Message.Reply) void {
            assert(reply.header.command == .reply);
            assert(reply.header.operation == .register);
            assert(reply.header.client > 0);
            assert(reply.header.op == reply.header.commit);
            assert(reply.header.size == @sizeOf(Header));

            const session = reply.header.commit; // The commit number becomes the session number.
            const request = reply.header.request;

            // We reserved the `0` commit number for the cluster `.root` operation.
            assert(session > 0);
            assert(request == 0);

            // For correctness, it's critical that all replicas evict deterministically:
            // We cannot depend on `HashMap.capacity()` since `HashMap.ensureTotalCapacity()` may
            // change across versions of the Zig std lib. We therefore rely on
            // `constants.clients_max`, which must be the same across all replicas, and must not
            // change after initializing a cluster.
            // We also do not depend on `HashMap.valueIterator()` being deterministic here. However,
            // we do require that all entries have different commit numbers and are iterated.
            // This ensures that we will always pick the entry with the oldest commit number.
            // We also check that a client has only one entry in the hash map (or it's buggy).
            const clients = self.client_sessions.count();
            assert(clients <= constants.clients_max);
            if (clients == constants.clients_max) {
                const evictee = self.client_sessions.evictee();
                self.client_sessions.remove(evictee);

                assert(self.client_sessions.count() == constants.clients_max - 1);

                log.err("{}: client_table_entry_create: clients={}/{} evicting client={}", .{
                    self.replica,
                    clients,
                    constants.clients_max,
                    evictee,
                });
            }

            log.debug("{}: client_table_entry_create: write (client={} session={} request={})", .{
                self.replica,
                reply.header.client,
                session,
                request,
            });

            // Any duplicate .register requests should have received the same session number if the
            // client table entry already existed, or been dropped if a session was being committed:
            const reply_slot = self.client_sessions.put(session, reply.header);
            assert(self.client_sessions.count() <= constants.clients_max);

            if (reply.header.size == @sizeOf(Header)) {
                self.client_replies.remove_reply(reply_slot);
            } else {
                self.client_replies.write_reply(reply_slot, reply, .commit);
            }
        }

        fn client_table_entry_update(self: *Self, reply: *Message.Reply) void {
            assert(reply.header.command == .reply);
            assert(reply.header.operation != .register);
            assert(reply.header.client > 0);
            assert(reply.header.op == reply.header.commit);
            assert(reply.header.commit > 0);
            assert(reply.header.request > 0);

            if (self.client_sessions.get(reply.header.client)) |entry| {
                assert(entry.header.command == .reply);
                assert(entry.header.op == entry.header.commit);
                assert(entry.header.commit >= entry.session);

                assert(entry.header.client == reply.header.client);
                assert(entry.header.request + 1 == reply.header.request);
                assert(entry.header.op < reply.header.op);
                assert(entry.header.commit < reply.header.commit);

                // TODO Use this reply's prepare to cross-check against the entry's prepare, if we
                // still have access to the prepare in the journal (it may have been snapshotted).

                log.debug("{}: client_table_entry_update: client={} session={} request={}", .{
                    self.replica,
                    reply.header.client,
                    entry.session,
                    reply.header.request,
                });

                entry.header = reply.header.*;

                const reply_slot = self.client_sessions.get_slot_for_header(reply.header).?;
                if (entry.header.size == @sizeOf(Header)) {
                    self.client_replies.remove_reply(reply_slot);
                } else {
                    self.client_replies.write_reply(reply_slot, reply, .commit);
                }
            } else {
                // If no entry exists, then the session must have been evicted while being prepared.
                // We can still send the reply, the next request will receive an eviction message.
            }
        }

        /// Construct a SV message, including attached headers from the current log_view.
        /// The caller owns the returned message, if any, which has exactly 1 reference.
        fn create_start_view_message(self: *Self, nonce: u128) *Message.StartView {
            assert(self.status == .normal);
            assert(self.replica == self.primary_index(self.view));
            assert(self.commit_min == self.commit_max);
            assert(self.commit_min <= self.op);
            assert(self.view >= self.view_durable());
            assert(self.log_view >= self.log_view_durable());
            assert(self.log_view == self.view);

            self.primary_update_view_headers();
            assert(self.view_headers.command == .start_view);
            assert(self.view_headers.array.get(0).op == self.op);
            self.view_headers.verify();

            const message = self.message_bus.get_message(.start_view);
            defer self.message_bus.unref(message);

            message.header.* = .{
                .size = @sizeOf(Header) * (1 + self.view_headers.array.count_as(u32)),
                .command = .start_view,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
                .checkpoint_op = self.op_checkpoint(),
                .op = self.op,
                .commit = self.commit_min, // (Same as commit_max.)
                .nonce = nonce,
            };

            stdx.copy_disjoint(
                .exact,
                Header.Prepare,
                std.mem.bytesAsSlice(Header.Prepare, message.body()),
                self.view_headers.array.const_slice(),
            );
            message.header.set_checksum_body(message.body());
            message.header.set_checksum();

            return message.ref();
        }

        fn primary_update_view_headers(self: *Self) void {
            assert(self.status != .recovering_head);
            assert(self.replica == self.primary_index(self.view));
            assert(self.view == self.log_view);
            assert(self.view_headers.command == .start_view);
            if (self.status == .recovering) assert(self.solo());

            self.view_headers.array.clear();

            var op = self.op + 1;
            while (op > 0 and
                self.view_headers.array.count() < constants.view_change_headers_suffix_max)
            {
                op -= 1;
                self.view_headers.append(self.journal.header_with_op(op).?);
            }
            assert(self.view_headers.array.count() + 2 <= constants.view_change_headers_max);

            // Determine the consecutive extent of the log that we can help recover.
            // This may precede op_repair_min if we haven't had a view-change recently.
            const range_min = (self.op + 1) -| constants.journal_slot_count;
            const range = self.journal.find_latest_headers_break_between(range_min, self.op);
            const op_min = if (range) |r| r.op_max + 1 else range_min;
            assert(op_min <= op);
            assert(op_min <= self.op_repair_min());

            // The SV includes headers corresponding to the op_prepare_max for preceding
            // checkpoints (as many as we have and can help repair, which is at most 2).
            for ([_]u64{
                self.op_prepare_max() -| constants.vsr_checkpoint_interval,
                self.op_prepare_max() -| constants.vsr_checkpoint_interval * 2,
            }) |op_hook| {
                if (op > op_hook and op_hook >= op_min) {
                    op = op_hook;
                    self.view_headers.append(self.journal.header_with_op(op).?);
                }
            }
        }

        /// The caller owns the returned message, if any, which has exactly 1 reference.
        fn create_message_from_header(self: *Self, header: Header) *Message {
            assert(
                header.view == self.view or
                    header.command == .request_start_view or
                    header.command == .request_headers or
                    header.command == .request_prepare or
                    header.command == .request_reply or
                    header.command == .request_sync_checkpoint or
                    header.command == .reply or
                    header.command == .ping or header.command == .pong,
            );
            assert(header.size == @sizeOf(Header));

            const message = self.message_bus.pool.get_message(null);
            defer self.message_bus.unref(message);

            message.header.* = header;
            message.header.set_checksum_body(message.body());
            message.header.set_checksum();

            return message.ref();
        }

        /// Discards uncommitted ops during a view change from after and including `op`.
        /// This is required to maximize availability in the presence of storage faults.
        /// Refer to the CTRL protocol from Protocol-Aware Recovery for Consensus-Based Storage.
        fn primary_discard_uncommitted_ops_from(self: *Self, op: u64, checksum: u128) void {
            assert(self.status == .view_change);
            assert(self.primary_index(self.view) == self.replica);
            assert(self.repairs_allowed());

            assert(self.valid_hash_chain(@src()));

            const slot = self.journal.slot_with_op(op).?;
            assert(op > self.commit_max);
            assert(op <= self.op);
            assert(self.journal.header_with_op_and_checksum(op, checksum) != null);
            assert(self.journal.dirty.bit(slot));

            log.debug("{}: primary_discard_uncommitted_ops_from: ops={}..{} view={}", .{
                self.replica,
                op,
                self.op,
                self.view,
            });

            self.op = op - 1;
            self.journal.remove_entries_from(op);

            assert(self.journal.header_for_op(op) == null);
            assert(!self.journal.dirty.bit(slot));
            assert(!self.journal.faulty.bit(slot));

            // We require that `self.op` always exists. Rewinding `self.op` could change that.
            // However, we do this only as the primary within a view change, with all headers intact.
            assert(self.journal.header_with_op(self.op) != null);
        }

        fn flush_loopback_queue(self: *Self) void {
            // There are five cases where a replica will send a message to itself:
            // However, of these five cases, all but one call send_message_to_replica().
            //
            // 1. In on_request(), the primary sends a synchronous prepare to itself, but this is
            //    done by calling on_prepare() directly, and subsequent prepare timeout retries will
            //    never resend to self.
            // 2. In on_prepare(), after writing to storage, the primary sends a (typically)
            //    asynchronous prepare_ok to itself.
            // 3. In transition_to_view_change_status(), the new primary sends a synchronous DVC to
            //    itself.
            // 4. In primary_start_view_as_the_new_primary(), the new primary sends itself a
            //    prepare_ok message for each uncommitted message.
            // 5. In send_start_view_change(), a replica sends itself a start_view_change message.
            if (self.loopback_queue) |message| {
                defer self.message_bus.unref(message);

                assert(!self.standby());

                assert(message.next == null);
                self.loopback_queue = null;
                assert(message.header.replica == self.replica);
                self.on_message(message);
                // We do not call flush_loopback_queue() within on_message() to avoid recursion.
            }
            // We expect that delivering a prepare_ok or do_view_change message to ourselves will
            // not result in any further messages being added synchronously to the loopback queue.
            assert(self.loopback_queue == null);
        }

        fn ignore_ping_client(self: *const Self, message: *const Message.PingClient) bool {
            assert(message.header.command == .ping_client);
            assert(message.header.client != 0);

            if (self.standby()) {
                log.warn("{}: on_ping_client: misdirected message (standby)", .{self.replica});
                return true;
            }

            // We must only ever send our view number to a client via a pong message if we are
            // in normal status. Otherwise, we may be partitioned from the cluster with a newer
            // view number, leak this to the client, which would then pass this to the cluster
            // in subsequent client requests, which would then ignore these client requests with
            // a newer view number, locking out the client. The principle here is that we must
            // never send view numbers for views that have not yet started.
            if (self.status != .normal) return true;

            return false;
        }

        fn ignore_prepare_ok(self: *Self, message: *const Message.PrepareOk) bool {
            assert(message.header.command == .prepare_ok);
            assert(message.header.replica < self.replica_count);

            if (self.primary_index(message.header.view) == self.replica) {
                assert(message.header.view <= self.view);
            }

            if (self.status != .normal) {
                log.debug("{}: on_prepare_ok: ignoring ({})", .{ self.replica, self.status });
                return true;
            }

            if (message.header.view < self.view) {
                log.debug("{}: on_prepare_ok: ignoring (older view)", .{self.replica});
                return true;
            }

            if (message.header.view > self.view) {
                // Another replica is treating us as the primary for a view we do not know about.
                // This may be caused by a fault in the network topology.
                log.warn("{}: on_prepare_ok: misdirected message (newer view)", .{self.replica});
                return true;
            }

            if (self.backup()) {
                log.warn("{}: on_prepare_ok: misdirected message (backup)", .{self.replica});
                return true;
            }

            return false;
        }

        fn ignore_repair_message(self: *Self, message: *const Message) bool {
            assert(message.header.command == .request_start_view or
                message.header.command == .request_headers or
                message.header.command == .request_prepare or
                message.header.command == .request_reply or
                message.header.command == .headers);
            switch (message.header.command) {
                .headers => assert(message.header.replica < self.replica_count),
                else => {},
            }

            const command: []const u8 = @tagName(message.header.command);

            if (message.header.command == .request_headers or
                message.header.command == .request_prepare or
                message.header.command == .request_reply)
            {
                // A recovering_head/syncing replica can still assist others with WAL/Reply-repair,
                // but does not itself install headers, since its head is unknown.
            } else {
                if (self.status != .normal and self.status != .view_change) {
                    log.debug("{}: on_{s}: ignoring ({})", .{ self.replica, command, self.status });
                    return true;
                }
            }

            if (message.header.command == .request_headers or
                message.header.command == .request_prepare or
                message.header.command == .request_reply or
                message.header.command == .headers)
            {
                // A replica in a different view can assist WAL repair.
            } else {
                assert(message.header.command == .request_start_view);

                if (message.header.view < self.view) {
                    log.debug("{}: on_{s}: ignoring (older view)", .{ self.replica, command });
                    return true;
                }

                if (message.header.view > self.view) {
                    log.debug("{}: on_{s}: ignoring (newer view)", .{ self.replica, command });
                    return true;
                }
            }

            if (self.ignore_repair_message_during_view_change(message)) return true;

            if (message.header.replica == self.replica) {
                log.warn("{}: on_{s}: misdirected message (self)", .{ self.replica, command });
                return true;
            }

            if (self.standby()) {
                switch (message.header.command) {
                    .headers => {},
                    .request_start_view, .request_headers, .request_prepare, .request_reply => {
                        log.warn("{}: on_{s}: misdirected message (standby)", .{ self.replica, command });
                        return true;
                    },
                    else => unreachable,
                }
            }

            if (self.primary_index(self.view) != self.replica) {
                switch (message.header.command) {
                    // Only the primary may receive these messages:
                    .request_start_view => {
                        log.warn("{}: on_{s}: misdirected message (backup)", .{ self.replica, command });
                        return true;
                    },
                    .request_prepare, .headers, .request_headers, .request_reply => {},
                    else => unreachable,
                }
            }
            return false;
        }

        fn ignore_repair_message_during_view_change(self: *Self, message: *const Message) bool {
            if (self.status != .view_change) return false;

            const command: []const u8 = @tagName(message.header.command);

            switch (message.header.command) {
                .request_start_view => {
                    log.debug("{}: on_{s}: ignoring (view change)", .{ self.replica, command });
                    return true;
                },
                .request_headers, .request_prepare, .request_reply => {
                    if (self.primary_index(self.view) != message.header.replica) {
                        log.debug("{}: on_{s}: ignoring (view change, requested by backup)", .{
                            self.replica,
                            command,
                        });
                        return true;
                    }
                },
                .headers => {
                    if (self.primary_index(self.view) != self.replica) {
                        log.debug("{}: on_{s}: ignoring (view change, received by backup)", .{
                            self.replica,
                            command,
                        });
                        return true;
                    } else if (!self.do_view_change_quorum) {
                        log.debug("{}: on_{s}: ignoring (view change, waiting for quorum)", .{
                            self.replica,
                            command,
                        });
                        return true;
                    }
                },
                else => unreachable,
            }

            return false;
        }

        fn ignore_request_message(self: *Self, message: *Message.Request) bool {
            if (self.standby()) {
                log.warn("{}: on_request: misdirected message (standby)", .{self.replica});
                return true;
            }

            if (self.status != .normal) {
                log.debug("{}: on_request: ignoring ({})", .{ self.replica, self.status });
                return true;
            }

            if (!message.header.operation.valid(StateMachine)) {
                // Some possible causes:
                // - client bug
                // - client memory corruption
                // - client/replica version mismatch
                log.err("{}: on_request: ignoring invalid operation (client={} operation={})", .{
                    self.replica,
                    message.header.client,
                    @intFromEnum(message.header.operation),
                });
                return true;
            }

            if (self.solo()) {
                if (self.view_durable_updating()) {
                    log.debug("{}: on_request: ignoring (still persisting view)", .{self.replica});
                    return true;
                }
            }

            if (self.ignore_request_message_backup(message)) return true;
            if (self.ignore_request_message_duplicate(message)) return true;
            if (self.ignore_request_message_preparing(message)) return true;
            return false;
        }

        /// Returns whether the request is stale, or a duplicate of the latest committed request.
        /// Resends the reply to the latest request if the request has been committed.
        fn ignore_request_message_duplicate(self: *Self, message: *const Message.Request) bool {
            assert(self.status == .normal);
            assert(self.primary());
            assert(self.syncing == .idle);

            assert(message.header.command == .request);
            assert(message.header.client > 0);
            assert(message.header.view <= self.view); // See ignore_request_message_backup().
            assert(message.header.session == 0 or message.header.operation != .register);
            assert(message.header.request == 0 or message.header.operation != .register);

            if (self.client_sessions.get(message.header.client)) |entry| {
                assert(entry.header.command == .reply);
                assert(entry.header.client == message.header.client);

                if (message.header.operation == .register) {
                    // Fall through below to check if we should resend the .register session reply.
                } else if (entry.session > message.header.session) {
                    // The client must not reuse the ephemeral client ID when registering a new session.
                    log.err("{}: on_request: ignoring older session (client bug)", .{self.replica});
                    return true;
                } else if (entry.session < message.header.session) {
                    // This cannot be because of a partition since we check the client's view number.
                    log.err("{}: on_request: ignoring newer session (client bug)", .{self.replica});
                    return true;
                }

                if (entry.header.request > message.header.request) {
                    log.debug("{}: on_request: ignoring older request", .{self.replica});
                    return true;
                } else if (entry.header.request == message.header.request) {
                    if (message.header.checksum == entry.header.request_checksum) {
                        assert(entry.header.operation == message.header.operation);

                        log.debug("{}: on_request: replying to duplicate request", .{self.replica});
                        self.on_request_repeat_reply(message, entry);
                        return true;
                    } else {
                        log.err("{}: on_request: request collision (client bug)", .{self.replica});
                        return true;
                    }
                } else if (entry.header.request + 1 == message.header.request) {
                    if (message.header.parent == entry.header.context) {
                        // The client has proved that they received our last reply.
                        log.debug("{}: on_request: new request", .{self.replica});
                        return false;
                    } else {
                        // The client may have only one request inflight at a time.
                        log.err("{}: on_request: ignoring new request (client bug)", .{
                            self.replica,
                        });
                        return true;
                    }
                } else {
                    // Caused by one of the following:
                    // - client bug, or
                    // - this primary is no longer the actual primary
                    log.err("{}: on_request: ignoring newer request (client|network bug)", .{self.replica});
                    return true;
                }
            } else if (message.header.operation == .register) {
                log.debug("{}: on_request: new session", .{self.replica});
                return false;
            } else if (self.pipeline.queue.message_by_client(message.header.client)) |_| {
                // The client registered with the previous primary, which committed and replied back
                // to the client before the view change, after which the register operation was
                // reloaded into the pipeline to be driven to completion by the new primary, which
                // now receives a request from the client that appears to have no session.
                // However, the session is about to be registered, so we must wait for it to commit.
                log.debug("{}: on_request: waiting for session to commit", .{self.replica});
                return true;
            } else {
                // We must have all commits to know whether a session has been evicted. For example,
                // there is the risk of sending an eviction message (even as the primary) if we are
                // partitioned and don't yet know about a session. We solve this by having clients
                // include the view number and rejecting messages from clients with newer views.
                log.err("{}: on_request: no session", .{self.replica});
                self.send_eviction_message_to_client(message.header.client);
                return true;
            }
        }

        fn on_request_repeat_reply(
            self: *Self,
            message: *const Message.Request,
            entry: *const ClientSessions.Entry,
        ) void {
            assert(self.status == .normal);
            assert(self.primary());

            assert(message.header.command == .request);
            assert(message.header.client > 0);
            assert(message.header.view <= self.view); // See ignore_request_message_backup().
            assert(message.header.session == 0 or message.header.operation != .register);
            assert(message.header.request == 0 or message.header.operation != .register);
            assert(message.header.checksum == entry.header.request_checksum);
            assert(message.header.request == entry.header.request);

            if (entry.header.size == @sizeOf(Header)) {
                const reply = self.create_message_from_header(@bitCast(entry.header)).into(.reply).?;
                defer self.message_bus.unref(reply);

                self.send_reply_message_to_client(reply);
                return;
            }

            const slot = self.client_sessions.get_slot_for_client(message.header.client).?;
            if (self.client_replies.read_reply_sync(slot, entry)) |reply| {
                on_request_repeat_reply_callback(
                    &self.client_replies,
                    &entry.header,
                    reply,
                    null,
                );
            } else {
                self.client_replies.read_reply(
                    slot,
                    entry,
                    on_request_repeat_reply_callback,
                    null,
                ) catch |err| {
                    assert(err == error.Busy);

                    log.debug("{}: on_request: ignoring (client_replies busy)", .{
                        self.replica,
                    });
                };
            }
        }

        fn on_request_repeat_reply_callback(
            client_replies: *ClientReplies,
            reply_header: *const Header.Reply,
            reply_: ?*Message.Reply,
            destination_replica: ?u8,
        ) void {
            const self = @fieldParentPtr(Self, "client_replies", client_replies);
            assert(reply_header.size > @sizeOf(Header));
            assert(destination_replica == null);

            const reply = reply_ orelse {
                if (self.client_sessions.get_slot_for_header(reply_header)) |slot| {
                    self.client_replies.faulty.set(slot.index);
                } else {
                    // The read may have been a repair for an older op,
                    // or a newer op that we haven't seen yet.
                }
                return;
            };
            assert(reply.header.checksum == reply_header.checksum);
            assert(reply.header.size > @sizeOf(Header));

            log.debug("{}: on_request: repeat reply (client={} request={})", .{
                self.replica,
                reply.header.client,
                reply.header.request,
            });

            self.send_reply_message_to_client(reply);
        }

        /// Returns whether the replica is eligible to process this request as the primary.
        /// Takes the client's perspective into account if the client is aware of a newer view.
        /// Forwards requests to the primary if the client has an older view.
        fn ignore_request_message_backup(self: *Self, message: *Message.Request) bool {
            assert(self.status == .normal);
            assert(message.header.command == .request);

            // The client is aware of a newer view:
            // Even if we think we are the primary, we may be partitioned from the rest of the cluster.
            // We therefore drop the message rather than flood our partition with traffic.
            if (message.header.view > self.view) {
                log.debug("{}: on_request: ignoring (newer view)", .{self.replica});
                return true;
            } else if (self.primary()) {
                return false;
            }

            if (message.header.operation == .register) {
                // We do not forward `.register` requests for the sake of `Header.peer_type()`.
                // This enables the MessageBus to identify client connections on the first message.
                log.debug("{}: on_request: ignoring (backup, register)", .{self.replica});
            } else if (message.header.view < self.view) {
                // The client may not know who the primary is, or may be retrying after a primary failure.
                // We forward to the new primary ahead of any client retry timeout to reduce latency.
                // Since the client is already connected to all replicas, the client may yet receive the
                // reply from the new primary directly.
                log.debug("{}: on_request: forwarding (backup)", .{self.replica});
                self.send_message_to_replica(self.primary_index(self.view), message);
            } else {
                assert(message.header.view == self.view);
                // The client has the correct view, but has retried against a backup.
                // This may mean that the primary is down and that we are about to do a view change.
                // There is also not much we can do as the client already knows who the primary is.
                // We do not forward as this would amplify traffic on the network.

                // TODO This may also indicate a client-primary partition. If we see enough of these,
                // should we trigger a view change to select a primary that clients can reach?
                // This is a question of weighing the probability of a partition vs routing error.
                log.debug("{}: on_request: ignoring (backup, same view)", .{self.replica});
            }

            assert(self.backup());
            return true;
        }

        fn ignore_request_message_preparing(self: *Self, message: *const Message.Request) bool {
            assert(self.status == .normal);
            assert(self.primary());

            assert(message.header.command == .request);
            assert(message.header.client > 0);
            assert(message.header.view <= self.view); // See ignore_request_message_backup().

            if (self.pipeline.queue.message_by_client(message.header.client)) |pipeline_message| {
                assert(pipeline_message.header.command == .request or
                    pipeline_message.header.command == .prepare);

                switch (pipeline_message.header.into_any()) {
                    .request => |pipeline_message_header| {
                        assert(pipeline_message_header.client == message.header.client);

                        if (pipeline_message.header.checksum == message.header.checksum) {
                            assert(pipeline_message_header.request == message.header.request);
                            log.debug("{}: on_request: ignoring (already queued)", .{self.replica});
                            return true;
                        }
                    },
                    .prepare => |pipeline_message_header| {
                        assert(pipeline_message_header.client == message.header.client);

                        if (pipeline_message_header.request_checksum == message.header.checksum) {
                            assert(pipeline_message_header.op > self.commit_max);
                            assert(pipeline_message_header.request == message.header.request);
                            log.debug("{}: on_request: ignoring (already preparing)", .{self.replica});
                            return true;
                        }
                    },
                    else => unreachable,
                }

                log.err("{}: on_request: ignoring (client forked)", .{self.replica});
                return true;
            }

            if (self.pipeline.queue.full()) {
                log.debug("{}: on_request: ignoring (pipeline full)", .{self.replica});
                return true;
            }

            return false;
        }

        fn ignore_start_view_change_message(
            self: *const Self,
            message: *const Message.StartViewChange,
        ) bool {
            assert(message.header.command == .start_view_change);
            assert(message.header.replica < self.replica_count);

            if (self.standby()) {
                log.warn("{}: on_start_view_change: misdirected message (standby)", .{self.replica});
                return true;
            }

            switch (self.status) {
                .normal,
                .view_change,
                => {},
                .recovering => unreachable, // Single node clusters don't have view changes.
                .recovering_head => {
                    log.debug("{}: on_start_view_change: ignoring (status={})", .{
                        self.replica,
                        self.status,
                    });
                    return true;
                },
            }

            if (self.syncing != .idle) {
                log.debug("{}: on_start_view_change: ignoring (sync_status={s})", .{
                    self.replica,
                    @tagName(self.syncing),
                });
                return true;
            }

            if (message.header.view < self.view) {
                log.debug("{}: on_start_view_change: ignoring (older view)", .{self.replica});
                return true;
            }

            return false;
        }

        fn ignore_view_change_message(self: *const Self, message: *const Message) bool {
            assert(message.header.command == .do_view_change or
                message.header.command == .start_view);
            assert(self.status != .recovering); // Single node clusters don't have view changes.
            assert(message.header.replica < self.replica_count);

            const command: []const u8 = @tagName(message.header.command);

            if (message.header.view < self.view) {
                log.debug("{}: on_{s}: ignoring (older view)", .{ self.replica, command });
                return true;
            }

            switch (message.header.into_any()) {
                .start_view => |message_header| {
                    // This may be caused by faults in the network topology.
                    if (message.header.replica == self.replica) {
                        log.warn("{}: on_{s}: misdirected message (self)", .{ self.replica, command });
                        return true;
                    }

                    if (self.status == .recovering_head) {
                        if (message_header.view > self.view or
                            message_header.op >= self.op_prepare_max() or
                            message_header.nonce == self.nonce)
                        {
                            // This SV is guaranteed to have originated after the replica crash,
                            // it is safe to use to determine the head op.
                        } else {
                            log.debug("{}: on_{s}: ignoring (recovering_head, nonce mismatch)", .{
                                self.replica,
                                command,
                            });
                            return true;
                        }
                    }

                    // Syncing replicas must be careful about receiving SV messages, since they
                    // may have fast-forwarded their commit_max via their checkpoint target.
                    if (message_header.commit < self.op_checkpoint()) {
                        log.debug("{}: on_{s}: ignoring (older checkpoint)", .{
                            self.replica,
                            command,
                        });
                        return true;
                    }
                },
                .do_view_change => {
                    assert(message.header.view > 0); // The initial view is already zero.

                    if (self.standby()) {
                        log.warn("{}: on_{s}: misdirected message (standby)", .{ self.replica, command });
                        return true;
                    }

                    if (self.status == .recovering_head) {
                        log.debug("{}: on_{s}: ignoring (recovering_head)", .{ self.replica, command });
                        return true;
                    }

                    if (message.header.view == self.view and self.status == .normal) {
                        log.debug("{}: on_{s}: ignoring (view started)", .{
                            self.replica,
                            command,
                        });
                        return true;
                    }

                    if (self.do_view_change_quorum) {
                        log.debug("{}: on_{s}: ignoring (quorum received already)", .{
                            self.replica,
                            command,
                        });
                        return true;
                    }

                    if (self.primary_index(self.view) != self.replica) {
                        for (self.do_view_change_from_all_replicas) |dvc| assert(dvc == null);

                        log.debug("{}: on_{s}: ignoring (backup awaiting start_view)", .{
                            self.replica,
                            command,
                        });
                        return true;
                    }
                },
                else => unreachable,
            }

            return false;
        }

        fn ignore_request_sync_checkpoint_message(
            self: *const Self,
            message: *const Message.RequestSyncCheckpoint,
        ) bool {
            assert(message.header.command == .request_sync_checkpoint);

            const command = @tagName(message.header.command);
            if (self.replica == message.header.replica) {
                log.warn("{}: on_{s}: misdirected message (self)", .{ self.replica, command });
                return true;
            }

            if (self.standby()) {
                log.warn("{}: on_{s}: misdirected message (standby)", .{ self.replica, command });
                return true;
            }

            if (self.syncing != .idle) {
                log.debug("{}: on_{s}: ignoring (sync_status={s})", .{
                    self.replica,
                    command,
                    @tagName(self.syncing),
                });
                return true;
            }

            if (self.superblock.staging.vsr_state.checkpoint.commit_min != message.header.checkpoint_op) {
                log.debug("{}: on_{s}: ignoring different checkpoint (local={} message={})", .{
                    self.replica,
                    command,
                    self.op_checkpoint(),
                    message.header.checkpoint_op,
                });
                return true;
            }

            if (self.superblock.staging.checkpoint_id() != message.header.checkpoint_id) {
                log.warn("{}: on_{s}: ignoring divergent checkpoint (local={x:0>32} message={x:0>32} remote={})", .{
                    self.replica,
                    command,
                    self.superblock.staging.checkpoint_id(),
                    message.header.checkpoint_id,
                    message.header.replica,
                });
                return true;
            }

            return false;
        }

        fn ignore_sync_checkpoint_message(
            self: *const Self,
            message: *const Message.SyncCheckpoint,
        ) bool {
            assert(message.header.command == .sync_checkpoint);
            assert(message.header.replica < self.replica_count);

            if (self.syncing != .requesting_checkpoint) {
                log.debug("{}: on_{s}: ignoring (status={} sync_status={s})", .{
                    self.replica,
                    @tagName(message.header.command),
                    self.status,
                    @tagName(self.syncing),
                });
                return true;
            }

            const stage: *const SyncStage.RequestingCheckpoint =
                &self.syncing.requesting_checkpoint;

            if (message.header.checkpoint_op != stage.target.checkpoint_op) {
                log.debug("{}: on_{s}: ignoring, wrong op (got={} want={})", .{
                    self.replica,
                    @tagName(message.header.command),
                    message.header.checkpoint_op,
                    stage.target.checkpoint_op,
                });
                return true;
            }

            if (message.header.checkpoint_id != stage.target.checkpoint_id) {
                log.debug("{}: on_{s}: ignoring, wrong op (got={x:0>32} want={x:0>32})", .{
                    self.replica,
                    @tagName(message.header.command),
                    message.header.checkpoint_id,
                    stage.target.checkpoint_id,
                });
                return true;
            }
            return false;
        }

        fn is_repair(self: *const Self, message: *const Message.Prepare) bool {
            assert(message.header.command == .prepare);

            if (self.status == .normal) {
                if (message.header.view < self.view) return true;
                if (message.header.view == self.view and message.header.op <= self.op) return true;
            } else if (self.status == .view_change) {
                if (message.header.view < self.view) return true;
                // The view has already started or is newer.
            }

            return false;
        }

        /// Returns the index into the configuration of the primary for a given view.
        pub fn primary_index(self: *const Self, view: u32) u8 {
            return @intCast(@mod(view, self.replica_count));
        }

        /// Returns whether the replica is the primary for the current view.
        /// This may be used only when the replica status is normal.
        pub fn primary(self: *const Self) bool {
            assert(self.status == .normal);
            return self.primary_index(self.view) == self.replica;
        }

        /// Returns whether the replica is a backup for the current view.
        /// This may be used only when the replica status is normal.
        fn backup(self: *const Self) bool {
            return !self.primary();
        }

        /// Returns whether the replica is a single-replica cluster.
        ///
        /// Single-replica clusters often are a special case (no view changes or
        /// repairs, prepares are written to WAL sequentially).
        ///
        /// Note that a solo cluster might still have standby nodes.
        pub fn solo(self: *const Self) bool {
            return self.replica_count == 1 and !self.standby();
        }

        /// Returns whether the replica is a standby.
        ///
        /// Standbys follow the cluster without participating in consensus. In particular,
        /// standbys receive and replicate prepares, but never send prepare-oks.
        pub fn standby(self: *const Self) bool {
            assert(self.replica < self.node_count);
            return self.replica >= self.replica_count;
        }

        /// Advances `op` to where we need to be before `header` can be processed as a prepare.
        ///
        /// This function temporarily violates the "replica.op must exist in WAL" invariant.
        fn jump_to_newer_op_in_normal_status(
            self: *Self,
            header: *const Header.Prepare,
        ) void {
            assert(self.status == .normal);
            assert(self.backup());
            assert(header.view == self.view);
            assert(header.op > self.op + 1);
            // We may have learned of a higher `commit_max` through a commit message before jumping
            // to a newer op that is less than `commit_max` but greater than `commit_min`:
            assert(header.op > self.commit_min);
            // Never overwrite an op that still needs to be checkpointed.
            assert(header.op <= self.op_prepare_max());

            log.debug("{}: jump_to_newer_op: advancing: op={}..{} checksum={}..{}", .{
                self.replica,
                self.op,
                header.op - 1,
                self.journal.header_with_op(self.op).?.checksum,
                header.parent,
            });

            self.op = header.op - 1;
            assert(self.op >= self.commit_min);
            assert(self.op + 1 == header.op);
            assert(self.journal.header_with_op(self.op) == null);
        }

        /// Returns whether the head op is certain.
        ///
        /// After recovering the WAL, there are 2 possible outcomes:
        /// * All entries valid. The highest op is certain, and safe to set as `replica.op`.
        /// * One or more entries are faulty. The highest op isn't certain — it may be one of the
        ///   broken entries.
        ///
        /// The replica must refrain from repairing any faulty slots until the highest op is known.
        /// Otherwise, if we were to repair a slot while uncertain of `replica.op`:
        ///
        /// * we may nack an op that we shouldn't, or
        /// * we may replace a prepared op that we were guaranteeing for the primary, potentially
        ///   forking the log.
        ///
        ///
        /// Test for a fault the right of the current op. The fault might be our true op, and
        /// sharing our current `replica.op` might cause the cluster's op to likewise regress.
        ///
        /// Note that for our purposes here, we only care about entries that were faulty during
        /// WAL recovery, not ones that were found to be faulty after the fact (e.g. due to
        /// `request_prepare`).
        ///
        /// Cases (`✓`: `replica.op_checkpoint`, `✗`: faulty, `o`: `replica.op`):
        /// * ` ✓ o ✗ `: View change is unsafe.
        /// * ` ✗ ✓ o `: View change is unsafe.
        /// * ` ✓ ✗ o `: View change is safe.
        /// * ` ✓ = o `: View change is unsafe if any slots are faulty.
        ///              (`replica.op_checkpoint` == `replica.op`).
        fn op_head_certain(self: *const Self) bool {
            assert(self.status == .recovering);

            // "op-head < op-checkpoint" is possible if op_checkpoint…head (inclusive) is corrupt.
            if (self.op < self.op_checkpoint()) return false;

            const slot_op_checkpoint = self.journal.slot_for_op(self.op_checkpoint());
            const slot_op_head = self.journal.slot_with_op(self.op).?;
            if (slot_op_head.index == slot_op_checkpoint.index) {
                return self.journal.faulty.count == 0;
            }

            // For the op-head to be faulty, this must be a header that was restored from the
            // superblock VSR headers atop a corrupt slot. We can't trust the head: that corrupt
            // slot may have originally been op that is a wrap ahead.
            if (self.journal.faulty.bit(slot_op_head)) return false;

            // If faulty, this slot may hold either:
            // - op=op_checkpoint, or
            // - op=op_prepare_max
            if (self.journal.faulty.bit(slot_op_checkpoint)) return false;

            const slot_known_range = vsr.SlotRange{
                .head = slot_op_checkpoint,
                .tail = slot_op_head,
            };

            var iterator = self.journal.faulty.bits.iterator(.{ .kind = .set });
            while (iterator.next()) |slot| {
                if (!slot_known_range.contains(.{ .index = slot })) return false;
            }
            return true;
        }

        /// The op of the highest checkpointed prepare.
        pub fn op_checkpoint(self: *const Self) u64 {
            return self.superblock.working.vsr_state.checkpoint.commit_min;
        }

        /// Returns the op that will be `op_checkpoint` after the next checkpoint.
        fn op_checkpoint_next(self: *const Self) u64 {
            assert(vsr.Checkpoint.valid(self.op_checkpoint()));
            assert(self.op_checkpoint() <= self.commit_min);
            assert(self.op_checkpoint() <= self.op or
                self.status == .recovering or self.status == .recovering_head);

            const checkpoint_next = vsr.Checkpoint.checkpoint_after(self.op_checkpoint());
            assert(vsr.Checkpoint.valid(checkpoint_next));
            assert(checkpoint_next > self.op_checkpoint()); // The checkpoint always advances.

            return checkpoint_next;
        }

        /// Returns the next op that will trigger a checkpoint.
        ///
        /// See `op_checkpoint_next` for more detail.
        fn op_checkpoint_next_trigger(self: *const Self) u64 {
            return vsr.Checkpoint.trigger_for_checkpoint(self.op_checkpoint_next()).?;
        }

        /// Returns the highest op that this replica can safely prepare to its WAL.
        ///
        /// Receiving and storing an op higher than `op_prepare_max()` is forbidden;
        /// doing so would overwrite a message (or the slot of a message) that has not yet been
        /// committed and checkpointed.
        fn op_prepare_max(self: *const Self) u64 {
            return vsr.Checkpoint.prepare_max_for_checkpoint(self.op_checkpoint_next()).?;
        }

        /// Returns checkpoint id associated with the op.
        ///
        /// Specifically, returns the checkpoint id corresponding to the checkpoint with:
        ///
        ///   prepare.op > checkpoint_op
        ///   prepare.op ≤ checkpoint_after(checkpoint_op)
        ///
        /// Returns `null` for ops which are too far in the past/future to know their checkpoint
        /// ids.
        fn checkpoint_id_for_op(self: *const Self, op: u64) ?u128 {
            const checkpoint_now = self.op_checkpoint();
            const checkpoint_next_1 = vsr.Checkpoint.checkpoint_after(checkpoint_now);
            const checkpoint_next_2 = vsr.Checkpoint.checkpoint_after(checkpoint_next_1);

            if (op + constants.vsr_checkpoint_interval <= checkpoint_now) {
                // Case 1: op is from a too distant past for us to know its checkpoint id.
                return null;
            }

            if (op <= checkpoint_now) {
                // Case 2: op is from the previous checkpoint whose id we still remember.
                return self.superblock.working.vsr_state.checkpoint.grandparent_checkpoint_id;
            }

            if (op <= checkpoint_next_1) {
                // Case 3: op is in the current checkpoint.
                return self.superblock.working.vsr_state.checkpoint.parent_checkpoint_id;
            }

            if (op <= checkpoint_next_2) {
                // Case 4: op is in the next checkpoint (which we have not checkpointed).
                return self.superblock.working.checkpoint_id();
            }

            // Case 5: op is from the too far future for us to know anything!
            return null;
        }

        /// Returns the oldest op that the replica must/(is permitted to) repair.
        ///
        /// Safety condition: repairing an old op must not overwrite a newer op from the next wrap.
        ///
        /// Availability condition: each committed op must be present either in a quorum of WALs or
        /// it in a quorum of checkpoints.
        ///
        /// If op=trigger+1 is committed, the corresponding checkpoint is durably present on
        /// a quorum of replicas. Repairing all ops since the latest durable checkpoint satisfies
        /// both conditions.
        ///
        /// When called from status=recovering_head or status=recovering, the caller is responsible
        /// for ensuring that replica.op is valid.
        fn op_repair_min(self: *const Self) u64 {
            if (self.status == .recovering) assert(self.solo());
            assert(self.op >= self.op_checkpoint());
            assert(self.op <= self.op_prepare_max());
            assert(self.commit_max >= self.op -| constants.pipeline_prepare_queue_max);

            const repair_min = repair_min: {
                if (self.op_checkpoint() == 0) {
                    break :repair_min 0;
                }

                const op_checkpoint_trigger =
                    vsr.Checkpoint.trigger_for_checkpoint(self.op_checkpoint()).?;
                // After state sync, commit_max might lag behind checkpoint_op.
                maybe(self.commit_max < op_checkpoint_trigger);
                if (self.commit_max > op_checkpoint_trigger) {
                    if (self.op == self.op_checkpoint()) {
                        // Don't allow "op_repair_min > op_head".
                        break :repair_min self.op_checkpoint();
                    }
                    break :repair_min self.op_checkpoint() + 1;
                } else {
                    break :repair_min (self.op_checkpoint() + 1) -|
                        constants.vsr_checkpoint_interval;
                }
            };

            assert(repair_min <= self.op);
            assert(repair_min <= self.commit_min + 1);
            assert(repair_min <= self.op_checkpoint() + 1);
            assert(self.op - repair_min < constants.journal_slot_count);
            assert(self.checkpoint_id_for_op(repair_min) != null);
            return repair_min;
        }

        /// The replica repairs backwards from `commit_max`. But if `commit_max` is too high
        /// (part of the next WAL wrap), then bound it such that uncommitted WAL entries are not
        /// overwritten.
        fn op_repair_max(self: *const Self) u64 {
            assert(self.status != .recovering_head);
            assert(self.op >= self.op_checkpoint());
            assert(self.op <= self.op_prepare_max());
            assert(self.op <= self.commit_max + constants.pipeline_prepare_queue_max);

            return @min(self.commit_max, self.op_prepare_max());
        }

        /// Panics if immediate neighbors in the same view would have a broken hash chain.
        /// Assumes gaps and does not require that a precedes b.
        fn panic_if_hash_chain_would_break_in_the_same_view(
            self: *const Self,
            a: *const Header.Prepare,
            b: *const Header.Prepare,
        ) void {
            assert(a.command == .prepare);
            assert(b.command == .prepare);
            assert(a.cluster == b.cluster);
            if (a.view == b.view and a.op + 1 == b.op and a.checksum != b.parent) {
                assert(a.valid_checksum());
                assert(b.valid_checksum());
                log.err("{}: panic_if_hash_chain_would_break: a: {}", .{ self.replica, a });
                log.err("{}: panic_if_hash_chain_would_break: b: {}", .{ self.replica, b });
                @panic("hash chain would break");
            }
        }

        fn primary_pipeline_prepare(self: *Self, request: Request) void {
            assert(self.status == .normal);
            assert(self.primary());
            assert(self.commit_min == self.commit_max);
            assert(self.commit_max + self.pipeline.queue.prepare_queue.count == self.op);
            assert(!self.pipeline.queue.prepare_queue.full());
            self.pipeline.queue.verify();

            assert(!self.ignore_request_message(request.message));

            defer self.message_bus.unref(request.message);

            log.debug("{}: primary_pipeline_prepare: request checksum={} client={}", .{
                self.replica,
                request.message.header.checksum,
                request.message.header.client,
            });

            // Guard against the wall clock going backwards by taking the max with timestamps issued:
            self.state_machine.prepare_timestamp = @max(
                // The cluster `commit_timestamp` may be ahead of our `prepare_timestamp` because this
                // may be our first prepare as a recently elected primary:
                @max(
                    self.state_machine.prepare_timestamp,
                    self.state_machine.commit_timestamp,
                ) + 1,
                @as(u64, @intCast(request.realtime)),
            );
            assert(self.state_machine.prepare_timestamp > self.state_machine.commit_timestamp);

            switch (request.message.header.operation) {
                .reserved, .root => unreachable,
                .register => {},
                .reconfigure => self.primary_prepare_reconfiguration(request.message),
                else => self.state_machine.prepare(
                    request.message.header.operation.cast(StateMachine),
                    request.message.body(),
                ),
            }
            const prepare_timestamp = self.state_machine.prepare_timestamp;

            // Reuse the Request message as a Prepare message by replacing the header.
            const message = request.message.base().build(.prepare);

            // Copy the header to the stack before overwriting it to avoid UB.
            const request_header: Header.Request = request.message.header.*;

            const checkpoint_id = checkpoint_id: {
                if (self.op + 1 <= self.op_checkpoint_next()) {
                    break :checkpoint_id self.superblock.working.vsr_state.checkpoint.parent_checkpoint_id;
                } else {
                    break :checkpoint_id self.superblock.working.checkpoint_id();
                }
            };

            const latest_entry = self.journal.header_with_op(self.op).?;
            message.header.* = Header.Prepare{
                .cluster = self.cluster,
                .size = request_header.size,
                .view = self.view,
                .command = .prepare,
                .replica = self.replica,
                .parent = latest_entry.checksum,
                .client = request_header.client,
                .request_checksum = request_header.checksum,
                .checkpoint_id = checkpoint_id,
                .op = self.op + 1,
                .commit = self.commit_max,
                .timestamp = timestamp: {
                    // When running in AOF recovery mode, we allow clients to set a timestamp
                    // explicitly, but they can still pass in 0.
                    if (constants.aof_recovery) {
                        if (request_header.timestamp == 0) {
                            break :timestamp prepare_timestamp;
                        } else {
                            break :timestamp request_header.timestamp;
                        }
                    } else {
                        break :timestamp prepare_timestamp;
                    }
                },
                .request = request_header.request,
                .operation = request_header.operation,
            };
            message.header.set_checksum_body(message.body());
            message.header.set_checksum();

            log.debug("{}: primary_pipeline_prepare: prepare {}", .{ self.replica, message.header.checksum });

            if (self.primary_pipeline_pending()) |_| {
                // Do not restart the prepare timeout as it is already ticking for another prepare.
                const previous = self.pipeline.queue.prepare_queue.tail_ptr().?;
                assert(previous.message.header.checksum == message.header.parent);
                assert(self.prepare_timeout.ticking);
                assert(self.primary_abdicate_timeout.ticking);
            } else {
                assert(!self.prepare_timeout.ticking);
                assert(!self.primary_abdicate_timeout.ticking);
                self.prepare_timeout.start();
                self.primary_abdicate_timeout.start();
            }
            self.pipeline.queue.push_prepare(message);
            self.on_prepare(message);

            // We expect `on_prepare()` to increment `self.op` to match the primary's latest prepare:
            // This is critical to ensure that pipelined prepares do not receive the same op number.
            assert(self.op == message.header.op);
        }

        fn primary_prepare_reconfiguration(
            self: *const Self,
            request: *Message.Request,
        ) void {
            assert(self.primary());
            assert(request.header.command == .request);
            assert(request.header.operation == .reconfigure);
            assert(request.header.size == @sizeOf(vsr.Header) + @sizeOf(vsr.ReconfigurationRequest));
            const reconfiguration_request = std.mem.bytesAsValue(
                vsr.ReconfigurationRequest,
                request.body()[0..@sizeOf(vsr.ReconfigurationRequest)],
            );
            reconfiguration_request.*.result = reconfiguration_request.validate(.{
                .members = &self.superblock.working.vsr_state.members,
                .epoch = 0,
                .replica_count = self.replica_count,
                .standby_count = self.standby_count,
            });
            assert(reconfiguration_request.result != .reserved);
        }

        /// Returns the next prepare in the pipeline waiting for a quorum.
        /// Returns null when the pipeline is empty.
        /// Returns null when the pipeline is nonempty but all prepares have a quorum.
        fn primary_pipeline_pending(self: *const Self) ?*const Prepare {
            assert(self.status == .normal);
            assert(self.primary());

            var prepares = self.pipeline.queue.prepare_queue.iterator();
            while (prepares.next_ptr()) |prepare| {
                assert(prepare.message.header.command == .prepare);
                if (!prepare.ok_quorum_received) {
                    return prepare;
                }
            } else {
                return null;
            }
        }

        fn pipeline_prepare_by_op_and_checksum(
            self: *Self,
            op: u64,
            checksum: u128,
        ) ?*Message.Prepare {
            return switch (self.pipeline) {
                .cache => |*cache| cache.prepare_by_op_and_checksum(op, checksum),
                .queue => |*queue| if (queue.prepare_by_op_and_checksum(op, checksum)) |prepare|
                    prepare.message
                else
                    null,
            };
        }

        /// Repair. Each step happens in sequence — step n+1 executes when step n is done.
        ///
        /// 1. If we are a backup and have fallen too far behind the primary, initiate state sync.
        /// 2. Advance the head op to `op_repair_max = min(op_prepare_max, commit_max)`.
        ///    To advance the head op we request+await a SV. Either:
        ///    - the SV's "hook" headers include op_prepare_max (if we are ≤1 wrap behind), or
        ///    - the SV is too far ahead, so we will fall back from WAL repair to state sync.
        /// 3. Acquire missing or disconnected headers in reverse chronological order, backwards from
        ///    op_repair_max.
        ///    A header is disconnected if it breaks the chain with its newer neighbor to the right.
        /// 4. Repair missing or corrupt prepares in chronological order.
        /// 5. Commit up to op_repair_max. If committing triggers a checkpoint, op_repair_max
        ///    increases, so go to step 1 and repeat.
        fn repair(self: *Self) void {
            if (!self.repair_timeout.ticking) {
                log.debug("{}: repair: ignoring (optimistic, not ticking)", .{self.replica});
                return;
            }

            self.repair_timeout.reset();

            assert(self.status == .normal or self.status == .view_change);
            assert(self.repairs_allowed());

            assert(self.op_checkpoint() <= self.op);
            assert(self.op_checkpoint() <= self.commit_min);
            assert(self.commit_min <= self.op);
            assert(self.commit_min <= self.commit_max);
            assert(self.commit_max >= self.op -| constants.pipeline_prepare_queue_max);
            assert(self.journal.header_with_op(self.op) != null);

            self.sync_reclaim_tables();

            // Request outstanding possibly committed headers to advance our op number:
            // This handles the case of an idle cluster, where a backup will not otherwise advance.
            // This is not required for correctness, but for durability.
            if (self.op < self.op_repair_max() or
                (self.status == .normal and self.op < self.view_headers.array.get(0).op))
            {
                assert(!self.solo());
                assert(self.replica != self.primary_index(self.view));

                log.debug(
                    "{}: repair: break: view={} break={}..{} " ++
                        "(commit={}..{} op={} view_headers_op={})",
                    .{
                        self.replica,
                        self.view,
                        self.op + 1,
                        self.op_repair_max(),

                        self.commit_min,
                        self.commit_max,
                        self.op,
                        self.view_headers.array.get(0).op,
                    },
                );

                self.send_header_to_replica(
                    self.primary_index(self.view),
                    @bitCast(Header.RequestStartView{
                        .command = .request_start_view,
                        .cluster = self.cluster,
                        .replica = self.replica,
                        .view = self.view,
                        .nonce = self.nonce,
                    }),
                );
            }

            if (self.op < self.op_repair_max()) return;

            // Request any missing or disconnected headers:
            if (self.journal.find_latest_headers_break_between(
                self.op_repair_min(),
                self.op,
            )) |range| {
                assert(!self.solo());
                assert(range.op_min >= self.op_repair_min());
                assert(range.op_max < self.op);

                log.debug(
                    "{}: repair: break: view={} break={}..{} (commit={}..{} op={})",
                    .{
                        self.replica,
                        self.view,
                        range.op_min,
                        range.op_max,
                        self.commit_min,
                        self.commit_max,
                        self.op,
                    },
                );

                self.send_header_to_replica(
                    self.choose_any_other_replica(),
                    @bitCast(Header.RequestHeaders{
                        .command = .request_headers,
                        .cluster = self.cluster,
                        .replica = self.replica,
                        .op_min = range.op_min,
                        .op_max = range.op_max,
                    }),
                );
                return;
            }
            assert(self.valid_hash_chain_between(self.op_repair_min(), self.op));

            if (self.journal.dirty.count > 0) {
                // Request and repair any dirty or faulty prepares.
                self.repair_prepares();
            } else if (self.client_replies.faulty.findFirstSet()) |slot| {
                // After we have all prepares, repair replies.
                const entry = &self.client_sessions.entries[slot];
                assert(!self.client_sessions.entries_free.isSet(slot));
                assert(entry.session != 0);
                assert(entry.header.size > @sizeOf(Header));

                self.send_header_to_replica(
                    self.choose_any_other_replica(),
                    @bitCast(Header.RequestReply{
                        .command = .request_reply,
                        .cluster = self.cluster,
                        .replica = self.replica,
                        .reply_client = entry.header.client,
                        .reply_op = entry.header.op,
                        .reply_checksum = entry.header.checksum,
                    }),
                );
            }

            if (self.commit_min < self.commit_max) {
                // Try to the commit prepares we already have, even if we don't have all of them.
                // This helps when a replica is recovering from a crash and has a mostly intact
                // journal, with just some prepares missing. We do have the headers and know
                // that they form a valid hashchain. Committing may discover more faulty prepares
                // and drive further repairs.
                assert(!self.solo());
                self.commit_journal();
            }

            if (self.status == .view_change and self.primary_index(self.view) == self.replica) {
                if (self.journal.dirty.count == 0 and self.commit_min == self.commit_max) {
                    if (self.commit_stage != .idle) {
                        // If we still have a commit running, we started it the last time we were
                        // primary, and its still running. Wait for it to finish before repairing
                        // the pipeline so that it doesn't wind up in the new pipeline.
                        assert(self.commit_prepare.?.header.op >= self.commit_min);
                        assert(self.commit_prepare.?.header.op <= self.commit_min + 1);
                        assert(self.commit_prepare.?.header.view < self.view);
                        return;
                    }

                    // Repair the pipeline, which may discover faulty prepares and drive more repairs.
                    switch (self.primary_repair_pipeline()) {
                        // primary_repair_pipeline() is already working.
                        .busy => {},
                        .done => self.primary_start_view_as_the_new_primary(),
                    }
                }
            }
        }

        /// Decide whether or not to insert or update a header:
        ///
        /// A repair may never advance or replace `self.op` (critical for correctness):
        ///
        /// Repairs must always backfill in behind `self.op` but may never advance `self.op`.
        /// Otherwise, a split-brain primary may reapply an op that was removed through a view
        /// change, which could be committed by a higher `commit_max` number in a commit message.
        ///
        /// See this commit message for an example:
        /// https://github.com/coilhq/tigerbeetle/commit/6119c7f759f924d09c088422d5c60ac6334d03de
        ///
        /// Our guiding principles around repairs in general:
        ///
        /// * The latest op makes sense of everything else and must not be replaced with a different
        /// op or advanced except by the primary in the current view.
        ///
        /// * Do not jump to a view in normal status without receiving a start_view message.
        ///
        /// * Do not commit until the hash chain between `self.commit_min` and `self.op` is fully
        /// connected, to ensure that all the ops in this range are correct.
        ///
        /// * Ensure that `self.commit_max` is never advanced for a newer view without first
        /// receiving a start_view message, otherwise `self.commit_max` may refer to different ops.
        ///
        /// * Ensure that `self.op` is never advanced by a repair since repairs may occur in a view
        /// change where the view has not yet started.
        ///
        /// * Do not assume that an existing op with a older viewstamp can be replaced by an op with
        /// a newer viewstamp, but only compare ops in the same view or with reference to the chain.
        /// See Figure 3.7 on page 41 in Diego Ongaro's Raft thesis for an example of where an op
        /// with an older view number may be committed instead of an op with a newer view number:
        /// http://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf.
        ///
        /// * Do not replace an op belonging to the current WAL wrap with an op belonging to a
        ///   previous wrap. In other words, don't repair checkpointed ops.
        ///
        fn repair_header(self: *Self, header: *const Header.Prepare) bool {
            assert(self.status == .normal or self.status == .view_change);
            assert(header.valid_checksum());
            assert(header.invalid() == null);
            assert(header.command == .prepare);

            if (header.view > self.view) {
                log.debug("{}: repair_header: op={} checksum={} view={} (newer view)", .{
                    self.replica,
                    header.op,
                    header.checksum,
                    header.view,
                });
                return false;
            }

            if (header.op > self.op) {
                log.debug("{}: repair_header: op={} checksum={} (advances hash chain head)", .{
                    self.replica,
                    header.op,
                    header.checksum,
                });
                return false;
            } else if (header.op == self.op and !self.journal.has(header)) {
                assert(self.journal.header_with_op(self.op) != null);
                log.debug("{}: repair_header: op={} checksum={} (changes hash chain head)", .{
                    self.replica,
                    header.op,
                    header.checksum,
                });
                return false;
            }

            if (header.op < self.op_repair_min()) {
                // Slots too far back belong to the next wrap of the log.
                log.debug("{}: repair_header: false (precedes op_repair_min={})", .{
                    self.replica,
                    self.op_repair_min(),
                });
                return false;
            }

            if (self.journal.header_for_prepare(header)) |existing| {
                if (existing.checksum == header.checksum) {
                    if (self.journal.has_clean(header)) {
                        log.debug("{}: repair_header: op={} checksum={} (checksum clean)", .{
                            self.replica,
                            header.op,
                            header.checksum,
                        });
                        return false;
                    } else {
                        log.debug("{}: repair_header: op={} checksum={} (checksum dirty)", .{
                            self.replica,
                            header.op,
                            header.checksum,
                        });
                    }
                } else if (existing.view == header.view) {
                    // The journal must have wrapped:
                    // We expect that the same view and op would have had the same checksum.
                    assert(existing.op != header.op);
                    if (existing.op > header.op) {
                        log.debug("{}: repair_header: op={} checksum={} (same view, newer op)", .{
                            self.replica,
                            header.op,
                            header.checksum,
                        });
                    } else {
                        log.debug("{}: repair_header: op={} checksum={} (same view, older op)", .{
                            self.replica,
                            header.op,
                            header.checksum,
                        });
                    }
                } else {
                    assert(existing.view != header.view);
                    assert(existing.op == header.op or existing.op != header.op);

                    log.debug("{}: repair_header: op={} checksum={} (different view)", .{
                        self.replica,
                        header.op,
                        header.checksum,
                    });
                }
            } else {
                log.debug("{}: repair_header: op={} checksum={} (gap)", .{
                    self.replica,
                    header.op,
                    header.checksum,
                });
            }

            assert(header.op < self.op or
                self.journal.header_with_op(self.op).?.checksum == header.checksum);

            if (!self.repair_header_would_connect_hash_chain(header)) {
                // We cannot replace this op until we are sure that this would not:
                // 1. undermine any prior prepare_ok guarantee made to the primary, and
                // 2. leak stale ops back into our in-memory headers (and so into a view change).
                log.debug("{}: repair_header: op={} checksum={} (disconnected from hash chain)", .{
                    self.replica,
                    header.op,
                    header.checksum,
                });
                return false;
            }
            assert(header.checkpoint_id == self.checkpoint_id_for_op(header.op).?);

            // If we already committed this op, the repair must be the identical message.
            if (self.op_checkpoint() < header.op and header.op <= self.commit_min) {
                assert(self.journal.has(header));
            }

            self.journal.set_header_as_dirty(header);
            return true;
        }

        /// If we repair this header, would this connect the hash chain through to the latest op?
        /// This offers a strong guarantee that may be used to replace an existing op.
        ///
        /// Here is an example of what could go wrong if we did not check for complete connection:
        ///
        /// 1. We do a prepare that's going to be committed.
        /// 2. We do a stale prepare to the right, ignoring the hash chain break to the left.
        /// 3. We do another stale prepare that replaces the first since it connects to the second.
        ///
        /// This would violate our quorum replication commitment to the primary.
        /// The mistake in this example was not that we ignored the break to the left, which we must
        /// do to repair reordered ops, but that we did not check for connection to the right.
        fn repair_header_would_connect_hash_chain(
            self: *Self,
            header: *const Header.Prepare,
        ) bool {
            var entry = header;

            while (entry.op < self.op) {
                if (self.journal.next_entry(entry)) |next| {
                    if (entry.checksum == next.parent) {
                        assert(entry.view <= next.view);
                        assert(entry.op + 1 == next.op);
                        entry = next;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }

            assert(entry.op == self.op);
            assert(entry.checksum == self.journal.header_with_op(self.op).?.checksum);
            return true;
        }

        /// Reads prepares into the pipeline (before we start the view as the new primary).
        fn primary_repair_pipeline(self: *Self) enum { done, busy } {
            assert(self.status == .view_change);
            assert(self.primary_index(self.view) == self.replica);
            assert(self.commit_max == self.commit_min);
            assert(self.commit_max <= self.op);
            assert(self.journal.dirty.count == 0);
            assert(self.pipeline == .cache);

            if (self.pipeline_repairing) {
                log.debug("{}: primary_repair_pipeline: already repairing...", .{self.replica});
                return .busy;
            }

            if (self.primary_repair_pipeline_op()) |_| {
                log.debug("{}: primary_repair_pipeline: repairing", .{self.replica});
                assert(!self.pipeline_repairing);
                self.pipeline_repairing = true;
                self.primary_repair_pipeline_read();
                return .busy;
            }

            // All prepares needed to reconstruct the pipeline queue are now available in the cache.
            return .done;
        }

        fn primary_repair_pipeline_done(self: *Self) PipelineQueue {
            assert(self.status == .view_change);
            assert(self.primary_index(self.view) == self.replica);
            assert(self.commit_max == self.commit_min);
            assert(self.commit_max <= self.op);
            assert(self.commit_max >= self.op -| constants.pipeline_prepare_queue_max);
            assert(self.journal.dirty.count == 0);
            assert(self.valid_hash_chain_between(self.op_repair_min(), self.op));
            assert(self.pipeline == .cache);
            assert(!self.pipeline_repairing);
            assert(self.primary_repair_pipeline() == .done);

            var pipeline_queue = PipelineQueue{};
            var op = self.commit_max + 1;
            var parent = self.journal.header_with_op(self.commit_max).?.checksum;
            while (op <= self.op) : (op += 1) {
                const journal_header = self.journal.header_with_op(op).?;
                assert(journal_header.op == op);
                assert(journal_header.parent == parent);

                const prepare =
                    self.pipeline.cache.prepare_by_op_and_checksum(op, journal_header.checksum).?;
                assert(prepare.header.op == op);
                assert(prepare.header.op <= self.op);
                assert(prepare.header.checksum == journal_header.checksum);
                assert(prepare.header.parent == parent);
                assert(self.journal.has(prepare.header));

                pipeline_queue.push_prepare(prepare);
                parent = prepare.header.checksum;
            }
            assert(self.commit_max + pipeline_queue.prepare_queue.count == self.op);

            pipeline_queue.verify();
            return pipeline_queue;
        }

        /// Returns the next `op` number that needs to be read into the pipeline.
        /// Returns null when all necessary prepares are in the pipeline cache.
        fn primary_repair_pipeline_op(self: *const Self) ?u64 {
            assert(self.status == .view_change);
            assert(self.primary_index(self.view) == self.replica);
            assert(self.commit_max == self.commit_min);
            assert(self.commit_max <= self.op);
            assert(self.pipeline == .cache);

            var op = self.commit_max + 1;
            while (op <= self.op) : (op += 1) {
                const op_header = self.journal.header_with_op(op).?;
                if (!self.pipeline.cache.contains_header(op_header)) {
                    return op;
                }
            }
            return null;
        }

        fn primary_repair_pipeline_read(self: *Self) void {
            assert(self.status == .view_change);
            assert(self.primary_index(self.view) == self.replica);
            assert(self.commit_max == self.commit_min);
            assert(self.commit_max <= self.op);
            assert(self.pipeline == .cache);
            assert(self.pipeline_repairing);

            const op = self.primary_repair_pipeline_op().?;
            const op_checksum = self.journal.header_with_op(op).?.checksum;
            log.debug("{}: primary_repair_pipeline_read: op={} checksum={}", .{
                self.replica,
                op,
                op_checksum,
            });
            self.journal.read_prepare(repair_pipeline_read_callback, op, op_checksum, null);
        }

        fn repair_pipeline_read_callback(
            self: *Self,
            prepare: ?*Message.Prepare,
            destination_replica: ?u8,
        ) void {
            assert(destination_replica == null);

            assert(self.pipeline_repairing);
            self.pipeline_repairing = false;

            if (prepare == null) {
                log.debug("{}: repair_pipeline_read_callback: prepare == null", .{self.replica});
                return;
            }

            // Our state may have advanced significantly while we were reading from disk.
            if (self.status != .view_change) {
                assert(self.primary_index(self.view) != self.replica);

                log.debug("{}: repair_pipeline_read_callback: no longer in view change status", .{
                    self.replica,
                });
                return;
            }

            if (self.primary_index(self.view) != self.replica) {
                log.debug("{}: repair_pipeline_read_callback: no longer primary", .{self.replica});
                return;
            }

            // We may even be several views ahead and may now have a completely different pipeline.
            const op = self.primary_repair_pipeline_op() orelse {
                log.debug("{}: repair_pipeline_read_callback: pipeline changed", .{self.replica});
                return;
            };

            assert(op > self.commit_max);
            assert(op <= self.op);

            if (prepare.?.header.op != op) {
                log.debug("{}: repair_pipeline_read_callback: op changed", .{self.replica});
                return;
            }

            if (prepare.?.header.checksum != self.journal.header_with_op(op).?.checksum) {
                log.debug("{}: repair_pipeline_read_callback: checksum changed", .{self.replica});
                return;
            }

            assert(self.status == .view_change);
            assert(self.primary_index(self.view) == self.replica);

            log.debug("{}: repair_pipeline_read_callback: op={} checksum={}", .{
                self.replica,
                prepare.?.header.op,
                prepare.?.header.checksum,
            });

            const prepare_evicted = self.pipeline.cache.insert(prepare.?.ref());
            if (prepare_evicted) |message_evicted| self.message_bus.unref(message_evicted);

            if (self.primary_repair_pipeline_op()) |_| {
                assert(!self.pipeline_repairing);
                self.pipeline_repairing = true;
                self.primary_repair_pipeline_read();
            } else {
                self.repair();
            }
        }

        fn repair_prepares(self: *Self) void {
            assert(self.status == .normal or self.status == .view_change);
            assert(self.repairs_allowed());
            assert(self.journal.dirty.count > 0);
            assert(self.op >= self.commit_min);
            assert(self.op - self.commit_min <= constants.journal_slot_count);
            assert(self.op - self.op_checkpoint() <= constants.journal_slot_count);
            assert(self.valid_hash_chain_between(self.op_repair_min(), self.op));

            if (self.op < constants.journal_slot_count) {
                // The op is known, and this is the first WAL cycle.
                // Therefore, any faulty ops to the right of `replica.op` are corrupt reserved
                // entries from the initial format, or corrupt prepares which were since truncated.
                var op: usize = self.op + 1;
                while (op < constants.journal_slot_count) : (op += 1) {
                    const slot = self.journal.slot_for_op(op);
                    assert(slot.index == op);

                    if (self.journal.faulty.bit(slot)) {
                        assert(self.journal.headers[op].operation == .reserved);
                        assert(self.journal.headers_redundant[op].operation == .reserved);
                        self.journal.dirty.clear(slot);
                        self.journal.faulty.clear(slot);
                        log.debug("{}: repair_prepares: remove slot={} " ++
                            "(faulty, op known, first cycle)", .{
                            self.replica,
                            slot.index,
                        });
                    }
                }
            }

            // Request enough prepares to utilize our max IO depth:
            var budget = self.journal.writes.available();
            if (budget == 0) {
                log.debug("{}: repair_prepares: waiting for IOP", .{self.replica});
                return;
            }

            // Repair prepares in chronological order. Older prepares will be overwritten by the
            // cluster earlier, so we prioritize their repair. This also encourages concurrent
            // commit/repair.
            var op = self.op_repair_min();
            while (op <= self.op) : (op += 1) {
                const slot = self.journal.slot_with_op(op).?;
                if (self.journal.dirty.bit(slot)) {
                    // Rebroadcast outstanding `request_prepare` every `repair_timeout` tick.
                    // Continue to request prepares until our budget is depleted.
                    if (self.repair_prepare(op)) {
                        budget -= 1;
                        if (budget == 0) {
                            log.debug("{}: repair_prepares: request budget used", .{self.replica});
                            return;
                        }
                    }
                } else {
                    assert(!self.journal.faulty.bit(slot));
                }
            }

            // Clean up out-of-bounds dirty slots so repair() can finish.
            const slots_repaired = vsr.SlotRange{
                .head = self.journal.slot_for_op(self.op_repair_min()),
                .tail = self.journal.slot_with_op(self.op).?,
            };
            var slot_index: usize = 0;
            while (slot_index < constants.journal_slot_count) : (slot_index += 1) {
                const slot = self.journal.slot_for_op(slot_index);
                if (slots_repaired.head.index == slots_repaired.tail.index or
                    slots_repaired.contains(slot))
                {
                    // In-bounds — handled by the previous loop. The slot is either already
                    // repaired, or we sent a request_prepare and are waiting for a reply.
                } else {
                    // This op must be either:
                    // - less-than-or-equal-to `op_checkpoint` — we committed before
                    //   checkpointing, but the entry in our WAL was found corrupt after
                    //   recovering from a crash.
                    // - or (indistinguishably) this might originally have been an op greater
                    //   than replica.op, which was truncated, but is now corrupt.
                    if (self.journal.dirty.bit(slot)) {
                        log.debug("{}: repair_prepares: remove slot={} " ++
                            "(faulty, precedes checkpoint)", .{
                            self.replica,
                            slot.index,
                        });
                        self.journal.remove_entry(slot);
                    }
                }
            }
        }

        /// During a view change, for uncommitted ops, which are few, we optimize for latency:
        ///
        /// * request a `prepare` from all backups in parallel,
        /// * repair as soon as we get a `prepare`
        ///
        /// For committed ops, which represent the bulk of ops, we optimize for throughput:
        ///
        /// * have multiple requests in flight to prime the repair queue,
        /// * rotate these requests across the cluster round-robin,
        /// * to spread the load across connected peers,
        /// * to take advantage of each peer's outgoing bandwidth, and
        /// * to parallelize disk seeks and disk read bandwidth.
        ///
        /// This is effectively "many-to-one" repair, where a single replica recovers using the
        /// resources of many replicas, for faster recovery.
        fn repair_prepare(self: *Self, op: u64) bool {
            const slot = self.journal.slot_with_op(op).?;
            const checksum = self.journal.header_with_op(op).?.checksum;

            assert(self.status == .normal or self.status == .view_change);
            assert(self.repairs_allowed());
            assert(self.journal.dirty.bit(slot));

            // We may be appending to or repairing the journal concurrently.
            // We do not want to re-request any of these prepares unnecessarily.
            if (self.journal.writing(op, checksum)) {
                log.debug("{}: repair_prepare: op={} checksum={} (already writing)", .{
                    self.replica,
                    op,
                    checksum,
                });
                return false;
            }

            // The message may be available in the local pipeline.
            // For example (replica_count=3):
            // 1. View=1: Replica 1 is primary, and prepares op 5. The local write fails.
            // 2. Time passes. The view changes (e.g. due to a timeout)…
            // 3. View=4: Replica 1 is primary again, and is repairing op 5
            //    (which is still in the pipeline).
            //
            // Using the pipeline to repair is faster than a `request_prepare`.
            // Also, messages in the pipeline are never corrupt.
            if (self.pipeline_prepare_by_op_and_checksum(op, checksum)) |prepare| {
                assert(prepare.header.op == op);
                assert(prepare.header.checksum == checksum);

                if (self.solo()) {
                    // This op won't start writing until all ops in the pipeline preceding it have
                    // been written.
                    log.debug("{}: repair_prepare: op={} checksum={} (serializing append)", .{
                        self.replica,
                        op,
                        checksum,
                    });
                    const pipeline_head = self.pipeline.queue.prepare_queue.head_ptr().?;
                    assert(pipeline_head.message.header.op < op);
                    return false;
                }

                log.debug("{}: repair_prepare: op={} checksum={} (from pipeline)", .{
                    self.replica,
                    op,
                    checksum,
                });
                self.write_prepare(prepare, .pipeline);
                return true;
            }

            const request_prepare = Header.RequestPrepare{
                .command = .request_prepare,
                .cluster = self.cluster,
                .replica = self.replica,
                .prepare_op = op,
                .prepare_checksum = checksum,
            };

            if (self.status == .view_change and op > self.commit_max) {
                // Only the primary is allowed to do repairs in a view change:
                assert(self.primary_index(self.view) == self.replica);

                const reason = if (self.journal.faulty.bit(slot)) "faulty" else "dirty";
                log.debug(
                    "{}: repair_prepare: op={} checksum={} (uncommitted, {s}, view_change)",
                    .{
                        self.replica,
                        op,
                        checksum,
                        reason,
                    },
                );

                self.send_header_to_other_replicas(@bitCast(request_prepare));
            } else {
                const nature = if (op > self.commit_max) "uncommitted" else "committed";
                const reason = if (self.journal.faulty.bit(slot)) "faulty" else "dirty";
                log.debug("{}: repair_prepare: op={} checksum={} ({s}, {s})", .{
                    self.replica,
                    op,
                    checksum,
                    nature,
                    reason,
                });

                self.send_header_to_replica(
                    self.choose_any_other_replica(),
                    @bitCast(request_prepare),
                );
            }

            return true;
        }

        fn repairs_allowed(self: *const Self) bool {
            switch (self.status) {
                .view_change => {
                    if (self.do_view_change_quorum) {
                        assert(self.primary_index(self.view) == self.replica);
                        return true;
                    } else {
                        return false;
                    }
                },
                .normal => return true,
                else => return false,
            }
        }

        /// Replaces the header if the header is different and at least op_repair_min.
        /// The caller must ensure that the header is trustworthy (part of the current view's log).
        fn replace_header(self: *Self, header: *const Header.Prepare) void {
            assert(self.status == .normal or self.status == .view_change or
                self.status == .recovering_head);
            assert(self.op_checkpoint() <= self.commit_min);

            assert(header.valid_checksum());
            assert(header.invalid() == null);
            assert(header.command == .prepare);
            assert(header.view <= self.view);
            assert(header.op <= self.op); // Never advance the op.
            assert(header.op <= self.op_prepare_max());

            // If we already committed this op, the repair must be the identical message.
            if (self.op_checkpoint() < header.op and header.op <= self.commit_min) {
                assert(self.journal.has(header));
            }

            if (header.op < self.op_repair_min()) return;

            // We must not set an op as dirty if we already have it exactly because:
            // 1. this would trigger a repair and delay the view change, or worse,
            // 2. prevent repairs to another replica when we have the op.
            if (!self.journal.has(header)) self.journal.set_header_as_dirty(header);
        }

        /// Replicates to the next replica in the configuration (until we get back to the primary):
        /// Replication starts and ends with the primary, we never forward back to the primary.
        /// Does not flood the network with prepares that have already committed.
        /// Replication to standbys works similarly, jumping off the replica just before primary.
        /// TODO Use recent heartbeat data for next replica to leapfrog if faulty (optimization).
        fn replicate(self: *Self, message: *Message.Prepare) void {
            assert(self.status == .normal);
            assert(message.header.command == .prepare);
            assert(message.header.view == self.view);
            assert(message.header.op == self.op);

            if (message.header.op <= self.commit_max) {
                log.debug("{}: replicate: not replicating (committed)", .{self.replica});
                return;
            }

            const next = next: {
                // Replication in the ring of active replicas.
                if (!self.standby()) {
                    const next_replica = @mod(self.replica + 1, self.replica_count);
                    if (next_replica != self.primary_index(message.header.view)) break :next next_replica;
                }

                if (self.standby_count > 0) {
                    const first_standby = self.standby_index_to_replica(message.header.view);
                    // Jump-off point from the ring of active replicas to the ring of standbys.
                    if (!self.standby()) break :next first_standby;

                    // Replication across sandbys.
                    const my_index = self.standby_replica_to_index(self.replica);
                    const next_standby = self.standby_index_to_replica(my_index + 1);
                    if (next_standby != first_standby) break :next next_standby;
                }

                log.debug("{}: replicate: not replicating (completed)", .{self.replica});
                return;
            };
            assert(next != self.replica);
            assert(next < self.node_count);
            if (self.standby()) assert(next >= self.replica_count);

            log.debug("{}: replicate: replicating to replica {}", .{ self.replica, next });
            self.send_message_to_replica(next, message);
        }

        /// Conversions between usual `self.replica` and "nth standby" coordinate spaces.
        /// We arrange standbys into a logical ring for replication.
        fn standby_index_to_replica(self: *const Self, index: u32) u8 {
            assert(self.standby_count > 0);
            return self.replica_count + @as(u8, @intCast(@mod(index, self.standby_count)));
        }

        fn standby_replica_to_index(self: *const Self, replica: u8) u32 {
            assert(self.standby_count > 0);
            assert(replica >= self.replica_count);
            assert(replica < self.node_count);
            return replica - self.replica_count;
        }

        fn reset_quorum_messages(self: *Self, messages: *DVCQuorumMessages, command: Command) void {
            assert(messages.len == constants.replicas_max);
            var view: ?u32 = null;
            var count: usize = 0;
            for (messages, 0..) |*received, replica| {
                if (received.*) |message| {
                    assert(replica < self.replica_count);
                    assert(message.header.command == command);
                    assert(message.header.replica == replica);
                    // We may have transitioned into a newer view:
                    // However, all messages in the quorum should have the same view.
                    assert(message.header.view <= self.view);
                    if (view) |v| {
                        assert(message.header.view == v);
                    } else {
                        view = message.header.view;
                    }

                    self.message_bus.unref(message);
                    count += 1;
                }
                received.* = null;
            }
            assert(count <= self.replica_count);
            log.debug("{}: reset {} {s} message(s) from view={?}", .{
                self.replica,
                count,
                @tagName(command),
                view,
            });
        }

        fn reset_quorum_counter(self: *Self, counter: *QuorumCounter) void {
            var counter_iterator = counter.iterator(.{});
            while (counter_iterator.next()) |replica| {
                assert(replica < self.replica_count);
            }

            counter.* = quorum_counter_null;
            assert(counter.count() == 0);

            var replica: usize = 0;
            while (replica < self.replica_count) : (replica += 1) {
                assert(!counter.isSet(replica));
            }
        }

        fn reset_quorum_do_view_change(self: *Self) void {
            self.reset_quorum_messages(&self.do_view_change_from_all_replicas, .do_view_change);
            self.do_view_change_quorum = false;
        }

        fn reset_quorum_start_view_change(self: *Self) void {
            self.reset_quorum_counter(&self.start_view_change_from_all_replicas);
        }

        fn send_prepare_ok(self: *Self, header: *const Header.Prepare) void {
            assert(header.command == .prepare);
            assert(header.cluster == self.cluster);
            assert(header.replica == self.primary_index(header.view));
            assert(header.view <= self.view);
            assert(header.op <= self.op or header.view < self.view);

            if (self.status != .normal) {
                log.debug("{}: send_prepare_ok: not sending ({})", .{ self.replica, self.status });
                return;
            }

            if (header.op > self.op) {
                assert(header.view < self.view);
                // An op may be reordered concurrently through a view change while being journalled:
                log.debug("{}: send_prepare_ok: not sending (reordered)", .{self.replica});
                return;
            }

            if (self.syncing != .idle) {
                log.debug("{}: send_prepare_ok: not sending (sync_status={s})", .{
                    self.replica,
                    @tagName(self.syncing),
                });
                return;
            }

            assert(self.status == .normal);
            // After a view change, replicas send prepare_oks for ops with older views.
            // However, we only send to the primary of the current view (see below where we send).
            assert(header.view <= self.view);
            assert(header.op <= self.op);

            if (self.journal.has_clean(header)) {
                log.debug("{}: send_prepare_ok: op={} checksum={}", .{
                    self.replica,
                    header.op,
                    header.checksum,
                });

                if (self.standby()) return;

                const checkpoint_id = self.checkpoint_id_for_op(header.op) orelse {
                    log.debug("{}: send_prepare_ok: not sending (old)", .{self.replica});
                    return;
                };
                assert(checkpoint_id == header.checkpoint_id);

                // It is crucial that replicas stop accepting prepare messages from earlier views
                // once they start the view change protocol. Without this constraint, the system
                // could get into a state in which there are two active primaries: the old one,
                // which hasn't failed but is merely slow or not well connected to the network, and
                // the new one. If a replica sent a prepare_ok message to the old primary after
                // sending its log to the new one, the old primary might commit an operation that
                // the new primary doesn't learn about in the do_view_change messages.

                // We therefore only ever send to the primary of the current view, never to the
                // primary of the prepare header's view:
                self.send_header_to_replica(
                    self.primary_index(self.view),
                    @bitCast(Header.PrepareOk{
                        .command = .prepare_ok,
                        .checkpoint_id = checkpoint_id,
                        .parent = header.parent,
                        .client = header.client,
                        .prepare_checksum = header.checksum,
                        .request = header.request,
                        .cluster = self.cluster,
                        .replica = self.replica,
                        .epoch = header.epoch,
                        .view = self.view,
                        .op = header.op,
                        .commit = header.commit,
                        .timestamp = header.timestamp,
                        .operation = header.operation,
                    }),
                );
            } else {
                log.debug("{}: send_prepare_ok: not sending (dirty)", .{self.replica});
                return;
            }
        }

        fn send_prepare_oks_after_view_change(self: *Self) void {
            assert(self.status == .normal);

            var op = self.commit_max + 1;
            while (op <= self.op) : (op += 1) {
                // We may have breaks or stale headers in our uncommitted chain here. However:
                // * being able to send what we have will allow the pipeline to commit earlier, and
                // * the primary will drop any prepare_ok for a prepare not in the pipeline.
                // This is safe only because the primary can verify against the prepare checksum.
                if (self.journal.header_with_op(op)) |header| {
                    self.send_prepare_ok(header);
                    defer self.flush_loopback_queue();
                }
            }
        }

        fn send_start_view_change(self: *Self) void {
            assert(self.status == .normal or self.status == .view_change);
            assert(!self.solo());

            if (self.standby()) return;

            const header = Header.StartViewChange{
                .command = .start_view_change,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
            };

            self.send_header_to_other_replicas(header.frame_const().*);

            if (!self.start_view_change_from_all_replicas.isSet(self.replica)) {
                self.send_header_to_replica(self.replica, header.frame_const().*);
                defer self.flush_loopback_queue();
            }
        }

        fn send_do_view_change(self: *Self) void {
            assert(self.status == .view_change);
            assert(!self.solo());
            assert(self.view > self.log_view);
            assert(self.view >= self.view_durable());
            assert(self.log_view >= self.log_view_durable());
            assert(!self.do_view_change_quorum);
            // The DVC headers are already up to date, either via:
            // - transition_to_view_change_status(), or
            // - superblock's vsr_headers (after recovery).
            assert(self.view_headers.command == .do_view_change);
            assert(self.view_headers.array.get(0).op >= self.op);
            self.view_headers.verify();

            const BitSet = std.bit_set.IntegerBitSet(128);
            comptime assert(BitSet.MaskInt ==
                std.meta.fieldInfo(Header.DoViewChange, .present_bitset).type);
            comptime assert(BitSet.MaskInt ==
                std.meta.fieldInfo(Header.DoViewChange, .nack_bitset).type);

            // Collect nack and presence bits for the headers, so that the new primary can run CTRL
            // protocol to truncate uncommitted headers. When:
            // - a header has quorum of nacks -- the header is truncated
            // - a header isn't truncated and is present -- the header gets into the next view
            // - a header is neither truncated nor present -- the primary waits for more
            //   DVC messages to decide whether to keep or truncate the header.
            var nacks = BitSet.initEmpty();
            var present = BitSet.initEmpty();
            for (self.view_headers.array.const_slice(), 0..) |*header, i| {
                const slot = self.journal.slot_for_op(header.op);
                const journal_header = self.journal.header_for_op(header.op);
                const dirty = self.journal.dirty.bit(slot);
                const faulty = self.journal.faulty.bit(slot);

                // Case 1: We have this header in memory, but haven't persisted it to disk yet.
                if (journal_header != null and journal_header.?.checksum == header.checksum and
                    dirty and !faulty)
                {
                    nacks.set(i);
                }

                // Case 2: We have a _different_ prepare — safe to nack even if it is faulty.
                if (journal_header != null and journal_header.?.checksum != header.checksum) {
                    nacks.set(i);
                }

                // Case 3: We don't have a prepare at all, and that's not due to a fault.
                if (journal_header == null and !faulty) {
                    nacks.set(i);
                }

                // Presence bit: the prepare is on disk, is being written to disk, or is cached
                // in memory. These conditions mirror logic in `on_request_prepare` and imply
                // that we can help the new primary to repair this prepare.
                if ((self.journal.prepare_inhabited[slot.index] and
                    self.journal.prepare_checksums[slot.index] == header.checksum) or
                    self.journal.writing(header.op, header.checksum) or
                    self.pipeline_prepare_by_op_and_checksum(header.op, header.checksum) != null)
                {
                    if (journal_header != null) {
                        assert(journal_header.?.checksum == header.checksum);
                    }
                    maybe(nacks.isSet(i));
                    present.set(i);
                }
            }

            const message = self.message_bus.get_message(.do_view_change);
            defer self.message_bus.unref(message);

            message.header.* = .{
                .size = @sizeOf(Header) * (1 + self.view_headers.array.count_as(u32)),
                .command = .do_view_change,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
                // The latest normal view (as specified in the 2012 paper) is different to the view
                // number contained in the prepare headers we include in the body. The former shows
                // how recent a view change the replica participated in, which may be much higher.
                // We use the `request` field to send this in addition to the current view number:
                .log_view = self.log_view,
                .checkpoint_op = self.op_checkpoint(),
                // This is usually the head op, but it may be farther ahead if we are lagging behind
                // a checkpoint. (In which case the op is inherited from the SV).
                .op = self.view_headers.array.get(0).op,
                // For command=start_view, commit_min=commit_max.
                // For command=do_view_change, the new primary uses this op to trust extra headers
                // from non-canonical DVCs.
                .commit_min = self.commit_min,
                // Signal which headers correspond to definitely not-prepared messages.
                .nack_bitset = nacks.mask,
                // Signal which headers correspond to locally available prepares.
                .present_bitset = present.mask,
            };

            stdx.copy_disjoint(
                .exact,
                Header.Prepare,
                std.mem.bytesAsSlice(Header.Prepare, message.body()),
                self.view_headers.array.const_slice(),
            );
            message.header.set_checksum_body(message.body());
            message.header.set_checksum();

            assert(message.header.op >= self.op);
            // Each replica must advertise its own commit number, so that the new primary can know
            // which headers must be replaced in its log. Otherwise, a gap in the log may prevent
            // the new primary from repairing its log, resulting in the log being forked if the new
            // primary also discards uncommitted operations.
            // It is also safe not to use `commit_max` here because the new primary will assume that
            // operations after the highest `commit_min` may yet have been committed before the old
            // primary crashed. The new primary will use the NACK protocol to be sure of a discard.
            assert(message.header.commit_min == self.commit_min);
            DVCQuorum.verify_message(message);

            if (self.standby()) return;

            self.send_message_to_other_replicas(message);

            if (self.replica == self.primary_index(self.view) and
                self.do_view_change_from_all_replicas[self.replica] == null)
            {
                self.send_message_to_replica(self.replica, message);
                defer self.flush_loopback_queue();
            }
        }

        fn send_eviction_message_to_client(self: *Self, client: u128) void {
            assert(self.status == .normal);
            assert(self.primary());

            log.err("{}: too many sessions, sending eviction message to client={}", .{
                self.replica,
                client,
            });

            self.send_header_to_client(client, @bitCast(Header.Eviction{
                .command = .eviction,
                .cluster = self.cluster,
                .replica = self.replica,
                .view = self.view,
                .client = client,
            }));
        }

        fn send_reply_message_to_client(self: *Self, reply: *Message.Reply) void {
            assert(reply.header.command == .reply);
            assert(reply.header.view <= self.view);

            // If the request committed in a different view than the one it was originally prepared
            // in, we must inform the client about this newer view before we send it a reply.
            // Otherwise, the client might send a next request to the old primary, which would
            // observe a broken hash chain.
            //
            // To do this, we always set reply's view to the current one, and use the `context`
            // field for hash chaining.

            if (reply.header.view == self.view) {
                // Hot path: no need to clone the message if the view is the same.
                self.message_bus.send_message_to_client(reply.header.client, reply.base());
                return;
            }

            const reply_copy = self.message_bus.get_message(.reply);
            defer self.message_bus.unref(reply_copy);

            // Copy the message and update the view.
            // We could optimize this by using in-place modification if `reply.references == 1`.
            // We don't bother, as that complicates reasoning on the call-site, and this is
            // a cold path anyway.
            stdx.copy_disjoint(
                .inexact,
                u8,
                reply_copy.buffer,
                reply.buffer[0..reply.header.size],
            );
            reply_copy.header.view = self.view;
            reply_copy.header.set_checksum();

            self.message_bus.send_message_to_client(reply.header.client, reply_copy.base());
        }

        fn send_header_to_client(self: *Self, client: u128, header: Header) void {
            assert(header.cluster == self.cluster);
            assert(header.view == self.view);

            const message = self.create_message_from_header(header);
            defer self.message_bus.unref(message);

            self.message_bus.send_message_to_client(client, message);
        }

        fn send_header_to_other_replicas(self: *Self, header: Header) void {
            const message = self.create_message_from_header(header);
            defer self.message_bus.unref(message);

            var replica: u8 = 0;
            while (replica < self.replica_count) : (replica += 1) {
                if (replica != self.replica) {
                    self.send_message_to_replica_base(replica, message);
                }
            }
        }

        fn send_header_to_other_replicas_and_standbys(self: *Self, header: Header) void {
            const message = self.create_message_from_header(header);
            defer self.message_bus.unref(message);

            var replica: u8 = 0;
            while (replica < self.node_count) : (replica += 1) {
                if (replica != self.replica) {
                    self.send_message_to_replica_base(replica, message);
                }
            }
        }

        fn send_header_to_replica(self: *Self, replica: u8, header: Header) void {
            const message = self.create_message_from_header(header);
            defer self.message_bus.unref(message);

            self.send_message_to_replica_base(replica, message);
        }

        /// `message` is a `*MessageType(command)`.
        fn send_message_to_other_replicas(self: *Self, message: anytype) void {
            assert(@typeInfo(@TypeOf(message)) == .Pointer);
            assert(!@typeInfo(@TypeOf(message)).Pointer.is_const);

            self.send_message_to_other_replicas_base(message.base());
        }

        fn send_message_to_other_replicas_base(self: *Self, message: *Message) void {
            var replica: u8 = 0;
            while (replica < self.replica_count) : (replica += 1) {
                if (replica != self.replica) {
                    self.send_message_to_replica_base(replica, message);
                }
            }
        }

        /// `message` is a `*MessageType(command)`.
        fn send_message_to_replica(self: *Self, replica: u8, message: anytype) void {
            assert(@typeInfo(@TypeOf(message)) == .Pointer);
            assert(!@typeInfo(@TypeOf(message)).Pointer.is_const);

            self.send_message_to_replica_base(replica, message.base());
        }

        fn send_message_to_replica_base(self: *Self, replica: u8, message: *Message) void {
            // Switch on the header type so that we don't log opaque bytes for the per-command data.
            switch (message.header.into_any()) {
                inline else => |header| {
                    log.debug("{}: sending {s} to replica {}: {}", .{
                        self.replica,
                        @tagName(message.header.command),
                        replica,
                        header,
                    });
                },
            }

            if (message.header.invalid()) |reason| {
                log.err("{}: send_message_to_replica: invalid ({s})", .{ self.replica, reason });
                @panic("send_message_to_replica: invalid message");
            }

            assert(message.header.cluster == self.cluster);

            // TODO According to message.header.command, assert on the destination replica.
            switch (message.header.into_any()) {
                .reserved => unreachable,
                .request => {
                    assert(!self.standby());
                    // Do not assert message.header.replica because we forward .request messages.
                    assert(self.status == .normal);
                    assert(message.header.view <= self.view);
                },
                .prepare => |header| {
                    maybe(self.standby());
                    assert(self.replica != replica);
                    // Do not assert message.header.replica because we forward .prepare messages.
                    switch (self.status) {
                        .normal => assert(message.header.view <= self.view),
                        .view_change => assert(message.header.view < self.view),
                        // These are replies to a request_prepare:
                        else => assert(message.header.view <= self.view),
                    }
                    assert(header.operation != .reserved);
                },
                .prepare_ok => |header| {
                    assert(!self.standby());
                    assert(self.status == .normal);
                    assert(self.syncing == .idle);
                    assert(header.view == self.view);
                    assert(header.op <= self.op_prepare_max());
                    // We must only ever send a prepare_ok to the latest primary of the active view:
                    // We must never straddle views by sending to a primary in an older view.
                    // Otherwise, we would be enabling a partitioned primary to commit.
                    assert(replica == self.primary_index(self.view));
                    assert(header.replica == self.replica);
                },
                .reply => |header| {
                    assert(!self.standby());
                    assert(header.view <= self.view);
                    assert(header.op <= self.op_checkpoint_next_trigger());
                },
                .start_view_change => {
                    assert(!self.standby());
                    assert(self.status == .normal or self.status == .view_change);
                    assert(message.header.view == self.view);
                    assert(message.header.replica == self.replica);
                },
                .do_view_change => |header| {
                    assert(!self.standby());
                    assert(self.status == .view_change);
                    assert(self.view > self.log_view);
                    assert(!self.do_view_change_quorum);
                    assert(header.view == self.view);
                    assert(header.replica == self.replica);
                    maybe(header.op == self.op);
                    assert(header.op >= self.op);
                    assert(header.commit_min == self.commit_min);
                    assert(header.checkpoint_op == self.op_checkpoint());
                    assert(header.log_view == self.log_view);
                },
                .start_view => |header| {
                    assert(!self.standby());
                    assert(self.status == .normal);
                    assert(!self.do_view_change_quorum);
                    assert(self.syncing == .idle);
                    assert(header.view == self.view);
                    assert(header.replica == self.replica);
                    assert(header.replica != replica);
                    assert(header.commit == self.commit_min);
                    assert(header.commit == self.commit_max);
                    assert(header.checkpoint_op == self.op_checkpoint());
                },
                .headers => {
                    assert(!self.standby());
                    assert(message.header.view == self.view);
                    assert(message.header.replica == self.replica);
                    assert(message.header.replica != replica);
                },
                .ping => {
                    maybe(self.standby());
                    assert(message.header.replica == self.replica);
                    assert(message.header.replica != replica);
                },
                .pong => {
                    maybe(self.standby());
                    assert(self.status == .normal or self.status == .view_change);
                    assert(message.header.replica == self.replica);
                    assert(message.header.replica != replica);
                },
                .ping_client => unreachable,
                .pong_client => unreachable,
                .commit => {
                    assert(!self.standby());
                    assert(self.status == .normal);
                    assert(self.primary());
                    assert(self.syncing == .idle);
                    assert(message.header.view == self.view);
                    assert(message.header.replica == self.replica);
                    assert(message.header.replica != replica);
                },
                .request_start_view => {
                    maybe(self.standby());
                    assert(message.header.view >= self.view);
                    assert(message.header.replica == self.replica);
                    assert(message.header.replica != replica);
                    assert(self.primary_index(message.header.view) == replica);
                },
                .request_headers => {
                    maybe(self.standby());
                    assert(message.header.replica == self.replica);
                    assert(message.header.replica != replica);
                },
                .request_prepare => {
                    maybe(self.standby());
                    assert(message.header.replica == self.replica);
                    assert(message.header.replica != replica);
                },
                .request_reply => {
                    assert(message.header.replica == self.replica);
                    assert(message.header.replica != replica);
                },
                .eviction => {
                    assert(!self.standby());
                    assert(self.status == .normal);
                    assert(self.primary());
                    assert(message.header.view == self.view);
                    assert(message.header.replica == self.replica);
                },
                .request_blocks => {
                    maybe(self.standby());
                    assert(message.header.replica == self.replica);
                    assert(message.header.replica != replica);
                },
                .block => {
                    assert(!self.standby());
                },
                .request_sync_checkpoint => {
                    maybe(self.standby());
                    assert(self.syncing != .idle);
                    assert(message.header.replica == self.replica);
                    assert(message.header.replica != replica);
                },
                .sync_checkpoint => {
                    assert(!self.standby());
                    assert(self.syncing == .idle);
                    assert(message.header.replica == self.replica);
                    assert(message.header.replica != replica);
                },
            }

            if (replica != self.replica) {
                // Critical: Do not advertise a view/log_view before it is durable.
                // See view_durable()/log_view_durable().
                if (message.header.view > self.view_durable() and
                    message.header.command != .request_start_view)
                {
                    // Pings are used for syncing time, so they must not be
                    // blocked on persisting view.
                    assert(message.header.command != .ping);
                    assert(message.header.command != .pong);

                    log.debug("{}: send_message_to_replica: dropped {s} " ++
                        "(view_durable={} message.view={})", .{
                        self.replica,
                        @tagName(message.header.command),
                        self.view_durable(),
                        message.header.view,
                    });
                    return;
                }

                // For DVCs, SVCs, and prepare_oks we must wait for the log_view to be durable:
                // - A DVC includes the log_view.
                // - A SV or a prepare_ok imply the log_view.
                if (message.header.command == .do_view_change or
                    message.header.command == .start_view or
                    message.header.command == .prepare_ok)
                {
                    if (self.log_view_durable() < self.log_view) {
                        log.debug("{}: send_message_to_replica: dropped {s} " ++
                            "(log_view_durable={} log_view={})", .{
                            self.replica,
                            @tagName(message.header.command),
                            self.log_view_durable(),
                            self.log_view,
                        });
                        return;
                    }
                    assert(message.header.command != .do_view_change or std.mem.eql(
                        u8,
                        message.body(),
                        std.mem.sliceAsBytes(self.superblock.working.vsr_headers().slice),
                    ));
                }
            }

            if (replica == self.replica) {
                assert(self.loopback_queue == null);
                self.loopback_queue = message.ref();
            } else {
                if (self.event_callback) |hook| {
                    hook(self, .{ .message_sent = message });
                }
                self.message_bus.send_message_to_replica(replica, message);
            }
        }

        /// The highest durable view.
        /// A replica must not advertise a view higher than its durable view.
        ///
        /// The advertised `view` must never backtrack after a crash.
        /// This ensures the old primary is isolated — if a backup's view backtracks, it could
        /// ack a prepare to the old primary, forking the log. See VRR §8.2 for more detail.
        ///
        /// Equivalent to `superblock.working.vsr_state.view`.
        fn view_durable(self: *const Self) u32 {
            return self.superblock.working.vsr_state.view;
        }

        /// The highest durable log_view.
        /// A replica must not advertise a log_view (in a DVC) higher than its durable log_view.
        ///
        /// A replica's advertised `log_view` must never backtrack after a crash.
        /// (`log_view` is only advertised within DVC messages).
        ///
        /// To understand why, consider the following replica logs, where:
        ///
        ///   - numbers in replica rows denote the version of the op, and
        ///   - a<b<c denotes the view in which the op was prepared.
        ///
        /// Replica 0 prepares some ops, but they never arrive at replica 1/2:
        ///
        ///       view=a
        ///           op  │ 0  1  2
        ///    replica 0  │ 1a 2a 3a (log_view=a, leader)
        ///    replica 1  │ -  -  -  (log_view=a, follower — but never receives any prepares)
        ///   (replica 2) │ -  -  -  (log_view=_, partitioned)
        ///
        /// After a view change, replica 1 prepares some ops, but they never arrive at replica 0/2:
        ///
        ///       view=b
        ///           op  │ 0  1  2
        ///   (replica 0) │ 1a 2a 3a (log_view=a, partitioned)
        ///    replica 1  │ 4b 5b 6b (log_view=b, leader)
        ///    replica 2  │ -  -  -  (log_view=b, follower — but never receives any prepares)
        ///
        /// After another view change, replica 2 loads replica 1's ops:
        ///
        ///       view=c
        ///           op  │ 0  1  2
        ///    replica 0  │ 1a 2a 3a (log_view=c, follower)
        ///   (replica 1) │ 4b 5b 6b (log_view=b, partitioned)
        ///    replica 2  │ 1c 2c 3c (log_view=c, leader)
        ///
        /// Suppose replica 0 crashes and its log_view regresses to a.
        /// If replica 2 is partitioned, replicas 0 and 1 start view d with the DVCs:
        ///
        ///    replica 0  │ 1a 2a 3a (log_view=a, log_view backtracked!)
        ///    replica 1  │ 4b 5b 6b (log_view=b)
        ///
        /// Replica 1's higher log_view is canonical, so 4b/5b/6b replace 1a/2a/3a even though
        /// the latter may have been committed during view c. The log has forked.
        ///
        /// Therefore, a replica's log_view must never regress.
        ///
        /// Equivalent to `superblock.working.vsr_state.log_view`.
        fn log_view_durable(self: *const Self) u32 {
            return self.superblock.working.vsr_state.log_view;
        }

        fn view_durable_updating(self: *const Self) bool {
            return self.superblock.updating(.view_change);
        }

        /// Persist the current view and log_view to the superblock.
        /// `view_durable` and `log_view_durable` will update asynchronously, when their respective
        /// updates are durable.
        fn view_durable_update(self: *Self) void {
            assert(self.status == .normal or self.status == .view_change or
                (self.status == .recovering and self.solo()));
            assert(self.view >= self.log_view);
            assert(self.view >= self.view_durable());
            assert(self.log_view >= self.log_view_durable());
            assert(self.log_view > self.log_view_durable() or self.view > self.view_durable());
            // The primary must only persist the SV headers after repairs are done.
            // Otherwise headers could be nacked, truncated, then restored after a crash.
            assert(self.log_view < self.view or self.replica != self.primary_index(self.view) or
                self.status == .normal or self.status == .recovering);
            assert(self.view_headers.array.count() > 0);
            assert(self.view_headers.array.get(0).view <= self.log_view);
            assert(self.commit_max >= self.op -| constants.pipeline_prepare_queue_max);

            if (self.view_durable_updating()) return;

            log.debug("{}: view_durable_update: view_durable={}..{} log_view_durable={}..{}", .{
                self.replica,
                self.view_durable(),
                self.view,
                self.log_view_durable(),
                self.log_view,
            });

            self.superblock.view_change(
                view_durable_update_callback,
                &self.superblock_context_view_change,
                .{
                    .commit_max = self.commit_max,
                    .view = self.view,
                    .log_view = self.log_view,
                    .headers = &self.view_headers,
                },
            );
            assert(self.view_durable_updating());
        }

        fn view_durable_update_callback(context: *SuperBlock.Context) void {
            const self = @fieldParentPtr(Self, "superblock_context_view_change", context);
            assert(self.status == .normal or self.status == .view_change or
                (self.status == .recovering and self.solo()) or
                self.status == .recovering_head);
            assert(!self.view_durable_updating());
            assert(self.superblock.working.vsr_state.view <= self.view);
            assert(self.superblock.working.vsr_state.log_view <= self.log_view);
            assert(self.superblock.working.vsr_state.checkpoint.commit_min <= self.commit_min);
            assert(self.superblock.working.vsr_state.commit_max <= self.commit_max);

            log.debug("{}: view_durable_update_callback: " ++
                "(view_durable={} log_view_durable={})", .{
                self.replica,
                self.view_durable(),
                self.log_view_durable(),
            });

            assert(self.view_durable() <= self.view);
            assert(self.log_view_durable() <= self.view_durable());
            assert(self.log_view_durable() <= self.log_view);

            // recovering_head here is triggered by state sync.
            if (self.status == .recovering_head) return;

            // The view/log_view incremented while the previous view-change update was being saved.
            const update = self.log_view_durable() < self.log_view or
                self.view_durable() < self.view;
            const update_dvc = update and self.log_view < self.view;
            const update_sv = update and self.log_view == self.view and
                (self.replica != self.primary_index(self.view) or self.status == .normal);
            assert(!(update_dvc and update_sv));

            if (update_dvc or update_sv) self.view_durable_update();

            // Reset SVC timeout in case the view-durable update took a long time.
            if (self.view_change_status_timeout.ticking) self.view_change_status_timeout.reset();

            // Trigger work that was deferred until after the view-change update.
            if (self.status == .normal) {
                assert(self.log_view == self.view);

                if (self.primary_index(self.view) == self.replica) {
                    // Only replies to `request_start_view` need a nonce,
                    // to guarantee freshness of the message.
                    const nonce = 0;
                    const start_view = self.create_start_view_message(nonce);
                    defer self.message_bus.unref(start_view);

                    self.send_message_to_other_replicas(start_view);
                } else {
                    self.send_prepare_oks_after_view_change();
                }
            }

            if (self.status == .view_change and self.log_view < self.view) {
                if (!self.do_view_change_quorum) self.send_do_view_change();
            }
        }

        fn set_op_and_commit_max(self: *Self, op: u64, commit_max: u64, source: SourceLocation) void {
            assert(self.status == .view_change or self.status == .normal or
                self.status == .recovering_head);

            assert(op <= self.op_prepare_max());
            maybe(op >= self.commit_max);
            maybe(op >= commit_max);

            if (op < self.op) {
                // Uncommitted ops may not survive a view change, but never truncate committed ops.
                assert(op >= @max(commit_max, self.commit_max));
            }

            // We expect that our commit numbers may also be greater even than `commit_max` because
            // we may be the old primary joining towards the end of the view change and we may have
            // committed `op` already.
            // However, this is bounded by pipelining.
            // The intersection property only requires that all possibly committed operations must
            // survive into the new view so that they can then be committed by the new primary.
            // This guarantees that if the old primary possibly committed the operation, then the
            // new primary will also commit the operation.
            if (commit_max < self.commit_max and self.commit_min == self.commit_max) {
                log.debug("{}: {s}: k={} < commit_max={} and commit_min == commit_max", .{
                    self.replica,
                    source.fn_name,
                    commit_max,
                    self.commit_max,
                });
            }

            assert(self.commit_min <= self.commit_max);
            assert(self.op >= self.commit_max or self.op < self.commit_max);
            assert(self.op <= op + constants.pipeline_prepare_queue_max);

            const previous_op = self.op;
            const previous_commit_max = self.commit_max;

            self.op = op;
            self.journal.remove_entries_from(self.op + 1);

            // Crucially, we must never rewind `commit_max` (and then `commit_min`) because
            // `commit_min` represents what we have already applied to our state machine:
            self.commit_max = @max(self.commit_max, commit_max);
            assert(self.commit_max >= self.commit_min);
            assert(self.commit_max >= self.op -| constants.pipeline_prepare_queue_max);

            log.debug("{}: {s}: view={} op={}..{} commit={}..{}", .{
                self.replica,
                source.fn_name,
                self.view,
                previous_op,
                self.op,
                previous_commit_max,
                self.commit_max,
            });
        }

        /// Load the new view's headers from the DVC quorum.
        ///
        /// The iteration order of DVCs for repair does not impact the final result.
        /// In other words, you can't end up in a situation with a DVC quorum like:
        ///
        ///   replica     headers  commit_min
        ///         0   4 5 _ _ 8           4 (new primary; handling DVC quorum)
        ///         1   4 _ 6 _ 8           4
        ///         2   4 _ _ 7 8           4
        ///         3  (4 5 6 7 8)          8 (didn't participate in view change)
        ///         4  (4 5 6 7 8)          8 (didn't participate in view change)
        ///
        /// where the new primary's headers depends on which of replica 1 and 2's DVC is used
        /// for repair before the other (i.e. whether they repair op 6 or 7 first).
        ///
        /// For the above case to occur, replicas 0, 1, and 2 must all share the highest `log_view`.
        /// And since they share the latest `log_view`, ops 5,6,7 were just installed by
        /// `replace_header`, which is order-independent (it doesn't use the hash chain).
        ///
        /// (If replica 0's log_view was greater than 1/2's, then replica 0 must have all
        /// headers from previous views. Which means 6,7 are from the current view. But since
        /// replica 0 doesn't have 6/7, then replica 1/2 must share the latest log_view. ∎)
        fn primary_set_log_from_do_view_change_messages(self: *Self) void {
            assert(self.status == .view_change);
            assert(self.view > self.log_view);
            assert(self.primary_index(self.view) == self.replica);
            assert(!self.solo());
            assert(self.syncing == .idle);
            assert(self.commit_max <= self.op_prepare_max());
            assert(self.do_view_change_quorum);
            assert(self.do_view_change_from_all_replicas[self.replica] != null);
            DVCQuorum.verify(self.do_view_change_from_all_replicas);

            const dvcs_all = DVCQuorum.dvcs_all(self.do_view_change_from_all_replicas);
            assert(dvcs_all.count() >= self.quorum_view_change);

            for (dvcs_all.const_slice()) |message| {
                assert(message.header.op <=
                    self.op_prepare_max() + constants.pipeline_prepare_queue_max);
            }

            // The `prepare_timestamp` prevents a primary's own clock from running backwards.
            // Therefore, `prepare_timestamp`:
            // 1. is advanced if behind the cluster, but never reset if ahead of the cluster, i.e.
            // 2. may not always reflect the timestamp of the latest prepared op, and
            // 3. should be advanced before discarding the timestamps of any uncommitted headers.
            const timestamp_max = DVCQuorum.timestamp_max(self.do_view_change_from_all_replicas);
            if (self.state_machine.prepare_timestamp < timestamp_max) {
                self.state_machine.prepare_timestamp = timestamp_max;
            }

            var quorum_headers = DVCQuorum.quorum_headers(
                self.do_view_change_from_all_replicas,
                .{
                    .quorum_nack_prepare = self.quorum_nack_prepare,
                    .quorum_view_change = self.quorum_view_change,
                    .replica_count = self.replica_count,
                },
            ).complete_valid;

            const header_head = quorum_headers.next().?;
            assert(header_head.op >= self.op_checkpoint());
            assert(header_head.op >= self.commit_min);
            assert(header_head.op >= self.commit_max);
            assert(header_head.op <= self.op_prepare_max());
            for (dvcs_all.const_slice()) |dvc| assert(header_head.op >= dvc.header.commit_min);

            // When computing the new commit_max, we cannot simply rely on the fact that our own
            // commit_min is attached to our own DVC in the quorum.
            // Consider the case:
            // 1. Start committing op=N…M.
            // 2. Send `do_view_change` to self.
            // 3. Finish committing op=N…M.
            // 4. Remaining `do_view_change` messages arrive, completing the quorum.
            // In this scenario, our own DVC's commit is `N-1`, but `commit_min=M`.
            // Don't let the commit backtrack.
            const commit_max = @max(
                self.commit_min,
                DVCQuorum.commit_max(self.do_view_change_from_all_replicas),
            );
            assert(self.commit_min >=
                self.do_view_change_from_all_replicas[self.replica].?.header.commit_min);

            {
                // "`replica.op` exists" invariant may be broken briefly between
                // set_op_and_commit_max() and replace_header().

                self.set_op_and_commit_max(header_head.op, commit_max, @src());
                assert(self.commit_max <= self.op_prepare_max());
                assert(self.commit_max <= self.op);
                maybe(self.journal.header_with_op(self.op) == null);

                self.replace_header(header_head);
                assert(self.journal.header_with_op(self.op) != null);
            }

            while (quorum_headers.next()) |header| {
                assert(header.op < header_head.op);
                self.replace_header(header);
            }
            assert(self.journal.header_with_op(self.commit_max) != null);

            const dvcs_uncanonical =
                DVCQuorum.dvcs_uncanonical(self.do_view_change_from_all_replicas);
            for (dvcs_uncanonical.const_slice()) |message| {
                const message_headers = message_body_as_view_headers(message.base_const());
                for (message_headers.slice) |*header| {
                    if (vsr.Headers.dvc_header_type(header) != .valid) continue;

                    // We must trust headers that other replicas have committed, because
                    // repair_header() will not repair a header if the hash chain has a gap.
                    if (header.op <= message.header.commit_min) {
                        log.debug(
                            "{}: on_do_view_change: committed: replica={} op={} checksum={}",
                            .{
                                self.replica,
                                message.header.replica,
                                header.op,
                                header.checksum,
                            },
                        );
                        self.replace_header(header);
                    } else {
                        _ = self.repair_header(header);
                    }
                }
            }
        }

        fn primary_log_do_view_change_quorum(
            self: *const Self,
            comptime context: []const u8,
        ) void {
            assert(self.status == .view_change);
            assert(self.primary_index(self.view) == self.replica);
            assert(self.view > self.log_view);

            const dvcs_all = DVCQuorum.dvcs_all(self.do_view_change_from_all_replicas);
            for (dvcs_all.const_slice()) |dvc| {
                log.debug(
                    "{}: {s}: dvc: replica={} log_view={} op={} commit_min={} checkpoint={}",
                    .{
                        self.replica,
                        context,
                        dvc.header.replica,
                        dvc.header.log_view,
                        dvc.header.op,
                        dvc.header.commit_min,
                        dvc.header.checkpoint_op,
                    },
                );

                const BitSet = std.bit_set.IntegerBitSet(128);
                const dvc_headers = message_body_as_view_headers(dvc.base_const());
                const dvc_nacks = BitSet{ .mask = dvc.header.nack_bitset };
                const dvc_present = BitSet{ .mask = dvc.header.present_bitset };
                for (dvc_headers.slice, 0..) |*header, i| {
                    log.debug("{}: {s}: dvc: header: " ++
                        "replica={} op={} checksum={} nack={} present={} type={s}", .{
                        self.replica,
                        context,
                        dvc.header.replica,
                        header.op,
                        header.checksum,
                        dvc_nacks.isSet(i),
                        dvc_present.isSet(i),
                        @tagName(vsr.Headers.dvc_header_type(header)),
                    });
                }
            }
        }

        fn primary_start_view_as_the_new_primary(self: *Self) void {
            assert(self.status == .view_change);
            assert(self.primary_index(self.view) == self.replica);
            assert(self.syncing == .idle);
            assert(self.view == self.log_view);
            assert(self.do_view_change_quorum);
            assert(!self.pipeline_repairing);
            assert(self.primary_repair_pipeline() == .done);

            assert(self.commit_min == self.commit_max);
            assert(self.commit_max <= self.op);
            assert(self.journal.dirty.count == 0);
            assert(self.journal.faulty.count == 0);
            assert(self.valid_hash_chain_between(self.op_repair_min(), self.op));

            {
                const pipeline_queue = self.primary_repair_pipeline_done();
                assert(pipeline_queue.request_queue.empty());
                assert(pipeline_queue.prepare_queue.count + self.commit_max == self.op);
                if (!pipeline_queue.prepare_queue.empty()) {
                    const prepares = &pipeline_queue.prepare_queue;
                    assert(prepares.head_ptr_const().?.message.header.op == self.commit_max + 1);
                    assert(prepares.tail_ptr_const().?.message.header.op == self.op);
                }

                var pipeline_prepares = pipeline_queue.prepare_queue.iterator();
                while (pipeline_prepares.next()) |prepare| {
                    assert(self.journal.has(prepare.message.header));
                    assert(!prepare.ok_quorum_received);
                    assert(prepare.ok_from_all_replicas.count() == 0);

                    log.debug("{}: start_view_as_the_new_primary: pipeline " ++
                        "(op={} checksum={x} parent={x})", .{
                        self.replica,
                        prepare.message.header.op,
                        prepare.message.header.checksum,
                        prepare.message.header.parent,
                    });
                }

                self.pipeline.cache.deinit(self.message_bus.pool);
                self.pipeline = .{ .queue = pipeline_queue };
                self.pipeline.queue.verify();
            }

            self.view_headers.command = .start_view;
            self.primary_update_view_headers();
            self.view_headers.verify();

            self.transition_to_normal_from_view_change_status(self.view);

            assert(self.status == .normal);
            assert(self.primary());

            // SVs will be sent out after the view_durable update completes.
            assert(self.view_durable_updating());
            assert(self.log_view > self.log_view_durable());

            // Send prepare_ok messages to ourself to contribute to the pipeline.
            self.send_prepare_oks_after_view_change();
        }

        fn transition_to_recovering_head(self: *Self) void {
            assert(!self.solo());
            assert(self.commit_stage == .idle);
            assert(self.journal.header_with_op(self.op) != null);

            if (self.status == .recovering) {
                assert(self.pipeline == .cache);
                if (self.log_view < self.view) {
                    assert(self.op < self.commit_min);
                }
            } else {
                // During state sync, we may use recovering_head to avoid violating invariants when
                // the checkpoint jumps ahead.
                assert(self.syncing != .idle);
                assert(self.op < self.commit_min);
            }

            self.status = .recovering_head;

            self.ping_timeout.start();
            self.prepare_timeout.stop();
            self.primary_abdicate_timeout.stop();
            self.commit_message_timeout.stop();
            self.normal_heartbeat_timeout.stop();
            self.start_view_change_window_timeout.stop();
            self.start_view_change_message_timeout.stop();
            self.view_change_status_timeout.stop();
            self.do_view_change_message_timeout.stop();
            self.request_start_view_message_timeout.stop();
            self.repair_timeout.stop();
            self.repair_sync_timeout.stop();
            self.grid_repair_message_timeout.start();

            if (self.pipeline == .queue) {
                // Convert the pipeline queue into a cache.
                var queue: PipelineQueue = self.pipeline.queue;
                self.pipeline = .{ .cache = PipelineCache.init_from_queue(&queue) };
                queue.deinit(self.message_bus.pool);
            }

            log.warn("{}: transition_to_recovering_head: " ++
                "op_checkpoint={} commit_min={} op_head={} log_view={} view={}", .{
                self.replica,
                self.op_checkpoint(),
                self.commit_min,
                self.op,
                self.log_view,
                self.view,
            });
        }

        fn transition_to_normal_from_recovering_status(self: *Self) void {
            assert(self.status == .recovering);
            assert(self.view == self.log_view);
            assert(self.commit_max >= self.op -| constants.pipeline_prepare_queue_max);
            assert(self.commit_stage == .idle);
            assert(self.journal.header_with_op(self.op) != null);
            assert(self.pipeline == .cache);
            assert(self.view_headers.command == .start_view);

            self.status = .normal;

            if (self.primary()) {
                log.debug(
                    "{}: transition_to_normal_from_recovering_status: view={} primary",
                    .{
                        self.replica,
                        self.view,
                    },
                );

                assert(self.solo());
                assert(!self.prepare_timeout.ticking);
                assert(!self.primary_abdicate_timeout.ticking);
                assert(!self.normal_heartbeat_timeout.ticking);
                assert(!self.start_view_change_window_timeout.ticking);
                assert(!self.view_change_status_timeout.ticking);
                assert(!self.do_view_change_message_timeout.ticking);
                assert(!self.request_start_view_message_timeout.ticking);
                assert(!self.repair_sync_timeout.ticking);

                self.ping_timeout.start();
                self.start_view_change_message_timeout.start();
                self.commit_message_timeout.start();
                self.repair_timeout.start();
                self.grid_repair_message_timeout.start();

                self.pipeline.cache.deinit(self.message_bus.pool);
                self.pipeline = .{ .queue = .{} };
            } else {
                log.debug(
                    "{}: transition_to_normal_from_recovering_status: view={} backup",
                    .{
                        self.replica,
                        self.view,
                    },
                );

                assert(!self.prepare_timeout.ticking);
                assert(!self.primary_abdicate_timeout.ticking);
                assert(!self.normal_heartbeat_timeout.ticking);
                assert(!self.start_view_change_window_timeout.ticking);
                assert(!self.commit_message_timeout.ticking);
                assert(!self.view_change_status_timeout.ticking);
                assert(!self.do_view_change_message_timeout.ticking);
                assert(!self.request_start_view_message_timeout.ticking);
                assert(!self.repair_sync_timeout.ticking);

                self.ping_timeout.start();
                self.normal_heartbeat_timeout.start();
                self.start_view_change_message_timeout.start();
                self.repair_timeout.start();
                self.repair_sync_timeout.start();
                self.grid_repair_message_timeout.start();
            }
        }

        fn transition_to_normal_from_recovering_head_status(self: *Self, view_new: u32) void {
            assert(!self.solo());
            assert(self.status == .recovering_head);
            assert(self.view >= self.log_view);
            assert(self.view <= view_new);
            assert(self.replica != self.primary_index(view_new));
            assert(self.commit_max >= self.op -| constants.pipeline_prepare_queue_max);
            assert(self.commit_stage == .idle);
            assert(self.journal.header_with_op(self.op) != null);
            assert(self.pipeline == .cache);
            assert(self.view_headers.command == .start_view);

            log.debug(
                "{}: transition_to_normal_from_recovering_head_status: view={}..{} backup",
                .{
                    self.replica,
                    self.view,
                    view_new,
                },
            );

            self.status = .normal;
            if (self.log_view == view_new) {
                // Recovering to the same view we lost the head in.
                assert(self.view == view_new);
            } else {
                self.view = view_new;
                self.log_view = view_new;
                self.view_durable_update();
            }

            assert(self.backup());
            assert(!self.prepare_timeout.ticking);
            assert(!self.primary_abdicate_timeout.ticking);
            assert(!self.normal_heartbeat_timeout.ticking);
            assert(!self.start_view_change_window_timeout.ticking);
            assert(!self.commit_message_timeout.ticking);
            assert(!self.view_change_status_timeout.ticking);
            assert(!self.do_view_change_message_timeout.ticking);
            assert(!self.request_start_view_message_timeout.ticking);
            assert(!self.repair_sync_timeout.ticking);

            self.ping_timeout.start();
            self.normal_heartbeat_timeout.start();
            self.start_view_change_message_timeout.start();
            self.repair_timeout.start();
            self.repair_sync_timeout.start();
            self.grid_repair_message_timeout.start();
        }

        fn transition_to_normal_from_view_change_status(self: *Self, view_new: u32) void {
            // In the VRR paper it's possible to transition from normal to normal for the same view.
            // For example, this could happen after a state sync triggered by an op jump.
            assert(self.status == .view_change);
            assert(self.commit_max >= self.op -| constants.pipeline_prepare_queue_max);
            assert(view_new >= self.view);
            assert(self.journal.header_with_op(self.op) != null);
            assert(!self.primary_abdicating);
            assert(self.view_headers.command == .start_view);

            self.status = .normal;

            if (self.primary()) {
                log.debug(
                    "{}: transition_to_normal_from_view_change_status: view={}..{} primary",
                    .{ self.replica, self.view, view_new },
                );

                assert(!self.prepare_timeout.ticking);
                assert(!self.normal_heartbeat_timeout.ticking);
                assert(!self.primary_abdicate_timeout.ticking);
                assert(!self.repair_sync_timeout.ticking);
                assert(!self.pipeline_repairing);
                assert(self.pipeline == .queue);
                assert(self.view == view_new);
                assert(self.log_view == view_new);
                assert(self.commit_min == self.commit_max);
                assert(self.journal.dirty.count == 0);
                assert(self.journal.faulty.count == 0);

                // Now that the primary is repaired and in status=normal, it can update its
                // view-change headers.
                self.view_durable_update();

                self.ping_timeout.start();
                self.commit_message_timeout.start();
                self.start_view_change_window_timeout.stop();
                self.start_view_change_message_timeout.start();
                self.view_change_status_timeout.stop();
                self.do_view_change_message_timeout.stop();
                self.request_start_view_message_timeout.stop();
                self.repair_timeout.start();
                self.grid_repair_message_timeout.start();

                // Do not reset the pipeline as there may be uncommitted ops to drive to completion.
                if (self.pipeline.queue.prepare_queue.count > 0) {
                    self.prepare_timeout.start();
                    self.primary_abdicate_timeout.start();
                }
            } else {
                log.debug("{}: transition_to_normal_from_view_change_status: view={}..{} backup", .{
                    self.replica,
                    self.view,
                    view_new,
                });

                assert(!self.prepare_timeout.ticking);
                assert(!self.normal_heartbeat_timeout.ticking);
                assert(!self.primary_abdicate_timeout.ticking);
                assert(!self.repair_sync_timeout.ticking);
                assert(self.request_start_view_message_timeout.ticking);
                assert(self.pipeline == .cache);

                if (self.log_view == view_new and self.view == view_new) {
                    // We recovered into the same view we crashed in, with a detour through
                    // status=recovering_head.
                } else {
                    self.view = view_new;
                    self.log_view = view_new;
                    self.view_durable_update();
                }

                self.ping_timeout.start();
                self.commit_message_timeout.stop();
                self.normal_heartbeat_timeout.start();
                self.start_view_change_window_timeout.stop();
                self.start_view_change_message_timeout.start();
                self.view_change_status_timeout.stop();
                self.do_view_change_message_timeout.stop();
                self.request_start_view_message_timeout.stop();
                self.repair_timeout.start();
                self.repair_sync_timeout.start();
                self.grid_repair_message_timeout.start();
            }

            self.heartbeat_timestamp = 0;
            self.reset_quorum_start_view_change();
            self.reset_quorum_do_view_change();

            assert(self.do_view_change_quorum == false);
        }

        /// A replica i that notices the need for a view change advances its view, sets its status
        /// to view_change, and sends a ⟨do_view_change v, i⟩ message to all the other replicas,
        /// where v identifies the new view. A replica notices the need for a view change either
        /// based on its own timer, or because it receives a start_view_change or do_view_change
        /// message for a view with a larger number than its own view.
        fn transition_to_view_change_status(self: *Self, view_new_min: u32) void {
            assert(self.status == .normal or
                self.status == .view_change or
                self.status == .recovering);
            assert(view_new_min >= self.log_view);
            assert(view_new_min >= self.view);
            assert(view_new_min > self.view or self.status == .recovering);
            assert(view_new_min > self.log_view);
            assert(self.commit_max >= self.op -| constants.pipeline_prepare_queue_max);
            defer assert(self.view_headers.command == .do_view_change);

            const view_new = view: {
                if (self.syncing == .idle or
                    self.primary_index(view_new_min) != self.replica)
                {
                    break :view view_new_min;
                } else {
                    // A syncing replica is not eligible to be primary.
                    break :view view_new_min + 1;
                }
            };

            log.debug("{}: transition_to_view_change_status: view={}..{} status={}..{}", .{
                self.replica,
                self.view,
                view_new,
                self.status,
                Status.view_change,
            });

            if (self.status == .normal or
                (self.status == .recovering and self.log_view == self.view) or
                (self.status == .view_change and self.log_view == self.view))
            {
                self.view_change_update_view_headers();
            }

            self.view_headers.verify();
            assert(self.view_headers.command == .do_view_change);
            assert(self.view_headers.array.get(self.view_headers.array.count() - 1).op <=
                self.commit_max);

            const status_before = self.status;
            self.status = .view_change;
            if (self.view == view_new) {
                assert(status_before == .recovering);
            } else {
                self.view = view_new;
                self.view_durable_update();
            }

            if (self.pipeline == .queue) {
                var queue: PipelineQueue = self.pipeline.queue;
                self.pipeline = .{ .cache = PipelineCache.init_from_queue(&queue) };
                queue.deinit(self.message_bus.pool);
            }

            self.ping_timeout.start();
            self.commit_message_timeout.stop();
            self.normal_heartbeat_timeout.stop();
            self.start_view_change_window_timeout.stop();
            self.start_view_change_message_timeout.start();
            self.view_change_status_timeout.start();
            self.do_view_change_message_timeout.start();
            self.repair_timeout.stop();
            self.repair_sync_timeout.stop();
            self.prepare_timeout.stop();
            self.primary_abdicate_timeout.stop();
            self.grid_repair_message_timeout.start();

            if (self.primary_index(self.view) == self.replica) {
                self.request_start_view_message_timeout.stop();
            } else {
                self.request_start_view_message_timeout.start();
            }

            // Do not reset quorum counters only on entering a view, assuming that the view will be
            // followed only by a single subsequent view change to the next view, because multiple
            // successive view changes can fail, e.g. after a view change timeout.
            // We must therefore reset our counters here to avoid counting messages from an older
            // view, which would violate the quorum intersection property essential for correctness.
            self.heartbeat_timestamp = 0;
            self.primary_abdicating = false;
            self.reset_quorum_start_view_change();
            self.reset_quorum_do_view_change();

            assert(self.do_view_change_quorum == false);

            self.send_do_view_change();
        }

        fn view_change_update_view_headers(self: *Self) void {
            // Either:
            // - Transition from normal status.
            // - Recovering from normal status.
            // - Retired primary that didn't finish repair.
            assert(self.status == .normal or
                (self.status == .recovering and self.log_view == self.view) or
                (self.status == .view_change and self.log_view == self.view));

            const primary_repairing =
                self.status == .view_change and self.log_view == self.view;
            if (primary_repairing) {
                assert(self.primary_index(self.view) == self.replica);
                assert(self.do_view_change_quorum);
            }

            assert(self.view == self.log_view);

            // The DVC headers include:
            // - all available cluster-uncommitted ops, and
            // - the highest cluster-committed op (if available).
            // We cannot safely go beyond that in all cases for fear of concealing a break:
            // - During a prior view-change we might have only accepted a single header from the
            //   DVC: "header.op = op_prepare_max", and then not completed any
            //   repair.
            // - Similarly, we might have receive a catch-up SV message and only installed a
            //   single (checkpoint trigger) hook header.
            //
            // DVC headers are stitched together from the journal and existing view headers (they
            // might belong to the next log wrap), to guarantee that a DVC with log_view=v includes
            // all uncommitted ops with views <v. Special case: a primary that has collected a DVC
            // quorum and installed surviving headers into the journal ignores view headers (they
            // might have some truncated ops).
            var view_headers_updated = vsr.Headers.ViewChangeArray{
                .command = .do_view_change,
                .array = .{},
            };

            const view_headers_op_max = self.view_headers.array.get(0).op;
            var op = if (primary_repairing) self.op else @max(self.op, view_headers_op_max);
            for (0..constants.pipeline_prepare_queue_max + 1) |_| {
                const header_journal: ?*const vsr.Header.Prepare =
                    self.journal.header_with_op(op);

                const header_view: ?*const vsr.Header.Prepare = header: {
                    if (primary_repairing or op > view_headers_op_max) break :header null;

                    const header = &self.view_headers.array.const_slice()[view_headers_op_max - op];
                    break :header switch (vsr.Headers.dvc_header_type(header)) {
                        .valid => header,
                        .blank => null,
                    };
                };

                if (header_journal != null and header_view != null) {
                    assert(header_journal.?.op == header_view.?.op);
                    assert(header_journal.?.view == header_view.?.view);
                    assert(header_journal.?.checksum == header_view.?.checksum);
                }

                if (header_journal == null and header_view == null) {
                    assert(view_headers_updated.array.count() > 0);
                    assert(op != self.op);
                    view_headers_updated.append_blank(op);
                } else {
                    if (header_journal) |h| {
                        view_headers_updated.append(h);
                    } else {
                        // Transition from normal status, but the SV headers were part of the next
                        // wrap, so we didn't install them to our journal, and we didn't catch up.
                        // We will reuse the SV headers as our DVC headers to ensure that
                        // participating in another view-change won't allow the op to backtrack.
                        assert(self.log_view == self.view);
                        view_headers_updated.append(header_view.?);
                    }
                }

                if (op <= self.commit_max) break;
                op -= 1;
            } else unreachable;

            assert(op <= self.commit_max);
            assert(op == self.commit_max or self.commit_max > self.op);

            self.view_headers = view_headers_updated;
            self.view_headers.verify();
        }

        /// Transition from "not syncing" to "syncing".
        fn sync_start_from_committing(self: *Self) void {
            assert(!self.solo());
            assert(self.status != .recovering);
            assert(self.syncing == .idle);

            log.debug("{}: sync_start_from_committing " ++
                "(commit_stage={s} checkpoint_op={} checkpoint_id={x:0>32})", .{
                self.replica,
                @tagName(self.commit_stage),
                self.op_checkpoint(),
                self.superblock.staging.checkpoint_id(),
            });

            self.sync_tables = null;

            // We learned that we are lagging behind the cluster, so we know we shouldn't actually
            // be the primary. But we can't join a new view until the view's headers are within our
            // `op_prepare_max`. We need to bump our `op_checkpoint` first – but that is
            // async. So even if we are arriving at `sync_start_from_committing` via
            // `on_start_view`, `on_start_view` leaves us as the "primary" for now.
            if (self.status == .normal and self.primary()) {
                self.transition_to_view_change_status(self.view + 1);
            }

            // Abort grid operations.
            // Wait for non-grid operations to finish.
            switch (self.commit_stage) {
                // The transition which follows these stages is synchronous:
                .next,
                .next_pipeline,
                .cleanup,
                => unreachable,

                // Uninterruptible states:
                .next_journal,
                .setup_client_replies,
                .checkpoint_data,
                .checkpoint_superblock,
                => self.sync_dispatch(.canceling_commit),

                .idle, // (StateMachine.open() may be running.)
                .prefetch_state_machine,
                .compact_state_machine,
                => self.sync_dispatch(.canceling_grid),
            }
        }

        /// Transition from "syncing" to "syncing" — a new target was discovered.
        fn sync_start_from_sync(self: *Self) void {
            assert(!self.solo());
            assert(self.status != .recovering);
            assert(self.syncing != .idle);
            assert(self.sync_tables == null);
            assert(self.grid_repair_tables.executing() == 0);

            log.debug("{}: sync_start_from_sync " ++
                "(checkpoint_op={} checkpoint_id={x:0>32})", .{
                self.replica,
                self.op_checkpoint(),
                self.superblock.staging.checkpoint_id(),
            });

            switch (self.syncing) {
                .idle => unreachable,

                // Uninterruptible states:
                .canceling_commit, // No sync target selected yet.
                .canceling_grid, // No sync target selected yet.
                .updating_superblock, // Hopefully won't need to resync.
                => return,

                .requesting_target => {
                    assert(self.commit_stage == .idle);
                    assert(self.sync_target_max != null);
                    self.sync_dispatch(.requesting_target);
                },

                // We had a sync target already, but we discovered a newer one.
                // Re-sync to the new target.
                .requesting_checkpoint => |*stage| {
                    assert(self.commit_stage == .idle);
                    assert(self.sync_target_max.?.checkpoint_op >= stage.target.checkpoint_op);
                    if (self.sync_target_max.?.checkpoint_op == stage.target.checkpoint_op) return;

                    self.sync_dispatch(.{ .requesting_checkpoint = .{
                        .target = self.sync_target_max.?,
                    } });
                },
            }
        }

        /// sync_dispatch() is called between every sync-state transition.
        fn sync_dispatch(self: *Self, state_new: SyncStage) void {
            assert(!self.solo());
            assert(SyncStage.valid_transition(self.syncing, state_new));
            if (self.op < self.commit_min) assert(self.status == .recovering_head);

            const state_old = self.syncing;
            self.syncing = state_new;

            // We were already syncing, but discovered a newer target:
            if (self.syncing.target()) |target| {
                assert(self.commit_stage == .idle);

                if (target.checkpoint_op < self.sync_target_max.?.checkpoint_op) {
                    log.debug("{}: sync_dispatch: change target op={}..{} id={x:0>32}..{x:0>32}", .{
                        self.replica,
                        target.checkpoint_op,
                        self.sync_target_max.?.checkpoint_op,
                        target.checkpoint_id,
                        self.sync_target_max.?.checkpoint_id,
                    });
                    self.syncing =
                        .{ .requesting_checkpoint = .{ .target = self.sync_target_max.? } };
                }
            }

            // We have been waiting for a target:
            if (self.syncing == .requesting_target) {
                assert(self.commit_stage == .idle);

                if (self.sync_target_max) |target| {
                    if (target.checkpoint_op > self.op_checkpoint() or
                        (target.checkpoint_op == self.op_checkpoint() and
                        target.checkpoint_id != self.superblock.working.checkpoint_id()))
                    {
                        self.syncing = .{ .requesting_checkpoint = .{ .target = target } };
                    }
                }
            }

            if (self.syncing.target()) |target| {
                log.debug("{}: sync_dispatch: {s}..{s}: " ++
                    "(target_op={} target_id={x:0>32})", .{
                    self.replica,
                    @tagName(state_old),
                    @tagName(self.syncing),
                    target.checkpoint_op,
                    target.checkpoint_id,
                });
            } else {
                log.debug("{}: sync_dispatch: {s}..{s}", .{
                    self.replica,
                    @tagName(state_old),
                    @tagName(self.syncing),
                });
            }

            if (self.event_callback) |hook| hook(self, .sync_stage_changed);

            switch (self.syncing) {
                .idle => {},
                .canceling_commit => {}, // Waiting for an uninterruptible commit step.
                .canceling_grid => {
                    self.grid.cancel(sync_cancel_grid_callback);
                    self.sync_reclaim_tables();

                    assert(self.grid_repair_tables.executing() == 0);
                    assert(self.grid.read_global_queue.empty());
                },
                .requesting_target => {}, // Waiting for a usable sync target.
                .requesting_checkpoint => self.sync_message_timeout.start(),
                .updating_superblock => self.sync_superblock_update(),
            }
        }

        fn sync_cancel_commit_callback(self: *Self) void {
            assert(!self.solo());
            assert(self.syncing == .canceling_commit);

            switch (self.commit_stage) {
                .idle,
                .next,
                .next_pipeline,
                .cleanup,
                .prefetch_state_machine,
                .compact_state_machine,
                => unreachable,

                .next_journal,
                .setup_client_replies,
                .checkpoint_data,
                .checkpoint_superblock,
                => {},
            }

            // Even though the commit flow has been stopped, there may still be pending IO
            // between beats.
            self.sync_dispatch(.canceling_grid);
        }

        fn sync_cancel_grid_callback(grid: *Grid) void {
            const self = @fieldParentPtr(Self, "grid", grid);
            assert(self.syncing == .canceling_grid);
            assert(self.sync_tables == null);
            assert(self.grid_repair_tables.executing() == 0);
            assert(self.grid.blocks_missing.faulty_blocks.count() == 0);
            assert(self.grid.read_queue.empty());
            assert(self.grid.read_global_queue.empty());
            assert(self.grid.write_queue.empty());
            assert(self.grid.read_iops.executing() == 0);
            assert(self.grid.write_iops.executing() == 0);

            if (self.commit_stage == .idle) {
                assert(self.commit_prepare == null);
            } else {
                if (self.commit_prepare) |prepare| self.message_bus.unref(prepare);
                self.commit_prepare = null;
                self.commit_stage = .idle;
            }

            var grid_reads = self.grid_reads.iterate();
            while (grid_reads.next()) |grid_read| {
                assert(grid_read.message.base().references == 1);

                self.message_bus.unref(grid_read.message);
                self.grid_reads.release(grid_read);
            }

            var grid_repair_writes = self.grid_repair_writes.iterate();
            while (grid_repair_writes.next()) |write| self.grid_repair_writes.release(write);

            self.sync_dispatch(.requesting_target);
        }

        fn sync_requesting_checkpoint_callback(
            self: *Self,
            checkpoint_state: *const vsr.CheckpointState,
        ) void {
            assert(!self.solo());
            assert(self.syncing == .requesting_checkpoint);
            assert(!self.superblock.updating(.checkpoint));
            assert(self.commit_stage == .idle);
            assert(self.commit_prepare == null);
            assert(self.sync_tables == null);
            assert(self.grid.read_global_queue.empty());
            assert(self.grid.write_queue.empty());
            assert(self.grid_repair_tables.executing() == 0);
            assert(self.grid_repair_writes.executing() == 0);
            assert(self.grid.blocks_missing.faulty_blocks.count() == 0);

            if (self.status == .normal) assert(!self.primary());

            const stage: *const SyncStage.RequestingCheckpoint =
                &self.syncing.requesting_checkpoint;
            assert(stage.target.checkpoint_id == vsr.checksum(std.mem.asBytes(checkpoint_state)));

            log.debug("{[replica]}: sync_requesting_checkpoint_callback: " ++
                "checkpoint_op={[checkpoint_op]} checkpoint_id={[checkpoint_id]x:0>32} " ++
                "manifest_oldest_checksum={[manifest_oldest_checksum]} " ++
                "manifest_oldest_address={[manifest_oldest_address]} " ++
                "manifest_newest_checksum={[manifest_newest_checksum]} " ++
                "manifest_newest_address={[manifest_newest_address]} ", .{
                .replica = self.replica,
                .checkpoint_op = stage.target.checkpoint_op,
                .checkpoint_id = stage.target.checkpoint_id,
                .manifest_oldest_checksum = checkpoint_state.manifest_oldest_checksum,
                .manifest_oldest_address = checkpoint_state.manifest_oldest_address,
                .manifest_newest_checksum = checkpoint_state.manifest_newest_checksum,
                .manifest_newest_address = checkpoint_state.manifest_newest_address,
            });

            // Faulty bits will be set in sync_content().
            while (self.client_replies.faulty.findFirstSet()) |slot| {
                self.client_replies.faulty.unset(slot);
            }

            self.sync_dispatch(.{ .updating_superblock = .{
                .target = stage.target,
                .checkpoint_state = checkpoint_state.*,
            } });
        }

        fn sync_superblock_update(self: *Self) void {
            assert(!self.solo());
            assert(self.syncing == .updating_superblock);
            assert(self.sync_message_timeout.ticking);
            assert(self.sync_tables == null);
            assert(self.grid.read_global_queue.empty());
            assert(self.grid.write_queue.empty());
            assert(self.grid_repair_tables.executing() == 0);
            assert(self.grid_repair_writes.executing() == 0);
            assert(self.grid.blocks_missing.faulty_blocks.count() == 0);
            maybe(self.state_machine_opened);

            const stage: *const SyncStage.UpdatingSuperBlock = &self.syncing.updating_superblock;

            self.state_machine_opened = false;
            self.state_machine.reset();

            self.grid.free_set.reset();
            self.grid.free_set_checkpoint.reset();
            self.client_sessions_checkpoint.reset();
            self.client_sessions.reset();

            // Bump commit_max before the superblock update so that a view_durable_update()
            // during the sync_start update uses the correct (new) commit_max.
            self.advance_commit_max(stage.target.checkpoint_op, @src());

            const sync_op_max =
                vsr.Checkpoint.trigger_for_checkpoint(stage.target.checkpoint_op).?;
            const sync_op_min = sync_op_min: {
                const syncing_already = self.superblock.staging.vsr_state.sync_op_max > 0;
                const sync_min_old = self.superblock.staging.vsr_state.sync_op_min;

                const sync_min_new = if (vsr.Checkpoint.trigger_for_checkpoint(
                    self.op_checkpoint(),
                )) |trigger|
                    // +1 because sync_op_min is inclusive, but (when !syncing_already)
                    // `vsr_state.commit_min` itself does not need to be synced.
                    trigger + 1
                else
                    0;

                break :sync_op_min if (syncing_already)
                    @min(sync_min_old, sync_min_new)
                else
                    sync_min_new;
            };

            self.sync_message_timeout.stop();
            self.superblock.sync(
                sync_superblock_update_callback,
                &self.superblock_context,
                .{
                    .checkpoint = stage.checkpoint_state,
                    .commit_max = self.commit_max,
                    .sync_op_max = sync_op_max,
                    .sync_op_min = sync_op_min,
                },
            );
        }

        fn sync_superblock_update_callback(superblock_context: *SuperBlock.Context) void {
            const self = @fieldParentPtr(Self, "superblock_context", superblock_context);
            assert(self.sync_tables == null);
            assert(self.grid.read_global_queue.empty());
            assert(self.grid.write_queue.empty());
            assert(self.grid_repair_tables.executing() == 0);
            assert(self.grid_repair_writes.executing() == 0);
            assert(self.grid.blocks_missing.faulty_blocks.count() == 0);
            assert(self.syncing == .updating_superblock);
            assert(!self.state_machine_opened);

            const stage: *const SyncStage.UpdatingSuperBlock = &self.syncing.updating_superblock;

            assert(self.superblock.working.checkpoint_id() == stage.target.checkpoint_id);
            assert(stdx.equal_bytes(
                vsr.CheckpointState,
                &self.superblock.working.vsr_state.checkpoint,
                &stage.checkpoint_state,
            ));

            self.commit_min = self.superblock.working.vsr_state.checkpoint.commit_min;
            if (self.op < self.commit_min) {
                self.transition_to_recovering_head();
            }

            self.grid.open(grid_open_callback);
            self.sync_dispatch(.idle);
        }

        /// We have just:
        /// - finished superblock sync,
        /// - replaced our superblock,
        /// - repaired the manifest blocks,
        /// - and opened the state machine.
        /// Now we sync:
        /// - the missed LSM table blocks (index/data).
        fn sync_content(self: *Self) void {
            assert(self.syncing == .idle);
            assert(self.state_machine_opened);
            assert(self.superblock.working.vsr_state.sync_op_max > 0);
            assert(self.sync_tables == null);
            assert(self.grid_repair_tables.executing() == 0);

            self.sync_tables = .{};
            if (self.grid_repair_tables.available() > 0) {
                self.sync_enqueue_tables();
            }
            // Client replies are synced in lockstep with client sessions in
            // `client_sessions_open_callback`.
        }

        pub fn sync_content_done(self: *const Self) bool {
            if (self.superblock.staging.vsr_state.sync_op_max == 0) {
                return true;
            } else {
                for (0..constants.clients_max) |entry_slot| {
                    if (self.client_sessions.entries_free.isSet(entry_slot)) continue;

                    const entry = &self.client_sessions.entries[entry_slot];
                    if (entry.header.op >= self.superblock.working.vsr_state.sync_op_min and
                        entry.header.op <= self.superblock.working.vsr_state.sync_op_max)
                    {
                        if (!self.client_replies.reply_durable(.{ .index = entry_slot })) {
                            return false;
                        }
                    }
                }
                return self.sync_tables == null and self.grid_repair_tables.executing() == 0;
            }
        }

        /// State sync finished, and we must repair all of the tables we missed.
        fn sync_enqueue_tables(self: *Self) void {
            assert(self.syncing == .idle);
            assert(self.sync_tables != null);
            assert(self.state_machine_opened);
            assert(self.superblock.working.vsr_state.sync_op_max > 0);
            assert(self.grid_repair_tables.available() > 0);

            const sync_op_min = self.superblock.working.vsr_state.sync_op_min;
            const sync_op_max = self.superblock.working.vsr_state.sync_op_max;
            while (self.sync_tables.?.next(&self.state_machine.forest)) |table_info| {
                assert(self.grid_repair_tables.available() > 0);
                assert(table_info.label.event == .reserved);

                //FIXME
                if (table_info.snapshot_min.timestamp >= Snapshot.readable_at_commit(sync_op_min) and
                    table_info.snapshot_min.timestamp <= Snapshot.readable_at_commit(sync_op_max))
                {
                    log.debug("{}: sync_enqueue_tables: " ++
                        "request address={} checksum={} level={} snapshot_min={} ({}..{})", .{
                        self.replica,
                        table_info.address,
                        table_info.checksum,
                        table_info.label.level,
                        table_info.snapshot_min,
                        Snapshot.readable_at_commit(sync_op_min),
                        Snapshot.readable_at_commit(sync_op_max),
                    });

                    const table = self.grid_repair_tables.acquire().?;
                    table.* = .{ .replica = self, .table = undefined };

                    const enqueue_result = self.grid.blocks_missing.enqueue_table(
                        &table.table,
                        table_info.address,
                        table_info.checksum,
                    );

                    switch (enqueue_result) {
                        .insert => {},
                        .duplicate => {
                            // Duplicates are only possible due to move-table.
                            assert(table_info.label.level > 0);

                            self.grid_repair_tables.release(table);
                        },
                    }

                    if (self.grid_repair_tables.available() == 0) break;
                } else {
                    if (StateMachine.Forest.Storage == TestStorage) {
                        self.superblock.storage.verify_table(
                            table_info.address,
                            table_info.checksum,
                        );
                    }
                }
            }

            if (self.grid_repair_tables.executing() == 0) {
                assert(self.sync_tables.?.next(&self.state_machine.forest) == null);

                log.debug("{}: sync_enqueue_tables: all tables synced (commit={}..{})", .{
                    self.replica,
                    sync_op_min,
                    sync_op_max,
                });

                self.sync_tables = null;
            }
        }

        fn sync_reclaim_tables(self: *Self) void {
            while (self.grid.blocks_missing.reclaim_table()) |queue_table| {
                const table = @fieldParentPtr(RepairTable, "table", queue_table);
                self.grid_repair_tables.release(table);
            }
            assert(self.grid_repair_tables.available() <= constants.grid_missing_tables_max);

            if (self.sync_tables) |_| {
                assert(self.syncing == .idle);
                assert(self.grid.callback != .cancel);

                if (self.grid_repair_tables.available() > 0) {
                    self.sync_enqueue_tables();
                }
            }
        }

        /// Whether it is safe to commit or send prepare_ok messages.
        /// Returns true if the hash chain is valid and up to date for the current view.
        /// This is a stronger guarantee than `valid_hash_chain_between()` below.
        fn valid_hash_chain(self: *Self, source: SourceLocation) bool {
            assert(self.op_checkpoint() <= self.commit_min);
            assert(self.op_checkpoint() <= self.op);

            // If we know we could validate the hash chain even further, then wait until we can:
            // This is partial defense-in-depth in case `self.op` is ever advanced by a reordered op.
            if (self.op < self.op_repair_max()) {
                log.debug("{}: {s}: waiting for repair (op={} < op_repair_max={}, commit_max={})", .{
                    self.replica,
                    source.fn_name,
                    self.op,
                    self.op_repair_max(),
                    self.commit_max,
                });
                return false;
            }

            if (self.op == self.op_checkpoint()) {
                // The head op almost always exceeds op_checkpoint because the
                // previous checkpoint trigger is ahead of op_checkpoint by a measure.
                //
                // However, state sync arrives at the op_checkpoint unconventionally –
                // the ops between the checkpoint and the previous checkpoint trigger may not be
                // in our journal yet.
                log.debug("{}: {s}: recently synced; waiting for ops (op=checkpoint={})", .{
                    self.replica,
                    source.fn_name,
                    self.op,
                });
                return false;
            }

            // When commit_min=op_checkpoint, the checkpoint may be missing.
            // valid_hash_chain_between() will still verify that we are connected.
            const op_verify_min = @max(self.commit_min, self.op_checkpoint() + 1);

            // We must validate the hash chain as far as possible, since `self.op` may disclose a fork:
            if (!self.valid_hash_chain_between(op_verify_min, self.op)) {
                log.debug("{}: {s}: waiting for repair (hash chain)", .{ self.replica, source.fn_name });
                return false;
            }

            return true;
        }

        /// Returns true if all operations are present, correctly ordered and connected by hash
        /// chain, between `op_min` and `op_max` (both inclusive).
        fn valid_hash_chain_between(self: *const Self, op_min: u64, op_max: u64) bool {
            assert(op_min <= op_max);
            assert(op_min <= self.commit_min + 1);
            assert(op_min <= self.commit_min or self.commit_min == self.op_checkpoint());
            assert(op_max >= self.op_checkpoint());

            // If we use anything less than self.op then we may commit ops for a forked hash chain
            // that have since been reordered by a new primary.
            assert(op_max == self.op);
            var b = self.journal.header_with_op(op_max).?;

            var op = op_max;
            while (op > op_min) {
                op -= 1;

                if (self.journal.header_with_op(op)) |a| {
                    assert(a.op + 1 == b.op);
                    if (a.checksum == b.parent) {
                        assert(ascending_viewstamps(a, b));
                        b = a;
                    } else {
                        log.debug("{}: valid_hash_chain_between: break: A: {}", .{ self.replica, a });
                        log.debug("{}: valid_hash_chain_between: break: B: {}", .{ self.replica, b });
                        return false;
                    }
                } else {
                    log.debug("{}: valid_hash_chain_between: missing op={}", .{ self.replica, op });
                    return false;
                }
            }
            assert(b.op == op_min);

            // The op immediately after the checkpoint always connects to the checkpoint.
            if (op_min <= self.op_checkpoint() + 1 and op_max > self.op_checkpoint()) {
                assert(self.superblock.working.vsr_state.checkpoint.commit_min ==
                    self.op_checkpoint());
                assert(self.superblock.working.vsr_state.checkpoint.commit_min_checksum ==
                    self.journal.header_with_op(self.op_checkpoint() + 1).?.parent);
            }

            return true;
        }

        fn jump_view(self: *Self, header: *const Header) void {
            if (header.view < self.view) return;
            if (header.replica >= self.replica_count) return; // Ignore messages from standbys.

            const to: Status = switch (header.command) {
                .prepare, .commit => .normal,
                // When we are recovering_head we can't participate in a view-change anyway.
                // But there is a chance that the primary is actually running, despite the DVC/SVC.
                .do_view_change,
                .start_view_change,
                // For pings, we don't actually know where the new view is started or not.
                // Conservatively transition to view change: at worst, we'll send a larger DVC
                // instead of a RSV.
                .ping,
                .pong,
                => if (self.status == .recovering_head) Status.normal else .view_change,
                // on_start_view() handles the (possible) transition to view-change manually, before
                // transitioning to normal.
                .start_view => return,
                else => return,
            };

            if (self.standby()) {
                // Standbys don't participate in view changes, so switching to `.view_change` is
                // useless. This also prevents an isolated replica from locking a standby into a
                // view higher than that of the rest of the cluster.
                if (to != .normal) return;
            }

            // Compare status transitions and decide whether to view jump or ignore:
            switch (self.status) {
                .normal => switch (to) {
                    // If the transition is to `.normal`, then ignore if for the same view:
                    .normal => if (header.view == self.view) return,
                    // If the transition is to `.view_change`, then ignore if the view has started:
                    .view_change => if (header.view == self.view) return,
                    else => unreachable,
                },
                .view_change => switch (to) {
                    // This is an interesting special case:
                    // If the transition is to `.normal` in the same view, then we missed the
                    // `start_view` message and we must also consider this a view jump:
                    // If we don't handle this below then our `view_change_status_timeout` will fire
                    // and we will disrupt the cluster with another view change for a newer view.
                    .normal => {},
                    // If the transition is to `.view_change`, then ignore if for the same view:
                    .view_change => if (header.view == self.view) return,
                    else => unreachable,
                },
                // We need a start_view from any other replica — don't request it from ourselves.
                .recovering_head => if (self.primary_index(header.view) == self.replica) return,
                .recovering => return,
            }

            switch (to) {
                .normal => {
                    if (header.view == self.view) {
                        assert(self.status == .view_change or self.status == .recovering_head);

                        log.debug("{}: jump_view: waiting to exit view change", .{self.replica});
                    } else {
                        assert(header.view > self.view);
                        assert(self.status == .view_change or self.status == .recovering_head or
                            self.status == .normal);

                        log.debug("{}: jump_view: waiting to jump to newer view ({}..{})", .{
                            self.replica,
                            self.view,
                            header.view,
                        });
                    }

                    // TODO Debounce and decouple this from `on_message()` by moving into `tick()`:
                    // (Using request_start_view_message_timeout).
                    log.debug("{}: jump_view: requesting start_view message", .{self.replica});
                    self.send_header_to_replica(
                        self.primary_index(header.view),
                        @bitCast(Header.RequestStartView{
                            .command = .request_start_view,
                            .cluster = self.cluster,
                            .replica = self.replica,
                            .view = header.view,
                            .nonce = self.nonce,
                        }),
                    );
                },
                .view_change => {
                    assert(self.status == .normal or self.status == .view_change);
                    assert(self.view < header.view);
                    assert(!self.standby());

                    if (header.view == self.view + 1) {
                        log.debug("{}: jump_view: jumping to view change", .{self.replica});
                    } else {
                        log.debug("{}: jump_view: jumping to next view change", .{self.replica});
                    }
                    self.transition_to_view_change_status(header.view);
                },
                else => unreachable,
            }
        }

        fn jump_sync_target(self: *Self, header: *const Header) void {
            if (header.into_const(.commit)) |h| assert(h.commit >= h.checkpoint_op);

            if (header.replica >= self.replica_count) return; // Ignore messages from standbys.
            if (header.replica == self.replica) return; // Ignore messages from self (misdirected).

            const candidate = switch (header.into_any()) {
                inline .commit, .ping => |h| .{
                    .checkpoint_id = h.checkpoint_id,
                    .checkpoint_op = h.checkpoint_op,
                },
                else => return,
            };

            if (candidate.checkpoint_op == self.op_checkpoint() and
                candidate.checkpoint_id != self.superblock.working.checkpoint_id())
            {
                log.err("{}: on_{s}: jump_sync_target: checkpoint diverged " ++
                    "(op={} id_from_superblock={x:0>32} id_from_commit={x:0>32})", .{
                    self.replica,
                    @tagName(header.command),
                    candidate.checkpoint_op,
                    self.superblock.working.checkpoint_id(),
                    candidate.checkpoint_id,
                });

                // Either this replica, the header's replica, or both, have diverged.
                @panic("checkpoint diverged");
            }

            // Don't sync backwards, or to our current checkpoint.
            if (candidate.checkpoint_op <= self.op_checkpoint()) return;

            // Don't sync to the immediately-next checkpoint unless it has been committed atop.
            // - If the checkpoint has been committed atop, that guarantees that at least a
            //   commit-quorum of replicas has reached that exact checkpoint.
            // - If the next checkpoint has *not* been committed atop yet, then we can (and should)
            //   sync via WAL replay instead to maximize durability.
            //
            // To understand why this is critical, consider the case (R=3) where:
            // 1. R₀ is primary.
            // 2. R₀ prepares and acks all ops.
            // 3. R₁ prepares and acks odd ops.
            // 4. R₂ prepares and acks even ops.
            // 5. R₀ is able to commit an entire checkpoint, but not beyond that.
            // If R₁/R₂ state-sync to R₀'s latest checkpoint, then a single block corruption on R₀
            // could lead to permanent unavailability.
            if (candidate.checkpoint_op == self.op_checkpoint_next()) {
                if (header.into_const(.commit)) |h| {
                    if (h.commit <= self.op_checkpoint_next_trigger()) return;
                } else {
                    return;
                }
            }

            if (self.sync_target_max != null and
                self.sync_target_max.?.checkpoint_op >= candidate.checkpoint_op)
            {
                return;
            }

            log.debug("{}: on_{s}: jump_sync_target: op={} id={x:0>32} (syncing={s})", .{
                self.replica,
                @tagName(header.command),
                candidate.checkpoint_op,
                candidate.checkpoint_id,
                @tagName(self.syncing),
            });

            self.sync_target_max = .{
                .checkpoint_id = candidate.checkpoint_id,
                .checkpoint_op = candidate.checkpoint_op,
            };

            if (self.syncing != .idle) {
                self.sync_start_from_sync();
            }
        }

        fn write_prepare(
            self: *Self,
            message: *Message.Prepare,
            trigger: Journal.Write.Trigger,
        ) void {
            assert(self.status == .normal or self.status == .view_change);
            assert(self.status == .normal or self.primary_index(self.view) == self.replica);
            assert(self.status == .normal or self.do_view_change_quorum);
            assert(message.base().references > 0);
            assert(message.header.command == .prepare);
            assert(message.header.operation != .reserved);
            assert(message.header.view <= self.view);
            assert(message.header.op <= self.op);
            assert(message.header.op >= self.op_repair_min());

            if (!self.journal.has(message.header)) {
                log.debug("{}: write_prepare: ignoring op={} checksum={} (header changed)", .{
                    self.replica,
                    message.header.op,
                    message.header.checksum,
                });
                return;
            }

            if (self.journal.writing(message.header.op, message.header.checksum)) {
                log.debug("{}: write_prepare: ignoring op={} checksum={} (already writing)", .{
                    self.replica,
                    message.header.op,
                    message.header.checksum,
                });
                return;
            }

            // Criteria for caching:
            // - The primary does not update the cache since it is (or will be) reconstructing its
            //   pipeline.
            // - Cache uncommitted ops, since it will avoid a WAL read in the common case.
            if (self.pipeline == .cache and
                self.replica != self.primary_index(self.view) and
                self.commit_min < message.header.op)
            {
                const prepare_evicted = self.pipeline.cache.insert(message.ref());
                if (prepare_evicted) |m| self.message_bus.unref(m);
            }

            self.journal.write_prepare(write_prepare_callback, message, trigger);
        }

        fn write_prepare_callback(
            self: *Self,
            wrote: ?*Message.Prepare,
            trigger: Journal.Write.Trigger,
        ) void {
            // `null` indicates that we did not complete the write for some reason.
            const message = wrote orelse return;

            self.send_prepare_ok(message.header);
            defer self.flush_loopback_queue();

            switch (trigger) {
                .append => {},
                // If this was a repair, continue immediately to repair the next prepare:
                // This is an optimization to eliminate waiting until the next repair timeout.
                .repair => self.repair(),
                .pipeline => self.repair(),
                .fix => unreachable,
            }
        }

        fn send_request_blocks(self: *Self) void {
            assert(self.grid_repair_message_timeout.ticking);
            assert(self.grid.callback != .cancel);
            maybe(self.state_machine_opened);

            var message = self.message_bus.get_message(.request_blocks);
            defer self.message_bus.unref(message);

            const requests = std.mem.bytesAsSlice(
                vsr.BlockRequest,
                message.buffer[@sizeOf(Header)..],
            )[0..constants.grid_repair_request_max];
            assert(requests.len > 0);

            const requests_count: u32 = @intCast(self.grid.next_batch_of_block_requests(requests));
            if (requests_count == 0) return;

            for (requests[0..requests_count]) |*request| {
                assert(!self.grid.free_set.is_free(request.block_address));

                log.debug("{}: send_request_blocks: request address={} checksum={}", .{
                    self.replica,
                    request.block_address,
                    request.block_checksum,
                });
            }

            message.header.* = .{
                .command = .request_blocks,
                .cluster = self.cluster,
                .replica = self.replica,
                .size = @sizeOf(Header) + requests_count * @sizeOf(vsr.BlockRequest),
            };
            message.header.set_checksum_body(message.body());
            message.header.set_checksum();

            self.send_message_to_replica(self.choose_any_other_replica(), message);
        }

        fn send_request_sync_checkpoint(self: *Self) void {
            assert(!self.solo());
            assert(self.syncing == .requesting_checkpoint);
            assert(self.sync_message_timeout.ticking);

            const stage: *const SyncStage.RequestingCheckpoint =
                &self.syncing.requesting_checkpoint;

            self.send_header_to_replica(
                self.choose_any_other_replica(),
                @bitCast(Header.RequestSyncCheckpoint{
                    .command = .request_sync_checkpoint,
                    .cluster = self.cluster,
                    .replica = self.replica,
                    .size = @sizeOf(Header),
                    .checkpoint_id = stage.target.checkpoint_id,
                    .checkpoint_op = stage.target.checkpoint_op,
                }),
            );
        }

        fn send_sync_checkpoint(self: *Self, parameters: struct { replica: u8 }) void {
            assert(!self.standby());
            assert(self.syncing == .idle);
            assert(self.replica != parameters.replica);

            const reply = self.message_bus.get_message(.sync_checkpoint);
            defer self.message_bus.unref(reply);

            reply.header.* = .{
                .command = .sync_checkpoint,
                .cluster = self.cluster,
                .replica = self.replica,
                .size = @sizeOf(Header) + @sizeOf(vsr.CheckpointState),
                .checkpoint_id = self.superblock.working.checkpoint_id(),
                .checkpoint_op = self.op_checkpoint(),
            };

            stdx.copy_disjoint(
                .exact,
                u8,
                reply.body(),
                std.mem.asBytes(&self.superblock.working.vsr_state.checkpoint),
            );

            reply.header.set_checksum_body(reply.body());
            reply.header.set_checksum();

            // Note that our checkpoint may not be canonical — that is the syncing replica's
            // responsibility to check.
            self.send_message_to_replica(parameters.replica, reply);
        }
    };
}

/// A do-view-change:
/// - selects the view's head (modulo nack+truncation during repair)
/// - discards uncommitted ops (to maximize availability in the presence of storage faults)
/// - retains all committed ops
/// - retains all possibly-committed ops (because they might be committed — we can't tell)
///   (Some of these may be discarded during repair, via the nack protocol).
/// Refer to the CTRL protocol from Protocol-Aware Recovery for Consensus-Based Storage.
///
/// Terminology:
///
/// - *DVC* refers to a command=do_view_change message.
/// - *SV* refers to a command=start_view message.
///
/// - The *head* message (of a view) is the message (committed or uncommitted) within that view with
///   the highest op.
///
/// - *gap*: There is a header for op X and X+n (n>1), but no header at op X+1.
/// - *blank*: A header that explicitly marks a gap in the DVC headers.
///   (See `vsr.Headers.dvc_blank()`).
/// - *break*/*chain break*: The header for op X is not the parent of the header for op X+1.
/// - *fork*: A correctness bug in which a committed (or possibly committed) message is discarded.
///
/// The cluster can have many different "versions" of the "same" header.
/// That is, different headers (different checksum) with the same op.
/// But at most one version (per op) is "canonical", the remainder are "uncanonical".
/// - A *canonical message* is any DVC message from the most recent log_view in the quorum.
/// - An *uncanonical header* may have been removed/changed during a prior view.
/// - A *canonical header* was part of the most recent log_view.
///   - (That is, the canonical headers are the union of headers from all canonical messages).
///   - Canonical headers do not necessarily survive into the new view, but they take
///     precedence over uncanonical headers.
///   - Canonical headers may be committed or uncommitted.
///
///
/// Invariants (for each DVC message):
///
/// - The "valid" headers all belong to the same hash chain.
///   - Reason: If multiple replicas with the same canonical log_view disagree about an op, the new
///     primary could not determine which is correct.
///   - The DVC-sender is responsible for ensuring blanks do not conceal chain breaks.
///   - For example,
///     - a DVC of 6a,7_,8a is valid (6a/8a belong to the same chain).
///     - a DVC of 6b,7_,8a is invalid (the gap at 7 conceal a chain break).
///     - a DVC of 6b,7b,8a is invalid (7b/8a is a chain break)..
/// - All pipeline headers present on the replica must be included in the DVC headers.
///   - When `replica.commit_max ≤ replica.op`,
///     the DVC must include a valid/blank header for every op in that range.
///   - When `replica.commit_max > replica.op`, only a single header is included
///     (`replica.commit_max` if available in the SV, otherwise `replica.op`).
///   - (The DVC will need a valid header corresponding to its `commit_max` to complete, since the
///     entire pipeline may be truncated, and the new primary still needs a header for its head op.)
///
/// Each header in the DVC body is one of:
///
/// | Header State || Derived Information
/// | Blank | Nack ||  Nack | Description
/// |-------|------||-------|-------------
/// |   yes |  yes ||   yes | No header, and replica did not prepare this op during its last view.
/// |   yes |   no ||    no | No header, but the replica may have prepared this op during its last
/// |       |      ||       | view. Since the replica does not know the header, it cannot nack.
/// |    no |  yes ||   yes | Valid header, but the replica has never prepared the message.
/// |    no |   no || maybe | Valid header, and the replica has prepared the message.
/// |       |      ||       | Counts as a nack iff the header does not match the canonical header
/// |       |      ||       | for this op.
///
/// Where:
///
/// - Blank:
///   - Yes: Send a bogus header that indicates that the sender does not know the actual
///     command=prepare header for that op.
///   - No: Send the actual header. The corresponding header may be corrupt, prepared, or nacked.
/// - Nack (header state):
///   - Yes: The corresponding header in the message body was definitely not
///     prepared in the latest view. (The corresponding header may be blank or ¬blank).
///   - No: The corresponding header in the message body was either prepared during the latest view,
///     or _might_ have been prepared, but due to WAL corruption we can't tell.
/// - Nack (derived): based on Blank/Nack, whether the new primary counts it as a nack.
/// - The header corresponding to the sender's `replica.op` is always "valid", never a "blank".
///   (Otherwise the replica would be in status=recovering_head and unable to participate).
///
/// Invariants (across all DVCs in the quorum):
///
/// - The valid headers of every DVC with the same log_view must not conflict.
///   - In other words:
///     dvc₁.headers[i].op       == dvc₂.headers[j].op implies
///     dvc₁.headers[i].checksum == dvc₂.headers[j].checksum.
///   - Reason: the headers bundled with the DVC(s) with the highest log_view will be
///     loaded into the new primary with `replace_header()`, not `repair_header()`.
/// - Any pipeline message which could have been committed is included in some canonical DVC.
///
/// Perhaps unintuitively, it is safe to advertise a header before its message is prepared
/// (e.g. the write is still queued, or the prepare has not arrived). The header is either:
///
/// - committed — so another replica in the quorum must have a copy, according to the quorum
///   intersection property. Or,
/// - uncommitted — if the header is chosen, but cannot be recovered from any replica, then
///   it will be discarded by the nack protocol.
const DVCQuorum = struct {
    const DVCArray = stdx.BoundedArray(*const Message.DoViewChange, constants.replicas_max);

    fn verify(dvc_quorum: DVCQuorumMessages) void {
        const dvcs = DVCQuorum.dvcs_all(dvc_quorum);
        for (dvcs.const_slice()) |message| verify_message(message);

        // Verify that DVCs with the same log_view do not conflict.
        for (dvcs.const_slice(), 0..) |dvc_a, i| {
            for (dvcs.const_slice()[0..i]) |dvc_b| {
                if (dvc_a.header.log_view != dvc_b.header.log_view) continue;

                const headers_a = message_body_as_view_headers(dvc_a.base_const());
                const headers_b = message_body_as_view_headers(dvc_b.base_const());
                // Find the intersection of the ops covered by each DVC.
                const op_max = @min(dvc_a.header.op, dvc_b.header.op);
                const op_min = @max(
                    headers_a.slice[headers_a.slice.len - 1].op,
                    headers_b.slice[headers_b.slice.len - 1].op,
                );
                // If a replica is lagging, its headers may not overlap at all.
                maybe(op_min > op_max);

                var op = op_min;
                while (op <= op_max) : (op += 1) {
                    const header_a = &headers_a.slice[dvc_a.header.op - op];
                    const header_b = &headers_b.slice[dvc_b.header.op - op];
                    if (vsr.Headers.dvc_header_type(header_a) == .valid and
                        vsr.Headers.dvc_header_type(header_b) == .valid)
                    {
                        assert(header_a.checksum == header_b.checksum);
                    }
                }
            }
        }
    }

    fn verify_message(message: *const Message.DoViewChange) void {
        assert(message.header.command == .do_view_change);
        assert(message.header.commit_min <= message.header.op);

        const checkpoint = message.header.checkpoint_op;
        assert(checkpoint <= message.header.commit_min);

        // The log_view:
        // * may be higher than the view in any of the prepare headers.
        // * must be lower than the view of this view change.
        const log_view = message.header.log_view;
        assert(log_view < message.header.view);

        // Ignore the result, init() verifies the headers.
        const headers = message_body_as_view_headers(message.base_const());
        assert(headers.slice.len >= 1);
        assert(headers.slice.len <= constants.pipeline_prepare_queue_max + 1);
        assert(headers.slice[0].op == message.header.op);
        assert(headers.slice[0].view <= log_view);

        const nacks = message.header.nack_bitset;
        comptime assert(@TypeOf(nacks) == u128);
        assert(@popCount(nacks) <= headers.slice.len);
        assert(@clz(nacks) + headers.slice.len >= @bitSizeOf(u128));

        const present = message.header.present_bitset;
        comptime assert(@TypeOf(present) == u128);
        assert(@popCount(present) <= headers.slice.len);
        assert(@clz(present) + headers.slice.len >= @bitSizeOf(u128));
    }

    fn dvcs_all(dvc_quorum: DVCQuorumMessages) DVCArray {
        var array = DVCArray{};
        for (dvc_quorum, 0..) |received, replica| {
            if (received) |message| {
                assert(message.header.command == .do_view_change);
                assert(message.header.replica == replica);

                array.append_assume_capacity(message);
            }
        }
        return array;
    }

    fn dvcs_canonical(dvc_quorum: DVCQuorumMessages) DVCArray {
        return dvcs_with_log_view(dvc_quorum, DVCQuorum.log_view_max(dvc_quorum));
    }

    fn dvcs_with_log_view(dvc_quorum: DVCQuorumMessages, log_view: u32) DVCArray {
        var array = DVCArray{};
        const dvcs = DVCQuorum.dvcs_all(dvc_quorum);
        for (dvcs.const_slice()) |message| {
            if (message.header.log_view == log_view) {
                array.append_assume_capacity(message);
            }
        }
        return array;
    }

    fn dvcs_uncanonical(dvc_quorum: DVCQuorumMessages) DVCArray {
        const log_view_max_ = DVCQuorum.log_view_max(dvc_quorum);
        var array = DVCArray{};
        const dvcs = DVCQuorum.dvcs_all(dvc_quorum);
        for (dvcs.const_slice()) |message| {
            assert(message.header.log_view <= log_view_max_);

            if (message.header.log_view < log_view_max_) {
                array.append_assume_capacity(message);
            }
        }
        return array;
    }

    fn op_checkpoint_max(dvc_quorum: DVCQuorumMessages) u64 {
        var checkpoint_max: ?u64 = null;
        const dvcs = dvcs_all(dvc_quorum);
        for (dvcs.const_slice()) |dvc| {
            const dvc_checkpoint = dvc.header.checkpoint_op;
            if (checkpoint_max == null or checkpoint_max.? < dvc_checkpoint) {
                checkpoint_max = dvc_checkpoint;
            }
        }
        return checkpoint_max.?;
    }

    /// Returns the highest `log_view` of any DVC.
    ///
    /// The headers bundled with DVCs with the highest `log_view` are canonical, since
    /// the replica has knowledge of previous view changes in which headers were replaced.
    fn log_view_max(dvc_quorum: DVCQuorumMessages) u32 {
        var log_view_max_: ?u32 = null;
        const dvcs = DVCQuorum.dvcs_all(dvc_quorum);
        for (dvcs.const_slice()) |message| {
            // `log_view` is the view when this replica was last in normal status, which:
            // * may be higher than the view in any of the prepare headers.
            // * must be lower than the view of this view change.
            assert(message.header.log_view < message.header.view);

            if (log_view_max_ == null or log_view_max_.? < message.header.log_view) {
                log_view_max_ = message.header.log_view;
            }
        }
        return log_view_max_.?;
    }

    fn commit_max(dvc_quorum: DVCQuorumMessages) u64 {
        const dvcs = DVCQuorum.dvcs_all(dvc_quorum);
        assert(dvcs.count() > 0);

        var commit_max_: u64 = 0;
        for (dvcs.const_slice()) |dvc| {
            const dvc_headers = message_body_as_view_headers(dvc.base_const());
            // DVC generation stops when a header with op ≤ commit_max is appended.
            const dvc_commit_max_tail = dvc_headers.slice[dvc_headers.slice.len - 1].op;
            // An op cannot be uncommitted if it is definitely outside the pipeline.
            // Use `do_view_change_op_head` instead of `replica.op` since the former is
            // about to become the new `replica.op`.
            const dvc_commit_max_pipeline =
                dvc.header.op -| constants.pipeline_prepare_queue_max;

            commit_max_ = @max(commit_max_, dvc_commit_max_tail);
            commit_max_ = @max(commit_max_, dvc_commit_max_pipeline);
            commit_max_ = @max(commit_max_, dvc.header.commit_min);
            commit_max_ = @max(commit_max_, dvc_headers.slice[0].commit);
        }
        return commit_max_;
    }

    /// Returns the highest `timestamp` from any replica.
    fn timestamp_max(dvc_quorum: DVCQuorumMessages) u64 {
        var timestamp_max_: ?u64 = null;
        const dvcs = DVCQuorum.dvcs_all(dvc_quorum);
        for (dvcs.const_slice()) |dvc| {
            const dvc_headers = message_body_as_view_headers(dvc.base_const());
            const dvc_head = &dvc_headers.slice[0];
            if (timestamp_max_ == null or timestamp_max_.? < dvc_head.timestamp) {
                timestamp_max_ = dvc_head.timestamp;
            }
        }
        return timestamp_max_.?;
    }

    fn op_max_canonical(dvc_quorum: DVCQuorumMessages) u64 {
        var op_max: ?u64 = null;
        const dvcs = DVCQuorum.dvcs_canonical(dvc_quorum);
        for (dvcs.const_slice()) |message| {
            if (op_max == null or op_max.? < message.header.op) {
                op_max = message.header.op;
            }
        }
        return op_max.?;
    }

    /// When the view is ready to begin:
    /// - Return an iterator over the canonical DVC's headers, from high-to-low op.
    ///   The first header returned is the new head message.
    /// Otherwise:
    /// - Return the reason the view cannot begin.
    fn quorum_headers(dvc_quorum: DVCQuorumMessages, options: struct {
        quorum_nack_prepare: u8,
        quorum_view_change: u8,
        replica_count: u8,
    }) union(enum) {
        // The quorum has fewer than "quorum_view_change" DVCs.
        // We are waiting for DVCs from the remaining replicas.
        awaiting_quorum,
        // The quorum has at least "quorum_view_change" DVCs.
        // The quorum has fewer than "replica_count" DVCs.
        // The quorum collected so far is insufficient to determine which headers can be nacked
        // (due to an excess of faults).
        // We must wait for DVCs from one or more remaining replicas.
        awaiting_repair,
        // All replicas have contributed a DVC, but there are too many faults to start a new view.
        // The cluster is deadlocked, unable to ever complete a view change.
        complete_invalid,
        // The quorum is complete, and sufficient to start the new view.
        complete_valid: HeaderIterator,
    } {
        assert(options.replica_count >= 2);
        assert(options.replica_count <= constants.replicas_max);
        assert(options.quorum_view_change >= 2);
        assert(options.quorum_view_change <= options.replica_count);
        if (options.replica_count == 2) {
            assert(options.quorum_nack_prepare == 1);
        } else {
            assert(options.quorum_nack_prepare == options.quorum_view_change);
        }

        const dvcs_all_ = DVCQuorum.dvcs_all(dvc_quorum);
        if (dvcs_all_.count() < options.quorum_view_change) return .awaiting_quorum;

        const log_view_canonical = DVCQuorum.log_view_max(dvc_quorum);
        const dvcs_canonical_ = DVCQuorum.dvcs_canonical(dvc_quorum);
        assert(dvcs_canonical_.count() > 0);
        assert(dvcs_canonical_.count() <= dvcs_all_.count());

        const op_head_max = DVCQuorum.op_max_canonical(dvc_quorum);
        const op_head_min = DVCQuorum.commit_max(dvc_quorum);

        // Iterate the highest definitely committed op and all maybe-uncommitted ops.
        var op = op_head_min;
        const op_head = while (op <= op_head_max) : (op += 1) {
            const header_canonical = for (dvcs_canonical_.const_slice()) |dvc| {
                // This DVC is canonical, but lagging far behind.
                if (dvc.header.op < op) continue;

                const headers = message_body_as_view_headers(dvc.base_const());
                const header_index = dvc.header.op - op;
                assert(header_index <= headers.slice.len);

                const header = &headers.slice[header_index];
                assert(header.op == op);

                if (vsr.Headers.dvc_header_type(header) == .valid) break header;
            } else null;

            var copies: usize = 0;
            var nacks: usize = 0;
            for (dvcs_all_.const_slice()) |dvc| {
                if (dvc.header.op < op) {
                    nacks += 1;
                    continue;
                }

                const headers = message_body_as_view_headers(dvc.base_const());
                const header_index = dvc.header.op - op;
                if (header_index >= headers.slice.len) {
                    nacks += 1;
                    continue;
                }

                const header = &headers.slice[header_index];
                assert(header.op == op);
                assert(header.view <= log_view_canonical);

                const header_nacks = std.bit_set.IntegerBitSet(128){ .mask = dvc.header.nack_bitset };
                const header_present = std.bit_set.IntegerBitSet(128){ .mask = dvc.header.present_bitset };

                if (vsr.Headers.dvc_header_type(header) == .valid and
                    header_present.isSet(header_index) and
                    header_canonical != null and header_canonical.?.checksum == header.checksum)
                {
                    copies += 1;
                }

                if (header_nacks.isSet(header_index)) {
                    // The op is nacked explicitly.
                    nacks += 1;
                } else if (vsr.Headers.dvc_header_type(header) == .valid) {
                    if (header_canonical != null and header_canonical.?.checksum != header.checksum) {
                        assert(dvc.header.log_view < log_view_canonical);
                        // The op is nacked implicitly, because the replica has a different header.
                        nacks += 1;
                    }
                    if (header_canonical == null) {
                        assert(header.view < log_view_canonical);
                        assert(dvc.header.log_view < log_view_canonical);
                        // The op is nacked implicitly, because the header has already been
                        // truncated in the latest log_view.
                        nacks += 1;
                    }
                }
            }

            // This is an abbreviated version of Protocol-Aware Recovery's CTRL protocol.
            // When we can confirm that an op is definitely uncommitted, truncate it to
            // improve availability.
            if (nacks >= options.quorum_nack_prepare) {
                // Never nack op_head_min (aka commit_max).
                assert(op > op_head_min);
                break op - 1;
            }

            if (header_canonical == null or
                (header_canonical != null and copies == 0))
            {
                if (dvcs_all_.count() < options.replica_count) {
                    return .awaiting_repair;
                } else {
                    return .complete_invalid;
                }
            }

            // This op is eligible to be the view's head.
            assert(header_canonical != null and copies > 0);
        } else op_head_max;
        assert(op_head >= op_head_min);
        assert(op_head <= op_head_max);

        return .{ .complete_valid = HeaderIterator{
            .dvcs = dvcs_canonical_,
            .op_max = op_head,
            .op_min = op_head_min,
        } };
    }

    /// Iterate the consecutive headers of a set of (same-log_view) DVCs, from high-to-low op.
    const HeaderIterator = struct {
        dvcs: DVCArray,
        op_max: u64,
        op_min: u64,
        child_op: ?u64 = null,
        child_parent: ?u128 = null,

        fn next(iterator: *HeaderIterator) ?*const Header.Prepare {
            assert(iterator.dvcs.count() > 0);
            assert(iterator.op_min <= iterator.op_max);
            assert((iterator.child_op == null) == (iterator.child_parent == null));

            if (iterator.child_op != null and iterator.child_op.? == iterator.op_min) return null;

            const op = (iterator.child_op orelse (iterator.op_max + 1)) - 1;

            var header: ?*const Header.Prepare = null;

            const log_view = iterator.dvcs.get(0).header.log_view;
            for (iterator.dvcs.const_slice()) |dvc| {
                assert(log_view == dvc.header.log_view);

                if (op > dvc.header.op) continue;

                const dvc_headers = message_body_as_view_headers(dvc.base_const());
                const dvc_header_index = dvc.header.op - op;
                if (dvc_header_index >= dvc_headers.slice.len) continue;

                const dvc_header = &dvc_headers.slice[dvc_header_index];
                if (vsr.Headers.dvc_header_type(dvc_header) == .valid) {
                    if (header) |h| {
                        assert(h.checksum == dvc_header.checksum);
                    } else {
                        header = dvc_header;
                    }
                }
            }

            if (iterator.child_parent) |parent| {
                assert(header.?.checksum == parent);
            }

            iterator.child_op = op;
            iterator.child_parent = header.?.parent;
            return header.?;
        }
    };
};

fn message_body_as_view_headers(message: *const Message) vsr.Headers.ViewChangeSlice {
    assert(message.header.size > @sizeOf(Header)); // Body must contain at least one header.
    assert(message.header.command == .do_view_change or
        message.header.command == .start_view);

    return vsr.Headers.ViewChangeSlice.init(
        switch (message.header.command) {
            .do_view_change => .do_view_change,
            .start_view => .start_view,
            else => unreachable,
        },
        message_body_as_headers_unchecked(message),
    );
}

/// Asserts that the headers are in descending op order.
/// The headers may contain gaps and/or breaks.
fn message_body_as_prepare_headers(message: *const Message) []const Header.Prepare {
    assert(message.header.size > @sizeOf(Header)); // Body must contain at least one header.
    assert(message.header.command == .start_view or
        message.header.command == .headers);

    const headers = message_body_as_headers_unchecked(message);
    var child: ?*const Header.Prepare = null;
    for (headers) |*header| {
        if (constants.verify) assert(header.valid_checksum());
        assert(header.command == .prepare);
        assert(header.cluster == message.header.cluster);
        assert(header.view <= message.header.view);

        if (child) |child_header| {
            // Headers must be provided in reverse order for the sake of `repair_header()`.
            // Otherwise, headers may never be repaired where the hash chain never connects.
            assert(header.op < child_header.op);
        }
        child = header;
    }

    return headers;
}

fn message_body_as_headers_unchecked(message: *const Message) []const Header.Prepare {
    assert(message.header.size > @sizeOf(Header)); // Body must contain at least one header.
    assert(message.header.command == .do_view_change or
        message.header.command == .start_view or
        message.header.command == .headers);

    return std.mem.bytesAsSlice(
        Header.Prepare,
        message.buffer[@sizeOf(Header)..message.header.size],
    );
}

/// The PipelineQueue belongs to a normal-status primary. It consists of two queues:
/// - A prepare queue, containing all messages currently being prepared.
/// - A request queue, containing all messages which are waiting to begin preparing.
///
/// Invariants:
/// - prepare_queue contains only messages with command=prepare.
/// - prepare_queue's messages have sequential, increasing ops.
/// - prepare_queue's messages are hash-chained.
/// - request_queue contains only messages with command=request.
/// - If request_queue is not empty, then prepare_queue is full OR 1-less than full.
///   (The caller is responsible for maintaining this invariant. If the caller removes an entry
///   from `prepare_queue`, an entry from request_queue should be moved over promptly.)
///
/// Note: The prepare queue may contain multiple prepares from a single client, but the request
/// queue may not (see message_by_client()).
const PipelineQueue = struct {
    const PrepareQueue = RingBuffer(Prepare, .{ .array = constants.pipeline_prepare_queue_max });
    const RequestQueue = RingBuffer(Request, .{ .array = constants.pipeline_request_queue_max });

    /// Messages that are preparing (uncommitted, being written to the WAL (may already be written
    /// to the WAL) and replicated (may just be waiting for acks)).
    prepare_queue: PrepareQueue = PrepareQueue.init(),
    /// Messages that are accepted from the client, but not yet preparing.
    /// When `pipeline_prepare_queue_max + pipeline_request_queue_max = clients_max`, the request
    /// queue guards against clients starving one another.
    request_queue: RequestQueue = RequestQueue.init(),

    fn deinit(pipeline: *PipelineQueue, message_pool: *MessagePool) void {
        while (pipeline.request_queue.pop()) |r| message_pool.unref(r.message);
        while (pipeline.prepare_queue.pop()) |p| message_pool.unref(p.message);
    }

    fn verify(pipeline: PipelineQueue) void {
        assert(pipeline.request_queue.count <= constants.pipeline_request_queue_max);
        assert(pipeline.prepare_queue.count <= constants.pipeline_prepare_queue_max);
        assert(pipeline.request_queue.empty() or
            constants.pipeline_prepare_queue_max == pipeline.prepare_queue.count or
            constants.pipeline_prepare_queue_max == pipeline.prepare_queue.count + 1);

        if (pipeline.prepare_queue.head_ptr_const()) |head| {
            var op = head.message.header.op;
            var parent = head.message.header.parent;
            var prepare_iterator = pipeline.prepare_queue.iterator();
            while (prepare_iterator.next_ptr()) |prepare| {
                assert(prepare.message.header.command == .prepare);
                assert(prepare.message.header.operation != .reserved);
                assert(prepare.message.header.op == op);
                assert(prepare.message.header.parent == parent);

                parent = prepare.message.header.checksum;
                op += 1;
            }
        }

        var request_iterator = pipeline.request_queue.iterator();
        while (request_iterator.next()) |request| {
            assert(request.message.header.command == .request);
        }
    }

    fn full(pipeline: PipelineQueue) bool {
        if (pipeline.prepare_queue.full()) {
            return pipeline.request_queue.full();
        } else {
            assert(pipeline.request_queue.empty() or
                pipeline.prepare_queue.count + 1 == constants.pipeline_prepare_queue_max);
            return false;
        }
    }

    /// Searches the pipeline for a prepare for a given op and checksum.
    /// When `checksum` is `null`, match any checksum.
    fn prepare_by_op_and_checksum(pipeline: *PipelineQueue, op: u64, checksum: u128) ?*Prepare {
        if (pipeline.prepare_queue.empty()) return null;

        // To optimize the search, we can leverage the fact that the pipeline's entries are
        // ordered and consecutive.
        const head_op = pipeline.prepare_queue.head_ptr().?.message.header.op;
        const tail_op = pipeline.prepare_queue.tail_ptr().?.message.header.op;
        assert(tail_op == head_op + pipeline.prepare_queue.count - 1);

        if (op < head_op) return null;
        if (op > tail_op) return null;

        const prepare = pipeline.prepare_queue.get_ptr(op - head_op).?;
        assert(prepare.message.header.op == op);

        if (checksum == prepare.message.header.checksum) return prepare;
        return null;
    }

    /// Searches the pipeline for a prepare matching the given ack.
    /// Asserts that the returned prepare corresponds to the prepare_ok.
    fn prepare_by_prepare_ok(pipeline: *PipelineQueue, ok: *const Message.PrepareOk) ?*Prepare {
        assert(ok.header.command == .prepare_ok);

        const prepare = pipeline.prepare_by_op_and_checksum(
            ok.header.op,
            ok.header.prepare_checksum,
        ) orelse return null;
        assert(prepare.message.header.command == .prepare);
        assert(prepare.message.header.parent == ok.header.parent);
        assert(prepare.message.header.client == ok.header.client);
        assert(prepare.message.header.request == ok.header.request);
        assert(prepare.message.header.cluster == ok.header.cluster);
        assert(prepare.message.header.epoch == ok.header.epoch);
        // A prepare may be committed in the same view or in a newer view:
        assert(prepare.message.header.view <= ok.header.view);
        assert(prepare.message.header.op == ok.header.op);
        assert(prepare.message.header.commit == ok.header.commit);
        assert(prepare.message.header.timestamp == ok.header.timestamp);
        assert(prepare.message.header.operation == ok.header.operation);
        assert(prepare.message.header.checkpoint_id == ok.header.checkpoint_id);

        return prepare;
    }

    /// Search the pipeline (both request & prepare queues) for a message from the given client.
    /// - A client may have multiple prepares in the pipeline if these were committed by the
    ///   previous primary and were reloaded into the pipeline after a view change.
    /// - A client may have at most one request in the pipeline.
    /// If there are multiple messages in the pipeline from the client, the *latest* message is
    /// returned (to help the caller identify bad client behavior).
    fn message_by_client(pipeline: PipelineQueue, client_id: u128) ?*const Message {
        var message: ?*const Message = null;
        var prepare_iterator = pipeline.prepare_queue.iterator();
        while (prepare_iterator.next_ptr()) |prepare| {
            if (prepare.message.header.client == client_id) message = prepare.message.base();
        }

        var request_iterator = pipeline.request_queue.iterator();
        while (request_iterator.next()) |request| {
            if (request.message.header.client == client_id) message = request.message.base();
        }
        return message;
    }

    /// Warning: This temporarily violates the prepare/request queue count invariant.
    /// After invocation, call pop_request→push_prepare to begin preparing the next request.
    fn pop_prepare(pipeline: *PipelineQueue) ?Prepare {
        if (pipeline.prepare_queue.pop()) |prepare| {
            assert(pipeline.request_queue.empty() or
                pipeline.prepare_queue.count + 1 == constants.pipeline_prepare_queue_max);
            return prepare;
        } else {
            assert(pipeline.request_queue.empty());
            return null;
        }
    }

    fn pop_request(pipeline: *PipelineQueue) ?Request {
        return pipeline.request_queue.pop();
    }

    fn push_request(pipeline: *PipelineQueue, request: Request) void {
        assert(request.message.header.command == .request);
        var queue_iterator = pipeline.request_queue.iterator();
        while (queue_iterator.next()) |queue_request| {
            assert(queue_request.message.header.client != request.message.header.client);
        }

        pipeline.request_queue.push_assume_capacity(request);
        if (constants.verify) pipeline.verify();
    }

    fn push_prepare(pipeline: *PipelineQueue, message: *Message.Prepare) void {
        assert(message.header.command == .prepare);
        assert(message.header.operation != .reserved);
        if (pipeline.prepare_queue.tail()) |tail| {
            assert(message.header.op == tail.message.header.op + 1);
            assert(message.header.parent == tail.message.header.checksum);
            assert(message.header.view >= tail.message.header.view);
        } else {
            assert(pipeline.request_queue.empty());
        }

        pipeline.prepare_queue.push_assume_capacity(.{ .message = message.ref() });
        if (constants.verify) pipeline.verify();
    }
};

/// Prepares in the cache may be committed or uncommitted, and may not belong to the current view.
///
/// Invariants:
/// - The cache contains only messages with command=prepare.
/// - If a message with op X is in the cache, it is in `prepares[X % prepares.len]`.
const PipelineCache = struct {
    const prepares_max =
        constants.pipeline_prepare_queue_max +
        constants.pipeline_request_queue_max;

    prepares: [prepares_max]?*Message.Prepare =
        [_]?*Message.Prepare{null} ** prepares_max,

    /// Converting a PipelineQueue to a PipelineCache discards all accumulated acks.
    /// "prepare_ok"s from previous views are not valid, even if the pipeline entry is reused
    /// after a cycle of view changes. In other words, when a view change cycles around, so
    /// that the original primary becomes a primary of a new view, pipeline entries may be
    /// reused. However, the pipeline's prepare_ok quorums must not be reused, since the
    /// replicas that sent them may have swapped them out during a previous view change.
    fn init_from_queue(queue: *PipelineQueue) PipelineCache {
        var cache = PipelineCache{};
        var prepares = queue.prepare_queue.iterator();
        while (prepares.next()) |prepare| {
            const prepare_evicted = cache.insert(prepare.message.ref());
            assert(prepare_evicted == null);
            assert(prepare.message.header.command == .prepare);
        }
        return cache;
    }

    fn deinit(pipeline: *PipelineCache, message_pool: *MessagePool) void {
        for (&pipeline.prepares) |*entry| {
            if (entry.*) |m| {
                message_pool.unref(m);
                entry.* = null;
            }
        }
    }

    fn empty(pipeline: *const PipelineCache) bool {
        for (pipeline.prepares) |*entry| {
            if (entry) |_| return true;
        }
        return false;
    }

    fn contains_header(pipeline: *const PipelineCache, header: *const Header.Prepare) bool {
        assert(header.command == .prepare);
        assert(header.operation != .reserved);

        const slot = header.op % prepares_max;
        const prepare = pipeline.prepares[slot] orelse return false;
        return prepare.header.op == header.op and prepare.header.checksum == header.checksum;
    }

    /// Unlike the PipelineQueue, cached messages may not belong to the current view.
    /// Thus, a matching checksum is required.
    fn prepare_by_op_and_checksum(
        pipeline: *PipelineCache,
        op: u64,
        checksum: u128,
    ) ?*Message.Prepare {
        const slot = op % prepares_max;
        const prepare = pipeline.prepares[slot] orelse return null;
        if (prepare.header.op != op) return null;
        if (prepare.header.checksum != checksum) return null;
        return prepare;
    }

    /// Returns the message evicted from the cache, if any.
    fn insert(pipeline: *PipelineCache, prepare: *Message.Prepare) ?*Message.Prepare {
        assert(prepare.header.command == .prepare);
        assert(prepare.header.operation != .reserved);

        const slot = prepare.header.op % prepares_max;
        const prepare_evicted = pipeline.prepares[slot];
        pipeline.prepares[slot] = prepare;
        return prepare_evicted;
    }
};
