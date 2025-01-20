const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const constants = @import("../../constants.zig");
const vsr = @import("../../vsr.zig");

const message_pool = @import("../../message_pool.zig");
const MessagePool = message_pool.MessagePool;
const Message = MessagePool.Message;

const ReplicaSet = std.StaticBitSet(constants.members_max);
const Commits = std.ArrayList(struct {
    header: vsr.Header.Prepare,
    // null for operation=root and operation=upgrade
    release: ?vsr.Release,
    replicas: ReplicaSet = ReplicaSet.initEmpty(),
});

const ReplicaHead = struct {
    view: u32,
    op: u64,
};

pub fn StateCheckerType(comptime Client: type, comptime Replica: type) type {
    return struct {
        const StateChecker = @This();

        node_count: u8,
        replica_count: u8,

        commits: Commits,
        commit_mins: [constants.members_max]u64 = [_]u64{0} ** constants.members_max,

        replicas: []const Replica,
        clients: []const Client,
        clients_exhaustive: bool = true,

        /// The number of times the canonical state has been advanced.
        requests_committed: u64 = 0,

        /// Tracks the latest op acked by a replica across restarts. Marked as null if the op is
        /// truncated through a view change.
        replica_head_max: []?ReplicaHead,
        pub fn init(allocator: mem.Allocator, options: struct {
            cluster_id: u128,
            replica_count: u8,
            replicas: []const Replica,
            clients: []const Client,
        }) !StateChecker {
            const root_prepare = vsr.Header.Prepare.root(options.cluster_id);

            var commits = Commits.init(allocator);
            errdefer commits.deinit();

            var commit_replicas = ReplicaSet.initEmpty();
            for (options.replicas, 0..) |_, i| commit_replicas.set(i);
            try commits.append(.{
                .header = root_prepare,
                .release = null,
                .replicas = commit_replicas,
            });

            const replica_head_max = try allocator.alloc(?ReplicaHead, options.replicas.len);
            errdefer allocator.free(replica_head_max);
            for (replica_head_max) |*head| head.* = null;

            return StateChecker{
                .node_count = @intCast(options.replicas.len),
                .replica_count = options.replica_count,
                .commits = commits,
                .replicas = options.replicas,
                .clients = options.clients,
                .replica_head_max = replica_head_max,
            };
        }

        pub fn deinit(state_checker: *StateChecker) void {
            const allocator = state_checker.commits.allocator;
            state_checker.commits.deinit();
            allocator.free(state_checker.replica_head_max);
        }

        pub fn on_message(state_checker: *StateChecker, message: *const Message) void {
            if (message.header.into_const(.prepare_ok)) |header| {
                const head_max = &state_checker.replica_head_max[header.replica];
                if (head_max.* == null or header.view > head_max.*.?.view or
                    (header.view == head_max.*.?.view and header.op > head_max.*.?.op))
                {
                    head_max.* = .{ .view = header.view, .op = header.op };
                }
            }
        }

        pub fn on_head_op_updated(
            state_checker: *StateChecker,
            new_head: *const vsr.Header.Prepare,
            replica_index: u8,
        ) void {
            const head_max = &state_checker.replica_head_max[replica_index];

            // If the latest op acked by this replica was truncated through a view change,
            // replica_head_max is uncertain till the next prepare_ok is sent. To avoid peeking into
            // replica states, we conservatively mark replica_head_max as null when the new head is
            // from a future view (even though it *may* not have been truncated).
            if (head_max.* != null and
                (new_head.op < head_max.*.?.op or new_head.view > head_max.*.?.view))
            {
                head_max.* = null;
            }
        }

        /// Returns whether the replica's state changed since the last check_state().
        pub fn check_state(state_checker: *StateChecker, replica_index: u8) !void {
            const replica = &state_checker.replicas[replica_index];
            if (replica.syncing == .updating_checkpoint) {
                // Allow a syncing replica to fast-forward its commit.
                //
                // But "fast-forwarding" may actually move commit_min slightly backwards:
                // 1. Suppose op X is a checkpoint trigger.
                // 2. We are committing op X-1 but are stuck due to a block that does not exist in
                //    the cluster anymore.
                // 3. When we sync, `commit_min` "backtracks", to `X - lsm_compaction_ops`.
                const commit_min_source = state_checker.commit_mins[replica_index];
                const commit_min_target =
                    replica.syncing.updating_checkpoint.header.op;
                assert(commit_min_source <= commit_min_target + constants.lsm_compaction_ops);
                state_checker.commit_mins[replica_index] = commit_min_target;
                return;
            }

            const head_max = &state_checker.replica_head_max[replica_index];
            if (replica.status != .recovering_head and head_max.* != null) {
                assert(replica.view > head_max.*.?.view or
                    (replica.view == head_max.*.?.view and replica.op >= head_max.*.?.op));
            }

            const commit_root_op = replica.superblock.working.vsr_state.checkpoint.header.op;
            const commit_root = replica.superblock.working.vsr_state.checkpoint.header.checksum;

            const commit_a = state_checker.commit_mins[replica_index];
            const commit_b = replica.commit_min;

            const header_b = replica.journal.header_with_op(replica.commit_min);
            assert(header_b != null or replica.commit_min == replica.op_checkpoint());
            assert(header_b == null or header_b.?.op == commit_b);

            const checksum_a = state_checker.commits.items[commit_a].header.checksum;
            // Even if we have header_b, if its op is commit_root_op, we can't trust it.
            // If we just finished state sync, the header in our log might not have been
            // committed (it might be left over from before sync).
            const checksum_b = if (commit_b == commit_root_op) commit_root else header_b.?.checksum;

            assert(checksum_b != commit_root or
                replica.commit_min == replica.superblock.working.vsr_state.checkpoint.header.op);
            assert((commit_a == commit_b) == (checksum_a == checksum_b));

            if (checksum_a == checksum_b) return;

            assert(commit_b < commit_a or commit_a + 1 == commit_b);
            state_checker.commit_mins[replica_index] = commit_b;

            // If some other replica has already reached this state, then it will be in the commit
            // history:
            if (replica.commit_min < state_checker.commits.items.len) {
                const commit = &state_checker.commits.items[commit_b];
                if (replica.op_checkpoint() < replica.commit_min) {
                    if (commit.release) |release| assert(release.value == replica.release.value);
                } else {
                    // When op_checkpoint==commit_min, we recovered from checkpoint, so it is ok if
                    // the release doesn't match. (commit_min is not actually being executed.)
                    assert(replica.op_checkpoint() == replica.commit_min);
                }

                commit.replicas.set(replica_index);

                assert(replica.commit_min < state_checker.commits.items.len);
                // A replica may transition more than once to the same state, for example, when
                // restarting after a crash and replaying the log. The more important invariant is
                // that the cluster as a whole may not transition to the same state more than once,
                // and once transitioned may not regress.
                return;
            }

            if (header_b == null) return;
            assert(header_b.?.checksum == checksum_b);
            assert(header_b.?.parent == checksum_a);
            assert(header_b.?.op > 0);
            assert(header_b.?.command == .prepare);
            assert(header_b.?.operation != .reserved);

            if (header_b.?.client == 0) {
                assert(header_b.?.operation == .upgrade or
                    header_b.?.operation == .pulse);
            } else {
                if (state_checker.clients_exhaustive) {
                    // The replica has transitioned to state `b` that is not yet in the commit
                    // history. Check if this is a valid new state based on the originating client's
                    // inflight request.
                    const client = for (state_checker.clients) |*client| {
                        if (client.id == header_b.?.client) break client;
                    } else unreachable;

                    if (client.request_inflight == null) {
                        return error.ReplicaTransitionedToInvalidState;
                    }

                    const request = client.request_inflight.?.message;
                    assert(request.header.client == header_b.?.client);
                    assert(request.header.checksum == header_b.?.request_checksum);
                    assert(request.header.request == header_b.?.request);
                    assert(request.header.command == .request);
                    assert(request.header.operation == header_b.?.operation);
                    assert(request.header.size == header_b.?.size);
                    // `checksum_body` will not match; the leader's StateMachine updated the
                    // timestamps in the prepare body's accounts/transfers.
                } else {
                    // Either:
                    // - The cluster is running with one or more raw MessageBus "clients", so there
                    //   may be requests not found in `Cluster.clients`.
                    // - The test includes one or more client evictions.
                }
            }

            state_checker.requests_committed += 1;
            assert(state_checker.requests_committed == header_b.?.op);

            const release = release: {
                if (header_b.?.operation == .root or
                    header_b.?.operation == .upgrade)
                {
                    break :release null;
                } else {
                    break :release replica.release;
                }
            };

            assert(state_checker.commits.items.len == header_b.?.op);
            state_checker.commits.append(.{
                .header = header_b.?.*,
                .release = release,
            }) catch unreachable;
            state_checker.commits.items[header_b.?.op].replicas.set(replica_index);
        }

        pub fn replica_convergence(state_checker: *StateChecker, replica_index: u8) bool {
            const a = state_checker.commits.items.len - 1;
            const b = state_checker.commit_mins[replica_index];
            return a == b;
        }

        pub fn assert_cluster_convergence(state_checker: *StateChecker) void {
            for (state_checker.commits.items, 0..) |commit, i| {
                assert(commit.replicas.count() > 0);
                assert(commit.header.command == .prepare);
                assert(commit.header.op == i);
                if (i > 0) {
                    const previous = state_checker.commits.items[i - 1].header;
                    assert(commit.header.parent == previous.checksum);
                    assert(commit.header.view >= previous.view);
                }
            }
        }

        pub fn header_with_op(state_checker: *StateChecker, op: u64) vsr.Header.Prepare {
            const commit = &state_checker.commits.items[op];
            assert(commit.header.op == op);
            assert(commit.replicas.count() > 0);
            return commit.header;
        }
    };
}
