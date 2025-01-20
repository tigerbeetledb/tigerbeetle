const std = @import("std");
const assert = std.debug.assert;
const maybe = stdx.maybe;
const log = std.log.scoped(.test_replica);
const expectEqual = std.testing.expectEqual;
const expect = std.testing.expect;
const allocator = std.testing.allocator;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");
const Process = @import("../testing/cluster/message_bus.zig").Process;
const Message = @import("../message_pool.zig").MessagePool.Message;
const marks = @import("../testing/marks.zig");
const StateMachineType = @import("../testing/state_machine.zig").StateMachineType;
const Cluster = @import("../testing/cluster.zig").ClusterType(StateMachineType);
const LinkFilter = @import("../testing/cluster/network.zig").LinkFilter;
const Network = @import("../testing/cluster/network.zig").Network;

const slot_count = constants.journal_slot_count;
const checkpoint_1 = vsr.Checkpoint.checkpoint_after(0);
const checkpoint_2 = vsr.Checkpoint.checkpoint_after(checkpoint_1);
const checkpoint_3 = vsr.Checkpoint.checkpoint_after(checkpoint_2);
const checkpoint_1_trigger = vsr.Checkpoint.trigger_for_checkpoint(checkpoint_1).?;
const checkpoint_2_trigger = vsr.Checkpoint.trigger_for_checkpoint(checkpoint_2).?;
const checkpoint_3_trigger = vsr.Checkpoint.trigger_for_checkpoint(checkpoint_3).?;
const checkpoint_1_prepare_max = vsr.Checkpoint.prepare_max_for_checkpoint(checkpoint_1).?;
const checkpoint_2_prepare_max = vsr.Checkpoint.prepare_max_for_checkpoint(checkpoint_2).?;
// No test is using this yet:
// const checkpoint_3_prepare_max = vsr.Checkpoint.prepare_max_for_checkpoint(checkpoint_3).?;
const checkpoint_1_prepare_ok_max = checkpoint_1_trigger + constants.pipeline_prepare_queue_max;
const checkpoint_2_prepare_ok_max = checkpoint_2_trigger + constants.pipeline_prepare_queue_max;

const log_level = std.log.Level.err;

const releases = .{
    .{
        .release = vsr.Release.from(.{ .major = 0, .minor = 0, .patch = 10 }),
        .release_client_min = vsr.Release.from(.{ .major = 0, .minor = 0, .patch = 10 }),
    },
    .{
        .release = vsr.Release.from(.{ .major = 0, .minor = 0, .patch = 20 }),
        .release_client_min = vsr.Release.from(.{ .major = 0, .minor = 0, .patch = 10 }),
    },
    .{
        .release = vsr.Release.from(.{ .major = 0, .minor = 0, .patch = 30 }),
        .release_client_min = vsr.Release.from(.{ .major = 0, .minor = 0, .patch = 10 }),
    },
};

// TODO Detect when cluster has stabilized and stop run() early, rather than just running for a
//      fixed number of ticks.

comptime {
    // The tests are written for these configuration values in particular.
    assert(constants.journal_slot_count == 32);
    assert(constants.lsm_compaction_ops == 4);
}

test "Cluster: smoke" {
    const t = try TestContext.init(.{ .replica_count = 1 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(checkpoint_2_trigger, checkpoint_2_trigger);
    try expectEqual(t.replica(.R_).commit(), checkpoint_2_trigger);
}

test "Cluster: recovery: WAL prepare corruption (R=3, corrupt right of head)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    t.replica(.R_).stop();
    t.replica(.R0).corrupt(.{ .wal_prepare = 2 });

    // 2/3 can't commit when 1/2 is status=recovering_head.
    try t.replica(.R0).open();
    try expectEqual(t.replica(.R0).status(), .recovering_head);
    try t.replica(.R1).open();
    try c.request(4, 0);
    // With the aid of the last replica, the cluster can recover.
    try t.replica(.R2).open();
    try c.request(4, 4);
    try expectEqual(t.replica(.R_).commit(), 4);
}

test "Cluster: recovery: WAL prepare corruption (R=3, corrupt left of head, 3/3 corrupt)" {
    // The replicas recognize that the corrupt entry is outside of the pipeline and
    // must be committed.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(2, 2);
    t.replica(.R_).stop();
    t.replica(.R_).corrupt(.{ .wal_prepare = 1 });
    try t.replica(.R_).open();
    t.run();

    // The same prepare is lost by all WALs, so the cluster can never recover.
    // Each replica stalls trying to repair the header break.
    try expectEqual(t.replica(.R_).status(), .view_change);
    try expectEqual(t.replica(.R_).commit(), 0);
}

test "Cluster: recovery: WAL prepare corruption (R=3, corrupt root)" {
    // A replica can recover from a corrupt root prepare.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    t.replica(.R0).stop();
    t.replica(.R0).corrupt(.{ .wal_prepare = 0 });
    try t.replica(.R0).open();

    try c.request(1, 1);
    try expectEqual(t.replica(.R_).commit(), 1);
}

test "Cluster: recovery: WAL prepare corruption (R=3, corrupt checkpoint…head)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    // Trigger the first checkpoint.
    try c.request(checkpoint_1_trigger, checkpoint_1_trigger);
    t.replica(.R0).stop();

    // Corrupt op_checkpoint (27) and all ops that follow.
    var slot: usize = slot_count - constants.lsm_compaction_ops - 1;
    while (slot < slot_count) : (slot += 1) {
        t.replica(.R0).corrupt(.{ .wal_prepare = slot });
    }
    try t.replica(.R0).open();
    try expectEqual(t.replica(.R0).status(), .recovering_head);

    try c.request(slot_count, slot_count);
    try expectEqual(t.replica(.R0).status(), .normal);
    t.replica(.R1).stop();
    try c.request(slot_count + 1, slot_count + 1);
}

test "Cluster: recovery: WAL prepare corruption (R=1, corrupt between checkpoint and head)" {
    // R=1 can never recover if a WAL-prepare is corrupt.
    const t = try TestContext.init(.{ .replica_count = 1 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(2, 2);
    t.replica(.R0).stop();
    t.replica(.R0).corrupt(.{ .wal_prepare = 1 });
    if (t.replica(.R0).open()) {
        unreachable;
    } else |err| switch (err) {
        error.WALCorrupt => {},
        else => unreachable,
    }
}

test "Cluster: recovery: WAL header corruption (R=1)" {
    // R=1 locally repairs WAL-header corruption.
    const t = try TestContext.init(.{ .replica_count = 1 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(2, 2);
    t.replica(.R0).stop();
    t.replica(.R0).corrupt(.{ .wal_header = 1 });
    try t.replica(.R0).open();
    try c.request(3, 3);
}

test "Cluster: recovery: WAL torn prepare, standby with intact prepare (R=1 S=1)" {
    // R=1 recovers to find that its last prepare was a torn write, so it is truncated.
    // The standby received the prepare, though.
    //
    // R=1 handles this by incrementing its view during recovery, so that the standby can truncate
    // discard the truncated prepare.
    const t = try TestContext.init(.{
        .replica_count = 1,
        .standby_count = 1,
    });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(2, 2);
    t.replica(.R0).stop();
    t.replica(.R0).corrupt(.{ .wal_header = 2 });
    try t.replica(.R0).open();
    try c.request(3, 3);
    try expectEqual(t.replica(.R0).commit(), 3);
    try expectEqual(t.replica(.S0).commit(), 3);
}

test "Cluster: recovery: grid corruption (disjoint)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);

    // Checkpoint to ensure that the replicas will actually use the grid to recover.
    // All replicas must be at the same commit to ensure grid repair won't fail and
    // fall back to state sync.
    try c.request(checkpoint_1_trigger, checkpoint_1_trigger);
    try expectEqual(t.replica(.R_).op_checkpoint(), checkpoint_1);
    try expectEqual(t.replica(.R_).commit(), checkpoint_1_trigger);

    t.replica(.R_).stop();

    // Corrupt the whole grid.
    // Manifest blocks will be repaired as each replica opens its forest.
    // Table index/filter/data blocks will be repaired as the replica commits/compacts.
    for ([_]TestReplicas{
        t.replica(.R0),
        t.replica(.R1),
        t.replica(.R2),
    }, 0..) |replica, i| {
        const address_max = t.block_address_max();
        var address: u64 = 1 + i; // Addresses start at 1.
        while (address <= address_max) : (address += 3) {
            // Leave every third address un-corrupt.
            // Each block exists intact on exactly one replica.
            replica.corrupt(.{ .grid_block = address + 1 });
            replica.corrupt(.{ .grid_block = address + 2 });
        }
    }

    try t.replica(.R_).open();
    t.run();

    try expectEqual(t.replica(.R_).status(), .normal);
    try expectEqual(t.replica(.R_).commit(), checkpoint_1_trigger);
    try expectEqual(t.replica(.R_).op_checkpoint(), checkpoint_1);

    try c.request(checkpoint_2_trigger, checkpoint_2_trigger);
    try expectEqual(t.replica(.R_).op_checkpoint(), checkpoint_2);
    try expectEqual(t.replica(.R_).commit(), checkpoint_2_trigger);
}

test "Cluster: recovery: recovering_head, outdated start view" {
    // 1. Wait for B1 to ok op=3.
    // 2. Restart B1 while corrupting op=3, so that it gets into a .recovering_head with op=2.
    // 3. Try make B1 forget about op=3 by delivering it an outdated .start_view with op=2.
    const t = try TestContext.init(.{
        .replica_count = 3,
    });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    var a = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    try c.request(2, 2);

    b1.stop();
    b1.corrupt(.{ .wal_prepare = 2 });

    try b1.open();
    try expectEqual(b1.status(), .recovering_head);
    try expectEqual(b1.op_head(), 1);

    b1.record(.A0, .incoming, .start_view);
    t.run();
    try expectEqual(b1.status(), .normal);
    try expectEqual(b1.op_head(), 2);

    b2.drop_all(.R_, .bidirectional);

    try c.request(3, 3);

    b1.stop();
    b1.corrupt(.{ .wal_prepare = 3 });

    try b1.open();
    try expectEqual(b1.status(), .recovering_head);
    try expectEqual(b1.op_head(), 2);

    const mark = marks.check("ignoring (recovering_head, nonce mismatch)");
    a.stop();
    b1.replay_recorded();
    t.run();

    try expectEqual(b1.status(), .recovering_head);
    try expectEqual(b1.op_head(), 2);

    // Should B1 erroneously accept op=2 as head, unpartitioning B2 here would lead to a data loss.
    b2.pass_all(.R_, .bidirectional);
    t.run();
    try a.open();
    try c.request(4, 4);
    try mark.expect_hit();
}

test "Cluster: recovery: recovering head: idle cluster" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    var b = t.replica(.B1);

    try c.request(2, 2);

    b.stop();
    b.corrupt(.{ .wal_prepare = 3 });
    b.corrupt(.{ .wal_header = 3 });

    try b.open();
    try expectEqual(b.status(), .recovering_head);
    try expectEqual(b.op_head(), 2);

    t.run();

    try expectEqual(b.status(), .normal);
    try expectEqual(b.op_head(), 2);
}

test "Cluster: network: partition 2-1 (isolate backup, symmetric)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(2, 2);
    t.replica(.B2).drop_all(.__, .bidirectional);
    try c.request(3, 3);
    try expectEqual(t.replica(.A0).commit(), 3);
    try expectEqual(t.replica(.B1).commit(), 3);
    try expectEqual(t.replica(.B2).commit(), 2);
}

test "Cluster: network: partition 2-1 (isolate backup, asymmetric, send-only)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(2, 2);
    t.replica(.B2).drop_all(.__, .incoming);
    try c.request(3, 3);
    try expectEqual(t.replica(.A0).commit(), 3);
    try expectEqual(t.replica(.B1).commit(), 3);
    try expectEqual(t.replica(.B2).commit(), 2);
}

test "Cluster: network: partition 2-1 (isolate backup, asymmetric, receive-only)" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(2, 2);
    t.replica(.B2).drop_all(.__, .outgoing);
    try c.request(3, 3);
    try expectEqual(t.replica(.A0).commit(), 3);
    try expectEqual(t.replica(.B1).commit(), 3);
    // B2 may commit some ops, but at some point is will likely fall behind.
    // Prepares may be reordered by the network, and if B1 receives X+1 then X,
    // it will not forward X on, as it is a "repair".
    // And B2 is partitioned, so it cannot repair its hash chain.
    try expect(t.replica(.B2).commit() >= 2);
}

test "Cluster: network: partition 1-2 (isolate primary, symmetric)" {
    // The primary cannot communicate with either backup, but the backups can communicate with one
    // another. The backups will perform a view-change since they don't receive heartbeats.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(2, 2);

    const p = t.replica(.A0);
    p.drop_all(.B1, .bidirectional);
    p.drop_all(.B2, .bidirectional);
    try c.request(3, 3);
    try expectEqual(p.commit(), 2);
}

test "Cluster: network: partition 1-2 (isolate primary, asymmetric, send-only)" {
    // The primary can send to the backups, but not receive.
    // After a short interval of not receiving messages (specifically prepare_ok's) it will abdicate
    // by pausing heartbeats, allowing the next replica to take over as primary.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(1, 1);
    t.replica(.A0).drop_all(.B1, .incoming);
    t.replica(.A0).drop_all(.B2, .incoming);
    const mark = marks.check("send_commit: primary abdicating");
    try c.request(2, 2);
    try mark.expect_hit();
}

test "Cluster: network: partition 1-2 (isolate primary, asymmetric, receive-only)" {
    // The primary can receive from the backups, but not send to them.
    // The backups will perform a view-change since they don't receive heartbeats.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(1, 1);
    t.replica(.A0).drop_all(.B1, .outgoing);
    t.replica(.A0).drop_all(.B2, .outgoing);
    try c.request(2, 2);
}

test "Cluster: network: partition client-primary (symmetric)" {
    // Clients cannot communicate with the primary, but they still request/reply via a backup.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);

    t.replica(.A0).drop_all(.C_, .bidirectional);
    // TODO: https://github.com/tigerbeetle/tigerbeetle/issues/444
    // try c.request(1, 1);
    try c.request(1, 0);
}

test "Cluster: network: partition client-primary (asymmetric, drop requests)" {
    // Primary cannot receive messages from the clients.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);

    t.replica(.A0).drop_all(.C_, .incoming);
    // TODO: https://github.com/tigerbeetle/tigerbeetle/issues/444
    // try c.request(1, 1);
    try c.request(1, 0);
}

test "Cluster: network: partition client-primary (asymmetric, drop replies)" {
    // Clients cannot receive replies from the primary, but they receive replies from a backup.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);

    t.replica(.A0).drop_all(.C_, .outgoing);
    // TODO: https://github.com/tigerbeetle/tigerbeetle/issues/444
    // try c.request(1, 1);
    try c.request(1, 0);
}

test "Cluster: network: partition flexible quorum" {
    // Two out of four replicas should be able to carry on as long the pair includes the primary.
    const t = try TestContext.init(.{ .replica_count = 4 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);

    t.run();
    t.replica(.B2).stop();
    t.replica(.B3).stop();
    for (0..3) |_| t.run(); // Give enough time for the clocks to desync.

    try c.request(4, 4);
}

test "Cluster: network: primary no clock sync" {
    // When primary can't accept requests because the clock is not synchronized, it must proactively
    // abdicate (the rest of the cluster doesn't know that there are dropped requests).
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(3, 3);
    const a0 = t.replica(.A0);
    try expectEqual(a0.role(), .primary);
    try expectEqual(a0.commit(), 3);

    a0.drop(.R_, .incoming, .pong);
    for (0..3) |_| t.run(); // Give enough time for the clocks to desync.

    try expectEqual(a0.role(), .primary);
    const mark = marks.check("send_commit: primary abdicating");
    try c.request(5, 5);
    try mark.expect_hit();
    try expectEqual(a0.role(), .backup);
    try expectEqual(t.replica(.R_).commit(), 5);
}

test "Cluster: repair: partition 2-1, then backup fast-forward 1 checkpoint" {
    // A backup that has fallen behind by two checkpoints can catch up, without using state sync.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(3, 3);
    try expectEqual(t.replica(.R_).commit(), 3);

    var r_lag = t.replica(.B2);
    r_lag.stop();

    // Commit enough ops to checkpoint once, and then nearly wrap around, leaving enough slack
    // that the lagging backup can repair (without state sync).
    const commit = 3 + slot_count - constants.pipeline_prepare_queue_max;
    try c.request(commit, commit);
    try expectEqual(t.replica(.A0).op_checkpoint(), checkpoint_1);
    try expectEqual(t.replica(.B1).op_checkpoint(), checkpoint_1);

    try r_lag.open();
    try expectEqual(r_lag.status(), .normal);
    try expectEqual(r_lag.op_checkpoint(), 0);

    // Allow repair, but check that state sync doesn't run.
    const mark = marks.check("sync started");
    t.run();
    try mark.expect_not_hit();

    try expectEqual(t.replica(.R_).status(), .normal);
    try expectEqual(t.replica(.R_).op_checkpoint(), checkpoint_1);
    try expectEqual(t.replica(.R_).commit(), commit);
}

test "Cluster: repair: view-change, new-primary lagging behind checkpoint, forfeit" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(2, 2);
    try expectEqual(t.replica(.R_).commit(), 2);

    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    b1.drop_all(.__, .bidirectional);

    try c.request(checkpoint_1_prepare_max + 1, checkpoint_1_prepare_max + 1);
    try expectEqual(a0.op_checkpoint(), checkpoint_1);
    try expectEqual(b1.op_checkpoint(), 0);
    try expectEqual(b2.op_checkpoint(), checkpoint_1);
    try expectEqual(a0.commit(), checkpoint_1_prepare_max + 1);
    try expectEqual(b1.commit(), 2);
    try expectEqual(b2.commit(), checkpoint_1_prepare_max + 1);
    try expectEqual(a0.op_head(), checkpoint_1_prepare_max + 1);
    try expectEqual(b1.op_head(), 2);
    try expectEqual(b2.op_head(), checkpoint_1_prepare_max + 1);

    // Partition the primary, but restore B1. B1 will attempt to become the primary next,
    // but it is too far behind, so B2 becomes the new primary instead.
    b2.pass_all(.__, .bidirectional);
    b1.pass_all(.__, .bidirectional);
    a0.drop_all(.__, .bidirectional);
    // TODO: make sure that B1 uses WAL repair rather than state sync here.
    const mark = marks.check("on_do_view_change: lagging primary; forfeiting");
    t.run();
    try mark.expect_hit();

    try expectEqual(b2.role(), .primary);
    try expectEqual(b2.index(), t.replica(.A0).index());
    try expectEqual(b2.view(), b1.view());
    try expectEqual(b2.log_view(), b1.log_view());

    // Thanks to the new primary, the lagging backup is able to catch up to the latest
    // checkpoint/commit.
    try expectEqual(b1.role(), .backup);
    try expectEqual(b1.commit(), checkpoint_1_prepare_max + 1);
    try expectEqual(b1.op_checkpoint(), checkpoint_1);

    try expectEqual(t.replica(.R_).commit(), checkpoint_1_prepare_max + 1);
}

test "Cluster: repair: crash, corrupt committed pipeline op, repair it, view-change; dont nack" {
    // This scenario is also applicable when any op within the pipeline suffix is corrupted.
    // But we test by corrupting the last op to take advantage of recovering_head to learn the last
    // op's header without its prepare.
    //
    // Also, a corrupt last op maximizes uncertainty — there are no higher ops which
    // can definitively show that the last op is committed (via `header.commit`).
    const t = try TestContext.init(.{
        .replica_count = 3,
        .client_count = constants.pipeline_prepare_queue_max,
    });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(2, 2);

    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    b2.drop_all(.R_, .bidirectional);

    try c.request(4, 4);

    b1.stop();
    b1.corrupt(.{ .wal_prepare = 4 });

    // We can't learn op=4's prepare, only its header (via start_view).
    b1.drop(.R_, .bidirectional, .prepare);
    try b1.open();
    try expectEqual(b1.status(), .recovering_head);
    t.run();

    b1.pass_all(.R_, .bidirectional);
    b2.pass_all(.R_, .bidirectional);
    a0.stop();
    a0.drop_all(.R_, .outgoing);
    t.run();

    // The cluster is stuck trying to repair op=4 (requesting the prepare).
    // B2 can nack op=4, but B1 *must not*.
    try expectEqual(b1.status(), .view_change);
    try expectEqual(b1.commit(), 3);
    try expectEqual(b1.op_head(), 4);

    // A0 provides prepare=4.
    a0.pass_all(.R_, .outgoing);
    try a0.open();
    t.run();
    try expectEqual(t.replica(.R_).status(), .normal);
    try expectEqual(t.replica(.R_).commit(), 4);
    try expectEqual(t.replica(.R_).op_head(), 4);
}

test "Cluster: repair: corrupt reply" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(2, 2);
    try expectEqual(t.replica(.R_).commit(), 2);

    // Prevent any view changes, to ensure A0 repairs its corrupt prepare.
    t.replica(.R_).drop(.R_, .bidirectional, .do_view_change);

    // Block the client from seeing the reply from the cluster.
    t.replica(.R_).drop(.C_, .outgoing, .reply);
    try c.request(3, 2);

    // Corrupt all of the primary's saved replies.
    // (This is easier than figuring out the reply's actual slot.)
    var slot: usize = 0;
    while (slot < constants.clients_max) : (slot += 1) {
        t.replica(.A0).corrupt(.{ .client_reply = slot });
    }

    // The client will keep retrying request 3 until it receives a reply.
    // The primary requests the reply from one of its backups.
    // (Pass A0 only to ensure that no other client forwards the reply.)
    t.replica(.A0).pass(.C_, .outgoing, .reply);
    t.run();

    try expectEqual(c.replies(), 3);
}

test "Cluster: repair: ack committed prepare" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(2, 2);
    try expectEqual(t.replica(.R_).commit(), 2);

    const p = t.replica(.A0);
    const b1 = t.replica(.B1);
    const b2 = t.replica(.B2);

    // A0 commits 3.
    // B1 prepares 3, but does not commit.
    t.replica(.R_).drop(.R_, .bidirectional, .start_view_change);
    t.replica(.R_).drop(.R_, .bidirectional, .do_view_change);
    p.drop(.__, .outgoing, .commit);
    b2.drop(.__, .incoming, .prepare);
    try c.request(3, 3);
    try expectEqual(p.commit(), 3);
    try expectEqual(b1.commit(), 2);
    try expectEqual(b2.commit(), 2);

    try expectEqual(p.op_head(), 3);
    try expectEqual(b1.op_head(), 3);
    try expectEqual(b2.op_head(), 2);

    try expectEqual(p.status(), .normal);
    try expectEqual(b1.status(), .normal);
    try expectEqual(b2.status(), .normal);

    // Change views. B1/B2 participate. Don't allow B2 to repair op=3.
    try expectEqual(p.role(), .primary);
    t.replica(.R_).pass(.R_, .bidirectional, .start_view_change);
    t.replica(.R_).pass(.R_, .bidirectional, .do_view_change);
    p.drop(.__, .bidirectional, .prepare);
    p.drop(.__, .bidirectional, .do_view_change);
    p.drop(.__, .bidirectional, .start_view_change);
    t.run();
    try expectEqual(b1.commit(), 2);
    try expectEqual(b2.commit(), 2);
    try expectEqual(p.role(), .backup);

    try expectEqual(p.status(), .normal);
    try expectEqual(b1.status(), .normal);
    try expectEqual(b2.status(), .normal);

    // But other than that, heal A0/B1, but partition B2 completely.
    // (Prevent another view change.)
    p.pass_all(.__, .bidirectional);
    b1.pass_all(.__, .bidirectional);
    b2.drop_all(.__, .bidirectional);
    t.replica(.R_).drop(.R_, .bidirectional, .start_view_change);
    t.replica(.R_).drop(.R_, .bidirectional, .do_view_change);
    t.run();

    try expectEqual(p.status(), .normal);
    try expectEqual(b1.status(), .normal);
    try expectEqual(b2.status(), .normal);

    // A0 acks op=3 even though it already committed it.
    try expectEqual(p.commit(), 3);
    try expectEqual(b1.commit(), 3);
    try expectEqual(b2.commit(), 2);
}

test "Cluster: repair: primary checkpoint, backup crash before checkpoint, primary prepare" {
    // 1. Given 3 replica: A0, B1, B2.
    // 2. B2 is partitioned (for the entire scenario).
    // 3. A0 and B1 prepare and commit many messages...
    // 4. A0 commits a checkpoint trigger and checkpoints.
    // 5. B1 crashes before it can commit the trigger or checkpoint.
    // 6. A0 prepares a message.
    // 7. B1 restarts. The very first entry in its WAL is corrupt.
    // A0 has *not* already overwritten the corresponding entry in its own WAL, thanks to the
    // pipeline component of the vsr_checkpoint_ops.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    var p = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    // B2 does not participate in this scenario.
    b2.stop();
    try c.request(checkpoint_1_trigger - 1, checkpoint_1_trigger - 1);

    b1.drop(.R_, .incoming, .commit);
    try c.request(checkpoint_1_trigger, checkpoint_1_trigger);
    try expectEqual(p.op_checkpoint(), checkpoint_1);
    try expectEqual(b1.op_checkpoint(), 0);
    try expectEqual(p.commit(), checkpoint_1_trigger);
    try expectEqual(b1.commit(), checkpoint_1_trigger - 1);

    b1.pass(.R_, .incoming, .commit);
    b1.stop();
    b1.corrupt(.{ .wal_prepare = 1 });
    try c.request(
        checkpoint_1_trigger + constants.pipeline_prepare_queue_max,
        checkpoint_1_trigger,
    );
    try b1.open();
    t.run();

    try expectEqual(p.op_checkpoint(), checkpoint_1);
    try expectEqual(b1.op_checkpoint(), checkpoint_1);
    try expectEqual(p.commit(), checkpoint_1_trigger + constants.pipeline_prepare_queue_max);
    try expectEqual(b1.commit(), checkpoint_1_trigger + constants.pipeline_prepare_queue_max);
}

test "Cluster: view-change: DVC, 1+1/2 faulty header stall, 2+1/3 faulty header succeed" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(2, 2);
    try expectEqual(t.replica(.R_).commit(), 2);

    t.replica(.R0).stop();
    try c.request(4, 4);
    t.replica(.R1).stop();
    t.replica(.R2).stop();

    t.replica(.R1).corrupt(.{ .wal_prepare = 3 });

    // The nack quorum size is 2.
    // The new view must determine whether op=3 is possibly committed.
    // - R0 never received op=3 (it had already crashed), so it nacks.
    // - R1 did receive op=3, but upon recovering its WAL, it was corrupt, so it cannot nack.
    // The cluster must wait form R2 before recovering.
    try t.replica(.R0).open();
    try t.replica(.R1).open();
    const mark = marks.check("quorum received, awaiting repair");
    t.run();
    try expectEqual(t.replica(.R0).status(), .view_change);
    try expectEqual(t.replica(.R1).status(), .view_change);
    try mark.expect_hit();

    // R2 provides the missing header, allowing the view-change to succeed.
    try t.replica(.R2).open();
    t.run();
    try expectEqual(t.replica(.R_).status(), .normal);
    try expectEqual(t.replica(.R_).commit(), 4);
}

test "Cluster: view-change: DVC, 2/3 faulty header stall" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);

    t.replica(.R0).stop();
    try c.request(3, 3);
    t.replica(.R1).stop();
    t.replica(.R2).stop();

    t.replica(.R1).corrupt(.{ .wal_prepare = 2 });
    t.replica(.R2).corrupt(.{ .wal_prepare = 2 });

    try t.replica(.R_).open();
    const mark = marks.check("quorum received, deadlocked");
    t.run();
    try expectEqual(t.replica(.R_).status(), .view_change);
    try mark.expect_hit();
}

test "Cluster: view-change: duel of the primaries" {
    // In a cluster of 3, one replica gets partitioned away, and the remaining two _both_ become
    // primaries (for different views). Additionally, the primary from the  higher view is
    // abdicating. The primaries should figure out that they need to view-change to a higher view.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(2, 2);
    try expectEqual(t.replica(.R_).commit(), 2);

    try expectEqual(t.replica(.R_).view(), 1);
    try expectEqual(t.replica(.R1).role(), .primary);

    t.replica(.R2).drop_all(.R_, .bidirectional);
    t.replica(.R1).drop(.R_, .outgoing, .commit);
    try c.request(3, 3);

    try expectEqual(t.replica(.R0).commit_max(), 2);
    try expectEqual(t.replica(.R1).commit_max(), 3);
    try expectEqual(t.replica(.R2).commit_max(), 2);

    t.replica(.R0).pass_all(.R_, .bidirectional);
    t.replica(.R2).pass_all(.R_, .bidirectional);
    t.replica(.R1).drop_all(.R_, .bidirectional);
    t.replica(.R2).drop(.R0, .bidirectional, .prepare_ok);
    t.replica(.R2).drop(.R0, .outgoing, .do_view_change);
    t.run();

    // The stage is set: we have two primaries in different views, R2 is about to abdicate.
    try expectEqual(t.replica(.R1).view(), 1);
    try expectEqual(t.replica(.R1).status(), .normal);
    try expectEqual(t.replica(.R1).role(), .primary);
    try expectEqual(t.replica(.R1).commit(), 3);
    try expectEqual(t.replica(.R2).op_head(), 3);

    try expectEqual(t.replica(.R2).view(), 2);
    try expectEqual(t.replica(.R2).status(), .normal);
    try expectEqual(t.replica(.R2).role(), .primary);
    try expectEqual(t.replica(.R2).commit(), 2);
    try expectEqual(t.replica(.R2).op_head(), 3);

    t.replica(.R1).pass_all(.R_, .bidirectional);
    t.replica(.R2).pass_all(.R_, .bidirectional);
    t.replica(.R0).drop_all(.R_, .bidirectional);
    t.run();

    try expectEqual(t.replica(.R1).commit(), 3);
    try expectEqual(t.replica(.R2).commit(), 3);
}

test "Cluster: view_change: lagging replica advances checkpoint during view change" {
    // It could be the case that the replica with the most advanced checkpoint has its checkpoint
    // corrupted. In this case, a replica with a slightly older checkpoint must step up as primary.

    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();
    var c = t.clients(0, t.cluster.clients.len);
    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    b2.stop();

    // Ensure b1 only commits up till checkpoint_2_trigger - 1, so it stays at checkpoint_1 while
    // a0 moves to checkpoint_2.
    try c.request(checkpoint_2_trigger - 1, checkpoint_2_trigger - 1);
    b1.drop(.R_, .incoming, .commit);
    try c.request(checkpoint_2_trigger, checkpoint_2_trigger);

    try expectEqual(a0.commit(), checkpoint_2_trigger);
    try expectEqual(a0.op_checkpoint(), checkpoint_2);
    try expectEqual(b1.commit(), checkpoint_2_trigger - 1);
    try expectEqual(b1.op_checkpoint(), checkpoint_1);

    b1.stop();

    try b2.open();
    // Don't allow b2 to repair its grid, otherwise it could help a0 commit past op_prepare_max for
    // checkpoint_2.
    b2.drop(.R_, .incoming, .block);

    t.run();

    try expectEqual(b2.op_checkpoint(), checkpoint_2);
    try expectEqual(b2.commit_max(), checkpoint_2_trigger);
    try expectEqual(b2.status(), .normal);

    // Progress a0 & b2's head past op_prepare_ok_max for checkpoint_2 (commit_max stays at
    // op_prepare_ok_max since a syncing replica's don't prepare_ok ops past prepare_ok_max).
    try c.request(
        checkpoint_2_prepare_max,
        checkpoint_2_prepare_ok_max,
    );

    try expectEqual(a0.op_checkpoint(), checkpoint_2);
    try expectEqual(a0.commit_max(), checkpoint_2_prepare_ok_max);

    try expectEqual(b2.op_checkpoint(), checkpoint_2);
    try expectEqual(b2.commit_max(), checkpoint_2_prepare_ok_max);

    b2.stop();

    a0.stop();
    // Drop incoming DVCs to a0 to check if b1 steps up as primary.
    a0.drop(.R_, .incoming, .do_view_change);
    try a0.open();

    try b1.open();
    b1.pass(.R_, .incoming, .commit);

    t.run();

    try expectEqual(a0.status(), .normal);
    try expectEqual(a0.op_checkpoint(), checkpoint_2);

    // b1 is able to advance its checkpoint during view change and become primary.
    try expectEqual(b1.role(), .primary);
    try expectEqual(b1.status(), .normal);
    try expectEqual(b1.op_checkpoint(), checkpoint_2);
}

test "Cluster: view-change: primary with dirty log" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    // Commit past the checkpoint_2_trigger to ensure that the op we will corrupt won't be found in
    // B1's pipeline cache.
    const commit_max = checkpoint_2_trigger +
        constants.pipeline_prepare_queue_max +
        constants.pipeline_request_queue_max;

    // Partition B2 so that it falls behind the cluster.
    b2.drop_all(.R_, .bidirectional);
    try c.request(commit_max, commit_max);

    // Allow B2 to join the cluster and complete state sync.
    b2.pass_all(.R_, .bidirectional);
    t.run();

    try expectEqual(t.replica(.R_).commit(), commit_max);
    try TestReplicas.expect_sync_done(t.replica(.R_));

    // Crash A0, and force B2 to become the primary.
    a0.stop();
    b1.drop(.__, .incoming, .do_view_change);

    // B2 tries to become primary. (Don't let B1 become primary – it would not realize its
    // checkpoint entry is corrupt, which would defeat the purpose of this test).
    // B2 tries to repair (request_prepare) this corrupt op, even though it is before its
    // checkpoint. B1 discovers that this op is corrupt, and marks it as faulty.
    b1.corrupt(.{ .wal_prepare = checkpoint_2 % slot_count });
    t.run();

    try expectEqual(b1.status(), .normal);
    try expectEqual(b2.status(), .normal);
}

test "Cluster: view-change: nack older view" {
    // a0 prepares (but does not commit) three ops (`x`, `x + 1`, `x + 2`) at view `v`.
    // b1 prepares (but does not commit) the same ops at view `v + 1`.
    // b2 receives only `x + 2` op prepared at b1.
    // b1 gets permanently partitioned from the cluster, and a0 and b2 form a core.
    //
    // a0 and b2 and should be able to truncate all the prepared, but uncommitted ops.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(checkpoint_1_trigger, checkpoint_1_trigger);
    try expectEqual(t.replica(.R_).commit(), checkpoint_1_trigger);

    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    try expectEqual(a0.role(), .primary);
    t.replica(.R_).drop_all(.R_, .bidirectional);
    try c.request(checkpoint_1_trigger + 3, checkpoint_1_trigger);
    try expectEqual(a0.op_head(), checkpoint_1_trigger + 3);

    t.replica(.R_).pass(.R_, .bidirectional, .ping);
    t.replica(.R_).pass(.R_, .bidirectional, .pong);
    b1.pass(.R_, .bidirectional, .start_view_change);
    b1.pass(.R_, .incoming, .do_view_change);
    b1.pass(.R_, .outgoing, .start_view);
    a0.drop_all(.R_, .bidirectional);
    b2.pass(.R_, .incoming, .prepare);
    b2.filter(.R_, .incoming, struct {
        fn drop_message(message: *Message) bool {
            const prepare = message.into(.prepare) orelse return false;
            return prepare.header.op < checkpoint_1_trigger + 3;
        }
    }.drop_message);

    t.run();
    try expectEqual(b1.role(), .primary);
    try expectEqual(b1.status(), .normal);

    try expectEqual(t.replica(.R_).op_head(), checkpoint_1_trigger + 3);
    try expectEqual(t.replica(.R_).commit_max(), checkpoint_1_trigger);

    a0.pass_all(.R_, .bidirectional);
    b2.pass_all(.R_, .bidirectional);
    b2.filter(.R_, .incoming, null);
    b1.drop_all(.R_, .bidirectional);

    try c.request(checkpoint_1_trigger + 3, checkpoint_1_trigger + 3);
    try expectEqual(b2.commit_max(), checkpoint_1_trigger + 3);
    try expectEqual(a0.commit_max(), checkpoint_1_trigger + 3);
    try expectEqual(b1.commit_max(), checkpoint_1_trigger);
}

test "Cluster: sync: partition, lag, sync (transition from idle)" {
    for ([_]u64{
        // Normal case: the cluster has prepared beyond the checkpoint.
        // The lagging replica can learn the latest checkpoint from a commit message.
        checkpoint_2_prepare_max + 1,
        // Idle case: the idle cluster has not prepared beyond the checkpoint.
        // The lagging replica is far enough behind the cluster that it can sync to the latest
        // checkpoint anyway, since it cannot possibly recover via WAL repair.
        checkpoint_2_prepare_max,
    }) |cluster_commit_max| {
        log.info("test cluster_commit_max={}", .{cluster_commit_max});

        const t = try TestContext.init(.{ .replica_count = 3 });
        defer t.deinit();

        var c = t.clients(0, t.cluster.clients.len);

        t.replica(.R2).drop_all(.R_, .bidirectional);
        try c.request(cluster_commit_max, cluster_commit_max);

        t.replica(.R2).pass_all(.R_, .bidirectional);
        t.run();

        // R2 catches up via state sync.
        try expectEqual(t.replica(.R_).status(), .normal);
        try expectEqual(t.replica(.R_).commit(), cluster_commit_max);
        try expectEqual(t.replica(.R_).sync_status(), .idle);

        // The entire cluster is healthy and able to commit more.
        try c.request(checkpoint_3_trigger, checkpoint_3_trigger);
        try expectEqual(t.replica(.R_).status(), .normal);
        try expectEqual(t.replica(.R_).commit(), checkpoint_3_trigger);

        t.run(); // (Wait for grid sync to finish.)
        try TestReplicas.expect_sync_done(t.replica(.R_));
    }
}

test "Cluster: repair: R=2 (primary checkpoints, but backup lags behind)" {
    const t = try TestContext.init(.{ .replica_count = 2 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(checkpoint_1_trigger - 1, checkpoint_1_trigger - 1);

    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);

    // A0 prepares the trigger op, commits it, and checkpoints.
    // B1 prepares the trigger op, but does not commit/checkpoint.
    b1.drop(.R_, .incoming, .commit); // Prevent last commit.
    try c.request(checkpoint_1_trigger, checkpoint_1_trigger);
    try expectEqual(a0.commit(), checkpoint_1_trigger);
    try expectEqual(b1.commit(), checkpoint_1_trigger - 1);
    try expectEqual(a0.op_head(), checkpoint_1_trigger);
    try expectEqual(b1.op_head(), checkpoint_1_trigger);
    try expectEqual(a0.op_checkpoint(), checkpoint_1);
    try expectEqual(b1.op_checkpoint(), 0);

    // On B1, corrupt the same slot that A0 is about to overwrite with a new prepare.
    // (B1 doesn't have any prepare in this slot, thanks to the vsr_checkpoint_ops.)
    b1.stop();
    b1.pass(.R_, .incoming, .commit);
    b1.corrupt(.{ .wal_prepare = (checkpoint_1_trigger + 2) % slot_count });

    // Prepare a full pipeline of ops. Since B1 is still lagging behind, this doesn't actually
    // overwrite any entries from the previous wrap.
    const pipeline_prepare_queue_max = constants.pipeline_prepare_queue_max;
    try c.request(checkpoint_1_trigger + pipeline_prepare_queue_max, checkpoint_1_trigger);

    try b1.open();
    t.run();

    try expectEqual(t.replica(.R_).commit(), checkpoint_1_trigger + pipeline_prepare_queue_max);
    try expectEqual(c.replies(), checkpoint_1_trigger + pipeline_prepare_queue_max);

    // Neither replica used state sync, but it is "done" since all content is present.
    try TestReplicas.expect_sync_done(t.replica(.R_));
}

test "Cluster: sync: R=4, 2/4 ahead + idle, 2/4 lagging, sync" {
    const t = try TestContext.init(.{ .replica_count = 4 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(1, 1);
    try expectEqual(t.replica(.R_).commit(), 1);

    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);
    var b3 = t.replica(.B3);

    b2.stop();
    b3.stop();

    try c.request(checkpoint_2_trigger, checkpoint_2_trigger);
    try expectEqual(a0.status(), .normal);
    try expectEqual(b1.status(), .normal);

    try b2.open();
    try b3.open();
    t.run();
    t.run();

    try expectEqual(t.replica(.R_).status(), .normal);
    try expectEqual(t.replica(.R_).sync_status(), .idle);
    try expectEqual(t.replica(.R_).commit(), checkpoint_2_trigger);
    try expectEqual(t.replica(.R_).op_checkpoint(), checkpoint_2);

    try TestReplicas.expect_sync_done(t.replica(.R_));
}

test "Cluster: sync: view-change with lagging replica" {
    // Check that a cluster can view change even if view-change quorum contains syncing replicas.
    // This used to be a special case for an older sync protocol, but now this mostly holds by
    // construction.
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(1, 1); // Make sure that the logic doesn't depend on the root prepare.
    try expectEqual(t.replica(.R_).commit(), 1);

    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    b2.drop_all(.R_, .bidirectional); // Isolate B2.
    try c.request(checkpoint_2_trigger, checkpoint_2_trigger);

    // Allow B2 to join, but partition A0 to force a view change.
    // B2 is lagging far enough behind that it must state sync.
    // Despite this, the cluster of B1/B2 should recover to normal status.
    b2.pass_all(.R_, .bidirectional);
    a0.drop_all(.R_, .bidirectional);

    // Let the cluster run for some time without B2 state syncing.
    b2.drop(.R_, .bidirectional, .start_view);
    t.run();
    try expectEqual(b2.status(), .view_change);
    try expectEqual(b2.op_checkpoint(), 0);
    try c.request(checkpoint_2_trigger + 1, checkpoint_2_trigger); // Cluster is blocked.

    // Let B2 state sync. This unblocks the cluster.
    b2.pass(.R_, .bidirectional, .start_view);
    t.run();
    try expectEqual(b1.role(), .primary);
    try expectEqual(t.replica(.R_).status(), .normal);
    try expectEqual(t.replica(.R_).sync_status(), .idle);
    try expect(b2.commit() >= checkpoint_2_trigger);
    try expectEqual(t.replica(.R_).op_checkpoint(), checkpoint_2);

    // Note: we need to commit more --- state sync status is cleared only at checkpoint.
    try c.request(checkpoint_3_trigger, checkpoint_3_trigger);
    try TestReplicas.expect_sync_done(t.replica(.R_));
}

test "Cluster: sync: slightly lagging replica" {
    // Sometimes a replica must switch to state sync even if it is within journal_slot_count
    // ops from commit_max. Checkpointed ops are not repaired and might become unavailable.

    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(checkpoint_1 - 1, checkpoint_1 - 1);

    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    b2.drop_all(.R_, .bidirectional);
    try c.request(checkpoint_1_trigger + 1, checkpoint_1_trigger + 1);

    // Corrupt all copies of a checkpointed prepare.
    a0.corrupt(.{ .wal_prepare = checkpoint_1 });
    b1.corrupt(.{ .wal_prepare = checkpoint_1 });
    try c.request(checkpoint_1_prepare_max + 1, checkpoint_1_prepare_max + 1);

    // At this point, b2 won't be able to repair WAL and must state sync.
    b2.pass_all(.R_, .bidirectional);
    try c.request(checkpoint_1_prepare_max + 2, checkpoint_1_prepare_max + 2);
    try expectEqual(t.replica(.R_).commit(), checkpoint_1_prepare_max + 2);
}

test "Cluster: sync: checkpoint from a newer view" {
    // B1 appends (but does not commit) prepares across a checkpoint boundary.
    // Then the cluster truncates those prepares and commits past the checkpoint trigger.
    // When B1 subsequently joins, it should state sync and truncate the log. Immediately
    // after state sync, the log doesn't connect to B1's new checkpoint.
    const t = try TestContext.init(.{ .replica_count = 6 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(checkpoint_1 - 1, checkpoint_1 - 1);
    try expectEqual(t.replica(.R_).commit(), checkpoint_1 - 1);

    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);

    {
        // Prevent A0 from committing, prevent any other replica from becoming a primary, and
        // only allow B1 to learn about A0 prepares.
        t.replica(.R_).drop(.R_, .incoming, .prepare);
        t.replica(.R_).drop(.R_, .incoming, .prepare_ok);
        t.replica(.R_).drop(.R_, .incoming, .start_view_change);
        b1.pass(.A0, .incoming, .prepare);
        b1.filter(.A0, .incoming, struct {
            // Force b1 to sync, rather than repair.
            fn drop_message(message: *Message) bool {
                const prepare = message.into(.prepare) orelse return false;
                return prepare.header.op == checkpoint_1;
            }
        }.drop_message);
        try c.request(checkpoint_1 + 1, checkpoint_1 - 1);
        try expectEqual(a0.op_head(), checkpoint_1 + 1);
        try expectEqual(b1.op_head(), checkpoint_1 + 1);
        try expectEqual(a0.commit(), checkpoint_1 - 1);
        try expectEqual(b1.commit(), checkpoint_1 - 1);
    }

    {
        // Make the rest of cluster prepare and commit a different sequence of prepares.
        t.replica(.R_).pass(.R_, .incoming, .prepare);
        t.replica(.R_).pass(.R_, .incoming, .prepare_ok);
        t.replica(.R_).pass(.R_, .incoming, .start_view_change);

        a0.drop_all(.R_, .bidirectional);
        b1.drop_all(.R_, .bidirectional);
        try c.request(checkpoint_2, checkpoint_2);
    }

    {
        // Let B1 rejoin, but prevent it from jumping into view change.
        b1.pass_all(.R_, .bidirectional);
        b1.drop(.R_, .bidirectional, .start_view);
        b1.drop(.R_, .incoming, .ping);
        b1.drop(.R_, .incoming, .pong);

        try c.request(checkpoint_2_trigger - 1, checkpoint_2_trigger - 1);

        // Wipe B1 in-memory state and check that it ends up in a consistent state after restart.
        b1.stop();
        try b1.open();
        t.run();
    }

    t.replica(.R_).pass_all(.R_, .bidirectional);
    t.run();
    try expectEqual(t.replica(.R_).commit(), checkpoint_2_trigger - 1);
}

test "Cluster: prepare beyond checkpoint trigger" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(checkpoint_1_trigger - 1, checkpoint_1_trigger - 1);
    try expectEqual(t.replica(.R_).commit(), checkpoint_1_trigger - 1);

    // Temporarily drop acks so that requests may prepare but not commit.
    // (And to make sure we don't start checkpointing until we have had a chance to assert the
    // cluster's state.)
    t.replica(.R_).drop(.__, .bidirectional, .prepare_ok);

    // Prepare ops beyond the checkpoint.
    try c.request(checkpoint_1_prepare_ok_max, checkpoint_1_trigger - 1);
    try expectEqual(t.replica(.R_).op_checkpoint(), 0);
    try expectEqual(t.replica(.R_).commit(), checkpoint_1_trigger - 1);
    try expectEqual(t.replica(.R_).op_head(), checkpoint_1_prepare_ok_max - 1);

    t.replica(.R_).pass(.__, .bidirectional, .prepare_ok);
    t.run();
    try expectEqual(c.replies(), checkpoint_1_prepare_ok_max);
    try expectEqual(t.replica(.R_).op_checkpoint(), checkpoint_1);
    try expectEqual(t.replica(.R_).commit(), checkpoint_1_prepare_ok_max);
    try expectEqual(t.replica(.R_).op_head(), checkpoint_1_prepare_ok_max);
}

test "Cluster: upgrade: operation=upgrade near trigger-minus-bar" {
    const trigger_for_checkpoint = vsr.Checkpoint.trigger_for_checkpoint;
    for ([_]struct {
        request: u64,
        checkpoint: u64,
    }{
        .{
            // The entire last bar before the operation is free for operation=upgrade's, so when we
            // hit the checkpoint trigger we can immediately upgrade the cluster.
            .request = checkpoint_1_trigger - constants.lsm_compaction_ops,
            .checkpoint = checkpoint_1,
        },
        .{
            // Since there is a non-upgrade request in the last bar, the replica cannot upgrade
            // during checkpoint_1 and must pad ahead to the next checkpoint.
            .request = checkpoint_1_trigger - constants.lsm_compaction_ops + 1,
            .checkpoint = checkpoint_2,
        },
    }) |data| {
        const t = try TestContext.init(.{ .replica_count = 3 });
        defer t.deinit();

        var c = t.clients(0, t.cluster.clients.len);
        try c.request(data.request, data.request);

        t.replica(.R_).stop();
        try t.replica(.R_).open_upgrade(&[_]u8{ 10, 20 });

        // Prevent the upgrade from committing so that we can verify that the replica is still
        // running version 1.
        t.replica(.R_).drop(.__, .bidirectional, .prepare_ok);
        t.run();
        try expectEqual(t.replica(.R_).op_checkpoint(), 0);
        try expectEqual(t.replica(.R_).release(), 10);

        t.replica(.R_).pass(.__, .bidirectional, .prepare_ok);
        t.run();
        try expectEqual(t.replica(.R_).release(), 20);
        try expectEqual(t.replica(.R_).op_checkpoint(), data.checkpoint);
        try expectEqual(t.replica(.R_).commit(), trigger_for_checkpoint(data.checkpoint).?);
        try expectEqual(t.replica(.R_).op_head(), trigger_for_checkpoint(data.checkpoint).?);

        // Verify that the upgraded cluster is healthy; i.e. that it can commit.
        try c.request(data.request + 1, data.request + 1);
    }
}

test "Cluster: upgrade: R=1" {
    // R=1 clusters upgrade even though they don't build a quorum of upgrade targets.
    const t = try TestContext.init(.{ .replica_count = 1 });
    defer t.deinit();

    t.replica(.R_).stop();
    try t.replica(.R0).open_upgrade(&[_]u8{ 10, 20 });
    t.run();

    try expectEqual(t.replica(.R0).health(), .up);
    try expectEqual(t.replica(.R0).release(), 20);
    try expectEqual(t.replica(.R0).op_checkpoint(), checkpoint_1);
    try expectEqual(t.replica(.R0).commit(), checkpoint_1_trigger);
}

test "Cluster: upgrade: state-sync to new release" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);

    t.replica(.R_).stop();
    try t.replica(.R0).open_upgrade(&[_]u8{ 10, 20 });
    try t.replica(.R1).open_upgrade(&[_]u8{ 10, 20 });
    try t.replica(.R2).open_upgrade(&[_]u8{ 10, 20 });

    // R2 is advertising the new release (so that the upgrade can begin) but it doesn't actually
    // join in yet.
    t.replica(.R2).drop(.__, .bidirectional, .prepare);
    t.replica(.R2).drop(.__, .bidirectional, .start_view); // Prevent state sync.
    t.run();

    try expectEqual(t.replica(.R0).commit(), checkpoint_1_trigger);
    try c.request(constants.vsr_checkpoint_ops, constants.vsr_checkpoint_ops);
    try expectEqual(t.replica(.R0).commit(), checkpoint_2_trigger);

    // R2 state-syncs from R0/R1, updating its release from v1 to v2 via CheckpointState...
    t.replica(.R2).stop();
    t.replica(.R2).pass_all(.__, .bidirectional);
    try t.replica(.R2).open_upgrade(&[_]u8{10});
    try expectEqual(t.replica(.R2).health(), .up);
    try expectEqual(t.replica(.R2).release(), 10);
    try expectEqual(t.replica(.R2).commit(), 0);
    t.run();

    // ...But R2 doesn't have v2 available, so it shuts down.
    try expectEqual(t.replica(.R2).health(), .down);
    try expectEqual(t.replica(.R2).release(), 10);
    try expectEqual(t.replica(.R2).commit(), checkpoint_2);

    // Start R2 up with v2 available, and it recovers.
    try t.replica(.R2).open_upgrade(&[_]u8{ 10, 20 });
    try expectEqual(t.replica(.R2).health(), .up);
    try expectEqual(t.replica(.R2).release(), 20);
    try expectEqual(t.replica(.R2).commit(), checkpoint_2);

    t.run();
    try expectEqual(t.replica(.R2).commit(), t.replica(.R_).commit());
}

test "Cluster: scrub: background scrubber, fully corrupt grid" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    var c = t.clients(0, t.cluster.clients.len);
    try c.request(checkpoint_2_trigger, checkpoint_2_trigger);
    try expectEqual(t.replica(.R_).commit(), checkpoint_2_trigger);

    var a0 = t.replica(.A0);
    const b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    const a0_free_set = &t.cluster.replicas[a0.replicas.get(0)].grid.free_set;
    const b2_free_set = &t.cluster.replicas[b2.replicas.get(0)].grid.free_set;
    const b2_storage = &t.cluster.storages[b2.replicas.get(0)];

    // Corrupt B2's entire grid.
    // Note that we intentionally do *not* shut down B2 for this – the intent is to test the
    // scrubber, without leaning on Grid.read_block()'s `from_local_or_global_storage`.
    {
        const address_max = t.block_address_max();
        var address: u64 = 1;
        while (address <= address_max) : (address += 1) {
            b2.corrupt(.{ .grid_block = address });
        }
    }

    // Disable new read/write faults so that we can use `storage.faults` to track repairs.
    // (That is, as the scrubber runs, the number of faults will monotonically decrease.)
    b2_storage.options.read_fault_probability = 0;
    b2_storage.options.write_fault_probability = 0;

    // Tick until B2's grid repair stops making progress.
    {
        var faults_before = b2_storage.faults.count();
        while (true) {
            t.run();

            const faults_after = b2_storage.faults.count();
            assert(faults_after <= faults_before);
            if (faults_after == faults_before) break;

            faults_before = faults_after;
        }
    }

    // Verify that B2 repaired all blocks.
    const address_max = t.block_address_max();
    var address: u64 = 1;
    while (address <= address_max) : (address += 1) {
        if (a0_free_set.is_free(address)) {
            assert(b2_free_set.is_free(address));
            assert(b2_storage.area_faulty(.{ .grid = .{ .address = address } }));
        } else if (!a0_free_set.is_released(address)) {
            // Acquired (but not released) blocks are guaranteed to be repaired by the scrubber.
            assert(!b2_free_set.is_free(address));
            assert(!b2_free_set.is_released(address));
            assert(!b2_storage.area_faulty(.{ .grid = .{ .address = address } }));
        } else {
            // Acquired (but released) blocks are not guaranteed to be repaired by the scrubber.
            // Includes the following blocks that will be freed when checkpoint_2 becomes durable:
            // * Blocks released by ManifestLog compaction,
            // * Blocks released ClientSessions and FreeSet checkpoint trailers (these *could* be
            //   released at the checkpoint itself, since new checkpoint trailers are allocated
            //   at checkpoint, but we release them at checkpoint durability alongside other
            //   released blocks).
            maybe(b2_storage.area_faulty(.{ .grid = .{ .address = address } }));
        }
    }

    try TestReplicas.expect_equal_grid(a0, b2);
    try TestReplicas.expect_equal_grid(b1, b2);
}

// Compat(v0.15.3)
test "Cluster: client: empty command=request operation=register body" {
    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();

    // Wait for the primary to settle, since this test doesn't implement request retries.
    t.run();

    var client_bus = try t.client_bus(0);
    defer client_bus.deinit();

    var request_header = vsr.Header.Request{
        .cluster = t.cluster.options.cluster_id,
        .size = @sizeOf(vsr.Header),
        .client = client_bus.client_id,
        .request = 0,
        .command = .request,
        .operation = .register,
        .release = releases[0].release,
    };
    request_header.set_checksum_body(&.{}); // Note the absence of a `vsr.RegisterRequest`.
    request_header.set_checksum();

    client_bus.request(t.replica(.A0).index(), &request_header, &.{});
    t.run();

    const Reply = extern struct {
        header: vsr.Header.Reply,
        body: vsr.RegisterResult,
    };

    const reply = std.mem.bytesAsValue(Reply, client_bus.reply.?.buffer[0..@sizeOf(Reply)]);
    try expectEqual(reply.header.command, .reply);
    try expectEqual(reply.header.operation, .register);
    try expectEqual(reply.header.size, @sizeOf(Reply));
    try expectEqual(reply.header.request, 0);
    try expect(stdx.zeroed(std.mem.asBytes(&reply.body)));
}

test "Cluster: eviction: no_session" {
    const t = try TestContext.init(.{
        .replica_count = 3,
        .client_count = constants.clients_max + 1,
    });
    defer t.deinit();

    var c0 = t.clients(0, 1);
    var c = t.clients(1, constants.clients_max);

    // Register a single client.
    try c0.request(1, 1);
    // Register clients_max other clients.
    // This evicts the "extra" client, though the eviction message has not been sent yet.
    try c.request(constants.clients_max, constants.clients_max);

    // Try to send one last request -- which fails, since this client has been evicted.
    try c0.request(2, 1);
    try expectEqual(c0.eviction_reason(), .no_session);
    try expectEqual(c.eviction_reason(), null);
}

test "Cluster: eviction: client_release_too_low" {
    const t = try TestContext.init(.{
        .replica_count = 3,
        .client_release = .{ .value = releases[0].release.value - 1 },
    });
    defer t.deinit();

    var c0 = t.clients(0, 1);
    try c0.request(1, 0);
    try expectEqual(c0.eviction_reason(), .client_release_too_low);
}

test "Cluster: eviction: client_release_too_high" {
    const t = try TestContext.init(.{
        .replica_count = 3,
        .client_release = .{ .value = releases[0].release.value + 1 },
    });
    defer t.deinit();

    var c0 = t.clients(0, 1);
    try c0.request(1, 0);
    try expectEqual(c0.eviction_reason(), .client_release_too_high);
}

test "Cluster: eviction: session_too_low" {
    const t = try TestContext.init(.{
        .replica_count = 3,
        .client_count = constants.clients_max + 1,
    });
    defer t.deinit();

    var c0 = t.clients(0, 1);
    var c = t.clients(1, constants.clients_max);

    t.replica(.R_).record(.C0, .incoming, .request);
    try c0.request(1, 1);

    // Evict C0. (C0 doesn't know this yet, though).
    try c.request(constants.clients_max, constants.clients_max);
    try expectEqual(c0.eviction_reason(), null);

    // Replay C0's register message.
    t.replica(.R_).replay_recorded();
    t.run();

    const mark = marks.check("on_request: ignoring older session");

    // C0 now has a session again, but the client only knows the old (evicted) session number.
    try c0.request(2, 1);
    try mark.expect_hit();
    try expectEqual(c0.eviction_reason(), .session_too_low);
}

test "Cluster: view_change: DVC header doesn't match current header in journal" {
    // It could be the case that a replica's DVC headers don't match the journal's current state.
    // For example, a header could be blank in the DVC but present in the journal (could happen if
    // the DVC was computed when that header was corrupt/missing in the replica's journal, and the
    // replica is simply reusing an old DVC). The replica must check the journal before
    // broadcasating its DVC, so it appropriately acks/nacks headers in the DVC based on the current
    // state of the journal.

    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();
    var c = t.clients(0, t.cluster.clients.len);
    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    b2.stop();

    // Ensure b1 only commits up till checkpoint_2_trigger - 1, so it stays at checkpoint_1 while
    // a0 moves to checkpoint_2.
    try c.request(checkpoint_2_trigger - 1, checkpoint_2_trigger - 1);
    b1.drop(.R_, .incoming, .commit);
    try c.request(checkpoint_2_trigger, checkpoint_2_trigger);

    try expectEqual(a0.commit(), checkpoint_2_trigger);
    try expectEqual(a0.op_checkpoint(), checkpoint_2);
    try expectEqual(b1.commit(), checkpoint_2_trigger - 1);
    try expectEqual(b1.op_checkpoint(), checkpoint_1);

    b1.stop();

    try b2.open();
    t.run();

    // b2 performs state sync to get caught up with a0.
    try expectEqual(b2.op_checkpoint(), checkpoint_2);
    try expectEqual(b2.commit_max(), checkpoint_2_trigger);
    try expectEqual(b2.status(), .normal);
    try b2.expect_sync_done();

    try c.request(checkpoint_2_prepare_max, checkpoint_2_prepare_max);

    // a0 and b2 both prepare and commit up to the prepare_max for checkpoint_2.
    try expectEqual(a0.op_head(), checkpoint_2_prepare_max);
    try expectEqual(a0.op_checkpoint(), checkpoint_2);
    try expectEqual(a0.commit_max(), checkpoint_2_prepare_max);

    try expectEqual(b2.op_head(), checkpoint_2_prepare_max);
    try expectEqual(b2.op_checkpoint(), checkpoint_2);
    try expectEqual(b2.commit_max(), checkpoint_2_prepare_max);

    b2.stop();
    a0.stop();

    // Corrupt op_head() - 1 to ensure that the DVC headers computed by a0 on startup contain a
    // blank header for op_header() - 1.
    a0.corrupt(.{ .wal_prepare = (a0.op_head() - 1) % slot_count });

    const mark = marks.check("quorum received, awaiting repair");

    try a0.open();
    try b1.open();

    t.run();

    // The two replicas are stuck in view change:
    // B1 is still on checkpoint_1, it's DVC header lagging behind A0's. A0's DVC headers contain a
    // blank header for op_head() - 1, which it can't nack/ack because it is corrupted in the
    // journal. There aren't enough nacks for truncating op_head() -1 (nack_quorum=2), and no acks
    // for it to be retained in the view change.
    try expectEqual(a0.status(), .view_change);
    try expectEqual(b1.status(), .view_change);
    try mark.expect_hit();

    a0.stop();
    const a0_storage = &t.cluster.storages[a0.replicas.get(0)];

    a0_storage.faulty = false;
    const mark2 = marks.check("quorum received, awaiting repair");

    try a0.open();

    t.run();

    // The two replicas are stuck in view change still. a0 reuses its old DVC headers with a blank
    // header for op_head() - 1, but it still can't ack/nack it.
    try mark2.expect_hit();
    try expectEqual(a0.status(), .view_change);
    try expectEqual(b1.status(), .view_change);

    a0_storage.faulty = true;
    try b2.open();
    t.run();

    // a0 is able to resolve its dilemma about op_head() - 1 with the help of b2, which acks it.
    try expectEqual(t.replica(.R0).status(), .normal);
    try expectEqual(t.replica(.R0).op_checkpoint(), checkpoint_2);
    try expectEqual(t.replica(.R0).commit_max(), checkpoint_2_prepare_max);
}

test "Cluster: view_change: lagging replica repairs WAL using start_view from potential primary" {
    // It could be the case that the replica with the most advanced checkpoint has a corruption in
    // its grid. In this case, a replica on an older checkpoint can use a start_view message from
    // the most up-to-date replica to repair its WAL, advance its checkpoint, and become primary.

    const t = try TestContext.init(.{ .replica_count = 3 });
    defer t.deinit();
    var c = t.clients(0, t.cluster.clients.len);
    var a0 = t.replica(.A0);
    var b1 = t.replica(.B1);
    var b2 = t.replica(.B2);

    b2.stop();

    // Ensure b1 only commits up till checkpoint_2_trigger - 1, so it stays at checkpoint_1 while
    // a0 moves to checkpoint_2.
    try c.request(checkpoint_2_trigger - 1, checkpoint_2_trigger - 1);
    b1.drop(.R_, .incoming, .commit);
    try c.request(checkpoint_2_trigger, checkpoint_2_trigger);

    try expectEqual(a0.commit(), checkpoint_2_trigger);
    try expectEqual(a0.op_checkpoint(), checkpoint_2);
    try expectEqual(b1.commit(), checkpoint_2_trigger - 1);
    try expectEqual(b1.op_checkpoint(), checkpoint_1);

    // Start b2 so that the a0 & b2 can make progress to checkpoint_3; b1 is stopped so it remains
    // lagging at checkpoint_1.
    try b2.open();
    b1.stop();

    t.run();

    try expectEqual(b2.op_checkpoint(), checkpoint_2);
    try expectEqual(b2.commit_max(), checkpoint_2_trigger);
    try expectEqual(b2.status(), .normal);
    try b2.expect_sync_done();

    try c.request(
        checkpoint_3_trigger,
        checkpoint_3_trigger,
    );

    try expectEqual(a0.op_head(), checkpoint_3_trigger);
    try expectEqual(a0.op_checkpoint(), checkpoint_3);
    try expectEqual(b2.op_head(), checkpoint_3_trigger);
    try expectEqual(b2.op_checkpoint(), checkpoint_3);

    // Simulate compaction getting stuck on a0 due to a grid corruption. Corrupting the grid doesn't
    // work here since compaction in replica tests is always able to apply the move table
    // optimization. This is because all requests in replica tests are `echo` operations, which are
    // inserted into the LSM with monotonically increasing id.
    const a0_replica = &t.cluster.replicas[a0.replicas.get(0)];
    a0_replica.commit_stage = .compact;

    try c.request(
        checkpoint_3_trigger + 1,
        checkpoint_3_trigger,
    );

    try expectEqual(a0.op_head(), checkpoint_3_trigger + 1);
    try expectEqual(a0.commit(), checkpoint_3_trigger);

    try expectEqual(b2.op_head(), checkpoint_3_trigger + 1);
    try expectEqual(b2.commit(), checkpoint_3_trigger);

    const committing_prepare = a0_replica.pipeline.queue.prepare_queue.head_ptr_const().?;
    a0_replica.commit_prepare = committing_prepare.message.ref();

    // Partition a0, force b1 & b2 into view_change by blocking outgoing .do_view_change messages.
    a0.drop_all(.R_, .bidirectional);

    try b1.open();
    b1.drop(.R_, .outgoing, .do_view_change);
    b2.drop(.R_, .outgoing, .do_view_change);

    t.run();

    try expectEqual(b1.status(), .view_change);
    try expectEqual(b2.status(), .view_change);

    // Stop b2, allow a0 and b1 to view change. a0 can't step up as primary since it has a
    // corruption in its grid, due to which it can't make progress on its commit pipeline. However,
    // since it has an intact WAL, it is able to send a .start_view message to b1. With the help
    // of the .start_view message, b1 can repair, commit, advance from checkpoint_1 -> checkpoint_3,
    // and step up as primary.
    b2.stop();
    a0.pass_all(.R_, .bidirectional);
    b1.pass(.R_, .outgoing, .do_view_change);

    t.run();
    t.run();
    t.run();

    try expectEqual(b1.status(), .normal);
    try expectEqual(b1.role(), .primary);
    try expectEqual(b1.op_checkpoint(), checkpoint_3);
    try expectEqual(b1.commit(), checkpoint_3_trigger + 1);

    try expectEqual(a0.status(), .normal);
    try expectEqual(a0.role(), .backup);
    try expectEqual(a0.op_checkpoint(), checkpoint_3);
    try expectEqual(b1.commit(), checkpoint_3_trigger + 1);
}

const ProcessSelector = enum {
    __, // all replicas, standbys, and clients
    R_, // all (non-standby) replicas
    R0,
    R1,
    R2,
    R3,
    R4,
    R5,
    S_, // all standbys
    S0,
    S1,
    S2,
    S3,
    S4,
    S5,
    A0, // current primary
    B1, // backup immediately following current primary
    B2,
    B3,
    B4,
    B5,
    C_, // all clients
    C0,
};

const TestContext = struct {
    cluster: *Cluster,
    log_level: std.log.Level,
    client_requests: []usize,
    client_replies: []usize,

    pub fn init(options: struct {
        replica_count: u8,
        standby_count: u8 = 0,
        client_count: u8 = constants.clients_max,
        client_release: vsr.Release = releases[0].release,
        seed: u64 = 123,
    }) !*TestContext {
        const log_level_original = std.testing.log_level;
        std.testing.log_level = log_level;
        var prng = std.rand.DefaultPrng.init(options.seed);
        const random = prng.random();

        const cluster = try Cluster.init(allocator, .{
            .cluster_id = 0,
            .replica_count = options.replica_count,
            .standby_count = options.standby_count,
            .client_count = options.client_count,
            .storage_size_limit = vsr.sector_floor(128 * 1024 * 1024),
            .seed = random.int(u64),
            .releases = &releases,
            .client_release = options.client_release,
            .network = .{
                .node_count = options.replica_count + options.standby_count,
                .client_count = options.client_count,
                .seed = random.int(u64),
                .one_way_delay_mean = 3 + random.uintLessThan(u16, 10),
                .one_way_delay_min = random.uintLessThan(u16, 3),

                .path_maximum_capacity = 128,
                .path_clog_duration_mean = 0,
                .path_clog_probability = 0,
                .recorded_count_max = 16,
            },
            .storage = .{
                .read_latency_min = 1,
                .read_latency_mean = 5,
                .write_latency_min = 1,
                .write_latency_mean = 5,
            },
            .storage_fault_atlas = .{
                .faulty_superblock = false,
                .faulty_wal_headers = false,
                .faulty_wal_prepares = false,
                .faulty_client_replies = false,
                .faulty_grid = false,
            },
            .state_machine = .{
                .batch_size_limit = constants.message_body_size_max,
                .lsm_forest_node_count = 4096,
            },
            .on_client_reply = TestContext.on_client_reply,
        });
        errdefer cluster.deinit();

        for (cluster.storages) |*storage| storage.faulty = true;

        const client_requests = try allocator.alloc(usize, options.client_count);
        errdefer allocator.free(client_requests);
        @memset(client_requests, 0);

        const client_replies = try allocator.alloc(usize, options.client_count);
        errdefer allocator.free(client_replies);
        @memset(client_replies, 0);

        const context = try allocator.create(TestContext);
        errdefer allocator.destroy(context);

        context.* = .{
            .cluster = cluster,
            .log_level = log_level_original,
            .client_requests = client_requests,
            .client_replies = client_replies,
        };
        cluster.context = context;

        return context;
    }

    pub fn deinit(t: *TestContext) void {
        std.testing.log_level = t.log_level;
        allocator.free(t.client_replies);
        allocator.free(t.client_requests);
        t.cluster.deinit();
        allocator.destroy(t);
    }

    pub fn replica(t: *TestContext, selector: ProcessSelector) TestReplicas {
        const replica_processes = t.processes(selector);
        var replica_indexes = stdx.BoundedArrayType(u8, constants.members_max){};
        for (replica_processes.const_slice()) |p| replica_indexes.append_assume_capacity(p.replica);
        return TestReplicas{
            .context = t,
            .cluster = t.cluster,
            .replicas = replica_indexes,
        };
    }

    pub fn clients(t: *TestContext, index: usize, count: usize) TestClients {
        var client_indexes = stdx.BoundedArrayType(usize, constants.clients_max){};
        for (index..index + count) |i| client_indexes.append_assume_capacity(i);
        return TestClients{
            .context = t,
            .cluster = t.cluster,
            .clients = client_indexes,
        };
    }

    pub fn client_bus(t: *TestContext, client_index: usize) !*TestClientBus {
        // Reuse one of `Cluster.clients`' ids since the Network preallocated links for it.
        return TestClientBus.init(t, t.cluster.clients[client_index].id);
    }

    pub fn run(t: *TestContext) void {
        const tick_max = 4_100;
        var tick_count: usize = 0;
        while (tick_count < tick_max) : (tick_count += 1) {
            if (t.tick()) tick_count = 0;
        }
    }

    pub fn block_address_max(t: *TestContext) u64 {
        const grid_blocks = t.cluster.storages[0].grid_blocks();
        for (t.cluster.storages) |storage| {
            assert(storage.grid_blocks() == grid_blocks);
        }
        return grid_blocks; // NB: no -1 needed, addresses start from 1.
    }

    /// Returns whether the cluster state advanced.
    fn tick(t: *TestContext) bool {
        const commits_before = t.cluster.state_checker.commits.items.len;
        t.cluster.tick();
        return commits_before != t.cluster.state_checker.commits.items.len;
    }

    fn on_client_reply(
        cluster: *Cluster,
        client: usize,
        request: *const Message.Request,
        reply: *const Message.Reply,
    ) void {
        _ = request;
        _ = reply;
        const t: *TestContext = @ptrCast(@alignCast(cluster.context.?));
        t.client_replies[client] += 1;
    }

    const ProcessList = stdx.BoundedArrayType(
        Process,
        constants.members_max + constants.clients_max,
    );

    fn processes(t: *const TestContext, selector: ProcessSelector) ProcessList {
        const replica_count = t.cluster.options.replica_count;

        var view: u32 = 0;
        for (t.cluster.replicas) |*r| view = @max(view, r.view);

        var array = ProcessList{};
        switch (selector) {
            .R0 => array.append_assume_capacity(.{ .replica = 0 }),
            .R1 => array.append_assume_capacity(.{ .replica = 1 }),
            .R2 => array.append_assume_capacity(.{ .replica = 2 }),
            .R3 => array.append_assume_capacity(.{ .replica = 3 }),
            .R4 => array.append_assume_capacity(.{ .replica = 4 }),
            .R5 => array.append_assume_capacity(.{ .replica = 5 }),
            .S0 => array.append_assume_capacity(.{ .replica = replica_count + 0 }),
            .S1 => array.append_assume_capacity(.{ .replica = replica_count + 1 }),
            .S2 => array.append_assume_capacity(.{ .replica = replica_count + 2 }),
            .S3 => array.append_assume_capacity(.{ .replica = replica_count + 3 }),
            .S4 => array.append_assume_capacity(.{ .replica = replica_count + 4 }),
            .S5 => array.append_assume_capacity(.{ .replica = replica_count + 5 }),
            .A0 => array
                .append_assume_capacity(.{ .replica = @intCast((view + 0) % replica_count) }),
            .B1 => array
                .append_assume_capacity(.{ .replica = @intCast((view + 1) % replica_count) }),
            .B2 => array
                .append_assume_capacity(.{ .replica = @intCast((view + 2) % replica_count) }),
            .B3 => array
                .append_assume_capacity(.{ .replica = @intCast((view + 3) % replica_count) }),
            .B4 => array
                .append_assume_capacity(.{ .replica = @intCast((view + 4) % replica_count) }),
            .B5 => array
                .append_assume_capacity(.{ .replica = @intCast((view + 5) % replica_count) }),
            .C0 => array.append_assume_capacity(.{ .client = t.cluster.clients[0].id }),
            .__, .R_, .S_, .C_ => {
                if (selector == .__ or selector == .R_) {
                    for (t.cluster.replicas[0..replica_count], 0..) |_, i| {
                        array.append_assume_capacity(.{ .replica = @intCast(i) });
                    }
                }
                if (selector == .__ or selector == .S_) {
                    for (t.cluster.replicas[replica_count..], 0..) |_, i| {
                        array.append_assume_capacity(.{ .replica = @intCast(replica_count + i) });
                    }
                }
                if (selector == .__ or selector == .C_) {
                    for (t.cluster.clients) |*client| {
                        array.append_assume_capacity(.{ .client = client.id });
                    }
                }
            },
        }
        assert(array.count() > 0);
        return array;
    }
};

const TestReplicas = struct {
    context: *TestContext,
    cluster: *Cluster,
    replicas: stdx.BoundedArrayType(u8, constants.members_max),

    pub fn stop(t: *const TestReplicas) void {
        for (t.replicas.const_slice()) |r| {
            log.info("{}: crash replica", .{r});
            t.cluster.replica_crash(r);

            // For simplicity, ensure that any packets that are in flight to this replica are
            // discarded before it starts up again.
            const paths = t.peer_paths(.__, .incoming);
            for (paths.const_slice()) |path| {
                t.cluster.network.link_clear(path);
            }
        }
    }

    pub fn open(t: *const TestReplicas) !void {
        for (t.replicas.const_slice()) |r| {
            log.info("{}: restart replica", .{r});
            t.cluster.replica_restart(
                r,
                t.cluster.replicas[r].releases_bundled,
            ) catch |err| {
                assert(t.replicas.count() == 1);
                return switch (err) {
                    error.WALCorrupt => return error.WALCorrupt,
                    error.WALInvalid => return error.WALInvalid,
                    else => @panic("unexpected error"),
                };
            };
        }
    }

    pub fn open_upgrade(t: *const TestReplicas, releases_bundled_patch: []const u8) !void {
        var releases_bundled = vsr.ReleaseList{};
        for (releases_bundled_patch) |patch| {
            releases_bundled.append_assume_capacity(vsr.Release.from(.{
                .major = 0,
                .minor = 0,
                .patch = patch,
            }));
        }

        for (t.replicas.const_slice()) |r| {
            log.info("{}: restart replica", .{r});
            t.cluster.replica_restart(r, &releases_bundled) catch |err| {
                assert(t.replicas.count() == 1);
                return switch (err) {
                    error.WALCorrupt => return error.WALCorrupt,
                    error.WALInvalid => return error.WALInvalid,
                    else => @panic("unexpected error"),
                };
            };
        }
    }

    pub fn index(t: *const TestReplicas) u8 {
        assert(t.replicas.count() == 1);
        return t.replicas.get(0);
    }

    const Health = enum { up, down };

    pub fn health(t: *const TestReplicas) Health {
        var value_all: ?Health = null;
        for (t.replicas.const_slice()) |r| {
            const value: Health = switch (t.cluster.replica_health[r]) {
                .up => .up,
                .down => .down,
            };
            if (value_all) |all| {
                assert(all == value);
            } else {
                value_all = value;
            }
        }
        return value_all.?;
    }

    fn get(
        t: *const TestReplicas,
        comptime field: std.meta.FieldEnum(Cluster.Replica),
    ) std.meta.fieldInfo(Cluster.Replica, field).type {
        var value_all: ?std.meta.fieldInfo(Cluster.Replica, field).type = null;
        for (t.replicas.const_slice()) |r| {
            const replica = &t.cluster.replicas[r];
            const value = @field(replica, @tagName(field));
            if (value_all) |all| {
                if (all != value) {
                    for (t.replicas.const_slice()) |replica_index| {
                        log.err("replica={} field={s} value={}", .{
                            replica_index,
                            @tagName(field),
                            @field(&t.cluster.replicas[replica_index], @tagName(field)),
                        });
                    }
                    @panic("test failed: value mismatch");
                }
            } else {
                value_all = value;
            }
        }
        return value_all.?;
    }

    pub fn release(t: *const TestReplicas) u16 {
        var value_all: ?u16 = null;
        for (t.replicas.const_slice()) |r| {
            const value = t.cluster.replicas[r].release.triple().patch;
            if (value_all) |all| {
                assert(all == value);
            } else {
                value_all = value;
            }
        }
        return value_all.?;
    }

    pub fn status(t: *const TestReplicas) vsr.Status {
        return t.get(.status);
    }

    pub fn view(t: *const TestReplicas) u32 {
        return t.get(.view);
    }

    pub fn log_view(t: *const TestReplicas) u32 {
        return t.get(.log_view);
    }

    pub fn op_head(t: *const TestReplicas) u64 {
        return t.get(.op);
    }

    pub fn commit(t: *const TestReplicas) u64 {
        return t.get(.commit_min);
    }

    pub fn commit_max(t: *const TestReplicas) u64 {
        return t.get(.commit_max);
    }

    pub fn state_machine_opened(t: *const TestReplicas) bool {
        return t.get(.state_machine_opened);
    }

    fn sync_stage(t: *const TestReplicas) vsr.SyncStage {
        assert(t.replicas.count() > 0);

        var sync_stage_all: ?vsr.SyncStage = null;
        for (t.replicas.const_slice()) |r| {
            const replica = &t.cluster.replicas[r];
            if (sync_stage_all) |all| {
                assert(std.meta.eql(all, replica.syncing));
            } else {
                sync_stage_all = replica.syncing;
            }
        }
        return sync_stage_all.?;
    }

    pub fn sync_status(t: *const TestReplicas) std.meta.Tag(vsr.SyncStage) {
        return @as(std.meta.Tag(vsr.SyncStage), t.sync_stage());
    }

    fn sync_target(t: *const TestReplicas) ?vsr.SyncTarget {
        return t.sync_stage().target();
    }

    pub fn sync_target_checkpoint_op(t: *const TestReplicas) ?u64 {
        if (t.sync_target()) |target| {
            return target.checkpoint_op;
        } else {
            return null;
        }
    }

    pub fn sync_target_checkpoint_id(t: *const TestReplicas) ?u128 {
        if (t.sync_target()) |target| {
            return target.checkpoint_id;
        } else {
            return null;
        }
    }

    const Role = enum { primary, backup, standby };

    pub fn role(t: *const TestReplicas) Role {
        var role_all: ?Role = null;
        for (t.replicas.const_slice()) |r| {
            const replica = &t.cluster.replicas[r];
            const replica_role: Role = role: {
                if (replica.standby()) {
                    break :role .standby;
                } else if (replica.replica == replica.primary_index(replica.view)) {
                    break :role .primary;
                } else {
                    break :role .backup;
                }
            };
            assert(role_all == null or role_all.? == replica_role);
            role_all = replica_role;
        }
        return role_all.?;
    }

    pub fn op_checkpoint_id(t: *const TestReplicas) u128 {
        var checkpoint_id_all: ?u128 = null;
        for (t.replicas.const_slice()) |r| {
            const replica = &t.cluster.replicas[r];
            const replica_checkpoint_id = replica.superblock.working.checkpoint_id();
            assert(checkpoint_id_all == null or checkpoint_id_all.? == replica_checkpoint_id);
            checkpoint_id_all = replica_checkpoint_id;
        }
        return checkpoint_id_all.?;
    }

    pub fn op_checkpoint(t: *const TestReplicas) u64 {
        var checkpoint_all: ?u64 = null;
        for (t.replicas.const_slice()) |r| {
            const replica = &t.cluster.replicas[r];
            assert(checkpoint_all == null or checkpoint_all.? == replica.op_checkpoint());
            checkpoint_all = replica.op_checkpoint();
        }
        return checkpoint_all.?;
    }

    pub fn corrupt(
        t: *const TestReplicas,
        target: union(enum) {
            wal_header: usize, // slot
            wal_prepare: usize, // slot
            client_reply: usize, // slot
            grid_block: u64, // address
        },
    ) void {
        switch (target) {
            .wal_header => |slot| {
                const fault_offset = vsr.Zone.wal_headers.offset(slot * @sizeOf(vsr.Header));
                for (t.replicas.const_slice()) |r| {
                    t.cluster.storages[r].memory[fault_offset] +%= 1;
                }
            },
            .wal_prepare => |slot| {
                const fault_offset = vsr.Zone.wal_prepares.offset(slot *
                    constants.message_size_max);
                const fault_sector = @divExact(fault_offset, constants.sector_size);
                for (t.replicas.const_slice()) |r| {
                    t.cluster.storages[r].faults.set(fault_sector);
                }
            },
            .client_reply => |slot| {
                const fault_offset = vsr.Zone.client_replies.offset(slot *
                    constants.message_size_max);
                const fault_sector = @divExact(fault_offset, constants.sector_size);
                for (t.replicas.const_slice()) |r| {
                    t.cluster.storages[r].faults.set(fault_sector);
                }
            },
            .grid_block => |address| {
                const fault_offset = vsr.Zone.grid.offset((address - 1) * constants.block_size);
                const fault_sector = @divExact(fault_offset, constants.sector_size);
                for (t.replicas.const_slice()) |r| {
                    t.cluster.storages[r].faults.set(fault_sector);
                }
            },
        }
    }

    pub const LinkDirection = enum { bidirectional, incoming, outgoing };

    pub fn pass_all(t: *const TestReplicas, peer: ProcessSelector, direction: LinkDirection) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.const_slice()) |path| {
            t.cluster.network.link_filter(path).* = LinkFilter.initFull();
        }
    }

    pub fn drop_all(t: *const TestReplicas, peer: ProcessSelector, direction: LinkDirection) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.const_slice()) |path| t.cluster.network.link_filter(path).* = LinkFilter{};
    }

    pub fn pass(
        t: *const TestReplicas,
        peer: ProcessSelector,
        direction: LinkDirection,
        command: vsr.Command,
    ) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.const_slice()) |path| t.cluster.network.link_filter(path).insert(command);
    }

    pub fn drop(
        t: *const TestReplicas,
        peer: ProcessSelector,
        direction: LinkDirection,
        command: vsr.Command,
    ) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.const_slice()) |path| t.cluster.network.link_filter(path).remove(command);
    }

    pub fn filter(
        t: *const TestReplicas,
        peer: ProcessSelector,
        direction: LinkDirection,
        comptime drop_message_fn: ?fn (message: *Message) bool,
    ) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.const_slice()) |path| {
            t.cluster.network.link_drop_packet_fn(path).* = if (drop_message_fn) |f|
                &struct {
                    fn drop_packet(packet: *const Network.Packet) bool {
                        return f(packet.message);
                    }
                }.drop_packet
            else
                null;
        }
    }

    pub fn record(
        t: *const TestReplicas,
        peer: ProcessSelector,
        direction: LinkDirection,
        command: vsr.Command,
    ) void {
        const paths = t.peer_paths(peer, direction);
        for (paths.const_slice()) |path| t.cluster.network.link_record(path).insert(command);
    }

    pub fn replay_recorded(
        t: *const TestReplicas,
    ) void {
        t.cluster.network.replay_recorded();
    }

    // -1: no route to self.
    const paths_max = constants.members_max * (constants.members_max - 1 + constants.clients_max);

    fn peer_paths(
        t: *const TestReplicas,
        peer: ProcessSelector,
        direction: LinkDirection,
    ) stdx.BoundedArrayType(Network.Path, paths_max) {
        var paths = stdx.BoundedArrayType(Network.Path, paths_max){};
        const peers = t.context.processes(peer);
        for (t.replicas.const_slice()) |a| {
            const process_a = Process{ .replica = a };
            for (peers.const_slice()) |process_b| {
                if (direction == .bidirectional or direction == .outgoing) {
                    paths.append_assume_capacity(.{ .source = process_a, .target = process_b });
                }
                if (direction == .bidirectional or direction == .incoming) {
                    paths.append_assume_capacity(.{ .source = process_b, .target = process_a });
                }
            }
        }
        return paths;
    }

    fn expect_sync_done(t: TestReplicas) !void {
        assert(t.replicas.count() > 0);

        for (t.replicas.const_slice()) |replica_index| {
            const replica: *const Cluster.Replica = &t.cluster.replicas[replica_index];
            if (!replica.sync_content_done()) return error.SyncContentPending;

            // If the replica has finished syncing, but not yet checkpointed, then it might not have
            // updated its sync_op_max.
            maybe(replica.superblock.staging.vsr_state.sync_op_max > 0);

            try t.cluster.storage_checker.replica_sync(Cluster.Replica, replica);
        }
    }

    fn expect_equal_grid(want: TestReplicas, got: TestReplicas) !void {
        assert(want.replicas.count() == 1);
        assert(got.replicas.count() > 0);

        const want_replica: *const Cluster.Replica = &want.cluster.replicas[want.replicas.get(0)];

        for (got.replicas.const_slice()) |replica_index| {
            const got_replica: *const Cluster.Replica = &got.cluster.replicas[replica_index];

            const address_max = want.context.block_address_max();
            var address: u64 = 1;
            while (address <= address_max) : (address += 1) {
                const address_free = want_replica.grid.free_set.is_free(address);
                assert(address_free == got_replica.grid.free_set.is_free(address));
                if (address_free) continue;

                const block_want = want_replica.superblock.storage.grid_block(address).?;
                const block_got = got_replica.superblock.storage.grid_block(address).?;

                try expectEqual(
                    std.mem.bytesToValue(vsr.Header, block_want[0..@sizeOf(vsr.Header)]),
                    std.mem.bytesToValue(vsr.Header, block_got[0..@sizeOf(vsr.Header)]),
                );
            }
        }
    }
};

const TestClients = struct {
    context: *TestContext,
    cluster: *Cluster,
    clients: stdx.BoundedArrayType(usize, constants.clients_max),
    requests: usize = 0,

    pub fn request(t: *TestClients, requests: usize, expect_replies: usize) !void {
        assert(t.requests <= requests);
        defer assert(t.requests == requests);

        outer: while (true) {
            for (t.clients.const_slice()) |c| {
                if (t.requests == requests) break :outer;
                t.context.client_requests[c] += 1;
                t.requests += 1;
            }
        }

        const tick_max = 3_000;
        var tick: usize = 0;
        while (tick < tick_max) : (tick += 1) {
            if (t.context.tick()) tick = 0;

            for (t.clients.const_slice()) |c| {
                const client = &t.cluster.clients[c];
                if (client.request_inflight == null and
                    t.context.client_requests[c] > client.request_number)
                {
                    if (client.request_number == 0) {
                        t.cluster.register(c);
                    } else {
                        const message = client.get_message();
                        errdefer client.release_message(message);

                        const body_size = 123;
                        @memset(message.buffer[@sizeOf(vsr.Header)..][0..body_size], 42);
                        t.cluster.request(c, .echo, message, body_size);
                    }
                }
            }
        }
        try std.testing.expectEqual(t.replies(), expect_replies);
    }

    pub fn replies(t: *const TestClients) usize {
        var replies_total: usize = 0;
        for (t.clients.const_slice()) |c| replies_total += t.context.client_replies[c];
        return replies_total;
    }

    pub fn eviction_reason(t: *const TestClients) ?vsr.Header.Eviction.Reason {
        var evicted_all: ?vsr.Header.Eviction.Reason = null;
        for (t.clients.const_slice(), 0..) |r, i| {
            const client_eviction_reason = t.cluster.client_eviction_reasons[r];
            if (i == 0) {
                assert(evicted_all == null);
            } else {
                assert(evicted_all == client_eviction_reason);
            }
            evicted_all = client_eviction_reason;
        }
        return evicted_all;
    }
};

/// TestClientBus supports tests which require fine-grained control of the client protocol.
/// Note that in particular, TestClientBus does *not* implement message retries.
const TestClientBus = struct {
    const MessagePool = @import("../message_pool.zig").MessagePool;
    const MessageBus = Cluster.MessageBus;

    context: *TestContext,
    client_id: u128,
    message_pool: *MessagePool,
    message_bus: MessageBus,
    reply: ?*Message = null,

    fn init(context: *TestContext, client_id: u128) !*TestClientBus {
        const message_pool = try allocator.create(MessagePool);
        errdefer allocator.destroy(message_pool);

        message_pool.* = try MessagePool.init(allocator, .client);
        errdefer message_pool.deinit(allocator);

        var client_bus = try allocator.create(TestClientBus);
        errdefer allocator.destroy(client_bus);

        client_bus.* = .{
            .context = context,
            .client_id = client_id,
            .message_pool = message_pool,
            .message_bus = try MessageBus.init(
                allocator,
                context.cluster.options.cluster_id,
                .{ .client = client_id },
                message_pool,
                on_message,
                .{ .network = context.cluster.network },
            ),
        };
        errdefer client_bus.message_bus.deinit(allocator);

        context.cluster.state_checker.clients_exhaustive = false;
        context.cluster.network.link(client_bus.message_bus.process, &client_bus.message_bus);

        return client_bus;
    }

    pub fn deinit(t: *TestClientBus) void {
        if (t.reply) |reply| {
            t.message_pool.unref(reply);
            t.reply = null;
        }
        t.message_bus.deinit(allocator);
        t.message_pool.deinit(allocator);
        allocator.destroy(t.message_pool);
        allocator.destroy(t);
    }

    fn on_message(message_bus: *Cluster.MessageBus, message: *Message) void {
        const t: *TestClientBus = @fieldParentPtr("message_bus", message_bus);
        assert(message.header.cluster == t.context.cluster.options.cluster_id);

        switch (message.header.command) {
            .reply => {
                assert(t.reply == null);
                t.reply = message.ref();
            },
            .pong_client => {},
            else => unreachable,
        }
    }

    pub fn request(
        t: *TestClientBus,
        replica: u8,
        header: *const vsr.Header.Request,
        body: []const u8,
    ) void {
        assert(replica < t.context.cluster.replicas.len);
        assert(body.len <= constants.message_body_size_max);

        const message = t.message_pool.get_message(.request);
        defer t.message_pool.unref(message);

        message.header.* = header.*;
        stdx.copy_disjoint(.inexact, u8, message.buffer[@sizeOf(vsr.Header)..], body);

        t.message_bus.send_message_to_replica(replica, message.base());
    }
};
