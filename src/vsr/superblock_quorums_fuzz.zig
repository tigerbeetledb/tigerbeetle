const std = @import("std");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const vsr = @import("../vsr.zig");

const superblock = @import("./superblock.zig");
const SuperBlockHeader = superblock.SuperBlockHeader;
const SuperBlockVersion = superblock.SuperBlockVersion;

const fuzz = @import("../testing/fuzz.zig");
const superblock_quorums = @import("superblock_quorums.zig");
const QuorumsType = superblock_quorums.QuorumsType;

pub fn main(fuzz_args: fuzz.FuzzArgs) !void {
    var prng = std.rand.DefaultPrng.init(fuzz_args.seed);

    // TODO When there is a top-level fuzz.zig main(), split these fuzzers into two different
    // commands.
    try fuzz_quorums_working(prng.random());

    try fuzz_quorum_repairs(prng.random(), .{ .superblock_copies = 4 });
    // TODO: Enable these once SuperBlockHeader is generic over its Constants.
    // try fuzz_quorum_repairs(prng.random(), .{ .superblock_copies = 6 });
    // try fuzz_quorum_repairs(prng.random(), .{ .superblock_copies = 8 });
}

pub fn fuzz_quorums_working(random: std.rand.Random) !void {
    const r = random;
    const t = test_quorums_working;
    const o = CopyTemplate.make_valid;
    const x = CopyTemplate.make_invalid_broken;
    const X = {}; // Ignored; just for text alignment + contrast.

    // No faults:
    try t(r, 2, &.{ o(3), o(3), o(3), o(3) }, 3);
    try t(r, 3, &.{ o(3), o(3), o(3), o(3) }, 3);

    // Single fault:
    try t(r, 3, &.{ x(X), o(4), o(4), o(4) }, 4);
    // Double fault, same quorum:
    try t(r, 2, &.{ x(X), x(X), o(4), o(4) }, 4);
    try t(r, 3, &.{ x(X), x(X), o(4), o(4) }, error.QuorumLost);
    // Double fault, different quorums:
    try t(r, 2, &.{ x(X), x(X), o(3), o(4) }, error.QuorumLost);
    // Triple fault.
    try t(r, 2, &.{ x(X), x(X), x(X), o(4) }, error.QuorumLost);

    // Partial format (broken sequence=1):
    try t(r, 2, &.{ x(X), o(1), o(1), o(1) }, 1);
    try t(r, 3, &.{ x(X), o(1), o(1), o(1) }, 1);
    try t(r, 2, &.{ x(X), x(X), o(1), o(1) }, 1);
    try t(r, 3, &.{ x(X), x(X), o(1), o(1) }, error.QuorumLost);
    try t(r, 2, &.{ x(X), x(X), x(X), o(1) }, error.QuorumLost);
    try t(r, 2, &.{ x(X), x(X), x(X), x(X) }, error.NotFound);

    // Partial checkpoint() to sequence=4 (2 quorums):
    try t(r, 2, &.{ o(3), o(2), o(2), o(2) }, 2); // open after 1/4
    try t(r, 2, &.{ o(3), o(3), o(2), o(2) }, 3); // open after 2/4
    try t(r, 2, &.{ o(3), o(3), o(3), o(2) }, 3); // open after 3/4
    // Partial checkpoint() to sequence=4 (3 quorums):
    try t(r, 2, &.{ o(1), o(2), o(3), o(3) }, 3);
    try t(r, 3, &.{ o(1), o(2), o(3), o(3) }, error.QuorumLost);

    // Skipped sequence.
    try t(r, 2, &.{ o(2), o(2), o(2), o(4) }, error.ParentSkipped); // open after 1/4
    try t(r, 2, &.{ o(2), o(2), o(4), o(4) }, 4); // open after 2/4
    try t(r, 2, &.{ o(2), o(2), o(4), o(4) }, 4); // open after 3/4

    // Forked sequence: same sequence number, different checksum, both valid.
    const f = CopyTemplate.make_invalid_fork;
    try t(r, 2, &.{ o(3), o(3), o(3), f(3) }, error.Fork);
    try t(r, 2, &.{ o(3), o(3), f(3), f(3) }, error.Fork);

    // Parent has wrong cluster|replica.
    const m = CopyTemplate.make_invalid_misdirect;
    try t(r, 2, &.{ m(2), m(2), m(2), o(3) }, 2);
    try t(r, 2, &.{ m(2), m(2), o(3), o(3) }, 3);
    try t(r, 2, &.{ m(2), o(3), o(3), o(3) }, 3);
    // Grandparent has wrong cluster|replica.
    try t(r, 2, &.{ m(2), m(2), m(2), o(4) }, 2);
    try t(r, 2, &.{ m(2), m(2), o(4), o(4) }, 4);
    try t(r, 2, &.{ m(2), o(4), o(4), o(4) }, 4);

    // Parent/child hash chain is broken.
    const p = CopyTemplate.make_invalid_parent;
    try t(r, 2, &.{ o(2), o(2), o(2), p(3) }, 2);
    try t(r, 2, &.{ o(2), o(2), p(3), p(3) }, error.ParentNotConnected);
    try t(r, 2, &.{ o(2), p(3), p(3), p(3) }, error.ParentNotConnected);
    try t(r, 2, &.{ p(3), p(3), p(3), p(3) }, 3);

    // Parent view is greater than child view.
    const v = CopyTemplate.make_invalid_vsr_state;
    try t(r, 2, &.{ v(2), v(2), o(3), o(3) }, error.VSRStateNotMonotonic);

    // A member of the quorum has an "invalid" copy, but an otherwise valid checksum.
    const h = CopyTemplate.make_valid_high_copy;
    try t(r, 2, &.{ o(2), o(2), o(3), h(3) }, 3);
}

fn test_quorums_working(
    random: std.rand.Random,
    threshold_count: u8,
    initial_copies: *const [4]CopyTemplate,
    result: QuorumsType(.{ .superblock_copies = 4 }).Error!u64,
) !void {
    const Quorums = QuorumsType(.{ .superblock_copies = 4 });
    const misdirect = random.boolean(); // true:cluster false:replica
    var quorums: Quorums = undefined;
    var headers: [4]SuperBlockHeader = undefined;
    var checksums: [6]u128 = undefined;
    for (&checksums) |*c| c.* = random.int(u128);

    var members = [_]u128{0} ** constants.members_max;
    for (members[0..6]) |*member| {
        member.* = random.int(u128);
    }

    // Create headers in ascending-sequence order to build the checksum/parent hash chain.
    var initial_templates = initial_copies.*;
    const copies = &initial_templates;
    std.mem.sort(CopyTemplate, copies, {}, CopyTemplate.less_than);

    for (&headers, 0..) |*header, i| {
        header.* = std.mem.zeroInit(SuperBlockHeader, .{
            .copy = @as(u8, @intCast(i)),
            .version = SuperBlockVersion,
            .release_format = vsr.Release.minimum,
            .sequence = copies[i].sequence,
            .parent = checksums[copies[i].sequence - 1],
            .vsr_state = std.mem.zeroInit(SuperBlockHeader.VSRState, .{
                .replica_id = members[1],
                .members = members,
                .replica_count = 6,
                .commit_max = 123,
                .checkpoint = std.mem.zeroInit(SuperBlockHeader.CheckpointState, .{
                    .header = header: {
                        var checkpoint_header = vsr.Header.Prepare.root(0);
                        checkpoint_header.op = 123;
                        checkpoint_header.set_checksum();
                        break :header checkpoint_header;
                    },
                    .free_set_blocks_acquired_checksum = vsr.checksum(&.{}),
                    .free_set_blocks_released_checksum = vsr.checksum(&.{}),
                    .client_sessions_checksum = vsr.checksum(&.{}),
                    .storage_size = superblock.data_file_size_min,
                }),
            }),
        });

        var checksum: ?u128 = null;
        switch (copies[i].variant) {
            .valid => {},
            .valid_high_copy => header.copy = 4,
            .invalid_broken => {
                if (random.boolean() and i > 0) {
                    // Error: duplicate header (if available).
                    header.* = headers[random.uintLessThanBiased(usize, i)];
                    checksum = random.int(u128);
                } else {
                    // Error: invalid checksum.
                    checksum = random.int(u128);
                }
            },
            // Ensure we have a different checksum.
            .invalid_fork => header.vsr_state.checkpoint.free_set_blocks_acquired_size += 1,
            .invalid_parent => header.parent += 1,
            .invalid_misdirect => {
                if (misdirect) {
                    header.cluster += 1;
                } else {
                    header.vsr_state.replica_id += 1;
                }
            },
            .invalid_vsr_state => header.vsr_state.view += 1,
        }
        header.checksum = checksum orelse header.calculate_checksum();

        if (copies[i].variant == .valid or copies[i].variant == .invalid_vsr_state) {
            checksums[header.sequence] = header.checksum;
        }
    }

    for (copies) |template| {
        if (template.variant == .valid_high_copy) break;
    } else {
        // Shuffling copies can only change the working quorum when we have a corrupt copy index,
        // because we guess that the true index is the slot.
        random.shuffle(SuperBlockHeader, &headers);
    }

    const threshold = switch (threshold_count) {
        2 => Quorums.Threshold.open,
        3 => Quorums.Threshold.verify,
        else => unreachable,
    };
    assert(threshold.count() == threshold_count);

    if (quorums.working(&headers, threshold)) |working| {
        try std.testing.expectEqual(result, working.header.sequence);
    } else |err| {
        try std.testing.expectEqual(result, err);
    }
}

pub const CopyTemplate = struct {
    sequence: u64,
    variant: Variant,

    const Variant = enum {
        valid,
        valid_high_copy,
        invalid_broken,
        invalid_fork,
        invalid_misdirect,
        invalid_parent,
        invalid_vsr_state,
    };

    pub fn make_valid(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .valid };
    }

    /// Construct a copy with a corrupt copy index (≥superblock_copies).
    pub fn make_valid_high_copy(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .valid_high_copy };
    }

    /// Construct a corrupt (invalid checksum) or duplicate copy copy.
    pub fn make_invalid_broken(_: void) CopyTemplate {
        // Use a high sequence so that invalid copies are the last generated by
        // test_quorums_working(), so that they can become duplicates of (earlier) valid copies.
        return .{ .sequence = 6, .variant = .invalid_broken };
    }

    /// Construct a copy with a valid checksum — but which differs from the "canonical" version
    /// of this sequence.
    pub fn make_invalid_fork(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .invalid_fork };
    }

    /// Construct a copy with either an incorrect "cluster" or "replica".
    pub fn make_invalid_misdirect(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .invalid_misdirect };
    }

    /// Construct a copy with an invalid "parent" checksum.
    pub fn make_invalid_parent(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .invalid_parent };
    }

    /// Construct a copy with a newer `VSRState` than its parent.
    pub fn make_invalid_vsr_state(sequence: u64) CopyTemplate {
        return .{ .sequence = sequence, .variant = .invalid_vsr_state };
    }

    fn less_than(_: void, a: CopyTemplate, b: CopyTemplate) bool {
        return a.sequence < b.sequence;
    }
};

// Verify that a torn header write during repair never compromises the existing quorum.
pub fn fuzz_quorum_repairs(
    random: std.rand.Random,
    comptime options: superblock_quorums.Options,
) !void {
    const superblock_copies = options.superblock_copies;
    const Quorums = QuorumsType(options);

    var q1: Quorums = undefined;
    var q2: Quorums = undefined;

    var members = [_]u128{0} ** constants.members_max;
    for (members[0..6]) |*member| {
        member.* = random.int(u128);
    }

    const headers_valid = blk: {
        var headers: [superblock_copies]SuperBlockHeader = undefined;
        for (&headers, 0..) |*header, i| {
            header.* = std.mem.zeroInit(SuperBlockHeader, .{
                .copy = @as(u8, @intCast(i)),
                .version = SuperBlockVersion,
                .release_format = vsr.Release.minimum,
                .sequence = 123,
                .vsr_state = std.mem.zeroInit(SuperBlockHeader.VSRState, .{
                    .replica_id = members[1],
                    .members = members,
                    .replica_count = 6,
                    .checkpoint = std.mem.zeroInit(SuperBlockHeader.CheckpointState, .{
                        .header = header: {
                            var checkpoint_header = vsr.Header.Prepare.root(0);
                            checkpoint_header.op = 123;
                            checkpoint_header.set_checksum();
                            break :header checkpoint_header;
                        },
                    }),
                }),
            });

            header.set_checksum();
        }
        break :blk headers;
    };

    const header_invalid = blk: {
        var header = headers_valid[0];
        header.checksum = 456;
        break :blk header;
    };

    // Generate a random valid 2/4 quorum.
    // 1 bits indicate valid headers.
    // 0 bits indicate invalid headers.
    var valid = std.bit_set.IntegerBitSet(superblock_copies).initEmpty();
    while (valid.count() < Quorums.Threshold.open.count() or random.boolean()) {
        valid.set(random.uintLessThan(usize, superblock_copies));
    }

    var working_headers: [superblock_copies]SuperBlockHeader = undefined;
    for (&working_headers, 0..) |*header, i| {
        header.* = if (valid.isSet(i)) headers_valid[i] else header_invalid;
    }
    random.shuffle(SuperBlockHeader, &working_headers);
    var repair_headers = working_headers;

    const working_quorum = q1.working(&working_headers, .open) catch unreachable;
    var quorum_repairs = working_quorum.repairs();
    while (quorum_repairs.next()) |repair_copy| {
        {
            // Simulate a torn header write, crash, recover sequence.
            var damaged_headers = repair_headers;
            damaged_headers[repair_copy] = header_invalid;
            const damaged_quorum = q2.working(&damaged_headers, .open) catch unreachable;
            assert(damaged_quorum.header.checksum == working_quorum.header.checksum);
        }

        // "Finish" the write so that we can test the next repair.
        repair_headers[repair_copy] = headers_valid[repair_copy];

        const quorum_repaired = q2.working(&repair_headers, .open) catch unreachable;
        assert(quorum_repaired.header.checksum == working_quorum.header.checksum);
    }

    // At the end of all repairs, we expect to have every copy of the superblock.
    // They do not need to be in their home slot.
    var copies = Quorums.QuorumCount.initEmpty();
    for (repair_headers) |repair_header| {
        assert(repair_header.checksum == working_quorum.header.checksum);
        copies.set(repair_header.copy);
    }
    assert(repair_headers.len == copies.count());
}
