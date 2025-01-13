//! SuperBlock invariants:
//!
//!   * vsr_state
//!     - vsr_state.replica and vsr_state.replica_count are immutable for now.
//!     - vsr_state.checkpoint.header.op is initially 0 (for a newly-formatted replica).
//!     - vsr_state.checkpoint.header.op ≤ vsr_state.commit_max
//!     - vsr_state.checkpoint.header.op_before ≤ vsr_state.checkpoint.header.op
//!     - vsr_state.log_view ≤ vsr_state.view
//!     - vsr_state.sync_op_min ≤ vsr_state.sync_op_max
//!
//!     - vsr_state.checkpoint.manifest_block_count = 0 implies:
//!       vsr_state.checkpoint.manifest_oldest_address=0
//!       vsr_state.checkpoint.manifest_oldest_checksum=0
//!       vsr_state.checkpoint.manifest_newest_address=0
//!       vsr_state.checkpoint.manifest_newest_checksum=0
//!       vsr_state.checkpoint.manifest_oldest_address=0
//!
//!     - vsr_state.checkpoint.manifest_block_count > 0 implies:
//!       vsr_state.checkpoint.manifest_oldest_address>0
//!       vsr_state.checkpoint.manifest_newest_address>0
//!
//!     - checkpoint() must advance the superblock's vsr_state.checkpoint.header.op.
//!     - view_change() must not advance the superblock's vsr_state.checkpoint.header.op.
//!     - The following are monotonically increasing:
//!       - vsr_state.log_view
//!       - vsr_state.view
//!       - vsr_state.commit_max
//!     - vsr_state.checkpoint.header.op may backtrack due to state sync.
//!
const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const meta = std.meta;

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");
const vsr = @import("../vsr.zig");
const log = std.log.scoped(.superblock);

pub const Quorums = @import("superblock_quorums.zig").QuorumsType(.{
    .superblock_copies = constants.superblock_copies,
});

pub const SuperBlockVersion: u16 =
    // Make sure that data files created by development builds are distinguished through version.
    if (constants.config.process.release.value == vsr.Release.minimum.value) 1 else 2;

const vsr_headers_reserved_size = constants.sector_size -
    ((constants.view_change_headers_max * @sizeOf(vsr.Header)) % constants.sector_size);

// Fields are aligned to work as an extern or packed struct.
pub const SuperBlockHeader = extern struct {
    checksum: u128 = undefined,
    checksum_padding: u128 = 0,

    /// Protects against misdirected reads at startup.
    /// For example, if multiple reads are all misdirected to a single copy of the superblock.
    /// Excluded from the checksum calculation to ensure that all copies have the same checksum.
    /// This simplifies writing and comparing multiple copies.
    /// TODO: u8 should be enough here, we use u16 only for alignment.
    copy: u16 = 0,

    /// The version of the superblock format in use, reserved for major breaking changes.
    version: u16,

    /// The release that the data file was originally formatted by.
    /// (Upgrades do not update this field.)
    release_format: vsr.Release,

    /// A monotonically increasing counter to locate the latest superblock at startup.
    sequence: u64,

    /// Protects against writing to or reading from the wrong data file.
    cluster: u128,

    /// The checksum of the previous superblock to hash chain across sequence numbers.
    parent: u128,
    parent_padding: u128 = 0,

    /// State stored on stable storage for the Viewstamped Replication consensus protocol.
    vsr_state: VSRState,

    /// Reserved for future minor features (e.g. changing a compression algorithm).
    flags: u64 = 0,

    /// The number of headers in vsr_headers_all.
    vsr_headers_count: u32,

    reserved: [1940]u8 = [_]u8{0} ** 1940,

    /// SV/DVC header suffix. Headers are ordered from high-to-low op.
    /// Unoccupied headers (after vsr_headers_count) are zeroed.
    ///
    /// When `vsr_state.log_view < vsr_state.view`, the headers are for a DVC.
    /// When `vsr_state.log_view = vsr_state.view`, the headers are for a SV.
    vsr_headers_all: [constants.view_change_headers_max]vsr.Header.Prepare,
    vsr_headers_reserved: [vsr_headers_reserved_size]u8 =
        [_]u8{0} ** vsr_headers_reserved_size,

    comptime {
        assert(@sizeOf(SuperBlockHeader) % constants.sector_size == 0);
        assert(@divExact(@sizeOf(SuperBlockHeader), constants.sector_size) >= 2);
        assert(@offsetOf(SuperBlockHeader, "parent") % @sizeOf(u256) == 0);
        assert(@offsetOf(SuperBlockHeader, "vsr_state") % @sizeOf(u256) == 0);
        assert(@offsetOf(SuperBlockHeader, "vsr_headers_all") == constants.sector_size);
        // Assert that there is no implicit padding in the struct.
        assert(stdx.no_padding(SuperBlockHeader));
    }

    pub const VSRState = extern struct {
        checkpoint: CheckpointState,

        /// Globally unique identifier of the replica, must be non-zero.
        replica_id: u128,

        members: vsr.Members,

        /// The highest operation up to which we may commit.
        commit_max: u64,

        /// See `sync_op_max`.
        sync_op_min: u64,

        /// When zero, all of the grid blocks and replies are synced.
        /// (When zero, `sync_op_min` is also zero.)
        ///
        /// When nonzero, we must repair grid-blocks/client-replies that would have been written
        /// during the commits between `sync_op_min` and `sync_op_max` (inclusive).
        /// (Those grid-blocks and client-replies were not written normally because we "skipped"
        /// past them via state sync.)
        sync_op_max: u64,

        /// This field was used by the old state sync protocol, but is now unused and is always set
        /// to zero.
        /// TODO: rename to reserved and assert that it is zero, once it is actually set to zero
        /// in all superblocks (in the next release).
        sync_view: u32 = 0,

        /// The last view in which the replica's status was normal.
        log_view: u32,

        /// The view number of the replica.
        view: u32,

        /// Number of replicas (determines sizes of the quorums), part of VSR configuration.
        replica_count: u8,

        reserved: [779]u8 = [_]u8{0} ** 779,

        comptime {
            assert(@sizeOf(VSRState) == 2048);
            // Assert that there is no implicit padding in the struct.
            assert(stdx.no_padding(VSRState));
        }

        pub fn root(options: struct {
            cluster: u128,
            replica_id: u128,
            members: vsr.Members,
            replica_count: u8,
            release: vsr.Release,
        }) VSRState {
            return .{
                .checkpoint = .{
                    .header = vsr.Header.Prepare.root(options.cluster),
                    .parent_checkpoint_id = 0,
                    .grandparent_checkpoint_id = 0,
                    .free_set_blocks_acquired_checksum = comptime vsr.checksum(&.{}),
                    .free_set_blocks_released_checksum = comptime vsr.checksum(&.{}),
                    .free_set_blocks_acquired_last_block_checksum = 0,
                    .free_set_blocks_released_last_block_checksum = 0,
                    .free_set_blocks_acquired_last_block_address = 0,
                    .free_set_blocks_released_last_block_address = 0,
                    .free_set_blocks_acquired_size = 0,
                    .free_set_blocks_released_size = 0,
                    .client_sessions_checksum = comptime vsr.checksum(&.{}),
                    .client_sessions_last_block_checksum = 0,
                    .client_sessions_last_block_address = 0,
                    .client_sessions_size = 0,
                    .manifest_oldest_checksum = 0,
                    .manifest_oldest_address = 0,
                    .manifest_newest_checksum = 0,
                    .manifest_newest_address = 0,
                    .manifest_block_count = 0,
                    .snapshots_block_checksum = 0,
                    .snapshots_block_address = 0,
                    .storage_size = data_file_size_min,
                    .release = options.release,
                },
                .replica_id = options.replica_id,
                .members = options.members,
                .replica_count = options.replica_count,
                .commit_max = 0,
                .sync_op_min = 0,
                .sync_op_max = 0,
                .log_view = 0,
                .view = 0,
            };
        }

        pub fn assert_internally_consistent(state: VSRState) void {
            assert(state.commit_max >= state.checkpoint.header.op);
            assert(state.sync_op_max >= state.sync_op_min);
            assert(state.view >= state.log_view);
            assert(state.replica_count > 0);
            assert(state.replica_count <= constants.replicas_max);
            assert(vsr.member_index(&state.members, state.replica_id) != null);

            // These fields are unused at the moment:
            assert(state.checkpoint.snapshots_block_checksum == 0);
            assert(state.checkpoint.snapshots_block_address == 0);

            assert(state.checkpoint.manifest_oldest_checksum_padding == 0);
            assert(state.checkpoint.manifest_newest_checksum_padding == 0);
            assert(state.checkpoint.snapshots_block_checksum_padding == 0);
            assert(state.checkpoint.free_set_blocks_acquired_last_block_checksum_padding == 0);
            assert(state.checkpoint.free_set_blocks_released_last_block_checksum_padding == 0);

            assert(state.checkpoint.client_sessions_last_block_checksum_padding == 0);
            assert(state.checkpoint.storage_size >= data_file_size_min);

            if (state.checkpoint.free_set_blocks_acquired_last_block_address == 0) {
                assert(state.checkpoint.free_set_blocks_acquired_size == 0);
                assert(state.checkpoint.free_set_blocks_acquired_checksum ==
                    comptime vsr.checksum(&.{}));
                assert(state.checkpoint.free_set_blocks_acquired_last_block_checksum == 0);
            } else {
                assert(state.checkpoint.free_set_blocks_acquired_size > 0);
            }

            if (state.checkpoint.free_set_blocks_released_last_block_address == 0) {
                assert(state.checkpoint.free_set_blocks_released_size == 0);
                assert(state.checkpoint.free_set_blocks_released_checksum ==
                    comptime vsr.checksum(&.{}));
                assert(state.checkpoint.free_set_blocks_released_last_block_checksum == 0);
            } else {
                assert(state.checkpoint.free_set_blocks_released_size > 0);
            }

            if (state.checkpoint.client_sessions_last_block_address == 0) {
                assert(state.checkpoint.client_sessions_last_block_checksum == 0);
                assert(state.checkpoint.client_sessions_size == 0);
                assert(state.checkpoint.client_sessions_checksum == comptime vsr.checksum(&.{}));
            } else {
                assert(state.checkpoint.client_sessions_size == vsr.ClientSessions.encode_size);
            }

            if (state.checkpoint.manifest_block_count == 0) {
                assert(state.checkpoint.manifest_oldest_address == 0);
                assert(state.checkpoint.manifest_newest_address == 0);
                assert(state.checkpoint.manifest_oldest_checksum == 0);
                assert(state.checkpoint.manifest_newest_checksum == 0);
            } else {
                assert(state.checkpoint.manifest_oldest_address != 0);
                assert(state.checkpoint.manifest_newest_address != 0);

                assert((state.checkpoint.manifest_block_count == 1) ==
                    (state.checkpoint.manifest_oldest_address ==
                    state.checkpoint.manifest_newest_address));

                assert((state.checkpoint.manifest_block_count == 1) ==
                    (state.checkpoint.manifest_oldest_checksum ==
                    state.checkpoint.manifest_newest_checksum));
            }
        }

        pub fn monotonic(old: VSRState, new: VSRState) bool {
            old.assert_internally_consistent();
            new.assert_internally_consistent();
            if (old.checkpoint.header.op == new.checkpoint.header.op) {
                if (old.checkpoint.header.checksum == 0 and old.checkpoint.header.op == 0) {
                    // "old" is the root VSRState.
                    assert(old.commit_max == 0);
                    assert(old.sync_op_min == 0);
                    assert(old.sync_op_max == 0);
                    assert(old.log_view == 0);
                    assert(old.view == 0);
                } else {
                    assert(stdx.equal_bytes(CheckpointState, &old.checkpoint, &new.checkpoint));
                }
            } else {
                assert(old.checkpoint.header.checksum != new.checkpoint.header.checksum);
                assert(old.checkpoint.parent_checkpoint_id !=
                    new.checkpoint.parent_checkpoint_id);
            }
            assert(old.replica_id == new.replica_id);
            assert(old.replica_count == new.replica_count);
            assert(stdx.equal_bytes([constants.members_max]u128, &old.members, &new.members));

            if (old.checkpoint.header.op > new.checkpoint.header.op) return false;
            if (old.view > new.view) return false;
            if (old.log_view > new.log_view) return false;
            if (old.commit_max > new.commit_max) return false;

            return true;
        }

        pub fn would_be_updated_by(old: VSRState, new: VSRState) bool {
            assert(monotonic(old, new));

            return !stdx.equal_bytes(VSRState, &old, &new);
        }

        /// Compaction is one bar ahead of superblock's commit_min.
        /// The commits from the bar following commit_min were in the mutable table, and
        /// thus not preserved in the checkpoint.
        /// But the corresponding `compact()` updates were preserved, and must not be repeated
        /// to ensure deterministic storage.
        pub fn op_compacted(state: VSRState, op: u64) bool {
            // If commit_min is 0, we have never checkpointed, so no compactions are checkpointed.
            return state.checkpoint.header.op > 0 and
                op <= vsr.Checkpoint.trigger_for_checkpoint(state.checkpoint.header.op).?;
        }
    };

    /// CheckpointState for SuperBlockVersion=1 (and 0 for development builds).
    ///
    /// We maintain this so replicas with SuperBlockVersion=2 are able to parse older SuperBlocks
    /// (see `read_header_callback` for translation from CheckpointStateOld → CheckpointState).
    pub const CheckpointStateOld = extern struct {
        /// The last prepare of the checkpoint committed to the state machine.
        /// At startup, replay the log hereafter.
        header: vsr.Header.Prepare,

        free_set_last_block_checksum: u128,
        free_set_last_block_checksum_padding: u128 = 0,
        client_sessions_last_block_checksum: u128,
        client_sessions_last_block_checksum_padding: u128 = 0,
        manifest_oldest_checksum: u128,
        manifest_oldest_checksum_padding: u128 = 0,
        manifest_newest_checksum: u128,
        manifest_newest_checksum_padding: u128 = 0,
        snapshots_block_checksum: u128,
        snapshots_block_checksum_padding: u128 = 0,

        /// Checksum covering the entire encoded free set. Strictly speaking it is redundant:
        /// free_set_last_block_checksum indirectly covers the same data. It is still useful
        /// to protect from encoding-decoding bugs as a defense in depth.
        free_set_checksum: u128,

        /// Checksum covering the entire client sessions, as defense-in-depth.
        client_sessions_checksum: u128,

        /// The checkpoint_id() of the checkpoint which last updated our commit_min.
        /// Following state sync, this is set to the last checkpoint that we skipped.
        parent_checkpoint_id: u128,
        /// The parent_checkpoint_id of the parent checkpoint.
        /// TODO We might be able to remove this when
        /// https://github.com/tigerbeetle/tigerbeetle/issues/1378 is fixed.
        grandparent_checkpoint_id: u128,

        free_set_last_block_address: u64,
        client_sessions_last_block_address: u64,
        manifest_oldest_address: u64,
        manifest_newest_address: u64,
        snapshots_block_address: u64,

        // Logical storage size in bytes.
        //
        // If storage_size is less than the data file size, then the grid blocks beyond storage_size
        // were used previously, but have since been freed.
        //
        // If storage_size is more than the data file size, then the data file might have been
        // truncated/corrupted.
        storage_size: u64,

        // Size of the encoded trailers in bytes.
        // It is equal to the sum of sizes of individual trailer blocks and is used for assertions.
        free_set_size: u64,
        client_sessions_size: u64,

        /// The number of manifest blocks in the manifest log.
        manifest_block_count: u32,

        /// All prepares between `CheckpointStateOld.commit_min` (i.e. `op_checkpoint`) and
        /// `trigger_for_checkpoint(checkpoint_after(commit_min))` must be executed by this release.
        /// (Prepares with `operation=upgrade` are the exception – upgrades in the last
        /// `lsm_compaction_ops` before a checkpoint trigger may be replayed by a different release.
        release: vsr.Release,

        reserved: [472]u8 = [_]u8{0} ** 472,

        comptime {
            assert(@sizeOf(CheckpointStateOld) % @sizeOf(u128) == 0);
            assert(@sizeOf(CheckpointStateOld) == 1024);
            assert(stdx.no_padding(CheckpointStateOld));
        }
    };

    /// The content of CheckpointState is deterministic for the corresponding checkpoint.
    ///
    /// This struct is sent in a `start_view` message from the primary to a syncing replica.
    pub const CheckpointState = extern struct {
        /// The last prepare of the checkpoint committed to the state machine.
        /// At startup, replay the log hereafter.
        header: vsr.Header.Prepare,

        free_set_blocks_acquired_last_block_checksum: u128,
        free_set_blocks_acquired_last_block_checksum_padding: u128 = 0,

        free_set_blocks_released_last_block_checksum: u128,
        free_set_blocks_released_last_block_checksum_padding: u128 = 0,

        client_sessions_last_block_checksum: u128,
        client_sessions_last_block_checksum_padding: u128 = 0,
        manifest_oldest_checksum: u128,
        manifest_oldest_checksum_padding: u128 = 0,
        manifest_newest_checksum: u128,
        manifest_newest_checksum_padding: u128 = 0,
        snapshots_block_checksum: u128,
        snapshots_block_checksum_padding: u128 = 0,

        /// Checksum covering the entire encoded free set. Strictly speaking it is redundant:
        /// free_set_last_block_checksum indirectly covers the same data. It is still useful
        /// to protect from encoding-decoding bugs as a defense in depth.
        free_set_blocks_acquired_checksum: u128,
        free_set_blocks_released_checksum: u128,

        /// Checksum covering the entire client sessions, as defense-in-depth.
        client_sessions_checksum: u128,

        /// The checkpoint_id() of the checkpoint which last updated our commit_min.
        /// Following state sync, this is set to the last checkpoint that we skipped.
        parent_checkpoint_id: u128,
        /// The parent_checkpoint_id of the parent checkpoint.
        /// TODO We might be able to remove this when
        /// https://github.com/tigerbeetle/tigerbeetle/issues/1378 is fixed.
        grandparent_checkpoint_id: u128,

        free_set_blocks_acquired_last_block_address: u64,
        free_set_blocks_released_last_block_address: u64,

        client_sessions_last_block_address: u64,
        manifest_oldest_address: u64,
        manifest_newest_address: u64,
        snapshots_block_address: u64,

        // Logical storage size in bytes.
        //
        // If storage_size is less than the data file size, then the grid blocks beyond storage_size
        // were used previously, but have since been freed.
        //
        // If storage_size is more than the data file size, then the data file might have been
        // truncated/corrupted.
        storage_size: u64,

        // Size of the encoded trailers in bytes.
        // It is equal to the sum of sizes of individual trailer blocks and is used for assertions.
        free_set_blocks_acquired_size: u64,
        free_set_blocks_released_size: u64,

        client_sessions_size: u64,

        /// The number of manifest blocks in the manifest log.
        manifest_block_count: u32,

        /// All prepares between `CheckpointState.commit_min` (i.e. `op_checkpoint`) and
        /// `trigger_for_checkpoint(checkpoint_after(commit_min))` must be executed by this release.
        /// (Prepares with `operation=upgrade` are the exception – upgrades in the last
        /// `lsm_compaction_ops` before a checkpoint trigger may be replayed by a different release.
        release: vsr.Release,

        reserved: [408]u8 = [_]u8{0} ** 408,

        comptime {
            assert(@sizeOf(CheckpointState) % @sizeOf(u128) == 0);
            assert(@sizeOf(CheckpointState) == 1024);
            assert(stdx.no_padding(CheckpointState));
        }
    };

    pub fn calculate_checksum(superblock: *const SuperBlockHeader) u128 {
        comptime assert(meta.fieldIndex(SuperBlockHeader, "checksum") == 0);
        comptime assert(meta.fieldIndex(SuperBlockHeader, "checksum_padding") == 1);
        comptime assert(meta.fieldIndex(SuperBlockHeader, "copy") == 2);

        const checksum_size = @sizeOf(@TypeOf(superblock.checksum));
        comptime assert(checksum_size == @sizeOf(u128));

        const checksum_padding_size = @sizeOf(@TypeOf(superblock.checksum_padding));
        comptime assert(checksum_padding_size == @sizeOf(u128));

        const copy_size = @sizeOf(@TypeOf(superblock.copy));
        comptime assert(copy_size == 2);

        const ignore_size = checksum_size + checksum_padding_size + copy_size;

        return vsr.checksum(std.mem.asBytes(superblock)[ignore_size..]);
    }

    pub fn set_checksum(superblock: *SuperBlockHeader) void {
        assert(superblock.copy < constants.superblock_copies);
        assert(superblock.version == SuperBlockVersion);
        assert(superblock.release_format.value > 0);
        assert(superblock.flags == 0);

        assert(stdx.zeroed(&superblock.reserved));
        assert(stdx.zeroed(&superblock.vsr_state.reserved));
        assert(stdx.zeroed(&superblock.vsr_state.checkpoint.reserved));
        assert(stdx.zeroed(&superblock.vsr_headers_reserved));

        assert(superblock.checksum_padding == 0);
        assert(superblock.parent_padding == 0);

        superblock.checksum = superblock.calculate_checksum();
    }

    pub fn valid_checksum(superblock: *const SuperBlockHeader) bool {
        return superblock.checksum == superblock.calculate_checksum();
    }

    pub fn checkpoint_id(superblock: *const SuperBlockHeader) u128 {
        return vsr.checksum(std.mem.asBytes(&superblock.vsr_state.checkpoint));
    }

    pub fn parent_checkpoint_id(superblock: *const SuperBlockHeader) u128 {
        return superblock.vsr_state.checkpoint.parent_checkpoint_id;
    }

    /// Does not consider { checksum, copy } when comparing equality.
    pub fn equal(a: *const SuperBlockHeader, b: *const SuperBlockHeader) bool {
        assert(a.release_format.value == b.release_format.value);

        assert(stdx.zeroed(&a.reserved));
        assert(stdx.zeroed(&b.reserved));

        assert(stdx.zeroed(&a.vsr_state.reserved));
        assert(stdx.zeroed(&b.vsr_state.reserved));

        assert(stdx.zeroed(&a.vsr_headers_reserved));
        assert(stdx.zeroed(&b.vsr_headers_reserved));

        assert(a.checksum_padding == 0);
        assert(b.checksum_padding == 0);
        assert(a.parent_padding == 0);
        assert(b.parent_padding == 0);

        if (a.version != b.version) return false;
        if (a.cluster != b.cluster) return false;
        if (a.sequence != b.sequence) return false;
        if (a.parent != b.parent) return false;
        if (!stdx.equal_bytes(VSRState, &a.vsr_state, &b.vsr_state)) return false;
        if (a.vsr_headers_count != b.vsr_headers_count) return false;
        if (!stdx.equal_bytes(
            [constants.view_change_headers_max]vsr.Header.Prepare,
            &a.vsr_headers_all,
            &b.vsr_headers_all,
        )) return false;

        return true;
    }

    pub fn vsr_headers(superblock: *const SuperBlockHeader) vsr.Headers.ViewChangeSlice {
        return vsr.Headers.ViewChangeSlice.init(
            if (superblock.vsr_state.log_view < superblock.vsr_state.view)
                .do_view_change
            else
                .start_view,
            superblock.vsr_headers_all[0..superblock.vsr_headers_count],
        );
    }

    pub fn manifest_references(superblock: *const SuperBlockHeader) ManifestReferences {
        const checkpoint_state = &superblock.vsr_state.checkpoint;
        return .{
            .oldest_address = checkpoint_state.manifest_oldest_address,
            .oldest_checksum = checkpoint_state.manifest_oldest_checksum,
            .newest_address = checkpoint_state.manifest_newest_address,
            .newest_checksum = checkpoint_state.manifest_newest_checksum,
            .block_count = checkpoint_state.manifest_block_count,
        };
    }

    pub fn free_set_reference(
        superblock: *const SuperBlockHeader,
        bitset: vsr.FreeSet.BitsetKinds,
    ) TrailerReference {
        switch (bitset) {
            .blocks_acquired => {
                return .{
                    .checksum = superblock.vsr_state.checkpoint
                        .free_set_blocks_acquired_checksum,
                    .last_block_address = superblock.vsr_state.checkpoint
                        .free_set_blocks_acquired_last_block_address,
                    .last_block_checksum = superblock.vsr_state.checkpoint
                        .free_set_blocks_acquired_last_block_checksum,
                    .trailer_size = superblock.vsr_state.checkpoint
                        .free_set_blocks_acquired_size,
                };
            },
            .blocks_released => {
                return .{
                    .checksum = superblock.vsr_state.checkpoint
                        .free_set_blocks_released_checksum,
                    .last_block_address = superblock.vsr_state.checkpoint
                        .free_set_blocks_released_last_block_address,
                    .last_block_checksum = superblock.vsr_state.checkpoint
                        .free_set_blocks_released_last_block_checksum,
                    .trailer_size = superblock.vsr_state.checkpoint
                        .free_set_blocks_released_size,
                };
            },
        }
    }

    pub fn client_sessions_reference(superblock: *const SuperBlockHeader) TrailerReference {
        const checkpoint = &superblock.vsr_state.checkpoint;
        return .{
            .checksum = checkpoint.client_sessions_checksum,
            .last_block_address = checkpoint.client_sessions_last_block_address,
            .last_block_checksum = checkpoint.client_sessions_last_block_checksum,
            .trailer_size = checkpoint.client_sessions_size,
        };
    }
};

pub const ManifestReferences = struct {
    /// The chronologically first manifest block in the chain.
    oldest_checksum: u128,
    oldest_address: u64,
    /// The chronologically last manifest block in the chain.
    newest_checksum: u128,
    newest_address: u64,
    /// The number of manifest blocks in the chain.
    block_count: u32,

    pub fn empty(references: *const ManifestReferences) bool {
        if (references.block_count == 0) {
            assert(references.oldest_address == 0);
            assert(references.oldest_checksum == 0);
            assert(references.newest_address == 0);
            assert(references.newest_checksum == 0);
            return true;
        } else {
            assert(references.oldest_address != 0);
            assert(references.newest_address != 0);
            return false;
        }
    }
};

pub const TrailerReference = struct {
    /// Checksum over the entire encoded trailer.
    checksum: u128,
    last_block_address: u64,
    last_block_checksum: u128,
    trailer_size: u64,

    pub fn empty(reference: *const TrailerReference) bool {
        if (reference.trailer_size == 0) {
            assert(reference.checksum == vsr.checksum(&.{}));
            assert(reference.last_block_address == 0);
            assert(reference.last_block_checksum == 0);
            return true;
        } else {
            assert(reference.last_block_address > 0);
            return false;
        }
    }
};

comptime {
    switch (constants.superblock_copies) {
        4, 6, 8 => {},
        else => @compileError("superblock_copies must be either { 4, 6, 8 } for flexible quorums."),
    }
}

/// The size of the entire superblock storage zone.
pub const superblock_zone_size = superblock_copy_size * constants.superblock_copies;

/// Leave enough padding after every superblock copy so that it is feasible, in the future, to
/// modify the `pipeline_prepare_queue_max` of an existing cluster (up to a maximum of clients_max).
/// (That is, this space is reserved for potential `vsr_headers`).
const superblock_copy_padding: comptime_int = stdx.div_ceil(
    (constants.clients_max - constants.pipeline_prepare_queue_max) * @sizeOf(vsr.Header),
    constants.sector_size,
) * constants.sector_size;

/// The size of an individual superblock header copy, including padding.
pub const superblock_copy_size = @sizeOf(SuperBlockHeader) + superblock_copy_padding;
comptime {
    assert(superblock_copy_padding % constants.sector_size == 0);
    assert(superblock_copy_size % constants.sector_size == 0);
}

/// The size of a data file that has an empty grid.
pub const data_file_size_min =
    superblock_zone_size +
    constants.journal_size +
    constants.client_replies_size +
    vsr.Zone.size(.grid_padding).?;

/// This table shows the sequence number progression of the SuperBlock's headers.
///
/// action        working  staging  disk
/// format        seq      seq      seq
///                0                 -        Initially the file has no headers.
///                0        1        -
///                0        1        1        Write a copyset for the first sequence.
///                1        1        1        Read quorum; verify 3/4 are valid.
///
/// open          seq      seq      seq
///                                 a
///               a                 a         Read quorum; verify 2/4 are valid.
///               a        (a)      a         Repair any broken copies of `a`.
///
/// checkpoint    seq      seq      seq
///               a        a        a
///               a        a+1
///               a        a+1      a+1
///               a+1      a+1      a+1       Read quorum; verify 3/4 are valid.
///
/// view_change   seq      seq      seq
///               a                 a
///               a        a+1      a         The new sequence reuses the original parent.
///               a        a+1      a+1
///               a+1      a+1      a+1       Read quorum; verify 3/4 are valid.
///               working  staging  disk
///
pub fn SuperBlockType(comptime Storage: type) type {
    return struct {
        const SuperBlock = @This();

        pub const Context = struct {
            superblock: *SuperBlock,
            callback: *const fn (context: *Context) void,
            caller: Caller,

            write: Storage.Write = undefined,
            read: Storage.Read = undefined,
            read_threshold: ?Quorums.Threshold = null,
            copy: ?u8 = null,
            /// Used by format(), checkpoint(), view_change().
            vsr_state: ?SuperBlockHeader.VSRState = null,
            /// Used by format() and view_change().
            vsr_headers: ?vsr.Headers.ViewChangeArray = null,
            repairs: ?Quorums.RepairIterator = null, // Used by open().
        };

        storage: *Storage,

        /// The superblock that was recovered at startup after a crash or that was last written.
        working: *align(constants.sector_size) SuperBlockHeader,

        /// The superblock that will replace the current working superblock once written.
        /// We cannot mutate any working state directly until it is safely on stable storage.
        /// Otherwise, we may accidentally externalize guarantees that are not yet durable.
        staging: *align(constants.sector_size) SuperBlockHeader,

        /// The copies that we read into at startup or when verifying the written superblock.
        reading: []align(constants.sector_size) SuperBlockHeader,

        /// It might seem that, at startup, we simply install the copy with the highest sequence.
        ///
        /// However, there's a scenario where:
        /// 1. We are able to write sequence 7 to 3/4 copies, with the last write being lost.
        /// 2. We startup and read all copies, with reads misdirected to the copy with sequence 6.
        ///
        /// Another scenario:
        /// 1. We begin to write sequence 7 to 1 copy and then crash.
        /// 2. At startup, the read to this copy fails, and we recover at sequence=6.
        /// 3. We then checkpoint another sequence 7 to 3/4 copies and crash.
        /// 4. At startup, we then see 4 copies with the same sequence with 1 checksum different.
        ///
        /// To mitigate these scenarios, we ensure that we are able to read a quorum of copies.
        /// This also gives us confidence that our working superblock has sufficient redundancy.
        quorums: Quorums = Quorums{},

        /// Whether the superblock has been opened. An open superblock may not be formatted.
        opened: bool = false,
        /// Runtime limit on the size of the datafile.
        storage_size_limit: u64,

        /// There may only be a single caller queued at a time, to ensure that the VSR protocol is
        /// careful to submit at most one view change at a time.
        queue_head: ?*Context = null,
        queue_tail: ?*Context = null,

        /// Set to non-null after open().
        /// Used for logging.
        replica_index: ?u8 = null,

        pub const Options = struct {
            storage: *Storage,
            storage_size_limit: u64,
        };

        pub fn init(allocator: mem.Allocator, options: Options) !SuperBlock {
            assert(options.storage_size_limit >= data_file_size_min);
            assert(options.storage_size_limit <= constants.storage_size_limit_max);
            assert(options.storage_size_limit % constants.sector_size == 0);

            const a = try allocator.alignedAlloc(SuperBlockHeader, constants.sector_size, 1);
            errdefer allocator.free(a);

            const b = try allocator.alignedAlloc(SuperBlockHeader, constants.sector_size, 1);
            errdefer allocator.free(b);

            const reading = try allocator.alignedAlloc(
                [constants.superblock_copies]SuperBlockHeader,
                constants.sector_size,
                1,
            );
            errdefer allocator.free(reading);

            return SuperBlock{
                .storage = options.storage,
                .working = &a[0],
                .staging = &b[0],
                .reading = &reading[0],
                .storage_size_limit = options.storage_size_limit,
            };
        }

        pub fn deinit(superblock: *SuperBlock, allocator: mem.Allocator) void {
            allocator.destroy(superblock.working);
            allocator.destroy(superblock.staging);
            allocator.free(superblock.reading);
        }

        pub const FormatOptions = struct {
            cluster: u128,
            release: vsr.Release,
            replica: u8,
            replica_count: u8,
        };

        pub fn format(
            superblock: *SuperBlock,
            callback: *const fn (context: *Context) void,
            context: *Context,
            options: FormatOptions,
        ) void {
            assert(!superblock.opened);
            assert(superblock.replica_index == null);

            assert(options.release.value > 0);
            assert(options.replica_count > 0);
            assert(options.replica_count <= constants.replicas_max);
            assert(options.replica < options.replica_count + constants.standbys_max);

            const members = vsr.root_members(options.cluster);
            const replica_id = members[options.replica];

            superblock.replica_index = vsr.member_index(&members, replica_id);

            // This working copy provides the parent checksum, and will not be written to disk.
            // We therefore use zero values to make this parent checksum as stable as possible.
            superblock.working.* = .{
                .copy = 0,
                .version = SuperBlockVersion,
                .sequence = 0,
                .release_format = options.release,
                .cluster = options.cluster,
                .parent = 0,
                .vsr_state = .{
                    .checkpoint = .{
                        .header = mem.zeroes(vsr.Header.Prepare),
                        .parent_checkpoint_id = 0,
                        .grandparent_checkpoint_id = 0,
                        .manifest_oldest_checksum = 0,
                        .manifest_oldest_address = 0,
                        .manifest_newest_checksum = 0,
                        .manifest_newest_address = 0,
                        .manifest_block_count = 0,
                        .free_set_blocks_acquired_checksum = 0,
                        .free_set_blocks_released_checksum = 0,
                        .free_set_blocks_acquired_last_block_checksum = 0,
                        .free_set_blocks_released_last_block_checksum = 0,
                        .free_set_blocks_acquired_last_block_address = 0,
                        .free_set_blocks_released_last_block_address = 0,
                        .free_set_blocks_acquired_size = 0,
                        .free_set_blocks_released_size = 0,
                        .client_sessions_checksum = 0,
                        .client_sessions_last_block_checksum = 0,
                        .client_sessions_last_block_address = 0,
                        .client_sessions_size = 0,
                        .storage_size = 0,
                        .snapshots_block_checksum = 0,
                        .snapshots_block_address = 0,
                        .release = vsr.Release.zero,
                    },
                    .replica_id = replica_id,
                    .members = members,
                    .commit_max = 0,
                    .sync_op_min = 0,
                    .sync_op_max = 0,
                    .sync_view = 0,
                    .log_view = 0,
                    .view = 0,
                    .replica_count = options.replica_count,
                },
                .vsr_headers_count = 0,
                .vsr_headers_all = mem.zeroes(
                    [constants.view_change_headers_max]vsr.Header.Prepare,
                ),
            };

            superblock.working.set_checksum();

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .format,
                .vsr_state = SuperBlockHeader.VSRState.root(.{
                    .cluster = options.cluster,
                    .release = options.release,
                    .replica_id = replica_id,
                    .members = members,
                    .replica_count = options.replica_count,
                }),
                .vsr_headers = vsr.Headers.ViewChangeArray.root(options.cluster),
            };

            // TODO At a higher layer, we must:
            // 1. verify that there is no valid superblock, and
            // 2. zero the superblock, WAL and client table to ensure storage determinism.

            superblock.acquire(context);
        }

        pub fn open(
            superblock: *SuperBlock,
            callback: *const fn (context: *Context) void,
            context: *Context,
        ) void {
            assert(!superblock.opened);

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .open,
            };

            superblock.acquire(context);
        }

        const UpdateCheckpoint = struct {
            header: vsr.Header.Prepare,
            view_attributes: ?struct {
                log_view: u32,
                view: u32,
                headers: *const vsr.Headers.ViewChangeArray,
            },
            commit_max: u64,
            sync_op_min: u64,
            sync_op_max: u64,
            manifest_references: ManifestReferences,
            free_set_references: struct {
                blocks_acquired: TrailerReference,
                blocks_released: TrailerReference,
            },
            client_sessions_reference: TrailerReference,
            storage_size: u64,
            release: vsr.Release,
        };

        /// Must update the commit_min and commit_min_checksum.
        pub fn checkpoint(
            superblock: *SuperBlock,
            callback: *const fn (context: *Context) void,
            context: *Context,
            update: UpdateCheckpoint,
        ) void {
            assert(superblock.opened);
            assert(update.header.op <= update.commit_max);
            assert(update.header.op > superblock.staging.vsr_state.checkpoint.header.op);
            assert(update.header.checksum !=
                superblock.staging.vsr_state.checkpoint.header.checksum);
            assert(update.sync_op_min <= update.sync_op_max);
            assert(update.release.value >= superblock.staging.vsr_state.checkpoint.release.value);

            assert(update.storage_size <= superblock.storage_size_limit);
            assert(update.storage_size >= data_file_size_min);
            assert((update.storage_size == data_file_size_min) ==
                (update.free_set_references.blocks_acquired.empty() and
                update.free_set_references.blocks_released.empty()));

            // NOTE: Within the vsr_state.checkpoint assignment below, do not read from vsr_state
            // directly. A miscompilation bug (as of Zig 0.11.0) causes fields to receive the
            // incorrect values.
            const vsr_state_staging = superblock.staging.vsr_state;
            const update_client_sessions = &update.client_sessions_reference;

            var vsr_state = superblock.staging.vsr_state;
            vsr_state.checkpoint = .{
                .header = update.header,
                .parent_checkpoint_id = superblock.staging.checkpoint_id(),
                .grandparent_checkpoint_id = vsr_state_staging.checkpoint.parent_checkpoint_id,

                .free_set_blocks_acquired_checksum = update.free_set_references
                    .blocks_acquired.checksum,
                .free_set_blocks_released_checksum = update.free_set_references
                    .blocks_released.checksum,

                .free_set_blocks_acquired_size = update.free_set_references
                    .blocks_acquired.trailer_size,
                .free_set_blocks_released_size = update.free_set_references
                    .blocks_released.trailer_size,

                .free_set_blocks_acquired_last_block_checksum = update.free_set_references
                    .blocks_acquired.last_block_checksum,
                .free_set_blocks_released_last_block_checksum = update.free_set_references
                    .blocks_released.last_block_checksum,

                .free_set_blocks_acquired_last_block_address = update.free_set_references
                    .blocks_acquired.last_block_address,
                .free_set_blocks_released_last_block_address = update.free_set_references
                    .blocks_released.last_block_address,

                .client_sessions_checksum = update_client_sessions.checksum,
                .client_sessions_last_block_checksum = update_client_sessions.last_block_checksum,
                .client_sessions_last_block_address = update_client_sessions.last_block_address,
                .client_sessions_size = update.client_sessions_reference.trailer_size,

                .manifest_oldest_checksum = update.manifest_references.oldest_checksum,
                .manifest_oldest_address = update.manifest_references.oldest_address,
                .manifest_newest_checksum = update.manifest_references.newest_checksum,
                .manifest_newest_address = update.manifest_references.newest_address,
                .manifest_block_count = update.manifest_references.block_count,

                .storage_size = update.storage_size,
                .snapshots_block_checksum = vsr_state_staging.checkpoint.snapshots_block_checksum,
                .snapshots_block_address = vsr_state_staging.checkpoint.snapshots_block_address,
                .release = update.release,
            };
            vsr_state.commit_max = update.commit_max;
            vsr_state.sync_op_min = update.sync_op_min;
            vsr_state.sync_op_max = update.sync_op_max;
            vsr_state.sync_view = 0;
            if (update.view_attributes) |*view_attributes| {
                assert(view_attributes.log_view <= view_attributes.view);
                view_attributes.headers.verify();
                vsr_state.log_view = view_attributes.log_view;
                vsr_state.view = view_attributes.view;
            }

            assert(superblock.staging.vsr_state.would_be_updated_by(vsr_state));

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .checkpoint,
                .vsr_state = vsr_state,
                .vsr_headers = if (update.view_attributes) |*view_attributes|
                    view_attributes.headers.*
                else
                    vsr.Headers.ViewChangeArray.init(
                        superblock.staging.vsr_headers().command,
                        superblock.staging.vsr_headers().slice,
                    ),
            };
            superblock.log_context(context);
            superblock.acquire(context);
        }

        const UpdateViewChange = struct {
            commit_max: u64,
            log_view: u32,
            view: u32,
            headers: *const vsr.Headers.ViewChangeArray,
            sync_checkpoint: ?struct {
                checkpoint: *const vsr.CheckpointState,
                sync_op_min: u64,
                sync_op_max: u64,
            },
        };

        /// The replica calls view_change():
        ///
        /// - to persist its view/log_view — it cannot advertise either value until it is certain
        ///   they will never backtrack.
        /// - to update checkpoint during sync
        ///
        /// The update must advance view/log_view (monotonically increasing) or checkpoint.
        // TODO: the current naming confusing and needs changing: during sync, this function doesn't
        //       necessary advance the view.
        pub fn view_change(
            superblock: *SuperBlock,
            callback: *const fn (context: *Context) void,
            context: *Context,
            update: UpdateViewChange,
        ) void {
            assert(superblock.opened);
            assert(superblock.staging.vsr_state.commit_max <= update.commit_max);
            assert(superblock.staging.vsr_state.view <= update.view);
            assert(superblock.staging.vsr_state.log_view <= update.log_view);
            assert(superblock.staging.vsr_state.log_view < update.log_view or
                superblock.staging.vsr_state.view < update.view or
                update.sync_checkpoint != null);
            assert((update.headers.command == .start_view and update.log_view == update.view) or
                (update.headers.command == .do_view_change and update.log_view < update.view));
            assert(
                superblock.staging.vsr_state.checkpoint.header.op <= update.headers.array.get(0).op,
            );

            update.headers.verify();
            assert(update.view >= update.log_view);

            var vsr_state = superblock.staging.vsr_state;
            vsr_state.commit_max = update.commit_max;
            vsr_state.log_view = update.log_view;
            vsr_state.view = update.view;
            if (update.sync_checkpoint) |*sync_checkpoint| {
                assert(superblock.staging.vsr_state.checkpoint.header.op <
                    sync_checkpoint.checkpoint.header.op);

                const checkpoint_next = vsr.Checkpoint.checkpoint_after(
                    superblock.staging.vsr_state.checkpoint.header.op,
                );
                const checkpoint_next_next = vsr.Checkpoint.checkpoint_after(checkpoint_next);

                if (sync_checkpoint.checkpoint.header.op == checkpoint_next) {
                    assert(sync_checkpoint.checkpoint.parent_checkpoint_id ==
                        superblock.staging.checkpoint_id());
                } else if (sync_checkpoint.checkpoint.header.op == checkpoint_next_next) {
                    assert(sync_checkpoint.checkpoint.grandparent_checkpoint_id ==
                        superblock.staging.checkpoint_id());
                }

                vsr_state.checkpoint = sync_checkpoint.checkpoint.*;
                vsr_state.sync_op_min = sync_checkpoint.sync_op_min;
                vsr_state.sync_op_max = sync_checkpoint.sync_op_max;
            }
            assert(superblock.staging.vsr_state.would_be_updated_by(vsr_state));

            context.* = .{
                .superblock = superblock,
                .callback = callback,
                .caller = .view_change,
                .vsr_state = vsr_state,
                .vsr_headers = update.headers.*,
            };
            superblock.log_context(context);
            superblock.acquire(context);
        }

        pub fn updating(superblock: *const SuperBlock, caller: Caller) bool {
            assert(superblock.opened);

            if (superblock.queue_head) |head| {
                if (head.caller == caller) return true;
            }

            if (superblock.queue_tail) |tail| {
                if (tail.caller == caller) return true;
            }

            return false;
        }

        fn write_staging(superblock: *SuperBlock, context: *Context) void {
            assert(context.caller != .open);
            assert(context.caller == .format or superblock.opened);
            assert(context.copy == null);
            context.vsr_state.?.assert_internally_consistent();
            assert(superblock.queue_head == context);
            assert(superblock.queue_tail == null);

            superblock.staging.* = superblock.working.*;
            superblock.staging.sequence = superblock.staging.sequence + 1;
            superblock.staging.parent = superblock.staging.checksum;
            superblock.staging.vsr_state = context.vsr_state.?;

            if (context.vsr_headers) |*headers| {
                assert(context.caller.updates_vsr_headers());

                superblock.staging.vsr_headers_count = headers.array.count_as(u32);
                stdx.copy_disjoint(
                    .exact,
                    vsr.Header.Prepare,
                    superblock.staging.vsr_headers_all[0..headers.array.count()],
                    headers.array.const_slice(),
                );
                @memset(
                    superblock.staging.vsr_headers_all[headers.array.count()..],
                    std.mem.zeroes(vsr.Header.Prepare),
                );
            } else {
                assert(!context.caller.updates_vsr_headers());
            }

            context.copy = 0;
            superblock.staging.set_checksum();
            superblock.write_header(context);
        }

        fn write_header(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);

            // We update the working superblock for a checkpoint/format/view_change:
            // open() does not update the working superblock, since it only writes to repair.
            if (context.caller == .open) {
                assert(superblock.staging.sequence == superblock.working.sequence);
            } else {
                assert(superblock.staging.sequence == superblock.working.sequence + 1);
                assert(superblock.staging.parent == superblock.working.checksum);
            }

            // The superblock cluster and replica should never change once formatted:
            assert(superblock.staging.cluster == superblock.working.cluster);
            assert(superblock.staging.vsr_state.replica_id ==
                superblock.working.vsr_state.replica_id);

            const storage_size = superblock.staging.vsr_state.checkpoint.storage_size;
            assert(storage_size >= data_file_size_min);
            assert(storage_size <= constants.storage_size_limit_max);

            assert(context.copy.? < constants.superblock_copies);
            superblock.staging.copy = context.copy.?;
            // Updating the copy number should not affect the checksum, which was previously set:
            assert(superblock.staging.valid_checksum());

            const buffer = mem.asBytes(superblock.staging);
            const offset = superblock_copy_size * @as(u32, context.copy.?);

            log.debug("{?}: {s}: write_header: " ++
                "checksum={x:0>32} sequence={} copy={} size={} offset={}", .{
                superblock.replica_index,
                @tagName(context.caller),
                superblock.staging.checksum,
                superblock.staging.sequence,
                context.copy.?,
                buffer.len,
                offset,
            });

            SuperBlock.assert_bounds(offset, buffer.len);

            superblock.storage.write_sectors(
                write_header_callback,
                &context.write,
                buffer,
                .superblock,
                offset,
            );
        }

        fn write_header_callback(write: *Storage.Write) void {
            const context: *Context = @alignCast(@fieldParentPtr("write", write));
            const superblock = context.superblock;
            const copy = context.copy.?;

            assert(superblock.queue_head == context);

            assert(copy < constants.superblock_copies);
            assert(copy == superblock.staging.copy);

            if (context.caller == .open) {
                context.copy = null;
                superblock.repair(context);
                return;
            }

            if (copy + 1 == constants.superblock_copies) {
                context.copy = null;
                superblock.read_working(context, .verify);
            } else {
                context.copy = copy + 1;
                superblock.write_header(context);
            }
        }

        fn read_working(
            superblock: *SuperBlock,
            context: *Context,
            threshold: Quorums.Threshold,
        ) void {
            assert(superblock.queue_head == context);
            assert(context.copy == null);
            assert(context.read_threshold == null);

            // We do not submit reads in parallel, as while this would shave off 1ms, it would also
            // increase the risk that a single fault applies to more reads due to temporal locality.
            // This would make verification reads more flaky when we do experience a read fault.
            // See "An Analysis of Data Corruption in the Storage Stack".

            context.copy = 0;
            context.read_threshold = threshold;
            for (superblock.reading) |*copy| copy.* = undefined;
            superblock.read_header(context);
        }

        fn read_header(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);
            assert(context.copy.? < constants.superblock_copies);
            assert(context.read_threshold != null);

            const buffer = mem.asBytes(&superblock.reading[context.copy.?]);
            const offset = superblock_copy_size * @as(u32, context.copy.?);

            log.debug("{?}: {s}: read_header: copy={} size={} offset={}", .{
                superblock.replica_index,
                @tagName(context.caller),
                context.copy.?,
                buffer.len,
                offset,
            });

            SuperBlock.assert_bounds(offset, buffer.len);

            superblock.storage.read_sectors(
                read_header_callback,
                &context.read,
                buffer,
                .superblock,
                offset,
            );
        }

        fn read_header_callback(read: *Storage.Read) void {
            const context: *Context = @alignCast(@fieldParentPtr("read", read));
            const superblock = context.superblock;
            const threshold = context.read_threshold.?;

            assert(superblock.queue_head == context);

            assert(context.copy.? < constants.superblock_copies);
            if (context.copy.? + 1 != constants.superblock_copies) {
                context.copy = context.copy.? + 1;
                superblock.read_header(context);
                return;
            }

            context.read_threshold = null;
            context.copy = null;

            if (superblock.quorums.working(superblock.reading, threshold)) |quorum| {
                assert(quorum.valid);
                assert(quorum.copies.count() >= threshold.count());

                const working = quorum.header;
                if (threshold == .verify) {
                    if (working.checksum != superblock.staging.checksum) {
                        @panic("superblock failed verification after writing");
                    }
                    assert(working.equal(superblock.staging));
                }

                if (context.caller == .format) {
                    assert(working.sequence == 1);
                    assert(working.vsr_state.checkpoint.header.checksum ==
                        vsr.Header.Prepare.root(working.cluster).checksum);
                    assert(working.vsr_state.checkpoint.free_set_blocks_acquired_size == 0);
                    assert(working.vsr_state.checkpoint.free_set_blocks_released_size == 0);
                    assert(working.vsr_state.checkpoint.client_sessions_size == 0);
                    assert(working.vsr_state.checkpoint.storage_size == data_file_size_min);
                    assert(working.vsr_state.checkpoint.header.op == 0);
                    assert(working.vsr_state.commit_max == 0);
                    assert(working.vsr_state.log_view == 0);
                    assert(working.vsr_state.view == 0);
                    assert(working.vsr_headers_count == 1);

                    assert(working.vsr_state.replica_count <= constants.replicas_max);
                    assert(vsr.member_index(
                        &working.vsr_state.members,
                        working.vsr_state.replica_id,
                    ) != null);
                }

                superblock.working.* = working.*;
                superblock.staging.* = working.*;

                const working_checkpoint = &superblock.working.vsr_state.checkpoint;
                const staging_checkpoint = &superblock.staging.vsr_state.checkpoint;

                // The SuperBlock on disk is an older version wherein @TypeOf(VSRState.checkpoint)
                // is CheckpointStateOld. Translate CheckpointStateOld → CheckpointState (zeroing
                // out new fields) and update the working and staging superblocks.
                if (working.version < SuperBlockVersion) {
                    const checkpoint_old: *const vsr.CheckpointStateOld =
                        @ptrCast(&working.vsr_state.checkpoint);
                    const checkpoint_new = vsr.CheckpointState{
                        .header = checkpoint_old.header,
                        .parent_checkpoint_id = checkpoint_old.parent_checkpoint_id,
                        .grandparent_checkpoint_id = checkpoint_old.grandparent_checkpoint_id,

                        .free_set_blocks_acquired_checksum = checkpoint_old.free_set_checksum,
                        .free_set_blocks_released_checksum = comptime vsr.checksum(&.{}),
                        .free_set_blocks_acquired_last_block_checksum = checkpoint_old
                            .free_set_last_block_checksum,
                        .free_set_blocks_released_last_block_checksum = 0,
                        .free_set_blocks_acquired_last_block_address = checkpoint_old
                            .free_set_last_block_address,
                        .free_set_blocks_released_last_block_address = 0,
                        .free_set_blocks_acquired_size = checkpoint_old.free_set_size,
                        .free_set_blocks_released_size = 0,

                        .client_sessions_checksum = checkpoint_old.client_sessions_checksum,
                        .client_sessions_last_block_checksum = checkpoint_old
                            .client_sessions_last_block_checksum,
                        .client_sessions_last_block_address = checkpoint_old
                            .client_sessions_last_block_address,
                        .client_sessions_size = checkpoint_old.client_sessions_size,

                        .manifest_oldest_checksum = checkpoint_old.manifest_oldest_checksum,
                        .manifest_oldest_address = checkpoint_old.manifest_oldest_address,
                        .manifest_newest_checksum = checkpoint_old.manifest_newest_checksum,
                        .manifest_newest_address = checkpoint_old.manifest_newest_address,
                        .manifest_block_count = checkpoint_old.manifest_block_count,

                        .snapshots_block_checksum = checkpoint_old.snapshots_block_checksum,
                        .snapshots_block_address = checkpoint_old.snapshots_block_address,

                        .storage_size = data_file_size_min,
                        .release = checkpoint_old.release,
                    };

                    working_checkpoint.* = checkpoint_new;
                    staging_checkpoint.* = checkpoint_new;
                    superblock.working.version = SuperBlockVersion;
                    superblock.staging.version = SuperBlockVersion;
                }

                log.debug(
                    "{[replica]?}: " ++
                        "{[caller]s}: installed working superblock: checksum={[checksum]x:0>32} " ++
                        "sequence={[sequence]} " ++
                        "release={[release]} " ++
                        "cluster={[cluster]x:0>32} replica_id={[replica_id]} " ++
                        "size={[size]} " ++
                        "free_set_blocks_acquired_size={[free_set_blocks_acquired_size]} " ++
                        "free_set_blocks_released_size={[free_set_blocks_released_size]} " ++
                        "client_sessions_size={[client_sessions_size]} " ++
                        "checkpoint_id={[checkpoint_id]x:0>32} " ++
                        "commit_min_checksum={[commit_min_checksum]} commit_min={[commit_min]} " ++
                        "commit_max={[commit_max]} log_view={[log_view]} view={[view]} " ++
                        "sync_op_min={[sync_op_min]} sync_op_max={[sync_op_max]} " ++
                        "manifest_oldest_checksum={[manifest_oldest_checksum]} " ++
                        "manifest_oldest_address={[manifest_oldest_address]} " ++
                        "manifest_newest_checksum={[manifest_newest_checksum]} " ++
                        "manifest_newest_address={[manifest_newest_address]} " ++
                        "manifest_block_count={[manifest_block_count]} " ++
                        "snapshots_block_checksum={[snapshots_block_checksum]} " ++
                        "snapshots_block_address={[snapshots_block_address]}",
                    .{
                        .replica = superblock.replica_index,
                        .caller = @tagName(context.caller),
                        .checksum = superblock.working.checksum,
                        .sequence = superblock.working.sequence,
                        .release = working_checkpoint.release,
                        .cluster = superblock.working.cluster,
                        .replica_id = superblock.working.vsr_state.replica_id,
                        .size = working_checkpoint.storage_size,
                        .free_set_blocks_acquired_size = working_checkpoint
                            .free_set_blocks_acquired_size,
                        .free_set_blocks_released_size = working_checkpoint
                            .free_set_blocks_released_size,
                        .client_sessions_size = working_checkpoint.client_sessions_size,
                        .checkpoint_id = superblock.working.checkpoint_id(),
                        .commit_min_checksum = working_checkpoint.header.checksum,
                        .commit_min = working_checkpoint.header.op,
                        .commit_max = superblock.working.vsr_state.commit_max,
                        .sync_op_min = superblock.working.vsr_state.sync_op_min,
                        .sync_op_max = superblock.working.vsr_state.sync_op_max,
                        .log_view = superblock.working.vsr_state.log_view,
                        .view = superblock.working.vsr_state.view,
                        .manifest_oldest_checksum = working_checkpoint.manifest_oldest_checksum,
                        .manifest_oldest_address = working_checkpoint.manifest_oldest_address,
                        .manifest_newest_checksum = working_checkpoint.manifest_newest_checksum,
                        .manifest_newest_address = working_checkpoint.manifest_newest_address,
                        .manifest_block_count = working_checkpoint.manifest_block_count,
                        .snapshots_block_checksum = working_checkpoint.snapshots_block_checksum,
                        .snapshots_block_address = working_checkpoint.snapshots_block_address,
                    },
                );
                for (superblock.working.vsr_headers().slice) |*header| {
                    log.debug("{?}: {s}: vsr_header: op={} checksum={}", .{
                        superblock.replica_index,
                        @tagName(context.caller),
                        header.op,
                        header.checksum,
                    });
                }

                if (superblock.working.vsr_state.checkpoint.storage_size >
                    superblock.storage_size_limit)
                {
                    vsr.fatal(
                        .storage_size_exceeds_limit,
                        "data file too large size={} > limit={}, " ++
                            "restart the replica increasing '--limit-storage'",
                        .{
                            superblock.working.vsr_state.checkpoint.storage_size,
                            superblock.storage_size_limit,
                        },
                    );
                }

                if (context.caller == .open) {
                    if (context.repairs) |_| {
                        // We just verified that the repair completed.
                        assert(threshold == .verify);
                        superblock.release(context);
                    } else {
                        assert(threshold == .open);

                        context.repairs = quorum.repairs();
                        context.copy = null;
                        superblock.repair(context);
                    }
                } else {
                    // TODO Consider calling TRIM() on Grid's free suffix after checkpointing.
                    superblock.release(context);
                }
            } else |err| switch (err) {
                error.Fork => @panic("superblock forked"),
                error.NotFound => @panic("superblock not found"),
                error.QuorumLost => @panic("superblock quorum lost"),
                error.ParentNotConnected => @panic("superblock parent not connected"),
                error.ParentSkipped => @panic("superblock parent superseded"),
                error.VSRStateNotMonotonic => @panic("superblock vsr state not monotonic"),
            }
        }

        fn repair(superblock: *SuperBlock, context: *Context) void {
            assert(context.caller == .open);
            assert(context.copy == null);
            assert(superblock.queue_head == context);

            if (context.repairs.?.next()) |repair_copy| {
                context.copy = repair_copy;
                log.warn("{?}: repair: copy={}", .{ superblock.replica_index, repair_copy });

                superblock.staging.* = superblock.working.*;
                superblock.write_header(context);
            } else {
                superblock.release(context);
            }
        }

        fn acquire(superblock: *SuperBlock, context: *Context) void {
            if (superblock.queue_head) |head| {
                // All operations are mutually exclusive with themselves.
                assert(head.caller != context.caller);
                assert(Caller.transitions.get(head.caller).?.contains(context.caller));
                assert(superblock.queue_tail == null);

                log.debug("{?}: {s}: enqueued after {s}", .{
                    superblock.replica_index,
                    @tagName(context.caller),
                    @tagName(head.caller),
                });

                superblock.queue_tail = context;
            } else {
                assert(superblock.queue_tail == null);

                superblock.queue_head = context;
                log.debug("{?}: {s}: started", .{
                    superblock.replica_index,
                    @tagName(context.caller),
                });

                if (Storage == @import("../testing/storage.zig").Storage) {
                    // We should have finished all pending superblock io before starting any more.
                    superblock.storage.assert_no_pending_reads(.superblock);
                    superblock.storage.assert_no_pending_writes(.superblock);
                }

                if (context.caller == .open) {
                    superblock.read_working(context, .open);
                } else {
                    superblock.write_staging(context);
                }
            }
        }

        fn release(superblock: *SuperBlock, context: *Context) void {
            assert(superblock.queue_head == context);

            log.debug("{?}: {s}: complete", .{
                superblock.replica_index,
                @tagName(context.caller),
            });

            if (Storage == @import("../testing/storage.zig").Storage) {
                // We should have finished all pending io by now.
                superblock.storage.assert_no_pending_reads(.superblock);
                superblock.storage.assert_no_pending_writes(.superblock);
            }

            switch (context.caller) {
                .format => {},
                .open => {
                    assert(!superblock.opened);
                    superblock.opened = true;
                    superblock.replica_index = vsr.member_index(
                        &superblock.working.vsr_state.members,
                        superblock.working.vsr_state.replica_id,
                    ).?;
                },
                .checkpoint,
                .view_change,
                => {
                    assert(stdx.equal_bytes(
                        SuperBlockHeader.VSRState,
                        &superblock.staging.vsr_state,
                        &context.vsr_state.?,
                    ));
                    assert(stdx.equal_bytes(
                        SuperBlockHeader.VSRState,
                        &superblock.working.vsr_state,
                        &context.vsr_state.?,
                    ));
                },
            }

            const queue_tail = superblock.queue_tail;
            superblock.queue_head = null;
            superblock.queue_tail = null;
            if (queue_tail) |tail| superblock.acquire(tail);

            context.callback(context);
        }

        fn assert_bounds(offset: u64, size: u64) void {
            assert(offset + size <= superblock_zone_size);
        }

        fn log_context(superblock: *const SuperBlock, context: *const Context) void {
            log.debug("{[replica]?}: {[caller]s}: " ++
                "commit_min={[commit_min_old]}..{[commit_min_new]} " ++
                "commit_max={[commit_max_old]}..{[commit_max_new]} " ++
                "commit_min_checksum={[commit_min_checksum_old]}..{[commit_min_checksum_new]} " ++
                "log_view={[log_view_old]}..{[log_view_new]} " ++
                "view={[view_old]}..{[view_new]} " ++
                "head={[head_old]}..{[head_new]?}", .{
                .replica = superblock.replica_index,
                .caller = @tagName(context.caller),

                .commit_min_old = superblock.staging.vsr_state.checkpoint.header.op,
                .commit_min_new = context.vsr_state.?.checkpoint.header.op,

                .commit_max_old = superblock.staging.vsr_state.commit_max,
                .commit_max_new = context.vsr_state.?.commit_max,

                .commit_min_checksum_old = superblock.staging.vsr_state.checkpoint.header.checksum,
                .commit_min_checksum_new = context.vsr_state.?.checkpoint.header.checksum,

                .log_view_old = superblock.staging.vsr_state.log_view,
                .log_view_new = context.vsr_state.?.log_view,

                .view_old = superblock.staging.vsr_state.view,
                .view_new = context.vsr_state.?.view,

                .head_old = superblock.staging.vsr_headers().slice[0].checksum,
                .head_new = if (context.vsr_headers) |*headers|
                    @as(?u128, headers.array.get(0).checksum)
                else
                    null,
            });
        }
    };
}

pub const Caller = enum {
    format,
    open,
    checkpoint,
    view_change,

    /// Beyond formatting and opening of the superblock, which are mutually exclusive of all
    /// other operations, only the following queue combinations are allowed:
    ///
    /// from state → to states
    const transitions = sets: {
        const Set = std.enums.EnumSet(Caller);
        break :sets std.enums.EnumMap(Caller, Set).init(.{
            .format = Set.init(.{}),
            .open = Set.init(.{}),
            .checkpoint = Set.init(.{ .view_change = true }),
            .view_change = Set.init(.{ .checkpoint = true }),
        });
    };

    fn updates_vsr_headers(caller: Caller) bool {
        return switch (caller) {
            .format => true,
            .open => unreachable,
            .checkpoint => true,
            .view_change => true,
        };
    }
};

test "SuperBlockHeader" {
    const expect = std.testing.expect;

    var a = std.mem.zeroes(SuperBlockHeader);
    a.version = SuperBlockVersion;
    a.release_format = vsr.Release.minimum;
    a.set_checksum();

    assert(a.copy == 0);
    try expect(a.valid_checksum());

    a.copy += 1;
    try expect(a.valid_checksum());

    a.version += 1;
    try expect(!a.valid_checksum());
}
