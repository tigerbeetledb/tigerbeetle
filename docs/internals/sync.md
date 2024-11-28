# State Sync

State sync synchronizes the state of a lagging replica with the healthy cluster.

State sync is used when a lagging replica's log no longer intersects with the cluster's current
log — [WAL repair](./vsr.md#protocol-repair-wal) cannot catch the replica up.

(VRR refers to state sync as "state transfer", but we already have
[transfers](../../reference/transfer.md) elsewhere.)

In the context of state sync, "state" refers to:

1. the superblock `vsr_state.checkpoint`
2. the grid (manifest, free set, and client sessions blocks)
3. the grid (LSM table data; acquired blocks only)
4. client replies

State sync consists of four protocols:

- [Sync Superblock](./vsr.md#protocol-requeststart-view) (syncs 1)
- [Repair Grid](./vsr.md#protocol-repair-grid) (syncs 2)
- [Sync Forest](./vsr.md#protocol-sync-forest) (syncs 3)
- [Sync Client Replies](./vsr.md#protocol-sync-client-replies) (syncs 4)

The target of superblock-sync is the latest checkpoint of the healthy cluster. When we catch up to
the latest checkpoint (or very close to it), then we can transition back to a healthy state.

State sync is lazy — logically, sync is completed when the superblock is synced. The data
pointed to by the new superblock can be transferred on-demand.

The state (superblock) and the WAL are updated atomically — [`start_view`](./vsr.md#start_view)
message includes both.

## Glossary

Replica roles:

- _syncing replica_: A replica performing superblock-sync. (Any step within _1_-_5_ of the
  [sync algorithm](#algorithm))
- _healthy replica_: A replica _not_ performing superblock-sync — part of the active cluster.

Checkpoints:

- [_checkpoint id_/_checkpoint identifier_](#checkpoint-identifier): Uniquely identifies a
  particular checkpoint reproducibly across replicas. It is a hash over the entire state.
- _Durable checkpoint_: A checkpoint whose state is present on at least replication quorum different
  replicas.

## Algorithm

0. [Sync is needed](#0-scenarios).
1. [Trigger sync in response to `start_view`](#1-triggers).
2. Interrupt the in-progress commit process:
  2.1. Wait for write operations to finish.
  2.2. Cancel potentially stalled read operations. (See `Grid.cancel()`.)
  2.3. Wait for cancellation to finish.
3. Install the new checkpoint and matching headers into the superblock:
   - Bump `vsr_state.checkpoint.header` to the sync target header.
   - Bump `vsr_state.checkpoint.parent_checkpoint_id` to the checkpoint id that is previous to our
     sync target (i.e. it isn't _our_ previous checkpoint).
   - Bump `replica.commit_min`.
   - Set `vsr_state.sync_op_min` to the minimum op which has not been repaired.
   - Set `vsr_state.sync_op_max` to the maximum op which has not been repaired.
4. Repair [replies](./vsr.md#protocol-sync-client-replies),
   [free set, client sessions, and manifest blocks](./vsr.md#protocol-repair-grid), and
   [table blocks](./vsr.md#protocol-sync-forest) that were created within the `sync_op_{min,max}`
   range.
5. Update the superblock with:
    - Set `vsr_state.sync_op_min = 0`
    - Set `vsr_state.sync_op_max = 0`

If the replica starts up with `vsr_state.sync_op_max ≠ 0`, go to step _4_.

### 0: Scenarios

Scenarios requiring state sync:

1. A replica was down/partitioned/slow for a while and the rest of the cluster moved on. The lagging
   replica is too far behind to catch up via WAL repair.
2. A replica was just formatted and is being added to the cluster (i.e. via
   [reconfiguration](./vsr.md#protocol-reconfiguration)). The new replica is too far behind to catch
   up via WAL repair.

Deciding between between WAL repair and state sync:

* If a replica lags by more than one checkpoint behind the primary, it must use state sync.
* If a replica is on the same checkpoint as the primary, it can only repair WAL.
* If a replica is just one checkpoint behind, either WAL repair or state sync might be necessary:
  * State sync is incorrect if there is only a single other replica on the next checkpoint --- the
    replica that is ahead could have its state corrupted.
  * WAL repair is incorrect if all reachable peer replicas have already wrapped their logs and
    evicted some prepares from the preceding checkpoint.
  * Summarizing, if the next checkpoint is durable (replicated on a quorum of replicas), the
    lagging replica must eventually state sync.

### 1: Triggers

State sync is triggered when a replica receives a `start_view` message with a more advanced
checkpoint.

If a replica isn't making progress committing because a grid block or a prepare can't be repaired
for some time, the replica proactively sends `request_start_view` to initiate the sync (see
`repair_sync_timeout`).

## Concepts

### Syncing Replica

Syncing replicas participate in replication normally. They can append prepares, commit, and are
eligible to become primaries. In particular, a syncing replica can advance its own checkpoint as a
part of the normal commit process.

The only restriction is that syncing replicas don't contribute to their checkpoint's replication
quorum. That is, for the cluster as a whole to advance the checkpoint, there must be at least a
replication quorum of healthy replicas.

The mechanism for discovering sufficiently replicated (durable) checkpoints uses `prepare_ok`
messages. Sending a `prepare_ok` signals that the replica has a recent checkpoint fully synced. As a
consequence, observing a `commit_max` sufficiently ahead of a checkpoint signifies the durability of
the checkpoint.

For this reason, syncing replicas withhold `prepare_ok` until `commit_max` confirms that their
checkpoint is fully replicated on a quorum of different replicas. See `op_prepare_max`,
`op_prepare_ok_max` and `op_repair_min` for details.

### Checkpoint Identifier

A _checkpoint id_ is a hash of the superblock `CheckpointState`.

A checkpoint identifier is attached to the following message types:

- `command=commit`: Current checkpoint identifier of sender.
- `command=ping`: Current checkpoint identifier of sender.
- `command=prepare`: The attached checkpoint id is the checkpoint id during which the corresponding
  prepare was originally prepared.
- `command=prepare_ok`: The attached checkpoint id is the checkpoint id during which the
  corresponding prepare was originally prepared.

### Storage Determinism

When everything works, storage is deterministic. If non-determinism is detected (via checkpoint id
mismatches) the replica which detects the mismatch will panic. This scenario should prompt operator
investigation and manual intervention.
