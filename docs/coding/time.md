# Time

Time is a critical component of all distributed systems and databases. Within TigerBeetle, we keep
track of two types of time: logical time and physical time.

## Logical Time

TigerBeetle uses [Viewstamped replication](https://pmg.csail.mit.edu/papers/vr-revisited.pdf) for
its consensus protocol. This ensures
[strict serializability](http://www.bailis.org/blog/linearizability-versus-serializability/) for all
operations.

## Physical Time

TigerBeetle uses physical time in addition to the logical time provided by the consensus algorithm.
Financial transactions require physical time for multiple reasons, including:

- **Liquidity** - TigerBeetle supports [Two-Phase Transfers](./two-phase-transfers.md) that reserve
  funds and hold them in a pending state until they are posted, voided, or the transfer times out. A
  timeout is useful to ensure that the reserved funds are not held in limbo indefinitely.
- **Compliance and Auditing** - For regulatory and security purposes, it is useful to have a
  specific idea of when (in terms of wall clock time) transfers took place.

### Physical Time is Propagated Through Consensus

TigerBeetle clusters are distributed systems, so we cannot rely on all replicas in a cluster keeping
their system clocks perfectly in sync. Because of this, we "trust but verify".

In the TigerBeetle consensus protocol messages, replicas exchange timestamp information and monitor
the skew of their clock with respect to the other replicas. The system ensures that the clock skew
is within an acceptable window and may choose to stop processing transactions if the clocks are far
out of sync.

Importantly, the goal is not to reimplement or replace clock synchronization protocols, but to
verify that the cluster is operating within acceptable error bounds.

## Why TigerBeetle Manages Timestamps

Timestamps on [`Transfer`s](../reference/transfer.md#timestamp) and
[`Account`s](../reference/account.md#timestamp) are **set by the primary node** in the
TigerBeetle cluster when it receives the operation.

The primary then propagates the operations to all replicas, including the timestamps it determined.
All replicas process the state machine transitions deterministically, based on the primary's
timestamp -- _not_ based on their own system time.

Primary nodes monitor their clock skew with respect to the other replicas and may abdicate their
role as primary if they appear to be far off the rest of the cluster.

This is why the `timestamp` field must be set to `0` when operations are submitted, and it is then
set by the primary.

Similarly, the [`Transfer.timeout`](../reference/transfer.md#timeout) is given as an interval
in seconds, rather than as an absolute timestamp, because it is also managed by the primary. The
`timeout` is calculated relative to the `timestamp` when the operation arrives at the primary.

### Timestamps are Totally Ordered

All `timestamp`s within TigerBeetle are unique, immutable and
[totally ordered](http://book.mixu.net/distsys/time.html). A transfer that is created before another
transfer is guaranteed to have an earlier `timestamp` (even if they were created in the same
request).

In other systems this is also called a "physical" timestamp, "ingestion" timestamp, "record"
timestamp, or "system" timestamp.

## Further Reading

Watch this talk on
[Detecting Clock Sync Failure in Highly Available Systems](https://youtu.be/7R-Iz6sJG6Q?si=9sD2TpfD29AxUjOY)
on YouTube for more details.

You can also read the blog post
[Three Clocks are Better than One](https://tigerbeetle.com/blog/2021-08-30-three-clocks-are-better-than-one)
for more on how nodes determine their own time and clock skew.
