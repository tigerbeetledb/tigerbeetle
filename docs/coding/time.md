# Time

Time is a critical component of all distributed systems and databases. Within TigerBeetle, we keep
track of two types of time: logical time and physical time. Logical time is about ordering events
relative to each other, and physical time is the everyday time, a numeric timestamp.

## Logical Time

TigerBeetle uses a consensus protocol ([Viewstamped
replication](https://pmg.csail.mit.edu/papers/vr-revisited.pdf)) to guarantee [strict
serializability](http://www.bailis.org/blog/linearizability-versus-serializability/) for all
operations.

In other words, for an external observer, TigerBeetle cluster behaves as if it is just a single
machine which processes the incoming requests in-order. If an application submits a batch of
transfers with transfer `T1`, receives a reply, and then submits a batch with another transfer `T2`,
it is guaranteed that `T2` will observe the effects of `T1`. Note, however, that there could be
concurrent requests from multiple applications, so, unless `T1` and `T2` are in the same batch of
transfers, some other transfer could happen in-between them. See the
[reference](../reference/sessions.md) for precise guarantees.

## Physical Time

TigerBeetle uses physical time in addition to the logical time provided by the consensus algorithm.
Financial transactions require physical time for multiple reasons, including:

- **Liquidity** - TigerBeetle supports [Two-Phase Transfers](./two-phase-transfers.md) that reserve
  funds and hold them in a pending state until they are posted, voided, or the transfer times out. A
  timeout is useful to ensure that the reserved funds are not held in limbo indefinitely.
- **Compliance and Auditing** - For regulatory and security purposes, it is useful to have a
  specific idea of when (in terms of wall clock time) transfers took place.

TigerBeetle uses two-layered approach to physical time. On the basic layer, each replica asks the
underling operating system about the current time. Then, timing information from several replicas is
aggregated to make sure that replica roughly agree on the time, to prevent a replica with a bad
clock from issuing incorrect timestamps. Additionally, this "cluster time" is made strictly
monotonic, for end user's convenience.

## TigerBeetle Manages Timestamps

An important invariant is that TigerBeetle cluster assign all timestamps. In particular, Timestamps
on [`Transfer`s](../reference/transfer.md#timestamp) and
[`Account`s](../reference/account.md#timestamp) are set by the cluster when the corresponding event
arrives at the primary. This is why the `timestamp` field must be set to `0` when operations are
submitted.

Similarly, the [`Transfer.timeout`](../reference/transfer.md#timeout) is given as an interval
in seconds, rather than as an absolute timestamp, because it is also managed by the primary. The
`timeout` is calculated relative to the `timestamp` when the operation arrives at the primary.

This restriction is needed to make sure that any two timestamps always refer to the same underlying
clock (cluster's physical time) and are directly comparable. This in turn provides a set of powerful
guarantees.

### Timestamps are Totally Ordered

All `timestamp`s within TigerBeetle are unique, immutable and
[totally ordered](http://book.mixu.net/distsys/time.html). A transfer that is created before another
transfer is guaranteed to have an earlier `timestamp` (even if they were created in the same
request).

In other systems this is also called a "physical" timestamp, "ingestion" timestamp, "record"
timestamp, or "system" timestamp.

## Further Reading

If you are curious how exactly it is that TigerBeetle achieves strictly monotonic physical time, we
have a talk and a blog post with details:

* [Detecting Clock Sync Failure in Highly Available Systems (YouTube)](https://youtu.be/7R-Iz6sJG6Q?si=9sD2TpfD29AxUjOY)
* [Three Clocks are Better than One (TigerBeetle Blog)](https://tigerbeetle.com/blog/three-clocks-are-better-than-one/)
