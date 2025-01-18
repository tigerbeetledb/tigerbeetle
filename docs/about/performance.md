# Performance

TigerBeetle provides more performance than a general-purpose relational database such as MySQL or an
in-memory database such as Redis:

- TigerBeetle **uses small, simple fixed-size data structures** (accounts and transfers) and a
  tightly scoped domain.

- TigerBeetle **uses multiple Log-Structured Merge (LSM) Trees** for storing objects and indices.
  This data structure is highly optimized for write-heavy workloads like Online Transaction
  Processing (OLTP). TigerBeetle squeezes even more performance out of LSM trees by using multiple
  trees, each storing separate types of homogeneous data.

- TigerBeetle **performs all balance tracking logic in the database**. This is a paradigm shift
  where we move the code once to the data, not the data back and forth to the code in the critical
  path. This eliminates the need for complex caching logic outside the database. The “Accounting”
  business logic is built into TigerBeetle so that you can **keep your application layer simple and
  completely stateless**.

- TigerBeetle **supports batching by design**. You can batch all the transfer prepares or commits
  that you receive in a fixed 10ms window (or in a dynamic 1ms through 10ms window according to
  load) and then send them all in a single network request to the database. This enables
  low-overhead networking, large sequential disk write patterns and amortized fsync and consensus
  across hundreds and thousands of transfers.

> Everything is a batch. It's your choice whether a batch contains 100 transfers or 10,000 transfers
> but our measurements show that **latency is _less_ where batch sizes are larger, thanks to
> Little's Law** (e.g. 50ms for a batch of a hundred transfers vs 20ms for a batch of ten thousand
> transfers). TigerBeetle is able to amortize the cost of I/O to achieve lower latency, even for
> fairly large batch sizes, by eliminating the cost of queueing delay incurred by small batches.

- If your system is not under load, TigerBeetle also **optimizes the latency of small batches**.
  After copying from the kernel's TCP receive buffer (TigerBeetle does not do user-space TCP),
  TigerBeetle **does zero-copy Direct I/O** from network protocol to disk, and then to state machine
  and back, to reduce memory pressure and L1-L3 cache pollution.

- TigerBeetle **uses io_uring for zero-syscall networking and storage I/O**. The cost of a syscall
  in terms of context switches adds up quickly for a few thousand transfers. (You can read about the
  security of using io_uring [here](./safety.md#io_uring-security).)

- TigerBeetle **does zero-deserialization** by using fixed-size data structures that are optimized
  for cache line alignment to **minimize L1-L3 cache misses**.

- TigerBeetle **takes advantage of Heidi Howard's Flexible Quorums** to reduce the cost of
  **synchronous replication to one (or two) remote replicas at most** (in addition to the leader)
  with **asynchronous replication** between the remaining followers. This improves write
  availability without sacrificing strict serializability or durability. This also reduces server
  deployment cost by as much as 20% because a 4-node cluster with Flexible Quorums can now provide
  the same `f=2` guarantee for the replication quorum as a 5-node cluster.

> ["The major availability breakdowns and performance anomalies we see in cloud environments tend to
> be caused by subtle underlying faults, i.e. gray failure (slowly failing hardware) rather than
> fail-stop
> failure."](https://www.microsoft.com/en-us/research/wp-content/uploads/2017/06/paper-1.pdf)

- TigerBeetle **routes around transient gray failure latency spikes**. For example, if a disk write
  that typically takes 4ms starts taking 4 seconds because the disk is slowly failing, TigerBeetle
  will use cluster redundancy to mask the gray failure automatically without the user seeing any
  4-second latency spike. This is a relatively new performance technique in the literature known as
  "tail tolerance".

## Single-Core By Design

TigerBeetle uses a single core by design and uses a single leader node to process events. Adding
more nodes can therefore increase reliability but not throughput.

For a high-performance database, this may seem like an unusual choice. However, sharding in
financial databases is notoriously difficult and contention issues often negate the would-be
benefits. Specifically, a small number of hot accounts are often involved in a large proportion of
the transactions so the shards responsible for those accounts become bottlenecks.

For more details on when single-threaded implementations of algorithms outperform multi-threaded
implementations, see ["Scalability! But at what
COST?](https://www.usenix.org/system/files/conference/hotos15/hotos15-paper-mcsherry.pdf).
