# Safety

TigerBeetle is designed to a higher safety standard than a general-purpose relational database such
as MySQL or an in-memory database such as Redis:

- Strict consistency, CRCs and crash safety are not enough.

- TigerBeetle **handles and recovers from Latent Sector Errors** (e.g. at least
  [0.031% of SSD disks per year on average](https://www.usenix.org/system/files/fast20-maneas.pdf),
  and
  [1.4% of Enterprise HDD disks per year on average](https://www.usenix.org/legacy/events/fast08/tech/full_papers/bairavasundaram/bairavasundaram.pdf))
  **detects and repairs disk corruption or misdirected I/O where firmware reads/writes the wrong
  sector** (e.g. at least
  [0.023% of SSD disks per year on average](https://www.usenix.org/system/files/fast20-maneas.pdf),
  and
  [0.466% of Nearline HDD disks per year on average](https://www.usenix.org/legacy/events/fast08/tech/full_papers/bairavasundaram/bairavasundaram.pdf)),
  and **detects and repairs data tampering** (on a minority of the cluster, as if it were
  non-Byzantine corruption) with hash-chained checksums.

- TigerBeetle **uses Direct I/O by design** to sidestep
  [cache coherency bugs in the kernel page cache](https://www.usenix.org/system/files/atc20-rebello.pdf)
  after an EIO fsync error.

- TigerBeetle **exceeds the fsync durability of a single disk** and the hardware of a single server
  because disk firmware can contain bugs and because single server systems fail.

- TigerBeetle **provides strict serializability**, the gold standard of consistency, as a replicated
  state machine, and as a cluster of TigerBeetle servers (called replicas), for optimal high
  availability and distributed fault-tolerance.

- TigerBeetle **performs synchronous replication** to a quorum of backup TigerBeetle servers using
  the pioneering [Viewstamped Replication](http://pmg.csail.mit.edu/papers/vr-revisited.pdf) and
  consensus protocol for low-latency automated leader election and to eliminate the risk of
  split-brain associated with ad hoc manual failover systems.

- TigerBeetle is “fault-aware” and **recovers from local storage failures in the context of the
  global consensus protocol**, providing
  [more safety than replicated state machines such as ZooKeeper and LogCabin](https://www.youtube.com/watch?v=fDY6Wi0GcPs).
  For example, TigerBeetle can disentangle corruption in the middle of the committed journal (caused
  by bitrot) from torn writes at the end of the journal (caused by a power failure) to uphold
  durability guarantees given for committed data and maximize availability.

- TigerBeetle does not depend on synchronized system clocks, does not use leader leases, and
  **performs leader-based timestamping** so that your application can deal only with safe relative
  quantities of time with respect to transfer timeouts. To ensure that the leader's clock is within
  safe bounds of "true time", TigerBeetle combines all the clocks in the cluster to create a
  fault-tolerant clock that we call
  ["cluster time"](https://tigerbeetle.com/blog/2021-08-30-three-clocks-are-better-than-one).

## Fault Models

We adopt the following fault models with respect to storage, network, memory and processing:

### Storage Fault Model

- Disks experience data corruption with significant and material probability.

- Disk firmware or hardware may cause writes to be misdirected and written to the wrong sector, or
  not written at all, with low but nevertheless material probability.

- Disk firmware or hardware may cause reads to be misdirected and read from the wrong sector, or not
  read at all, with low but nevertheless material probability.

- Corruption does not always imply a system crash. Data may be corrupted at any time during its
  storage lifecycle: before being written, while being written, after being written, and while being
  read.

- Disk sector writes are not atomic. For example, an Advanced Format 4096 byte sector write to a
  disk with an emulated logical sector size of 4096 bytes, but a physical sector size of 512 bytes
  is not atomic and would be split into 8 physical sector writes, which may or may not be atomic.
  Therefore, we do not depend on any sector atomicity guarantees from the disk.

- The Linux kernel page cache is not reliable and may misrepresent the state of data on disk after
  an EIO or latent sector error. See
  _[Can Applications Recover from fsync Failures?](https://www.usenix.org/system/files/atc20-rebello.pdf)_
  from the University of Wisconsin – Madison presented at the 2020 USENIX Annual Technical
  Conference.

- File system metadata (such as a file's size) is unreliable and may change at any time.

- Disk performance and read and write latencies can sometimes be volatile, causing latency spikes on
  the order of seconds. A slow disk does not always indicate a failing disk, and a slow disk may
  return to median performance levels — for example, an SSD undergoing garbage collection.

### Network Fault Model

- Messages may be lost.

- Messages may be corrupted.

- Messages may be delayed.

- Messages may be replayed.

- TCP checksums are inadequate to prevent checksum collisions.

- Network performance may be asymmetrical for the upload and download paths.

### Memory Fault Model

- Memory protected with error-correcting codes is sufficient for our purpose. We make no further
  effort to protect against memory faults.

- Non-ECC memory is not recommended by TigerBeetle.

### Processing Fault Model

- The system may crash at any time.

- The system may freeze process execution for minutes or hours at a time, for example, during a VM
  migration.

- The system clock may jump backwards or forwards in time, at any time.

- NTP can help, but we cannot depend on NTP for strict serializability.

- NTP may stop working because of a network partition, which may not impact TigerBeetle. We,
  therefore, need to detect when a TigerBeetle cluster's clocks are not being synchronized by NTP so
  that financial transaction timestamps are accurate and within the operator's tolerance for error.

## Is TigerBeetle ACID-compliant?

Yes. Let's discuss each part:

### Atomicity

As part of replication, each operation is durably stored in at least a quorum of replicas'
Write-Ahead Logs (WAL) before the primary will acknowledge the operation as committed. WAL entries
are executed through the state machine business logic and the resulting state changes are stored in
TigerBeetle's LSM-Forest local storage engine.

The WAL is what allows TigerBeetle to achieve atomicity and durability since the WAL is the source
of truth. If TigerBeetle crashes, the WAL is replayed at startup from the last checkpoint on disk.

However, financial atomicity goes further than this: events and transfers can be linked when created
so they all succeed or fail together.

### Consistency

TigerBeetle guarantees strict serializability. And at the cluster level, stale reads are not
possible since all operations (not only writes, but also reads) go through the global consensus
protocol.

However, financial consistency requires more than this. TigerBeetle exposes a double-entry
accounting API to guarantee that money cannot be created or destroyed, but only transferred from one
account to another. And transfer history is immutable.

### Isolation

All client requests (and all events within a client request batch) are executed with the highest
level of isolation, serially through the state machine, one after another, before the next operation
begins. Counterintuitively, the use of batching and serial execution means that TigerBeetle can also
provide this level of isolation optimally, without the cost of locks for all the individual events
within a batch.

### Durability

Up until 2018, traditional DBMS durability has focused on the Crash Consistency Model, however,
Fsyncgate and
[Protocol Aware Recovery](https://www.usenix.org/conference/fast18/presentation/alagappan) have
shown that this model can lead to real data loss for users in the wild. TigerBeetle therefore adopts
an explicit storage fault model, which we then verify and test with incredible levels of corruption,
something which few distributed systems historically were designed to handle. Our emphasis on
protecting Durability is what sets TigerBeetle apart, not only as a ledger but as a DBMS.

However, absolute durability is impossible, because all hardware can ultimately fail. Data we write
today might not be available tomorrow. TigerBeetle embraces limited disk reliability and maximizes
data durability in spite of imperfect disks. We actively work against such entropy by taking
advantage of cluster-wide storage. A record would need to get corrupted on all replicas in a cluster
to get lost, and even in that case the system would safely halt.

## `io_uring` Security

`io_uring` is a relatively new part of the Linux kernel (support for it was added in version 5.1,
which was released in May 2019). Since then, many kernel exploits have been found related to
`io_uring` and in 2023
[Google announced](https://security.googleblog.com/2023/06/learnings-from-kctf-vrps-42-linux.html)
that they were disabling it in ChromeOS, for Android apps, and on Google production servers.

Google's post is primarily about how they secure operating systems and web servers that handle
hostile user content. In the Google blog post, they specifically note:

> we currently consider it safe only for use by trusted components

As a financial system of record, TigerBeetle is a trusted component and it should be running in a
trusted environment.

Furthermore, TigerBeetle only uses 128-byte [`Account`s](../reference/account.md) and
[`Transfer`s](../reference/transfer.md) with pure integer fields. TigerBeetle has no
(de)serialization and does not take user-generated strings, which significantly constrains the
attack surface.

We are confident that `io_uring` is the safest (and most performant) way for TigerBeetle to handle
async I/O. It is significantly easier for the kernel to implement this correctly than for us to
include a userspace multithreaded thread pool (for example, as libuv does).
