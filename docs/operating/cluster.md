# Cluster Recommendations

A TigerBeetle **cluster** is a set of machines each running the TigerBeetle server for strict
serializability, high availability and durability. The TigerBeetle server is a single binary.

Each server operates on a single local data file.

The TigerBeetle server binary plus its single data file is called a **replica**.

A cluster guarantees strict serializability, the highest level of consistency, by automatically
electing a primary replica to order and backup transactions across replicas in the cluster.

## Fault Tolerance

**The optimal, recommended size for any production cluster is 6 replicas.**

Given a cluster of 6 replicas:

- 4/6 replicas are required to elect a new primary if the old primary fails.
- A cluster remains highly available (able to process transactions), preserving strict
  serializability, provided that at least 3/6 machines have not failed (provided that the primary
  has not also failed) or provided that at least 4/6 machines have not failed (if the primary also
  failed and a new primary needs to be elected).
- A cluster preserves durability (surviving, detecting, and repairing corruption of any data file)
  provided that the cluster remains available. If machines go offline temporarily and the cluster
  becomes available again later, the cluster will be able to repair data file corruption once
  availability is restored.
- A cluster will correctly remain unavailable if too many machine failures have occurred to preserve
  data. In other words, TigerBeetle is designed to operate correctly or else to shut down safely if
  safe operation with respect to strict serializability is no longer possible due to permanent data
  loss.

### Geographic Fault Tolerance

All 6 replicas may be within the same data center (zero geographic fault tolerance), or spread
across 2 or more data centers, availability zones or regions (“sites”) for geographic fault
tolerance.

**For mission critical availability, the optimal number of sites is 3**, since each site would then
contain 2 replicas so that the loss of an entire site would not impair the availability of the
cluster.

Sites should preferably be within a few milliseconds of each other, since each transaction must be
replicated across sites before being committed.

### Hardware Fault Tolerance

It is important to ensure independent fault domains for each replica's data file, that each
replica's data file is stored on a separate disk (required), machine (required), rack (recommended),
data center (recommended) etc.
