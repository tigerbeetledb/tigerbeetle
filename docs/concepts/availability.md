---
sidebar_position: 5
---

# High Availability

TigerBeetle can be run on a single machine, but it is designed to run as a multi-server **cluster**
for high availability and durability.

The TigerBeetle server is a single binary. Each server operates on a single local data file. The
TigerBeetle server binary plus its single data file is called a **replica**.

TigerBeetle processes all events through a single replica, so additional replicas provide
availability and durability but they do not increase the system throughput.

## 6-Replica Cluster (Recommended for Production Deployment)

For a production deployment, we strongly recommended deploying **6 replicas across 3 data centers or
availability zones**.

This configuration enables the cluster to continue operating even if an entire availability zone is
down, disconnected, or lost.

If only 2 availability zones are used, 6 replicas are still recommended, but it will not provide the
same fault tolerance. The cluster can tolerate 2 individual servers going down and continue
processing transactions. If 3 replicas go down, for example if one of the availability zones goes
offline, the cluster would still be able to process transactions but it would not be able to select
a new leader and continue if the leader goes down (see below for more about leaders and replicas).

## Automated Failover Using Viewstamped Replication (VSR) Consensus

TigerBeetle uses the
[Viewstamped Replication (VSR)](https://www.pmg.csail.mit.edu/papers/vr-revisited.pdf) consensus
algorithm to provide high availability with durable replication and automated failover.

The cluster consists of a single **leader** and a set of **followers**. The leader receives requests
from clients, replicates the data to the followers for durability, and processes the requests
through the state machine.

### Durable Replication

When clients submit requests to a leader, it will replicate those requests to the followers and
append them to the **Write-Ahead Log** before responding to the client. This ensures that once a
client receives a reply, they know that the events in their request were durably stored and will not
be lost even if the leader subsequently crashes.

### Fast Recovery

A new leader is selected automatically if the current leader becomes unavailable or is falling
behind the rest of the cluster, for example due to network or hardware issues.

VSR uses deterministic leader election, in contrast to Raft's randomized leader election.
Deterministic leader election means that a new leader can be selected quickly, without Raft's
problem of dueling leaders and without needing to add a random delay before initiating a leader
election. This means the cluster can recover quickly and continue processing transactions.

### Fault Tolerance and Faster Writes with Fewer Replicas

TigerBeetle also makes use of **flexible quorums**, introduced by
[Flexible Paxos](https://fpaxos.github.io/), to provide fault tolerance and faster writes while
using fewer replicas. Prior to this paper, many consensus systems required the number of nodes to be
$2F+1$, where F is the number of faults or failed servers the system can tolerate.

Flexible quorums enable TigerBeetle to tolerate 3 failed replicas with a cluster size of 6 rather
than 7. The leader replicates requests to one or two specific followers and waits for their response
before replying to the client. Replication to the other followers can be done asynchronously, which
makes writing to the system faster. This strategy ensures that the next leaders chosen if the
current one fails will have the transactions, which maintains the guarantee that committed
transactions will never be reverted.

## Zero-Downtime Upgrades (Coming Soon!)

TigerBeetle is being built to support zero-downtime upgrades. It is designed to be able to include
multiple versions of the server code in a single binary. This way, newer versions can be deployed
while the old version is still in use across the cluster. The cluster will automatically switch to
using the newer version once all of the replicas are running with a binary that includes that newer
version.

While TigerBeetle's API is not yet guaranteed to be stable, the storage format is. This stability
guarantee means that new versions of the code can be deployed without fear of data on disk being
lost or corrupted due to incompatible server versions.

## Next: Safety

As we have seen, TigerBeetle is carefully optimized to provide high performance and high
availability. However, there is one thing we care even more about, and that is
[safety](./safety.md).
