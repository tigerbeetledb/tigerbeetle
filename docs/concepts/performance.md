---
sidebar_position: 4
sidebar_label: High Throughput, Low Latency
---

# High Throughput, Low Latency

TigerBeetle is designed to be in the data plane of business transaction processing. It provides
exceptionally high throughput and low latency.

In this section, we'll go into why all types of applications benefit from high performance and how
TigerBeetle achieves this performance.

## High Performance Benefits Everyone

Some types of businesses clearly need high performance, but way more applications benefit from high
performance than might be immediately obvious.

Performance matters for user experience, longevity, and cost-efficiency.

As we mentioned before, the world is becoming more transactional. All types of industries are seeing
higher volumes of lower-value transactions: payments, energy, API usage, etc. This trend is only
going to continue accelerating.

Beyond that, performance is important for user experience. Low latency in many industries directly
translates to more sales, such as in eCommerce. High throughput is important to keep the UX
predictable. Many companies' businesses experience huge spikes at particular times: Black Friday,
Chinese New Year, or when you land on the front page of Hacker News. You want to build your system
not only for the average load, but also to handle high peak load without users suddenly seeing huge
latency spikes.

In addition to helping you handle the busiest days, performance also buys longevity. You want your
business to grow over time. Switching out the database once you've scaled is a costly migration.
Instead, you can start building on a database that will handle your scale for the next 30 years.

Finally, cranking the maximum performance out of the hardware also makes it more cost efficient. You
want to handle your busiest days and decades of growth without balooning infrastructure costs.

## Single-Core by Design

One of the most important and maybe most surprising part of TigerBeetle's design is that it is
single-threaded.

This choice of single-threaded architecture for a high performance system was pioneered by the
financial exchange [LMAX](https://www.infoq.com/presentations/LMAX/) and has been leveraged by
numerous other financial companies. Beyond the financial domain, Frank McSherry and co. showed in
[_Scalability! But at what COST?_](https://www.usenix.org/system/files/conference/hotos15/hotos15-paper-mcsherry.pdf)
that single-threaded implementations can outperform multi-threaded implementations of many different
types of algorithms.

Using a single core provides consistency while sidestepping the need for locks to protect state
shared between threads. This is particularly important for financial applications where account
balance updates must be fully atomic. Avoiding concurrency primitives like locks means the system
spends less time managing the overhead and context-switching from multiple threads and more time
processing transactions. Having a single core process data also enables the CPU to more effectively
use its L1 and L2 caches, which are much faster than the L3 cache that is shared between cores or
RAM.

## Optimized for Write-Heavy Workloads

In contrast to most general purpose databases, TigerBeetle uses Log-Structured Merge (LSM) trees
instead of Balanced Search trees (B-Trees) for storing data. B-Trees are designed for a mix of reads
and writes, but they can have unpredictable write performance because writes may involve multiple
disk interactions to modify and rebalance nodes in the tree. LSM trees enable writes to be batched
in memory and then flushed to disk in a sequential manner.

TigerBeetle goes further in its use of LSM trees and uses an "LSM Forest". Every type of object and
every index is stored in a separate LSM tree. This improves read and write performance because we
know exactly where each piece of data should go or be based on its type. Furthermore, TigerBeetle
uses small values and fixed-size data structures and this, combined with the fact that different
types of data are in different trees, means that it can forego length-prefixes on the entries
entirely. This eliminates 3-33% space overhead and further improves write performance.

## Batching, Batching, Batching

Everything in TigerBeetle is batched in order to make most efficient use of all of the system's
resources.
[Clients batch thousands of events into a single request](../reference/requests/README.md#batching-events)
to maximize throughput while minimizing network overhead. TigerBeetle replicas then run consensus
over these same batches for efficient replication. Replicas write events to disk in batches to
amortize the fsync cost over the batch. TigerBeetle also uses `io_uring` (see below) to batch I/O
requests to the kernel.

## Extreme Engineering

Every aspect of TigerBeetle is engineered for high performance and specifically to squeeze the
maximum efficiency out of all of the physical resources.

TigerBeetle does zero deserialization by using fixed-sized data structures. These data structures
are optimized for CPU cache alignment to minimize L1-L3 cache misses.

TigerBeetle also uses direct I/O to copy data directly from the kernel's TCP receive buffer to disk,
and then to the state machine and back. This avoids the operating system's cache to provide more
predictable performance and safety guarantees. Minimizing copying of data reduces memory usage and
improves CPU performance by avoiding thrashing the CPU's cache.

Finally, TigerBeetle leverages [`io_uring`](https://en.wikipedia.org/wiki/Io_uring), a newer feature
in the Linux kernel, to batch system calls. Requests to read or write from the network and disk are
batched before being sent to and from the kernel. This massively improves performance by minimizing
the switching costs between TigerBeetle and the kernel.

## Next: High Availability

As you can tell, TigerBeetle is highly optimized to provide the performance you need to handle your
business's peak traffic and your traffic for the next decades. It also comes fully-equipped to run a
multi-server cluster for [high availability](./availability.md).
