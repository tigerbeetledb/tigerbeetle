# Cloud

Tigerbeetle is [optimized for performance](../performance.md), exploiting "close to the metal"
technologies such as **Direct I/O** and **io_uring**.

This raises the question of whether or not these benefits are realised in a cloud environment where
the kernel is available to the app but where the network and the storage is virtualised.

## Direct I/O

Direct I/O eliminates memory copies between user space and the kernel within the OS, so whether the
OS is virtualized or not beyond that, Direct I/O would still be of benefit, and perhaps even more so
in a cloud environment by virtue of reducing memory pressure and freeing up the more limited
per-core memory bandwidth, especially with noisy neighbors.

A quick example, tested with Ubuntu in a VM on a Mac, we still see a 7% relative throughput
improvement for Direct I/O, regardless of whether Parallels is propagating O_DIRECT to the physical
disk, it’s still saving the cost of the memcpy to Ubuntu’s page cache (and beyond that appears to
also avoid polluting the CPU’s L1-3 cache by memcpy’ing through it - hard to be certain given all
the various memcpy() implementations).

At the same time, where cloud environments support locally attached high-performance block devices
(NVMe SSD), running local storage (as opposed to something like EBS) would definitely be preferable
if only from a performance point of view.

From a safety point of view, we haven’t yet tested whether any VMs would disregard O_DIRECT for an
NVMe device, or interpolate their own block device caching layer and mark dirty pages as clean
despite an EIO disk fault, but after a physical system reboot our hash-chaining and ongoing disk
scrubbing would at least be able to detect any issues related to this.

We are intentionally designing TigerBeetle to repair these local storage failures automatically on a
fine-grained basis using cluster redundancy.

## io_uring

In a similar way, io_uring removes (or amortizes by orders of magnitude) the cost of syscalls
between user space and the kernel, regardless of whether those are both within a virtualized
environment or not. io_uring is being developed by Jens Axboe specifically to reduce the cost of
large scale server fleets, which are typically cloud native, and there’s already been
[work done](https://www.phoronix.com/scan.php?page=news_item&px=KVM-IO-uring-Passthrough-LF2020) to
share the host’s io_uring queues with virtualized guests.

Our testing is only getting started though, we’re still building out the system end to end, so it
will be great to benchmark more performance numbers in various environments (and across cloud
providers) and share these as we go.

Credit to @tdaly61 from the Mojaloop community for prompting us with some great questions about
Tigerbeetle in the cloud.

You can read more about how we use io_uring in
[A Programmer-Friendly I/O Abstraction Over io_uring and kqueue](https://tigerbeetle.com/blog/2022-11-23-a-friendly-abstraction-over-iouring-and-kqueue).
