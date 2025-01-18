# Hardware

TigerBeetle is designed to operate and provide more than adequate performance even on commodity
hardware.

## Storage

Local NVMe drives are highly recommended for production deployments, and there's no requirement for
RAID.

In cloud or more complex deployments, remote block storage (eg, EBS, NVMe-oF) may be used but will
be slower and care must be taken to ensure
[independent fault domains](./deploy.md#hardware-fault-tolerance) across replicas.

Currently, TigerBeetle uses around 16TiB for 40 billion transfers. If you wish to use more capacity
than a single disk, RAID 10 / RAID 0 is recommended over parity RAID levels.

The data file is created before the server is initially run and grows automatically. TigerBeetle has
been more extensively tested on ext4, but ext4 only supports data files up to 16TiB. XFS is
supported, but has seen less testing. TigerBeetle can also be run against the raw block device.

## Memory

ECC memory is required for production deployments.

A replica requires at least 6 GiB RAM per machine. Between 16 GiB and 32 GiB or more (depending on
budget) is recommended to be allocated to each replica for caching. TigerBeetle uses static
allocation and will use exactly how much memory is explicitly allocated to it for caching via
command line argument.

## CPU

TigerBeetle requires only a single core per replica machine. TigerBeetle at present [does not
utilize more cores](../about/performance.md#single-core-by-design), but may in future.

It's recommended to have at least one additional core free for the operating system.

## Network

A minimum of a 1Gbps network connection is recommended.

## Multitenancy

There are no restrictions on sharing a server with other tenant processes.
