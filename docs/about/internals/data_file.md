# Data File

> “Just show me the tables already!”
> — probably not Fred Brooks

Each TigerBeetle replica stores all data inside a single file, called the data file (conventional
extension is `.tigerbeetle`). This document describes the high level layout of the data file. The
presentation is simplified a bit, to provide intuition without drowning the reader in details.
Consult the source code for byte-level details!

The data file is divided into several zones, with the main ones being:

- write-ahead log
- superblock
- grid

The grid forms the bulk of the data file (up to several terabytes). It is an elastic array of 64KiB
blocks:

```zig
pub const Block = [constants.block_size]u8;
pub const BlockPtr = *align(constants.sector_size) Block;
```

The grid serves as a raw storage layer. Higher level data structures (notably, the LSM tree) are
mapped to physical grid blocks. Because TigerBeetle is deterministic, the used portion of the grid
is identical across up-to-date replicas. This storage determinism is exploited to implement state
sync and repair on the level of grid blocks, see [the repair protocol](./vsr.md#protocol-repair-grid).

A grid block is identified by a pair of a `u64` index and `u128` checksum:

```zig
pub const BlockReference = struct {
    index: u64,
    checksum: u128,
};
```

The block checksum is stored outside of the block itself, to protect from misdirected writes. So, to
read a block, you need to know the block's index and checksum from "elsewhere", where "elsewhere" is
either a different block, or the superblock. Overall, the grid is used to implement a purely
functional, persistent (in both senses), garbage collected data structure which is updated
atomically by swapping the pointer to the root node. This is the classic copy-on-write technique
commonly used in filesystems. In fact, you can think of TigerBeetle's data file as a filesystem.

The superblock is what holds this logical "root pointer". Physically, the "root pointer" is comprised
of a couple of block references. These blocks, taken together, specify the manifests of all LSM trees.

Superblock is located at a fixed position in the data file, so, when a replica starts up, it can
read the superblock, read root block indexes and hashes from the superblock, and through those get
access to the rest of the data in the grid. Besides the manifest, superblock also references a
compressed bitset, which is itself stored in the grid, of all grid blocks which are not currently
allocated.

```zig
pub const SuperBlock = struct {
    manifest_oldest: BlockReference,
    manifest_newest: BlockReference,
    free_set: BlockReference,
};
```

Superblock durable updates must be atomic and need to write a fair amount of data (several
megabytes). To amortize this cost, superblock is flushed to disk relatively infrequently. The normal
mode of operation is that a replica starts up, reads the current superblock and free set to memory,
then proceeds allocating and writing new grid blocks, picking up free entries from the bit set. That
is, although the replica does write freshly allocated grid blocks to disk immediately, it does not
update the superblock on disk (so the logical state reachable from the superblock stays the same).
Only after a relatively large amount of new grid blocks are written, the replica atomically writes
the new superblock, with a new free set and a new logical "root pointer" (the superblock manifest).
If the replica crashes and restarts, it starts from the previous superblock, but, due to
determinism, replaying the operations after the crash results in exactly the same on-disk and
in-memory state.

To implement atomic update of the superblock, the superblock is physically stored as 4
distinct copies on disk. After startup, replica picks the latest superblock which has at least 2
copies written. Picking just the latest copy would be wrong --- unlike the grid blocks, the
superblock stores its own checksum, and is vulnerable to misdirected reads (i.e., a misdirected read
can hide the sole latest copy).

Because the superblock (and hence, logical grid state) is updated infrequently and in bursts, it
can't represent the entirety of persistent state. The rest of the state is stored in the write-ahead
log (WAL). The WAL is a ring buffer with prepares, and represents the logical diff which should be
applied to the state represented by superblock/grid to get the actual current state of the system.
WAL inner workings are described in the [VSR documentation](./vsr.md#protocol-normal), but, on a
high-level, when a replica processes a prepare, the replica:

* writes the prepare to the WAL on disk
* applies changes from the prepare to the in-memory data structure representing the current state
* applies changes from the prepare to the pending on-disk state by allocating and writing fresh grid
  blocks

When enough prepares are received, the superblock is updated to point to the accumulated-so-far new
disk state.

This covers how the three major areas of the data file -- the write-ahead log, the superblock and
the grid -- work together to represent abstract persistent logical state.

Concretely, the state of TigerBeetle is a collection (forest) of LSM trees. LSM structure is
described [in a separate document](./lsm.md), here only high level on-disk layout is discussed.

Each LSM tree stores a set of values. Values are:

* uniform in size,
* small (hundreds of bytes),
* sorted by key,
* which is embedded in the value itself (e.g, an `Account` value uses `timestamp` as a unique key).

To start from the middle, values are arranged in tables on disk. Each table represents a sorted
array of values and is physically stored in multiple blocks. Specifically:

* A table's data blocks each store a sorted array of values.
* A table's index block stores pointers to the data blocks, as well as boundary keys.

```zig
const TableDataBlock = struct {
    values_sorted: [value_count_max]Value,
};

const TableIndexBlock = struct {
    data_block_checksums: [data_block_count_max]u128,
    data_block_indexes:   [data_block_count_max]u64,
    data_block_key_max:   [data_block_count_max]Key,
};

const TableInfo = struct {
    tree_id: u16,
    index_block_index: u64,
    index_block_checksum: u128,
    key_min: Key,
    key_max: Key,
};
```

To lookup a value in a table, binary search the index block to locate the data block which should
hold the value, then binary search inside the data block.

Table size is physically limited by a single index block which can hold only so many references to
data blocks. However, tables are further artificially limited to hold only a certain (compile-time
constant) number of entries. Tables are arranged in levels. Each subsequent level contains
exponentially more tables.

Tables in a single level are pairwise disjoint. Tables in different layers overlap, but the key LSM:
invariant is observed: values in shallow layers override values in deeper layers. This means that
all modification happen to the first (purely in-memory) level.

An asynchronous compaction process rebalances layers. Compaction removes one table from level A, finds
all tables from level A+1 that intersect that table, removes all those tables from level A+1 and
inserts the result of the intersection.

Schematically, the effect of compaction can be represented as a sequence of events:

```zig
const CompactionEvent = struct {
    label: Label
    table: TableInfo, // points to table's index block
};

const Label = struct {
    level: u6,
    event: enum(u2) { insert, update, remove },
};
```

What's more, the current state of a tree can be represented implicitly as a sequence of such
insertion and removal events, which starts from the empty set of tables. And that's exactly how it
is represented physically in a data file!

Specifically, each LSM tree is a collection of layers which is stored implicitly as log of events.
The log consists of a sequence of `ManifestBlock`s:

```zig
const ManifestBlock = struct {
  previous_manifest_block: BlockReference,
  labels: [entry_count_max]Label,
  tables: [entry_count_max]TableInfo,
};
```

The manifest is an on-disk (in-grid) linked list, where each manifest block holds a reference to the
previous block.

The superblock then stores the oldest and newest manifest log blocks for all trees:

```zig
const Superblock = {
  manifest_block_oldest_address: u64,
  manifest_block_oldest_checksum: u128,
  manifest_block_newest_address: u64,
  manifest_block_newest_checksum: u128,
  free_set_last_address: u64,
  free_set_last_checksum: u128,
};
```

Tying everything together:

State is represented as a collection of LSM trees. Superblock is the root of all state. For each LSM
tree, superblock contains the pointers to the blocks constituting each tree's manifest log -- a sequence
of individual tables additions and deletions. By replaying this manifest log, it is possible to
reconstruct the manifest in memory. `Manifest` describes levels and tables of a single LSM tree. A
table is a pointer to its index block. The index block is a sorted array of pointers to data blocks.
Data blocks are sorted arrays of values.
