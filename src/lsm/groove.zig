const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const schema = @import("schema.zig");

const TableType = @import("table.zig").TableType;
const TreeType = @import("tree.zig").TreeType;
const GridType = @import("../vsr/grid.zig").GridType;
const CompositeKeyType = @import("composite_key.zig").CompositeKeyType;
const NodePool = @import("node_pool.zig").NodePool(constants.lsm_manifest_node_size, 16);
const CacheMapType = @import("cache_map.zig").CacheMapType;
const ScopeCloseMode = @import("tree.zig").ScopeCloseMode;
const ManifestLogType = @import("manifest_log.zig").ManifestLogType;
const ScanBuilderType = @import("scan_builder.zig").ScanBuilderType;
const Snapshot = schema.Snapshot;

fn ObjectTreeHelpers(comptime Object: type) type {
    assert(@hasField(Object, "timestamp"));
    assert(std.meta.fieldInfo(Object, .timestamp).type == u64);

    return struct {
        inline fn key_from_value(value: *const Object) u64 {
            return value.timestamp & ~@as(u64, tombstone_bit);
        }

        const sentinel_key = std.math.maxInt(u64);
        const tombstone_bit = 1 << (64 - 1);

        inline fn tombstone(value: *const Object) bool {
            return (value.timestamp & tombstone_bit) != 0;
        }

        inline fn tombstone_from_key(timestamp: u64) Object {
            assert(timestamp & tombstone_bit == 0);

            var value = std.mem.zeroes(Object); // Full zero-initialized Value.
            value.timestamp = timestamp | tombstone_bit;
            return value;
        }
    };
}

const IdTreeValue = extern struct {
    id: u128,
    timestamp: u64,
    padding: u64 = 0,

    comptime {
        // Assert that there is no implicit padding.
        assert(@sizeOf(IdTreeValue) == 32);
        assert(stdx.no_padding(IdTreeValue));
    }

    inline fn key_from_value(value: *const IdTreeValue) u128 {
        return value.id;
    }

    const sentinel_key = std.math.maxInt(u128);
    const tombstone_bit = 1 << (64 - 1);

    inline fn tombstone(value: *const IdTreeValue) bool {
        return (value.timestamp & tombstone_bit) != 0;
    }

    inline fn tombstone_from_key(id: u128) IdTreeValue {
        return .{
            .id = id,
            .timestamp = tombstone_bit,
        };
    }
};

/// Normalizes index tree field types into either u64 or u128 for CompositeKey
fn IndexCompositeKeyType(comptime Field: type) type {
    switch (@typeInfo(Field)) {
        .Enum => |e| {
            return switch (@bitSizeOf(e.tag_type)) {
                0...@bitSizeOf(u64) => u64,
                @bitSizeOf(u65)...@bitSizeOf(u128) => u128,
                else => @compileError("Unsupported enum tag for index: " ++ @typeName(e.tag_type)),
            };
        },
        .Int => |i| {
            if (i.signedness != .unsigned) {
                @compileError("Index int type (" ++ @typeName(Field) ++ ") is not unsigned");
            }
            return switch (@bitSizeOf(Field)) {
                0...@bitSizeOf(u64) => u64,
                @bitSizeOf(u65)...@bitSizeOf(u128) => u128,
                else => @compileError("Unsupported int type for index: " ++ @typeName(Field)),
            };
        },
        else => @compileError("Index type " ++ @typeName(Field) ++ " is not supported"),
    }
}

comptime {
    assert(IndexCompositeKeyType(u1) == u64);
    assert(IndexCompositeKeyType(u16) == u64);
    assert(IndexCompositeKeyType(enum(u16) { x }) == u64);

    assert(IndexCompositeKeyType(u32) == u64);
    assert(IndexCompositeKeyType(u63) == u64);
    assert(IndexCompositeKeyType(u64) == u64);

    assert(IndexCompositeKeyType(enum(u65) { x }) == u128);
    assert(IndexCompositeKeyType(u65) == u128);
    assert(IndexCompositeKeyType(u128) == u128);
}

fn IndexTreeType(
    comptime Storage: type,
    comptime Field: type,
    comptime value_count_max: usize,
) type {
    const CompositeKey = CompositeKeyType(IndexCompositeKeyType(Field));
    const Table = TableType(
        CompositeKey.Key,
        CompositeKey,
        CompositeKey.key_from_value,
        CompositeKey.sentinel_key,
        CompositeKey.tombstone,
        CompositeKey.tombstone_from_key,
        value_count_max,
        .secondary_index,
    );

    return TreeType(Table, Storage);
}

/// A Groove is a collection of LSM trees auto generated for fields on a struct type
/// as well as custom derived fields from said struct type.
pub fn GrooveType(
    comptime Storage: type,
    comptime Object: type,
    /// An anonymous struct instance which contains the following:
    ///
    /// - ids: { .tree = u16 }:
    ///     An anonymous struct which maps each of the groove's trees to a stable, forest-unique,
    ///     tree identifier.
    ///
    /// - value_count_max: { .field = usize }:
    ///     An anonymous struct which contains, for each field of `Object`,
    ///     the maximum number of values per table for the corresponding index tree.
    ///
    /// - ignored: [][]const u8:
    ///     An array of fields on the Object type that should not be given index trees
    ///
    /// - derived: { .field = *const fn (*const Object) DerivedType }:
    ///     An anonymous struct which contain fields that don't exist on the Object
    ///     but can be derived from an Object instance using the field's corresponding function.
    comptime groove_options: anytype,
) type {
    @setEvalBranchQuota(64000);

    const has_id = @hasField(Object, "id");
    if (has_id) assert(std.meta.fieldInfo(Object, .id).type == u128);

    assert(@hasField(Object, "timestamp"));
    assert(std.meta.fieldInfo(Object, .timestamp).type == u64);

    comptime var index_fields: []const std.builtin.Type.StructField = &.{};

    const primary_field = if (has_id) "id" else "timestamp";
    const PrimaryKey = @TypeOf(@field(@as(Object, undefined), primary_field));

    // Generate index LSM trees from the struct fields.
    for (std.meta.fields(Object)) |field| {
        // See if we should ignore this field from the options.
        //
        // By default, we ignore the "timestamp" field since it's a special identifier.
        // Since the "timestamp" is ignored by default, it shouldn't be provided in groove_options.ignored.
        comptime var ignored = mem.eql(u8, field.name, "timestamp") or mem.eql(u8, field.name, "id");
        for (groove_options.ignored) |ignored_field_name| {
            comptime assert(!std.mem.eql(u8, ignored_field_name, "timestamp"));
            comptime assert(!std.mem.eql(u8, ignored_field_name, "id"));
            ignored = ignored or std.mem.eql(u8, field.name, ignored_field_name);
        }

        if (!ignored) {
            const IndexTree = IndexTreeType(
                Storage,
                field.type,
                @field(groove_options.value_count_max, field.name),
            );
            index_fields = index_fields ++ [_]std.builtin.Type.StructField{
                .{
                    .name = field.name,
                    .type = IndexTree,
                    .default_value = null,
                    .is_comptime = false,
                    .alignment = @alignOf(IndexTree),
                },
            };
        }
    }

    // Generate IndexTrees for fields derived from the Value in groove_options.
    const derived_fields = std.meta.fields(@TypeOf(groove_options.derived));
    for (derived_fields) |field| {
        // Get the function info for the derived field.
        const derive_func = @field(groove_options.derived, field.name);
        const derive_func_info = @typeInfo(@TypeOf(derive_func)).Fn;

        // Make sure it has only one argument.
        if (derive_func_info.params.len != 1) {
            @compileError("expected derive fn to take in *const " ++ @typeName(Object));
        }

        // Make sure the function takes in a reference to the Value:
        const derive_arg = derive_func_info.params[0];
        if (derive_arg.is_generic) @compileError("expected derive fn arg to not be generic");
        if (derive_arg.type != *const Object) {
            @compileError("expected derive fn to take in *const " ++ @typeName(Object));
        }

        // Get the return value from the derived field as the DerivedType.
        const derive_return_type = derive_func_info.return_type orelse {
            @compileError("expected derive fn to return valid tree index type");
        };

        const DerivedType = derive_return_type;
        const IndexTree = IndexTreeType(
            Storage,
            DerivedType,
            @field(groove_options.value_count_max, field.name),
        );

        index_fields = index_fields ++ [_]std.builtin.Type.StructField{
            .{
                .name = field.name,
                .type = IndexTree,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(IndexTree),
            },
        };
    }

    comptime var index_options_fields: []const std.builtin.Type.StructField = &.{};
    for (index_fields) |index_field| {
        const IndexTree = index_field.type;
        index_options_fields = index_options_fields ++ [_]std.builtin.Type.StructField{
            .{
                .name = index_field.name,
                .type = IndexTree.Options,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(IndexTree.Options),
            },
        };
    }

    const _ObjectTree = blk: {
        const Table = TableType(
            u64, // key = timestamp
            Object,
            ObjectTreeHelpers(Object).key_from_value,
            ObjectTreeHelpers(Object).sentinel_key,
            ObjectTreeHelpers(Object).tombstone,
            ObjectTreeHelpers(Object).tombstone_from_key,
            groove_options.value_count_max.timestamp,
            .general,
        );
        break :blk TreeType(Table, Storage);
    };

    const _IdTree = if (!has_id) void else blk: {
        const Table = TableType(
            u128,
            IdTreeValue,
            IdTreeValue.key_from_value,
            IdTreeValue.sentinel_key,
            IdTreeValue.tombstone,
            IdTreeValue.tombstone_from_key,
            groove_options.value_count_max.id,
            .general,
        );
        break :blk TreeType(Table, Storage);
    };

    const _IndexTrees = @Type(.{
        .Struct = .{
            .layout = .Auto,
            .fields = index_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });
    const IndexTreeOptions = @Type(.{
        .Struct = .{
            .layout = .Auto,
            .fields = index_options_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    const has_scan = index_fields.len > 0;

    // Verify groove index count:
    const indexes_count_actual = std.meta.fields(_IndexTrees).len;
    const indexes_count_expect = std.meta.fields(Object).len -
        groove_options.ignored.len -
        // The id/timestamp fields are implicitly ignored since it's the primary key for ObjectTree:
        (@as(usize, 1) + @intFromBool(has_id)) +
        std.meta.fields(@TypeOf(groove_options.derived)).len;

    assert(indexes_count_actual == indexes_count_expect);
    assert(indexes_count_actual == std.meta.fields(IndexTreeOptions).len);

    // Generate a helper function for interacting with an Index field type.
    const IndexTreeFieldHelperType = struct {
        /// Returns true if the field is a derived field.
        fn is_derived(comptime field_name: []const u8) bool {
            comptime var derived = false;
            inline for (derived_fields) |derived_field| {
                derived = derived or std.mem.eql(u8, derived_field.name, field_name);
            }
            return derived;
        }

        /// Gets the index type from the index name (even if the index is derived).
        fn IndexType(comptime field_name: []const u8) type {
            if (!is_derived(field_name)) {
                return @TypeOf(@field(@as(Object, undefined), field_name));
            }

            const derived_fn = @TypeOf(@field(groove_options.derived, field_name));
            return std.meta.Child(@typeInfo(derived_fn).Fn.return_type.?);
        }

        fn HelperType(comptime field_name: []const u8) type {
            return struct {
                const Index = IndexType(field_name);
                const IndexInteger = switch (@typeInfo(Index)) {
                    .Int => Index,
                    .Enum => |info| info.int_type,
                    else => @compileError("Unsupported index type for " ++ field_name),
                };

                inline fn int_from_index(index: Index) IndexInteger {
                    return switch (@typeInfo(Index)) {
                        .Int => index,
                        .Enum => @intFromEnum(index),
                        else => unreachable,
                    };
                }

                /// Try to extract an index from the object, deriving it when necessary.
                pub fn derive_index(object: *const Object) IndexInteger {
                    if (comptime is_derived(field_name)) {
                        return int_from_index(@field(groove_options.derived, field_name)(object));
                    } else {
                        return int_from_index(@field(object, field_name));
                    }
                }
            };
        }
    }.HelperType;

    const ObjectsCacheHelpers = struct {
        const tombstone_bit = 1 << (64 - 1);

        inline fn key_from_value(value: *const Object) PrimaryKey {
            if (has_id) {
                return value.id;
            } else {
                return value.timestamp & ~@as(u64, tombstone_bit);
            }
        }

        inline fn hash(key: PrimaryKey) u64 {
            return stdx.hash_inline(key);
        }

        inline fn tombstone_from_key(a: PrimaryKey) Object {
            var obj: Object = undefined;
            if (has_id) {
                obj.id = a;
                obj.timestamp = 0;
            } else {
                obj.timestamp = a;
            }
            obj.timestamp |= tombstone_bit;
            return obj;
        }

        inline fn tombstone(a: *const Object) bool {
            return (a.timestamp & tombstone_bit) != 0;
        }
    };

    const _ObjectsCache = CacheMapType(
        PrimaryKey,
        Object,
        ObjectsCacheHelpers.key_from_value,
        ObjectsCacheHelpers.hash,
        ObjectsCacheHelpers.tombstone_from_key,
        ObjectsCacheHelpers.tombstone,
    );

    return struct {
        const Groove = @This();

        pub const ObjectTree = _ObjectTree;
        pub const IdTree = _IdTree;
        pub const IndexTrees = _IndexTrees;
        pub const ObjectsCache = _ObjectsCache;
        pub const config = groove_options;

        const Grid = GridType(Storage);
        const ManifestLog = ManifestLogType(Storage);

        const Callback = *const fn (*Groove) void;

        const trees_total = @as(usize, 1) + @intFromBool(has_id) + std.meta.fields(IndexTrees).len;
        const TreesBitSet = std.StaticBitSet(trees_total);

        const PrefetchKeys = std.AutoHashMapUnmanaged(
            union(enum) {
                id: PrimaryKey,
                timestamp: u64,
            },
            struct { level: u8 },
        );

        pub const ScanBuilder = if (has_scan) ScanBuilderType(Groove, Storage) else void;

        compacting: ?struct {
            /// Count which tree compactions are in progress.
            pending: TreesBitSet = TreesBitSet.initFull(),
            callback: Callback,
        } = null,

        grid: *Grid,
        objects: ObjectTree,
        ids: IdTree,
        indexes: IndexTrees,

        /// Object IDs enqueued to be prefetched.
        /// Prefetching ensures that point lookups against the latest snapshot are synchronous.
        /// This shields state machine implementations from the challenges of concurrency and I/O,
        /// and enables simple state machine function signatures that commit writes atomically.
        prefetch_keys: PrefetchKeys,

        /// The snapshot to prefetch from.
        prefetch_snapshot: ?Snapshot,

        /// This is used to accelerate point lookups and is not used for range queries.
        /// It's also where prefetched data is loaded into, so we don't have a different
        /// prefetch cache to our object cache.
        ///
        /// The values cache is only used for the latest snapshot for simplicity.
        /// Earlier snapshots will still be able to utilize the block cache.
        ///
        /// The values cache is updated on every `insert()`/`upsert()`/`remove()` and stores
        /// a duplicate of data that's already in table_mutable. This is done because
        /// keeping table_mutable as an array, and simplifying the compaction path
        /// is faster than trying to amortize and save memory.
        ///
        /// Invariant: if something is in the mutable or immutable table, it _must_ exist in our
        /// object cache.
        objects_cache: ObjectsCache,

        scan_builder: ScanBuilder,

        pub const Options = struct {
            /// The maximum number of objects that might be prefetched and not modified by a batch.
            prefetch_entries_for_read_max: u32,
            /// The maximum number of objects that might be prefetched and then modified by a batch.
            prefetch_entries_for_update_max: u32,
            cache_entries_max: u32,

            tree_options_object: ObjectTree.Options,
            tree_options_id: if (has_id) IdTree.Options else void,
            tree_options_index: IndexTreeOptions,
        };

        pub fn init(
            allocator: mem.Allocator,
            node_pool: *NodePool,
            grid: *Grid,
            options: Options,
        ) !Groove {
            var objects_cache = try ObjectsCache.init(allocator, .{
                .cache_value_count_max = options.cache_entries_max,

                // In the worst case, each Map must be able to store the value_count_max (to
                // contain either TableMutable or TableImmutable) as well as the maximum number of
                // prefetches a bar may perform, excluding prefetches already accounted for by
                // value_count_max.
                .map_value_count_max = @as(u32, ObjectTree.Table.value_count_max) +
                    (options.prefetch_entries_for_read_max * constants.lsm_batch_multiple),

                // Scopes are limited to a single beat, so the maximum number of entries in a
                // single scope is value_count_max / constants.lsm_batch_multiple.
                .scope_value_count_max = @divExact(ObjectTree.Table.value_count_max, constants.lsm_batch_multiple),

                .name = @typeName(Object),
            });
            errdefer objects_cache.deinit(allocator);

            // Initialize the object LSM tree.
            var object_tree = try ObjectTree.init(
                allocator,
                node_pool,
                grid,
                .{
                    .id = @field(groove_options.ids, "timestamp"),
                    .name = @typeName(Object),
                },
                options.tree_options_object,
            );
            errdefer object_tree.deinit(allocator);

            var id_tree = if (!has_id) {} else (try IdTree.init(
                allocator,
                node_pool,
                grid,
                .{
                    .id = @field(groove_options.ids, "id"),
                    .name = @typeName(Object) ++ ".id",
                },
                options.tree_options_id,
            ));
            errdefer if (has_id) id_tree.deinit(allocator);

            var index_trees_initialized: usize = 0;
            var index_trees: IndexTrees = undefined;

            // Make sure to deinit initialized index LSM trees on error.
            errdefer inline for (std.meta.fields(IndexTrees), 0..) |field, field_index| {
                if (index_trees_initialized >= field_index + 1) {
                    @field(index_trees, field.name).deinit(allocator);
                }
            };

            // Initialize index LSM trees.
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(index_trees, field.name) = try field.type.init(
                    allocator,
                    node_pool,
                    grid,
                    .{
                        .id = @field(groove_options.ids, field.name),
                        .name = @typeName(Object) ++ "." ++ field.name,
                    },
                    @field(options.tree_options_index, field.name),
                );
                index_trees_initialized += 1;
            }

            var prefetch_keys = PrefetchKeys{};
            try prefetch_keys.ensureTotalCapacity(
                allocator,
                options.prefetch_entries_for_read_max + options.prefetch_entries_for_update_max,
            );
            errdefer prefetch_keys.deinit(allocator);

            var scan_builder = if (has_scan) try ScanBuilder.init(allocator) else {};
            errdefer if (has_scan) scan_builder.deinit(allocator);

            return Groove{
                .grid = grid,
                .objects = object_tree,
                .ids = id_tree,
                .indexes = index_trees,

                .prefetch_keys = prefetch_keys,
                .prefetch_snapshot = null,
                .objects_cache = objects_cache,

                .scan_builder = scan_builder,
            };
        }

        pub fn deinit(groove: *Groove, allocator: mem.Allocator) void {
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).deinit(allocator);
            }

            groove.objects.deinit(allocator);
            if (has_id) groove.ids.deinit(allocator);

            groove.prefetch_keys.deinit(allocator);
            groove.objects_cache.deinit(allocator);

            if (has_scan) groove.scan_builder.deinit(allocator);

            groove.* = undefined;
        }

        pub fn reset(groove: *Groove) void {
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).reset();
            }
            groove.objects.reset();
            if (has_id) groove.ids.reset();

            groove.prefetch_keys.clearRetainingCapacity();
            groove.objects_cache.reset();

            if (has_scan) groove.scan_builder.reset();

            groove.* = .{
                .grid = groove.grid,
                .objects = groove.objects,
                .ids = groove.ids,
                .indexes = groove.indexes,
                .prefetch_keys = groove.prefetch_keys,
                .prefetch_snapshot = null,
                .objects_cache = groove.objects_cache,
                .scan_builder = groove.scan_builder,
            };
        }

        pub fn get(groove: *const Groove, key: PrimaryKey) ?*const Object {
            return groove.objects_cache.get(key);
        }

        /// Must be called directly before the state machine begins queuing ids for prefetch.
        /// When `snapshot` is null, prefetch from the current snapshot.
        pub fn prefetch_setup(groove: *Groove, snapshot: ?Snapshot) void {
            // We currently don't have anything that uses or tests snapshots. Leave this
            // here as a warning that they're not fully tested yet.
            assert(snapshot == null);

            const snapshot_target = snapshot orelse Snapshot.latest;
            assert(snapshot_target.timestamp <= Snapshot.latest.timestamp);

            groove.prefetch_snapshot = snapshot_target;
            assert(groove.prefetch_keys.count() == 0);
        }

        /// This must be called by the state machine for every key to be prefetched.
        /// We tolerate duplicate IDs enqueued by the state machine.
        /// For example, if all unique operations require the same two dependencies.
        pub fn prefetch_enqueue(groove: *Groove, key: PrimaryKey) void {
            if (has_id) {
                if (!groove.ids.key_range_contains(groove.prefetch_snapshot.?, key)) return;

                if (groove.objects_cache.has(key)) {
                    return;
                }

                groove.prefetch_from_memory_by_id(key);
            } else {
                if (groove.objects_cache.has(key)) {
                    return;
                }

                groove.prefetch_from_memory_by_timestamp(key);
            }
        }

        /// This function attempts to prefetch a value for the given id from the IdTree's
        /// table blocks in the grid cache.
        /// If found in the IdTree, we attempt to prefetch a value for the timestamp.
        fn prefetch_from_memory_by_id(groove: *Groove, id: PrimaryKey) void {
            switch (groove.ids.lookup_from_levels_cache(
                groove.prefetch_snapshot.?,
                id,
            )) {
                .negative => {},
                .positive => |id_tree_value| {
                    if (IdTreeValue.tombstone(id_tree_value)) return;
                    groove.prefetch_from_memory_by_timestamp(id_tree_value.timestamp);
                },
                .possible => |level| {
                    groove.prefetch_keys.putAssumeCapacity(
                        .{ .id = id },
                        .{ .level = level },
                    );
                },
            }
        }

        /// This function attempts to prefetch a value for the timestamp from the ObjectTree's
        /// table blocks in the grid cache.
        fn prefetch_from_memory_by_timestamp(groove: *Groove, timestamp: u64) void {
            switch (groove.objects.lookup_from_levels_cache(
                groove.prefetch_snapshot.?,
                timestamp,
            )) {
                .negative => {},
                .positive => |object| {
                    assert(!ObjectTreeHelpers(Object).tombstone(object));
                    groove.objects_cache.upsert(object);
                },
                .possible => |level| {
                    groove.prefetch_keys.putAssumeCapacity(
                        .{ .timestamp = timestamp },
                        .{ .level = level },
                    );
                },
            }
        }
        /// Ensure the objects corresponding to all ids enqueued with prefetch_enqueue() are
        /// available in `objects_cache`.
        pub fn prefetch(
            groove: *Groove,
            callback: *const fn (*PrefetchContext) void,
            context: *PrefetchContext,
        ) void {
            context.* = .{
                .groove = groove,
                .callback = callback,
                .snapshot = groove.prefetch_snapshot.?,
                .key_iterator = groove.prefetch_keys.iterator(),
            };
            groove.prefetch_snapshot = null;
            context.start_workers();
        }

        pub const PrefetchContext = struct {
            groove: *Groove,
            callback: *const fn (*PrefetchContext) void,
            snapshot: Snapshot,

            key_iterator: PrefetchKeys.Iterator,
            /// The goal is to fully utilize the disk I/O to ensure the prefetch completes as
            /// quickly as possible, so we run multiple lookups in parallel based on the max
            /// I/O depth of the Grid.
            workers: [Grid.read_iops_max]PrefetchWorker = undefined,
            /// The number of workers that are currently running in parallel.
            workers_pending: u32 = 0,

            next_tick: Grid.NextTick = undefined,

            fn start_workers(context: *PrefetchContext) void {
                assert(context.workers_pending == 0);

                // Track an extra "worker" that will finish after the loop.
                // This allows the callback to be called asynchronously on `next_tick`
                // if all workers are finished synchronously.
                context.workers_pending += 1;

                for (&context.workers, 1..) |*worker, i| {
                    assert(context.workers_pending == i);

                    worker.* = .{ .context = context };
                    context.workers_pending += 1;
                    worker.lookup_start_next();

                    // If the worker finished synchronously (e.g `workers_pending` decreased),
                    // we don't need to start new ones.
                    if (context.workers_pending == i) break;
                }

                assert(context.workers_pending > 0);
                context.workers_pending -= 1;

                if (context.workers_pending == 0) {
                    // All workers finished synchronously,
                    // calling the callback on `next_tick`.
                    context.groove.grid.on_next_tick(worker_next_tick, &context.next_tick);
                }
            }

            fn worker_next_tick(completion: *Grid.NextTick) void {
                const context = @fieldParentPtr(PrefetchContext, "next_tick", completion);
                assert(context.workers_pending == 0);
                context.finish();
            }

            fn worker_finished(context: *PrefetchContext) void {
                context.workers_pending -= 1;
                if (context.workers_pending == 0) context.finish();
            }

            fn finish(context: *PrefetchContext) void {
                assert(context.workers_pending == 0);

                assert(context.key_iterator.next() == null);
                context.groove.prefetch_keys.clearRetainingCapacity();
                assert(context.groove.prefetch_keys.count() == 0);

                context.callback(context);
            }
        };

        pub const PrefetchWorker = struct {
            // Since lookup contexts are used one at a time, it's safe to access
            // the union's fields and reuse the same memory for all context instances.
            // Can't use extern/packed union as the LookupContextes aren't ABI compliant.
            const LookupContext = union(enum) {
                id: if (has_id) IdTree.LookupContext else void,
                object: ObjectTree.LookupContext,

                pub const Field = std.meta.FieldEnum(LookupContext);
                pub fn FieldType(comptime field: Field) type {
                    return std.meta.fieldInfo(LookupContext, field).type;
                }

                pub inline fn parent(
                    comptime field: Field,
                    completion: *FieldType(field),
                ) *PrefetchWorker {
                    const lookup = @fieldParentPtr(LookupContext, @tagName(field), completion);
                    return @fieldParentPtr(PrefetchWorker, "lookup", lookup);
                }

                pub inline fn get(self: *LookupContext, comptime field: Field) *FieldType(field) {
                    self.* = @unionInit(LookupContext, @tagName(field), undefined);
                    return &@field(self, @tagName(field));
                }
            };

            context: *PrefetchContext,
            lookup: LookupContext = undefined,

            fn lookup_start_next(worker: *PrefetchWorker) void {
                const prefetch_entry = worker.context.key_iterator.next() orelse {
                    worker.context.worker_finished();
                    return;
                };

                // prefetch_enqueue() ensures that the tree's cache is checked before queueing the
                // object for prefetching. If not in the LSM tree's cache, the object must be read
                // from disk and added to the auxiliary prefetch_objects hash map.
                switch (prefetch_entry.key_ptr.*) {
                    .id => |id| {
                        if (has_id) {
                            worker.context.groove.ids.lookup_from_levels_storage(.{
                                .callback = lookup_id_callback,
                                .context = worker.lookup.get(.id),
                                .snapshot = worker.context.snapshot,
                                .key = id,
                                .level_min = prefetch_entry.value_ptr.level,
                            });
                        } else unreachable;
                    },
                    .timestamp => |timestamp| {
                        worker.context.groove.objects.lookup_from_levels_storage(.{
                            .callback = lookup_object_callback,
                            .context = worker.lookup.get(.object),
                            .snapshot = worker.context.snapshot,
                            .key = timestamp,
                            .level_min = prefetch_entry.value_ptr.level,
                        });
                    },
                }
            }

            fn lookup_id_callback(
                completion: *IdTree.LookupContext,
                result: ?*const IdTreeValue,
            ) void {
                const worker = LookupContext.parent(.id, completion);
                worker.lookup = undefined;

                if (result) |id_tree_value| {
                    if (!id_tree_value.tombstone()) {
                        worker.lookup_by_timestamp(id_tree_value.timestamp);
                        return;
                    }
                }

                worker.lookup_start_next();
            }

            fn lookup_by_timestamp(worker: *PrefetchWorker, timestamp: u64) void {
                switch (worker.context.groove.objects.lookup_from_levels_cache(
                    worker.context.snapshot,
                    timestamp,
                )) {
                    .negative => {
                        lookup_object_callback(worker.lookup.get(.object), null);
                    },
                    .positive => |value| {
                        lookup_object_callback(worker.lookup.get(.object), value);
                    },
                    .possible => |level_min| {
                        worker.context.groove.objects.lookup_from_levels_storage(.{
                            .callback = lookup_object_callback,
                            .context = worker.lookup.get(.object),
                            .snapshot = worker.context.snapshot,
                            .key = timestamp,
                            .level_min = level_min,
                        });
                    },
                }
            }

            fn lookup_object_callback(
                completion: *ObjectTree.LookupContext,
                result: ?*const Object,
            ) void {
                const worker = LookupContext.parent(.object, completion);
                worker.lookup = undefined;

                if (result) |object| {
                    assert(!ObjectTreeHelpers(Object).tombstone(object));
                    worker.context.groove.objects_cache.upsert(object);
                } else {
                    assert(!has_id);
                }

                worker.lookup_start_next();
            }
        };

        /// Insert the value into the objects tree and associated index trees. It's up to the
        /// caller to ensure it doesn't already exist.
        pub fn insert(groove: *Groove, object: *const Object) void {
            if (constants.verify) {
                assert(!groove.objects_cache.has(@field(object, primary_field)));
            }

            groove.objects_cache.upsert(object);

            if (has_id) {
                groove.ids.put(&IdTreeValue{ .id = object.id, .timestamp = object.timestamp });
                groove.ids.key_range_update(object.id);
            }
            groove.objects.put(object);

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);

                const index = Helper.derive_index(object);
                // Fields with `value == 0` are not indexed.
                if (index != 0) {
                    @field(groove.indexes, field.name).put(&.{
                        .timestamp = object.timestamp,
                        .field = index,
                    });
                }
            }
        }

        /// Update the value. Requires the old object to be provided.
        /// Update the object and index trees by diff'ing the old and new values.
        pub fn update(
            groove: *Groove,
            values: struct { old: *const Object, new: *const Object },
        ) void {
            const old = values.old;
            const new = values.new;

            if (constants.verify) {
                const old_from_cache = groove.objects_cache.get(@field(old, primary_field)).?;

                // While all that's actually required is that the _contents_ of the old_from_cache
                // and old objects are identical, in current usage they're always the same piece of
                // memory. We'll assert that for now, and this can be weakened in future if
                // required.
                assert(old_from_cache == old);
            }

            // Sanity check to ensure the caller didn't accidentally pass in an alias.
            assert(new != old);

            if (has_id) assert(old.id == new.id);
            assert(old.timestamp == new.timestamp);

            // The ID can't change, so no need to update the ID tree. Update the object tree entry
            // if any of the fields (even ignored) are different. We assume the caller will pass in
            // an object that has changes.
            // Unlike the index trees, the new and old values in the object tree share the same
            // key. Therefore put() is sufficient to overwrite the old value.
            if (constants.verify) {
                const tombstone = ObjectTreeHelpers(Object).tombstone;
                const key_from_value = ObjectTreeHelpers(Object).key_from_value;

                assert(!stdx.equal_bytes(Object, old, new));
                assert(key_from_value(old) == key_from_value(new));
                assert(!tombstone(old) and !tombstone(new));
            }

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);
                const old_index = Helper.derive_index(old);
                const new_index = Helper.derive_index(new);

                // Only update the indexes that change.
                if (old_index != new_index) {
                    if (old_index != 0) {
                        @field(groove.indexes, field.name).remove(&.{
                            .timestamp = old.timestamp,
                            .field = old_index,
                        });
                    }

                    if (new_index != 0) {
                        @field(groove.indexes, field.name).put(&.{
                            .timestamp = new.timestamp,
                            .field = new_index,
                        });
                    }
                }
            }

            // Putting the objects_cache upsert after the index tree updates is critical:
            // We diff the old and new objects, but the old object will be a pointer into the
            // objects_cache. If we upsert first, there's a high chance old.* == new.* (always,
            // unless old comes from the stash) and no secondary indexes will be updated!
            groove.objects_cache.upsert(new);
            groove.objects.put(new);
        }

        /// Asserts that the object with the given PrimaryKey exists.
        pub fn remove(groove: *Groove, key: PrimaryKey) void {
            // TODO: Nothing currently calls or tests this method. The forest fuzzer should be
            // extended to cover it.
            assert(false);

            const object = groove.objects_cache.get(key).?;

            groove.objects.remove(object);
            if (has_id) {
                groove.ids.remove(&IdTreeValue{ .id = object.id, .timestamp = object.timestamp });
            }

            groove.objects_cache.remove(key);

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);

                const index = Helper.derive_index(object);
                if (index != 0) {
                    @field(groove.indexes, field.name).remove(&.{
                        .timestamp = object.timestamp,
                        .field = index,
                    });
                }
            }
        }

        pub fn scope_open(groove: *Groove) void {
            groove.objects_cache.scope_open();

            if (has_id) {
                groove.ids.scope_open();
            }
            groove.objects.scope_open();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).scope_open();
            }
        }

        pub fn scope_close(groove: *Groove, mode: ScopeCloseMode) void {
            groove.objects_cache.scope_close(mode);

            if (has_id) {
                groove.ids.scope_close(mode);
            }
            groove.objects.scope_close(mode);

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).scope_close(mode);
            }
        }

        pub fn open_commence(groove: *Groove, manifest_log: *ManifestLog) void {
            assert(groove.compacting == null);

            if (has_id) groove.ids.open_commence(manifest_log);
            groove.objects.open_commence(manifest_log);

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).open_commence(manifest_log);
            }
        }

        pub fn open_complete(groove: *Groove) void {
            assert(groove.compacting == null);

            if (has_id) groove.ids.open_complete();
            groove.objects.open_complete();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).open_complete();
            }
        }

        pub fn compact(groove: *Groove, callback: Callback, op: u64) void {
            assert(groove.compacting == null);

            // Compact the IdTree and ObjectTree.
            if (has_id) groove.ids.compact(compact_tree_callback(.ids), op);
            groove.objects.compact(compact_tree_callback(.objects), op);

            inline for (std.meta.fields(IndexTrees)) |field| {
                const compact_tree_callback_ = compact_tree_callback(.{ .index = field.name });
                @field(groove.indexes, field.name).compact(compact_tree_callback_, op);
            }

            // Compact the objects_cache on the last beat of the bar, just like the trees do to
            // their mutable tables.
            const compaction_beat = op % constants.lsm_batch_multiple;
            if (compaction_beat == constants.lsm_batch_multiple - 1) {
                groove.objects_cache.compact();
            }

            groove.compacting = .{ .callback = callback };
        }

        fn compact_tree_callback(
            comptime tree_field: TreeField,
        ) *const fn (*TreeFor(tree_field)) void {
            return struct {
                fn tree_callback(tree: *TreeFor(tree_field)) void {
                    // Derive the groove pointer from the tree using the tree_field.
                    const groove = switch (tree_field) {
                        .ids => @fieldParentPtr(Groove, "ids", tree),
                        .objects => @fieldParentPtr(Groove, "objects", tree),
                        .index => |field| blk: {
                            const indexes: *align(@alignOf(IndexTrees)) IndexTrees =
                                @alignCast(@fieldParentPtr(IndexTrees, field, tree));
                            break :blk @fieldParentPtr(Groove, "indexes", indexes);
                        },
                    };

                    assert(groove.compacting.?.pending.isSet(comptime tree_field.offset()));
                    groove.compacting.?.pending.unset(comptime tree_field.offset());

                    groove.compact_callback();
                }
            }.tree_callback;
        }

        fn compact_callback(groove: *Groove) void {
            assert(groove.compacting != null);

            // Guard until all pending sync ops complete.
            if (groove.compacting.?.pending.count() > 0) return;

            const callback = groove.compacting.?.callback;
            groove.compacting = null;
            callback(groove);
        }

        const TreeField = union(enum) {
            ids,
            objects,
            index: []const u8,

            fn offset(comptime field: TreeField) usize {
                switch (field) {
                    .objects => return 0,
                    .ids => return 1,
                    .index => |index_tree_name| {
                        for (std.meta.fields(IndexTrees), 0..) |index_tree_field, i| {
                            if (std.mem.eql(u8, index_tree_field.name, index_tree_name)) {
                                return @as(usize, 1) + @intFromBool(has_id) + i;
                            }
                        } else unreachable;
                    },
                }
            }
        };

        /// Returns LSM tree type for the given index field name (or ObjectTree if null).
        fn TreeFor(comptime tree_field: TreeField) type {
            return switch (tree_field) {
                .ids => IdTree,
                .objects => ObjectTree,
                .index => |field| @TypeOf(@field(@as(IndexTrees, undefined), field)),
            };
        }

        pub fn compact_end(groove: *Groove) void {
            assert(groove.compacting == null);

            if (has_id) groove.ids.compact_end();
            groove.objects.compact_end();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).compact_end();
            }
        }

        pub fn assert_between_bars(groove: *const Groove) void {
            assert(groove.compacting == null);

            if (has_id) groove.ids.assert_between_bars();
            groove.objects.assert_between_bars();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).assert_between_bars();
            }
        }
    };
}

test "Groove" {
    const Transfer = @import("../tigerbeetle.zig").Transfer;
    const Storage = @import("../storage.zig").Storage;

    const Groove = GrooveType(
        Storage,
        Transfer,
        .{
            .ids = .{
                .timestamp = 1,
                .id = 2,
                .debit_account_id = 3,
                .credit_account_id = 4,
                .pending_id = 5,
                .timeout = 6,
                .ledger = 7,
                .code = 8,
                .amount = 9,
            },
            // Doesn't matter for this test.
            .value_count_max = .{
                .timestamp = constants.lsm_batch_multiple,
                .id = constants.lsm_batch_multiple,
                .debit_account_id = constants.lsm_batch_multiple,
                .credit_account_id = constants.lsm_batch_multiple,
                .pending_id = constants.lsm_batch_multiple,
                .timeout = constants.lsm_batch_multiple,
                .ledger = constants.lsm_batch_multiple,
                .code = constants.lsm_batch_multiple,
                .amount = constants.lsm_batch_multiple,
            },
            .ignored = [_][]const u8{ "user_data_128", "user_data_64", "user_data_32", "flags" },
            .derived = .{},
        },
    );

    std.testing.refAllDecls(Groove);
    std.testing.refAllDecls(Groove.PrefetchWorker);
    std.testing.refAllDecls(Groove.PrefetchContext);
}
