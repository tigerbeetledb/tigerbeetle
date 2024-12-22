const std = @import("std");
const assert = std.debug.assert;

const Allocator = std.mem.Allocator;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const is_composite_key = @import("composite_key.zig").is_composite_key;

const ScanTreeType = @import("scan_tree.zig").ScanTreeType;
const ScanMergeUnionType = @import("scan_merge.zig").ScanMergeUnionType;
const ScanMergeIntersectionType = @import("scan_merge.zig").ScanMergeIntersectionType;
const ScanMergeDifferenceType = @import("scan_merge.zig").ScanMergeDifferenceType;
const ScanBuffer = @import("scan_buffer.zig").ScanBuffer;
const ScanState = @import("scan_state.zig").ScanState;

const Direction = @import("../direction.zig").Direction;
const TimestampRange = @import("timestamp_range.zig").TimestampRange;

const Error = @import("scan_buffer.zig").Error;

/// ScanBuilder is a helper to create and combine scans using
/// any of the Groove's indexes.
pub fn ScanBuilderType(
    // TODO: Instead of a single Groove per ScanType, introduce the concept of Orthogonal Grooves.
    // For example, indexes from the Grooves `Transfers` and `PendingTransfers` can be
    // used together in the same query, and the timestamp they produce can be used for
    // lookups in either Grooves:
    // ```
    // SELECT Transfers WHERE Transfers.code=1 AND Transfers.pending_status=posted.
    // ```
    //
    // Although the relation between orthogonal grooves is always 1:1 by the timestamp,
    // when looking up an object in the Groove `A` by a timestamp found in the Groove `B`, it will
    // require additional information to correctly assert if `B` "must have" or "may have" a
    // corresponding match in `A`.
    // E.g.: Every AccountBalance **must have** a corresponding Account, however the opposite
    // isn't true.
    // ```
    // SELECT AccountBalances WHERE Accounts.user_data_32=100
    // ```
    comptime Groove: type,
    comptime Storage: type,
) type {
    return struct {
        const ScanBuilder = @This();

        pub const Scan = ScanType(Groove, Storage);

        /// Each `ScanTree` consumes memory and I/O, so they are limited by `lsm_scans_max`.
        scans: *[constants.lsm_scans_max]Scan,
        scan_count: u32 = 0,

        /// Merging `ScanTree`s does not require additional resources, so `ScanMerge`s are stored
        /// in a separate buffer limited to `lsm_scans_max - 1`.
        /// If `lsm_scans_max = 4`, we can have at most 4 scans and 3 merge operations:
        /// M₁(M₂(S₁, S₂), M₃(S₃, S₄)).
        merges: *[constants.lsm_scans_max - 1]Scan,
        merge_count: u32 = 0,

        pub fn init(self: *ScanBuilder, allocator: Allocator) !void {
            self.* = .{
                .scans = undefined,
                .merges = undefined,
            };

            self.scans = try allocator.create([constants.lsm_scans_max]Scan);
            errdefer allocator.destroy(self.scans);

            self.merges = try allocator.create([constants.lsm_scans_max - 1]Scan);
            errdefer allocator.destroy(self.merges);
        }

        pub fn deinit(self: *ScanBuilder, allocator: Allocator) void {
            allocator.destroy(self.scans);
            allocator.destroy(self.merges);

            self.* = undefined;
        }

        pub fn reset(self: *ScanBuilder) void {
            self.* = .{
                .scans = self.scans,
                .merges = self.merges,
            };
        }

        /// Initializes a Scan over the secondary index specified by `index`,
        /// searching for an exact match in the `CompositeKey`'s prefix.
        /// Produces the criteria equivalent to `WHERE field = $value`.
        /// Results are ordered by `timestamp`.
        pub fn scan_prefix(
            self: *ScanBuilder,
            comptime index: std.meta.FieldEnum(Groove.IndexTrees),
            buffer: *const ScanBuffer,
            snapshot: u64,
            prefix: CompositeKeyPrefixType(index),
            timestamp_range: TimestampRange,
            direction: Direction,
        ) *Scan {
            const field = comptime std.enums.nameCast(std.meta.FieldEnum(Scan.Dispatcher), index);
            const ScanImpl = ScanImplType(field);
            return self.scan_add(
                field,
                ScanImpl.init(
                    &@field(self.groove().indexes, @tagName(index)),
                    buffer,
                    snapshot,
                    key_from_value(index, prefix, timestamp_range.min),
                    key_from_value(index, prefix, timestamp_range.max),
                    direction,
                ),
            ) catch unreachable; //TODO: define error handling for the query API.
        }

        /// Initializes a Scan over the `id` searching for an exact match.
        /// Produces the criteria equivalent to `WHERE id = $value`.
        /// Results are always unique.
        pub fn scan_id(
            self: *ScanBuilder,
            buffer: *const ScanBuffer,
            snapshot: u64,
            id: u128,
            direction: Direction,
        ) *Scan {
            comptime assert(Groove.IdTree != void);

            const ScanImpl = ScanImplType(.id);
            return self.scan_add(
                .id,
                ScanImpl.init(
                    &self.groove().ids,
                    buffer,
                    snapshot,
                    id,
                    id,
                    direction,
                ),
            ) catch unreachable; //TODO: define error handling for the query API.
        }

        /// Initializes a Scan over a timestamp range.
        /// Produces the criteria equivalent to `WHERE timestamp BETWEEN $min AND $max`.
        /// Results are ordered by `timestamp`.
        pub fn scan_timestamp(
            self: *ScanBuilder,
            buffer: *const ScanBuffer,
            snapshot: u64,
            timestamp_range: TimestampRange,
            direction: Direction,
        ) *Scan {
            const ScanImpl = ScanImplType(.timestamp);
            return self.scan_add(
                .timestamp,
                ScanImpl.init(
                    &self.groove().objects,
                    buffer,
                    snapshot,
                    timestamp_range.min,
                    timestamp_range.max,
                    direction,
                ),
            ) catch unreachable; //TODO: define error handling for the query API.
        }

        /// Initializes a Scan performing the union operation over multiple scans.
        /// E.g. S₁ ∪ S₂ ∪ Sₙ.
        /// Produces the criteria equivalent to
        /// `WHERE <condition_1> OR <condition_2> OR <condition_N>`.
        ///
        /// All scans must yield results in the same direction.
        pub fn merge_union(
            self: *ScanBuilder,
            scans: []const *Scan,
        ) *Scan {
            const Impl = ScanImplType(.merge_union);
            return self.merge_add(
                .merge_union,
                Impl.init(scans),
            ) catch unreachable; //TODO: define error handling for the query API.;
        }

        /// Initializes a Scan performing the intersection operation over multiple scans.
        /// E.g. S₁ ∩ S₂ ∩ Sₙ.
        /// Produces the criteria equivalent to
        /// WHERE <condition_1> AND <condition_2> AND <condition_N>`.
        ///
        /// All scans must yield results in the same direction.
        pub fn merge_intersection(
            self: *ScanBuilder,
            scans: []const *Scan,
        ) *Scan {
            const Impl = ScanImplType(.merge_intersection);
            return self.merge_add(
                .merge_intersection,
                Impl.init(scans),
            ) catch unreachable; //TODO: define error handling for the query API.;
        }

        /// Initializes a Scan performing the difference (minus) of two scans.
        /// E.g. S₁ - S₂.
        /// Produces the criteria equivalent to
        /// `WHERE <condition_1> AND NOT <condition_2>`.
        ///
        /// Both scans must yield results in the same direction.
        pub fn merge_difference(
            self: *ScanBuilder,
            scan_a: *Scan,
            scan_b: *Scan,
        ) *Scan {
            _ = scan_b;
            _ = scan_a;
            _ = self;
            stdx.unimplemented("merge_difference not implemented");
        }

        fn scan_add(
            self: *ScanBuilder,
            comptime field: std.meta.FieldEnum(Scan.Dispatcher),
            init_expression: ScanImplType(field),
        ) Error!*Scan {
            if (self.scan_count == self.scans.len) {
                return Error.ScansMaxExceeded;
            }

            const scan = &self.scans[self.scan_count];
            self.scan_count += 1;
            assert(self.scan_count <= self.scans.len);

            scan.* = .{
                .dispatcher = @unionInit(
                    Scan.Dispatcher,
                    @tagName(field),
                    init_expression,
                ),
                .assigned = false,
            };

            return scan;
        }

        fn merge_add(
            self: *ScanBuilder,
            comptime field: std.meta.FieldEnum(Scan.Dispatcher),
            init_expression: ScanImplType(field),
        ) Error!*Scan {
            if (self.merge_count == self.merges.len) {
                return Error.ScansMaxExceeded;
            }

            const scan = &self.merges[self.merge_count];
            self.merge_count += 1;
            assert(self.merge_count <= self.merges.len);

            scan.* = .{
                .dispatcher = @unionInit(
                    Scan.Dispatcher,
                    @tagName(field),
                    init_expression,
                ),
                .assigned = false,
            };

            return scan;
        }

        fn CompositeKeyType(comptime index: std.meta.FieldEnum(Groove.IndexTrees)) type {
            const IndexTree = std.meta.fieldInfo(Groove.IndexTrees, index).type;
            return IndexTree.Table.Value;
        }

        fn CompositeKeyPrefixType(comptime index: std.meta.FieldEnum(Groove.IndexTrees)) type {
            const CompositeKey = CompositeKeyType(index);
            return std.meta.fieldInfo(CompositeKey, .field).type;
        }

        fn ScanImplType(comptime field: std.meta.FieldEnum(Scan.Dispatcher)) type {
            return std.meta.fieldInfo(Scan.Dispatcher, field).type;
        }

        fn key_from_value(
            comptime field: std.meta.FieldEnum(Groove.IndexTrees),
            prefix: CompositeKeyPrefixType(field),
            timestamp: u64,
        ) CompositeKeyType(field).Key {
            return CompositeKeyType(field).key_from_value(&.{
                .field = prefix,
                .timestamp = timestamp,
            });
        }

        inline fn groove(self: *ScanBuilder) *Groove {
            return @alignCast(@fieldParentPtr("scan_builder", self));
        }
    };
}

/// Common `Scan` interface.
///
/// Allows combining different underlying scans into a single output,
/// for example `(A₁ ∪ A₂) ∩ B₁` produces the criteria equivalent to
/// `WHERE (<condition_a1> OR <condition_a2>) AND <condition_b>`.
pub fn ScanType(
    comptime Groove: type,
    comptime Storage: type,
) type {
    return struct {
        const Scan = @This();

        /// This pattern of callback `fn(*Context, *Scan)` with the `Context` holding the function
        /// pointer is well suited for this use case as it allows composing multiple scans from
        /// a pool without requiring the caller to keep track of the reference of the topmost scan.
        /// Example:
        /// ```
        /// var scan1: *Scan = ... // Add some condition
        /// var scan2: *Scan = ... // Add another condition
        /// var scan3: *Scan = merge_union(scan1, scan2); // Merge both scans.
        /// scan3.read(&context); // scan3 will be returned during the callback.
        /// ```
        pub const Callback = *const fn (*Context, *Scan) void;
        pub const Context = struct {
            callback: Callback,
        };

        /// Comptime dispatcher for all scan implementations that share the same interface.
        /// Generates a tagged union with an specialized `ScanTreeType` for each queryable field in
        /// the `Groove` (e.g. `timestamp`, `id` if present, and secondary indexes), plus a
        /// `ScanMergeType` for each merge operation (e.g. union, intersection, and difference).
        ///
        /// Example:
        /// ```
        /// const Dispatcher = union(enum) {
        ///   .timestamp: ScanTree(...),
        ///   .id: ScanTree(...),
        ///   .code: ScanTree(...),
        ///   .ledger: ScanTree(...),
        ///   // ... All other indexes ...
        ///   .merge_union: ...
        ///   .merge_intersection: ...
        ///   .merge_difference: ...
        /// };
        /// ```
        pub const Dispatcher = T: {
            var type_info = @typeInfo(union(enum) {
                timestamp: ScanTreeType(*Context, Groove.ObjectTree, Storage),

                merge_union: ScanMergeUnionType(Groove, Storage),
                merge_intersection: ScanMergeIntersectionType(Groove, Storage),
                merge_difference: ScanMergeDifferenceType(Groove, Storage),
            });

            // Union field for the id tree:
            if (Groove.IdTree != void) {
                const ScanTree = ScanTreeType(*Context, Groove.IdTree, Storage);
                type_info.Union.fields = type_info.Union.fields ++
                    [_]std.builtin.Type.UnionField{.{
                    .name = "id",
                    .type = ScanTree,
                    .alignment = @alignOf(ScanTree),
                }};
            }

            // Union fields for each index tree:
            for (std.meta.fields(Groove.IndexTrees)) |field| {
                const IndexTree = field.type;
                const ScanTree = ScanTreeType(*Context, IndexTree, Storage);
                type_info.Union.fields = type_info.Union.fields ++
                    [_]std.builtin.Type.UnionField{.{
                    .name = field.name,
                    .type = ScanTree,
                    .alignment = @alignOf(ScanTree),
                }};
            }

            // We need a tagged union for dynamic dispatching.
            type_info.Union.tag_type = blk: {
                const union_fields = type_info.Union.fields;
                var tag_fields: [union_fields.len]std.builtin.Type.EnumField =
                    undefined;
                for (&tag_fields, union_fields, 0..) |*tag_field, union_field, i| {
                    tag_field.* = .{
                        .name = union_field.name,
                        .value = i,
                    };
                }

                break :blk @Type(.{ .Enum = .{
                    .tag_type = std.math.IntFittingRange(0, tag_fields.len - 1),
                    .fields = &tag_fields,
                    .decls = &.{},
                    .is_exhaustive = true,
                } });
            };

            break :T @Type(type_info);
        };

        dispatcher: Dispatcher,
        assigned: bool,

        pub fn read(scan: *Scan, context: *Context) void {
            switch (scan.dispatcher) {
                inline else => |*scan_impl, tag| read_dispatch(
                    tag,
                    scan_impl,
                    context,
                ),
            }
        }

        // Comptime generates an specialized callback function for each type.
        // TODO(Zig): remove this function and move this logic to `read`,
        // but for some reason, the Zig compiler can't resolve the correct type.
        fn read_dispatch(
            comptime tag: std.meta.Tag(Dispatcher),
            scan_impl: *std.meta.fieldInfo(Dispatcher, tag).type,
            context: *Context,
        ) void {
            const Impl = @TypeOf(scan_impl.*);
            const on_read_callback = struct {
                fn callback(ctx: *Context, ptr: *Impl) void {
                    ctx.callback(ctx, parent(tag, ptr));
                }
            }.callback;

            scan_impl.read(context, on_read_callback);
        }

        pub fn next(scan: *Scan) error{ReadAgain}!?u64 {
            switch (scan.dispatcher) {
                inline .merge_union,
                .merge_intersection,
                .merge_difference,
                => |*scan_merge| return try scan_merge.next(),
                inline else => |*scan_tree, index| {
                    while (try scan_tree.next()) |value| {
                        const ScanTree = @TypeOf(scan_tree.*);
                        if (ScanTree.Tree.Table.tombstone(&value)) {
                            // When iterating over `ScanTreeType`, it can return a tombstone, which
                            // indicates the value was deleted, and must be ignored in the results.
                            continue;
                        }

                        if (Groove.IdTree != void) {
                            if (index == .id) {
                                // When iterating over `IdTree` it can return a timestamp zero,
                                // which indicates an orphaned id.
                                if (value.timestamp == 0) {
                                    assert(Groove.config.orphaned_ids);
                                    continue;
                                }
                            }
                        }

                        assert(value.timestamp >= TimestampRange.timestamp_min);
                        assert(value.timestamp <= TimestampRange.timestamp_max);
                        return value.timestamp;
                    }
                    return null;
                },
            }
        }

        pub fn state(scan: *const Scan) ScanState {
            switch (scan.dispatcher) {
                inline else => |*scan_impl| return scan_impl.state,
            }
        }

        pub fn snapshot(scan: *const Scan) u64 {
            return switch (scan.dispatcher) {
                inline else => |*scan_impl| scan_impl.snapshot,
            };
        }

        pub fn probe(scan: *Scan, timestamp: u64) void {
            switch (scan.dispatcher) {
                inline .timestamp,
                .merge_union,
                .merge_intersection,
                .merge_difference,
                => |*scan_impl| scan_impl.probe(timestamp),
                inline else => |*scan_impl, tag| {
                    const ScanTree = @TypeOf(scan_impl.*);
                    const Value = ScanTree.Tree.Table.Value;

                    if (comptime is_composite_key(Value)) {
                        const prefix = prefix: {
                            const prefix_min = Value.key_prefix(scan_impl.key_min);
                            const prefix_max = Value.key_prefix(scan_impl.key_max);
                            assert(prefix_min == prefix_max);
                            break :prefix prefix_min;
                        };
                        scan_impl.probe(Value.key_from_value(&.{
                            .field = prefix,
                            .timestamp = timestamp,
                        }));
                    } else {
                        comptime assert(tag == .id);
                        comptime assert(Groove.IdTree != void);

                        // Scans over the IdTree cannot probe for a next timestamp.
                        assert(scan_impl.key_min == scan_impl.key_max);
                    }
                },
            }
        }

        /// Returns the direction of the output timestamp values.
        pub fn direction(scan: *const Scan) Direction {
            switch (scan.dispatcher) {
                inline .timestamp,
                .merge_union,
                .merge_intersection,
                .merge_difference,
                => |*scan_impl| return scan_impl.direction,
                inline else => |*scan_impl, tag| {
                    const ScanTree = @TypeOf(scan_impl.*);
                    const Value = ScanTree.Tree.Table.Value;
                    if (comptime is_composite_key(Value)) {
                        // Secondary indexes can only produce results sorted by timestamp if
                        // scanning over the same key prefix.
                        assert(Value.key_prefix(scan_impl.key_min) ==
                            Value.key_prefix(scan_impl.key_max));
                    } else {
                        comptime assert(tag == .id);
                        comptime assert(Groove.IdTree != void);
                        assert(scan_impl.key_min == scan_impl.key_max);
                    }

                    return scan_impl.direction;
                },
            }
        }

        inline fn parent(
            comptime field: std.meta.FieldEnum(Dispatcher),
            impl: *std.meta.FieldType(Dispatcher, field),
        ) *Scan {
            const dispatcher: *Dispatcher = @alignCast(@fieldParentPtr(
                @tagName(field),
                impl,
            ));
            return @fieldParentPtr("dispatcher", dispatcher);
        }
    };
}
