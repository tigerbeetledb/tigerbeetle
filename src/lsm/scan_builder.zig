const std = @import("std");
const assert = std.debug.assert;

const Allocator = std.mem.Allocator;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");

const ScanTreeType = @import("scan_tree.zig").ScanTreeType;
const ScanMergeUnionType = @import("scan_merge.zig").ScanMergeUnionType;
const ScanMergeIntersectionType = @import("scan_merge.zig").ScanMergeIntersectionType;
const ScanMergeDifferenceType = @import("scan_merge.zig").ScanMergeDifferenceType;
const ScanBuffer = @import("scan_buffer.zig").ScanBuffer;
const ScanState = @import("scan_state.zig").ScanState;
const Snapshot = @import("schema.zig").Snapshot;

const Direction = @import("../direction.zig").Direction;
const TimestampRange = @import("timestamp_range.zig").TimestampRange;

const Error = @import("scan_buffer.zig").Error;

/// ScanBuilder is a helper to create and combine scans using
/// any of the Groove's secondary indexes.
pub fn ScanBuilderType(
    comptime Groove: type,
    comptime Storage: type,
) type {
    comptime assert(std.meta.fields(Groove.IndexTrees).len > 0);

    return struct {
        const ScanBuilder = @This();

        pub const Scan = ScanType(Groove, Storage);

        // Since `ScanTree` consume memory and IO, it is limited by `lsm_scans_max`,
        // however, merging scans does not require resources.
        // E.g, if `lsm_scans_max = 4` we can have at most 3 merge operations:
        // M₁(M₂(C₁, C₂), M₃(C₃, C₄))
        const ScanSlots = stdx.BoundedArray(Scan, constants.lsm_scans_max);
        const MergeSlots = stdx.BoundedArray(Scan, constants.lsm_scans_max - 1);

        scan_slots: *ScanSlots,
        merge_slots: *MergeSlots,

        pub fn init(allocator: Allocator) !ScanBuilder {
            const scan_slots = try allocator.create(ScanSlots);
            errdefer allocator.destroy(scan_slots);
            scan_slots.* = .{};

            const merge_slots = try allocator.create(MergeSlots);
            errdefer allocator.destroy(merge_slots);
            merge_slots.* = .{};

            return ScanBuilder{
                .scan_slots = scan_slots,
                .merge_slots = merge_slots,
            };
        }

        pub fn deinit(self: *ScanBuilder, allocator: Allocator) void {
            allocator.destroy(self.scan_slots);
            allocator.destroy(self.merge_slots);

            self.* = undefined;
        }

        pub fn reset(self: *ScanBuilder) void {
            self.scan_slots.* = .{};
            self.merge_slots.* = .{};

            self.* = .{
                .scan_slots = self.scan_slots,
                .merge_slots = self.merge_slots,
            };
        }

        pub fn Tree(comptime field: std.meta.FieldEnum(Groove.IndexTrees)) type {
            return std.meta.fieldInfo(Groove.IndexTrees, field).type;
        }

        pub fn CompositeKeyType(comptime field: std.meta.FieldEnum(Groove.IndexTrees)) type {
            return Tree(field).Table.Value;
        }

        pub fn CompositeKeyPrefix(comptime field: std.meta.FieldEnum(Groove.IndexTrees)) type {
            const CompositeKey = CompositeKeyType(field);
            return std.meta.fieldInfo(CompositeKey, .field).type;
        }

        fn ScanImplType(comptime field: std.meta.FieldEnum(Scan.Dispatcher)) type {
            return std.meta.fieldInfo(Scan.Dispatcher, field).type;
        }

        fn key_from_value(
            comptime field: std.meta.FieldEnum(Groove.IndexTrees),
            prefix: CompositeKeyPrefix(field),
            timestamp: u64,
        ) CompositeKeyType(field).Key {
            return CompositeKeyType(field).key_from_value(&.{
                .field = prefix,
                .timestamp = timestamp,
            });
        }

        /// Initializes a Scan over the secondary index specified by `index`,
        /// searching for an exact match in the `CompositeKey`'s prefix.
        /// Produces the criteria equivalent to `WHERE field = $value`.
        /// Results are ordered by `timestamp`.
        pub fn scan_prefix(
            self: *ScanBuilder,
            comptime index: std.meta.FieldEnum(Groove.IndexTrees),
            buffer: *const ScanBuffer,
            snapshot: Snapshot,
            value: CompositeKeyPrefix(index),
            timestamp_range: TimestampRange,
            direction: Direction,
        ) *Scan {
            const field = comptime index_to_dispatcher(index);
            const ScanImpl = ScanImplType(field);
            return self.scan_add(
                field,
                ScanImpl.init(
                    self.tree(index),
                    buffer,
                    snapshot,
                    key_from_value(index, value, timestamp_range.min),
                    key_from_value(index, value, timestamp_range.max),
                    direction,
                ),
            ) catch unreachable; //TODO: define error handling for the query API.
        }

        /// Initializes a Scan over the secondary index specified by `index`,
        /// searching for an inclusive range of `CompositeKey`s.
        /// Results are ordered by the (prefix, timestamp) tuple.
        /// Range scans cannot be used in the `merge_{union,intersection,difference}` functions.
        pub fn scan_range(
            self: *ScanBuilder,
            comptime index: std.meta.FieldEnum(Groove.IndexTrees),
            buffer: *const ScanBuffer,
            snapshot: Snapshot,
            min: CompositeKeyType(index),
            max: CompositeKeyType(index),
            direction: Direction,
        ) *Scan {
            const field = comptime index_to_dispatcher(index);
            const ScanImpl = ScanImplType(field);
            return scan_add(
                self,
                field,
                ScanImpl.init(
                    self.tree(index),
                    buffer,
                    snapshot,
                    min.key_from_value(),
                    max.key_from_value(),
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
            _ = scans;
            _ = self;
            stdx.unimplemented("merge_intersection not implemented");
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
            if (self.scan_slots.full()) return Error.ScansMaxExceeded;

            const scan = self.scan_slots.add_one_assume_capacity();
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
            if (self.merge_slots.full()) return Error.ScansMaxExceeded;

            const scan = self.merge_slots.add_one_assume_capacity();
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

        fn groove(self: *ScanBuilder) *Groove {
            return @fieldParentPtr(Groove, "scan_builder", self);
        }

        fn tree(
            self: *ScanBuilder,
            comptime field: std.meta.FieldEnum(Groove.IndexTrees),
        ) *Tree(field) {
            return &@field(self.groove().indexes, @tagName(field));
        }

        fn index_to_dispatcher(
            comptime field: std.meta.FieldEnum(Groove.IndexTrees),
        ) std.meta.FieldEnum(Scan.Dispatcher) {
            return std.meta.stringToEnum(
                std.meta.FieldEnum(Scan.Dispatcher),
                @tagName(field),
            ).?;
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
    comptime assert(std.meta.fields(Groove.IndexTrees).len > 0);

    return struct {
        const Scan = @This();

        /// This pattern of callback `fn(*Context, *Scan)` with the `Context` holding the function
        /// pointer is well suited for this use case as it allows composing multiple scans from
        /// a pool without requiring the caller to keep track of the reference of the topmost scan.
        /// Example:
        /// ```
        /// var scan1: *Scan = scan_groove.equal(...); // Add some condition
        /// var scan2: *Scan = scan_groove.equal(...); // Add another condition
        /// var scan3: *Scan = scan_groove.merge_union(scan1, scan2); // Merge both scans.
        /// scan3.read(&context); // This scan will be returned during the callback.
        /// ```
        pub const Callback = *const fn (*Context, *Scan) void;
        pub const Context = struct {
            callback: Callback,
        };

        /// Generates a tagged union with a specialized ScanTree field for each
        /// index in the groove, plus one field for each `merge` operation that shares
        /// the same interface.
        ///
        /// Example:
        /// ```
        /// const Dispatcher = union(enum) {
        ///   .code: ScanTree(...),
        ///   .ledger: ScanTree(...),
        ///   // ...
        ///   .merge_union: ...
        ///   .merge_intersection: ...
        ///   .merge_difference: ...
        /// };
        /// ```
        pub const Dispatcher = T: {
            var type_info = @typeInfo(union(enum) {
                merge_union: ScanMergeUnionType(Groove, Storage),
                merge_intersection: ScanMergeIntersectionType(Groove, Storage),
                merge_difference: ScanMergeDifferenceType(Groove, Storage),
            });

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
                inline else => |*scan_tree| {
                    while (try scan_tree.next()) |value| {
                        // When iterating over `ScanTreeType` the result is a `CompositeKey`,
                        // check if it's not a tombstone and return only the `timestamp` part.
                        const ScanTree = @TypeOf(scan_tree.*);
                        if (ScanTree.Tree.Table.tombstone(&value)) {
                            continue;
                        }

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

        pub fn snapshot(scan: *const Scan) Snapshot {
            return switch (scan.dispatcher) {
                inline else => |*scan_impl| scan_impl.snapshot,
            };
        }

        /// Returns the direction of the output timestamp values,
        /// or null if the scan can't yield sorted timestamps.
        pub fn timestamp_direction(scan: *const Scan) ?Direction {
            switch (scan.dispatcher) {
                inline .merge_union,
                .merge_intersection,
                .merge_difference,
                => |scan_merge| return scan_merge.direction,
                inline else => |*scan_tree| {
                    const ScanTree = @TypeOf(scan_tree.*);
                    const key_prefix = ScanTree.Tree.Table.Value.key_prefix;
                    return if (key_prefix(scan_tree.key_min) == key_prefix(scan_tree.key_max))
                        scan_tree.direction
                    else
                        null;
                },
            }
        }

        fn parent(
            comptime field: std.meta.FieldEnum(Dispatcher),
            impl: *std.meta.FieldType(Dispatcher, field),
        ) *Scan {
            const dispatcher: *Dispatcher = @alignCast(@fieldParentPtr(
                Dispatcher,
                @tagName(field),
                impl,
            ));
            return @fieldParentPtr(Scan, "dispatcher", dispatcher);
        }
    };
}
