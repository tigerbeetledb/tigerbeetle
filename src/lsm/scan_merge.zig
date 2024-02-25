const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");

const ScanState = @import("scan_state.zig").ScanState;
const Direction = @import("../direction.zig").Direction;
const KWayMergeIteratorType = @import("k_way_merge.zig").KWayMergeIteratorType;
const ScanType = @import("scan_builder.zig").ScanType;
const Snapshot = @import("schema.zig").Snapshot;

/// Union ∪ operation over an array of non-specialized `Scan` instances.
/// At a high level, this is an ordered iterator over the set-union of the timestamps of
/// each of the component Scans.
pub fn ScanMergeUnionType(
    comptime Groove: type,
    comptime Storage: type,
) type {
    return struct {
        const ScanMergeUnion = @This();
        const Scan = ScanType(Groove, Storage);

        pub const Callback = *const fn (context: *Scan.Context, self: *ScanMergeUnion) void;

        /// Adapts the `Scan` interface into a KWayMerge peek/pop stream.
        const KWayMergeScanStream = struct {
            scan: *Scan,
            current: ?u64 = null,

            pub fn peek(
                self: *KWayMergeScanStream,
            ) error{ Empty, Drained }!u64 {
                if (self.current == null) {
                    self.current = self.scan.next() catch |err| switch (err) {
                        error.ReadAgain => return error.Drained,
                    };
                }
                return self.current orelse error.Empty;
            }

            pub fn pop(
                self: *KWayMergeScanStream,
            ) u64 {
                assert(self.current != null);
                defer self.current = null;

                return self.current.?;
            }
        };

        const KWayMergeIterator = KWayMergeIteratorType(
            ScanMergeUnion,
            u64,
            u64,
            merge_key_from_value,
            constants.lsm_scans_max,
            merge_stream_peek,
            merge_stream_pop,
            merge_stream_precedence,
        );

        direction: Direction,
        snapshot: Snapshot,
        scan_context: Scan.Context = .{ .callback = &scan_read_callback },

        state: union(ScanState) {
            /// The scan has not been executed yet.
            /// The underlying scans are still uninitialized or in the state `.idle`.
            idle,

            /// The scan is at a valid position and ready to yield values, e.g. calling `next()`.
            /// All underlying scans are in the state `.seeking`.
            seeking,

            /// The scan needs to load data from the underlying scans, e.g. calling `read()`.
            /// At least one underlying scan is in the state `.needs_data`, while other ones may
            /// be in the state `.seeking`.
            needs_data,

            /// The scan is attempting to load data from the underlying scans,
            /// e.g. in between calling `read()` and receiving the callback.
            /// The underlying scans are either in the state `.buffering` or `.seeking`.
            buffering: struct {
                context: *Scan.Context,
                callback: Callback,
                pending_count: u32,
            },
        },
        streams: stdx.BoundedArray(KWayMergeScanStream, constants.lsm_scans_max),

        merge_iterator: ?KWayMergeIterator,

        pub fn init(
            scans: []const *Scan,
        ) ScanMergeUnion {
            assert(scans.len > 0);
            assert(scans.len <= constants.lsm_scans_max);

            const direction_first = scans[0].timestamp_direction().?;
            const snapshot_first = scans[0].snapshot();

            if (scans.len > 1) for (scans[1..]) |scan| {
                // Union merge can be applied only in scans that yield sorted timestamps.
                // All inner scans must have the same direction.
                assert(scan.timestamp_direction().? == direction_first);

                // All inner scans must have the same snapshot.
                assert(scan.snapshot().timestamp == snapshot_first.timestamp);
            };

            var self = ScanMergeUnion{
                .direction = direction_first,
                .snapshot = snapshot_first,
                .state = .idle,
                .streams = .{},
                .merge_iterator = null,
            };

            for (scans) |scan| {
                assert(scan.assigned == false);
                assert(scan.state() == .idle);

                // Mark this scan as `assigned`, so it can't be used to compose other merges.
                scan.assigned = true;
                self.streams.append_assume_capacity(.{ .scan = scan });
            }

            return self;
        }

        pub fn read(self: *ScanMergeUnion, context: *Scan.Context, callback: Callback) void {
            assert(self.state == .idle or self.state == .needs_data);
            assert(self.streams.count() > 0);

            const state_before = self.state;
            self.state = .{
                .buffering = .{
                    .context = context,
                    .callback = callback,
                    .pending_count = 0,
                },
            };

            for (self.streams.slice()) |*stream| {
                switch (stream.scan.state()) {
                    .idle => assert(state_before == .idle),
                    .seeking => continue,
                    .needs_data => assert(state_before == .needs_data),
                    .buffering => unreachable,
                }

                self.state.buffering.pending_count += 1;
                stream.scan.read(&self.scan_context);
            }

            assert(self.state.buffering.pending_count > 0);
        }

        /// Moves the iterator to the next position and returns its `Value` or `null` if the
        /// iterator has no more values to iterate.
        /// May return `error.ReadAgain` if the scan needs to be loaded, in this case
        /// call `read()` and resume the iteration after the read callback.
        pub fn next(self: *ScanMergeUnion) error{ReadAgain}!?u64 {
            switch (self.state) {
                .idle => {
                    assert(self.merge_iterator == null);
                    return error.ReadAgain;
                },
                .seeking => return self.merge_iterator.?.pop() catch |err| switch (err) {
                    error.Drained => {
                        self.state = .needs_data;
                        return error.ReadAgain;
                    },
                },
                .needs_data => return error.ReadAgain,
                .buffering => unreachable,
            }
        }

        fn scan_read_callback(context: *Scan.Context, scan: *Scan) void {
            const self: *ScanMergeUnion = @fieldParentPtr(ScanMergeUnion, "scan_context", context);
            assert(self.state == .buffering);
            assert(self.state.buffering.pending_count > 0);
            assert(self.state.buffering.pending_count <= self.streams.count());

            if (constants.verify) {
                assert(found: {
                    for (self.streams.const_slice()) |*stream| {
                        if (stream.scan == scan) break :found true;
                    } else break :found false;
                });
            }

            self.state.buffering.pending_count -= 1;
            if (self.state.buffering.pending_count == 0) {
                const context_outer = self.state.buffering.context;
                const callback = self.state.buffering.callback;
                self.state = .seeking;

                if (self.merge_iterator == null) {
                    self.merge_iterator = KWayMergeIterator.init(
                        self,
                        @intCast(self.streams.count()),
                        self.direction,
                    );
                }
                callback(context_outer, self);
            }
        }

        inline fn merge_key_from_value(value: *const u64) u64 {
            return value.*;
        }

        fn merge_stream_peek(
            self: *ScanMergeUnion,
            stream_index: u32,
        ) error{ Empty, Drained }!u64 {
            assert(stream_index < self.streams.count());

            var stream = &self.streams.slice()[stream_index];
            return stream.peek();
        }

        fn merge_stream_pop(
            self: *ScanMergeUnion,
            stream_index: u32,
        ) u64 {
            assert(stream_index < self.streams.count());

            var stream = &self.streams.slice()[stream_index];
            return stream.pop();
        }

        fn merge_stream_precedence(self: *const ScanMergeUnion, a: u32, b: u32) bool {
            _ = self;
            return a < b;
        }
    };
}

/// Intersection ∩ operation over an array of non-specialized `Scan` instances.
pub fn ScanMergeIntersectionType(comptime Groove: type, comptime Storage: type) type {
    // TODO: Implement intersection logic.
    return ScanMergeUnionType(Groove, Storage);
}

/// Difference (minus) operation over two non-specialized `Scan` instances.
pub fn ScanMergeDifferenceType(comptime Groove: type, comptime Storage: type) type {
    // TODO: Implement difference logic.
    return ScanMergeUnionType(Groove, Storage);
}
