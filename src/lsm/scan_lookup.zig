const std = @import("std");
const assert = std.debug.assert;

const Allocator = std.mem.Allocator;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");

const GridType = @import("../vsr/grid.zig").GridType;
const ScanType = @import("scan_builder.zig").ScanType;
const Snapshot = @import("schema.zig").Snapshot;

/// Implements the lookup logic for loading objects from scans.
pub fn ScanLookupType(
    comptime Groove: type,
    comptime Scan: type,
    comptime Storage: type,
) type {
    return struct {
        const ScanLookup = @This();
        const Grid = GridType(Storage);
        const Object = Groove.ObjectTree.Table.Value;

        pub const Callback = *const fn (*ScanLookup) void;

        const LookupWorker = struct {
            scan_lookup: *ScanLookup,
            lookup_context: Groove.ObjectTree.LookupContext = undefined,
            index_produced: ?usize = null,
        };

        /// Since the workload is always sorted by timestamp,
        /// adjacent objects are often going to be in the same table-value block.
        /// The grid is aware when N lookups ask for the same grid block concurrently,
        /// and queues up the reads internally such that they actually hit the storage once.
        ///
        /// To maximize IO utilization, we allow at least `Grid.read_iops_max` lookups to run,
        /// up to an arbitrary constant based on the maximum number of objects per block.
        /// Reasoning: the larger the block size is, higher is the probability of multiple
        /// lookups hitting the same grid block:
        const lookup_workers_max = @max(
            stdx.div_ceil(
                @divFloor(constants.block_size, @sizeOf(Object)),
                Grid.read_iops_max,
            ),
            Grid.read_iops_max,
        );

        groove: *Groove,
        scan: *Scan,
        scan_context: Scan.Context = .{ .callback = &scan_read_callback },

        buffer: ?[]Object,
        buffer_produced_len: usize,
        state: enum { idle, scan, lookup, finished },
        callback: ?Callback,

        workers: [lookup_workers_max]LookupWorker = undefined,
        /// The number of workers that are currently running in parallel.
        workers_pending: u32 = 0,

        pub fn init(
            groove: *Groove,
            scan: *Scan,
        ) ScanLookup {
            return .{
                .groove = groove,
                .scan = scan,
                .buffer = null,
                .callback = null,
                .buffer_produced_len = 0,
                .state = .idle,
            };
        }

        pub fn read(
            self: *ScanLookup,
            buffer: []Object,
            callback: Callback,
        ) void {
            assert(self.state == .idle);
            assert(self.callback == null);
            assert(self.workers_pending == 0);
            assert(self.buffer == null);
            assert(self.buffer_produced_len == 0);

            self.buffer = buffer;
            self.callback = callback;
            self.state = .scan;

            self.groove.objects.table_mutable.sort();
            self.scan.read(&self.scan_context);
        }

        pub fn slice(self: *const ScanLookup) []const Object {
            assert(self.state == .finished);
            assert(self.workers_pending == 0);
            return self.buffer.?[0..self.buffer_produced_len];
        }

        fn scan_read_callback(context: *Scan.Context, scan: *Scan) void {
            var self: *ScanLookup = @fieldParentPtr(ScanLookup, "scan_context", context);
            assert(self.state == .scan);
            assert(self.scan == scan);

            self.lookup_start();
        }

        fn lookup_start(self: *ScanLookup) void {
            assert(self.state == .scan);
            assert(self.workers_pending == 0);

            self.state = .lookup;

            for (&self.workers, 0..) |*worker, i| {
                assert(self.workers_pending == i);

                worker.* = .{ .scan_lookup = self };
                self.workers_pending += 1;
                self.lookup_worker_next(worker);

                // If the worker finished synchronously (e.g `workers_pending`
                // decreased), we don't need to start new ones.
                if (self.workers_pending == i) break;
            }

            // The lookup may have been completed synchronously,
            // and the last worker already called the callback.
            // It's safe to call the callback synchronously here since this function
            // is always called by `scan_read_callback`.
            assert(self.workers_pending > 0 or self.state != .lookup);
        }

        fn lookup_worker_next(self: *ScanLookup, worker: *LookupWorker) void {
            assert(self == worker.scan_lookup);
            assert(self.state == .lookup);

            while (self.state == .lookup) {
                if (self.buffer_produced_len == self.buffer.?.len) {
                    // The provided buffer was exhausted.
                    self.state = .finished;
                    break;
                }

                const timestamp = self.scan.next() catch |err| switch (err) {
                    error.ReadAgain => {
                        // The scan needs to be buffered again.
                        self.state = .scan;
                        break;
                    },
                } orelse {
                    // Reached the end of the scan.
                    self.state = .finished;
                    break;
                };

                // Incrementing the produced len once we are sure that
                // there is an object to lookup for that position.
                worker.index_produced = self.buffer_produced_len;
                self.buffer_produced_len += 1;

                const objects = &self.groove.objects;
                if (objects.table_mutable.get(timestamp) orelse
                    objects.table_immutable.get(timestamp)) |object|
                {
                    // TODO(batiati) Handle this properly when we implement snapshot queries.
                    assert(self.scan.snapshot().timestamp == Snapshot.latest.timestamp);

                    // Object present in table mutable/immutable,
                    // continue the loop to fetch the next one.
                    self.buffer.?[worker.index_produced.?] = object.*;
                    continue;
                } else switch (objects.lookup_from_levels_cache(
                    self.scan.snapshot(),
                    timestamp,
                )) {
                    // Since the scan already found the key,
                    // we don't expected `negative` here.
                    .negative => unreachable,

                    // Object is cached in memory,
                    // continue the loop to fetch the next one.
                    .positive => |object| {
                        self.buffer.?[worker.index_produced.?] = object.*;
                        continue;
                    },

                    // The object needs to be loaded from storage, returning now,
                    // the iteration will be resumed when we receive the callback.
                    .possible => |level_min| {
                        objects.lookup_from_levels_storage(.{
                            .callback = lookup_worker_callback,
                            .context = &worker.lookup_context,
                            .snapshot = self.scan.snapshot(),
                            .key = timestamp,
                            .level_min = level_min,
                        });
                        return;
                    },
                }
            }

            // The worker finished synchronously by reading from cache.
            switch (self.state) {
                .idle, .lookup => unreachable,
                .scan, .finished => self.lookup_worker_finished(),
            }
        }

        fn lookup_worker_callback(
            completion: *Groove.ObjectTree.LookupContext,
            result: ?*const Object,
        ) void {
            // Since the scan produced a valid key, it's expected to be found here.
            assert(result != null);

            const worker = @fieldParentPtr(LookupWorker, "lookup_context", completion);
            const self: *ScanLookup = worker.scan_lookup;

            assert(worker.index_produced != null);
            assert(worker.index_produced.? < self.buffer_produced_len);

            worker.lookup_context = undefined;
            self.buffer.?[worker.index_produced.?] = result.?.*;

            switch (self.state) {
                .idle => unreachable,
                .lookup => self.lookup_worker_next(worker),
                .scan, .finished => self.lookup_worker_finished(),
            }
        }

        fn lookup_worker_finished(self: *ScanLookup) void {
            // One worker may have been finished, but the overall state cannot be narrowed
            // until all workers have finished.
            assert(self.state != .idle);
            assert(self.workers_pending > 0);

            self.workers_pending -= 1;
            if (self.workers_pending == 0) {
                switch (self.state) {
                    .idle, .lookup => unreachable,
                    // The scan's buffer was consumed and it needs to read again:
                    .scan => self.scan.read(&self.scan_context),
                    .finished => {
                        const callback = self.callback.?;
                        self.callback = null;

                        callback(self);
                    },
                }
            }
        }
    };
}
