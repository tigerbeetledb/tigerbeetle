const std = @import("std");
const assert = std.debug.assert;

const snapshot_latest = @import("tree.zig").snapshot_latest;

const GridType = @import("../vsr/grid.zig").GridType;

pub const ScanLookupStatus = enum {
    idle,
    scan,
    lookup,
    buffer_finished,
    scan_finished,
};

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

        pub const Callback = *const fn (*ScanLookup, []const Object) void;

        const LookupWorker = struct {
            index: u8,
            scan_lookup: *ScanLookup,
            lookup_context: Groove.ObjectTree.LookupContext = undefined,
            index_produced: ?usize = null,
        };

        groove: *Groove,
        scan: *Scan,
        scan_context: Scan.Context = .{ .callback = &scan_read_callback },

        buffer: ?[]Object,
        buffer_produced_len: ?usize,
        state: ScanLookupStatus,
        callback: ?Callback,

        workers: [Grid.read_iops_max]LookupWorker = undefined,
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
                .buffer_produced_len = null,
                .callback = null,
                .state = .idle,
            };
        }

        pub fn read(
            self: *ScanLookup,
            buffer: []Object,
            callback: Callback,
        ) void {
            assert(self.state == .idle or
                // `read()` can be called multiple times when the buffer has finished,
                // but the scan still yields.
                self.state == .buffer_finished);
            assert(self.callback == null);
            assert(self.workers_pending == 0);
            assert(self.buffer == null);
            assert(self.buffer_produced_len == null);

            self.* = .{
                .groove = self.groove,
                .scan = self.scan,
                .buffer = buffer,
                .buffer_produced_len = 0,
                .callback = callback,
                .state = .scan,
            };

            self.groove.objects.table_mutable.sort();
            self.scan.read(&self.scan_context);
        }

        fn slice(self: *const ScanLookup) []const Object {
            assert(self.state == .buffer_finished or self.state == .scan_finished);
            assert(self.workers_pending == 0);
            return self.buffer.?[0..self.buffer_produced_len.?];
        }

        fn scan_read_callback(context: *Scan.Context, scan: *Scan) void {
            var self: *ScanLookup = @alignCast(@fieldParentPtr("scan_context", context));
            assert(self.state == .scan);
            assert(self.scan == scan);

            self.lookup_start();
        }

        fn lookup_start(self: *ScanLookup) void {
            assert(self.state == .scan);
            assert(self.workers_pending == 0);

            self.groove.grid.trace.start(.{
                .lookup = .{ .tree = @enumFromInt(self.groove.objects.config.id) },
            });

            self.state = .lookup;

            for (&self.workers, 0..) |*worker, index| {
                assert(self.workers_pending == index);

                worker.* = .{
                    .index = @intCast(index),
                    .scan_lookup = self,
                };
                self.workers_pending += 1;

                self.groove.grid.trace.start(
                    .{ .lookup_worker = .{ .index = worker.index, .tree = @enumFromInt(self.groove.objects.config.id) } },
                );

                self.lookup_worker_next(worker);

                // If the worker finished synchronously (e.g `workers_pending`
                // decreased), we don't need to start new ones.
                if (self.workers_pending == index) break;
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
                if (self.buffer_produced_len.? == self.buffer.?.len) {
                    // The provided buffer was exhausted.
                    self.state = .buffer_finished;
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
                    self.state = .scan_finished;
                    break;
                };

                // Incrementing the produced len once we are sure that
                // there is an object to lookup for that position.
                worker.index_produced = self.buffer_produced_len.?;
                self.buffer_produced_len = self.buffer_produced_len.? + 1;

                const objects = &self.groove.objects;
                if (objects.table_mutable.get(timestamp) orelse
                    objects.table_immutable.get(timestamp)) |object|
                {
                    // TODO(batiati) Handle this properly when we implement snapshot queries.
                    assert(self.scan.snapshot() == snapshot_latest);

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
                .scan, .buffer_finished, .scan_finished => self.lookup_worker_finished(worker),
            }
        }

        fn lookup_worker_callback(
            completion: *Groove.ObjectTree.LookupContext,
            result: ?*const Object,
        ) void {
            // Since the scan produced a valid key, it's expected to be found here.
            assert(result != null);

            const worker: *LookupWorker = @fieldParentPtr("lookup_context", completion);
            const self: *ScanLookup = worker.scan_lookup;

            assert(worker.index_produced != null);
            assert(worker.index_produced.? < self.buffer_produced_len.?);

            worker.lookup_context = undefined;
            self.buffer.?[worker.index_produced.?] = result.?.*;

            switch (self.state) {
                .idle => unreachable,
                .lookup => self.lookup_worker_next(worker),
                .scan, .scan_finished, .buffer_finished => self.lookup_worker_finished(worker),
            }
        }

        fn lookup_worker_finished(self: *ScanLookup, worker: *const LookupWorker) void {
            // One worker may have been finished, but the overall state cannot be narrowed
            // until all workers have finished.
            assert(self.state != .idle);
            assert(self.workers_pending > 0);

            self.groove.grid.trace.stop(
                .{ .lookup_worker = .{ .index = worker.index, .tree = @enumFromInt(self.groove.objects.config.id) } },
            );

            self.workers_pending -= 1;
            if (self.workers_pending == 0) {
                self.groove.grid.trace.stop(.{
                    .lookup = .{ .tree = @enumFromInt(self.groove.objects.config.id) },
                });

                switch (self.state) {
                    .idle, .lookup => unreachable,
                    // The scan's buffer was consumed and it needs to read again:
                    .scan => self.scan.read(&self.scan_context),
                    // Either the lookup buffer was filled, or the scan reached the end:
                    .buffer_finished, .scan_finished => {
                        const callback = self.callback.?;
                        const results = self.slice();
                        self.buffer = null;
                        self.buffer_produced_len = null;
                        self.callback = null;

                        callback(self, results);
                    },
                }
            }
        }
    };
}
