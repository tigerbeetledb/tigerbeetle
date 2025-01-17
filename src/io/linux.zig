const std = @import("std");
const assert = std.debug.assert;
const os = std.os;
const posix = std.posix;
const linux = os.linux;
const IO_Uring = linux.IoUring;
const io_uring_cqe = linux.io_uring_cqe;
const io_uring_sqe = linux.io_uring_sqe;
const log = std.log.scoped(.io);

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");
const FIFOType = @import("../fifo.zig").FIFOType;
const buffer_limit = @import("../io.zig").buffer_limit;
const DirectIO = @import("../io.zig").DirectIO;
const DoublyLinkedListType = @import("../list.zig").DoublyLinkedListType;
const parse_dirty_semver = stdx.parse_dirty_semver;
const maybe = stdx.maybe;

pub const IO = struct {
    const CompletionList = DoublyLinkedListType(Completion, .awaiting_back, .awaiting_next);

    ring: IO_Uring,

    /// Operations not yet submitted to the kernel and waiting on available space in the
    /// submission queue.
    unqueued: FIFOType(Completion) = .{ .name = "io_unqueued" },

    /// Completions that are ready to have their callbacks run.
    completed: FIFOType(Completion) = .{ .name = "io_completed" },

    // TODO Track these as metrics:
    ios_queued: u32 = 0,
    ios_in_kernel: u32 = 0,

    /// The head of a doubly-linked list of all operations that are:
    /// - in the submission queue, or
    /// - in the kernel, or
    /// - in the completion queue, or
    /// - in the `completed` list (excluding zero-duration timeouts)
    awaiting: CompletionList = .{},

    // This is the completion that performs the cancellation.
    // This is *not* the completion that is being canceled.
    cancel_completion: Completion = undefined,

    cancel_status: union(enum) {
        // Not canceling.
        inactive,
        // Waiting to start canceling the next awaiting operation.
        next,
        // The target's cancellation SQE is queued; waiting for the cancellation's completion.
        queued: struct { target: *Completion },
        // Currently canceling the target operation.
        wait: struct { target: *Completion },
        // All operations have been canceled.
        done,
    } = .inactive,

    pub fn init(entries: u12, flags: u32) !IO {
        // Detect the linux version to ensure that we support all io_uring ops used.
        const uts = posix.uname();
        const version = try parse_dirty_semver(&uts.release);
        if (version.order(std.SemanticVersion{ .major = 5, .minor = 5, .patch = 0 }) == .lt) {
            @panic("Linux kernel 5.5 or greater is required for io_uring OP_ACCEPT");
        }

        errdefer |err| switch (err) {
            error.SystemOutdated => {
                log.err("io_uring is not available", .{});
                log.err("likely cause: the syscall is disabled by seccomp", .{});
            },
            error.PermissionDenied => {
                log.err("io_uring is not available", .{});
                log.err("likely cause: the syscall is disabled by sysctl, " ++
                    "try 'sysctl -w kernel.io_uring_disabled=0'", .{});
            },
            else => {},
        };

        return IO{ .ring = try IO_Uring.init(entries, flags) };
    }

    pub fn deinit(self: *IO) void {
        self.ring.deinit();
    }

    /// Pass all queued submissions to the kernel and peek for completions.
    pub fn tick(self: *IO) !void {
        assert(self.cancel_status != .done);

        // We assume that all timeouts submitted by `run_for_ns()` will be reaped by `run_for_ns()`
        // and that `tick()` and `run_for_ns()` cannot be run concurrently.
        // Therefore `timeouts` here will never be decremented and `etime` will always be false.
        var timeouts: usize = 0;
        var etime = false;

        try self.flush(0, &timeouts, &etime);
        assert(etime == false);

        // Flush any SQEs that were queued while running completion callbacks in `flush()`:
        // This is an optimization to avoid delaying submissions until the next tick.
        // At the same time, we do not flush any ready CQEs since SQEs may complete synchronously.
        // We guard against an io_uring_enter() syscall if we know we do not have any queued SQEs.
        // We cannot use `self.ring.sq_ready()` here since this counts flushed and unflushed SQEs.
        const queued = self.ring.sq.sqe_tail -% self.ring.sq.sqe_head;
        if (queued > 0) {
            try self.flush_submissions(0, &timeouts, &etime);
            assert(etime == false);
        }
    }

    /// Pass all queued submissions to the kernel and run for `nanoseconds`.
    /// The `nanoseconds` argument is a u63 to allow coercion to the i64 used
    /// in the kernel_timespec struct.
    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        assert(self.cancel_status != .done);

        // We must use the same clock source used by io_uring (CLOCK_MONOTONIC) since we specify the
        // timeout below as an absolute value. Otherwise, we may deadlock if the clock sources are
        // dramatically different. Any kernel that supports io_uring will support CLOCK_MONOTONIC.
        var current_ts: posix.timespec = undefined;
        posix.clock_gettime(posix.CLOCK.MONOTONIC, &current_ts) catch unreachable;
        // The absolute CLOCK_MONOTONIC time after which we may return from this function:
        const timeout_ts: os.linux.kernel_timespec = .{
            .tv_sec = current_ts.tv_sec,
            .tv_nsec = current_ts.tv_nsec + nanoseconds,
        };
        var timeouts: usize = 0;
        var etime = false;
        while (!etime) {
            const timeout_sqe = self.ring.get_sqe() catch blk: {
                // The submission queue is full, so flush submissions to make space:
                try self.flush_submissions(0, &timeouts, &etime);
                break :blk self.ring.get_sqe() catch unreachable;
            };
            // Submit an absolute timeout that will be canceled if any other SQE completes first:
            timeout_sqe.prep_timeout(&timeout_ts, 1, os.linux.IORING_TIMEOUT_ABS);
            timeout_sqe.user_data = 0;
            timeouts += 1;

            // We don't really want to count this timeout as an io,
            // but it's tricky to track separately.
            self.ios_queued += 1;

            // The amount of time this call will block is bounded by the timeout we just submitted:
            try self.flush(1, &timeouts, &etime);
        }
        // Reap any remaining timeouts, which reference the timespec in the current stack frame.
        // The busy loop here is required to avoid a potential deadlock, as the kernel determines
        // when the timeouts are pushed to the completion queue, not us.
        while (timeouts > 0) _ = try self.flush_completions(0, &timeouts, &etime);
    }

    fn flush(self: *IO, wait_nr: u32, timeouts: *usize, etime: *bool) !void {
        // Flush any queued SQEs and reuse the same syscall to wait for completions if required:
        try self.flush_submissions(wait_nr, timeouts, etime);
        // We can now just peek for any CQEs without waiting and without another syscall:
        try self.flush_completions(0, timeouts, etime);

        // The SQE array is empty from flush_submissions(). Fill it up with unqueued completions.
        // This runs before `self.completed` is flushed below to prevent new IO from reserving SQE
        // slots and potentially starving those in `self.unqueued`.
        // Loop over a copy to avoid an infinite loop of `enqueue()` re-adding to `self.unqueued`.
        {
            var copy = self.unqueued;
            self.unqueued.reset();
            while (copy.pop()) |completion| self.enqueue(completion);
        }

        // Run completions only after all completions have been flushed:
        // Loop until all completions are processed. Calls to complete() may queue more work
        // and extend the duration of the loop, but this is fine as it 1) executes completions
        // that become ready without going through another syscall from flush_submissions() and
        // 2) potentially queues more SQEs to take advantage more of the next flush_submissions().
        while (self.completed.pop()) |completion| {
            if (completion.operation == .timeout and
                completion.operation.timeout.timespec.tv_sec == 0 and
                completion.operation.timeout.timespec.tv_nsec == 0)
            {
                // Zero-duration timeouts are a special case, and aren't listed in `awaiting`.
                maybe(self.awaiting.empty());
                assert(completion.result == -@as(i32, @intFromEnum(posix.E.TIME)));
                assert(completion.awaiting_back == null);
                assert(completion.awaiting_next == null);
            } else {
                assert(!self.awaiting.empty());
                self.awaiting.remove(completion);
            }

            switch (self.cancel_status) {
                .inactive => completion.complete(),
                .next => {},
                .queued => if (completion.operation == .cancel) completion.complete(),
                .wait => |wait| if (wait.target == completion) {
                    self.cancel_status = .next;
                },
                .done => unreachable,
            }
        }

        // At this point, unqueued could have completions either by 1) those who didn't get an SQE
        // during the popping of unqueued or 2) completion.complete() which start new IO. These
        // unqueued completions will get priority to acquiring SQEs on the next flush().
    }

    fn flush_completions(self: *IO, wait_nr: u32, timeouts: *usize, etime: *bool) !void {
        var cqes: [256]io_uring_cqe = undefined;
        var wait_remaining = wait_nr;
        while (true) {
            // Guard against waiting indefinitely (if there are too few requests inflight),
            // especially if this is not the first time round the loop:
            const completed = self.ring.copy_cqes(&cqes, wait_remaining) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                else => return err,
            };
            if (completed > wait_remaining) wait_remaining = 0 else wait_remaining -= completed;
            for (cqes[0..completed]) |cqe| {
                self.ios_in_kernel -= 1;

                if (cqe.user_data == 0) {
                    timeouts.* -= 1;
                    // We are only done if the timeout submitted was completed due to time, not if
                    // it was completed due to the completion of an event, in which case `cqe.res`
                    // would be 0. It is possible for multiple timeout operations to complete at the
                    // same time if the nanoseconds value passed to `run_for_ns()` is very short.
                    if (-cqe.res == @intFromEnum(posix.E.TIME)) etime.* = true;
                    continue;
                }
                const completion: *Completion = @ptrFromInt(cqe.user_data);
                completion.result = cqe.res;
                // We do not run the completion here (instead appending to a linked list) to avoid:
                // * recursion through `flush_submissions()` and `flush_completions()`,
                // * unbounded stack usage, and
                // * confusing stack traces.
                self.completed.push(completion);
            }

            if (completed < cqes.len) break;
        }
    }

    fn flush_submissions(self: *IO, wait_nr: u32, timeouts: *usize, etime: *bool) !void {
        while (true) {
            const submitted = self.ring.submit_and_wait(wait_nr) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                // Wait for some completions and then try again:
                // See https://github.com/axboe/liburing/issues/281 re: error.SystemResources.
                // Be careful also that copy_cqes() will flush before entering to wait (it does):
                // https://github.com/axboe/liburing/commit/35c199c48dfd54ad46b96e386882e7ac341314c5
                error.CompletionQueueOvercommitted, error.SystemResources => {
                    try self.flush_completions(1, timeouts, etime);
                    continue;
                },
                else => return err,
            };

            self.ios_queued -= submitted;
            self.ios_in_kernel += submitted;

            break;
        }
    }

    fn enqueue(self: *IO, completion: *Completion) void {
        switch (self.cancel_status) {
            .inactive => {},
            .queued => assert(completion.operation == .cancel),
            else => unreachable,
        }

        const sqe = self.ring.get_sqe() catch |err| switch (err) {
            error.SubmissionQueueFull => {
                self.unqueued.push(completion);
                return;
            },
        };
        completion.prep(sqe);

        self.awaiting.push(completion);
        self.ios_queued += 1;
    }

    /// Cancel should be invoked at most once, before any of the memory owned by read/recv buffers
    /// is freed (so that lingering async operations do not write to them).
    ///
    /// After this function is invoked:
    /// - No more completion callbacks will be called.
    /// - No more IO may be submitted.
    ///
    /// This function doesn't return until either:
    /// - All events submitted to io_uring have completed.
    ///   (They may complete with `error.Canceled`).
    /// - Or, an io_uring error occurs.
    ///
    /// TODO(Linux):
    /// - Linux kernel ≥5.19 supports the IORING_ASYNC_CANCEL_ALL and IORING_ASYNC_CANCEL_ANY flags,
    ///   which would allow all events to be cancelled simultaneously with a single "cancel"
    ///   operation, without IO needing to maintain the `awaiting` doubly-linked list and the `next`
    ///   cancellation stage.
    /// - Linux kernel ≥6.0 supports `io_uring_register_sync_cancel` which would remove the `queued`
    ///   cancellation stage.
    pub fn cancel_all(self: *IO) void {
        assert(self.cancel_status == .inactive);

        // Even if we return early due to an io_uring error, IO won't allow more operations.
        defer self.cancel_status = .done;

        self.cancel_status = .next;

        // Discard any operations that haven't started yet.
        while (self.unqueued.pop()) |_| {}

        while (self.awaiting.tail) |target| {
            assert(!self.awaiting.empty());
            assert(self.cancel_status == .next);
            assert(target.operation != .cancel);

            self.cancel(target);
            assert(self.cancel_status == .queued);

            while (self.cancel_status == .queued or self.cancel_status == .wait) {
                self.run_for_ns(constants.tick_ms * std.time.ns_per_ms) catch |err| {
                    std.debug.panic("IO.cancel_all: run_for_ns error: {}", .{err});
                };
            }
            assert(self.cancel_status == .next);
        }
        assert(self.awaiting.empty());
        assert(self.ios_queued == 0);
        assert(self.ios_in_kernel == 0);
    }

    fn cancel(self: *IO, target: *Completion) void {
        self.cancel_completion = .{
            .io = self,
            .context = self,
            .callback = struct {
                fn wrapper(
                    ctx: ?*anyopaque,
                    comp: *Completion,
                    res: *const anyopaque,
                ) void {
                    const io: *IO = @ptrCast(@alignCast(ctx.?));
                    const result =
                        @as(*const CancelError!void, @ptrCast(@alignCast(res))).*;
                    io.cancel_callback(comp, result);
                }
            }.wrapper,
            .operation = .{ .cancel = .{ .target = target } },
        };

        self.cancel_status = .{ .queued = .{ .target = target } };
        self.enqueue(&self.cancel_completion);
    }

    const CancelError = error{
        NotRunning,
        NotInterruptable,
    } || posix.UnexpectedError;

    fn cancel_callback(self: *IO, completion: *Completion, result: CancelError!void) void {
        assert(self.cancel_status == .queued);
        assert(completion == &self.cancel_completion);
        assert(completion.operation == .cancel);
        assert(completion.operation.cancel.target == self.cancel_status.queued.target);

        self.cancel_status = status: {
            result catch |err| switch (err) {
                error.NotRunning => break :status .next,
                error.NotInterruptable => {},
                error.Unexpected => unreachable,
            };
            // Wait for the target operation to complete or abort.
            break :status .{ .wait = .{ .target = self.cancel_status.queued.target } };
        };
    }

    /// This struct holds the data needed for a single io_uring operation
    pub const Completion = struct {
        io: *IO,
        result: i32 = undefined,
        next: ?*Completion = null,
        operation: Operation,
        context: ?*anyopaque,
        callback: *const fn (
            context: ?*anyopaque,
            completion: *Completion,
            result: *const anyopaque,
        ) void,

        /// Used by the `IO.awaiting` doubly-linked list.
        awaiting_back: ?*Completion = null,
        awaiting_next: ?*Completion = null,

        fn prep(completion: *Completion, sqe: *io_uring_sqe) void {
            switch (completion.operation) {
                .cancel => |op| {
                    sqe.prep_cancel(@intFromPtr(op.target), 0);
                },
                .accept => |*op| {
                    sqe.prep_accept(
                        op.socket,
                        &op.address,
                        &op.address_size,
                        posix.SOCK.CLOEXEC,
                    );
                },
                .close => |op| {
                    sqe.prep_close(op.fd);
                },
                .connect => |*op| {
                    sqe.prep_connect(
                        op.socket,
                        &op.address.any,
                        op.address.getOsSockLen(),
                    );
                },
                .fsync => |op| {
                    sqe.prep_fsync(op.fd, op.flags);
                },
                .openat => |op| {
                    sqe.prep_openat(
                        op.dir_fd,
                        op.file_path,
                        op.flags,
                        op.mode,
                    );
                },
                .read => |op| {
                    sqe.prep_read(
                        op.fd,
                        op.buffer[0..buffer_limit(op.buffer.len)],
                        op.offset,
                    );
                },
                .recv => |op| {
                    sqe.prep_recv(op.socket, op.buffer, posix.MSG.NOSIGNAL);
                },
                .send => |op| {
                    sqe.prep_send(op.socket, op.buffer, posix.MSG.NOSIGNAL);
                },
                .statx => |op| {
                    sqe.prep_statx(
                        op.dir_fd,
                        op.file_path,
                        op.flags,
                        op.mask,
                        op.statxbuf,
                    );
                },
                .timeout => |*op| {
                    sqe.prep_timeout(&op.timespec, 0, 0);
                },
                .write => |op| {
                    sqe.prep_write(
                        op.fd,
                        op.buffer[0..buffer_limit(op.buffer.len)],
                        op.offset,
                    );
                },
            }
            sqe.user_data = @intFromPtr(completion);
        }

        fn complete(completion: *Completion) void {
            switch (completion.operation) {
                .cancel => {
                    const result: CancelError!void = result: {
                        if (completion.result < 0) {
                            break :result switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                // No operation matching the completion is queued, so there is
                                // nothing to cancel.
                                .NOENT => error.NotRunning,
                                // The operation as far enough along that it cannot be canceled.
                                // It should complete soon.
                                .ALREADY => error.NotInterruptable,
                                // SQE is invalid.
                                .INVAL => unreachable,
                                else => |errno| stdx.unexpected_errno("cancel", errno),
                            };
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .accept => {
                    const result: AcceptError!posix.socket_t = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .AGAIN => error.WouldBlock,
                                .BADF => error.FileDescriptorInvalid,
                                .CONNABORTED => error.ConnectionAborted,
                                .FAULT => unreachable,
                                .INVAL => error.SocketNotListening,
                                .MFILE => error.ProcessFdQuotaExceeded,
                                .NFILE => error.SystemFdQuotaExceeded,
                                .NOBUFS => error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .OPNOTSUPP => error.OperationNotSupported,
                                .PERM => error.PermissionDenied,
                                .PROTO => error.ProtocolFailure,
                                else => |errno| stdx.unexpected_errno("accept", errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .close => {
                    const result: CloseError!void = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                // A success, see https://github.com/ziglang/zig/issues/2425
                                .INTR => {},
                                .BADF => error.FileDescriptorInvalid,
                                .DQUOT => error.DiskQuota,
                                .IO => error.InputOutput,
                                .NOSPC => error.NoSpaceLeft,
                                else => |errno| stdx.unexpected_errno("close", errno),
                            };
                            break :blk err;
                        } else {
                            assert(completion.result == 0);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .connect => {
                    const result: ConnectError!void = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .ACCES => error.AccessDenied,
                                .ADDRINUSE => error.AddressInUse,
                                .ADDRNOTAVAIL => error.AddressNotAvailable,
                                .AFNOSUPPORT => error.AddressFamilyNotSupported,
                                .AGAIN, .INPROGRESS => error.WouldBlock,
                                .ALREADY => error.OpenAlreadyInProgress,
                                .BADF => error.FileDescriptorInvalid,
                                .CONNREFUSED => error.ConnectionRefused,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .FAULT => unreachable,
                                .ISCONN => error.AlreadyConnected,
                                .NETUNREACH => error.NetworkUnreachable,
                                .HOSTUNREACH => error.HostUnreachable,
                                .NOENT => error.FileNotFound,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .PERM => error.PermissionDenied,
                                .PROTOTYPE => error.ProtocolNotSupported,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                else => |errno| stdx.unexpected_errno("connect", errno),
                            };
                            break :blk err;
                        } else {
                            assert(completion.result == 0);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .fsync => {
                    const result: anyerror!void = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .BADF => error.FileDescriptorInvalid,
                                .IO => error.InputOutput,
                                .INVAL => unreachable,
                                else => |errno| stdx.unexpected_errno("fsync", errno),
                            };
                            break :blk err;
                        } else {
                            assert(completion.result == 0);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .openat => {
                    const result: OpenatError!fd_t = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .FAULT => unreachable,
                                .INVAL => unreachable,
                                .BADF => unreachable,
                                .ACCES => error.AccessDenied,
                                .FBIG => error.FileTooBig,
                                .OVERFLOW => error.FileTooBig,
                                .ISDIR => error.IsDir,
                                .LOOP => error.SymLinkLoop,
                                .MFILE => error.ProcessFdQuotaExceeded,
                                .NAMETOOLONG => error.NameTooLong,
                                .NFILE => error.SystemFdQuotaExceeded,
                                .NODEV => error.NoDevice,
                                .NOENT => error.FileNotFound,
                                .NOMEM => error.SystemResources,
                                .NOSPC => error.NoSpaceLeft,
                                .NOTDIR => error.NotDir,
                                .PERM => error.AccessDenied,
                                .EXIST => error.PathAlreadyExists,
                                .BUSY => error.DeviceBusy,
                                .OPNOTSUPP => error.FileLocksNotSupported,
                                .AGAIN => error.WouldBlock,
                                .TXTBSY => error.FileBusy,
                                else => |errno| stdx.unexpected_errno("openat", errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .read => {
                    const result: ReadError!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR, .AGAIN => {
                                    // Some file systems, like XFS, can return EAGAIN even when
                                    // reading from a blocking file without flags like RWF_NOWAIT.
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .BADF => error.NotOpenForReading,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .FAULT => unreachable,
                                .INVAL => error.Alignment,
                                .IO => error.InputOutput,
                                .ISDIR => error.IsDir,
                                .NOBUFS => error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .NXIO => error.Unseekable,
                                .OVERFLOW => error.Unseekable,
                                .SPIPE => error.Unseekable,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                else => |errno| stdx.unexpected_errno("read", errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .recv => {
                    const result: RecvError!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .AGAIN => error.WouldBlock,
                                .BADF => error.FileDescriptorInvalid,
                                .CONNREFUSED => error.ConnectionRefused,
                                .FAULT => unreachable,
                                .INVAL => unreachable,
                                .NOMEM => error.SystemResources,
                                .NOTCONN => error.SocketNotConnected,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                .OPNOTSUPP => error.OperationNotSupported,
                                else => |errno| stdx.unexpected_errno("recv", errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .send => {
                    const result: SendError!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .ACCES => error.AccessDenied,
                                .AGAIN => error.WouldBlock,
                                .ALREADY => error.FastOpenAlreadyInProgress,
                                .AFNOSUPPORT => error.AddressFamilyNotSupported,
                                .BADF => error.FileDescriptorInvalid,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .DESTADDRREQ => unreachable,
                                .FAULT => unreachable,
                                .INVAL => unreachable,
                                .ISCONN => unreachable,
                                .MSGSIZE => error.MessageTooBig,
                                .NOBUFS => error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .NOTCONN => error.SocketNotConnected,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .OPNOTSUPP => error.OperationNotSupported,
                                .PIPE => error.BrokenPipe,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                else => |errno| stdx.unexpected_errno("send", errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .statx => {
                    const result: StatxError!void = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .FAULT => unreachable,
                                .INVAL => unreachable,
                                .BADF => unreachable,
                                .ACCES => error.AccessDenied,
                                .LOOP => error.SymLinkLoop,
                                .NAMETOOLONG => error.NameTooLong,
                                .NOENT => error.FileNotFound,
                                .NOMEM => error.SystemResources,
                                .NOTDIR => error.NotDir,
                                else => |errno| stdx.unexpected_errno("statx", errno),
                            };
                            break :blk err;
                        } else {
                            assert(completion.result == 0);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
                .timeout => {
                    assert(completion.result < 0);
                    const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                        .INTR => {
                            completion.io.enqueue(completion);
                            return;
                        },
                        .CANCELED => error.Canceled,
                        .TIME => {}, // A success.
                        else => |errno| stdx.unexpected_errno("timeout", errno),
                    };
                    const result: TimeoutError!void = err;
                    completion.callback(completion.context, completion, &result);
                },
                .write => {
                    const result: WriteError!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(posix.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .AGAIN => error.WouldBlock,
                                .BADF => error.NotOpenForWriting,
                                .DESTADDRREQ => error.NotConnected,
                                .DQUOT => error.DiskQuota,
                                .FAULT => unreachable,
                                .FBIG => error.FileTooBig,
                                .INVAL => error.Alignment,
                                .IO => error.InputOutput,
                                .NOSPC => error.NoSpaceLeft,
                                .NXIO => error.Unseekable,
                                .OVERFLOW => error.Unseekable,
                                .PERM => error.AccessDenied,
                                .PIPE => error.BrokenPipe,
                                .SPIPE => error.Unseekable,
                                else => |errno| stdx.unexpected_errno("write", errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @intCast(completion.result);
                        }
                    };
                    completion.callback(completion.context, completion, &result);
                },
            }
        }
    };

    /// This union encodes the set of operations supported as well as their arguments.
    const Operation = union(enum) {
        cancel: struct {
            target: *Completion,
        },
        accept: struct {
            socket: posix.socket_t,
            address: posix.sockaddr = undefined,
            address_size: posix.socklen_t = @sizeOf(posix.sockaddr),
        },
        close: struct {
            fd: fd_t,
        },
        connect: struct {
            socket: posix.socket_t,
            address: std.net.Address,
        },
        fsync: struct {
            fd: fd_t,
            flags: u32,
        },
        openat: struct {
            dir_fd: fd_t,
            file_path: [*:0]const u8,
            flags: posix.O,
            mode: posix.mode_t,
        },
        read: struct {
            fd: fd_t,
            buffer: []u8,
            offset: u64,
        },
        recv: struct {
            socket: posix.socket_t,
            buffer: []u8,
        },
        send: struct {
            socket: posix.socket_t,
            buffer: []const u8,
        },
        statx: struct {
            dir_fd: fd_t,
            file_path: [*:0]const u8,
            flags: u32,
            mask: u32,
            statxbuf: *std.os.linux.Statx,
        },
        timeout: struct {
            timespec: os.linux.kernel_timespec,
        },
        write: struct {
            fd: fd_t,
            buffer: []const u8,
            offset: u64,
        },
    };

    pub const AcceptError = error{
        WouldBlock,
        FileDescriptorInvalid,
        ConnectionAborted,
        SocketNotListening,
        ProcessFdQuotaExceeded,
        SystemFdQuotaExceeded,
        SystemResources,
        FileDescriptorNotASocket,
        OperationNotSupported,
        PermissionDenied,
        ProtocolFailure,
    } || posix.UnexpectedError;

    pub fn accept(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: AcceptError!posix.socket_t,
        ) void,
        completion: *Completion,
        socket: posix.socket_t,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const AcceptError!posix.socket_t, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .accept = .{
                    .socket = socket,
                    .address = undefined,
                    .address_size = @sizeOf(posix.sockaddr),
                },
            },
        };
        self.enqueue(completion);
    }

    pub const CloseError = error{
        FileDescriptorInvalid,
        DiskQuota,
        InputOutput,
        NoSpaceLeft,
    } || posix.UnexpectedError;

    pub fn close(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: CloseError!void,
        ) void,
        completion: *Completion,
        fd: fd_t,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const CloseError!void, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .close = .{ .fd = fd },
            },
        };
        self.enqueue(completion);
    }

    pub const ConnectError = error{
        AccessDenied,
        AddressInUse,
        AddressNotAvailable,
        AddressFamilyNotSupported,
        WouldBlock,
        OpenAlreadyInProgress,
        FileDescriptorInvalid,
        ConnectionRefused,
        ConnectionResetByPeer,
        AlreadyConnected,
        NetworkUnreachable,
        HostUnreachable,
        FileNotFound,
        FileDescriptorNotASocket,
        PermissionDenied,
        ProtocolNotSupported,
        ConnectionTimedOut,
        SystemResources,
    } || posix.UnexpectedError;

    pub fn connect(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: ConnectError!void,
        ) void,
        completion: *Completion,
        socket: posix.socket_t,
        address: std.net.Address,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const ConnectError!void, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .connect = .{
                    .socket = socket,
                    .address = address,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const FsyncError = error{
        FileDescriptorInvalid,
        InputOutput,
    } || posix.UnexpectedError;

    pub fn fsync(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: FsyncError!void,
        ) void,
        completion: *Completion,
        fd: fd_t,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const FsyncError!void, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .fsync = .{
                    .fd = fd,
                    .flags = os.linux.IORING_FSYNC_DATASYNC,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const OpenatError = posix.OpenError || posix.UnexpectedError;

    pub fn openat(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: OpenatError!fd_t,
        ) void,
        completion: *Completion,
        dir_fd: fd_t,
        file_path: [*:0]const u8,
        flags: posix.O,
        mode: posix.mode_t,
    ) void {
        var new_flags = flags;
        new_flags.CLOEXEC = true;

        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const OpenatError!fd_t, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .openat = .{
                    .dir_fd = dir_fd,
                    .file_path = file_path,
                    .flags = new_flags,
                    .mode = mode,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const ReadError = error{
        WouldBlock,
        NotOpenForReading,
        ConnectionResetByPeer,
        Alignment,
        InputOutput,
        IsDir,
        SystemResources,
        Unseekable,
        ConnectionTimedOut,
    } || posix.UnexpectedError;

    pub fn read(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: ReadError!usize,
        ) void,
        completion: *Completion,
        fd: fd_t,
        buffer: []u8,
        offset: u64,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const ReadError!usize, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .read = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const RecvError = error{
        WouldBlock,
        FileDescriptorInvalid,
        ConnectionRefused,
        SystemResources,
        SocketNotConnected,
        FileDescriptorNotASocket,
        ConnectionResetByPeer,
        ConnectionTimedOut,
        OperationNotSupported,
    } || posix.UnexpectedError;

    pub fn recv(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: RecvError!usize,
        ) void,
        completion: *Completion,
        socket: posix.socket_t,
        buffer: []u8,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const RecvError!usize, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .recv = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const SendError = error{
        AccessDenied,
        WouldBlock,
        FastOpenAlreadyInProgress,
        AddressFamilyNotSupported,
        FileDescriptorInvalid,
        ConnectionResetByPeer,
        MessageTooBig,
        SystemResources,
        SocketNotConnected,
        FileDescriptorNotASocket,
        OperationNotSupported,
        BrokenPipe,
        ConnectionTimedOut,
    } || posix.UnexpectedError;

    pub fn send(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: SendError!usize,
        ) void,
        completion: *Completion,
        socket: posix.socket_t,
        buffer: []const u8,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const SendError!usize, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .send = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const StatxError = error{
        SymLinkLoop,
        FileNotFound,
        NameTooLong,
        NotDir,
    } || std.fs.File.StatError || posix.UnexpectedError;

    pub fn statx(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: StatxError!void,
        ) void,
        completion: *Completion,
        dir_fd: fd_t,
        file_path: [*:0]const u8,
        flags: u32,
        mask: u32,
        statxbuf: *std.os.linux.Statx,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const StatxError!void, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .statx = .{
                    .dir_fd = dir_fd,
                    .file_path = file_path,
                    .flags = flags,
                    .mask = mask,
                    .statxbuf = statxbuf,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const TimeoutError = error{Canceled} || posix.UnexpectedError;

    pub fn timeout(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: TimeoutError!void,
        ) void,
        completion: *Completion,
        nanoseconds: u63,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const TimeoutError!void, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .timeout = .{
                    .timespec = .{ .tv_sec = 0, .tv_nsec = nanoseconds },
                },
            },
        };

        // Special case a zero timeout as a yield.
        if (nanoseconds == 0) {
            completion.result = -@as(i32, @intFromEnum(posix.E.TIME));
            self.completed.push(completion);
            return;
        }

        self.enqueue(completion);
    }

    pub const WriteError = error{
        WouldBlock,
        NotOpenForWriting,
        NotConnected,
        DiskQuota,
        FileTooBig,
        Alignment,
        InputOutput,
        NoSpaceLeft,
        Unseekable,
        AccessDenied,
        BrokenPipe,
    } || posix.UnexpectedError;

    pub fn write(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: WriteError!usize,
        ) void,
        completion: *Completion,
        fd: fd_t,
        buffer: []const u8,
        offset: u64,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const WriteError!usize, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .write = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const Event = posix.fd_t;
    pub const INVALID_EVENT: Event = -1;

    pub fn open_event(self: *IO) !Event {
        _ = self;

        // eventfd initialized with no (zero) previous write value.
        const event_fd = posix.eventfd(0, linux.EFD.CLOEXEC) catch |err| switch (err) {
            error.SystemResources,
            error.SystemFdQuotaExceeded,
            error.ProcessFdQuotaExceeded,
            => return error.SystemResources,
            error.Unexpected => return error.Unexpected,
        };
        assert(event_fd != INVALID_EVENT);
        errdefer os.close(event_fd);

        return event_fd;
    }

    pub fn event_listen(
        self: *IO,
        event: Event,
        completion: *Completion,
        comptime on_event: fn (*Completion) void,
    ) void {
        assert(event != INVALID_EVENT);
        const Context = struct {
            const Context = @This();
            var buffer: u64 = undefined;

            fn on_read(
                _: *Context,
                completion_inner: *Completion,
                result: ReadError!usize,
            ) void {
                const bytes = result catch unreachable; // eventfd reads should not fail.
                assert(bytes == @sizeOf(u64));
                on_event(completion_inner);
            }
        };

        self.read(
            *Context,
            undefined,
            Context.on_read,
            completion,
            event,
            std.mem.asBytes(&Context.buffer),
            0, // eventfd reads must always start from 0 offset.
        );
    }

    pub fn event_trigger(self: *IO, event: Event, completion: *Completion) void {
        assert(event != INVALID_EVENT);
        _ = self;
        _ = completion;

        const value: u64 = 1;
        const bytes = posix.write(event, std.mem.asBytes(&value)) catch unreachable;
        assert(bytes == @sizeOf(u64));
    }

    pub fn close_event(self: *IO, event: Event) void {
        assert(event != INVALID_EVENT);
        _ = self;

        posix.close(event);
    }

    pub const INVALID_SOCKET = -1;

    /// Creates a socket that can be used for async operations with the IO instance.
    pub fn open_socket(self: *IO, family: u32, sock_type: u32, protocol: u32) !posix.socket_t {
        _ = self;
        return posix.socket(family, sock_type | posix.SOCK.CLOEXEC, protocol);
    }

    /// Closes a socket opened by the IO instance.
    pub fn close_socket(self: *IO, socket: posix.socket_t) void {
        _ = self;
        posix.close(socket);
    }

    /// Opens a directory with read only access.
    pub fn open_dir(dir_path: []const u8) !fd_t {
        return posix.open(dir_path, .{ .CLOEXEC = true, .ACCMODE = .RDONLY }, 0);
    }

    pub const fd_t = posix.fd_t;
    pub const INVALID_FILE: fd_t = -1;

    /// Opens or creates a journal file:
    /// - For reading and writing.
    /// - For Direct I/O (if possible in development mode, but required in production mode).
    /// - Obtains an advisory exclusive lock to the file descriptor.
    /// - Allocates the file contiguously on disk if this is supported by the file system.
    /// - Ensures that the file data (and file inode in the parent directory) is durable on disk.
    ///   The caller is responsible for ensuring that the parent directory inode is durable.
    /// - Verifies that the file size matches the expected file size before returning.
    pub fn open_data_file(
        self: *IO,
        dir_fd: fd_t,
        relative_path: []const u8,
        size: u64,
        method: enum { create, create_or_open, open, open_read_only },
        direct_io: DirectIO,
    ) !fd_t {
        _ = self;

        assert(relative_path.len > 0);
        assert(size % constants.sector_size == 0);
        // Be careful with openat(2): "If pathname is absolute, then dirfd is ignored." (man page)
        assert(!std.fs.path.isAbsolute(relative_path));

        var flags: posix.O = .{
            .CLOEXEC = true,
            .ACCMODE = if (method == .open_read_only) .RDONLY else .RDWR,
            .DSYNC = true,
        };
        var mode: posix.mode_t = 0;

        const kind: enum { file, block_device } = blk: {
            const stat = posix.fstatat(
                dir_fd,
                relative_path,
                0,
            ) catch |err| switch (err) {
                error.FileNotFound => {
                    if (method == .create or method == .create_or_open) {
                        // It's impossible to distinguish creating a new file and opening a new
                        // block device with the current API. So if it's possible that we should
                        // create a file we try that instead of failing here.
                        break :blk .file;
                    } else {
                        @panic("Path does not exist.");
                    }
                },
                else => |err_| return err_,
            };
            if (posix.S.ISBLK(stat.mode)) {
                break :blk .block_device;
            } else {
                if (!posix.S.ISREG(stat.mode)) {
                    @panic("file path does not point to block device or regular file.");
                }
                break :blk .file;
            }
        };

        // This is not strictly necessary on 64bit systems but it's harmless.
        // This will avoid errors with handling large files on certain configurations
        // of 32bit kernels. In all other cases, it's a noop.
        // See: <https://github.com/torvalds/linux/blob/ab27740f76654ed58dd32ac0ba0031c18a6dea3b/fs/open.c#L1602>
        if (@hasField(posix.O, "LARGEFILE")) flags.LARGEFILE = true;

        switch (kind) {
            .block_device => {
                if (direct_io != .direct_io_disabled) {
                    // Block devices should always support Direct IO.
                    flags.DIRECT = true;
                    // Use O_EXCL when opening as a block device to obtain an advisory exclusive
                    // lock. Normally, you can't do this for files you don't create, but for
                    // block devices this guarantees:
                    //     - that there are no mounts using this block device
                    //     - that no new mounts can use this block device while we have it open
                    //
                    // However it doesn't prevent other processes with root from opening without
                    // O_EXCL and writing (mount is just a special case that always checks O_EXCL).
                    //
                    // This should be stronger than flock(2) locks, which work on a separate system.
                    // The relevant kernel code (as of v6.7) is here:
                    // <https://github.com/torvalds/linux/blob/7da71072e1d6967c0482abcbb5991ffb5953fdf2/block/bdev.c#L932>
                    flags.EXCL = true;
                }
                log.info("opening block device \"{s}\"...", .{relative_path});
            },
            .file => {
                var direct_io_supported = false;
                const dir_on_tmpfs = try fs_is_tmpfs(dir_fd);

                if (dir_on_tmpfs) {
                    log.warn(
                        "tmpfs is not durable, and your data will be lost on reboot",
                        .{},
                    );
                }

                // Special case. tmpfs doesn't support Direct I/O. Normally we would panic
                // here (see below) but being able to benchmark production workloads
                // on tmpfs is very useful for removing
                // disk speed from the equation.
                if (direct_io != .direct_io_disabled and !dir_on_tmpfs) {
                    direct_io_supported = try fs_supports_direct_io(dir_fd);
                    if (direct_io_supported) {
                        flags.DIRECT = true;
                    } else if (direct_io == .direct_io_optional) {
                        log.warn("This file system does not support Direct I/O.", .{});
                    } else {
                        assert(direct_io == .direct_io_required);
                        // We require Direct I/O for safety to handle fsync failure correctly, and
                        // therefore panic in production if it is not supported.
                        log.err("This file system does not support Direct I/O.", .{});
                        log.err("TigerBeetle uses Direct I/O to bypass the kernel page cache, " ++
                            "to ensure that data is durable when writes complete.", .{});
                        log.err("If this is a production replica, Direct I/O is required.", .{});
                        log.err("If this is a development/testing replica, " ++
                            "re-run with --development set to bypass this error.", .{});
                        @panic("file system does not support Direct I/O");
                    }
                }

                switch (method) {
                    .create => {
                        flags.CREAT = true;
                        flags.EXCL = true;
                        mode = 0o666;
                        log.info("creating \"{s}\"...", .{relative_path});
                    },
                    .create_or_open => {
                        flags.CREAT = true;
                        mode = 0o666;
                        log.info("opening or creating \"{s}\"...", .{relative_path});
                    },
                    .open, .open_read_only => {
                        log.info("opening \"{s}\"...", .{relative_path});
                    },
                }
            },
        }

        // This is critical as we rely on O_DSYNC for fsync() whenever we write to the file:
        assert(flags.DSYNC);

        const fd = try posix.openat(dir_fd, relative_path, flags, mode);
        // TODO Return a proper error message when the path exists or does not exist (init/start).
        errdefer posix.close(fd);

        {
            // Make sure we're getting the type of file descriptor we expect.
            const stat = try posix.fstat(fd);
            switch (kind) {
                .file => assert(posix.S.ISREG(stat.mode)),
                .block_device => assert(posix.S.ISBLK(stat.mode)),
            }
        }

        // Obtain an advisory exclusive lock that works only if all processes actually use flock().
        // LOCK_NB means that we want to fail the lock without waiting if another process has it.
        //
        // This is wrapped inside a retry loop with a sleep because of the interaction between
        // io_uring semantics and flock: flocks are held per fd, but io_uring will keep a reference
        // to the fd alive even once a process has been terminated, until all async operations have
        // been completed.
        //
        // This means that when killing and starting a tigerbeetle process in an automated way, you
        // can see "another process holds the data file lock" errors, even though the process really
        // has terminated.
        for (0..2) |_| {
            posix.flock(fd, posix.LOCK.EX | posix.LOCK.NB) catch |err| switch (err) {
                error.WouldBlock => {
                    std.time.sleep(50 * std.time.ns_per_ms);
                    continue;
                },
                else => return err,
            };
            break;
        } else {
            posix.flock(fd, posix.LOCK.EX | posix.LOCK.NB) catch |err| switch (err) {
                error.WouldBlock => @panic("another process holds the data file lock"),
                else => return err,
            };
        }

        // Ask the file system to allocate contiguous sectors for the file (if possible):
        // If the file system does not support `fallocate()`, then this could mean more seeks or a
        // panic if we run out of disk space (ENOSPC).
        if (method == .create and kind == .file) {
            log.info("allocating {}...", .{std.fmt.fmtIntSizeBin(size)});
            fs_allocate(fd, size) catch |err| switch (err) {
                error.OperationNotSupported => {
                    log.warn("file system does not support fallocate(), an ENOSPC will panic", .{});
                    log.info("allocating by writing to the last sector " ++
                        "of the file instead...", .{});

                    const sector_size = constants.sector_size;
                    const sector: [sector_size]u8 align(sector_size) = [_]u8{0} ** sector_size;

                    // Handle partial writes where the physical sector is
                    // less than a logical sector:
                    const write_offset = size - sector.len;
                    var written: usize = 0;
                    while (written < sector.len) {
                        written += try posix.pwrite(fd, sector[written..], write_offset + written);
                    }
                },
                else => |e| return e,
            };
        }

        // The best fsync strategy is always to fsync before reading because this prevents us from
        // making decisions on data that was never durably written by a previously crashed process.
        // We therefore always fsync when we open the path, also to wait for any pending O_DSYNC.
        // Thanks to Alex Miller from FoundationDB for diving into our source and pointing this out.
        try posix.fsync(fd);

        // We fsync the parent directory to ensure that the file inode is durably written.
        // The caller is responsible for the parent directory inode stored under the grandparent.
        // We always do this when opening because we don't know if this was done before crashing.
        try posix.fsync(dir_fd);

        switch (kind) {
            .file => {
                if ((try posix.fstat(fd)).size < size) {
                    @panic("data file inode size was truncated or corrupted");
                }
            },
            .block_device => {
                const BLKGETSIZE64 = os.linux.IOCTL.IOR(0x12, 114, usize);
                var block_device_size: usize = 0;

                switch (os.linux.E.init(os.linux.ioctl(
                    fd,
                    BLKGETSIZE64,
                    @intFromPtr(&block_device_size),
                ))) {
                    .SUCCESS => {},

                    // These are the only errors that are supposed to be possible from ioctl(2).
                    .BADF => return error.InvalidFileDescriptor,
                    .NOTTY => return error.BadRequest,
                    .FAULT => return error.InvalidAddress,
                    else => |err| return stdx.unexpected_errno("open_file:ioctl", err),
                }

                if (block_device_size < size) {
                    std.debug.panic(
                        "The block device used is too small ({} available/{} needed).",
                        .{
                            std.fmt.fmtIntSizeBin(block_device_size),
                            std.fmt.fmtIntSizeBin(size),
                        },
                    );
                }

                if (method == .create or method == .create_or_open) {
                    // Check that the first superblock_zone_size bytes are 0.
                    // - It'll ensure that the block device is not directly TigerBeetle.
                    // - It'll be very likely to catch any cases where there's an existing
                    //   other filesystem.
                    // - In the case of there being a partition table (eg, two partitions,
                    //   one starting at 0MiB, one at 1024MiB) and the operator tries to format
                    //   the raw disk (/dev/sda) while a partition later is
                    //   TigerBeetle (/dev/sda2) it'll be blocked by the MBR/GPT existing.
                    const superblock_zone_size =
                        @import("../vsr/superblock.zig").superblock_zone_size;
                    var read_buf: [superblock_zone_size]u8 align(constants.sector_size) = undefined;

                    // We can do this without worrying about retrying partial reads because on
                    // linux, read(2) on block devices can not be interrupted by signals.
                    // See signal(7).
                    assert(superblock_zone_size == try posix.read(fd, &read_buf));
                    if (!std.mem.allEqual(u8, &read_buf, 0)) {
                        std.debug.panic(
                            "Superblock on block device not empty. " ++
                                "If this is the correct block device to use, " ++
                                "please zero the first {} using a tool like dd.",
                            .{std.fmt.fmtIntSizeBin(superblock_zone_size)},
                        );
                    }
                    // Reset position in the block device to compensate for read(2).
                    try posix.lseek_CUR(fd, -superblock_zone_size);
                    assert(try posix.lseek_CUR_get(fd) == 0);
                }
            },
        }

        return fd;
    }

    /// Detects whether the underlying file system for a given directory fd is tmpfs. This is used
    /// to relax our Direct I/O check - running on tmpfs for benchmarking is useful.
    fn fs_is_tmpfs(dir_fd: fd_t) !bool {
        var statfs: stdx.StatFs = undefined;

        while (true) {
            const res = stdx.fstatfs(dir_fd, &statfs);
            switch (os.linux.E.init(res)) {
                .SUCCESS => {
                    return statfs.f_type == stdx.TmpfsMagic;
                },
                .INTR => continue,
                else => |err| return stdx.unexpected_errno("fs_is_tmpfs", err),
            }
        }
    }

    /// Detects whether the underlying file system for a given directory fd supports Direct I/O.
    /// Not all Linux file systems support `O_DIRECT`, e.g. a shared macOS volume.
    fn fs_supports_direct_io(dir_fd: fd_t) !bool {
        if (!@hasField(posix.O, "DIRECT")) return false;

        var cookie: [16]u8 = .{'0'} ** 16;
        _ = stdx.array_print(16, &cookie, "{0x}", .{std.crypto.random.int(u64)});

        const path: [:0]const u8 = "fs_supports_direct_io-" ++ cookie ++ "";
        const dir = std.fs.Dir{ .fd = dir_fd };
        const flags: posix.O = .{ .CLOEXEC = true, .CREAT = true, .TRUNC = true };
        const fd = try posix.openatZ(dir_fd, path, flags, 0o666);
        defer posix.close(fd);
        defer dir.deleteFile(path) catch {};

        while (true) {
            const dir_flags: posix.O = .{ .CLOEXEC = true, .ACCMODE = .RDONLY, .DIRECT = true };
            const res = os.linux.openat(dir_fd, path, dir_flags, 0);
            switch (os.linux.E.init(res)) {
                .SUCCESS => {
                    posix.close(@intCast(res));
                    return true;
                },
                .INTR => continue,
                .INVAL => return false,
                else => |err| return stdx.unexpected_errno("fs_supports_direct_io", err),
            }
        }
    }

    /// Allocates a file contiguously using fallocate() if supported.
    /// Alternatively, writes to the last sector so that at least the file size is correct.
    fn fs_allocate(fd: fd_t, size: u64) !void {
        const mode: i32 = 0;
        const offset: i64 = 0;
        const length: i64 = @intCast(size);

        while (true) {
            const rc = os.linux.fallocate(fd, mode, offset, length);
            switch (os.linux.E.init(rc)) {
                .SUCCESS => return,
                .BADF => return error.FileDescriptorInvalid,
                .FBIG => return error.FileTooBig,
                .INTR => continue,
                .INVAL => return error.ArgumentsInvalid,
                .IO => return error.InputOutput,
                .NODEV => return error.NoDevice,
                .NOSPC => return error.NoSpaceLeft,
                .NOSYS => return error.SystemOutdated,
                .OPNOTSUPP => return error.OperationNotSupported,
                .PERM => return error.PermissionDenied,
                .SPIPE => return error.Unseekable,
                .TXTBSY => return error.FileBusy,
                else => |errno| return stdx.unexpected_errno("fs_allocate", errno),
            }
        }
    }
};
