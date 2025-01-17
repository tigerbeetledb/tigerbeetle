const std = @import("std");
const builtin = @import("builtin");
const os = std.os;
const posix = std.posix;
const testing = std.testing;
const assert = std.debug.assert;

const Time = @import("../time.zig").Time;
const IO = @import("../io.zig").IO;

test "open/write/read/close/statx" {
    try struct {
        const Context = @This();
        const StatxType = if (builtin.target.os.tag == .linux) std.os.linux.Statx else void;

        path: [:0]const u8 = "test_io_write_read_close",
        io: IO,
        done: bool = false,

        fd: ?posix.fd_t = null,
        write_buf: [20]u8 = [_]u8{97} ** 20,
        read_buf: [20]u8 = [_]u8{98} ** 20,

        written: usize = 0,
        read: usize = 0,

        statx: StatxType = undefined,

        fn run_test() !void {
            var self: Context = .{
                .io = try IO.init(32, 0),
            };
            defer self.io.deinit();

            // The file gets created below, either by createFile or openat.
            defer std.fs.cwd().deleteFile(self.path) catch {};

            var completion: IO.Completion = undefined;

            if (builtin.target.os.tag == .linux) {
                self.io.openat(
                    *Context,
                    &self,
                    openat_callback,
                    &completion,
                    posix.AT.FDCWD,
                    self.path,
                    .{ .ACCMODE = .RDWR, .TRUNC = true, .CREAT = true },
                    std.fs.File.default_mode,
                );
            } else {
                const file = try std.fs.cwd().createFile(self.path, .{
                    .read = true,
                    .truncate = true,
                });
                self.openat_callback(&completion, file.handle);
            }
            while (!self.done) try self.io.tick();

            try testing.expectEqual(self.write_buf.len, self.written);
            try testing.expectEqual(self.read_buf.len, self.read);
            try testing.expectEqualSlices(u8, &self.write_buf, &self.read_buf);

            if (builtin.target.os.tag == .linux) {
                // Offset of 10 specified to read / write below.
                try testing.expectEqual(self.statx.size - 10, self.written);
            }
        }

        fn openat_callback(
            self: *Context,
            completion: *IO.Completion,
            result: anyerror!posix.fd_t,
        ) void {
            self.fd = result catch @panic("openat error");
            self.io.write(
                *Context,
                self,
                write_callback,
                completion,
                self.fd.?,
                &self.write_buf,
                10,
            );
        }

        fn write_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.WriteError!usize,
        ) void {
            self.written = result catch @panic("write error");
            self.io.read(*Context, self, read_callback, completion, self.fd.?, &self.read_buf, 10);
        }

        fn read_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.ReadError!usize,
        ) void {
            self.read = result catch @panic("read error");
            self.io.close(*Context, self, close_callback, completion, self.fd.?);
        }

        fn close_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.CloseError!void,
        ) void {
            _ = result catch @panic("close error");

            if (builtin.target.os.tag == .linux) {
                self.io.statx(
                    *Context,
                    self,
                    statx_callback,
                    completion,
                    posix.AT.FDCWD,
                    self.path,
                    0,
                    os.linux.STATX_BASIC_STATS,
                    &self.statx,
                );
            } else {
                self.done = true;
            }
        }

        fn statx_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.StatxError!void,
        ) void {
            _ = completion;
            _ = result catch @panic("statx error");

            assert(!self.done);
            self.done = true;
        }
    }.run_test();
}

test "accept/connect/send/receive" {
    try struct {
        const Context = @This();

        io: *IO,
        done: bool = false,
        server: posix.socket_t,
        client: posix.socket_t,

        accepted_sock: posix.socket_t = undefined,

        send_buf: [10]u8 = [_]u8{ 1, 0, 1, 0, 1, 0, 1, 0, 1, 0 },
        recv_buf: [5]u8 = [_]u8{ 0, 1, 0, 1, 0 },

        sent: usize = 0,
        received: usize = 0,

        fn run_test() !void {
            var io = try IO.init(32, 0);
            defer io.deinit();

            const address = try std.net.Address.parseIp4("127.0.0.1", 0);
            const kernel_backlog = 1;

            const server = try io.open_socket(
                address.any.family,
                posix.SOCK.STREAM,
                posix.IPPROTO.TCP,
            );
            defer io.close_socket(server);

            const client = try io.open_socket(
                address.any.family,
                posix.SOCK.STREAM,
                posix.IPPROTO.TCP,
            );
            defer io.close_socket(client);

            try posix.setsockopt(
                server,
                posix.SOL.SOCKET,
                posix.SO.REUSEADDR,
                &std.mem.toBytes(@as(c_int, 1)),
            );
            try posix.bind(server, &address.any, address.getOsSockLen());
            try posix.listen(server, kernel_backlog);

            var client_address = std.net.Address.initIp4(undefined, undefined);
            var client_address_len = client_address.getOsSockLen();
            try posix.getsockname(server, &client_address.any, &client_address_len);

            var self: Context = .{
                .io = &io,
                .server = server,
                .client = client,
            };

            var client_completion: IO.Completion = undefined;
            self.io.connect(
                *Context,
                &self,
                connect_callback,
                &client_completion,
                client,
                client_address,
            );

            var server_completion: IO.Completion = undefined;
            self.io.accept(*Context, &self, accept_callback, &server_completion, server);

            while (!self.done) try self.io.tick();

            try testing.expectEqual(self.send_buf.len, self.sent);
            try testing.expectEqual(self.recv_buf.len, self.received);

            try testing.expectEqualSlices(u8, self.send_buf[0..self.received], &self.recv_buf);
        }

        fn connect_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.ConnectError!void,
        ) void {
            _ = result catch @panic("connect error");

            self.io.send(
                *Context,
                self,
                send_callback,
                completion,
                self.client,
                &self.send_buf,
            );
        }

        fn send_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.SendError!usize,
        ) void {
            _ = completion;

            self.sent = result catch @panic("send error");
        }

        fn accept_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.AcceptError!posix.socket_t,
        ) void {
            self.accepted_sock = result catch @panic("accept error");
            self.io.recv(
                *Context,
                self,
                recv_callback,
                completion,
                self.accepted_sock,
                &self.recv_buf,
            );
        }

        fn recv_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.RecvError!usize,
        ) void {
            _ = completion;

            self.received = result catch @panic("recv error");
            self.done = true;
        }
    }.run_test();
}

test "timeout" {
    const ms = 20;
    const margin = 100;
    const count = 10;

    try struct {
        const Context = @This();

        io: IO,
        timer: *Time,
        count: u32 = 0,
        stop_time: u64 = 0,

        fn run_test() !void {
            var timer = Time{};
            const start_time = timer.monotonic();
            var self: Context = .{
                .timer = &timer,
                .io = try IO.init(32, 0),
            };
            defer self.io.deinit();

            var completions: [count]IO.Completion = undefined;
            for (&completions) |*completion| {
                self.io.timeout(
                    *Context,
                    &self,
                    timeout_callback,
                    completion,
                    ms * std.time.ns_per_ms,
                );
            }
            while (self.count < count) try self.io.tick();

            try self.io.tick();
            try testing.expectEqual(@as(u32, count), self.count);

            try testing.expectApproxEqAbs(
                @as(f64, ms),
                @as(f64, @floatFromInt((self.stop_time - start_time) / std.time.ns_per_ms)),
                margin,
            );
        }

        fn timeout_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.TimeoutError!void,
        ) void {
            _ = completion;
            _ = result catch @panic("timeout error");

            if (self.stop_time == 0) self.stop_time = self.timer.monotonic();
            self.count += 1;
        }
    }.run_test();
}

test "event" {
    try struct {
        const Context = @This();

        io: IO,
        count: u32 = 0,
        main_thread_id: std.Thread.Id,
        event: IO.Event = IO.INVALID_EVENT,
        event_completion: IO.Completion = undefined,

        const delay = 5 * std.time.ns_per_ms;
        const events_count = 5;

        fn run_test() !void {
            var self: Context = .{
                .io = try IO.init(32, 0),
                .main_thread_id = std.Thread.getCurrentId(),
            };
            defer self.io.deinit();

            self.event = try self.io.open_event();
            defer self.io.close_event(self.event);

            var timer = Time{};
            const start = timer.monotonic();

            // Listen to the event and spawn a thread that triggers the completion after some time.
            self.io.event_listen(self.event, &self.event_completion, on_event);
            const thread = try std.Thread.spawn(.{}, Context.trigger_event, .{&self});

            // Wait for the number of events to complete.
            while (self.count < events_count) try self.io.tick();
            thread.join();

            // Make sure the event was triggered multiple times.
            assert(self.count == events_count);

            // Make sure at least some time has passed.
            const elapsed = timer.monotonic() - start;
            assert(elapsed >= delay);
        }

        fn trigger_event(self: *Context) void {
            assert(std.Thread.getCurrentId() != self.main_thread_id);
            while (self.count < events_count) {
                std.time.sleep(delay + 1);

                // Triggering the event:
                self.io.event_trigger(self.event, &self.event_completion);
            }
        }

        fn on_event(completion: *IO.Completion) void {
            const self: *Context = @fieldParentPtr("event_completion", completion);
            assert(std.Thread.getCurrentId() == self.main_thread_id);

            self.count += 1;
            if (self.count == events_count) return;

            // Reattaching the event.
            self.io.event_listen(self.event, &self.event_completion, on_event);
        }
    }.run_test();
}

test "submission queue full" {
    const ms = 20;
    const count = 10;

    try struct {
        const Context = @This();

        io: IO,
        count: u32 = 0,

        fn run_test() !void {
            var self: Context = .{ .io = try IO.init(1, 0) };
            defer self.io.deinit();

            var completions: [count]IO.Completion = undefined;
            for (&completions) |*completion| {
                self.io.timeout(
                    *Context,
                    &self,
                    timeout_callback,
                    completion,
                    ms * std.time.ns_per_ms,
                );
            }
            while (self.count < count) try self.io.tick();

            try self.io.tick();
            try testing.expectEqual(@as(u32, count), self.count);
        }

        fn timeout_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.TimeoutError!void,
        ) void {
            _ = completion;
            _ = result catch @panic("timeout error");

            self.count += 1;
        }
    }.run_test();
}

test "tick to wait" {
    // Use only IO.tick() to see if pending IO is actually processed

    try struct {
        const Context = @This();

        io: IO,
        accepted: posix.socket_t = IO.INVALID_SOCKET,
        connected: bool = false,
        received: bool = false,

        fn run_test() !void {
            var self: Context = .{ .io = try IO.init(1, 0) };
            defer self.io.deinit();

            const address = try std.net.Address.parseIp4("127.0.0.1", 0);
            const kernel_backlog = 1;

            const server =
                try self.io.open_socket(address.any.family, posix.SOCK.STREAM, posix.IPPROTO.TCP);
            defer self.io.close_socket(server);

            try posix.setsockopt(
                server,
                posix.SOL.SOCKET,
                posix.SO.REUSEADDR,
                &std.mem.toBytes(@as(c_int, 1)),
            );
            try posix.bind(server, &address.any, address.getOsSockLen());
            try posix.listen(server, kernel_backlog);

            var client_address = std.net.Address.initIp4(undefined, undefined);
            var client_address_len = client_address.getOsSockLen();
            try posix.getsockname(server, &client_address.any, &client_address_len);

            const client = try self.io.open_socket(
                client_address.any.family,
                posix.SOCK.STREAM,
                posix.IPPROTO.TCP,
            );
            defer self.io.close_socket(client);

            // Start the accept
            var server_completion: IO.Completion = undefined;
            self.io.accept(*Context, &self, accept_callback, &server_completion, server);

            // Start the connect
            var client_completion: IO.Completion = undefined;
            self.io.connect(
                *Context,
                &self,
                connect_callback,
                &client_completion,
                client,
                client_address,
            );

            // Tick the IO to drain the accept & connect completions
            assert(!self.connected);
            assert(self.accepted == IO.INVALID_SOCKET);

            while (self.accepted == IO.INVALID_SOCKET or !self.connected)
                try self.io.tick();

            assert(self.connected);
            assert(self.accepted != IO.INVALID_SOCKET);
            defer self.io.close_socket(self.accepted);

            // Start receiving on the client
            var recv_completion: IO.Completion = undefined;
            var recv_buffer: [64]u8 = undefined;
            @memset(&recv_buffer, 0xaa);
            self.io.recv(
                *Context,
                &self,
                recv_callback,
                &recv_completion,
                client,
                &recv_buffer,
            );

            // Drain out the recv completion from any internal IO queues
            try self.io.tick();
            try self.io.tick();
            try self.io.tick();

            // Complete the recv() *outside* of the IO instance.
            // Other tests already check .tick() with IO based completions.
            // This simulates IO being completed by an external system
            var send_buf = std.mem.zeroes([64]u8);
            const wrote = try os_send(self.accepted, &send_buf, 0);
            try testing.expectEqual(wrote, send_buf.len);

            // Wait for the recv() to complete using only IO.tick().
            // If tick is broken, then this will deadlock
            assert(!self.received);
            while (!self.received) {
                try self.io.tick();
            }

            // Make sure the receive actually happened
            assert(self.received);
            try testing.expect(std.mem.eql(u8, &recv_buffer, &send_buf));
        }

        fn accept_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.AcceptError!posix.socket_t,
        ) void {
            _ = completion;

            assert(self.accepted == IO.INVALID_SOCKET);
            self.accepted = result catch @panic("accept error");
        }

        fn connect_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.ConnectError!void,
        ) void {
            _ = completion;
            _ = result catch @panic("connect error");

            assert(!self.connected);
            self.connected = true;
        }

        fn recv_callback(
            self: *Context,
            completion: *IO.Completion,
            result: IO.RecvError!usize,
        ) void {
            _ = completion;
            _ = result catch |err| std.debug.panic("recv error: {}", .{err});

            assert(!self.received);
            self.received = true;
        }

        // TODO: use posix.send() instead when it gets fixed for windows
        fn os_send(sock: posix.socket_t, buf: []const u8, flags: u32) !usize {
            if (builtin.target.os.tag != .windows) {
                return posix.send(sock, buf, flags);
            }

            const rc = os.windows.sendto(sock, buf.ptr, buf.len, flags, null, 0);
            if (rc == os.windows.ws2_32.SOCKET_ERROR) {
                switch (os.windows.ws2_32.WSAGetLastError()) {
                    .WSAEACCES => return error.AccessDenied,
                    .WSAEADDRNOTAVAIL => return error.AddressNotAvailable,
                    .WSAECONNRESET => return error.ConnectionResetByPeer,
                    .WSAEMSGSIZE => return error.MessageTooBig,
                    .WSAENOBUFS => return error.SystemResources,
                    .WSAENOTSOCK => return error.FileDescriptorNotASocket,
                    .WSAEAFNOSUPPORT => return error.AddressFamilyNotSupported,
                    .WSAEDESTADDRREQ => unreachable, // A destination address is required.
                    // The lpBuffers, lpTo, lpOverlapped, lpNumberOfBytesSent,
                    // or lpCompletionRoutine parameters are not part of the user address space,
                    // or the lpTo parameter is too small.
                    .WSAEFAULT => unreachable,
                    .WSAEHOSTUNREACH => return error.NetworkUnreachable,
                    // TODO: WSAEINPROGRESS, WSAEINTR
                    .WSAEINVAL => unreachable,
                    .WSAENETDOWN => return error.NetworkSubsystemFailed,
                    .WSAENETRESET => return error.ConnectionResetByPeer,
                    .WSAENETUNREACH => return error.NetworkUnreachable,
                    .WSAENOTCONN => return error.SocketNotConnected,
                    // The socket has been shut down; it is not possible to WSASendTo on a socket
                    // after shutdown has been invoked with how set to SD_SEND or SD_BOTH.
                    .WSAESHUTDOWN => unreachable,
                    .WSAEWOULDBLOCK => return error.WouldBlock,
                    // A successful WSAStartup call must occur before using this function.
                    .WSANOTINITIALISED => unreachable,
                    else => |err| return os.windows.unexpectedWSAError(err),
                }
            } else {
                return @intCast(rc);
            }
        }
    }.run_test();
}

test "pipe data over socket" {
    try struct {
        io: IO,
        tx: Pipe,
        rx: Pipe,
        server: Socket = .{},

        const buffer_size = 1 * 1024 * 1024;

        const Context = @This();
        const Socket = struct {
            fd: posix.socket_t = IO.INVALID_SOCKET,
            completion: IO.Completion = undefined,
        };
        const Pipe = struct {
            socket: Socket = .{},
            buffer: []u8,
            transferred: usize = 0,
        };

        fn run() !void {
            const tx_buf = try testing.allocator.alloc(u8, buffer_size);
            defer testing.allocator.free(tx_buf);
            const rx_buf = try testing.allocator.alloc(u8, buffer_size);
            defer testing.allocator.free(rx_buf);

            @memset(tx_buf, 1);
            @memset(rx_buf, 0);
            var self = Context{
                .io = try IO.init(32, 0),
                .tx = .{ .buffer = tx_buf },
                .rx = .{ .buffer = rx_buf },
            };
            defer self.io.deinit();

            self.server.fd = try self.io.open_socket(
                posix.AF.INET,
                posix.SOCK.STREAM,
                posix.IPPROTO.TCP,
            );
            defer self.io.close_socket(self.server.fd);

            const address = try std.net.Address.parseIp4("127.0.0.1", 0);
            try posix.setsockopt(
                self.server.fd,
                posix.SOL.SOCKET,
                posix.SO.REUSEADDR,
                &std.mem.toBytes(@as(c_int, 1)),
            );

            try posix.bind(self.server.fd, &address.any, address.getOsSockLen());
            try posix.listen(self.server.fd, 1);

            var client_address = std.net.Address.initIp4(undefined, undefined);
            var client_address_len = client_address.getOsSockLen();
            try posix.getsockname(self.server.fd, &client_address.any, &client_address_len);

            self.io.accept(
                *Context,
                &self,
                on_accept,
                &self.server.completion,
                self.server.fd,
            );

            self.tx.socket.fd = try self.io.open_socket(
                posix.AF.INET,
                posix.SOCK.STREAM,
                posix.IPPROTO.TCP,
            );
            defer self.io.close_socket(self.tx.socket.fd);

            self.io.connect(
                *Context,
                &self,
                on_connect,
                &self.tx.socket.completion,
                self.tx.socket.fd,
                client_address,
            );

            var tick: usize = 0xdeadbeef;
            while (self.rx.transferred != self.rx.buffer.len) : (tick +%= 1) {
                if (tick % 61 == 0) {
                    const timeout_ns = tick % (10 * std.time.ns_per_ms);
                    try self.io.run_for_ns(@as(u63, @intCast(timeout_ns)));
                } else {
                    try self.io.tick();
                }
            }

            try testing.expect(self.server.fd != IO.INVALID_SOCKET);
            try testing.expect(self.tx.socket.fd != IO.INVALID_SOCKET);
            try testing.expect(self.rx.socket.fd != IO.INVALID_SOCKET);
            self.io.close_socket(self.rx.socket.fd);

            try testing.expectEqual(self.tx.transferred, buffer_size);
            try testing.expectEqual(self.rx.transferred, buffer_size);
            try testing.expect(std.mem.eql(u8, self.tx.buffer, self.rx.buffer));
        }

        fn on_accept(
            self: *Context,
            completion: *IO.Completion,
            result: IO.AcceptError!posix.socket_t,
        ) void {
            assert(self.rx.socket.fd == IO.INVALID_SOCKET);
            assert(&self.server.completion == completion);
            self.rx.socket.fd = result catch |err| std.debug.panic("accept error {}", .{err});

            assert(self.rx.transferred == 0);
            self.do_receiver(0);
        }

        fn on_connect(
            self: *Context,
            completion: *IO.Completion,
            result: IO.ConnectError!void,
        ) void {
            _ = result catch unreachable;

            assert(self.tx.socket.fd != IO.INVALID_SOCKET);
            assert(&self.tx.socket.completion == completion);

            assert(self.tx.transferred == 0);
            self.do_sender(0);
        }

        fn do_sender(self: *Context, bytes: usize) void {
            self.tx.transferred += bytes;
            assert(self.tx.transferred <= self.tx.buffer.len);

            if (self.tx.transferred < self.tx.buffer.len) {
                self.io.send(
                    *Context,
                    self,
                    on_send,
                    &self.tx.socket.completion,
                    self.tx.socket.fd,
                    self.tx.buffer[self.tx.transferred..],
                );
            }
        }

        fn on_send(
            self: *Context,
            completion: *IO.Completion,
            result: IO.SendError!usize,
        ) void {
            const bytes = result catch |err| std.debug.panic("send error: {}", .{err});
            assert(&self.tx.socket.completion == completion);
            self.do_sender(bytes);
        }

        fn do_receiver(self: *Context, bytes: usize) void {
            self.rx.transferred += bytes;
            assert(self.rx.transferred <= self.rx.buffer.len);

            if (self.rx.transferred < self.rx.buffer.len) {
                self.io.recv(
                    *Context,
                    self,
                    on_recv,
                    &self.rx.socket.completion,
                    self.rx.socket.fd,
                    self.rx.buffer[self.rx.transferred..],
                );
            }
        }

        fn on_recv(
            self: *Context,
            completion: *IO.Completion,
            result: IO.RecvError!usize,
        ) void {
            const bytes = result catch |err| std.debug.panic("recv error: {}", .{err});
            assert(&self.rx.socket.completion == completion);
            self.do_receiver(bytes);
        }
    }.run();
}

test "cancel_all" {
    const checksum = @import("../vsr/checksum.zig").checksum;
    const allocator = std.testing.allocator;
    const file_path = "test_cancel_all";
    const read_count = 8;
    const read_size = 1024 * 16;

    // For this test to be useful, we rely on open(DIRECT).
    // (See below).
    if (builtin.target.os.tag != .linux) return;

    try struct {
        const Context = @This();

        io: IO,
        canceled: bool = false,

        fn run_test() !void {
            defer std.fs.cwd().deleteFile(file_path) catch {};

            var context: Context = .{ .io = try IO.init(32, 0) };
            defer context.io.deinit();

            {
                // Initialize a file filled with test data.
                const file_buffer = try allocator.alloc(u8, read_size);
                defer allocator.free(file_buffer);
                for (file_buffer, 0..) |*b, i| b.* = @intCast(i % 256);

                try std.fs.cwd().writeFile(.{ .sub_path = file_path, .data = file_buffer });
            }

            var read_completions: [read_count]IO.Completion = undefined;
            var read_buffers: [read_count][]u8 = undefined;
            var read_buffer_checksums: [read_count]u128 = undefined;
            var read_buffers_allocated: u32 = 0;
            defer for (read_buffers[0..read_buffers_allocated]) |b| allocator.free(b);

            for (&read_buffers) |*read_buffer| {
                read_buffer.* = try allocator.alloc(u8, read_size);
                read_buffers_allocated += 1;
            }

            // Test cancellation:
            // 1. Re-open the file.
            // 2. Kick off multiple (async) reads.
            // 3. Abort the reads (ideally before they can complete, since that is more interesting
            //    to test).
            //
            // The reason to re-open the file with DIRECT is that it slows down the reads enough to
            // actually test the interesting case -- cancelling an in-flight read and verifying that
            // the buffer is not written to after `cancel_all()` completes.
            //
            // (Without DIRECT the reads all finish their callbacks even before io.tick() returns.)
            const file = try std.posix.open(file_path, .{ .DIRECT = true }, 0);
            defer std.posix.close(file);

            for (&read_completions, read_buffers) |*completion, buffer| {
                context.io.read(*Context, &context, read_callback, completion, file, buffer, 0);
            }
            try context.io.tick();

            // Set to true *before* calling cancel_all() to ensure that any farther callbacks from
            // IO completion will panic.
            context.canceled = true;

            context.io.cancel_all();

            // All of the in-flight reads are canceled at this point.
            // To verify, checksum all of the read buffer memory, then wait and make sure that there
            // are no farther modifications to the buffers.
            for (read_buffers, &read_buffer_checksums) |buffer, *buffer_checksum| {
                buffer_checksum.* = checksum(buffer);
            }

            const sleep_ms = 50;
            std.time.sleep(sleep_ms * std.time.ns_per_ms);

            for (read_buffers, read_buffer_checksums) |buffer, buffer_checksum| {
                try testing.expectEqual(checksum(buffer), buffer_checksum);
            }
        }

        fn read_callback(
            context: *Context,
            completion: *IO.Completion,
            result: IO.ReadError!usize,
        ) void {
            _ = completion;
            _ = result catch @panic("read error");

            assert(!context.canceled);
        }
    }.run_test();
}
