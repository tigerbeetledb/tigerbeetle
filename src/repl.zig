const std = @import("std");
const assert = std.debug.assert;

const vsr = @import("vsr.zig");
const stdx = vsr.stdx;
const constants = vsr.constants;
const IO = vsr.io.IO;
const Storage = vsr.storage.StorageType(IO);
const StateMachine = vsr.state_machine.StateMachineType(
    Storage,
    constants.state_machine_config,
);
const MessagePool = vsr.message_pool.MessagePool;
const RingBufferType = stdx.RingBufferType;

const tb = vsr.tigerbeetle;

const Terminal = @import("repl/terminal.zig").Terminal;
const Completion = @import("repl/completion.zig").Completion;
const Parser = @import("repl/parser.zig").Parser;

const repl_history_entries = 256;
const repl_history_entry_bytes_with_nul = 512;
const repl_history_entry_bytes_without_nul = 511;

const ReplBufferBoundedArray = stdx.BoundedArrayType(u8, repl_history_entry_bytes_without_nul);

pub fn ReplType(comptime MessageBus: type, comptime Time: type) type {
    const Client = vsr.ClientType(StateMachine, MessageBus, Time);

    // Requires 512 * 256 == 128KiB of stack space.
    const HistoryBuffer = RingBufferType(
        [repl_history_entry_bytes_with_nul]u8,
        .{ .array = repl_history_entries },
    );

    return struct {
        event_loop_done: bool,
        request_done: bool,

        interactive: bool,
        debug_logs: bool,

        client: Client,
        terminal: Terminal,
        completion: Completion,
        history: HistoryBuffer,

        /// Fixed-capacity buffer for reading input strings.
        buffer: ReplBufferBoundedArray,
        /// Saved input string while navigating through history.
        buffer_outside_history: ReplBufferBoundedArray,

        arguments: std.ArrayListUnmanaged(u8),
        message_pool: *MessagePool,
        io: *IO,

        const Repl = @This();

        fn fail(repl: *const Repl, comptime format: []const u8, arguments: anytype) !void {
            if (!repl.interactive) {
                try repl.terminal.print_error(format, arguments);
                std.process.exit(1);
            }

            try repl.terminal.print(format, arguments);
        }

        fn debug(repl: *const Repl, comptime format: []const u8, arguments: anytype) !void {
            if (repl.debug_logs) {
                try repl.terminal.print("[Debug] " ++ format, arguments);
            }
        }

        fn do_statement(
            repl: *Repl,
            statement: Parser.Statement,
        ) !void {
            try repl.debug("Running command: {}.\n", .{statement.operation});
            switch (statement.operation) {
                .none => {
                    // No input was parsed.
                    try repl.debug("No command was parsed, continuing.\n", .{});
                },
                .help => {
                    try repl.display_help();
                },

                .create_accounts,
                .create_transfers,
                .lookup_accounts,
                .lookup_transfers,
                .get_account_transfers,
                .get_account_balances,
                .query_accounts,
                .query_transfers,
                => |operation| {
                    const state_machine_operation =
                        std.meta.stringToEnum(StateMachine.Operation, @tagName(operation));
                    assert(state_machine_operation != null);

                    try repl.send(state_machine_operation.?, statement.arguments);
                },
            }
        }

        const prompt = "> ";

        fn read_until_newline_or_eof(
            repl: *Repl,
        ) !?[]const u8 {
            repl.buffer.clear();
            repl.buffer_outside_history.clear();

            try repl.terminal.prompt_mode_set();
            defer repl.terminal.prompt_mode_unset() catch {};

            var terminal_screen = try repl.terminal.get_screen();

            var buffer_index: usize = 0;
            var history_index = repl.history.count;

            var current_completion: []const u8 = "";

            while (true) {
                const user_input = try repl.terminal.read_user_input() orelse return null;

                // Clear the completion menu when a match is selected or another key is pressed.
                if (user_input != .tab and repl.completion.count() > 0) {
                    repl.completion.clear();
                    try repl.terminal.print("\x1b[{};1H\x1b[J\x1b[{};{}H", .{
                        terminal_screen.cursor_row + 1,
                        terminal_screen.cursor_row,
                        terminal_screen.cursor_column,
                    });
                }

                switch (user_input) {
                    .ctrlc => {
                        // Erase everything below the current cursor's position in case Ctrl-C was
                        // pressed somewhere inside the buffer.
                        try repl.terminal.print("^C\x1b[J\n", .{});
                        return &.{};
                    },
                    .newline => {
                        try repl.terminal.print("\n", .{});
                        return repl.buffer.const_slice();
                    },
                    .printable => |character| {
                        if (repl.buffer.count() == repl_history_entry_bytes_without_nul) {
                            continue;
                        }

                        const is_append = buffer_index == repl.buffer.count();
                        if (is_append) {
                            terminal_screen.update_cursor_position(1);
                            try repl.terminal.print("{c}", .{character});
                            // Some terminals may not automatically move/scroll us down to the next
                            // row after appending a character at the last column. This can cause us
                            // to incorrectly report the cursor's position, so we force the terminal
                            // to do so by moving the cursor forward and backward one space.
                            if (terminal_screen.cursor_column == terminal_screen.columns) {
                                try repl.terminal.print("\x20\x1b[{};{}H", .{
                                    terminal_screen.cursor_row,
                                    terminal_screen.cursor_column,
                                });
                            }
                        } else {
                            // If we're inserting mid-buffer, we need to redraw everything that
                            // comes after as well. We'll track as if the cursor moved to the end of
                            // the buffer after redrawing, and move it back to one position after
                            // the newly inserted character.
                            const buffer_redraw_len: isize = @intCast(
                                repl.buffer.count() - buffer_index,
                            );
                            // It's crucial to update in two steps because the terminal may have
                            // scrolled down as part of the text redraw.
                            terminal_screen.update_cursor_position(buffer_redraw_len);
                            terminal_screen.update_cursor_position(1 - buffer_redraw_len);
                            try repl.terminal.print("{c}{s}\x1b[{};{}H", .{
                                character,
                                repl.buffer.const_slice()[buffer_index..],
                                terminal_screen.cursor_row,
                                terminal_screen.cursor_column,
                            });
                        }

                        repl.buffer.insert_assume_capacity(buffer_index, character);
                        buffer_index += 1;
                    },
                    .backspace => if (buffer_index > 0) {
                        terminal_screen.update_cursor_position(-1);
                        try repl.terminal.print("\x1b[{};{}H{s}\x20\x1b[{};{}H", .{
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                            // If we're deleting mid-buffer, we need to redraw everything that
                            // comes after as well.
                            if (buffer_index < repl.buffer.count())
                                repl.buffer.const_slice()[buffer_index..]
                            else
                                "",
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                        });
                        buffer_index -= 1;
                        _ = repl.buffer.ordered_remove(buffer_index);
                    },
                    .tab => {
                        try repl.completion.split_and_complete(repl.buffer.slice(), buffer_index);

                        current_completion = try repl.completion.get_next_completion();

                        // Skip the current completion entry if adding it would overflow the buffer.
                        if ((repl.completion.prefix.count() + repl.completion.suffix.count() +
                            current_completion.len) >= repl_history_entry_bytes_without_nul)
                        {
                            continue;
                        }

                        // Move cursor to the start of the current row, clear the screen from start
                        // to end. Then print the leading buffer, currently selected completion and
                        // trailing buffer(if any).
                        terminal_screen.update_cursor_position(-@as(isize, @intCast(buffer_index)));

                        const buffer_row = terminal_screen.cursor_row;

                        terminal_screen.update_cursor_position(
                            @as(
                                isize,
                                @intCast(repl.completion.prefix.count() + current_completion.len),
                            ),
                        );

                        // \x1b[n;mH - Moves the cursor to row n, column m.
                        // \x1b[J - Clear the screen from current cursor position to end.
                        try repl.terminal.print("\x1b[{};1H\x1b[J{s}{s}{s}{s}\x20\x08", .{
                            buffer_row,
                            prompt,
                            repl.completion.prefix.const_slice(),
                            current_completion,
                            repl.completion.suffix.const_slice(),
                        });

                        // When cursor reaches the last row of terminal, scroll content upward
                        // to create space for displaying completion menu.
                        // \x1b[nS - Scroll whole page up by n lines; defualt 1 line.
                        // \x1b[nA - Moves cursor up n cells; default 1 cell up.
                        if (terminal_screen.cursor_row == terminal_screen.rows) {
                            try repl.terminal.print("\x1b[S\x1b[A", .{});
                        }

                        // Position cursor for completion menu. If the current buffer overflows to
                        // the next line after printing the prefix, selected completion and suffix,
                        // the completion menu should effectively be placed `row_delta` lines below
                        // the current buffer row.
                        const buffer_width =
                            repl.completion.prefix.count() +
                            repl.completion.suffix.count() +
                            current_completion.len + 1;
                        const row_delta = @divFloor(buffer_width, terminal_screen.columns) + 1;
                        try repl.terminal.print("\x1b[{};1H", .{buffer_row + row_delta});

                        // Display completion menu in the next line and highlight the currently
                        // selected completion.
                        var menu_width: usize = 0;
                        var match_itr = repl.completion.matches.iterator();

                        while (match_itr.next()) |match| {
                            const match_len = std.mem.indexOf(u8, match[0..], &[_]u8{'\x00'}).?;
                            menu_width += match_len + 2;

                            // \x1b[7m - Highlights the text by inverting the color. Inverts
                            //           the text and background color.
                            // \x1b[0m - Reset the style property of text. In this case, resets
                            //           the effect of \x1b[7m - color inversion.
                            if (std.mem.eql(u8, match[0..match_len], current_completion)) {
                                try repl.terminal.print("\x20\x1b[7m{s}\x1b[0m\x20", .{
                                    match[0..match_len],
                                });
                            } else {
                                try repl.terminal.print("\x20{s}\x20", .{match[0..match_len]});
                            }
                        }

                        // If the completion menu overflows past the last row of terminal screen,
                        // move the cursor up by number of rows that would be overflown.
                        const menu_rows = @divFloor(menu_width, terminal_screen.columns) + 1;
                        const required_rows = terminal_screen.cursor_row + menu_rows;
                        const overflow_rows = @as(
                            isize,
                            @intCast(required_rows),
                        ) - @as(
                            isize,
                            @intCast(terminal_screen.rows),
                        );
                        if (overflow_rows > 0) {
                            terminal_screen.update_cursor_position(
                                -@as(isize, @intCast(terminal_screen.columns)) * overflow_rows,
                            );
                        }

                        try repl.terminal.print("\x1b[{};{}H", .{
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                        });

                        // Re-fill the buffer with prefix, current match and suffix. Set the
                        // buffer index where the current selected match ends.
                        repl.buffer.clear();
                        repl.buffer.append_slice_assume_capacity(repl.completion.prefix.slice());
                        repl.buffer.append_slice_assume_capacity(current_completion);
                        repl.buffer.append_slice_assume_capacity(repl.completion.suffix.slice());
                        buffer_index = repl.buffer.count() - repl.completion.suffix.count();
                    },
                    .left => if (buffer_index > 0) {
                        terminal_screen.update_cursor_position(-1);
                        try repl.terminal.print("\x1b[{};{}H", .{
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                        });
                        buffer_index -= 1;
                    },
                    .right => if (buffer_index < repl.buffer.count()) {
                        terminal_screen.update_cursor_position(1);
                        try repl.terminal.print("\x1b[{};{}H", .{
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                        });
                        buffer_index += 1;
                    },
                    .up => if (history_index > 0) {
                        const history_index_next = history_index - 1;
                        const buffer_next_full = repl.history.get_ptr(history_index_next).?;
                        const buffer_next = std.mem.sliceTo(buffer_next_full, '\x00');
                        assert(buffer_next.len < repl_history_entry_bytes_with_nul);

                        // Move to the beginning of the current buffer.
                        terminal_screen.update_cursor_position(
                            -@as(isize, @intCast(buffer_index)),
                        );
                        const row_current_buffer_start = terminal_screen.cursor_row;

                        // Move to the end of the new buffer.
                        terminal_screen.update_cursor_position(
                            @as(isize, @intCast(buffer_next.len)),
                        );

                        try repl.terminal.print("\x1b[{};1H\x1b[J{s}{s}\x20\x1b[{};{}H", .{
                            row_current_buffer_start,
                            prompt,
                            buffer_next,
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                        });

                        if (history_index == repl.history.count) {
                            repl.buffer_outside_history.clear();
                            repl.buffer_outside_history.append_slice_assume_capacity(
                                repl.buffer.const_slice(),
                            );
                        }
                        history_index = history_index_next;

                        repl.buffer.clear();
                        repl.buffer.append_slice_assume_capacity(buffer_next);
                        buffer_index = repl.buffer.count();
                    },
                    .down => if (history_index < repl.history.count) {
                        const history_index_next = history_index + 1;

                        const buffer_next = if (history_index_next == repl.history.count)
                            repl.buffer_outside_history.const_slice()
                        else brk: {
                            const buffer_next_full = repl.history.get_ptr(history_index_next).?;
                            const buffer_next = std.mem.sliceTo(buffer_next_full, '\x00');
                            assert(buffer_next.len < repl_history_entry_bytes_with_nul);
                            break :brk buffer_next;
                        };

                        history_index = history_index_next;

                        // Move to the beginning of the current buffer.
                        terminal_screen.update_cursor_position(
                            -@as(isize, @intCast(buffer_index)),
                        );
                        const row_current_buffer_start = terminal_screen.cursor_row;

                        // Move to the end of the new buffer.
                        terminal_screen.update_cursor_position(
                            @as(isize, @intCast(buffer_next.len)),
                        );

                        try repl.terminal.print("\x1b[{};1H\x1b[J{s}{s}\x20\x1b[{};{}H", .{
                            row_current_buffer_start,
                            prompt,
                            buffer_next,
                            terminal_screen.cursor_row,
                            terminal_screen.cursor_column,
                        });

                        repl.buffer.clear();
                        repl.buffer.append_slice_assume_capacity(buffer_next);
                        buffer_index = repl.buffer.count();
                    },
                    .unhandled => {},
                }
            }
            unreachable;
        }

        fn do_repl(
            repl: *Repl,
            arguments: *std.ArrayListUnmanaged(u8),
        ) !void {
            try repl.terminal.print(prompt, .{});
            const input = repl.read_until_newline_or_eof() catch |err| {
                repl.event_loop_done = true;
                return err;
            } orelse {
                // EOF.
                repl.event_loop_done = true;
                try repl.fail("\nExiting.\n", .{});
                return;
            };

            if (input.len > 0) {
                if (repl.history.empty() or
                    !std.mem.eql(u8, repl.history.tail_ptr_const().?[0..input.len], input))
                {
                    // NB: Avoiding big stack allocations below.

                    assert(input.len < repl_history_entry_bytes_with_nul);

                    if (repl.history.full()) {
                        repl.history.advance_head();
                    }
                    const history_tail: *[repl_history_entry_bytes_with_nul]u8 =
                        repl.history.next_tail_ptr().?;
                    @memset(history_tail, '\x00');
                    stdx.copy_left(.inexact, u8, history_tail, input);
                    repl.history.advance_tail();
                }
            }

            const statement = Parser.parse_statement(
                input,
                &repl.terminal,
                arguments,
            ) catch |err| {
                switch (err) {
                    // These are parsing errors, so the REPL should
                    // not continue to execute this statement but can
                    // still accept new statements.
                    Parser.Error.IdentifierBad,
                    Parser.Error.OperationBad,
                    Parser.Error.ValueBad,
                    Parser.Error.KeyValuePairBad,
                    Parser.Error.KeyValuePairEqualMissing,
                    Parser.Error.SyntaxMatchNone,
                    Parser.Error.SliceOperationUnsupported,
                    // TODO(zig): This will be more convenient to express
                    // once https://github.com/ziglang/zig/issues/2473 is
                    // in.
                    => return,

                    // An unexpected error for which we do
                    // want the stacktrace.
                    error.AccessDenied,
                    error.BrokenPipe,
                    error.ConnectionResetByPeer,
                    error.DeviceBusy,
                    error.DiskQuota,
                    error.FileTooBig,
                    error.InputOutput,
                    error.InvalidArgument,
                    error.LockViolation,
                    error.NoSpaceLeft,
                    error.NotOpenForWriting,
                    error.OperationAborted,
                    error.OutOfMemory,
                    error.SystemResources,
                    error.Unexpected,
                    error.WouldBlock,
                    => return err,
                }
            };
            try repl.do_statement(statement);
        }

        fn display_help(repl: *Repl) !void {
            try repl.terminal.print("TigerBeetle CLI Client {}\n" ++
                \\  Hit enter after a semicolon to run a command.
                \\  Ctrl+D to exit.
                \\
                \\Examples:
                \\  create_accounts id=1 code=10 ledger=700 flags=linked|history,
                \\                  id=2 code=10 ledger=700;
                \\  create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
                \\  lookup_accounts id=1;
                \\  lookup_accounts id=1, id=2;
                \\  get_account_transfers account_id=1 flags=debits|credits;
                \\  get_account_balances account_id=1 flags=debits|credits;
                \\
                \\
            , .{constants.semver});
        }

        pub fn init(
            allocator: std.mem.Allocator,
            time: Time,
            options: struct {
                addresses: []const std.net.Address,
                cluster_id: u128,
                verbose: bool,
            },
        ) !Repl {
            var arguments = try std.ArrayListUnmanaged(u8).initCapacity(
                allocator,
                constants.message_size_max,
            );
            errdefer arguments.deinit(allocator);

            var message_pool = try allocator.create(MessagePool);
            errdefer allocator.destroy(message_pool);

            message_pool.* = try MessagePool.init(allocator, .client);
            errdefer message_pool.deinit(allocator);

            var io = try allocator.create(IO);
            errdefer allocator.destroy(io);

            io.* = try IO.init(32, 0);
            errdefer io.deinit();

            const client_id = stdx.unique_u128();
            const client = try Client.init(
                allocator,
                .{
                    .id = client_id,
                    .cluster = options.cluster_id,
                    .replica_count = @intCast(options.addresses.len),
                    .time = time,
                    .message_pool = message_pool,
                    .message_bus_options = .{
                        .configuration = options.addresses,
                        .io = io,
                    },
                },
            );
            errdefer client.deinit(allocator);

            return .{
                .client = client,
                .debug_logs = options.verbose,
                .request_done = true,
                .event_loop_done = false,
                .interactive = false,
                .terminal = undefined, // Init on run.
                .completion = undefined, // Init on run.
                .history = HistoryBuffer.init(), // No corresponding deinit.
                .buffer = .{},
                .buffer_outside_history = .{},
                .arguments = arguments,
                .message_pool = message_pool,
                .io = io,
            };
        }

        pub fn deinit(repl: *Repl, allocator: std.mem.Allocator) void {
            repl.client.deinit(allocator);
            repl.io.deinit();
            repl.message_pool.deinit(allocator);
            allocator.destroy(repl.message_pool);
            repl.arguments.deinit(allocator);
        }

        pub fn run(repl: *Repl, statements: []const u8) !void {
            repl.interactive = statements.len == 0;
            try Terminal.init(&repl.terminal, repl.interactive); // No corresponding deinit.

            try Completion.init(&repl.completion);

            repl.client.register(register_callback, @intCast(@intFromPtr(repl)));
            while (!repl.event_loop_done) {
                repl.client.tick();
                try repl.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
            }
            repl.event_loop_done = false;

            if (repl.interactive) {
                try repl.display_help();
            }

            var statements_iterator = if (statements.len > 0)
                std.mem.split(u8, statements, ";")
            else
                null;

            while (!repl.event_loop_done) {
                if (repl.request_done) {
                    repl.arguments.clearRetainingCapacity();
                    if (repl.interactive) {
                        try repl.do_repl(&repl.arguments);
                    } else blk: {
                        const statement_string = statements_iterator.?.next() orelse break :blk;

                        const statement = Parser.parse_statement(
                            statement_string,
                            &repl.terminal,
                            &repl.arguments,
                        ) catch |err| {
                            switch (err) {
                                // These are parsing errors and since this
                                // is not an interactive command, we should
                                // exit immediately. Parsing error info
                                // has already been emitted to stderr.
                                Parser.Error.IdentifierBad,
                                Parser.Error.OperationBad,
                                Parser.Error.ValueBad,
                                Parser.Error.KeyValuePairBad,
                                Parser.Error.KeyValuePairEqualMissing,
                                Parser.Error.SyntaxMatchNone,
                                Parser.Error.SliceOperationUnsupported,
                                // TODO: This will be more convenient to express
                                // once https://github.com/ziglang/zig/issues/2473 is
                                // in.
                                => std.posix.exit(1),

                                // An unexpected error for which we do
                                // want the stacktrace.
                                error.AccessDenied,
                                error.BrokenPipe,
                                error.ConnectionResetByPeer,
                                error.DeviceBusy,
                                error.DiskQuota,
                                error.FileTooBig,
                                error.InputOutput,
                                error.InvalidArgument,
                                error.LockViolation,
                                error.NoSpaceLeft,
                                error.NotOpenForWriting,
                                error.OperationAborted,
                                error.OutOfMemory,
                                error.SystemResources,
                                error.Unexpected,
                                error.WouldBlock,
                                => return err,
                            }
                        };

                        try repl.do_statement(statement);
                    }
                }

                repl.client.tick();
                try repl.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
            }
        }

        fn register_callback(
            user_data: u128,
            result: *const vsr.RegisterResult,
        ) void {
            _ = result;

            const repl: *Repl = @ptrFromInt(@as(usize, @intCast(user_data)));
            assert(!repl.event_loop_done);

            repl.event_loop_done = true;
        }

        fn send(
            repl: *Repl,
            operation: StateMachine.Operation,
            arguments: []const u8,
        ) !void {
            const operation_type = switch (operation) {
                .create_accounts, .create_transfers => "create",
                .get_account_transfers, .get_account_balances => "get",
                .lookup_accounts, .lookup_transfers => "lookup",
                .pulse => unreachable,
                .query_accounts, .query_transfers => "query",
            };
            const object_type = switch (operation) {
                .create_accounts, .lookup_accounts, .query_accounts => "accounts",
                .create_transfers, .lookup_transfers, .query_transfers => "transfers",
                .get_account_transfers => "account transfers",
                .get_account_balances => "account balances",
                .pulse => unreachable,
            };

            if (arguments.len == 0) {
                try repl.fail(
                    "No {s} to {s}.\n",
                    .{ object_type, operation_type },
                );
                return;
            }

            repl.request_done = false;

            try repl.debug("Sending command: {}.\n", .{operation});
            repl.client.request(
                client_request_callback,
                @intCast(@intFromPtr(repl)),
                operation,
                arguments,
                0,
            );
        }

        fn display_object(repl: *Repl, object: anytype) !void {
            assert(@TypeOf(object.*) == tb.Account or
                @TypeOf(object.*) == tb.Transfer or
                @TypeOf(object.*) == tb.AccountBalance);

            try repl.terminal.print("{{\n", .{});
            inline for (@typeInfo(@TypeOf(object.*)).Struct.fields, 0..) |object_field, i| {
                if (comptime std.mem.eql(u8, object_field.name, "reserved")) {
                    continue;
                    // No need to print out reserved.
                }

                if (i > 0) {
                    try repl.terminal.print(",\n", .{});
                }

                if (comptime std.mem.eql(u8, object_field.name, "flags")) {
                    try repl.terminal.print("  \"" ++ object_field.name ++ "\": [", .{});
                    var needs_comma = false;

                    inline for (@typeInfo(object_field.type).Struct.fields) |flag_field| {
                        if (comptime !std.mem.eql(u8, flag_field.name, "padding")) {
                            if (@field(@field(object, "flags"), flag_field.name)) {
                                if (needs_comma) {
                                    try repl.terminal.print(",", .{});
                                    needs_comma = false;
                                }

                                try repl.terminal.print("\"{s}\"", .{flag_field.name});
                                needs_comma = true;
                            }
                        }
                    }

                    try repl.terminal.print("]", .{});
                } else {
                    try repl.terminal.print(
                        "  \"{s}\": \"{}\"",
                        .{ object_field.name, @field(object, object_field.name) },
                    );
                }
            }
            try repl.terminal.print("\n}}\n", .{});
        }

        fn client_request_callback_error(
            user_data: u128,
            operation: StateMachine.Operation,
            timestamp: u64,
            result: []const u8,
        ) !void {
            const repl: *Repl = @ptrFromInt(@as(usize, @intCast(user_data)));
            assert(repl.request_done == false);
            try repl.debug("Operation completed: {} timestamp={}.\n", .{ operation, timestamp });

            defer {
                repl.request_done = true;

                if (!repl.interactive) {
                    repl.event_loop_done = true;
                }
            }

            switch (operation) {
                .create_accounts => {
                    const create_account_results = std.mem.bytesAsSlice(
                        tb.CreateAccountsResult,
                        result,
                    );

                    if (create_account_results.len > 0) {
                        for (create_account_results) |*reason| {
                            try repl.terminal.print(
                                "Failed to create account ({}): {any}.\n",
                                .{ reason.index, reason.result },
                            );
                        }
                    }
                },
                .lookup_accounts, .query_accounts => {
                    const account_results = std.mem.bytesAsSlice(
                        tb.Account,
                        result,
                    );

                    if (account_results.len == 0) {
                        try repl.fail("No accounts were found.\n", .{});
                    } else {
                        for (account_results) |*account| {
                            try repl.display_object(account);
                        }
                    }
                },
                .create_transfers => {
                    const create_transfer_results = std.mem.bytesAsSlice(
                        tb.CreateTransfersResult,
                        result,
                    );

                    if (create_transfer_results.len > 0) {
                        for (create_transfer_results) |*reason| {
                            try repl.terminal.print(
                                "Failed to create transfer ({}): {any}.\n",
                                .{ reason.index, reason.result },
                            );
                        }
                    }
                },
                .lookup_transfers, .get_account_transfers, .query_transfers => {
                    const transfer_results = std.mem.bytesAsSlice(
                        tb.Transfer,
                        result,
                    );

                    if (transfer_results.len == 0) {
                        try repl.fail("No transfers were found.\n", .{});
                    } else {
                        for (transfer_results) |*transfer| {
                            try repl.display_object(transfer);
                        }
                    }
                },
                .get_account_balances => {
                    const get_account_balances_results = std.mem.bytesAsSlice(
                        tb.AccountBalance,
                        result,
                    );

                    if (get_account_balances_results.len == 0) {
                        try repl.fail("No balances were found.\n", .{});
                    } else {
                        for (get_account_balances_results) |*balance| {
                            try repl.display_object(balance);
                        }
                    }
                },
                .pulse => unreachable,
            }
        }

        fn client_request_callback(
            user_data: u128,
            operation: StateMachine.Operation,
            timestamp: u64,
            result: []const u8,
            batch_count: u16,
        ) void {
            assert(batch_count == 0);
            client_request_callback_error(
                user_data,
                operation,
                timestamp,
                result,
            ) catch |err| {
                const repl: *Repl = @ptrFromInt(@as(usize, @intCast(user_data)));
                repl.fail("Error in callback: {any}", .{err}) catch return;
            };
        }
    };
}
