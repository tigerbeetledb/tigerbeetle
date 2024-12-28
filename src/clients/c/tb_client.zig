const std = @import("std");

// When referenced from unit_test.zig, there is no vsr import module. So use relative path instead.
pub const vsr = if (@import("root") == @This()) @import("vsr") else @import("../../vsr.zig");

pub const tb_packet_t = @import("tb_client/packet.zig").Packet.Extern;
pub const tb_packet_status_t = @import("tb_client/packet.zig").Packet.Status;

pub const tb_client_t = *anyopaque;
pub const tb_status_t = enum(c_int) {
    success = 0,
    unexpected,
    out_of_memory,
    address_invalid,
    address_limit_exceeded,
    system_resources,
    network_subsystem,
};
pub const tb_register_log_callback_status_t = enum(c_int) {
    success = 0,
    already_registered,
    not_registered,
};

pub const tb_operation_t = StateMachine.Operation;
pub const tb_completion_t = *const fn (
    context: usize,
    client: tb_client_t,
    packet: *tb_packet_t,
    timestamp: u64,
    result_ptr: ?[*]const u8,
    result_len: u32,
) callconv(.C) void;

const constants = @import("../../constants.zig");
const IO = @import("../../io.zig").IO;
const Storage = @import("../../storage.zig").StorageType(IO);
const MessageBus = @import("../../message_bus.zig").MessageBusClient;
const StateMachineType = @import("../../state_machine.zig").StateMachineType;
const StateMachine = StateMachineType(Storage, constants.state_machine_config);

const ContextType = @import("tb_client/context.zig").ContextType;
const ContextImplementation = @import("tb_client/context.zig").ContextImplementation;
pub const InitError = @import("tb_client/context.zig").Error;

const DefaultContext = blk: {
    const ClientType = @import("../../vsr/client.zig").ClientType;
    const Client = ClientType(StateMachine, MessageBus, vsr.time.Time);
    break :blk ContextType(Client);
};

const TestingContext = blk: {
    const EchoClientType = @import("tb_client/echo_client.zig").EchoClientType;
    const EchoClient = EchoClientType(StateMachine, MessageBus, vsr.time.Time);
    break :blk ContextType(EchoClient);
};

/// Logging is global per application; it would be nice to be able to define a different logger for
/// each client instance, though.
var logging: Logging = .{};

pub const Logging = struct {
    const Callback = *const fn (
        message_level: u8,
        message_ptr: [*]const u8,
        message_len: usize,
    ) callconv(.C) void;

    const log_line_max = 8192;

    /// A logger which defers to an application provided handler.
    pub fn application_logger(
        comptime message_level: std.log.Level,
        comptime scope: @Type(.EnumLiteral),
        comptime format: []const u8,
        args: anytype,
    ) void {
        // Debug logs are dropped here unless debug is set, because of the potential penalty in
        // crossing FFI to drop them.
        if (message_level == .debug and !logging.debug) {
            return;
        }

        // Other messages are silently dropped if no logging callback is specified - unless they're
        // warn or err. The value in having those for debugging is too high to silence them, even
        // until client libraries catch up and implement a callback handler.
        if (logging.callback == null and (message_level == .warn or message_level == .err)) {
            std.log.defaultLog(message_level, scope, format, args);
            return;
        }

        const callback = logging.callback orelse return;

        // Protect everything with a mutex - logging can be called from different threads
        // simultaneously, and there's only one buffer for now.
        logging.mutex.lock();
        defer logging.mutex.unlock();

        const prefix = if (scope == .default) ": " else "(" ++ @tagName(scope) ++ "): ";
        const output = std.fmt.bufPrint(
            &logging.buffer,
            prefix ++ format,
            args,
        ) catch |err| switch (err) {
            error.NoSpaceLeft => blk: {
                // Print an error indicating the log message has been truncated, before the
                // truncated log itself.
                const message = "the following log message has been truncated:";
                callback(@intFromEnum(std.log.Level.err), message.ptr, message.len);

                break :blk &logging.buffer;
            },
            else => unreachable,
        };

        callback(@intFromEnum(message_level), output.ptr, output.len);
    }

    callback: ?Callback = null,
    mutex: std.Thread.Mutex = .{},
    buffer: [log_line_max]u8 = undefined,
    debug: bool = false,
};

pub fn register_log_callback(
    callback_maybe: ?Logging.Callback,
    debug: bool,
) callconv(.C) tb_register_log_callback_status_t {
    if (logging.callback == null) {
        if (callback_maybe) |callback| {
            logging.callback = callback;
            logging.debug = debug;
            return .success;
        } else {
            return .not_registered;
        }
    } else {
        if (callback_maybe == null) {
            logging.callback = null;
            logging.debug = debug;
            return .success;
        } else {
            return .already_registered;
        }
    }
}

pub fn context_to_client(implementation: *ContextImplementation) tb_client_t {
    return @ptrCast(implementation);
}

fn client_to_context(tb_client: tb_client_t) *ContextImplementation {
    return @ptrCast(@alignCast(tb_client));
}

pub fn init_error_to_status(err: InitError) tb_status_t {
    return switch (err) {
        error.Unexpected => .unexpected,
        error.OutOfMemory => .out_of_memory,
        error.AddressInvalid => .address_invalid,
        error.AddressLimitExceeded => .address_limit_exceeded,
        error.SystemResources => .system_resources,
        error.NetworkSubsystemFailed => .network_subsystem,
    };
}

pub fn init(
    allocator: std.mem.Allocator,
    cluster_id: u128,
    addresses: []const u8,
    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,
) InitError!tb_client_t {
    const context = try DefaultContext.init(
        allocator,
        cluster_id,
        addresses,
        on_completion_ctx,
        on_completion_fn,
    );

    return context_to_client(&context.implementation);
}

pub fn init_echo(
    allocator: std.mem.Allocator,
    cluster_id: u128,
    addresses: []const u8,
    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,
) InitError!tb_client_t {
    const context = try TestingContext.init(
        allocator,
        cluster_id,
        addresses,
        on_completion_ctx,
        on_completion_fn,
    );

    return context_to_client(&context.implementation);
}

pub fn completion_context(client: tb_client_t) callconv(.C) usize {
    const context = client_to_context(client);
    return context.completion_ctx;
}

pub fn submit(
    client: tb_client_t,
    packet: *tb_packet_t,
) callconv(.C) void {
    const context = client_to_context(client);
    (context.submit_fn)(context, packet);
}

pub fn deinit(
    client: tb_client_t,
) callconv(.C) void {
    const context = client_to_context(client);
    (context.deinit_fn)(context);
}

test {
    std.testing.refAllDecls(DefaultContext);
}
