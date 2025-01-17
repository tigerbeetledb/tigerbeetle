const std = @import("std");
const assert = std.debug.assert;
const allocator = std.heap.c_allocator;

const c = @import("src/c.zig");
const translate = @import("src/translate.zig");
const tb = vsr.tigerbeetle;
const tb_client = vsr.tb_client;

const Account = tb.Account;
const Transfer = tb.Transfer;
const AccountFilter = tb.AccountFilter;
const AccountBalance = tb.AccountBalance;
const QueryFilter = tb.QueryFilter;

const vsr = @import("vsr");
const Storage = vsr.storage.StorageType(vsr.io.IO);
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const Operation = StateMachine.Operation;
const constants = vsr.constants;

pub const std_options = .{
    .log_level = .debug,
    .logFn = tb_client.Logging.application_logger,
};

// Cached value for JS (null).
var napi_null: c.napi_value = undefined;

/// N-API will call this constructor automatically to register the module.
export fn napi_register_module_v1(env: c.napi_env, exports: c.napi_value) c.napi_value {
    napi_null = translate.capture_null(env) catch return null;

    translate.register_function(env, exports, "init", init) catch return null;
    translate.register_function(env, exports, "deinit", deinit) catch return null;
    translate.register_function(env, exports, "submit", submit) catch return null;
    return exports;
}

// Add-on code

fn init(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    const args = translate.extract_args(env, info, .{
        .count = 1,
        .function = "init",
    }) catch return null;

    const cluster = translate.u128_from_object(env, args[0], "cluster_id") catch return null;
    const addresses = translate.slice_from_object(
        env,
        args[0],
        "replica_addresses",
    ) catch return null;

    return create(env, cluster, addresses) catch null;
}

fn deinit(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    const args = translate.extract_args(env, info, .{
        .count = 1,
        .function = "deinit",
    }) catch return null;

    destroy(env, args[0]) catch {};
    return null;
}

fn submit(env: c.napi_env, info: c.napi_callback_info) callconv(.C) c.napi_value {
    const args = translate.extract_args(env, info, .{
        .count = 4,
        .function = "submit",
    }) catch return null;

    const operation_int = translate.u32_from_value(env, args[1], "operation") catch return null;
    if (!@as(vsr.Operation, @enumFromInt(operation_int)).valid(StateMachine)) {
        translate.throw(env, "Unknown operation.") catch return null;
    }

    var is_array: bool = undefined;
    if (c.napi_is_array(env, args[2], &is_array) != c.napi_ok) {
        translate.throw(env, "Failed to check array argument type.") catch return null;
    }
    if (!is_array) {
        translate.throw(env, "Array argument must be an [object Array].") catch return null;
    }

    var callback_type: c.napi_valuetype = undefined;
    if (c.napi_typeof(env, args[3], &callback_type) != c.napi_ok) {
        translate.throw(env, "Failed to check callback argument type.") catch return null;
    }
    if (callback_type != c.napi_function) {
        translate.throw(env, "Callback argument must be a Function.") catch return null;
    }

    request(
        env,
        args[0], // tb_client
        @enumFromInt(@as(u8, @intCast(operation_int))),
        args[2], // request array
        args[3], // callback
    ) catch {};
    return null;
}

// tb_client Logic

fn create(
    env: c.napi_env,
    cluster_id: u128,
    addresses: []const u8,
) !c.napi_value {
    var tsfn_name: c.napi_value = undefined;
    if (c.napi_create_string_utf8(env, "tb_client", c.NAPI_AUTO_LENGTH, &tsfn_name) != c.napi_ok) {
        return translate.throw(
            env,
            "Failed to create resource name for thread-safe function.",
        );
    }

    var completion_tsfn: c.napi_threadsafe_function = undefined;
    if (c.napi_create_threadsafe_function(
        env,
        null, // No javascript function to call directly from here.
        null, // No async resource.
        tsfn_name,
        0, // Max queue size of 0 means no limit.
        1, // Number of acquires/threads that will be calling this TSFN.
        null, // No finalization data.
        null, // No finalization callback.
        null, // No custom context.
        on_completion_js, // Function to call on JS thread when TSFN is called.
        &completion_tsfn, // TSFN out handle.
    ) != c.napi_ok) {
        return translate.throw(env, "Failed to create thread-safe function.");
    }
    errdefer if (c.napi_release_threadsafe_function(
        completion_tsfn,
        c.napi_tsfn_abort,
    ) != c.napi_ok) {
        std.log.warn("Failed to release allocated thread-safe function on error.", .{});
    };

    if (c.napi_acquire_threadsafe_function(completion_tsfn) != c.napi_ok) {
        return translate.throw(env, "Failed to acquire reference to thread-safe function.");
    }

    const client = tb_client.init(
        allocator,
        cluster_id,
        addresses,
        @intFromPtr(completion_tsfn),
        on_completion,
    ) catch |err| switch (err) {
        error.OutOfMemory => return translate.throw(env, "Failed to allocate memory for Client."),
        error.Unexpected => return translate.throw(env, "Unexpected error occurred on Client."),
        error.AddressInvalid => return translate.throw(env, "Invalid replica address."),
        error.AddressLimitExceeded => return translate.throw(env, "Too many replica addresses."),
        error.SystemResources => return translate.throw(env, "Failed to reserve system resources."),
        error.NetworkSubsystemFailed => return translate.throw(env, "Network stack failure."),
    };
    errdefer tb_client.deinit(client);

    return try translate.create_external(env, client);
}

// Javascript is single threaded so no synchronization is necessary for closing/accessing a client.
fn destroy(env: c.napi_env, context: c.napi_value) !void {
    const client_ptr = try translate.value_external(
        env,
        context,
        "Failed to get client context pointer.",
    );
    const client: tb_client.tb_client_t = @ptrCast(@alignCast(client_ptr.?));
    defer tb_client.deinit(client);

    const completion_ctx = tb_client.completion_context(client);
    const completion_tsfn: c.napi_threadsafe_function = @ptrFromInt(completion_ctx);

    if (c.napi_release_threadsafe_function(completion_tsfn, c.napi_tsfn_abort) != c.napi_ok) {
        return translate.throw(env, "Failed to release allocated thread-safe function on error.");
    }
}

fn request(
    env: c.napi_env,
    context: c.napi_value,
    operation: Operation,
    array: c.napi_value,
    callback: c.napi_value,
) !void {
    const client_ptr = try translate.value_external(
        env,
        context,
        "Failed to get client context pointer.",
    );
    const client: tb_client.tb_client_t = @ptrCast(@alignCast(client_ptr.?));

    // Create a reference to the callback so it stay alive until the packet completes.
    var callback_ref: c.napi_ref = undefined;
    if (c.napi_create_reference(env, callback, 1, &callback_ref) != c.napi_ok) {
        return translate.throw(env, "Failed to create reference to callback.");
    }
    errdefer translate.delete_reference(env, callback_ref) catch {
        std.log.warn("Failed to delete reference to callback on error.", .{});
    };

    const array_length: u32 = try translate.array_length(env, array);
    const packet, const packet_data = switch (operation) {
        inline else => |op| blk: {
            const buffer = try BufferType(op).alloc(
                env,
                array_length,
            );
            errdefer buffer.free();

            const events = buffer.events();
            try decode_array(StateMachine.EventType(op), env, array, events);

            const packet = buffer.packet();
            break :blk .{ packet, std.mem.sliceAsBytes(events) };
        },
        .pulse => unreachable,
    };

    packet.* = .{
        .user_data = callback_ref,
        .operation = @intFromEnum(operation),
        .data = packet_data.ptr,
        .data_size = @intCast(packet_data.len),
        .tag = 0,
        .status = undefined,
    };

    tb_client.submit(client, packet);
}

fn on_completion(
    completion_ctx: usize,
    client: tb_client.tb_client_t,
    packet: *tb_client.tb_packet_t,
    timestamp: u64,
    result_ptr: ?[*]const u8,
    result_len: u32,
) callconv(.C) void {
    _ = client;
    _ = timestamp;

    switch (packet.status) {
        .ok => {
            switch (@as(Operation, @enumFromInt(packet.operation))) {
                inline else => |op| {
                    const event_count = @divExact(
                        packet.data_size,
                        @sizeOf(StateMachine.EventType(op)),
                    );
                    const buffer: BufferType(op) = .{
                        .ptr = @ptrCast(packet),
                        .count = event_count,
                    };

                    const Result = StateMachine.ResultType(op);
                    const results: []const Result = @alignCast(std.mem.bytesAsSlice(
                        Result,
                        result_ptr.?[0..result_len],
                    ));
                    @memcpy(buffer.results()[0..results.len], results);

                    // Store the size of the results in the `tag` field, so we can access it back
                    // during `on_completion_js`.
                    packet.tag = @intCast(results.len);
                },
                .pulse => unreachable,
            }
        },
        .client_evicted,
        .client_release_too_low,
        .client_release_too_high,
        .client_shutdown,
        => {}, // Handled on the JS side to throw exception.
        .too_much_data => unreachable, // We limit packet data size during request().
        .invalid_operation => unreachable, // We check the operation during request().
        .invalid_data_size => unreachable, // We set correct data size during request().
    }

    // Queue the packet to be processed on the JS thread to invoke its JS callback.
    const completion_tsfn: c.napi_threadsafe_function = @ptrFromInt(completion_ctx);
    switch (c.napi_call_threadsafe_function(completion_tsfn, packet, c.napi_tsfn_nonblocking)) {
        c.napi_ok => {},
        c.napi_queue_full => @panic(
            "ThreadSafe Function queue is full when created with no limit.",
        ),
        else => unreachable,
    }
}

fn on_completion_js(
    env: c.napi_env,
    unused_js_cb: c.napi_value,
    unused_context: ?*anyopaque,
    packet_argument: ?*anyopaque,
) callconv(.C) void {
    _ = unused_js_cb;
    _ = unused_context;

    // Extract the remaining packet information from the packet before it's freed.
    const packet: *tb_client.tb_packet_t = @ptrCast(@alignCast(packet_argument.?));
    const callback_ref: c.napi_ref = @ptrCast(@alignCast(packet.user_data.?));

    // Decode the packet's Buffer results into an array then free the packet/Buffer.
    const array_or_error = switch (@as(Operation, @enumFromInt(packet.operation))) {
        inline else => |op| blk: {
            const event_count = @divExact(
                packet.data_size,
                @sizeOf(StateMachine.EventType(op)),
            );
            const buffer: BufferType(op) = .{
                .ptr = @ptrCast(packet),
                .count = event_count,
            };
            defer buffer.free();

            switch (packet.status) {
                .ok => {
                    const result_count = packet.tag;
                    const results = buffer.results()[0..result_count];
                    break :blk encode_array(StateMachine.ResultType(op), env, results);
                },
                .client_shutdown => {
                    break :blk translate.throw(env, "Client was shutdown.");
                },
                .client_evicted => {
                    break :blk translate.throw(env, "Client was evicted.");
                },
                .client_release_too_low => {
                    break :blk translate.throw(env, "Client was evicted: release too old.");
                },
                .client_release_too_high => {
                    break :blk translate.throw(env, "Client was evicted: release too new.");
                },
                else => unreachable, // all other packet status' handled in previous callback.
            }
        },
        .pulse => unreachable,
    };

    // Parse Result array out of packet data, freeing it in the process.
    // NOTE: Ensure this is called before anything that could early-return to avoid a alloc leak.
    var callback_error = napi_null;
    const callback_result = array_or_error catch |err| switch (err) {
        error.ExceptionThrown => blk: {
            if (c.napi_get_and_clear_last_exception(env, &callback_error) != c.napi_ok) {
                std.log.warn("Failed to capture callback error from thrown Exception.", .{});
            }
            break :blk napi_null;
        },
    };

    // Make sure to delete the callback reference once we're done calling it.
    defer if (c.napi_delete_reference(env, callback_ref) != c.napi_ok) {
        std.log.warn("Failed to delete reference to user's JS callback.", .{});
    };

    const callback = translate.reference_value(
        env,
        callback_ref,
        "Failed to get callback from reference.",
    ) catch return;

    var args = [_]c.napi_value{ callback_error, callback_result };
    _ = translate.call_function(env, napi_null, callback, &args) catch return;
}

// (De)Serialization

fn decode_array(comptime Event: type, env: c.napi_env, array: c.napi_value, events: []Event) !void {
    for (events, 0..) |*event, i| {
        const object = try translate.array_element(env, array, @intCast(i));
        switch (Event) {
            Account,
            Transfer,
            AccountFilter,
            AccountBalance,
            QueryFilter,
            => {
                inline for (std.meta.fields(Event)) |field| {
                    const value: field.type = switch (@typeInfo(field.type)) {
                        .Struct => |info| @bitCast(try @field(
                            translate,
                            @typeName(info.backing_integer.?) ++ "_from_object",
                        )(
                            env,
                            object,
                            add_trailing_null(field.name),
                        )),
                        .Int => try @field(translate, @typeName(field.type) ++ "_from_object")(
                            env,
                            object,
                            add_trailing_null(field.name),
                        ),
                        // Arrays are only used for padding/reserved fields,
                        // instead of requiring the user to explicitly set an empty buffer,
                        // we just hide those fields and preserve their default value.
                        .Array => @as(
                            *const field.type,
                            @ptrCast(@alignCast(field.default_value.?)),
                        ).*,
                        else => unreachable,
                    };

                    @field(event, field.name) = value;
                }
            },
            u128 => event.* = try translate.u128_from_value(env, object, "lookup"),
            else => @compileError("invalid Event type"),
        }
    }
}

fn encode_array(comptime Result: type, env: c.napi_env, results: []const Result) !c.napi_value {
    const array = try translate.create_array(
        env,
        @intCast(results.len),
        "Failed to allocate array for results.",
    );

    for (results, 0..) |*result, i| {
        const object = try translate.create_object(
            env,
            "Failed to create " ++ @typeName(Result) ++ " object.",
        );

        inline for (std.meta.fields(Result)) |field| {
            const FieldInt = switch (@typeInfo(field.type)) {
                .Struct => |info| info.backing_integer.?,
                .Enum => |info| info.tag_type,
                // Arrays are only used for padding/reserved fields.
                .Array => continue,
                else => field.type,
            };

            const value: FieldInt = switch (@typeInfo(field.type)) {
                .Struct => @bitCast(@field(result, field.name)),
                .Enum => @intFromEnum(@field(result, field.name)),
                else => @field(result, field.name),
            };

            try @field(translate, @typeName(FieldInt) ++ "_into_object")(
                env,
                object,
                add_trailing_null(field.name),
                value,
                "Failed to set property \"" ++ field.name ++
                    "\" of " ++ @typeName(Result) ++ " object",
            );

            try translate.set_array_element(
                env,
                array,
                @intCast(i),
                object,
                "Failed to set element in results array.",
            );
        }
    }

    return array;
}

fn add_trailing_null(comptime input: []const u8) [:0]const u8 {
    // Concatenating `[]const u8` with an empty string `[0:0]const u8`,
    // gives us a null-terminated string `[:0]const u8`.
    const output = input ++ "";
    comptime assert(output.len == input.len);
    comptime assert(output[output.len] == 0);
    return output;
}

/// Each packet allocates enough room to hold both its Events and its Results.
/// Buffer is an abstraction over the memory management for this.
fn BufferType(comptime op: Operation) type {
    assert(op != .pulse);

    return struct {
        const Buffer = @This();
        const Event = StateMachine.EventType(op);
        const Result = StateMachine.ResultType(op);

        const body_align = @max(@alignOf(Event), @alignOf(Result));
        const body_offset = std.mem.alignForward(usize, @sizeOf(tb_client.tb_packet_t), body_align);

        ptr: [*]u8,
        count: u32,

        fn alloc(env: c.napi_env, count: u32) !Buffer {
            // Allocate enough bytes to hold memory for the Events and the Results.
            const body_size = @max(
                @sizeOf(Event) * count,
                @sizeOf(Result) * event_count(op, count),
            );
            if (@sizeOf(vsr.Header) + body_size > constants.message_size_max) {
                return translate.throw(env, "Batch is larger than the maximum message size.");
            }

            const max_align = @max(body_align, @alignOf(tb_client.tb_packet_t));
            const max_bytes = body_offset + body_size;

            const bytes = allocator.alignedAlloc(u8, max_align, max_bytes) catch |e| switch (e) {
                error.OutOfMemory => return translate.throw(
                    env,
                    "Batch allocation ran out of memory.",
                ),
            };
            errdefer allocator.free(bytes);

            return Buffer{
                .ptr = bytes.ptr,
                .count = count,
            };
        }

        fn free(buffer: Buffer) void {
            const body_size = @max(
                @sizeOf(Event) * buffer.count,
                @sizeOf(Result) * event_count(op, buffer.count),
            );

            const max_align = @max(body_align, @alignOf(tb_client.tb_packet_t));
            const max_bytes = body_offset + body_size;

            const bytes: []align(max_align) u8 = @alignCast(buffer.ptr[0..max_bytes]);
            allocator.free(bytes);
        }

        fn packet(buffer: Buffer) *tb_client.tb_packet_t {
            return @alignCast(@ptrCast(buffer.ptr));
        }

        fn events(buffer: Buffer) []Event {
            const event_bytes = buffer.ptr[body_offset..][0 .. @sizeOf(Event) * buffer.count];
            return @alignCast(std.mem.bytesAsSlice(Event, event_bytes));
        }

        fn results(buffer: Buffer) []Result {
            const result_size = @sizeOf(Result) * event_count(op, buffer.count);
            const result_bytes = buffer.ptr[body_offset..][0..result_size];
            return @alignCast(std.mem.bytesAsSlice(Result, result_bytes));
        }

        fn event_count(operation: Operation, count: usize) usize {
            // TODO(batiati): Refine the way we handle events with asymmetric results.
            return switch (operation) {
                .get_account_transfers,
                .get_account_balances,
                .query_accounts,
                .query_transfers,
                => 8190,
                else => count,
            };
        }
    };
}
