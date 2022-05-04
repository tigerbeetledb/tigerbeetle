const std = @import("std");
const builtin = @import("builtin");

const MessageBus = @import("../message_bus.zig").MessageBusClient;
const StateMachine = @import("../state_machine.zig").StateMachine;
const ClientThread = @import("client_thread.zig").ClientThread(StateMachine, MessageBus);

pub const tb_packet_t = @import("client_thread.zig").Packet;
pub const tb_packet_list_t = tb_packet_t.List;
pub const tb_packet_status_t = tb_packet_t.Status;

pub const tb_client_t = *anyopaque;
pub const tb_status_t = enum(c_int) {
    success = 0,
    unexpected,
    out_of_memory,
    invalid_address,
    system_resources,
    network_subsystem,
};

pub const tb_completion_t = fn (
    context: usize,
    client: tb_client_t,
    packet: *tb_packet_t,
    result_ptr: ?[*]const u8,
    result_len: u32,
) callconv(.C) void;

pub export fn tb_client_init(
    out_client: *tb_client_t,
    out_packets: *tb_packet_list_t,
    cluster_id: u32,
    addresses_ptr: [*c]const u8,
    addresses_len: u32,
    num_packets: u32,
    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,
) tb_status_t {
    const context = Context.allocator.create(Context) catch return .out_of_memory;
    context.on_completion_ctx = on_completion_ctx;
    context.on_completion_fn = on_completion_fn;

    const addresses = @ptrCast([*]const u8, addresses_ptr)[0..addresses_len];
    context.client_thread.init(
        Context.allocator,
        cluster_id, 
        addresses, 
        num_packets,
        Context.on_completion,
    ) catch |err| {
        Context.allocator.destroy(context);
        return switch (err) {
            error.Unexpected => .unexpected,
            error.OutOfMemory => .out_of_memory,
            error.InvalidAddress => .invalid_address,
            error.SystemResources => .system_resources,
            error.NetworkSubsystemFailed => .network_subsystem,
        };
    };

    var list = tb_packet_list_t{};
    for (context.client_thread.packets) |*packet| {
        list.push(tb_packet_list_t.from(packet));
    }
    
    out_client.* = @ptrCast(tb_client_t, context);
    out_packets.* = list;
    return .success;
}

pub export fn tb_client_submit(
    client: tb_client_t,
    packets: *tb_packet_list_t,
) void {
    const context = @ptrCast(*Context, @alignCast(@alignOf(Context), client));
    context.client_thread.submit(packets.*);
}

pub export fn tb_client_deinit(
    client: tb_client_t,
) void {
    const context = @ptrCast(*Context, @alignCast(@alignOf(Context), client));
    context.client_thread.deinit();
    Context.allocator.destroy(context);
}

/////////////////////////////////////////////////

const Context = struct {
    client_thread: ClientThread,
    on_completion_ctx: usize,
    on_completion_fn: tb_completion_t,

    // Pick the most suitable allocator
    const allocator = if (builtin.link_libc)
        std.heap.c_allocator
    else if (builtin.target.os.tag == .windows)
        (struct { var gpa = std.heap.HeapAllocator.init(); }).gpa.allocator()
    else
        @compileError("tb_client must be built with libc");

    // Wrapper for zig ClientThread to invoke C tb_completion_t
    fn on_completion(client_thread: *ClientThread, packet: *tb_packet_t, result: ?[]const u8)  void {
        const context = @fieldParentPtr(Context, "client_thread", client_thread);
        const tb_client = @ptrCast(tb_client_t, context);

        context.on_completion_fn(
            context.on_completion_ctx,
            tb_client,
            packet,
            if (result) |r| r.ptr else null,
            if (result) |r| @intCast(u32, r.len) else 0,
        );
    }
};

