//! This workload runs in an loop, generating and executing operations on a cluster through a
//! _driver_.
//!
//! Any successful operations are reconciled with a model, tracking what accounts exist. Future
//! operations are generated based on this model.
//!
//! After every operation, all accounts are queried, and basic invariants are checked.
//!
//! The workload and drivers communicate with a binary protocol over stdio. The protocol is based
//! on the extern structs in `src/tigerbeetle.zig` and `src/state_machine.zig`, and it works like
//! this:
//!
//! 1. Workload sends a request, which is:
//!    * the _operation_ (1 byte),
//!    * the _event count_ (4 bytes), and
//!    * the events (event count * size of event).
//! 2. The driver uses its client to submit those events. When receiving results, it sends them
//!    back on its stdout as:
//!    * the _operation_ (1 byte)
//!    * the _result count_ (4 bytes), and
//!    * the results (result count * size of result pair), where each pair holds an index and a
//!      result enum value (see `src/tigerbeetle.zig`)
//! 3. The workload receives the results, and expects them to be of the same operation type as
//!    originally requested. There might be fewer results than events, because clients can omit
//!    .ok results.
//!
//! Additionally, the workload itself sends `Progress` events on its stdout back to the supervisor.
//! This is used for tracing and liveness checks.

const std = @import("std");
const stdx = @import("../../stdx.zig");
const arbitrary = @import("./arbitrary.zig");
const tb = @import("../../tigerbeetle.zig");
const constants = @import("../../constants.zig");
const StateMachineType = @import("../../state_machine.zig").StateMachineType;
const RingBufferType = stdx.RingBufferType;

const log = std.log.scoped(.workload);
const assert = std.debug.assert;
const testing = std.testing;
const StateMachine = StateMachineType(void, constants.state_machine_config);

const events_count_max = 8190;
const pending_transfers_count_max = 1024;
const accounts_count_max = 128;

const DriverStdio = struct { input: std.fs.File, output: std.fs.File };

pub fn main(
    allocator: std.mem.Allocator,
    driver: *const DriverStdio,
) !void {
    var accounts_buffer = std.mem.zeroes([accounts_count_max]tb.Account);

    var model = Model{
        .accounts = std.ArrayListUnmanaged(tb.Account).initBuffer(&accounts_buffer),
    };
    defer model.pending_transfers.deinit(allocator);

    try model.pending_transfers.ensureTotalCapacity(allocator, pending_transfers_count_max);

    const seed = std.crypto.random.int(u64);
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    const stdout = std.io.getStdOut().writer().any();

    for (0..std.math.maxInt(u64)) |i| {
        const command_timestamp_start: u64 = @intCast(std.time.microTimestamp());
        const command = random_command(random, &model);
        const result = try execute(command, driver) orelse break;
        try reconcile(result, &command, &model);
        const command_timestamp_end: u64 = @intCast(std.time.microTimestamp());
        try progress_write(stdout, .{
            .event_count = command.event_count(),
            .timestamp_start_micros = command_timestamp_start,
            .timestamp_end_micros = command_timestamp_end,
        });

        const query_timestamp_start: u64 = @intCast(std.time.microTimestamp());
        const query = lookup_all_accounts(&model);
        const query_result = try execute(query, driver) orelse break;
        try reconcile(query_result, &query, &model);
        const query_timestamp_end: u64 = @intCast(std.time.microTimestamp());
        try progress_write(stdout, .{
            .event_count = query.event_count(),
            .timestamp_start_micros = query_timestamp_start,
            .timestamp_end_micros = query_timestamp_end,
        });

        log.info(
            "accounts created = {d}, transfers = {d}, pending transfers = {d}, commands run = {d}",
            .{
                model.accounts.items.len,
                model.transfers_created,
                model.pending_transfers.count(),
                i + 1,
            },
        );
    }
}

const Command = union(enum) {
    create_accounts: []tb.Account,
    create_transfers: []tb.Transfer,
    lookup_all_accounts: []u128,
    lookup_latest_transfers: []u128,

    fn event_count(command: Command) usize {
        return switch (command) {
            .create_accounts => |entries| entries.len,
            .create_transfers => |entries| entries.len,
            .lookup_all_accounts => |entries| entries.len,
            .lookup_latest_transfers => |entries| entries.len,
        };
    }
};

const CommandBuffers = FixedSizeBuffersType(Command);
var command_buffers: CommandBuffers = std.mem.zeroes(CommandBuffers);

const Result = union(enum) {
    create_accounts: []tb.CreateAccountsResult,
    create_transfers: []tb.CreateTransfersResult,
    lookup_all_accounts: []tb.Account,
    lookup_latest_transfers: []tb.Transfer,
};
const ResultBuffers = FixedSizeBuffersType(Result);
var result_buffers: ResultBuffers = std.mem.zeroes(ResultBuffers);

fn execute(command: Command, driver: *const DriverStdio) !?Result {
    switch (command) {
        inline else => |events, tag| {
            const operation = comptime operation_from_command(tag);
            try send(driver, operation, events);

            const buffer = @field(result_buffers, @tagName(tag))[0..events.len];
            const results = receive(driver, operation, buffer) catch |err| {
                switch (err) {
                    error.EndOfStream => return null,
                    else => return err,
                }
            };
            return @unionInit(Result, @tagName(tag), results);
        },
    }
}

/// State machine operations and Vortex workload commands are not 1:1. This function maps the
/// enum values from command to operation.
fn operation_from_command(tag: std.meta.Tag(Command)) StateMachine.Operation {
    return switch (tag) {
        .lookup_all_accounts => .lookup_accounts,
        .lookup_latest_transfers => .lookup_transfers,
        else => std.enums.nameCast(StateMachine.Operation, tag),
    };
}

fn reconcile(result: Result, command: *const Command, model: *Model) !void {
    switch (result) {
        .create_accounts => |entries| {
            const accounts_new = command.create_accounts;

            // Track results for all new accounts, assuming `.ok` if response from driver is
            // omitted.
            var accounts_results: [accounts_count_max]tb.CreateAccountResult = undefined;
            @memset(accounts_results[0..accounts_new.len], .ok);

            // Fill in non-ok results.
            for (entries) |entry| {
                accounts_results[entry.index] = entry.result;
            }

            for (
                accounts_results[0..accounts_new.len],
                accounts_new,
                0..,
            ) |account_result, account, index| {
                if (account_result == .ok) {
                    model.accounts.appendAssumeCapacity(account);
                } else {
                    log.err("got result {s} for event {d}: {any}", .{
                        @tagName(account_result),
                        index,
                        account,
                    });
                    return error.TestFailed;
                }
            }
        },
        .create_transfers => |entries| {
            const transfers = command.create_transfers;

            // Track results for all new transfers, assuming `.ok` if response from driver is
            // omitted.
            var transfers_results: [events_count_max]tb.CreateTransferResult = undefined;
            @memset(transfers_results[0..transfers.len], .ok);

            // Fill in non-ok results.
            for (entries) |entry| {
                transfers_results[entry.index] = entry.result;
            }

            // Collect all successful transfer IDs.
            var successful_transfer_ids: stdx.BoundedArrayType(u128, events_count_max) = .{};

            for (
                transfers,
                transfers_results[0..transfers.len],
                0..,
            ) |transfer, transfer_result, transfer_index| {
                // Check that linked transfers fail together.
                if (transfer_index > 0) {
                    const preceeding_transfer = transfers[transfer_index - 1];
                    if (preceeding_transfer.flags.linked) {
                        const preceeding_entry = entries[transfer_index - 1];
                        try testing.expect(preceeding_entry.index == transfer_index - 1);
                        try testing.expect(preceeding_entry.result != .ok);
                    }
                }

                // No further validation needed for failed transfers.
                if (transfer_result != .ok) {
                    continue;
                }

                successful_transfer_ids.append_assume_capacity(transfer.id);

                if (transfer.flags.pending) {
                    try testing.expect(!model.pending_transfers.contains(transfer.id));
                    assert(model.pending_transfers.count() <= pending_transfers_count_max);
                    model.pending_transfers.putAssumeCapacity(transfer.id, {});
                }

                if (transfer.flags.void_pending_transfer or transfer.flags.post_pending_transfer) {
                    try testing.expect(model.pending_transfers.remove(transfer.id));
                }

                try testing.expect(model.account_exists(transfer.debit_account_id));
                try testing.expect(model.account_exists(transfer.credit_account_id));
            }

            // Drop the oldest transfer IDs and add new ones.
            for (0..@min(successful_transfer_ids.count(), model.latest_transfers.count)) |_| {
                model.latest_transfers.retreat_tail();
            }
            try model.latest_transfers.push_slice(successful_transfer_ids.const_slice());

            model.transfers_created += transfers.len;
        },
        .lookup_all_accounts => |accounts_found| {
            // Get all known account ids.
            var id_buffer: [accounts_count_max]u128 = undefined;
            const account_ids_known = model.account_ids(id_buffer[0..]);

            // Check that timestamps are monotonically increasing.
            var timestamp_max: u64 = 0;
            for (accounts_found) |account| {
                if (account.timestamp <= timestamp_max) {
                    log.err(
                        "account {d} timestamp {d} is not greater than previous timestamp {d}",
                        .{ account.id, account.timestamp, timestamp_max },
                    );
                    return error.TestFailed;
                }
                timestamp_max = account.timestamp;
            }

            // Extract and sort all found account ids.
            var account_ids_found_buffer: [accounts_count_max]u128 = undefined;
            for (accounts_found, 0..) |account, i| {
                account_ids_found_buffer[i] = account.id;
            }
            const account_ids_found = account_ids_found_buffer[0..accounts_found.len];

            // All known accounts are found by the query, and no others.
            try testing.expectEqualSlices(u128, account_ids_known, account_ids_found);

            try testing.expectEqual(0, debits_credits_difference(accounts_found));
        },
        .lookup_latest_transfers => |transfers_found| {
            // Check that timestamps are monotonically increasing.
            var timestamp_max: u64 = 0;
            for (transfers_found) |transfer| {
                if (transfer.timestamp <= timestamp_max) {
                    log.err(
                        "transfer {d} timestamp {d} is not greater than previous timestamp {d}",
                        .{ transfer.id, transfer.timestamp, timestamp_max },
                    );
                    return error.TestFailed;
                }
                timestamp_max = transfer.timestamp;
            }
        },
    }
}

const LatestTransfers = RingBufferType(u128, .{ .array = events_count_max });

/// Tracks information about the accounts and transfers created by the workload.
const Model = struct {
    accounts: std.ArrayListUnmanaged(tb.Account),
    transfers_created: u64 = 0,
    latest_transfers: LatestTransfers = LatestTransfers.init(),
    pending_transfers: std.AutoHashMapUnmanaged(u128, void) = .{},

    // O(n) lookup, but it's limited by `accounts_count_max`, so it's OK for this test.
    fn account_exists(model: @This(), id: u128) bool {
        for (model.accounts.items) |account| {
            if (account.id == id) return true;
        }
        return false;
    }

    /// Returns a slice of the account ids known by the model.
    fn account_ids(model: @This(), buffer: []u128) []u128 {
        assert(buffer.len >= model.accounts.items.len);
        const ids = buffer[0..model.accounts.items.len];
        for (model.accounts.items, 0..) |account, i| {
            ids[i] = account.id;
        }
        return ids;
    }
};

fn debits_credits_difference(accounts: []tb.Account) i128 {
    var balance: i128 = 0;
    for (accounts) |account| {
        balance += @intCast(account.debits_posted);
        balance -= @intCast(account.credits_posted);
    }
    return balance;
}

fn random_command(random: std.Random, model: *const Model) Command {
    const command_tag = arbitrary.weighted(random, std.meta.Tag(Command), .{
        .create_accounts = if (model.accounts.items.len < accounts_count_max) 1 else 0,
        .create_transfers = if (model.accounts.items.len > 2) 10 else 0,
        .lookup_all_accounts = 0,
        .lookup_latest_transfers = 5,
    }).?;
    switch (command_tag) {
        .create_accounts => return random_create_accounts(random, model),
        .create_transfers => return random_create_transfers(random, model),
        .lookup_latest_transfers => return lookup_latest_transfers(model),
        .lookup_all_accounts => unreachable,
    }
}

fn random_create_accounts(random: std.Random, model: *const Model) Command {
    // NOTE: we're not generating closed or imported accounts yet.

    const events_count = random.intRangeAtMost(
        usize,
        1,
        accounts_count_max - model.accounts.items.len,
    );
    assert(events_count <= events_count_max);

    var events = command_buffers.create_accounts[0..events_count];
    for (events, 0..) |*event, i| {
        var flags = std.mem.zeroes(tb.AccountFlags);

        flags.history = arbitrary.odds(random, 1, 10);

        if (arbitrary.odds(random, 1, 10)) {
            flags.debits_must_not_exceed_credits = random.boolean();
            flags.credits_must_not_exceed_debits = !flags.debits_must_not_exceed_credits;
        }

        // The last account in a batch can't be linked.
        flags.linked = i < events_count - 1 and arbitrary.odds(random, 1, 100);

        event.* = std.mem.zeroInit(tb.Account, .{
            .id = random.intRangeAtMost(u128, 1, std.math.maxInt(u128)),
            .ledger = 1,
            .code = random.intRangeAtMost(u16, 1, 100),
            .flags = flags,
        });
    }

    return .{ .create_accounts = events[0..events_count] };
}

fn random_create_transfers(random: std.Random, model: *const Model) Command {
    const events_count = random.intRangeAtMost(usize, 1, events_count_max);
    assert(events_count <= events_count_max);

    var buffer = command_buffers.create_transfers[0..events_count];
    for (buffer, 0..) |*event, i| {
        var flags = std.mem.zeroes(tb.TransferFlags);
        var pending_id: u128 = 0;
        var code = random.intRangeAtMost(u16, 1, 100);

        var debit_account_id = arbitrary.element(random, tb.Account, model.accounts.items).id;
        var credit_account_id: u128 = 0;
        while (credit_account_id == 0 or credit_account_id == debit_account_id) {
            credit_account_id = arbitrary.element(random, tb.Account, model.accounts.items).id;
        }

        if (arbitrary.odds(random, 1, 10) and model.pending_transfers.count() > 0) {
            flags.post_pending_transfer = random.boolean();
            flags.void_pending_transfer = !flags.post_pending_transfer;
        } else if (arbitrary.odds(random, 1, 100_000)) {
            flags.closing_debit = random.boolean();
            flags.closing_credit = !flags.closing_debit;
        } else {
            flags = arbitrary.flags(random, tb.TransferFlags, .{
                .pending,
                .balancing_debit,
                .balancing_credit,
            });
        }

        // The last transfer in a batch can't be linked.
        flags.linked = i < events_count - 1 and arbitrary.odds(random, 1, 100);

        if (flags.post_pending_transfer or flags.void_pending_transfer) {
            pending_id = arbitrary.set_element(random, u128, model.pending_transfers);
            code = 0;
            debit_account_id = 0;
            credit_account_id = 0;
        }

        event.* = std.mem.zeroInit(tb.Transfer, .{
            // Use monotonically increasing IDs from 1 for easier debugging.
            .id = model.transfers_created + i + 1,
            .ledger = 1,
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .amount = random.uintAtMost(u128, 1 << 32),
            .code = code,
            .pending_id = pending_id,
        });
    }

    return .{ .create_transfers = buffer[0..events_count] };
}

fn lookup_all_accounts(model: *const Model) Command {
    const events_count = model.accounts.items.len;
    var buffer = command_buffers.lookup_all_accounts[0..events_count];
    for (buffer, model.accounts.items) |*event, account| {
        event.* = account.id;
    }
    return .{ .lookup_all_accounts = buffer[0..events_count] };
}

fn lookup_latest_transfers(model: *const Model) Command {
    const buffer = command_buffers.lookup_latest_transfers[0..model.latest_transfers.count];
    var ids = model.latest_transfers.iterator();
    var count: usize = 0;
    while (ids.next()) |id| {
        buffer[count] = id;
        count += 1;
    }
    return .{ .lookup_latest_transfers = buffer };
}

/// Converts a union type, where each field is of a slice type, into a struct of arrays of the
/// corresponding type, with the maximum count of driver events as its len. These buffers are used
/// to hold commands and results in the workload loop.
fn FixedSizeBuffersType(Union: type) type {
    const union_fields = @typeInfo(Union).Union.fields;
    var struct_fields: [union_fields.len]std.builtin.Type.StructField = undefined;

    var i = 0;
    for (union_fields) |union_field| {
        const info = @typeInfo(union_field.type);
        const field_type = [events_count_max]info.Pointer.child;
        struct_fields[i] = .{
            .name = union_field.name,
            .type = field_type,
            .default_value = null,
            .is_comptime = false,
            .alignment = @alignOf(field_type),
        };
        i += 1;
    }

    return @Type(.{ .Struct = .{
        .is_tuple = false,
        .fields = &struct_fields,
        .layout = .auto,
        .decls = &.{},
    } });
}

pub fn send(
    driver: *const DriverStdio,
    comptime op: StateMachine.Operation,
    events: []const StateMachine.EventType(op),
) !void {
    assert(events.len <= events_count_max);

    const writer = driver.input.writer().any();

    try writer.writeInt(u8, @intFromEnum(op), .little);
    try writer.writeInt(u32, @intCast(events.len), .little);

    const bytes: []const u8 = std.mem.sliceAsBytes(events);
    try writer.writeAll(bytes);
}

pub fn receive(
    driver: *const DriverStdio,
    comptime op: StateMachine.Operation,
    results: []StateMachine.ResultType(op),
) ![]StateMachine.ResultType(op) {
    assert(results.len <= events_count_max);
    const reader = driver.output.reader();

    const results_count = try reader.readInt(u32, .little);
    assert(results_count <= results.len);

    const buf: []u8 = std.mem.sliceAsBytes(results[0..results_count]);
    assert(try reader.readAtLeast(buf, buf.len) == buf.len);

    return results[0..results_count];
}

/// A message written to stdout by the workload, communicating the progress it makes.
pub const Progress = extern struct {
    event_count: u64,
    timestamp_start_micros: u64,
    timestamp_end_micros: u64,
};

fn progress_write(writer: std.io.AnyWriter, stats: Progress) !void {
    try writer.writeAll(std.mem.asBytes(&stats));
}
