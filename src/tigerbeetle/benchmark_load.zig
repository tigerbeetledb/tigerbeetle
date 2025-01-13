//! This script is a Zig TigerBeetle client that connects to a TigerBeetle
//! cluster and runs a workload on it (described below) while measuring
//! (and at the end, printing) observed latencies and throughput.
//!
//! It uses a single client to 1) create `account_count` accounts then 2)
//! generate `transfer_count` transfers between random different
//! accounts. It does not validate that the transfers succeed.
const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const panic = std.debug.panic;
const log = std.log;

const vsr = @import("vsr");
const constants = vsr.constants;
const stdx = vsr.stdx;
const flags = vsr.flags;
const random_int_exponential = vsr.testing.random_int_exponential;
const IO = vsr.io.IO;
const Storage = vsr.storage.StorageType(IO);
const MessagePool = vsr.message_pool.MessagePool;
const MessageBus = vsr.message_bus.MessageBusClient;
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const Client = vsr.ClientType(StateMachine, MessageBus);
const tb = vsr.tigerbeetle;
const IdPermutation = vsr.testing.IdPermutation;
const ZipfianGenerator = stdx.ZipfianGenerator;
const ZipfianShuffled = stdx.ZipfianShuffled;

const cli = @import("./cli.zig");

pub fn main(
    allocator: std.mem.Allocator,
    addresses: []const std.net.Address,
    cli_args: *const cli.Command.Benchmark,
) !void {
    const stderr = std.io.getStdErr().writer();

    if (builtin.mode != .ReleaseSafe and builtin.mode != .ReleaseFast) {
        try stderr.print("Benchmark must be built with '-Drelease' for reasonable results.\n", .{});
    }
    if (!vsr.constants.config.process.direct_io) {
        log.warn("direct io is disabled", .{});
    }
    if (vsr.constants.config.process.verify) {
        log.warn("extra assertions are enabled", .{});
    }

    if (cli_args.account_count < 2) vsr.fatal(
        .cli,
        "--account-count: need at least two accounts, got {}",
        .{cli_args.account_count},
    );

    // The first account_count_hot accounts are "hot" -- they will be the debit side of
    // transfer_hot_percent of the transfers.
    if (cli_args.account_count_hot > cli_args.account_count) vsr.fatal(
        .cli,
        "--account-count-hot: must be less-than-or-equal-to --account-count, got {}",
        .{cli_args.account_count_hot},
    );

    if (cli_args.transfer_hot_percent > 100) vsr.fatal(
        .cli,
        "--transfer-hot-percent: must be less-than-or-equal-to 100, got {}",
        .{cli_args.transfer_hot_percent},
    );

    const client_id = stdx.unique_u128();
    const cluster_id: u128 = 0;

    var io = try IO.init(32, 0);

    var message_pool = try MessagePool.init(allocator, .client);

    std.log.info("Benchmark running against {any}", .{addresses});

    var client = try Client.init(
        allocator,
        .{
            .id = client_id,
            .cluster = cluster_id,
            .replica_count = @intCast(addresses.len),
            .message_pool = &message_pool,
            .message_bus_options = .{
                .configuration = addresses,
                .io = &io,
            },
        },
    );

    var batch_accounts =
        try std.ArrayListUnmanaged(tb.Account).initCapacity(allocator, cli_args.account_batch_size);
    defer batch_accounts.deinit(allocator);

    // Each array position corresponds to a histogram bucket of 1ms. The last bucket is 10_000ms+.
    const batch_latency_histogram = try allocator.alloc(u64, 10_001);
    @memset(batch_latency_histogram, 0);
    defer allocator.free(batch_latency_histogram);

    const query_latency_histogram = try allocator.alloc(u64, 10_001);
    @memset(query_latency_histogram, 0);
    defer allocator.free(query_latency_histogram);

    var batch_transfers =
        try std.ArrayListUnmanaged(tb.Transfer).initCapacity(
        allocator,
        cli_args.transfer_batch_size,
    );
    defer batch_transfers.deinit(allocator);

    var batch_account_ids =
        try std.ArrayListUnmanaged(u128).initCapacity(allocator, cli_args.account_batch_size);
    defer batch_account_ids.deinit(allocator);

    var batch_transfer_ids =
        try std.ArrayListUnmanaged(u128).initCapacity(allocator, cli_args.transfer_batch_size);
    defer batch_transfer_ids.deinit(allocator);

    // var statsd_opt: ?StatsD = null;
    // defer if (statsd_opt) |*statsd| statsd.deinit(allocator);

    // if (cli_args.statsd) {
    //     statsd_opt = try StatsD.init(
    //         allocator,
    //         &io,
    //         std.net.Address.parseIp4("127.0.0.1", 8125) catch unreachable,
    //     );
    // }

    // If no seed was given, use a default seed for reproducibility.
    const seed = seed_from_arg: {
        const seed_argument = cli_args.seed orelse break :seed_from_arg 42;
        break :seed_from_arg vsr.testing.parse_seed(seed_argument);
    };

    log.info("Benchmark seed = {}", .{seed});

    var rng = std.rand.DefaultPrng.init(seed);
    const random = rng.random();
    const account_id_permutation: IdPermutation = switch (cli_args.id_order) {
        .sequential => .{ .identity = {} },
        .random => .{ .random = random.int(u64) },
        .reversed => .{ .inversion = {} },
    };

    assert(cli_args.account_count >= cli_args.account_count_hot);
    const account_generator = Generator.from_distribution(
        cli_args.account_distribution,
        cli_args.account_count - cli_args.account_count_hot,
        random,
    );
    const account_generator_hot = Generator.from_distribution(
        cli_args.account_distribution,
        cli_args.account_count_hot,
        random,
    );

    log.info("Account distribution: {s}", .{
        @tagName(cli_args.account_distribution),
    });

    var benchmark = Benchmark{
        .io = &io,
        .message_pool = &message_pool,
        .client = &client,
        .batch_accounts = batch_accounts,
        .account_count = cli_args.account_count,
        .account_count_hot = cli_args.account_count_hot,
        .account_generator = account_generator,
        .account_generator_hot = account_generator_hot,
        .flag_history = cli_args.flag_history,
        .flag_imported = cli_args.flag_imported,
        .account_index = 0,
        .query_count = cli_args.query_count,
        .query_index = 0,
        .account_id_permutation = account_id_permutation,
        .rng = rng,
        .timer = try std.time.Timer.start(),
        .batch_latency_histogram = batch_latency_histogram,
        .query_latency_histogram = query_latency_histogram,
        .batch_transfers = batch_transfers,
        .batch_start_ns = 0,
        .transfer_count = cli_args.transfer_count,
        .transfer_hot_percent = cli_args.transfer_hot_percent,
        .transfer_pending = cli_args.transfer_pending,
        .transfer_batch_size = cli_args.transfer_batch_size,
        .transfer_batch_delay_us = cli_args.transfer_batch_delay_us,
        .validate = cli_args.validate,
        .batch_index = 0,
        .transfers_sent = 0,
        .transfer_index = 0,
        .transfer_next_arrival_ns = 0,
        .callback = null,
        .done = false,
        .statsd = null, //if (statsd_opt) |*statsd| statsd else null,
        .print_batch_timings = cli_args.print_batch_timings,
        .id_order = cli_args.id_order,
        .batch_account_ids = batch_account_ids,
        .batch_transfer_ids = batch_transfer_ids,
    };

    benchmark.client.register(Benchmark.register_callback, @intCast(@intFromPtr(&benchmark)));
    while (!benchmark.done) {
        benchmark.client.tick();
        try benchmark.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
    benchmark.done = false;

    benchmark.create_accounts();

    while (!benchmark.done) {
        benchmark.client.tick();
        try benchmark.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
    benchmark.done = false;

    if (cli_args.checksum_performance) {
        const stdout = std.io.getStdOut().writer();
        stdout.print("\nmessage size max = {} bytes\n", .{
            constants.message_size_max,
        }) catch unreachable;

        const buffer = try allocator.alloc(u8, constants.message_size_max);
        defer allocator.free(buffer);
        benchmark.rng.fill(buffer);

        benchmark.timer.reset();
        _ = vsr.checksum(buffer);
        const checksum_duration_ns = benchmark.timer.read();

        stdout.print("checksum message size max = {} us\n", .{
            @divTrunc(checksum_duration_ns, std.time.ns_per_us),
        }) catch unreachable;
    }

    if (!benchmark.validate) return;

    // Reset our state so we can check our work.
    benchmark.rng = rng;
    benchmark.account_index = 0;
    benchmark.transfer_index = 0;
    benchmark.validate_accounts();

    while (!benchmark.done) {
        benchmark.client.tick();
        try benchmark.io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
}

const Generator = union(enum) {
    zipfian: ZipfianShuffled,
    latest: ZipfianGenerator,
    uniform: u64,

    fn from_distribution(
        distribution: cli.Command.Benchmark.Distribution,
        count: u64,
        random: std.Random,
    ) Generator {
        return switch (distribution) {
            .zipfian => .{
                .zipfian = ZipfianShuffled.init(count, random),
            },
            .latest => .{
                .latest = ZipfianGenerator.init(count),
            },
            .uniform => .{ .uniform = count },
        };
    }
};

const Benchmark = struct {
    io: *IO,
    message_pool: *MessagePool,
    client: *Client,
    batch_accounts: std.ArrayListUnmanaged(tb.Account),
    account_count: usize,
    account_count_hot: usize,
    account_generator: Generator,
    account_generator_hot: Generator,
    flag_history: bool,
    flag_imported: bool,
    account_index: usize,
    query_count: usize,
    query_index: usize,
    account_id_permutation: IdPermutation,
    rng: std.rand.DefaultPrng,
    timer: std.time.Timer,
    batch_latency_histogram: []u64,
    query_latency_histogram: []u64,
    batch_transfers: std.ArrayListUnmanaged(tb.Transfer),
    batch_start_ns: usize,
    transfers_sent: usize,
    transfer_count: usize,
    transfer_hot_percent: usize,
    transfer_pending: bool,
    transfer_batch_size: usize,
    transfer_batch_delay_us: usize,
    validate: bool,
    batch_index: usize,
    transfer_index: usize,
    transfer_next_arrival_ns: usize,
    callback: ?*const fn (*Benchmark, StateMachine.Operation, u64, []const u8) void,
    done: bool,
    statsd: ?*anyopaque,
    print_batch_timings: bool,
    id_order: cli.Command.Benchmark.IdOrder,
    batch_account_ids: std.ArrayListUnmanaged(u128),
    batch_transfer_ids: std.ArrayListUnmanaged(u128),

    fn create_account(b: *Benchmark) tb.Account {
        const random = b.rng.random();

        defer b.account_index += 1;
        return .{
            .id = b.account_id_permutation.encode(b.account_index + 1),
            .user_data_128 = random.int(u128),
            .user_data_64 = random.int(u64),
            .user_data_32 = random.int(u32),
            .reserved = 0,
            .ledger = 2,
            .code = 1,
            .flags = .{
                .history = b.flag_history,
                .imported = b.flag_imported,
            },
            .debits_pending = 0,
            .debits_posted = 0,
            .credits_pending = 0,
            .credits_posted = 0,
            .timestamp = if (b.flag_imported) b.account_index + 1 else 0,
        };
    }

    fn create_accounts(b: *Benchmark) void {
        if (b.account_index >= b.account_count) {
            b.create_transfers();
            return;
        }

        // Reset batch.
        b.batch_accounts.clearRetainingCapacity();

        // Fill batch.
        while (b.account_index < b.account_count and
            b.batch_accounts.items.len < b.batch_accounts.capacity)
        {
            b.batch_accounts.appendAssumeCapacity(b.create_account());
        }

        // Submit batch.
        b.send(
            create_accounts_finish,
            .create_accounts,
            std.mem.sliceAsBytes(b.batch_accounts.items),
        );
    }

    fn create_accounts_finish(
        b: *Benchmark,
        operation: StateMachine.Operation,
        timestamp: u64,
        result: []const u8,
    ) void {
        assert(operation == .create_accounts);
        assert(timestamp > 0);
        const create_accounts_results = std.mem.bytesAsSlice(
            tb.CreateAccountsResult,
            result,
        );
        if (create_accounts_results.len > 0) {
            panic("CreateAccountsResults: {any}", .{create_accounts_results});
        }

        b.create_accounts();
    }

    fn gen_account_index(b: *Benchmark, hint: enum { hot, cold }) u64 {
        assert(b.account_count > 0);
        stdx.maybe(b.account_count_hot == 0);
        assert(b.account_count >= b.account_count_hot);

        // The hint may be ignored if:
        // Always use hot accounts if `account_count == account_count_hot`.
        // Always use cold accounts if `account_count_hot == 0`.
        const source: @TypeOf(hint) = switch (hint) {
            .hot => if (b.account_count_hot > 0) .hot else .cold,
            .cold => if (b.account_count > b.account_count_hot) .cold else .hot,
        };

        // Select the generator and the count from each source.
        const generator: *Generator, const account_count: u64 = switch (source) {
            .hot => .{ &b.account_generator_hot, b.account_count_hot },
            .cold => .{ &b.account_generator, b.account_count - b.account_count_hot },
        };
        assert(account_count > 0);

        const random = b.rng.random();
        const index = switch (generator.*) {
            .zipfian => |gen| index: {
                // zipfian set size must be same as account set size
                assert(account_count == gen.gen.n);
                const index = gen.next(random);
                assert(index < account_count);
                break :index index;
            },
            .latest => |gen| index: {
                assert(account_count == gen.n);
                const index_rev = gen.next(random);
                assert(index_rev < account_count);
                break :index account_count - index_rev - 1;
            },
            .uniform => |count| index: {
                const index = random.uintLessThan(u64, count);
                assert(index < account_count);
                break :index index;
            },
        };

        return switch (source) {
            .hot => index,
            .cold => index + b.account_count_hot,
        };
    }

    fn create_transfer(b: *Benchmark) tb.Transfer {
        const random = b.rng.random();

        // The set of accounts is divided into two different "worlds" by
        // `account_count_hot`. Sometimes the debit account will be selected
        // from the first `account_count_hot` accounts; otherwise both
        // debit and credit will be selected from an account >= `account_count_hot`.
        const debit_account_index = b.gen_account_index(
            if (random.intRangeAtMost(usize, 1, 100) <= b.transfer_hot_percent)
                .hot
            else
                .cold,
        );
        const credit_account_index = index: {
            var index = b.gen_account_index(.cold);
            if (index == debit_account_index) {
                index = (index + 1) % b.account_count;
            }
            break :index index;
        };
        assert(debit_account_index < b.account_count);
        assert(credit_account_index < b.account_count);
        assert(debit_account_index != credit_account_index);

        const debit_account_id = b.account_id_permutation.encode(debit_account_index + 1);
        const credit_account_id = b.account_id_permutation.encode(credit_account_index + 1);
        assert(debit_account_id != credit_account_id);

        // 30% of pending transfers.
        const pending = b.transfer_pending and random.intRangeAtMost(u8, 0, 9) < 3;

        defer b.transfer_index += 1;
        return .{
            .id = b.account_id_permutation.encode(b.transfer_index + 1),
            .debit_account_id = debit_account_id,
            .credit_account_id = credit_account_id,
            .user_data_128 = random.int(u128),
            .user_data_64 = random.int(u64),
            .user_data_32 = random.int(u32),
            // TODO Benchmark posting/voiding pending transfers.
            .pending_id = 0,
            .ledger = 2,
            .code = random.int(u16) +| 1,
            .flags = .{
                .pending = pending,
                .imported = b.flag_imported,
            },
            .timeout = if (pending) random.intRangeAtMost(u32, 1, 60) else 0,
            .amount = random_int_exponential(random, u64, 10_000) +| 1,
            .timestamp = if (b.flag_imported) b.account_index + b.transfer_index + 1 else 0,
        };
    }

    fn create_transfers(b: *Benchmark) void {
        if (b.transfer_index >= b.transfer_count) {
            b.summary_transfers();
            return;
        }

        if (b.transfer_index == 0) {
            // Init timer.
            b.timer.reset();
            b.transfer_next_arrival_ns = b.timer.read();
        }

        b.batch_transfers.clearRetainingCapacity();

        b.batch_start_ns = b.timer.read();

        // Fill batch.
        while (b.transfer_index < b.transfer_count and
            b.batch_transfers.items.len < b.batch_transfers.capacity)
        {
            b.batch_transfers.appendAssumeCapacity(b.create_transfer());
        }

        assert(b.batch_transfers.items.len > 0);

        // Submit batch.
        b.send(
            create_transfers_finish,
            .create_transfers,
            std.mem.sliceAsBytes(b.batch_transfers.items),
        );
    }

    fn create_transfers_finish(
        b: *Benchmark,
        operation: StateMachine.Operation,
        timestamp: u64,
        result: []const u8,
    ) void {
        assert(operation == .create_transfers);
        assert(timestamp > 0);
        const create_transfers_results = std.mem.bytesAsSlice(
            tb.CreateTransfersResult,
            result,
        );
        if (create_transfers_results.len > 0) {
            panic("CreateTransfersResults: {any}", .{create_transfers_results});
        }

        // Record latencies.
        const batch_end_ns = b.timer.read();
        const ms_time = @divTrunc(batch_end_ns - b.batch_start_ns, std.time.ns_per_ms);

        if (b.print_batch_timings) {
            log.info("batch {}: {} tx in {} ms", .{
                b.batch_index,
                b.batch_transfers.items.len,
                ms_time,
            });
        }

        b.batch_latency_histogram[@min(ms_time, b.batch_latency_histogram.len - 1)] += 1;

        b.batch_index += 1;
        b.transfers_sent += b.batch_transfers.items.len;

        // if (b.statsd) |statsd| {
        //     statsd.gauge("benchmark.txns", b.batch_transfers.items.len) catch {};
        //     statsd.timing("benchmark.timings", ms_time) catch {};
        //     statsd.gauge("benchmark.batch", b.batch_index) catch {};
        //     statsd.gauge("benchmark.completed", b.transfers_sent) catch {};
        // }

        std.time.sleep(b.transfer_batch_delay_us * std.time.ns_per_us);
        b.create_transfers();
    }

    fn summary_transfers(b: *Benchmark) void {
        const total_ns = b.timer.read();
        const stdout = std.io.getStdOut().writer();

        stdout.print("{} batches in {d:.2} s\n", .{
            b.batch_index,
            @as(f64, @floatFromInt(total_ns)) / std.time.ns_per_s,
        }) catch unreachable;
        stdout.print("transfer batch size = {} txs\n", .{
            b.transfer_batch_size,
        }) catch unreachable;
        stdout.print("transfer batch delay = {} us\n", .{
            b.transfer_batch_delay_us,
        }) catch unreachable;
        stdout.print("load accepted = {} tx/s\n", .{
            @divTrunc(
                b.transfer_count * std.time.ns_per_s,
                total_ns,
            ),
        }) catch unreachable;
        print_percentiles_histogram(stdout, "batch", b.batch_latency_histogram);

        if (b.query_count > 0) {
            b.timer.reset();
            b.account_index = 0;
            b.query_account_transfers();
        } else {
            b.done = true;
        }
    }

    fn query_account_transfers(b: *Benchmark) void {
        if (b.query_index >= b.query_count) {
            b.summary_query();
            return;
        }

        // Use hot accounts for queries to equalize the number of results
        // returned on each execution.
        b.account_index = b.gen_account_index(.hot);
        var filter = tb.AccountFilter{
            .account_id = b.account_id_permutation.encode(b.account_index + 1),
            .user_data_128 = 0,
            .user_data_64 = 0,
            .user_data_32 = 0,
            .code = 0,
            .timestamp_min = 0,
            .timestamp_max = 0,
            .limit = @divExact(
                constants.message_size_max - @sizeOf(vsr.Header),
                @sizeOf(tb.Transfer),
            ),
            .flags = .{
                .credits = true,
                .debits = true,
                .reversed = false,
            },
        };

        b.batch_start_ns = b.timer.read();
        b.send(
            query_account_transfers_finish,
            .get_account_transfers,
            std.mem.asBytes(&filter),
        );
    }

    fn query_account_transfers_finish(
        b: *Benchmark,
        operation: StateMachine.Operation,
        timestamp: u64,
        result: []const u8,
    ) void {
        assert(operation == .get_account_transfers);
        assert(timestamp > 0);

        const batch_end_ns = b.timer.read();
        const transfers = std.mem.bytesAsSlice(tb.Transfer, result);
        const account_id = b.account_id_permutation.encode(b.account_index + 1);
        for (transfers) |*transfer| {
            assert(transfer.debit_account_id == account_id or
                transfer.credit_account_id == account_id);
        }

        const ms_time = @divTrunc(batch_end_ns - b.batch_start_ns, std.time.ns_per_ms);
        b.query_latency_histogram[@min(ms_time, b.query_latency_histogram.len - 1)] += 1;

        b.query_index += 1;
        b.query_account_transfers();
    }

    fn summary_query(b: *Benchmark) void {
        const total_ns = b.timer.read();

        const stdout = std.io.getStdOut().writer();

        stdout.print("\n{} queries in {d:.2} s\n", .{
            b.query_count,
            @as(f64, @floatFromInt(total_ns)) / std.time.ns_per_s,
        }) catch unreachable;
        print_percentiles_histogram(stdout, "query", b.query_latency_histogram);

        b.done = true;
    }

    fn validate_accounts(b: *Benchmark) void {
        if (b.account_index >= b.account_count) {
            b.validate_transfers();
            return;
        }

        // Reset batch.
        b.batch_accounts.clearRetainingCapacity();
        b.batch_account_ids.clearRetainingCapacity();

        // Fill batch.
        while (b.account_index < b.account_count and
            b.batch_accounts.items.len < b.batch_accounts.capacity)
        {
            const account = b.create_account();
            b.batch_accounts.appendAssumeCapacity(account);
            b.batch_account_ids.appendAssumeCapacity(account.id);
        }

        b.send(
            validate_accounts_finish,
            .lookup_accounts,
            std.mem.sliceAsBytes(b.batch_account_ids.items),
        );
    }

    fn validate_accounts_finish(
        b: *Benchmark,
        operation: StateMachine.Operation,
        timestamp: u64,
        result: []const u8,
    ) void {
        assert(operation == .lookup_accounts);
        assert(timestamp > 0);

        const accounts = std.mem.bytesAsSlice(tb.Account, result);

        for (b.batch_accounts.items, accounts) |expected, actual| {
            assert(expected.id == actual.id);
            assert(expected.user_data_128 == actual.user_data_128);
            assert(expected.user_data_64 == actual.user_data_64);
            assert(expected.user_data_32 == actual.user_data_32);
            assert(expected.code == actual.code);
            assert(@as(u16, @bitCast(expected.flags)) == @as(u16, @bitCast(actual.flags)));
        }

        b.validate_accounts();
    }

    fn validate_transfers(b: *Benchmark) void {
        if (b.transfer_index >= b.transfer_count) {
            b.summary_validate();
            return;
        }

        b.batch_transfers.clearRetainingCapacity();
        b.batch_transfer_ids.clearRetainingCapacity();

        b.batch_start_ns = b.timer.read();

        // Fill batch.
        while (b.transfer_index < b.transfer_count and
            b.batch_transfers.items.len < b.batch_transfers.capacity)
        {
            const transfer = b.create_transfer();
            b.batch_transfers.appendAssumeCapacity(transfer);
            b.batch_transfer_ids.appendAssumeCapacity(transfer.id);
        }

        assert(b.batch_transfer_ids.items.len > 0);

        // Submit batch.
        b.send(
            validate_transfers_finish,
            .lookup_transfers,
            std.mem.sliceAsBytes(b.batch_transfer_ids.items),
        );
    }

    fn validate_transfers_finish(
        b: *Benchmark,
        operation: StateMachine.Operation,
        timestamp: u64,
        result: []const u8,
    ) void {
        assert(operation == .lookup_transfers);
        assert(timestamp > 0);

        const transfers = std.mem.bytesAsSlice(tb.Transfer, result);

        for (b.batch_transfers.items, transfers) |expected, actual| {
            assert(expected.id == actual.id);
            assert(expected.debit_account_id == actual.debit_account_id);
            assert(expected.credit_account_id == actual.credit_account_id);
            assert(expected.amount == actual.amount);
            assert(expected.pending_id == actual.pending_id);
            assert(expected.user_data_128 == actual.user_data_128);
            assert(expected.user_data_64 == actual.user_data_64);
            assert(expected.user_data_32 == actual.user_data_32);
            assert(expected.timeout == actual.timeout);
            assert(expected.ledger == actual.ledger);
            assert(expected.code == actual.code);
            assert(@as(u16, @bitCast(expected.flags)) == @as(u16, @bitCast(actual.flags)));
        }

        b.validate_transfers();
    }

    fn summary_validate(b: *Benchmark) void {
        const stdout = std.io.getStdOut().writer();

        stdout.print("validated {d} accounts, {d} transfers\n", .{
            b.account_count,
            b.transfer_count,
        }) catch unreachable;

        b.done = true;
    }

    fn send(
        b: *Benchmark,
        callback: *const fn (*Benchmark, StateMachine.Operation, u64, []const u8) void,
        operation: StateMachine.Operation,
        payload: []u8,
    ) void {
        b.callback = callback;
        b.client.request(
            send_complete,
            @intCast(@intFromPtr(b)),
            operation,
            payload,
        );
    }

    fn send_complete(
        user_data: u128,
        operation: StateMachine.Operation,
        timestamp: u64,
        result: []u8,
    ) void {
        const b: *Benchmark = @ptrFromInt(@as(usize, @intCast(user_data)));
        const callback = b.callback.?;
        b.callback = null;

        callback(b, operation, timestamp, result);
    }

    fn register_callback(
        user_data: u128,
        result: *const vsr.RegisterResult,
    ) void {
        _ = result;

        const b: *Benchmark = @ptrFromInt(@as(usize, @intCast(user_data)));
        assert(!b.done);
        b.done = true;
    }
};

fn print_percentiles_histogram(
    stdout: anytype,
    label: []const u8,
    histogram_buckets: []const u64,
) void {
    var histogram_total: u64 = 0;
    for (histogram_buckets) |bucket| histogram_total += bucket;

    const percentiles = [_]u64{ 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99, 100 };
    for (percentiles) |percentile| {
        const histogram_percentile: usize = @divTrunc(histogram_total * percentile, 100);

        // Since each bucket in our histogram represents 1ms, the bucket we're in is the ms value.
        var sum: usize = 0;
        const latency = for (histogram_buckets, 0..) |bucket, bucket_index| {
            sum += bucket;
            if (sum >= histogram_percentile) break bucket_index;
        } else histogram_buckets.len;

        stdout.print("{s} latency p{} = {} ms{s}\n", .{
            label,
            percentile,
            latency,
            if (latency == histogram_buckets.len) "+ (exceeds histogram resolution)" else "",
        }) catch unreachable;
    }
}
