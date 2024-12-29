//! The Auditor constructs the expected state of its corresponding StateMachine from requests and
//! replies. It validates replies against its local state.
//!
//! The Auditor expects replies in ascending commit order.
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.test_auditor);

const constants = @import("../constants.zig");
const tb = @import("../tigerbeetle.zig");
const IdPermutation = @import("../testing/id.zig").IdPermutation;

const PriorityQueue = std.PriorityQueue;
const Storage = @import("../testing/storage.zig").Storage;
const StateMachine =
    @import("../state_machine.zig").StateMachineType(Storage, constants.state_machine_config);

pub const CreateAccountResultSet = std.enums.EnumSet(tb.CreateAccountResult);
// TODO(zig): See `Ordered` comments.
pub const CreateTransferResultSet = std.enums.EnumSet(tb.CreateTransferResult.Ordered);

/// Batch sizes apply to both `create` and `lookup` operations.
/// (More ids would fit in the `lookup` request, but then the response wouldn't fit.)
const accounts_batch_size_max = StateMachine.constants.batch_max.create_accounts;
const transfers_batch_size_max = StateMachine.constants.batch_max.create_transfers;

/// Store expected possible results for an in-flight request.
/// This reply validation takes advantage of the Workload's additional context about the request.
const InFlight = union(enum) {
    create_accounts: [accounts_batch_size_max]CreateAccountResultSet,
    create_transfers: [transfers_batch_size_max]CreateTransferResultSet,
};

const InFlightQueue = std.AutoHashMapUnmanaged(struct {
    client_index: usize,
    /// This index corresponds to Auditor.creates_sent/Auditor.creates_delivered.
    client_request: usize,
}, InFlight);

const PendingTransfer = struct {
    amount: u128,
    debit_account_index: usize,
    credit_account_index: usize,
    query_intersection_index: usize,
};

const PendingExpiry = struct {
    transfer_id: u128,
    transfer_timestamp: u64,
    expires_at: u64,
};

const PendingExpiryQueue = PriorityQueue(PendingExpiry, void, struct {
    /// Order by ascending expiration date and then by transfer's timestamp.
    fn compare(_: void, a: PendingExpiry, b: PendingExpiry) std.math.Order {
        const order = switch (std.math.order(a.expires_at, b.expires_at)) {
            .eq => std.math.order(a.transfer_timestamp, b.transfer_timestamp),
            else => |order| order,
        };
        assert(order != .eq);
        return order;
    }
}.compare);

pub const AccountingAuditor = struct {
    pub const AccountState = struct {
        /// Set to true when `create_accounts` returns `.ok` for an account.
        created: bool = false,
        /// The number of transfers created on the debit side.
        dr_transfer_count: u32 = 0,
        /// The number of transfers created on the credit side.
        cr_transfer_count: u32 = 0,
        /// Timestamp of the first transfer recorded.
        transfer_timestamp_min: u64 = 0,
        /// Timestamp of the last transfer recorded.
        transfer_timestamp_max: u64 = 0,

        fn update(
            state: *AccountState,
            comptime entry: enum { dr, cr },
            transfer_timestamp: u64,
        ) void {
            assert(state.created);
            switch (entry) {
                .dr => state.dr_transfer_count += 1,
                .cr => state.cr_transfer_count += 1,
            }

            if (state.transfer_timestamp_min == 0) {
                assert(state.transfer_timestamp_max == 0);
                state.transfer_timestamp_min = transfer_timestamp;
            }
            state.transfer_timestamp_max = transfer_timestamp;
        }

        pub fn transfers_count(self: *const AccountState, flags: tb.AccountFilterFlags) u32 {
            var transfer_count: u32 = 0;
            if (flags.debits) {
                transfer_count += self.dr_transfer_count;
            }
            if (flags.credits) {
                transfer_count += self.cr_transfer_count;
            }
            return transfer_count;
        }
    };

    pub const Options = struct {
        accounts_max: usize,
        account_id_permutation: IdPermutation,
        client_count: usize,

        batch_create_transfers_limit: u32,

        /// This is the maximum number of pending transfers, not counting those that have timed
        /// out.
        ///
        /// NOTE: Transfers that have posted/voided successfully (or not) that have _not_ yet
        /// reached their expiry are still included in this count — see `pending_expiries`.
        transfers_pending_max: usize,

        /// From the Auditor's point-of-view, all stalled requests are still in-flight, even if
        /// their reply has actually arrived at the ReplySequence.
        ///
        /// A request stops being "in-flight" when `on_reply` is called.
        ///
        /// This should equal the ReplySequence's `stalled_queue_capacity`.
        in_flight_max: usize,
    };

    pub const QueryIntersection = struct {
        user_data_64: u64,
        user_data_32: u32,
        code: u16,

        accounts: QueryIntersectionState = .{},
        transfers: QueryIntersectionState = .{},
    };

    pub const QueryIntersectionState = struct {
        /// The number of objects recorded.
        count: u32 = 0,
        /// Timestamp of the first object recorded.
        timestamp_min: u64 = 0,
        /// Timestamp of the last object recorded.
        timestamp_max: u64 = 0,
    };

    random: std.rand.Random,
    options: Options,

    /// The timestamp of the last processed reply.
    timestamp: u64 = 0,

    /// The account configuration. Balances are in sync with the remote StateMachine for a
    /// given commit (double-double entry accounting).
    accounts: []tb.Account,

    /// Additional account state. Keyed by account index.
    accounts_state: []AccountState,

    /// Known intersection values for a particular combination of secondary indexes.
    /// Counters are in sync with the remote StateMachine tracking the number of objects
    /// with such fields.
    query_intersections: []QueryIntersection,

    /// Map pending transfers to the (pending) amount and accounts.
    ///
    /// * Added in `on_create_transfers` for pending transfers.
    /// * Removed after a transfer is posted, voided, or timed out.
    ///
    /// All entries in `pending_transfers` have a corresponding entry in `pending_expiries`.
    pending_transfers: std.AutoHashMapUnmanaged(u128, PendingTransfer),

    /// After a transfer is posted/voided, the entry in `pending_expiries` is untouched.
    /// The timeout will not impact account balances (because the `pending_transfers` entry is
    /// removed), but until timeout the transfer still counts against `transfers_pending_max`.
    pending_expiries: PendingExpiryQueue,

    /// Track the expected result of the in-flight request for each client.
    /// Each member queue corresponds to entries of the client's request queue, but omits
    /// `register` messages.
    in_flight: InFlightQueue,

    /// The number of `create_accounts`/`create_transfers` sent, per client. Keyed by client index.
    creates_sent: []usize,

    /// The number of `create_accounts`/`create_transfers` delivered (i.e. replies received),
    /// per client. Keyed by client index.
    creates_delivered: []usize,

    pub fn init(
        allocator: std.mem.Allocator,
        random: std.rand.Random,
        options: Options,
    ) !AccountingAuditor {
        assert(options.accounts_max >= 2);
        assert(options.client_count > 0);

        const accounts = try allocator.alloc(tb.Account, options.accounts_max);
        errdefer allocator.free(accounts);
        @memset(accounts, undefined);

        const accounts_state = try allocator.alloc(AccountState, options.accounts_max);
        errdefer allocator.free(accounts_state);
        @memset(accounts_state, AccountState{});

        // The number of known intersection values for the secondary indices is kept low enough to
        // explore different cardinalities.
        const query_intersections = try allocator.alloc(
            QueryIntersection,
            options.accounts_max / 2,
        );
        errdefer allocator.free(query_intersections);
        for (query_intersections, 1..) |*query_intersection, index| {
            query_intersection.* = .{
                .user_data_64 = @intCast(index * 1_000_000),
                .user_data_32 = @intCast(index * 1_000),
                .code = @intCast(index), // It will be used to recover the index.
            };
        }

        var pending_transfers = std.AutoHashMapUnmanaged(u128, PendingTransfer){};
        errdefer pending_transfers.deinit(allocator);
        try pending_transfers.ensureTotalCapacity(
            allocator,
            @intCast(options.transfers_pending_max),
        );

        var pending_expiries = PendingExpiryQueue.init(allocator, {});
        errdefer pending_expiries.deinit();
        try pending_expiries.ensureTotalCapacity(options.transfers_pending_max);

        var in_flight = InFlightQueue{};
        errdefer in_flight.deinit(allocator);
        try in_flight.ensureTotalCapacity(allocator, @intCast(options.in_flight_max));

        const creates_sent = try allocator.alloc(usize, options.client_count);
        errdefer allocator.free(creates_sent);
        @memset(creates_sent, 0);

        const creates_delivered = try allocator.alloc(usize, options.client_count);
        errdefer allocator.free(creates_delivered);
        @memset(creates_delivered, 0);

        return .{
            .random = random,
            .options = options,
            .accounts = accounts,
            .accounts_state = accounts_state,
            .query_intersections = query_intersections,
            .pending_transfers = pending_transfers,
            .pending_expiries = pending_expiries,
            .in_flight = in_flight,
            .creates_sent = creates_sent,
            .creates_delivered = creates_delivered,
        };
    }

    pub fn deinit(self: *AccountingAuditor, allocator: std.mem.Allocator) void {
        allocator.free(self.accounts);
        allocator.free(self.accounts_state);
        self.pending_transfers.deinit(allocator);
        self.pending_expiries.deinit();
        self.in_flight.deinit(allocator);
        allocator.free(self.creates_sent);
        allocator.free(self.creates_delivered);
    }

    pub fn done(self: *const AccountingAuditor) bool {
        if (self.in_flight.count() != 0) return false;

        for (self.creates_sent, 0..) |sent, client_index| {
            if (sent != self.creates_delivered[client_index]) return false;
        }
        // Don't check pending_transfers; the workload might not have posted/voided every transfer.

        return true;
    }

    pub fn expect_create_accounts(
        self: *AccountingAuditor,
        client_index: usize,
    ) []CreateAccountResultSet {
        const result = self.in_flight.getOrPutAssumeCapacity(.{
            .client_index = client_index,
            .client_request = self.creates_sent[client_index],
        });
        assert(!result.found_existing);

        self.creates_sent[client_index] += 1;
        result.value_ptr.* = .{ .create_accounts = undefined };
        return result.value_ptr.*.create_accounts[0..];
    }

    pub fn expect_create_transfers(
        self: *AccountingAuditor,
        client_index: usize,
    ) []CreateTransferResultSet {
        const result = self.in_flight.getOrPutAssumeCapacity(.{
            .client_index = client_index,
            .client_request = self.creates_sent[client_index],
        });
        assert(!result.found_existing);

        self.creates_sent[client_index] += 1;
        result.value_ptr.* = .{ .create_transfers = undefined };
        return result.value_ptr.*.create_transfers[0..];
    }

    /// Expire pending transfers that have not been posted or voided.
    pub fn expire_pending_transfers(self: *AccountingAuditor, timestamp: u64) void {
        assert(self.timestamp < timestamp);
        defer self.timestamp = timestamp;

        var expired_count: u32 = 0;
        while (self.pending_expiries.peek()) |expiration| {
            if (timestamp < expiration.expires_at) break;
            defer _ = self.pending_expiries.remove();

            // Ignore the transfer if it was already posted/voided.
            const pending_transfer =
                self.pending_transfers.get(expiration.transfer_id) orelse continue;
            assert(self.pending_transfers.remove(expiration.transfer_id));
            assert(self.accounts_state[pending_transfer.debit_account_index].created);
            assert(self.accounts_state[pending_transfer.credit_account_index].created);

            const dr = &self.accounts[pending_transfer.debit_account_index];
            const cr = &self.accounts[pending_transfer.credit_account_index];
            dr.debits_pending -= pending_transfer.amount;
            cr.credits_pending -= pending_transfer.amount;

            // Each expiration round can expire at most one batch of transfers.
            expired_count += 1;
            if (expired_count == self.options.batch_create_transfers_limit) break;

            assert(!dr.debits_exceed_credits(0));
            assert(!dr.credits_exceed_debits(0));
            assert(!cr.debits_exceed_credits(0));
            assert(!cr.credits_exceed_debits(0));
        }
    }

    pub fn on_create_accounts(
        self: *AccountingAuditor,
        client_index: usize,
        timestamp: u64,
        accounts: []const tb.Account,
        results: []const tb.CreateAccountsResult,
    ) void {
        assert(accounts.len >= results.len);
        assert(self.timestamp < timestamp or
            (accounts.len == 0 and self.timestamp == timestamp));
        defer self.timestamp = timestamp;

        const results_expect = self.take_in_flight(client_index).create_accounts;
        var results_iterator = IteratorForCreateType(tb.CreateAccountsResult).init(results);
        defer assert(results_iterator.results.len == 0);

        for (accounts, 0..) |*account, i| {
            const account_timestamp = timestamp - accounts.len + i + 1;

            const result_actual = results_iterator.take(i) orelse .ok;
            if (!results_expect[i].contains(result_actual)) {
                log.err("on_create_accounts: account={} expect={} result={}", .{
                    account.*,
                    results_expect[i],
                    result_actual,
                });
                @panic("on_create_accounts: unexpected result");
            }

            const account_index = self.account_id_to_index(account.id);
            if (result_actual == .ok) {
                assert(!self.accounts_state[account_index].created);
                self.accounts_state[account_index].created = true;
                self.accounts[account_index] = account.*;
                self.accounts[account_index].timestamp = account_timestamp;

                const query_intersection_index = account.code - 1;
                const query_intersection = &self.query_intersections[query_intersection_index];
                assert(account.user_data_64 == query_intersection.user_data_64);
                assert(account.user_data_32 == query_intersection.user_data_32);
                assert(account.code == query_intersection.code);
                query_intersection.accounts.count += 1;
                if (query_intersection.accounts.timestamp_min == 0) {
                    query_intersection.accounts.timestamp_min = account_timestamp;
                }
                query_intersection.accounts.timestamp_max = account_timestamp;
            }

            if (account_index >= self.accounts.len) {
                assert(result_actual != .ok);
            }
        }
    }

    pub fn on_create_transfers(
        self: *AccountingAuditor,
        client_index: usize,
        timestamp: u64,
        transfers: []const tb.Transfer,
        results: []const tb.CreateTransfersResult,
    ) void {
        assert(transfers.len >= results.len);
        assert(self.timestamp < timestamp or
            (transfers.len == 0 and self.timestamp == timestamp));
        defer self.timestamp = timestamp;

        const results_expect = self.take_in_flight(client_index).create_transfers;
        var results_iterator = IteratorForCreateType(tb.CreateTransfersResult).init(results);
        defer assert(results_iterator.results.len == 0);

        for (transfers, 0..) |*transfer, i| {
            const transfer_timestamp = timestamp - transfers.len + i + 1;

            const result_actual = results_iterator.take(i) orelse .ok;
            if (!results_expect[i].contains(result_actual.to_ordered())) {
                log.err("on_create_transfers: transfer={} expect={} result={}", .{
                    transfer.*,
                    results_expect[i],
                    result_actual,
                });
                @panic("on_create_transfers: unexpected result");
            }

            if (result_actual != .ok) continue;

            const query_intersection_index = transfer.code - 1;
            const query_intersection = &self.query_intersections[query_intersection_index];
            assert(transfer.user_data_64 == query_intersection.user_data_64);
            assert(transfer.user_data_32 == query_intersection.user_data_32);
            assert(transfer.code == query_intersection.code);
            query_intersection.transfers.count += 1;
            if (query_intersection.transfers.timestamp_min == 0) {
                query_intersection.transfers.timestamp_min = transfer_timestamp;
            }
            query_intersection.transfers.timestamp_max = transfer_timestamp;

            if (transfer.flags.post_pending_transfer or transfer.flags.void_pending_transfer) {
                const p = self.pending_transfers.get(transfer.pending_id).?;
                const dr_state = &self.accounts_state[p.debit_account_index];
                const cr_state = &self.accounts_state[p.credit_account_index];
                dr_state.update(.dr, transfer_timestamp);
                cr_state.update(.cr, transfer_timestamp);

                const dr = &self.accounts[p.debit_account_index];
                const cr = &self.accounts[p.credit_account_index];

                assert(self.pending_transfers.remove(transfer.pending_id));
                // The transfer may still be in `pending_expiries` — removal would be O(n),
                // so don't bother.

                dr.debits_pending -= p.amount;
                cr.credits_pending -= p.amount;
                if (transfer.flags.post_pending_transfer) {
                    const amount = @min(transfer.amount, p.amount);
                    dr.debits_posted += amount;
                    cr.credits_posted += amount;
                }

                assert(!dr.debits_exceed_credits(0));
                assert(!dr.credits_exceed_debits(0));
                assert(!cr.debits_exceed_credits(0));
                assert(!cr.credits_exceed_debits(0));
            } else {
                const dr_index = self.account_id_to_index(transfer.debit_account_id);
                const cr_index = self.account_id_to_index(transfer.credit_account_id);
                const dr_state = &self.accounts_state[dr_index];
                const cr_state = &self.accounts_state[cr_index];
                dr_state.update(.dr, transfer_timestamp);
                cr_state.update(.cr, transfer_timestamp);

                const dr = &self.accounts[dr_index];
                const cr = &self.accounts[cr_index];

                if (transfer.flags.pending) {
                    if (transfer.timeout > 0) {
                        self.pending_transfers.putAssumeCapacity(transfer.id, .{
                            .amount = transfer.amount,
                            .debit_account_index = dr_index,
                            .credit_account_index = cr_index,
                            .query_intersection_index = transfer.code - 1,
                        });
                        self.pending_expiries.add(.{
                            .transfer_id = transfer.id,
                            .transfer_timestamp = transfer_timestamp,
                            .expires_at = transfer_timestamp + transfer.timeout_ns(),
                        }) catch unreachable;
                        // PriorityQueue lacks an "unmanaged" API, so verify that the workload
                        // hasn't created more pending transfers than permitted.
                        assert(self.pending_expiries.count() <= self.options.transfers_pending_max);
                    }
                    dr.debits_pending += transfer.amount;
                    cr.credits_pending += transfer.amount;
                } else {
                    dr.debits_posted += transfer.amount;
                    cr.credits_posted += transfer.amount;
                }

                assert(!dr.debits_exceed_credits(0));
                assert(!dr.credits_exceed_debits(0));
                assert(!cr.debits_exceed_credits(0));
                assert(!cr.credits_exceed_debits(0));
            }
        }
    }

    pub fn on_lookup_accounts(
        self: *AccountingAuditor,
        client_index: usize,
        timestamp: u64,
        ids: []const u128,
        results: []const tb.Account,
    ) void {
        _ = client_index;
        assert(ids.len >= results.len);
        assert(self.timestamp <= timestamp);
        defer self.timestamp = timestamp;

        var results_iterator = IteratorForLookupType(tb.Account).init(results);
        defer assert(results_iterator.results.len == 0);

        for (ids) |account_id| {
            const account_index = self.account_id_to_index(account_id);
            const account_lookup = results_iterator.take(account_id);

            if (account_index < self.accounts.len and
                self.accounts_state[account_index].created)
            {
                // If this assertion fails, `lookup_accounts` didn't return an account when it
                // should have.
                assert(account_lookup != null);
                assert(!account_lookup.?.debits_exceed_credits(0));
                assert(!account_lookup.?.credits_exceed_debits(0));

                const account_expect = &self.accounts[account_index];
                if (!std.mem.eql(
                    u8,
                    std.mem.asBytes(account_lookup.?),
                    std.mem.asBytes(account_expect),
                )) {
                    log.err("on_lookup_accounts: account data mismatch " ++
                        "account_id={} expect={} lookup={}", .{
                        account_id,
                        account_expect,
                        account_lookup.?,
                    });
                    @panic("on_lookup_accounts: account data mismatch");
                }
            } else {
                // If this assertion fails, `lookup_accounts` returned an account when it shouldn't.
                assert(account_lookup == null);
            }
        }
    }

    /// Most `lookup_transfers` validation is handled by Workload.
    /// (Workload has more context around transfers, so it can be much stricter.)
    pub fn on_lookup_transfers(
        self: *AccountingAuditor,
        client_index: usize,
        timestamp: u64,
        ids: []const u128,
        results: []const tb.Transfer,
    ) void {
        _ = client_index;
        assert(ids.len >= results.len);
        assert(self.timestamp <= timestamp);
        defer self.timestamp = timestamp;

        var results_iterator = IteratorForLookupType(tb.Transfer).init(results);
        defer assert(results_iterator.results.len == 0);

        for (ids) |id| {
            const result = results_iterator.take(id);
            assert(result == null or result.?.id == id);
        }
    }

    /// Returns a random account matching the given criteria.
    /// Returns null when no account matches the given criteria.
    pub fn pick_account(
        self: *const AccountingAuditor,
        match: struct {
            /// Whether the account is known to be created
            /// (we have received an `ok` for the respective `create_accounts`).
            created: ?bool,
            debits_must_not_exceed_credits: ?bool,
            credits_must_not_exceed_debits: ?bool,
            /// Don't match this account.
            exclude: ?u128 = null,
        },
    ) ?*const tb.Account {
        const offset = self.random.uintLessThanBiased(usize, self.accounts.len);
        var i: usize = 0;
        // Iterate `accounts`, starting from a random offset.
        while (i < self.accounts.len) : (i += 1) {
            const account_index = (offset + i) % self.accounts.len;
            if (match.created) |expect_created| {
                if (self.accounts_state[account_index].created) {
                    if (!expect_created) continue;
                } else {
                    if (expect_created) continue;
                }
            }

            const account = &self.accounts[account_index];
            if (match.debits_must_not_exceed_credits) |b| {
                if (account.flags.debits_must_not_exceed_credits != b) continue;
            }

            if (match.credits_must_not_exceed_debits) |b| {
                if (account.flags.credits_must_not_exceed_debits != b) continue;
            }

            if (match.exclude) |exclude_id| {
                if (account.id == exclude_id) continue;
            }
            return account;
        }
        return null;
    }

    pub fn account_id_to_index(self: *const AccountingAuditor, id: u128) usize {
        // -1 because id=0 is not valid, so index=0→id=1.
        return @as(usize, @intCast(self.options.account_id_permutation.decode(id))) - 1;
    }

    pub fn account_index_to_id(self: *const AccountingAuditor, index: usize) u128 {
        // +1 so that index=0 is encoded as a valid id.
        return self.options.account_id_permutation.encode(index + 1);
    }

    pub fn get_account(self: *const AccountingAuditor, id: u128) ?*const tb.Account {
        const index = self.account_id_to_index(id);
        return if (index < self.accounts.len) &self.accounts[index] else null;
    }

    pub fn get_account_state(self: *const AccountingAuditor, id: u128) ?*const AccountState {
        const index = self.account_id_to_index(id);
        return if (index < self.accounts_state.len) &self.accounts_state[index] else null;
    }

    fn take_in_flight(self: *AccountingAuditor, client_index: usize) InFlight {
        const key = .{
            .client_index = client_index,
            .client_request = self.creates_delivered[client_index],
        };
        self.creates_delivered[client_index] += 1;

        const in_flight = self.in_flight.get(key).?;
        assert(self.in_flight.remove(key));
        return in_flight;
    }
};

pub fn IteratorForCreateType(comptime Result: type) type {
    assert(Result == tb.CreateAccountsResult or Result == tb.CreateTransfersResult);

    return struct {
        const IteratorForCreate = @This();

        results: []const Result,

        pub fn init(results: []const Result) IteratorForCreate {
            return .{ .results = results };
        }

        pub fn take(
            self: *IteratorForCreate,
            event_index: usize,
        ) ?std.meta.fieldInfo(Result, .result).type {
            if (self.results.len > 0 and self.results[0].index == event_index) {
                defer self.results = self.results[1..];
                return self.results[0].result;
            } else {
                return null;
            }
        }
    };
}

pub fn IteratorForLookupType(comptime Result: type) type {
    assert(Result == tb.Account or Result == tb.Transfer);

    return struct {
        const IteratorForLookup = @This();

        results: []const Result,

        pub fn init(results: []const Result) IteratorForLookup {
            return .{ .results = results };
        }

        pub fn take(self: *IteratorForLookup, id: u128) ?*const Result {
            if (self.results.len > 0 and self.results[0].id == id) {
                defer self.results = self.results[1..];
                return &self.results[0];
            } else {
                return null;
            }
        }
    };
}
