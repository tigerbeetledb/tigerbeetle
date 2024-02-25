const std = @import("std");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const log = std.log.scoped(.state_machine);
const tracer = @import("tracer.zig");

const stdx = @import("./stdx.zig");
const global_constants = @import("constants.zig");
const tb = @import("tigerbeetle.zig");
const ScopeCloseMode = @import("lsm/tree.zig").ScopeCloseMode;
const WorkloadType = @import("state_machine/workload.zig").WorkloadType;

const Direction = @import("direction.zig").Direction;
const TimestampRange = @import("lsm/timestamp_range.zig").TimestampRange;

const Snapshot = @import("lsm/schema.zig").Snapshot;

const Account = tb.Account;
const AccountFlags = tb.AccountFlags;
const AccountBalance = tb.AccountBalance;

const Transfer = tb.Transfer;
const TransferFlags = tb.TransferFlags;

const CreateAccountsResult = tb.CreateAccountsResult;
const CreateTransfersResult = tb.CreateTransfersResult;

const CreateAccountResult = tb.CreateAccountResult;
const CreateTransferResult = tb.CreateTransferResult;

const AccountFilter = tb.AccountFilter;

pub fn StateMachineType(
    comptime Storage: type,
    comptime config: global_constants.StateMachineConfig,
) type {
    assert(config.message_body_size_max > 0);
    assert(config.lsm_batch_multiple > 0);
    assert(config.vsr_operations_reserved > 0);

    return struct {
        const StateMachine = @This();
        const Grid = @import("vsr/grid.zig").GridType(Storage);
        const GrooveType = @import("lsm/groove.zig").GrooveType;
        const ForestType = @import("lsm/forest.zig").ForestType;
        const ScanLookupType = @import("lsm/scan_lookup.zig").ScanLookupType;

        pub const constants = struct {
            pub const message_body_size_max = config.message_body_size_max;

            /// The maximum number of objects within a batch, by operation.
            pub const batch_max = struct {
                pub const create_accounts = operation_batch_max(.create_accounts);
                pub const create_transfers = operation_batch_max(.create_transfers);
                pub const lookup_accounts = operation_batch_max(.lookup_accounts);
                pub const lookup_transfers = operation_batch_max(.lookup_transfers);
                pub const get_account_transfers = operation_batch_max(.get_account_transfers);
                pub const get_account_history = operation_batch_max(.get_account_history);

                comptime {
                    assert(create_accounts > 0);
                    assert(create_transfers > 0);
                    assert(lookup_accounts > 0);
                    assert(lookup_transfers > 0);
                    assert(get_account_transfers > 0);
                    assert(get_account_history > 0);
                }

                fn operation_batch_max(comptime operation: Operation) usize {
                    return @divFloor(message_body_size_max, @max(
                        @sizeOf(Event(operation)),
                        @sizeOf(Result(operation)),
                    ));
                }
            };

            pub const tree_ids = struct {
                pub const accounts = .{
                    .id = 1,
                    .user_data_128 = 2,
                    .user_data_64 = 3,
                    .user_data_32 = 4,
                    .ledger = 5,
                    .code = 6,
                    .timestamp = 7,
                };

                pub const transfers = .{
                    .id = 8,
                    .debit_account_id = 9,
                    .credit_account_id = 10,
                    .amount = 11,
                    .pending_id = 12,
                    .user_data_128 = 13,
                    .user_data_64 = 14,
                    .user_data_32 = 15,
                    .timeout = 16,
                    .ledger = 17,
                    .code = 18,
                    .timestamp = 19,
                };

                pub const posted = .{
                    .timestamp = 20,
                };

                pub const account_history = .{
                    .timestamp = 21,
                };
            };
        };

        /// Used to determine if an operation can be batched at the VSR layer.
        /// If so, the StateMachine must support demuxing batched operations below.
        pub const batch_logical_allowed = std.enums.EnumArray(Operation, bool).init(.{
            .create_accounts = true,
            .create_transfers = true,
            // Don't batch lookups/queries for now.
            .lookup_accounts = false,
            .lookup_transfers = false,
            .get_account_transfers = false,
            .get_account_history = false,
        });

        pub fn DemuxerType(comptime operation: Operation) type {
            return struct {
                const Demuxer = @This();
                const DemuxerResult = Result(operation);

                results: []DemuxerResult,

                /// Create a Demuxer which can extract Results out of the reply bytes in-place.
                pub fn init(reply: []DemuxerResult) Demuxer {
                    return Demuxer{ .results = reply };
                }

                /// Returns a slice into the the original reply bytes with Results matching the
                /// Event range (offset and size). Each subsequent call to demux() must have ranges
                /// that are disjoint and increase monotonically.
                pub fn decode(self: *Demuxer, event_offset: u32, event_count: u32) []DemuxerResult {
                    const demuxed = blk: {
                        if (comptime batch_logical_allowed.get(operation)) {
                            // Count all results from out slice which match the Event range,
                            // updating the result.indexes to be related to the EVent in the process.
                            for (self.results, 0..) |*result, i| {
                                if (result.index < event_offset) break :blk i;
                                if (result.index >= event_offset + event_count) break :blk i;
                                result.index -= event_offset;
                            }
                        } else {
                            // Operations which aren't batched have the first Event consume the
                            // entire Result down below.
                            assert(event_offset == 0);
                        }
                        break :blk self.results.len;
                    };

                    // Return all results demuxed from the given Event, re-slicing them out of
                    // self.results to "consume" them from subsequent decode() calls.
                    defer self.results = self.results[demuxed..];
                    return self.results[0..demuxed];
                }
            };
        }

        const AccountsGroove = GrooveType(
            Storage,
            Account,
            .{
                .ids = constants.tree_ids.accounts,
                .value_count_max = .{
                    .id = config.lsm_batch_multiple * constants.batch_max.create_accounts,
                    .user_data_128 = config.lsm_batch_multiple * constants.batch_max.create_accounts,
                    .user_data_64 = config.lsm_batch_multiple * constants.batch_max.create_accounts,
                    .user_data_32 = config.lsm_batch_multiple * constants.batch_max.create_accounts,
                    .ledger = config.lsm_batch_multiple * constants.batch_max.create_accounts,
                    .code = config.lsm_batch_multiple * constants.batch_max.create_accounts,
                    // Transfers mutate the account balance for debits/credits pending/posted.
                    // Each transfer modifies two accounts.
                    .timestamp = config.lsm_batch_multiple * @as(usize, @max(
                        constants.batch_max.create_accounts,
                        2 * constants.batch_max.create_transfers,
                    )),
                },
                .ignored = &[_][]const u8{
                    "debits_posted",
                    "debits_pending",
                    "credits_posted",
                    "credits_pending",
                    "flags",
                    "reserved",
                },
                .derived = .{},
            },
        );

        const TransfersGroove = GrooveType(
            Storage,
            Transfer,
            .{
                .ids = constants.tree_ids.transfers,
                .value_count_max = .{
                    .timestamp = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .id = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .debit_account_id = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .credit_account_id = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .amount = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .pending_id = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .user_data_128 = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .user_data_64 = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .user_data_32 = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .timeout = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .ledger = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                    .code = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                },
                .ignored = &[_][]const u8{"flags"},
                .derived = .{},
            },
        );

        const PostedGroove = GrooveType(
            Storage,
            PostedGrooveValue,
            .{
                .ids = constants.tree_ids.posted,
                .value_count_max = .{
                    .timestamp = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                },
                .ignored = &[_][]const u8{ "fulfillment", "padding" },
                .derived = .{},
            },
        );

        pub const PostedGrooveValue = extern struct {
            timestamp: u64,
            fulfillment: enum(u8) {
                posted = 0,
                voided = 1,
            },
            padding: [7]u8,

            comptime {
                // Assert that there is no implicit padding.
                assert(@sizeOf(PostedGrooveValue) == 16);
                assert(stdx.no_padding(PostedGrooveValue));
            }
        };

        const AccountHistoryGroove = GrooveType(
            Storage,
            AccountHistoryGrooveValue,
            .{
                .ids = constants.tree_ids.account_history,
                .value_count_max = .{
                    .timestamp = config.lsm_batch_multiple * constants.batch_max.create_transfers,
                },
                .ignored = &[_][]const u8{
                    "dr_account_id",
                    "dr_debits_pending",
                    "dr_debits_posted",
                    "dr_credits_pending",
                    "dr_credits_posted",
                    "cr_account_id",
                    "cr_debits_pending",
                    "cr_debits_posted",
                    "cr_credits_pending",
                    "cr_credits_posted",
                    "reserved",
                },
                .derived = .{},
            },
        );

        pub const AccountHistoryGrooveValue = extern struct {
            dr_account_id: u128,
            dr_debits_pending: u128,
            dr_debits_posted: u128,
            dr_credits_pending: u128,
            dr_credits_posted: u128,
            cr_account_id: u128,
            cr_debits_pending: u128,
            cr_debits_posted: u128,
            cr_credits_pending: u128,
            cr_credits_posted: u128,
            timestamp: u64 = 0,
            reserved: [88]u8 = [_]u8{0} ** 88,

            comptime {
                assert(stdx.no_padding(AccountHistoryGrooveValue));
                assert(@sizeOf(AccountHistoryGrooveValue) == 256);
                assert(@alignOf(AccountHistoryGrooveValue) == 16);
            }
        };

        pub const Workload = WorkloadType(StateMachine);

        pub const Forest = ForestType(Storage, .{
            .accounts = AccountsGroove,
            .transfers = TransfersGroove,
            .posted = PostedGroove,
            .account_history = AccountHistoryGroove,
        });

        const TransfersScanLookup = ScanLookupType(
            TransfersGroove,
            TransfersGroove.ScanBuilder.Scan,
            Storage,
        );

        const AccountHistoryScanLookup = ScanLookupType(
            AccountHistoryGroove,
            // Both Objects use the same timestamp, so we can use the TransfersGroove's indexes.
            TransfersGroove.ScanBuilder.Scan,
            Storage,
        );

        pub const Operation = enum(u8) {
            /// Operations exported by TigerBeetle:
            create_accounts = config.vsr_operations_reserved + 0,
            create_transfers = config.vsr_operations_reserved + 1,
            lookup_accounts = config.vsr_operations_reserved + 2,
            lookup_transfers = config.vsr_operations_reserved + 3,
            get_account_transfers = config.vsr_operations_reserved + 4,
            get_account_history = config.vsr_operations_reserved + 5,
        };

        pub const Options = struct {
            lsm_forest_node_count: u32,
            cache_entries_accounts: u32,
            cache_entries_transfers: u32,
            cache_entries_posted: u32,
            cache_entries_account_history: u32,
        };

        /// Since prefetch contexts are used one at a time, it's safe to access
        /// the union's fields and reuse the same memory for all context instances.
        const PrefetchContext = union(enum) {
            null,
            accounts: AccountsGroove.PrefetchContext,
            transfers: TransfersGroove.PrefetchContext,
            posted: PostedGroove.PrefetchContext,

            pub const Field = std.meta.FieldEnum(PrefetchContext);
            pub fn FieldType(comptime field: Field) type {
                return std.meta.fieldInfo(PrefetchContext, field).type;
            }

            pub fn parent(
                comptime field: Field,
                completion: *FieldType(field),
            ) *StateMachine {
                comptime assert(field != .null);

                const context = @fieldParentPtr(PrefetchContext, @tagName(field), completion);
                return @fieldParentPtr(StateMachine, "prefetch_context", context);
            }

            pub fn get(self: *PrefetchContext, comptime field: Field) *FieldType(field) {
                comptime assert(field != .null);
                assert(self.* == .null);

                self.* = @unionInit(PrefetchContext, @tagName(field), undefined);
                return &@field(self, @tagName(field));
            }
        };

        /// Since scan lookups are used one at a time, it's safe to access
        /// the union's fields and reuse the same memory for all ScanLookup instances.
        const ScanLookup = union(enum) {
            null,
            transfer: TransfersScanLookup,
            account_history: AccountHistoryScanLookup,

            pub const Field = std.meta.FieldEnum(ScanLookup);
            pub fn FieldType(comptime field: Field) type {
                return std.meta.fieldInfo(ScanLookup, field).type;
            }

            pub fn parent(
                comptime field: Field,
                completion: *FieldType(field),
            ) *StateMachine {
                comptime assert(field != .null);

                const context = @fieldParentPtr(ScanLookup, @tagName(field), completion);
                return @fieldParentPtr(StateMachine, "scan_lookup", context);
            }

            pub fn get(self: *ScanLookup, comptime field: Field) *FieldType(field) {
                comptime assert(field != .null);
                assert(self.* == .null);

                self.* = @unionInit(ScanLookup, @tagName(field), undefined);
                return &@field(self, @tagName(field));
            }
        };

        prepare_timestamp: u64,
        commit_timestamp: u64,
        forest: Forest,

        prefetch_input: ?[]align(16) const u8 = null,
        prefetch_callback: ?*const fn (*StateMachine) void = null,
        prefetch_context: PrefetchContext = .null,

        scan_lookup: ScanLookup = .null,
        scan_buffer: []align(16) u8,
        scan_result_count: u32 = 0,
        scan_next_tick: Grid.NextTick = undefined,

        open_callback: ?*const fn (*StateMachine) void = null,
        compact_callback: ?*const fn (*StateMachine) void = null,
        checkpoint_callback: ?*const fn (*StateMachine) void = null,

        tracer_slot: ?tracer.SpanStart = null,

        pub fn init(allocator: mem.Allocator, grid: *Grid, options: Options) !StateMachine {
            var forest = try Forest.init(
                allocator,
                grid,
                options.lsm_forest_node_count,
                forest_options(options),
            );
            errdefer forest.deinit(allocator);

            var scan_buffer = try allocator.alignedAlloc(u8, 16, @max(
                constants.batch_max.get_account_transfers * @sizeOf(Transfer),
                constants.batch_max.get_account_history * @sizeOf(AccountHistoryGrooveValue),
            ));
            errdefer allocator.free(scan_buffer);

            return StateMachine{
                .prepare_timestamp = 0,
                .commit_timestamp = 0,
                .forest = forest,
                .scan_buffer = scan_buffer,
            };
        }

        pub fn deinit(self: *StateMachine, allocator: mem.Allocator) void {
            assert(self.tracer_slot == null);

            allocator.free(self.scan_buffer);
            self.forest.deinit(allocator);
        }

        // TODO Reset here and in LSM should clean up (i.e. end) tracer spans.
        // tracer.end() requires an event be passed in. We will need an additional tracer.end
        // function that doesn't require the explicit event be passed in. The Trace should store the
        // event so that it knows what event should be ending during reset() (and deinit(), maybe).
        // Then the original tracer.end() can assert that the two events match.
        pub fn reset(self: *StateMachine) void {
            self.forest.reset();

            self.* = .{
                .prepare_timestamp = 0,
                .commit_timestamp = 0,
                .forest = self.forest,
                .scan_buffer = self.scan_buffer,
            };
        }

        pub fn Event(comptime operation: Operation) type {
            return switch (operation) {
                .create_accounts => Account,
                .create_transfers => Transfer,
                .lookup_accounts => u128,
                .lookup_transfers => u128,
                .get_account_transfers => AccountFilter,
                .get_account_history => AccountFilter,
            };
        }

        pub fn Result(comptime operation: Operation) type {
            return switch (operation) {
                .create_accounts => CreateAccountsResult,
                .create_transfers => CreateTransfersResult,
                .lookup_accounts => Account,
                .lookup_transfers => Transfer,
                .get_account_transfers => Transfer,
                .get_account_history => AccountBalance,
            };
        }

        pub fn open(self: *StateMachine, callback: *const fn (*StateMachine) void) void {
            assert(self.open_callback == null);
            self.open_callback = callback;

            self.forest.open(forest_open_callback);
        }

        fn forest_open_callback(forest: *Forest) void {
            const self = @fieldParentPtr(StateMachine, "forest", forest);
            assert(self.open_callback != null);

            const callback = self.open_callback.?;
            self.open_callback = null;
            callback(self);
        }

        /// Updates `prepare_timestamp` to the highest timestamp of the response.
        pub fn prepare(self: *StateMachine, operation: Operation, input: []align(16) u8) void {
            self.prepare_timestamp += switch (operation) {
                .create_accounts => mem.bytesAsSlice(Account, input).len,
                .create_transfers => mem.bytesAsSlice(Transfer, input).len,
                .lookup_accounts => 0,
                .lookup_transfers => 0,
                .get_account_transfers => 0,
                .get_account_history => 0,
            };
        }

        pub fn prefetch(
            self: *StateMachine,
            callback: *const fn (*StateMachine) void,
            op: u64,
            operation: Operation,
            input: []align(16) const u8,
        ) void {
            _ = op;
            assert(self.prefetch_input == null);
            assert(self.prefetch_callback == null);

            tracer.start(
                &self.tracer_slot,
                .state_machine_prefetch,
                @src(),
            );

            self.prefetch_input = input;
            self.prefetch_callback = callback;

            // TODO(Snapshots) Pass in the target snapshot.
            self.forest.grooves.accounts.prefetch_setup(null);
            self.forest.grooves.transfers.prefetch_setup(null);
            self.forest.grooves.posted.prefetch_setup(null);

            return switch (operation) {
                .create_accounts => {
                    self.prefetch_create_accounts(mem.bytesAsSlice(Account, input));
                },
                .create_transfers => {
                    self.prefetch_create_transfers(mem.bytesAsSlice(Transfer, input));
                },
                .lookup_accounts => {
                    self.prefetch_lookup_accounts(mem.bytesAsSlice(u128, input));
                },
                .lookup_transfers => {
                    self.prefetch_lookup_transfers(mem.bytesAsSlice(u128, input));
                },
                .get_account_transfers => {
                    self.prefetch_get_account_transfers(parse_filter_from_input(input));
                },
                .get_account_history => {
                    self.prefetch_get_account_history(parse_filter_from_input(input));
                },
            };
        }

        fn prefetch_finish(self: *StateMachine) void {
            assert(self.prefetch_input != null);
            assert(self.prefetch_context == .null);
            assert(self.scan_lookup == .null);

            const callback = self.prefetch_callback.?;
            self.prefetch_input = null;
            self.prefetch_callback = null;

            tracer.end(
                &self.tracer_slot,
                .state_machine_prefetch,
            );

            callback(self);
        }

        fn prefetch_create_accounts(self: *StateMachine, accounts: []const Account) void {
            for (accounts) |*a| {
                self.forest.grooves.accounts.prefetch_enqueue(a.id);
            }

            self.forest.grooves.accounts.prefetch(
                prefetch_create_accounts_callback,
                self.prefetch_context.get(.accounts),
            );
        }

        fn prefetch_create_accounts_callback(
            completion: *AccountsGroove.PrefetchContext,
        ) void {
            const self: *StateMachine = PrefetchContext.parent(.accounts, completion);
            self.prefetch_context = .null;

            self.prefetch_finish();
        }

        fn prefetch_create_transfers(self: *StateMachine, transfers: []const Transfer) void {
            for (transfers) |*t| {
                self.forest.grooves.transfers.prefetch_enqueue(t.id);

                if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
                    self.forest.grooves.transfers.prefetch_enqueue(t.pending_id);
                }
            }

            self.forest.grooves.transfers.prefetch(
                prefetch_create_transfers_callback_transfers,
                self.prefetch_context.get(.transfers),
            );
        }

        fn prefetch_create_transfers_callback_transfers(completion: *TransfersGroove.PrefetchContext) void {
            const self: *StateMachine = PrefetchContext.parent(.transfers, completion);
            self.prefetch_context = .null;

            const transfers = mem.bytesAsSlice(Event(.create_transfers), self.prefetch_input.?);
            for (transfers) |*t| {
                if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
                    if (self.forest.grooves.transfers.get(t.pending_id)) |p| {
                        // This prefetch isn't run yet, but enqueue it here as well to save an extra
                        // iteration over transfers.
                        self.forest.grooves.posted.prefetch_enqueue(p.timestamp);

                        self.forest.grooves.accounts.prefetch_enqueue(p.debit_account_id);
                        self.forest.grooves.accounts.prefetch_enqueue(p.credit_account_id);
                    }
                } else {
                    self.forest.grooves.accounts.prefetch_enqueue(t.debit_account_id);
                    self.forest.grooves.accounts.prefetch_enqueue(t.credit_account_id);
                }
            }

            self.forest.grooves.accounts.prefetch(
                prefetch_create_transfers_callback_accounts,
                self.prefetch_context.get(.accounts),
            );
        }

        fn prefetch_create_transfers_callback_accounts(completion: *AccountsGroove.PrefetchContext) void {
            const self: *StateMachine = PrefetchContext.parent(.accounts, completion);
            self.prefetch_context = .null;

            self.forest.grooves.posted.prefetch(
                prefetch_create_transfers_callback_posted,
                self.prefetch_context.get(.posted),
            );
        }

        fn prefetch_create_transfers_callback_posted(completion: *PostedGroove.PrefetchContext) void {
            const self: *StateMachine = PrefetchContext.parent(.posted, completion);
            self.prefetch_context = .null;

            self.prefetch_finish();
        }

        fn prefetch_lookup_accounts(self: *StateMachine, ids: []const u128) void {
            for (ids) |id| {
                self.forest.grooves.accounts.prefetch_enqueue(id);
            }

            self.forest.grooves.accounts.prefetch(
                prefetch_lookup_accounts_callback,
                self.prefetch_context.get(.accounts),
            );
        }

        fn prefetch_lookup_accounts_callback(completion: *AccountsGroove.PrefetchContext) void {
            const self: *StateMachine = PrefetchContext.parent(.accounts, completion);
            self.prefetch_context = .null;

            self.prefetch_finish();
        }

        fn prefetch_lookup_transfers(self: *StateMachine, ids: []const u128) void {
            for (ids) |id| {
                self.forest.grooves.transfers.prefetch_enqueue(id);
            }

            self.forest.grooves.transfers.prefetch(
                prefetch_lookup_transfers_callback,
                self.prefetch_context.get(.transfers),
            );
        }

        fn prefetch_lookup_transfers_callback(completion: *TransfersGroove.PrefetchContext) void {
            const self: *StateMachine = PrefetchContext.parent(.transfers, completion);
            self.prefetch_context = .null;

            self.prefetch_finish();
        }

        fn prefetch_get_account_transfers(self: *StateMachine, filter: AccountFilter) void {
            assert(self.scan_result_count == 0);

            if (self.get_scan_from_filter(filter)) |scan| {
                var scan_buffer = std.mem.bytesAsSlice(
                    Transfer,
                    self.scan_buffer[0 .. @sizeOf(Transfer) *
                        constants.batch_max.get_account_transfers],
                );
                assert(scan_buffer.len == constants.batch_max.get_account_transfers);

                var scan_lookup = self.scan_lookup.get(.transfer);
                scan_lookup.* = TransfersScanLookup.init(
                    &self.forest.grooves.transfers,
                    scan,
                );

                scan_lookup.read(
                    // Limiting the buffer size according to the query limit.
                    scan_buffer[0..@min(filter.limit, scan_buffer.len)],
                    &prefetch_get_account_transfers_callback,
                );
            } else {
                // TODO(batiati): Improve the way we do validations on the state machine.
                log.info("invalid filter for get_account_transfers: {any}", .{filter});
                self.forest.grid.on_next_tick(
                    &prefetch_scan_next_tick_callback,
                    &self.scan_next_tick,
                );
            }
        }

        fn prefetch_get_account_transfers_callback(scan_lookup: *TransfersScanLookup) void {
            const self: *StateMachine = ScanLookup.parent(.transfer, scan_lookup);
            self.scan_result_count = @intCast(scan_lookup.slice().len);

            self.scan_lookup = .null;
            self.forest.scan_buffer_pool.reset();
            self.forest.grooves.transfers.scan_builder.reset();

            self.prefetch_finish();
        }

        fn prefetch_get_account_history(self: *StateMachine, filter: AccountFilter) void {
            assert(self.scan_result_count == 0);

            self.forest.grooves.accounts.prefetch_enqueue(filter.account_id);
            self.forest.grooves.accounts.prefetch(
                prefetch_get_account_history_lookup_account_callback,
                self.prefetch_context.get(.accounts),
            );
        }

        fn prefetch_get_account_history_lookup_account_callback(
            completion: *AccountsGroove.PrefetchContext,
        ) void {
            const self: *StateMachine = PrefetchContext.parent(.accounts, completion);
            self.prefetch_context = .null;

            const filter = parse_filter_from_input(self.prefetch_input.?);
            self.prefetch_get_account_history_scan(filter);
        }

        fn prefetch_get_account_history_scan(self: *StateMachine, filter: AccountFilter) void {
            assert(self.scan_result_count == 0);

            if (self.get_scan_from_filter(filter)) |scan| {
                if (self.forest.grooves.accounts.get(filter.account_id)) |account| {
                    if (account.flags.history) {
                        var scan_buffer = std.mem.bytesAsSlice(
                            AccountHistoryGrooveValue,
                            self.scan_buffer[0 .. @sizeOf(AccountHistoryGrooveValue) *
                                constants.batch_max.get_account_history],
                        );
                        assert(scan_buffer.len == constants.batch_max.get_account_history);

                        var scan_lookup = self.scan_lookup.get(.account_history);
                        scan_lookup.* = AccountHistoryScanLookup.init(
                            &self.forest.grooves.account_history,
                            scan,
                        );

                        scan_lookup.read(
                            // Limiting the buffer size according to the query limit.
                            scan_buffer[0..@min(filter.limit, scan_buffer.len)],
                            &prefetch_get_account_history_scan_callback,
                        );

                        return;
                    }
                }
            } else {
                // TODO(batiati): Improve the way we do validations on the state machine.
                log.info(
                    "invalid filter for get_account_history: {any}",
                    .{filter},
                );
            }

            // Returning an empty array on the next tick.
            self.forest.grid.on_next_tick(
                &prefetch_scan_next_tick_callback,
                &self.scan_next_tick,
            );
        }

        fn prefetch_get_account_history_scan_callback(scan_lookup: *AccountHistoryScanLookup) void {
            const self: *StateMachine = ScanLookup.parent(.account_history, scan_lookup);
            self.scan_result_count = @intCast(scan_lookup.slice().len);

            self.forest.scan_buffer_pool.reset();
            self.forest.grooves.transfers.scan_builder.reset();
            self.scan_lookup = .null;

            self.prefetch_finish();
        }

        // TODO(batiati): Using a zeroed filter in case of invalid input.
        // Implement input validation on `prepare` for all operations.
        fn parse_filter_from_input(input: []const u8) AccountFilter {
            return if (input.len != @sizeOf(AccountFilter))
                std.mem.zeroInit(AccountFilter, .{})
            else
                mem.bytesToValue(
                    AccountFilter,
                    input[0..@sizeOf(AccountFilter)],
                );
        }

        fn get_scan_from_filter(self: *StateMachine, filter: AccountFilter) ?*TransfersGroove.ScanBuilder.Scan {
            const filter_valid =
                filter.account_id != 0 and filter.account_id != std.math.maxInt(u128) and
                filter.timestamp_min != std.math.maxInt(u64) and
                filter.timestamp_max != std.math.maxInt(u64) and
                (filter.timestamp_max == 0 or filter.timestamp_min <= filter.timestamp_max) and
                filter.limit != 0 and
                (filter.flags.credits or filter.flags.debits) and
                filter.flags.padding == 0 and
                stdx.zeroed(&filter.reserved);

            if (!filter_valid) return null;

            const transfers_groove: *TransfersGroove = &self.forest.grooves.transfers;
            const scan_builder: *TransfersGroove.ScanBuilder = &transfers_groove.scan_builder;

            const timestamp_range: TimestampRange = .{
                .min = if (filter.timestamp_min == 0)
                    TimestampRange.timestamp_min
                else
                    filter.timestamp_min,

                .max = if (filter.timestamp_max == 0)
                    TimestampRange.timestamp_max
                else
                    filter.timestamp_max,
            };

            // This query may have 2 conditions:
            // `WHERE debit_account_id = $account_id OR credit_account_id = $account_id`.
            var scan_conditions: stdx.BoundedArray(*TransfersGroove.ScanBuilder.Scan, 2) = .{};
            const direction: Direction = if (filter.flags.reversed) .descending else .ascending;

            // Adding the condition for `debit_account_id = $account_id`.
            if (filter.flags.debits) {
                scan_conditions.append_assume_capacity(scan_builder.scan_prefix(
                    .debit_account_id,
                    self.forest.scan_buffer_pool.acquire_assume_capacity(),
                    Snapshot.latest,
                    filter.account_id,
                    timestamp_range,
                    direction,
                ));
            }

            // Adding the condition for `credit_account_id = $account_id`.
            if (filter.flags.credits) {
                scan_conditions.append_assume_capacity(scan_builder.scan_prefix(
                    .credit_account_id,
                    self.forest.scan_buffer_pool.acquire_assume_capacity(),
                    Snapshot.latest,
                    filter.account_id,
                    timestamp_range,
                    direction,
                ));
            }

            return switch (scan_conditions.count()) {
                1 => scan_conditions.get(0),
                // Creating an union `OR` with the conditions.
                2 => scan_builder.merge_union(scan_conditions.const_slice()),
                else => unreachable,
            };
        }

        fn prefetch_scan_next_tick_callback(completion: *Grid.NextTick) void {
            const self: *StateMachine = @fieldParentPtr(StateMachine, "scan_next_tick", completion);
            self.scan_lookup = .null;

            self.prefetch_finish();
        }

        pub fn commit(
            self: *StateMachine,
            client: u128,
            op: u64,
            timestamp: u64,
            operation: Operation,
            input: []align(16) const u8,
            output: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            _ = client;
            assert(op != 0);
            assert(timestamp > self.commit_timestamp or global_constants.aof_recovery);

            tracer.start(
                &self.tracer_slot,
                .state_machine_commit,
                @src(),
            );

            const result = switch (operation) {
                .create_accounts => self.execute(.create_accounts, timestamp, input, output),
                .create_transfers => self.execute(.create_transfers, timestamp, input, output),
                .lookup_accounts => self.execute_lookup_accounts(input, output),
                .lookup_transfers => self.execute_lookup_transfers(input, output),
                .get_account_transfers => self.execute_get_account_transfers(input, output),
                .get_account_history => self.execute_get_account_history(input, output),
            };

            tracer.end(
                &self.tracer_slot,
                .state_machine_commit,
            );

            return result;
        }

        pub fn compact(self: *StateMachine, callback: *const fn (*StateMachine) void, op: u64) void {
            assert(self.compact_callback == null);
            assert(self.checkpoint_callback == null);

            tracer.start(
                &self.tracer_slot,
                .state_machine_compact,
                @src(),
            );

            self.compact_callback = callback;
            self.forest.compact(compact_finish, op);
        }

        fn compact_finish(forest: *Forest) void {
            const self = @fieldParentPtr(StateMachine, "forest", forest);
            const callback = self.compact_callback.?;
            self.compact_callback = null;

            tracer.end(
                &self.tracer_slot,
                .state_machine_compact,
            );

            callback(self);
        }

        pub fn checkpoint(self: *StateMachine, callback: *const fn (*StateMachine) void) void {
            assert(self.compact_callback == null);
            assert(self.checkpoint_callback == null);

            self.checkpoint_callback = callback;
            self.forest.checkpoint(checkpoint_finish);
        }

        fn checkpoint_finish(forest: *Forest) void {
            const self = @fieldParentPtr(StateMachine, "forest", forest);
            const callback = self.checkpoint_callback.?;
            self.checkpoint_callback = null;
            callback(self);
        }

        fn scope_open(self: *StateMachine, operation: Operation) void {
            switch (operation) {
                .create_accounts => {
                    self.forest.grooves.accounts.scope_open();
                },
                .create_transfers => {
                    self.forest.grooves.accounts.scope_open();
                    self.forest.grooves.transfers.scope_open();
                    self.forest.grooves.posted.scope_open();
                    self.forest.grooves.account_history.scope_open();
                },
                else => unreachable,
            }
        }

        fn scope_close(self: *StateMachine, operation: Operation, mode: ScopeCloseMode) void {
            switch (operation) {
                .create_accounts => {
                    self.forest.grooves.accounts.scope_close(mode);
                },
                .create_transfers => {
                    self.forest.grooves.accounts.scope_close(mode);
                    self.forest.grooves.transfers.scope_close(mode);
                    self.forest.grooves.posted.scope_close(mode);
                    self.forest.grooves.account_history.scope_close(mode);
                },
                else => unreachable,
            }
        }

        fn execute(
            self: *StateMachine,
            comptime operation: Operation,
            timestamp: u64,
            input: []align(16) const u8,
            output: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            comptime assert(operation == .create_accounts or operation == .create_transfers);

            const events = mem.bytesAsSlice(Event(operation), input);
            var results = mem.bytesAsSlice(Result(operation), output);
            var count: usize = 0;

            var chain: ?usize = null;
            var chain_broken = false;

            for (events, 0..) |*event_, index| {
                var event = event_.*;

                const result = blk: {
                    if (event.flags.linked) {
                        if (chain == null) {
                            chain = index;
                            assert(chain_broken == false);
                            self.scope_open(operation);
                        }

                        if (index == events.len - 1) break :blk .linked_event_chain_open;
                    }

                    if (chain_broken) break :blk .linked_event_failed;
                    if (event.timestamp != 0) break :blk .timestamp_must_be_zero;

                    event.timestamp = timestamp - events.len + index + 1;

                    break :blk switch (operation) {
                        .create_accounts => self.create_account(&event),
                        .create_transfers => self.create_transfer(&event),
                        else => unreachable,
                    };
                };
                log.debug("{?}: {s} {}/{}: {}: {}", .{
                    self.forest.grid.superblock.replica_index,
                    @tagName(operation),
                    index + 1,
                    events.len,
                    result,
                    event,
                });
                if (result != .ok) {
                    if (chain) |chain_start_index| {
                        if (!chain_broken) {
                            chain_broken = true;
                            // Our chain has just been broken, discard the scope we started above.
                            self.scope_close(operation, .discard);

                            // Add errors for rolled back events in FIFO order:
                            var chain_index = chain_start_index;
                            while (chain_index < index) : (chain_index += 1) {
                                results[count] = .{
                                    .index = @intCast(chain_index),
                                    .result = .linked_event_failed,
                                };
                                count += 1;
                            }
                        } else {
                            assert(result == .linked_event_failed or result == .linked_event_chain_open);
                        }
                    }
                    results[count] = .{ .index = @intCast(index), .result = result };
                    count += 1;
                }
                if (chain != null and (!event.flags.linked or result == .linked_event_chain_open)) {
                    if (!chain_broken) {
                        // We've finished this linked chain, and all events have applied successfully.
                        self.scope_close(operation, .persist);
                    }

                    chain = null;
                    chain_broken = false;
                }
            }
            assert(chain == null);
            assert(chain_broken == false);

            return @sizeOf(Result(operation)) * count;
        }

        // Accounts that do not fit in the response are omitted.
        fn execute_lookup_accounts(
            self: *StateMachine,
            input: []const u8,
            output: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            const batch = mem.bytesAsSlice(u128, input);
            const output_len = @divFloor(output.len, @sizeOf(Account)) * @sizeOf(Account);
            const results = mem.bytesAsSlice(Account, output[0..output_len]);
            var results_count: usize = 0;
            for (batch) |id| {
                if (self.forest.grooves.accounts.get(id)) |account| {
                    results[results_count] = account.*;
                    results_count += 1;
                }
            }
            return results_count * @sizeOf(Account);
        }

        // Transfers that do not fit in the response are omitted.
        fn execute_lookup_transfers(
            self: *StateMachine,
            input: []const u8,
            output: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            const batch = mem.bytesAsSlice(u128, input);
            const output_len = @divFloor(output.len, @sizeOf(Transfer)) * @sizeOf(Transfer);
            const results = mem.bytesAsSlice(Transfer, output[0..output_len]);
            var results_count: usize = 0;
            for (batch) |id| {
                if (self.get_transfer(id)) |result| {
                    results[results_count] = result.*;
                    results_count += 1;
                }
            }
            return results_count * @sizeOf(Transfer);
        }

        fn execute_get_account_transfers(
            self: *StateMachine,
            input: []const u8,
            output: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            _ = input;
            assert(self.scan_result_count <= constants.batch_max.get_account_transfers);
            if (self.scan_result_count == 0) return 0;
            defer self.scan_result_count = 0;

            const result_size: usize = self.scan_result_count * @sizeOf(Transfer);
            stdx.copy_disjoint(
                .exact,
                u8,
                output[0..result_size],
                self.scan_buffer[0..result_size],
            );

            return result_size;
        }

        fn execute_get_account_history(
            self: *StateMachine,
            input: []const u8,
            output: *align(16) [constants.message_body_size_max]u8,
        ) usize {
            assert(self.scan_result_count <= constants.batch_max.get_account_history);
            if (self.scan_result_count == 0) return 0;
            defer self.scan_result_count = 0;

            const filter: AccountFilter = mem.bytesToValue(
                AccountFilter,
                input[0..@sizeOf(AccountFilter)],
            );

            const scan_results: []const AccountHistoryGrooveValue = mem.bytesAsSlice(
                AccountHistoryGrooveValue,
                self.scan_buffer[0 .. self.scan_result_count * @sizeOf(AccountHistoryGrooveValue)],
            );

            const output_slice: []AccountBalance = mem.bytesAsSlice(AccountBalance, output);
            var output_count: usize = 0;

            for (scan_results) |*result| {
                assert(result.dr_account_id != result.cr_account_id);

                output_slice[output_count] = if (filter.account_id == result.dr_account_id) .{
                    .timestamp = result.timestamp,
                    .debits_pending = result.dr_debits_pending,
                    .debits_posted = result.dr_debits_posted,
                    .credits_pending = result.dr_credits_pending,
                    .credits_posted = result.dr_credits_posted,
                } else if (filter.account_id == result.cr_account_id) .{
                    .timestamp = result.timestamp,
                    .debits_pending = result.cr_debits_pending,
                    .debits_posted = result.cr_debits_posted,
                    .credits_pending = result.cr_credits_pending,
                    .credits_posted = result.cr_credits_posted,
                } else {
                    // We have checked that this account has `flags.history == true`.
                    unreachable;
                };

                output_count += 1;
            }

            assert(output_count == self.scan_result_count);
            return output_count * @sizeOf(AccountBalance);
        }

        fn create_account(self: *StateMachine, a: *const Account) CreateAccountResult {
            assert(a.timestamp > self.commit_timestamp or global_constants.aof_recovery);

            if (a.reserved != 0) return .reserved_field;
            if (a.flags.padding != 0) return .reserved_flag;

            if (a.id == 0) return .id_must_not_be_zero;
            if (a.id == math.maxInt(u128)) return .id_must_not_be_int_max;

            if (a.flags.debits_must_not_exceed_credits and a.flags.credits_must_not_exceed_debits) {
                return .flags_are_mutually_exclusive;
            }

            if (a.debits_pending != 0) return .debits_pending_must_be_zero;
            if (a.debits_posted != 0) return .debits_posted_must_be_zero;
            if (a.credits_pending != 0) return .credits_pending_must_be_zero;
            if (a.credits_posted != 0) return .credits_posted_must_be_zero;
            if (a.ledger == 0) return .ledger_must_not_be_zero;
            if (a.code == 0) return .code_must_not_be_zero;

            if (self.forest.grooves.accounts.get(a.id)) |e| {
                return create_account_exists(a, e);
            }

            self.forest.grooves.accounts.insert(a);
            self.commit_timestamp = a.timestamp;
            return .ok;
        }

        fn create_account_exists(a: *const Account, e: *const Account) CreateAccountResult {
            assert(a.id == e.id);
            if (@as(u16, @bitCast(a.flags)) != @as(u16, @bitCast(e.flags))) return .exists_with_different_flags;
            if (a.user_data_128 != e.user_data_128) return .exists_with_different_user_data_128;
            if (a.user_data_64 != e.user_data_64) return .exists_with_different_user_data_64;
            if (a.user_data_32 != e.user_data_32) return .exists_with_different_user_data_32;
            assert(a.reserved == 0 and e.reserved == 0);
            if (a.ledger != e.ledger) return .exists_with_different_ledger;
            if (a.code != e.code) return .exists_with_different_code;
            return .exists;
        }

        fn create_transfer(self: *StateMachine, t: *const Transfer) CreateTransferResult {
            assert(t.timestamp > self.commit_timestamp or global_constants.aof_recovery);

            if (t.flags.padding != 0) return .reserved_flag;

            if (t.id == 0) return .id_must_not_be_zero;
            if (t.id == math.maxInt(u128)) return .id_must_not_be_int_max;

            if (t.flags.post_pending_transfer or t.flags.void_pending_transfer) {
                return self.post_or_void_pending_transfer(t);
            }

            if (t.debit_account_id == 0) return .debit_account_id_must_not_be_zero;
            if (t.debit_account_id == math.maxInt(u128)) return .debit_account_id_must_not_be_int_max;
            if (t.credit_account_id == 0) return .credit_account_id_must_not_be_zero;
            if (t.credit_account_id == math.maxInt(u128)) return .credit_account_id_must_not_be_int_max;
            if (t.credit_account_id == t.debit_account_id) return .accounts_must_be_different;

            if (t.pending_id != 0) return .pending_id_must_be_zero;
            if (!t.flags.pending) {
                if (t.timeout != 0) return .timeout_reserved_for_pending_transfer;
            }
            if (!t.flags.balancing_debit and !t.flags.balancing_credit) {
                if (t.amount == 0) return .amount_must_not_be_zero;
            }

            if (t.ledger == 0) return .ledger_must_not_be_zero;
            if (t.code == 0) return .code_must_not_be_zero;

            // The etymology of the DR and CR abbreviations for debit/credit is interesting, either:
            // 1. derived from the Latin past participles of debitum/creditum, i.e. debere/credere,
            // 2. standing for debit record and credit record, or
            // 3. relating to debtor and creditor.
            // We use them to distinguish between `cr` (credit account), and `c` (commit).
            const dr_account = self.forest.grooves.accounts.get(t.debit_account_id) orelse return .debit_account_not_found;
            const cr_account = self.forest.grooves.accounts.get(t.credit_account_id) orelse return .credit_account_not_found;
            assert(dr_account.id == t.debit_account_id);
            assert(cr_account.id == t.credit_account_id);
            assert(t.timestamp > dr_account.timestamp);
            assert(t.timestamp > cr_account.timestamp);

            if (dr_account.ledger != cr_account.ledger) return .accounts_must_have_the_same_ledger;
            if (t.ledger != dr_account.ledger) return .transfer_must_have_the_same_ledger_as_accounts;

            // If the transfer already exists, then it must not influence the overflow or limit checks.
            if (self.get_transfer(t.id)) |e| return create_transfer_exists(t, e);

            const amount = amount: {
                var amount = t.amount;
                if (t.flags.balancing_debit or t.flags.balancing_credit) {
                    if (amount == 0) amount = std.math.maxInt(u64);
                } else {
                    assert(amount != 0);
                }

                if (t.flags.balancing_debit) {
                    const dr_balance = dr_account.debits_posted + dr_account.debits_pending;
                    amount = @min(amount, dr_account.credits_posted -| dr_balance);
                    if (amount == 0) return .exceeds_credits;
                }

                if (t.flags.balancing_credit) {
                    const cr_balance = cr_account.credits_posted + cr_account.credits_pending;
                    amount = @min(amount, cr_account.debits_posted -| cr_balance);
                    if (amount == 0) return .exceeds_debits;
                }
                break :amount amount;
            };

            if (t.flags.pending) {
                if (sum_overflows(u128, amount, dr_account.debits_pending)) return .overflows_debits_pending;
                if (sum_overflows(u128, amount, cr_account.credits_pending)) return .overflows_credits_pending;
            }
            if (sum_overflows(u128, amount, dr_account.debits_posted)) return .overflows_debits_posted;
            if (sum_overflows(u128, amount, cr_account.credits_posted)) return .overflows_credits_posted;
            // We assert that the sum of the pending and posted balances can never overflow:
            if (sum_overflows(u128, amount, dr_account.debits_pending + dr_account.debits_posted)) {
                return .overflows_debits;
            }
            if (sum_overflows(u128, amount, cr_account.credits_pending + cr_account.credits_posted)) {
                return .overflows_credits;
            }

            if (sum_overflows(u64, t.timestamp, @as(u64, @intCast(t.timeout)) * std.time.ns_per_s)) return .overflows_timeout;
            if (dr_account.debits_exceed_credits(amount)) return .exceeds_credits;
            if (cr_account.credits_exceed_debits(amount)) return .exceeds_debits;

            var t2 = t.*;
            t2.amount = amount;
            self.forest.grooves.transfers.insert(&t2);

            var dr_account_new = dr_account.*;
            var cr_account_new = cr_account.*;
            if (t.flags.pending) {
                dr_account_new.debits_pending += amount;
                cr_account_new.credits_pending += amount;
            } else {
                dr_account_new.debits_posted += amount;
                cr_account_new.credits_posted += amount;
            }
            self.forest.grooves.accounts.update(.{ .old = dr_account, .new = &dr_account_new });
            self.forest.grooves.accounts.update(.{ .old = cr_account, .new = &cr_account_new });

            if (dr_account_new.flags.history or cr_account_new.flags.history) {
                var history = std.mem.zeroInit(AccountHistoryGrooveValue, .{
                    .timestamp = t2.timestamp,
                });

                if (dr_account_new.flags.history) {
                    history.dr_account_id = dr_account_new.id;
                    history.dr_debits_pending = dr_account_new.debits_pending;
                    history.dr_debits_posted = dr_account_new.debits_posted;
                    history.dr_credits_pending = dr_account_new.credits_pending;
                    history.dr_credits_posted = dr_account_new.credits_posted;
                }

                if (cr_account_new.flags.history) {
                    history.cr_account_id = cr_account_new.id;
                    history.cr_debits_pending = cr_account_new.debits_pending;
                    history.cr_debits_posted = cr_account_new.debits_posted;
                    history.cr_credits_pending = cr_account_new.credits_pending;
                    history.cr_credits_posted = cr_account_new.credits_posted;
                }

                self.forest.grooves.account_history.insert(&history);
            }

            self.commit_timestamp = t.timestamp;
            return .ok;
        }

        fn create_transfer_exists(t: *const Transfer, e: *const Transfer) CreateTransferResult {
            assert(t.id == e.id);
            // The flags change the behavior of the remaining comparisons, so compare the flags first.
            if (@as(u16, @bitCast(t.flags)) != @as(u16, @bitCast(e.flags))) return .exists_with_different_flags;
            if (t.debit_account_id != e.debit_account_id) {
                return .exists_with_different_debit_account_id;
            }
            if (t.credit_account_id != e.credit_account_id) {
                return .exists_with_different_credit_account_id;
            }
            if (t.amount != e.amount) return .exists_with_different_amount;
            assert(t.pending_id == 0 and e.pending_id == 0); // We know that the flags are the same.
            if (t.user_data_128 != e.user_data_128) return .exists_with_different_user_data_128;
            if (t.user_data_64 != e.user_data_64) return .exists_with_different_user_data_64;
            if (t.user_data_32 != e.user_data_32) return .exists_with_different_user_data_32;
            if (t.timeout != e.timeout) return .exists_with_different_timeout;
            assert(t.ledger == e.ledger); // If the accounts are the same, the ledger must be the same.
            if (t.code != e.code) return .exists_with_different_code;
            return .exists;
        }

        fn post_or_void_pending_transfer(self: *StateMachine, t: *const Transfer) CreateTransferResult {
            assert(t.id != 0);
            assert(t.flags.padding == 0);
            assert(t.timestamp > self.commit_timestamp);
            assert(t.flags.post_pending_transfer or t.flags.void_pending_transfer);

            if (t.flags.post_pending_transfer and t.flags.void_pending_transfer) {
                return .flags_are_mutually_exclusive;
            }
            if (t.flags.pending) return .flags_are_mutually_exclusive;
            if (t.flags.balancing_debit) return .flags_are_mutually_exclusive;
            if (t.flags.balancing_credit) return .flags_are_mutually_exclusive;

            if (t.pending_id == 0) return .pending_id_must_not_be_zero;
            if (t.pending_id == math.maxInt(u128)) return .pending_id_must_not_be_int_max;
            if (t.pending_id == t.id) return .pending_id_must_be_different;
            if (t.timeout != 0) return .timeout_reserved_for_pending_transfer;

            const p = self.get_transfer(t.pending_id) orelse return .pending_transfer_not_found;
            assert(p.id == t.pending_id);
            if (!p.flags.pending) return .pending_transfer_not_pending;

            const dr_account = self.forest.grooves.accounts.get(p.debit_account_id).?;
            const cr_account = self.forest.grooves.accounts.get(p.credit_account_id).?;
            assert(dr_account.id == p.debit_account_id);
            assert(cr_account.id == p.credit_account_id);
            assert(p.timestamp > dr_account.timestamp);
            assert(p.timestamp > cr_account.timestamp);
            assert(p.amount > 0);

            if (t.debit_account_id > 0 and t.debit_account_id != p.debit_account_id) {
                return .pending_transfer_has_different_debit_account_id;
            }
            if (t.credit_account_id > 0 and t.credit_account_id != p.credit_account_id) {
                return .pending_transfer_has_different_credit_account_id;
            }
            // The user_data field is allowed to differ across pending and posting/voiding transfers.
            if (t.ledger > 0 and t.ledger != p.ledger) return .pending_transfer_has_different_ledger;
            if (t.code > 0 and t.code != p.code) return .pending_transfer_has_different_code;

            const amount = if (t.amount > 0) t.amount else p.amount;
            if (amount > p.amount) return .exceeds_pending_transfer_amount;

            if (t.flags.void_pending_transfer and amount < p.amount) {
                return .pending_transfer_has_different_amount;
            }

            if (self.get_transfer(t.id)) |e| return post_or_void_pending_transfer_exists(t, e, p);

            if (self.get_posted(p.timestamp)) |posted| {
                switch (posted.fulfillment) {
                    .posted => return .pending_transfer_already_posted,
                    .voided => return .pending_transfer_already_voided,
                }
            }

            assert(p.timestamp < t.timestamp);
            if (p.timeout > 0) {
                // TODO: After implementing the actual transfer expiration,
                // this check could be turned into an assertion.
                const timeout_ns = @as(u64, @intCast(p.timeout)) * std.time.ns_per_s;
                if (t.timestamp >= p.timestamp + timeout_ns) return .pending_transfer_expired;
            }

            self.forest.grooves.transfers.insert(&Transfer{
                .id = t.id,
                .debit_account_id = p.debit_account_id,
                .credit_account_id = p.credit_account_id,
                .user_data_128 = if (t.user_data_128 > 0) t.user_data_128 else p.user_data_128,
                .user_data_64 = if (t.user_data_64 > 0) t.user_data_64 else p.user_data_64,
                .user_data_32 = if (t.user_data_32 > 0) t.user_data_32 else p.user_data_32,
                .ledger = p.ledger,
                .code = p.code,
                .pending_id = t.pending_id,
                .timeout = 0,
                .timestamp = t.timestamp,
                .flags = t.flags,
                .amount = amount,
            });

            self.forest.grooves.posted.insert(&PostedGrooveValue{
                .timestamp = p.timestamp,
                .fulfillment = fulfillment: {
                    if (t.flags.post_pending_transfer) break :fulfillment .posted;
                    if (t.flags.void_pending_transfer) break :fulfillment .voided;
                    unreachable;
                },
                .padding = [_]u8{0} ** 7,
            });

            var dr_account_new = dr_account.*;
            var cr_account_new = cr_account.*;
            dr_account_new.debits_pending -= p.amount;
            cr_account_new.credits_pending -= p.amount;

            if (t.flags.post_pending_transfer) {
                assert(amount > 0);
                assert(amount <= p.amount);
                dr_account_new.debits_posted += amount;
                cr_account_new.credits_posted += amount;
            }

            self.forest.grooves.accounts.update(.{ .old = dr_account, .new = &dr_account_new });
            self.forest.grooves.accounts.update(.{ .old = cr_account, .new = &cr_account_new });

            self.commit_timestamp = t.timestamp;
            return .ok;
        }

        fn post_or_void_pending_transfer_exists(
            t: *const Transfer,
            e: *const Transfer,
            p: *const Transfer,
        ) CreateTransferResult {
            assert(t.id == e.id);
            assert(t.id != p.id);
            assert(p.flags.pending);
            assert(t.pending_id == p.id);

            // Do not assume that `e` is necessarily a posting or voiding transfer.
            if (@as(u16, @bitCast(t.flags)) != @as(u16, @bitCast(e.flags))) {
                return .exists_with_different_flags;
            }

            if (t.amount == 0) {
                if (e.amount != p.amount) return .exists_with_different_amount;
            } else {
                if (t.amount != e.amount) return .exists_with_different_amount;
            }

            // If `e` posted or voided a different pending transfer, then the accounts will differ.
            if (t.pending_id != e.pending_id) return .exists_with_different_pending_id;

            assert(e.flags.post_pending_transfer or e.flags.void_pending_transfer);
            assert(e.debit_account_id == p.debit_account_id);
            assert(e.credit_account_id == p.credit_account_id);
            assert(e.pending_id == p.id);
            assert(e.timeout == 0);
            assert(e.ledger == p.ledger);
            assert(e.code == p.code);
            assert(e.timestamp > p.timestamp);

            assert(t.flags.post_pending_transfer == e.flags.post_pending_transfer);
            assert(t.flags.void_pending_transfer == e.flags.void_pending_transfer);
            assert(t.debit_account_id == 0 or t.debit_account_id == e.debit_account_id);
            assert(t.credit_account_id == 0 or t.credit_account_id == e.credit_account_id);
            assert(t.timeout == 0);
            assert(t.ledger == 0 or t.ledger == e.ledger);
            assert(t.code == 0 or t.code == e.code);
            assert(t.timestamp > e.timestamp);

            if (t.user_data_128 == 0) {
                if (e.user_data_128 != p.user_data_128) return .exists_with_different_user_data_128;
            } else {
                if (t.user_data_128 != e.user_data_128) return .exists_with_different_user_data_128;
            }

            if (t.user_data_64 == 0) {
                if (e.user_data_64 != p.user_data_64) return .exists_with_different_user_data_64;
            } else {
                if (t.user_data_64 != e.user_data_64) return .exists_with_different_user_data_64;
            }

            if (t.user_data_32 == 0) {
                if (e.user_data_32 != p.user_data_32) return .exists_with_different_user_data_32;
            } else {
                if (t.user_data_32 != e.user_data_32) return .exists_with_different_user_data_32;
            }

            return .exists;
        }

        fn get_transfer(self: *const StateMachine, id: u128) ?*const Transfer {
            return self.forest.grooves.transfers.get(id);
        }

        /// Returns whether a pending transfer, if it exists, has already been posted or voided.
        fn get_posted(
            self: *const StateMachine,
            pending_timestamp: u64,
        ) ?*const PostedGrooveValue {
            return self.forest.grooves.posted.get(pending_timestamp);
        }

        pub fn forest_options(options: Options) Forest.GroovesOptions {
            const batch_accounts_max: u32 = @intCast(constants.batch_max.create_accounts);
            const batch_transfers_max: u32 = @intCast(constants.batch_max.create_transfers);
            assert(batch_accounts_max == constants.batch_max.lookup_accounts);
            assert(batch_transfers_max == constants.batch_max.lookup_transfers);

            return .{
                .accounts = .{
                    // lookup_account() looks up 1 AccountImmutable per item.
                    .prefetch_entries_for_read_max = batch_accounts_max,
                    .prefetch_entries_for_update_max = @max(
                        batch_accounts_max, // create_account()
                        2 * batch_transfers_max, // create_transfer(), debit and credit accounts
                    ),
                    .cache_entries_max = options.cache_entries_accounts,
                    .tree_options_object = .{},
                    .tree_options_id = .{},
                    .tree_options_index = .{
                        .user_data_128 = .{},
                        .user_data_64 = .{},
                        .user_data_32 = .{},
                        .ledger = .{},
                        .code = .{},
                    },
                },
                .transfers = .{
                    // lookup_transfer() looks up 1 Transfer.
                    // create_transfer() looks up at most 1 Transfer for posting/voiding.
                    .prefetch_entries_for_read_max = batch_transfers_max,
                    // create_transfer() updates a single Transfer.
                    .prefetch_entries_for_update_max = batch_transfers_max,
                    .cache_entries_max = options.cache_entries_transfers,
                    .tree_options_object = .{},
                    .tree_options_id = .{},
                    .tree_options_index = .{
                        .debit_account_id = .{},
                        .credit_account_id = .{},
                        .user_data_128 = .{},
                        .user_data_64 = .{},
                        .user_data_32 = .{},
                        .pending_id = .{},
                        .timeout = .{},
                        .ledger = .{},
                        .code = .{},
                        .amount = .{},
                    },
                },
                .posted = .{
                    // Nothing lookups posted groove.
                    .prefetch_entries_for_read_max = 0,
                    // create_transfer() posts/voids at most one transfer.
                    .prefetch_entries_for_update_max = batch_transfers_max,
                    .cache_entries_max = options.cache_entries_posted,
                    .tree_options_object = .{},
                    .tree_options_id = {},
                    .tree_options_index = .{},
                },
                .account_history = .{
                    .prefetch_entries_for_read_max = 0,
                    .prefetch_entries_for_update_max = batch_transfers_max,
                    .cache_entries_max = options.cache_entries_account_history,
                    .tree_options_object = .{},
                    .tree_options_id = {},
                    .tree_options_index = .{},
                },
            };
        }
    };
}

fn sum_overflows(comptime Int: type, a: Int, b: Int) bool {
    comptime assert(Int != comptime_int);
    comptime assert(Int != comptime_float);
    _ = std.math.add(Int, a, b) catch return true;
    return false;
}

const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const expectEqualSlices = testing.expectEqualSlices;

fn sum_overflows_test(comptime Int: type) !void {
    try expectEqual(false, sum_overflows(Int, math.maxInt(Int), 0));
    try expectEqual(false, sum_overflows(Int, math.maxInt(Int) - 1, 1));
    try expectEqual(false, sum_overflows(Int, 1, math.maxInt(Int) - 1));

    try expectEqual(true, sum_overflows(Int, math.maxInt(Int), 1));
    try expectEqual(true, sum_overflows(Int, 1, math.maxInt(Int)));

    try expectEqual(true, sum_overflows(Int, math.maxInt(Int), math.maxInt(Int)));
    try expectEqual(true, sum_overflows(Int, math.maxInt(Int), math.maxInt(Int)));
}

test "sum_overflows" {
    try sum_overflows_test(u64);
    try sum_overflows_test(u128);
}

const TestContext = struct {
    const Storage = @import("testing/storage.zig").Storage;
    const data_file_size_min = @import("vsr/superblock.zig").data_file_size_min;
    const SuperBlock = @import("vsr/superblock.zig").SuperBlockType(Storage);
    const Grid = @import("vsr/grid.zig").GridType(Storage);
    const StateMachine = StateMachineType(Storage, .{
        // Overestimate the batch size because the test never compacts.
        .message_body_size_max = TestContext.message_body_size_max,
        .lsm_batch_multiple = 1,
        .vsr_operations_reserved = 128,
    });
    const message_body_size_max = 32 * @sizeOf(Account);

    storage: Storage,
    superblock: SuperBlock,
    grid: Grid,
    state_machine: StateMachine,

    fn init(ctx: *TestContext, allocator: mem.Allocator) !void {
        ctx.storage = try Storage.init(
            allocator,
            4096,
            .{
                .read_latency_min = 0,
                .read_latency_mean = 0,
                .write_latency_min = 0,
                .write_latency_mean = 0,
            },
        );
        errdefer ctx.storage.deinit(allocator);

        ctx.superblock = try SuperBlock.init(allocator, .{
            .storage = &ctx.storage,
            .storage_size_limit = data_file_size_min,
        });
        errdefer ctx.superblock.deinit(allocator);

        // Pretend that the superblock is open so that the Forest can initialize.
        ctx.superblock.opened = true;
        ctx.superblock.working.vsr_state.checkpoint.commit_min = 0;

        ctx.grid = try Grid.init(allocator, .{
            .superblock = &ctx.superblock,
            .missing_blocks_max = 0,
            .missing_tables_max = 0,
        });
        errdefer ctx.grid.deinit(allocator);

        ctx.state_machine = try StateMachine.init(allocator, &ctx.grid, .{
            .lsm_forest_node_count = 1,
            .cache_entries_accounts = 2048,
            .cache_entries_transfers = 2048,
            .cache_entries_posted = 2048,
            .cache_entries_account_history = 2048,
        });
        errdefer ctx.state_machine.deinit(allocator);
    }

    fn deinit(ctx: *TestContext, allocator: mem.Allocator) void {
        ctx.storage.deinit(allocator);
        ctx.superblock.deinit(allocator);
        ctx.grid.deinit(allocator);
        ctx.state_machine.deinit(allocator);
        ctx.* = undefined;
    }
};

const TestAction = union(enum) {
    // Set the account's balance.
    setup: struct {
        account: u128,
        debits_pending: u128,
        debits_posted: u128,
        credits_pending: u128,
        credits_posted: u128,
    },

    tick: u64,

    commit: TestContext.StateMachine.Operation,
    account: TestCreateAccount,
    transfer: TestCreateTransfer,

    lookup_account: struct {
        id: u128,
        balance: ?struct {
            debits_pending: u128,
            debits_posted: u128,
            credits_pending: u128,
            credits_posted: u128,
        } = null,
    },
    lookup_transfer: struct {
        id: u128,
        data: union(enum) {
            exists: bool,
            amount: u128,
        },
    },
};

const TestCreateAccount = struct {
    id: u128,
    debits_pending: u128 = 0,
    debits_posted: u128 = 0,
    credits_pending: u128 = 0,
    credits_posted: u128 = 0,
    user_data_128: u128 = 0,
    user_data_64: u64 = 0,
    user_data_32: u32 = 0,
    reserved: u1 = 0,
    ledger: u32,
    code: u16,
    flags_linked: ?enum { LNK } = null,
    flags_debits_must_not_exceed_credits: ?enum { @"D<C" } = null,
    flags_credits_must_not_exceed_debits: ?enum { @"C<D" } = null,
    flags_padding: u12 = 0,
    timestamp: u64 = 0,
    result: CreateAccountResult,

    fn event(a: TestCreateAccount) Account {
        return .{
            .id = a.id,
            .debits_pending = a.debits_pending,
            .debits_posted = a.debits_posted,
            .credits_pending = a.credits_pending,
            .credits_posted = a.credits_posted,
            .user_data_128 = a.user_data_128,
            .user_data_64 = a.user_data_64,
            .user_data_32 = a.user_data_32,
            .reserved = a.reserved,
            .ledger = a.ledger,
            .code = a.code,
            .flags = .{
                .linked = a.flags_linked != null,
                .debits_must_not_exceed_credits = a.flags_debits_must_not_exceed_credits != null,
                .credits_must_not_exceed_debits = a.flags_credits_must_not_exceed_debits != null,
                .padding = a.flags_padding,
            },
            .timestamp = a.timestamp,
        };
    }
};

const TestCreateTransfer = struct {
    id: u128,
    debit_account_id: u128,
    credit_account_id: u128,
    amount: u128 = 0,
    pending_id: u128 = 0,
    user_data_128: u128 = 0,
    user_data_64: u64 = 0,
    user_data_32: u32 = 0,
    timeout: u32 = 0,
    ledger: u32,
    code: u16,
    flags_linked: ?enum { LNK } = null,
    flags_pending: ?enum { PEN } = null,
    flags_post_pending_transfer: ?enum { POS } = null,
    flags_void_pending_transfer: ?enum { VOI } = null,
    flags_balancing_debit: ?enum { BDR } = null,
    flags_balancing_credit: ?enum { BCR } = null,
    flags_padding: u10 = 0,
    timestamp: u64 = 0,
    result: CreateTransferResult,

    fn event(t: TestCreateTransfer) Transfer {
        return .{
            .id = t.id,
            .debit_account_id = t.debit_account_id,
            .credit_account_id = t.credit_account_id,
            .amount = t.amount,
            .pending_id = t.pending_id,
            .user_data_128 = t.user_data_128,
            .user_data_64 = t.user_data_64,
            .user_data_32 = t.user_data_32,
            .timeout = t.timeout,
            .ledger = t.ledger,
            .code = t.code,
            .flags = .{
                .linked = t.flags_linked != null,
                .pending = t.flags_pending != null,
                .post_pending_transfer = t.flags_post_pending_transfer != null,
                .void_pending_transfer = t.flags_void_pending_transfer != null,
                .balancing_debit = t.flags_balancing_debit != null,
                .balancing_credit = t.flags_balancing_credit != null,
                .padding = t.flags_padding,
            },
            .timestamp = t.timestamp,
        };
    }
};

fn check(test_table: []const u8) !void {
    const parse_table = @import("testing/table.zig").parse;
    const allocator = std.testing.allocator;

    var context: TestContext = undefined;
    try context.init(allocator);
    defer context.deinit(allocator);

    var accounts = std.AutoHashMap(u128, Account).init(allocator);
    defer accounts.deinit();

    var transfers = std.AutoHashMap(u128, Transfer).init(allocator);
    defer transfers.deinit();

    var request = std.ArrayListAligned(u8, 16).init(allocator);
    defer request.deinit();

    var reply = std.ArrayListAligned(u8, 16).init(allocator);
    defer reply.deinit();

    var operation: ?TestContext.StateMachine.Operation = null;

    const test_actions = parse_table(TestAction, test_table);
    for (test_actions.const_slice()) |test_action| {
        switch (test_action) {
            .setup => |b| {
                assert(operation == null);

                const account = context.state_machine.forest.grooves.accounts.get(b.account).?;
                var account_new = account.*;

                account_new.debits_pending = b.debits_pending;
                account_new.debits_posted = b.debits_posted;
                account_new.credits_pending = b.credits_pending;
                account_new.credits_posted = b.credits_posted;
                if (!stdx.equal_bytes(Account, &account_new, account)) {
                    context.state_machine.forest.grooves.accounts.update(.{
                        .old = account,
                        .new = &account_new,
                    });
                }
            },

            .tick => |ticks| {
                assert(ticks > 0);
                context.state_machine.prepare_timestamp += ticks;
            },

            .account => |a| {
                assert(operation == null or operation.? == .create_accounts);
                operation = .create_accounts;

                const event = a.event();
                try request.appendSlice(std.mem.asBytes(&event));
                if (a.result == .ok) {
                    try accounts.put(a.id, event);
                } else {
                    const result = CreateAccountsResult{
                        .index = @intCast(@divExact(request.items.len, @sizeOf(Account)) - 1),
                        .result = a.result,
                    };
                    try reply.appendSlice(std.mem.asBytes(&result));
                }
            },
            .transfer => |t| {
                assert(operation == null or operation.? == .create_transfers);
                operation = .create_transfers;

                const event = t.event();
                try request.appendSlice(std.mem.asBytes(&event));
                if (t.result == .ok) {
                    try transfers.put(t.id, event);
                } else {
                    const result = CreateTransfersResult{
                        .index = @intCast(@divExact(request.items.len, @sizeOf(Transfer)) - 1),
                        .result = t.result,
                    };
                    try reply.appendSlice(std.mem.asBytes(&result));
                }
            },
            .lookup_account => |a| {
                assert(operation == null or operation.? == .lookup_accounts);
                operation = .lookup_accounts;

                try request.appendSlice(std.mem.asBytes(&a.id));
                if (a.balance) |b| {
                    var account = accounts.get(a.id).?;
                    account.debits_pending = b.debits_pending;
                    account.debits_posted = b.debits_posted;
                    account.credits_pending = b.credits_pending;
                    account.credits_posted = b.credits_posted;
                    try reply.appendSlice(std.mem.asBytes(&account));
                }
            },
            .lookup_transfer => |t| {
                assert(operation == null or operation.? == .lookup_transfers);
                operation = .lookup_transfers;

                try request.appendSlice(std.mem.asBytes(&t.id));
                switch (t.data) {
                    .exists => |exists| {
                        if (exists) {
                            var transfer = transfers.get(t.id).?;
                            try reply.appendSlice(std.mem.asBytes(&transfer));
                        }
                    },
                    .amount => |amount| {
                        var transfer = transfers.get(t.id).?;
                        transfer.amount = amount;
                        try reply.appendSlice(std.mem.asBytes(&transfer));
                    },
                }
            },

            .commit => |commit_operation| {
                assert(operation == null or operation.? == commit_operation);

                context.state_machine.prepare_timestamp += 1;
                context.state_machine.prepare(commit_operation, request.items);
                const timestamp = context.state_machine.prepare_timestamp;

                const reply_actual_buffer = try allocator.alignedAlloc(u8, 16, 4096);
                defer allocator.free(reply_actual_buffer);

                const reply_actual_size = context.state_machine.commit(
                    0,
                    1,
                    timestamp,
                    commit_operation,
                    request.items,
                    reply_actual_buffer[0..TestContext.message_body_size_max],
                );
                const reply_actual = reply_actual_buffer[0..reply_actual_size];

                if (commit_operation == .lookup_accounts) {
                    for (std.mem.bytesAsSlice(Account, reply_actual)) |*a| a.timestamp = 0;
                }

                if (commit_operation == .lookup_transfers) {
                    for (std.mem.bytesAsSlice(Transfer, reply_actual)) |*t| t.timestamp = 0;
                }

                switch (commit_operation) {
                    inline else => |commit_operation_comptime| {
                        const Result = TestContext.StateMachine.Result(commit_operation_comptime);
                        try testing.expectEqualSlices(
                            Result,
                            mem.bytesAsSlice(Result, reply.items),
                            mem.bytesAsSlice(Result, reply_actual),
                        );
                    },
                }

                request.clearRetainingCapacity();
                reply.clearRetainingCapacity();
                operation = null;
            },
        }
    }

    assert(operation == null);
    assert(request.items.len == 0);
    assert(reply.items.len == 0);
}

test "create_accounts" {
    try check(
        \\ account A1  0  0  0  0 U2 U2 U2 _ L3 C4 _   _   _ _ _ ok
        \\ account A0  1  1  1  1  _  _  _ 1 L0 C0 _ D<C C<D 1 1 timestamp_must_be_zero
        \\ account A0  1  1  1  1  _  _  _ 1 L0 C0 _ D<C C<D 1 _ reserved_field
        \\ account A0  1  1  1  1  _  _  _ _ L0 C0 _ D<C C<D 1 _ reserved_flag
        \\ account A0  1  1  1  1  _  _  _ _ L0 C0 _ D<C C<D _ _ id_must_not_be_zero
        \\ account -0  1  1  1  1  _  _  _ _ L0 C0 _ D<C C<D _ _ id_must_not_be_int_max
        \\ account A1  1  1  1  1 U1 U1 U1 _ L0 C0 _ D<C C<D _ _ flags_are_mutually_exclusive
        \\ account A1  1  1  1  1 U1 U1 U1 _ L9 C9 _ D<C   _ _ _ debits_pending_must_be_zero
        \\ account A1  0  1  1  1 U1 U1 U1 _ L9 C9 _ D<C   _ _ _ debits_posted_must_be_zero
        \\ account A1  0  0  1  1 U1 U1 U1 _ L9 C9 _ D<C   _ _ _ credits_pending_must_be_zero
        \\ account A1  0  0  0  1 U1 U1 U1 _ L9 C9 _ D<C   _ _ _ credits_posted_must_be_zero
        \\ account A1  0  0  0  0 U1 U1 U1 _ L0 C0 _ D<C   _ _ _ ledger_must_not_be_zero
        \\ account A1  0  0  0  0 U1 U1 U1 _ L9 C0 _ D<C   _ _ _ code_must_not_be_zero
        \\ account A1  0  0  0  0 U1 U1 U1 _ L9 C9 _ D<C   _ _ _ exists_with_different_flags
        \\ account A1  0  0  0  0 U1 U1 U1 _ L9 C9 _   _ C<D _ _ exists_with_different_flags
        \\ account A1  0  0  0  0 U1 U1 U1 _ L9 C9 _   _   _ _ _ exists_with_different_user_data_128
        \\ account A1  0  0  0  0 U2 U1 U1 _ L9 C9 _   _   _ _ _ exists_with_different_user_data_64
        \\ account A1  0  0  0  0 U2 U2 U1 _ L9 C9 _   _   _ _ _ exists_with_different_user_data_32
        \\ account A1  0  0  0  0 U2 U2 U2 _ L9 C9 _   _   _ _ _ exists_with_different_ledger
        \\ account A1  0  0  0  0 U2 U2 U2 _ L3 C9 _   _   _ _ _ exists_with_different_code
        \\ account A1  0  0  0  0 U2 U2 U2 _ L3 C4 _   _   _ _ _ exists
        \\ commit create_accounts
        \\
        \\ lookup_account -0 _
        \\ lookup_account A0 _
        \\ lookup_account A1 0 0 0 0
        \\ lookup_account A2 _
        \\ commit lookup_accounts
    );
}

test "create_accounts: empty" {
    try check(
        \\ commit create_transfers
    );
}

test "linked accounts" {
    try check(
        \\ account A7  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok // An individual event (successful):

        // A chain of 4 events (the last event in the chain closes the chain with linked=false):
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ linked_event_failed // Commit/rollback.
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ linked_event_failed // Commit/rollback.
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ exists              // Fail with .exists.
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ linked_event_failed // Fail without committing.

        // An individual event (successful):
        // This does not see any effect from the failed chain above.
        \\ account A1 0 0 0 0 _ _ _ _ L1 C1   _ _ _ _ _ ok

        // A chain of 2 events (the first event fails the chain):
        \\ account A1  0  0  0  0  _  _  _ _ L1 C2 LNK   _   _ _ _ exists_with_different_flags
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ linked_event_failed

        // An individual event (successful):
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok

        // A chain of 2 events (the last event fails the chain):
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ linked_event_failed
        \\ account A1  0  0  0  0  _  _  _ _ L2 C1   _   _   _ _ _ exists_with_different_ledger

        // A chain of 2 events (successful):
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ ok
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ commit create_accounts
        \\
        \\ lookup_account A7 0 0 0 0
        \\ lookup_account A1 0 0 0 0
        \\ lookup_account A2 0 0 0 0
        \\ lookup_account A3 0 0 0 0
        \\ lookup_account A4 0 0 0 0
        \\ commit lookup_accounts
    );

    try check(
        \\ account A7  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok // An individual event (successful):

        // A chain of 4 events:
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ linked_event_failed // Commit/rollback.
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ linked_event_failed // Commit/rollback.
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ exists              // Fail with .exists.
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ linked_event_failed // Fail without committing.
        \\ commit create_accounts
        \\
        \\ lookup_account A7 0 0 0 0
        \\ lookup_account A1 _
        \\ lookup_account A2 _
        \\ lookup_account A3 _
        \\ commit lookup_accounts
    );

    // TODO How can we test that events were in fact rolled back in LIFO order?
    // All our rollback handlers appear to be commutative.
}

test "linked_event_chain_open" {
    try check(
    // A chain of 3 events (the last event in the chain closes the chain with linked=false):
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok

        // An open chain of 2 events:
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ linked_event_failed
        \\ account A5  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ linked_event_chain_open
        \\ commit create_accounts
        \\
        \\ lookup_account A1 0 0 0 0
        \\ lookup_account A2 0 0 0 0
        \\ lookup_account A3 0 0 0 0
        \\ lookup_account A4 _
        \\ lookup_account A5 _
        \\ commit lookup_accounts
    );
}

test "linked_event_chain_open for an already failed batch" {
    try check(
    // An individual event (successful):
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok

        // An open chain of 3 events (the second one fails):
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ linked_event_failed
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ exists_with_different_flags
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ linked_event_chain_open
        \\ commit create_accounts
        \\
        \\ lookup_account A1 0 0 0 0
        \\ lookup_account A2 _
        \\ lookup_account A3 _
        \\ commit lookup_accounts
    );
}

test "linked_event_chain_open for a batch of 1" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1 LNK   _   _ _ _ linked_event_chain_open
        \\ commit create_accounts
        \\
        \\ lookup_account A1 _
        \\ commit lookup_accounts
    );
}

// The goal is to ensure that:
// 1. all CreateTransferResult enums are covered, with
// 2. enums tested in the order that they are defined, for easier auditing of coverage, and that
// 3. state machine logic cannot be reordered in any way, breaking determinism.
test "create_transfers/lookup_transfers" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L2 C2   _   _   _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ ok
        \\ account A5  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ ok
        \\ commit create_accounts

        // Set up initial balances.
        \\ setup A1  100   200    0     0
        \\ setup A2    0     0    0     0
        \\ setup A3    0     0  110   210
        \\ setup A4   20  -700    0  -500
        \\ setup A5    0 -1000   10 -1100

        // Bump the state machine time to `maxInt - 3s` for testing timeout overflow.
        \\ tick -3000000000

        // Test errors by descending precedence.
        \\ transfer   T0 A0 A0    0  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _ P1 1 timestamp_must_be_zero
        \\ transfer   T0 A0 A0    0  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _ P1 _ reserved_flag
        \\ transfer   T0 A0 A0    0  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _  _ _ id_must_not_be_zero
        \\ transfer   -0 A0 A0    0  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _  _ _ id_must_not_be_int_max
        \\ transfer   T1 A0 A0    0  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _  _ _ debit_account_id_must_not_be_zero
        \\ transfer   T1 -0 A0    0  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _  _ _ debit_account_id_must_not_be_int_max
        \\ transfer   T1 A8 A0    0  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _  _ _ credit_account_id_must_not_be_zero
        \\ transfer   T1 A8 -0    0  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _  _ _ credit_account_id_must_not_be_int_max
        \\ transfer   T1 A8 A8    0  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _  _ _ accounts_must_be_different
        \\ transfer   T1 A8 A9    0  T1  _  _  _    _ L0 C0   _ PEN   _   _   _   _  _ _ pending_id_must_be_zero
        \\ transfer   T1 A8 A9    0   _  _  _  _    1 L0 C0   _   _   _   _   _   _  _ _ timeout_reserved_for_pending_transfer
        \\ transfer   T1 A8 A9    0   _  _  _  _    _ L0 C0   _ PEN   _   _   _   _  _ _ amount_must_not_be_zero
        \\ transfer   T1 A8 A9    9   _  _  _  _    _ L0 C0   _ PEN   _   _   _   _  _ _ ledger_must_not_be_zero
        \\ transfer   T1 A8 A9    9   _  _  _  _    _ L9 C0   _ PEN   _   _   _   _  _ _ code_must_not_be_zero
        \\ transfer   T1 A8 A9    9   _  _  _  _    _ L9 C1   _ PEN   _   _   _   _  _ _ debit_account_not_found
        \\ transfer   T1 A1 A9    9   _  _  _  _    _ L9 C1   _ PEN   _   _   _   _  _ _ credit_account_not_found
        \\ transfer   T1 A1 A2    1   _  _  _  _    _ L9 C1   _ PEN   _   _   _   _  _ _ accounts_must_have_the_same_ledger
        \\ transfer   T1 A1 A3    1   _  _  _  _    _ L9 C1   _ PEN   _   _   _   _  _ _ transfer_must_have_the_same_ledger_as_accounts
        \\ transfer   T1 A1 A3  -99   _  _  _  _    _ L1 C1   _ PEN   _   _   _   _  _ _ overflows_debits_pending  // amount = max - A1.debits_pending + 1
        \\ transfer   T1 A1 A3 -109   _  _  _  _    _ L1 C1   _ PEN   _   _   _   _  _ _ overflows_credits_pending // amount = max - A3.credits_pending + 1
        \\ transfer   T1 A1 A3 -199   _  _  _  _    _ L1 C1   _ PEN   _   _   _   _  _ _ overflows_debits_posted   // amount = max - A1.debits_posted + 1
        \\ transfer   T1 A1 A3 -209   _  _  _  _    _ L1 C1   _ PEN   _   _   _   _  _ _ overflows_credits_posted  // amount = max - A3.credits_posted + 1
        \\ transfer   T1 A1 A3 -299   _  _  _  _    _ L1 C1   _ PEN   _   _   _   _  _ _ overflows_debits          // amount = max - A1.debits_pending - A1.debits_posted + 1
        \\ transfer   T1 A1 A3 -319   _  _  _  _    _ L1 C1   _ PEN   _   _   _   _  _ _ overflows_credits         // amount = max - A3.credits_pending - A3.credits_posted + 1
        \\ transfer   T1 A4 A5  199   _  _  _  _  999 L1 C1   _ PEN   _   _   _   _  _ _ overflows_timeout
        \\ transfer   T1 A4 A5  199   _  _  _  _    _ L1 C1   _   _   _   _   _   _  _ _ exceeds_credits           // amount = A4.credits_posted - A4.debits_pending - A4.debits_posted + 1
        \\ transfer   T1 A4 A5   91   _  _  _  _    _ L1 C1   _   _   _   _   _   _  _ _ exceeds_debits            // amount = A5.debits_posted - A5.credits_pending - A5.credits_posted + 1
        \\ transfer   T1 A1 A3  123   _  _  _  _    1 L1 C1   _ PEN   _   _   _   _  _ _ ok

        // Ensure that idempotence is only checked after validation.
        \\ transfer   T1 A1 A3  123   _  _  _  _    1 L2 C1   _ PEN   _   _   _   _  _ _ transfer_must_have_the_same_ledger_as_accounts
        \\ transfer   T1 A1 A3   -0   _ U1 U1 U1    _ L1 C2   _   _   _   _   _   _  _ _ exists_with_different_flags
        \\ transfer   T1 A3 A1   -0   _ U1 U1 U1    1 L1 C2   _ PEN   _   _   _   _  _ _ exists_with_different_debit_account_id
        \\ transfer   T1 A1 A4   -0   _ U1 U1 U1    1 L1 C2   _ PEN   _   _   _   _  _ _ exists_with_different_credit_account_id
        \\ transfer   T1 A1 A3   -0   _ U1 U1 U1    1 L1 C1   _ PEN   _   _   _   _  _ _ exists_with_different_amount
        \\ transfer   T1 A1 A3  123   _ U1 U1 U1    1 L1 C2   _ PEN   _   _   _   _  _ _ exists_with_different_user_data_128
        \\ transfer   T1 A1 A3  123   _  _ U1 U1    1 L1 C2   _ PEN   _   _   _   _  _ _ exists_with_different_user_data_64
        \\ transfer   T1 A1 A3  123   _  _  _ U1    1 L1 C2   _ PEN   _   _   _   _  _ _ exists_with_different_user_data_32
        \\ transfer   T1 A1 A3  123   _  _  _  _    2 L1 C2   _ PEN   _   _   _   _  _ _ exists_with_different_timeout
        \\ transfer   T1 A1 A3  123   _  _  _  _    1 L1 C2   _ PEN   _   _   _   _  _ _ exists_with_different_code
        \\ transfer   T1 A1 A3  123   _  _  _  _    1 L1 C1   _ PEN   _   _   _   _  _ _ exists
        \\ transfer   T2 A3 A1    7   _  _  _  _    _ L1 C2   _   _   _   _   _   _  _ _ ok
        \\ transfer   T3 A1 A3    3   _  _  _  _    _ L1 C2   _   _   _   _   _   _  _ _ ok
        \\ commit create_transfers
        \\
        \\ lookup_account A1 223 203   0   7
        \\ lookup_account A3   0   7 233 213
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 exists true
        \\ lookup_transfer T2 exists true
        \\ lookup_transfer T3 exists true
        \\ lookup_transfer -0 exists false
        \\ commit lookup_transfers
    );
}

test "create/lookup 2-phase transfers" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ commit create_accounts

        // First phase.
        \\ transfer   T1 A1 A2   15   _  _  _  _    _ L1 C1   _   _   _   _   _   _  _ _ ok // Not pending!
        \\ transfer   T2 A1 A2   15   _  _  _  _ 1000 L1 C1   _ PEN   _   _   _   _  _ _ ok
        \\ transfer   T3 A1 A2   15   _  _  _  _   50 L1 C1   _ PEN   _   _   _   _  _ _ ok
        \\ transfer   T4 A1 A2   15   _  _  _  _    1 L1 C1   _ PEN   _   _   _   _  _ _ ok
        \\ transfer   T5 A1 A2    7   _ U9 U9 U9   50 L1 C1   _ PEN   _   _   _   _  _ _ ok
        \\ transfer   T6 A1 A2    1   _  _  _  _    0 L1 C1   _ PEN   _   _   _   _  _ _ ok
        \\ commit create_transfers

        // Check balances before resolving.
        \\ lookup_account A1 53 15  0  0
        \\ lookup_account A2  0  0 53 15
        \\ commit lookup_accounts

        // Bump the state machine time in +1s for testing the timeout expiration.
        \\ tick 1000000000

        // Second phase.
        \\ transfer T101 A1 A2   13  T2 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ ok
        \\ transfer   T0 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN POS VOI   _   _  _ 1 timestamp_must_be_zero
        \\ transfer   T0 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN POS VOI   _   _  _ _ id_must_not_be_zero
        \\ transfer   -0 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN POS VOI   _   _  _ _ id_must_not_be_int_max
        \\ transfer T101 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN POS VOI   _   _  _ _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN POS VOI BDR   _  _ _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN POS VOI BDR BCR  _ _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN POS VOI   _ BCR  _ _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _ PEN   _ VOI   _   _  _ _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _   _   _ VOI BDR   _  _ _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _   _   _ VOI BDR BCR  _ _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _   _   _ VOI   _ BCR  _ _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _   _ POS   _ BDR   _  _ _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _   _ POS   _ BDR BCR  _ _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _   _ POS   _   _ BCR  _ _ flags_are_mutually_exclusive
        \\ transfer T101 A8 A9   16  T0 U2 U2 U2   50 L6 C7   _   _   _ VOI   _   _  _ _ pending_id_must_not_be_zero
        \\ transfer T101 A8 A9   16  -0 U2 U2 U2   50 L6 C7   _   _   _ VOI   _   _  _ _ pending_id_must_not_be_int_max
        \\ transfer T101 A8 A9   16 101 U2 U2 U2   50 L6 C7   _   _   _ VOI   _   _  _ _ pending_id_must_be_different
        \\ transfer T101 A8 A9   16 102 U2 U2 U2   50 L6 C7   _   _   _ VOI   _   _  _ _ timeout_reserved_for_pending_transfer
        \\ transfer T101 A8 A9   16 102 U2 U2 U2    _ L6 C7   _   _   _ VOI   _   _  _ _ pending_transfer_not_found
        \\ transfer T101 A8 A9   16  T1 U2 U2 U2    _ L6 C7   _   _   _ VOI   _   _  _ _ pending_transfer_not_pending
        \\ transfer T101 A8 A9   16  T3 U2 U2 U2    _ L6 C7   _   _   _ VOI   _   _  _ _ pending_transfer_has_different_debit_account_id
        \\ transfer T101 A1 A9   16  T3 U2 U2 U2    _ L6 C7   _   _   _ VOI   _   _  _ _ pending_transfer_has_different_credit_account_id
        \\ transfer T101 A1 A2   16  T3 U2 U2 U2    _ L6 C7   _   _   _ VOI   _   _  _ _ pending_transfer_has_different_ledger
        \\ transfer T101 A1 A2   16  T3 U2 U2 U2    _ L1 C7   _   _   _ VOI   _   _  _ _ pending_transfer_has_different_code
        \\ transfer T101 A1 A2   16  T3 U2 U2 U2    _ L1 C1   _   _   _ VOI   _   _  _ _ exceeds_pending_transfer_amount
        \\ transfer T101 A1 A2   14  T3 U2 U2 U2    _ L1 C1   _   _   _ VOI   _   _  _ _ pending_transfer_has_different_amount
        \\ transfer T101 A1 A2   15  T3 U2 U2 U2    _ L1 C1   _   _   _ VOI   _   _  _ _ exists_with_different_flags
        \\ transfer T101 A1 A2   14  T2 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ exists_with_different_amount
        \\ transfer T101 A1 A2    _  T2 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ exists_with_different_amount
        \\ transfer T101 A1 A2   13  T3 U2 U2 U2    _ L1 C1   _   _ POS   _   _   _  _ _ exists_with_different_pending_id
        \\ transfer T101 A1 A2   13  T2 U2 U2 U2    _ L1 C1   _   _ POS   _   _   _  _ _ exists_with_different_user_data_128
        \\ transfer T101 A1 A2   13  T2 U1 U2 U2    _ L1 C1   _   _ POS   _   _   _  _ _ exists_with_different_user_data_64
        \\ transfer T101 A1 A2   13  T2 U1 U1 U2    _ L1 C1   _   _ POS   _   _   _  _ _ exists_with_different_user_data_32
        \\ transfer T101 A1 A2   13  T2 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ exists
        \\ transfer T102 A1 A2   13  T2 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ pending_transfer_already_posted
        \\ transfer T103 A1 A2   15  T3 U1 U1 U1    _ L1 C1   _   _   _ VOI   _   _  _ _ ok
        \\ transfer T102 A1 A2   13  T3 U1 U1 U1    _ L1 C1   _   _ POS   _   _   _  _ _ pending_transfer_already_voided
        \\ transfer T102 A1 A2   15  T4 U1 U1 U1    _ L1 C1   _   _   _ VOI   _   _  _ _ pending_transfer_expired
        \\ transfer T105 A0 A0    _  T5 U0 U0 U0    _ L0 C0   _   _ POS   _   _   _  _ _ ok
        \\ transfer T106 A0 A0    0  T6 U0 U0 U0    _ L1 C1   _   _ POS   _   _   _  _ _ ok
        \\ commit create_transfers

        // Check balances after resolving.
        \\ lookup_account A1 15 36  0  0
        \\ lookup_account A2  0  0 15 36
        \\ commit lookup_accounts
    );
}

test "create_transfers: empty" {
    try check(
        \\ commit create_transfers
    );
}

test "create_transfers/lookup_transfers: failed transfer does not exist" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ commit create_accounts
        \\
        \\ transfer   T1 A1 A2   15   _  _  _  _    _ L1 C1   _   _   _   _   _   _  _ _ ok
        \\ transfer   T2 A1 A2   15   _  _  _  _    _ L0 C1   _   _   _   _   _   _  _ _ ledger_must_not_be_zero
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 15 0  0
        \\ lookup_account A2 0  0 0 15
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 exists true
        \\ lookup_transfer T2 exists false
        \\ commit lookup_transfers
    );
}

test "create_transfers: failed linked-chains are undone" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ commit create_accounts
        \\
        \\ transfer   T1 A1 A2   15   _  _  _  _    _ L1 C1 LNK   _   _   _   _   _  _ _ linked_event_failed
        \\ transfer   T2 A1 A2   15   _  _  _  _    _ L0 C1   _   _   _   _   _   _  _ _ ledger_must_not_be_zero
        \\ commit create_transfers
        \\
        \\ transfer   T3 A1 A2   15   _  _  _  _    1 L1 C1 LNK PEN   _   _   _   _  _ _ linked_event_failed
        \\ transfer   T4 A1 A2   15   _  _  _  _    _ L0 C1   _   _   _   _   _   _  _ _ ledger_must_not_be_zero
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 0 0 0
        \\ lookup_account A2 0 0 0 0
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 exists false
        \\ lookup_transfer T2 exists false
        \\ lookup_transfer T3 exists false
        \\ lookup_transfer T4 exists false
        \\ commit lookup_transfers
    );
}

test "create_transfers: failed linked-chains are undone within a commit" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 0 0 0 20
        \\
        \\ transfer   T1 A1 A2   15   _ _   _  _    _ L1 C1 LNK   _   _   _   _   _  _ _ linked_event_failed
        \\ transfer   T2 A1 A2    5   _ _   _  _    _ L0 C1   _   _   _   _   _   _  _ _ ledger_must_not_be_zero
        \\ transfer   T3 A1 A2   15   _ _   _  _    _ L1 C1   _   _   _   _   _   _  _ _ ok
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 15 0 20
        \\ lookup_account A2 0  0 0 15
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 exists false
        \\ lookup_transfer T2 exists false
        \\ lookup_transfer T3 exists true
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit | balancing_credit (*_must_not_exceed_*)" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 1  0 0 10
        \\ setup A2 0 10 2  0
        \\
        \\ transfer   T1 A1 A3  3     _  _  _  _    _ L2 C1   _   _   _   _ BDR   _  _ _ transfer_must_have_the_same_ledger_as_accounts
        \\ transfer   T1 A3 A2  3     _  _  _  _    _ L2 C1   _   _   _   _   _ BCR  _ _ transfer_must_have_the_same_ledger_as_accounts
        \\ transfer   T1 A1 A3  3     _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ ok
        \\ transfer   T2 A1 A3 13     _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ ok
        \\ transfer   T3 A3 A2  3     _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ ok
        \\ transfer   T4 A3 A2 13     _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ ok
        \\ transfer   T5 A1 A3  1     _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ exceeds_credits
        \\ transfer   T5 A1 A3  1     _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ exceeds_credits
        \\ transfer   T5 A3 A2  1     _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ exceeds_debits
        \\ transfer   T5 A1 A2  1     _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ exceeds_credits

        // "exists" requires that the amount matches exactly, even when BDR/BCR is set.
        \\ transfer   T1 A1 A3    2   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ exists_with_different_amount
        \\ transfer   T1 A1 A3    4   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ exists_with_different_amount
        \\ transfer   T1 A1 A3    3   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ exists
        \\ transfer   T2 A1 A3    6   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ exists
        \\ transfer   T3 A3 A2    3   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ exists
        \\ transfer   T4 A3 A2    5   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ exists
        \\ commit create_transfers
        \\
        \\ lookup_account A1 1  9 0 10
        \\ lookup_account A2 0 10 2  8
        \\ lookup_account A3 0  8 0  9
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 amount 3
        \\ lookup_transfer T2 amount 6
        \\ lookup_transfer T3 amount 3
        \\ lookup_transfer T4 amount 5
        \\ lookup_transfer T5 exists false
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit | balancing_credit (¬*_must_not_exceed_*)" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 1  0 0 10
        \\ setup A2 0 10 2  0
        \\
        \\ transfer   T1 A3 A1   99   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ exceeds_credits
        \\ transfer   T1 A3 A1   99   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ exceeds_credits
        \\ transfer   T1 A2 A3   99   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ exceeds_debits
        \\ transfer   T1 A1 A3   99   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ ok
        \\ transfer   T2 A1 A3   99   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ exceeds_credits
        \\ transfer   T3 A3 A2   99   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ ok
        \\ transfer   T4 A3 A2   99   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ exceeds_debits
        \\ commit create_transfers
        \\
        \\ lookup_account A1 1  9 0 10
        \\ lookup_account A2 0 10 2  8
        \\ lookup_account A3 0  8 0  9
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 amount 9
        \\ lookup_transfer T2 exists false
        \\ lookup_transfer T3 amount 8
        \\ lookup_transfer T4 exists false
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit | balancing_credit (amount=0)" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ ok
        \\ account A4  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 1  0 0 10
        \\ setup A2 0 10 2  0
        \\ setup A3 0 10 2  0
        \\
        \\ transfer   T1 A1 A4    0   _  _  _  _    _ L1 C1   _   _   _   _ BDR   _  _ _ ok
        \\ transfer   T2 A4 A2    0   _  _  _  _    _ L1 C1   _   _   _   _   _ BCR  _ _ ok
        \\ transfer   T3 A4 A3    0   _  _  _  _    _ L1 C1   _ PEN   _   _   _ BCR  _ _ ok
        \\ commit create_transfers
        \\
        \\ lookup_account A1 1  9  0 10
        \\ lookup_account A2 0 10  2  8
        \\ lookup_account A3 0 10 10  0
        \\ lookup_account A4 8  8  0  9
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 amount 9
        \\ lookup_transfer T2 amount 8
        \\ lookup_transfer T3 amount 8
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit & balancing_credit" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ ok
        \\ account A3  0  0  0  0  _  _  _ _ L1 C1   _   _   _ _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 0  0 0 20
        \\ setup A2 0 10 0  0
        \\ setup A3 0 99 0  0
        \\
        \\ transfer   T1 A1 A2    1   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ ok
        \\ transfer   T2 A1 A2   12   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ ok
        \\ transfer   T3 A1 A2    1   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ exceeds_debits
        \\ transfer   T3 A1 A3   12   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ ok
        \\ transfer   T4 A1 A3    1   _  _  _  _    _ L1 C1   _   _   _   _ BDR BCR  _ _ exceeds_credits
        \\ commit create_transfers
        \\
        \\ lookup_account A1 0 20 0 20
        \\ lookup_account A2 0 10 0 10
        \\ lookup_account A3 0 99 0 10
        \\ commit lookup_accounts
        \\
        \\ lookup_transfer T1 amount  1
        \\ lookup_transfer T2 amount  9
        \\ lookup_transfer T3 amount 10
        \\ lookup_transfer T4 exists false
        \\ commit lookup_transfers
    );
}

test "create_transfers: balancing_debit/balancing_credit + pending" {
    try check(
        \\ account A1  0  0  0  0  _  _  _ _ L1 C1   _ D<C   _ _ _ ok
        \\ account A2  0  0  0  0  _  _  _ _ L1 C1   _   _ C<D _ _ ok
        \\ commit create_accounts
        \\
        \\ setup A1 0  0 0 10
        \\ setup A2 0 10 0  0
        \\
        \\ transfer   T1 A1 A2    3   _  _  _  _    _ L1 C1   _ PEN   _   _ BDR   _  _ _ ok
        \\ transfer   T2 A1 A2   13   _  _  _  _    _ L1 C1   _ PEN   _   _ BDR   _  _ _ ok
        \\ transfer   T3 A1 A2    1   _  _  _  _    _ L1 C1   _ PEN   _   _ BDR   _  _ _ exceeds_credits
        \\ commit create_transfers
        \\
        \\ lookup_account A1 10  0  0 10
        \\ lookup_account A2  0 10 10  0
        \\ commit lookup_accounts
        \\
        \\ transfer   T3 A1 A2    0  T1  _  _  _    _ L1 C1   _   _ POS   _   _   _  _ _ ok
        \\ transfer   T4 A1 A2    5  T2  _  _  _    _ L1 C1   _   _ POS   _   _   _  _ _ ok
        \\ commit create_transfers
        \\
        \\ lookup_transfer T1 amount  3
        \\ lookup_transfer T2 amount  7
        \\ lookup_transfer T3 amount  3
        \\ lookup_transfer T4 amount  5
        \\ commit lookup_transfers
    );
}

test "StateMachine: Demuxer" {
    const StateMachine = StateMachineType(
        @import("testing/storage.zig").Storage,
        global_constants.state_machine_config,
    );

    var prng = std.rand.DefaultPrng.init(42);
    inline for ([_]StateMachine.Operation{
        .create_accounts,
        .create_transfers,
    }) |operation| {
        try expect(StateMachine.batch_logical_allowed.get(operation));

        const Result = StateMachine.Result(operation);
        var results: [@divExact(global_constants.message_body_size_max, @sizeOf(Result))]Result = undefined;

        for (0..100) |_| {
            // Generate Result errors to Events at random.
            var reply_len: u32 = 0;
            for (0..results.len) |i| {
                if (prng.random().boolean()) {
                    results[reply_len] = .{ .index = @intCast(i), .result = .ok };
                    reply_len += 1;
                }
            }

            // Demux events of random strides from the generated results.
            var demuxer = StateMachine.DemuxerType(operation).init(results[0..reply_len]);
            const event_count: u32 = @intCast(@max(1, prng.random().uintAtMost(usize, results.len)));
            var event_offset: u32 = 0;
            while (event_offset < event_count) {
                const event_size = @max(1, prng.random().uintAtMost(u32, event_count - event_offset));
                const reply = demuxer.decode(event_offset, event_size);
                defer event_offset += event_size;

                for (reply) |*result| {
                    try expectEqual(result.result, .ok);
                    try expect(result.index < event_offset + event_size);
                }
            }
        }
    }
}

test "StateMachine: ref all decls" {
    const Storage = @import("storage.zig").Storage;
    const StateMachine = StateMachineType(Storage, .{
        .message_body_size_max = 1000 * @sizeOf(Account),
        .lsm_batch_multiple = 1,
        .vsr_operations_reserved = 128,
    });

    std.testing.refAllDecls(StateMachine);
}
