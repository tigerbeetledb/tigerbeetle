const tb = @import("tigerbeetle.zig");
const demo = @import("demo.zig");

const Transfer = tb.Transfer;

pub fn main() !void {
    const commits = [_]Transfer{
        Transfer{
            .id = 1001,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .user_data = 0,
            .reserved = [_]u8{0} ** 32,
            .timeout = 0,
            .code = 0,
            .flags = .{ .post_pending_transfer = true }, // Post the pending 2-phase Transfer.
            .amount = 0, // Amount from pending Transfer.
        },
        Transfer{
            .id = 1002,
            .debit_account_id = 1,
            .credit_account_id = 2,
            .user_data = 0,
            .reserved = [_]u8{0} ** 32,
            .timeout = 0,
            .code = 0,
            .flags = .{ .post_pending_transfer = true }, // Post the pending 2-phase Transfer.
            .amount = 0, // Amount from pending Transfer.
        },
    };

    try demo.request(.create_transfers, commits, demo.on_create_transfers);
}
