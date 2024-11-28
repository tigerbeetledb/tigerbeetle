# Close Account

In accounting, a _closing entry_ calculates the net debit or credit balance for an account and then
credits or debits this balance respectively, to zero the account's balance and move the balance to
another account.

Additionally, it may be desirable to forbid further transfers on this account (i.e. at the end of
an accounting period, upon account termination, or even temporarily freezing the account for audit
purposes.

### Example

Given a set of accounts:

| Account | Debits Pending | Debits Posted | Credits Pending | Credits Posted | Flags             |
| ------: | -------------: | ------------: | --------------: | -------------: | ----------------- |
|     `A` |              0 |            10 |               0 |             20 | [`debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits)|
|     `B` |              0 |            30 |               0 |              5 | [`credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits)|
|     `C` |              0 |             0 |               0 |              0 |                   |

The "closing entries" for accounts `A` and `B` are expressed as _linked chains_, so they either
succeed or fail atomically.

- Account `A`: the linked transfers are `T1` and `T2`.

- Account `B`: the linked transfers are `T3` and `T4`.

- Account `C`: is the _control account_ and will not be closed.

| Transfer | Debit Account   | Credit Account | Amount       | Amount (recorded) | Flags          |
| -------: | --------------: | -------------: | -----------: | ----------------: | -------------- |
|     `T1` |             `A` |            `C` | `AMOUNT_MAX` |                10 | [`balancing_debit`](../../reference/transfer.md#flagsbalancing_debit),[`linked`](../../reference/transfer.md#flagslinked)  |
|     `T2` |             `A` |            `C` |           0  |                 0 | [`closing_debit`](../../reference/transfer.md#flagsclosing_debit), [`pending`](../../reference/transfer.md#flagspending)   |
|     `T3` |             `C` |            `B` | `AMOUNT_MAX` |                25 | [`balancing_credit`](../../reference/transfer.md#flagsbalancing_credit),[`linked`](../../reference/transfer.md#flagslinked)|
|     `T4` |             `C` |            `B` |           0  |                 0 | [`closing_credit`](../../reference/transfer.md#flagsclosing_credit), [`pending`](../../reference/transfer.md#flagspending) |


- `T1` and `T3` are _balancing transfers_ with `AMOUNT_MAX` as the `Transfer.amount` so that the
  application does not need to know (or query) the balance prior to closing the account.

  The stored transfer's `amount` will be set to the actual amount transferred.

- `T2` and `T4` are _closing transfers_ that will cause the respective account to be closed.

  The closing transfer must be also a _pending transfer_ so the action can be reversible.

After committing these transfers, `A` and `B` are closed with net balance zero, and will reject any
further transfers.

| Account | Debits Pending | Debits Posted | Credits Pending | Credits Posted | Flags             |
| ------: | -------------: | ------------: | --------------: | -------------: | ----------------- |
|     `A` |              0 |            20 |               0 |             20 | [`debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits), [`closed`](../../reference/account.md#flagsclosed)|
|     `B` |              0 |            30 |               0 |             30 | [`credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits), [`closed`](../../reference/account.md#flagsclosed)|
|     `C` |              0 |            25 |               0 |             10 |                   |

To re-open the closed account, the _pending closing transfer_ can be _voided_, reverting the
closing action (but not reverting the net balance):

| Transfer | Debit Account   | Credit Account | Amount       | Pending Transfer  | Flags          |
| -------: | --------------: | -------------: | -----------: | ----------------: | -------------- |
|     `T5` |             `A` |            `C` |           0  |              `T2` | [`void_pending_transfer`](../../reference/transfer.md#flagsvoid_pending_transfer)|
|     `T6` |             `C` |            `B` |           0  |              `T4` | [`void_pending_transfer`](../../reference/transfer.md#flagsvoid_pending_transfer)|

After committing these transfers, `A` and `B` are re-opened and can accept transfers again:

| Account | Debits Pending | Debits Posted | Credits Pending | Credits Posted | Flags            |
| ------: | -------------: | ------------: | --------------: | -------------: | ---------------- |
|     `A` |              0 |            20 |               0 |             20 | [`debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits)|
|     `B` |              0 |            30 |               0 |             30 | [`credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits)|
|     `C` |              0 |            25 |               0 |             10 |                  |
