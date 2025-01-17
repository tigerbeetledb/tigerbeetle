---
sidebar_position: 8
---

# Balance-invariant Transfers

For some accounts, it may be useful to enforce
[`flags.debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits)
or
[`flags.credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits)
balance invariants for only a subset of all transfers, rather than all transfers.

## Per-transfer `credits_must_not_exceed_debits`

This recipe requires three accounts:
1. The **source** account, to debit.
2. The **destination** account, to credit. (With _neither_
  [`flags.credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits) nor
  [`flags.debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits) set,
  since in this recipe we are only enforcing the invariant on a per-transfer basis.
3. The **control** account, to test the balance invariant. The control account should have
  [`flags.credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits)
  set.

| Id | Debit Account | Credit Account | Amount | Pending Id |                                               Flags |
| -: | ------------: | -------------: | -----: | ---------: | --------------------------------------------------: |
|  1 |        Source |    Destination |    123 |          - | [`linked`](../../reference/transfer.md#flagslinked) |
|  2 |   Destination |        Control |      1 |          - | [`linked`](../../reference/transfer.md#flagslinked), [`pending`](../../reference/transfer.md#flagspending), [`balancing_debit`](../../reference/transfer.md#flagsbalancing_debit) |
|  3 |             - |              - |      0 |          2 | [`void_pending_transfer`](../../reference/transfer.md#flagsvoid_pending_transfer) |

When the destination account's final credits do not exceed its debits, the chain will succeed.
When the destination account's final credits exceeds its debits, transfer `2` will fail with `exceeds_debits`.
