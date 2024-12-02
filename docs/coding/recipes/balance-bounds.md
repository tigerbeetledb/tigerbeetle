# Balance Bounds

It is easy to limit an account's balance using either
[`flags.debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits)
or
[`flags.credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits).

What if you want an account's balance to stay between an upper and a lower bound?

This is possible to check atomically using a set of linked transfers. (Note: with the
`must_not_exceed` flag invariants, an account is guaranteed to never violate those invariants. This
maximum balance approach must be enforced per-transfer -- it is possible to exceed the limit simply
by not enforcing it for a particular transfer.)

## Preconditions

1. Target Account Should Have a Limited Balance

The account whose balance you want to bound should have one of these flags set:

- [`flags.debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits)
  for accounts with [credit balances](../data-modeling.md#credit-balances)
- [`flags.credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits)
  for accounts with [debit balances](../data-modeling.md#debit-balances)

2. Create a Control Account with the Opposite Limit

There must also be a designated control account.

As you can see below, this account will never actually take control of the target account's funds,
but we will set up simultaneous transfers in and out of the control account to apply the limit.

This account must have the opposite limit applied as the target account:

- [`flags.credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits)
  if the target account has a [credit balance](../data-modeling.md#credit-balances)
- [`flags.debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits)
  if the target account has a [debit balance](../data-modeling.md#debit-balances)

3. Create an Operator Account

The operator account will be used to fund the Control Account.

## Executing a Transfer with a Balance Bounds Check

This consists of 5 [linked transfers](../../reference/requests/README.md#linked-events).

We will refer to two amounts:

- The **limit amount** is upper bound we want to maintain on the target account's balance.
- The **transfer amount** is the amount we want to transfer if and only if the target account's
  balance after a successful transfer would be within the bounds.

### If the Target Account Has a Credit Balance

In this case, we are keeping the Destination Account's balance between the bounds.

| Transfer | Debit Account | Credit Account | Amount       | Pending ID | Flags (Note: `\|` sets multiple flags)                                                                                                                                                                  |
| -------- | ------------- | -------------- | ------------ | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1        | Source        | Destination    | Transfer     | `0`        | [`flags.linked`](../../reference/transfer.md#flagslinked)                                                                                                                                               |
| 2        | Control       | Operator       | Limit        | `0`        | [`flags.linked`](../../reference/transfer.md#flagslinked)                                                                                                                                               |
| 3        | Destination   | Control        | `AMOUNT_MAX` | `0`        | [`flags.linked`](../../reference/transfer.md#flagslinked) \| [`flags.balancing_debit`](../../reference/transfer.md#flagsbalancing_debit) \| [`flags.pending`](../../reference/transfer.md#flagspending) |
| 4        | `0`           | `0`            | `0`          | `3`\*      | [`flags.linked`](../../reference/transfer.md#flagslinked) \| [`flags.void_pending_transfer`](../../reference/transfer.md#flagsvoid_pending_transfer)                                                    |
| 5        | Operator      | Control        | Limit        | `0`        | `0`                                                                                                                                                                                                     |

\*This must be set to the transfer ID of the pending transfer (in this example, it is transfer 3).

### If the Target Account Has a Debit Balance

In this case, we are keeping the Destination Account's balance between the bounds.

| Transfer | Debit Account | Credit Account | Amount       | Pending ID | Flags (Note `\|` sets multiple flags)                                                                                                                                                                     |
| -------- | ------------- | -------------- | ------------ | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1        | Destination   | Source         | Transfer     | `0`        | [`flags.linked`](../../reference/transfer.md#flagslinked)                                                                                                                                                 |
| 2        | Operator      | Control        | Limit        | `0`        | [`flags.linked`](../../reference/transfer.md#flagslinked)                                                                                                                                                 |
| 3        | Control       | Destination    | `AMOUNT_MAX` | `0`        | [`flags.balancing_credit`](../../reference/transfer.md#flagsbalancing_credit) \| [`flags.pending`](../../reference/transfer.md#flagspending) \| [`flags.linked`](../../reference/transfer.md#flagslinked) |
| 4        | `0`           | `0`            | `0`          | `3`\*      | [`flags.void_pending_transfer`](../../reference/transfer.md#flagsvoid_pending_transfer) \| [`flags.linked`](../../reference/transfer.md#flagslinked)                                                      |
| 5        | Control       | Operator       | Limit        | `0`        | `0`                                                                                                                                                                                                       |

\*This must be set to the transfer ID of the pending transfer (in this example, it is transfer 3).

### Understanding the Mechanism

Each of the 5 transfers is [linked](../../reference/requests/README.md#linked-events) so that all of
them will succeed or all of them will fail.

The first transfer is the one we actually want to send.

The second transfer sets the Control Account's balance to the upper bound we want to impose.

The third transfer uses a [`balancing_debit`](../../reference/transfer.md#flagsbalancing_debit) or
[`balancing_credit`](../../reference/transfer.md#flagsbalancing_credit) to transfer the Destination
Account's net credit balance or net debit balance, respectively, to the Control Account. This
transfer will fail if the first transfer would put the Destination Account's balance above the upper
bound.

The third transfer is also a pending transfer, so it won't actually transfer the Destination
Account's funds, even if it succeeds.

If everything to this point succeeds, the fourth and fifth transfers simply undo the effects of the
second and third transfers. The fourth transfer voids the pending transfer. And the fifth transfer
resets the Control Account's net balance to zero.
