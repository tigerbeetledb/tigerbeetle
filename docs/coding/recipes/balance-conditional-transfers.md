# Balance-Conditional Transfers

In some use cases, you may want to execute a transfer if and only if an account has at least a
certain balance.

It would be unsafe to check an account's balance using the
[`lookup_accounts`](../../reference/requests/lookup_accounts.md) and then perform the transfer,
because these requests are not be atomic and the account's balance may change between the lookup and
the transfer.

You can atomically run a check against an account's balance before executing a transfer by using a
control or temporary account and linked transfers.

## Preconditions

### 1. Target Account Must Have a Limited Balance

The account for whom you want to do the balance check must have one of these flags set:

- [`flags.debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits)
  for accounts with [credit balances](../data-modeling.md#credit-balances)
- [`flags.credits_must_not_exceed_debits`](../../reference/account.md#flagscredits_must_not_exceed_debits)
  for accounts with [debit balances](../data-modeling.md#debit-balances)

### 2. Create a Control Account

There must also be a designated control account. As you can see below, this account will never
actually take control of the target account's funds, but we will set up simultaneous transfers in
and out of the control account.

## Executing a Balance-Conditional Transfer

The balance-conditional transfer consists of 3
[linked transfers](../../reference/requests/README.md#linked-events).

We will refer to two amounts:

- The **threshold amount** is the minimum amount the target account should have in order to execute
  the transfer.
- The **transfer amount** is the amount we want to transfer if and only if the target account's
  balance meets the threshold.

### If the Source Account Has a Credit Balance

| Transfer | Debit Account | Credit Account | Amount    | Pending Id | Flags                                                          |
| -------- | ------------- | -------------- | --------- | ---------- | -------------------------------------------------------------- |
| 1        | Source        | Control        | Threshold |          - | [`flags.linked`](../../reference/transfer.md#flagslinked), [`pending`](../../reference/transfer.md#flagspending) |
| 2        | -             | -              | -         |          1 | [`flags.linked`](../../reference/transfer.md#flagslinked), [`void_pending_transfer`](../../reference/transfer.md#flagsvoid_pending_transfer) |
| 3        | Source        | Destination    | Transfer  |          - | N/A                                                            |

### If the Source Account Has a Debit Balance

| Transfer | Debit Account | Credit Account | Amount    | Pending Id | Flags                                                          |
| -------- | ------------- | -------------- | --------- | ---------- | -------------------------------------------------------------- |
| 1        | Control       | Source         | Threshold |          - | [`flags.linked`](../../reference/transfer.md#flagslinked), [`pending`](../../reference/transfer.md#flagspending) |
| 2        | -             | -              | -         |          1 | [`flags.linked`](../../reference/transfer.md#flagslinked), [`void_pending_transfer`](../../reference/transfer.md#flagsvoid_pending_transfer) |
| 3        | Destination   | Source         | Transfer  |          - | N/A                                                            |

### Understanding the Mechanism

Each of the 3 transfers is linked, meaning they will all succeed or fail together.

The first transfer attempts to transfer the threshold amount to the control account. If this
transfer would cause the source account's net balance to go below zero, the account's balance limit
flag would ensure that the first transfer fails. If the first transfer fails, the other two linked
transfers would also fail.

If the first transfer succeeds, it means that the source account did have the threshold balance. In
this case, the second transfer cancels the first transfer (returning the threshold amount to the
source account). Then, the third transfer would execute the desired transfer to the ultimate
destination account.

Note that in the tables above, we do the balance check on the source account. The balance check
could also be applied to the destination account instead.
