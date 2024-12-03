# Two-Phase Transfers

A two-phase transfer moves funds in stages:

1. Reserve funds ([pending](#reserve-funds-pending-transfer))
2. Resolve funds ([post](#post-pending-transfer), [void](#void-pending-transfer), or
   [expire](#expire-pending-transfer))

The name "two-phase transfer" is a reference to the
[two-phase commit protocol for distributed transactions](https://en.wikipedia.org/wiki/Two-phase_commit_protocol).

## Reserve Funds (Pending Transfer)

A pending transfer, denoted by [`flags.pending`](../reference/transfer.md#flagspending),
reserves its `amount` in the debit/credit accounts'
[`debits_pending`](../reference/account.md#debits_pending)/[`credits_pending`](../reference/account.md#credits_pending)
fields, respectively. Pending transfers leave the `debits_posted`/`credits_posted` unmodified.

## Resolve Funds

Pending transfers can be posted, voided, or they may time out.

### Post-Pending Transfer

A post-pending transfer, denoted by
[`flags.post_pending_transfer`](../reference/transfer.md#flagspost_pending_transfer), causes a
pending transfer to "post", transferring some or all of the pending transfer's reserved amount to
its destination.

- If the posted [`amount`](../reference/transfer.md#amount) is less than the pending transfer's
  amount, then only this amount is posted, and the remainder is restored to its original accounts.
- If the posted [`amount`](../reference/transfer.md#amount) is equal to the pending transfer's
  amount or equal to `AMOUNT_MAX` (`2^128 - 1`), the full pending transfer's amount is posted.
- If the posted [`amount`](../reference/transfer.md#amount) is greater than the pending transfer's
  amount (but less than `AMOUNT_MAX`),
  [`exceeds_pending_transfer_amount`](../reference/requests/create_transfers.md#exceeds_pending_transfer_amount)
  is returned.

<details>
<summary>Client &lt; 0.16.0</summary>

- If the posted [`amount`](../reference/transfer.md#amount) is 0, the full pending transfer's
  amount is posted.
- If the posted [`amount`](../reference/transfer.md#amount) is nonzero, then only this amount
  is posted, and the remainder is restored to its original accounts. It must be less than or equal
  to the pending transfer's amount.

</details>

Additionally, when `flags.post_pending_transfer` is set:

- [`pending_id`](../reference/transfer.md#pending_id) must reference a
  [pending transfer](#reserve-funds-pending-transfer).
- [`flags.void_pending_transfer`](../reference/transfer.md#flagsvoid_pending_transfer) must not
  be set.

The following fields may either be zero or they must match the value of the pending transfer's
field:

- [`debit_account_id`](../reference/transfer.md#debit_account_id)
- [`credit_account_id`](../reference/transfer.md#credit_account_id)
- [`ledger`](../reference/transfer.md#ledger)
- [`code`](../reference/transfer.md#code)

### Void-Pending Transfer

A void-pending transfer, denoted by
[`flags.void_pending_transfer`](../reference/transfer.md#flagsvoid_pending_transfer), restores
the pending amount its original accounts. Additionally, when this field is set:

- [`pending_id`](../reference/transfer.md#pending_id) must reference a
  [pending transfer](#reserve-funds-pending-transfer).
- [`flags.post_pending_transfer`](../reference/transfer.md#flagspost_pending_transfer) must not
  be set.

The following fields may either be zero or they must match the value of the pending transfer's
field:

- [`debit_account_id`](../reference/transfer.md#debit_account_id)
- [`credit_account_id`](../reference/transfer.md#credit_account_id)
- [`ledger`](../reference/transfer.md#ledger)
- [`code`](../reference/transfer.md#code)

### Expire Pending Transfer

A pending transfer may optionally be created with a
[timeout](../reference/transfer.md#timeout). If the timeout interval passes before the transfer
is either posted or voided, the transfer expires and the full amount is returned to the original
account.

Note that `timeout`s are given as intervals, specified in seconds, rather than as absolute
timestamps. For more details on why, read the page about [Time in TigerBeetle](./time.md).

### Errors

A pending transfer can only be posted or voided once. It cannot be posted twice or voided then
posted, etc.

Attempting to resolve a pending transfer more than once will return the applicable error result:

- [`pending_transfer_already_posted`](../reference/requests/create_transfers.md#pending_transfer_already_posted)
- [`pending_transfer_already_voided`](../reference/requests/create_transfers.md#pending_transfer_already_voided)
- [`pending_transfer_expired`](../reference/requests/create_transfers.md#pending_transfer_expired)

## Interaction with Account Invariants

The pending transfer's amount is reserved in a way that the second step in a two-phase transfer will
never cause the accounts' configured balance invariants
([`credits_must_not_exceed_debits`](../reference/account.md#flagscredits_must_not_exceed_debits)
or
[`debits_must_not_exceed_credits`](../reference/account.md#flagsdebits_must_not_exceed_credits))
to be broken, whether the second step is a post or void.

### Pessimistic Pending Transfers

If an account with
[`debits_must_not_exceed_credits`](../reference/account.md#flagsdebits_must_not_exceed_credits)
has `credits_posted = 100` and `debits_posted = 70` and a pending transfer is started causing the
account to have `debits_pending = 50`, the _pending_ transfer will fail. It will not wait to get to
_posted_ status to fail.

## All Transfers Are Immutable

To reiterate, completing a two-phase transfer (by either marking it void or posted) does not involve
modifying the pending transfer. Instead you create a new transfer.

The first transfer that is marked pending will always have its pending flag set.

The second transfer will have a
[`post_pending_transfer`](../reference/transfer.md#flagspost_pending_transfer) or
[`void_pending_transfer`](../reference/transfer.md#flagsvoid_pending_transfer) flag set and a
[`pending_id`](../reference/transfer.md#pending_id) field set to the
[`id`](../reference/transfer.md#id) of the first transfer. The
[`id`](../reference/transfer.md#id) of the second transfer will be unique, not the same
[`id`](../reference/transfer.md#id) as the initial pending transfer.

## Examples

The following examples show the state of two accounts in three steps:

1. Initially, before any transfers
2. After a pending transfer
3. And after the pending transfer is posted or voided

### Post Full Pending Amount

| Account `A` |            | Account `B` |            | Transfers            |                       |            |                         |
| ----------: | ---------: | ----------: | ---------: | :------------------- | :-------------------- | ---------: | :---------------------- |
|  **debits** |            | **credits** |            |                      |                       |            |                         |
| **pending** | **posted** | **pending** | **posted** | **debit_account_id** | **credit_account_id** | **amount** | **flags**               |
|         `w` |        `x` |         `y` |        `z` | -                    | -                     |          - | -                       |
|   123 + `w` |        `x` |   123 + `y` |        `z` | `A`                  | `B`                   |        123 | `pending`               |
|         `w` |  123 + `x` |         `y` |  123 + `z` | `A`                  | `B`                   |        123 | `post_pending_transfer` |

### Post Partial Pending Amount

| Account `A` |            | Account `B` |            | Transfers            |                       |            |                         |
| ----------: | ---------: | ----------: | ---------: | :------------------- | :-------------------- | ---------: | :---------------------- |
|  **debits** |            | **credits** |            |                      |                       |            |                         |
| **pending** | **posted** | **pending** | **posted** | **debit_account_id** | **credit_account_id** | **amount** | **flags**               |
|         `w` |        `x` |         `y` |        `z` | -                    | -                     |          - | -                       |
|   123 + `w` |        `x` |   123 + `y` |        `z` | `A`                  | `B`                   |        123 | `pending`               |
|         `w` |  100 + `x` |         `y` |  100 + `z` | `A`                  | `B`                   |        100 | `post_pending_transfer` |

### Void Pending Transfer

| Account `A` |            | Account `B` |            | Transfers            |                       |            |                         |
| ----------: | ---------: | ----------: | ---------: | :------------------- | :-------------------- | ---------: | :---------------------- |
|  **debits** |            | **credits** |            |                      |                       |            |                         |
| **pending** | **posted** | **pending** | **posted** | **debit_account_id** | **credit_account_id** | **amount** | **flags**               |
|         `w` |        `x` |         `y` |        `z` | -                    | -                     |          - | -                       |
|   123 + `w` |        `x` |   123 + `y` |        `z` | `A`                  | `B`                   |        123 | `pending`               |
|         `w` |        `x` |         `y` |        `z` | `A`                  | `B`                   |        123 | `void_pending_transfer` |

