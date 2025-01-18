# `Transfer`

A `transfer` is an immutable record of a financial transaction between two accounts.

In TigerBeetle, financial transactions are called "transfers" instead of "transactions" because the
latter term is heavily overloaded in the context of databases.

Note that transfers debit a single account and credit a single account on the same ledger. You can
compose these into more complex transactions using the methods described in
[Currency Exchange](../coding/recipes/currency-exchange.md) and
[Multi-Debit, Multi-Credit Transfers](../coding/recipes/multi-debit-credit-transfers.md).

### Updates

Transfers _cannot be modified_ after creation.

If a detail of a transfer is incorrect and needs to be modified, this is done using
[correcting transfers](../coding/recipes/correcting-transfers.md).

### Deletion

Transfers _cannot be deleted_ after creation.

If a transfer is made in error, its effects can be reversed using a
[correcting transfer](../coding/recipes/correcting-transfers.md).

### Guarantees

- Transfers are immutable. They are never modified once they are successfully created.
- There is at most one `Transfer` with a particular [`id`](#id).
- A [pending transfer](../coding/two-phase-transfers.md#reserve-funds-pending-transfer) resolves at
  most once.
- Transfer [timeouts](#timeout) are deterministic, driven by the
  [cluster's timestamp](../coding/time.md#why-tigerbeetle-manages-timestamps).

## Modes

Transfers can either be Single-Phase, where they are executed immediately, or Two-Phase, where they
are first put in a Pending state and then either Posted or Voided. For more details on the latter,
see the [Two-Phase Transfer guide](../coding/two-phase-transfers.md).

Fields used by each mode of transfer:

| Field                         | Single-Phase | Pending  | Post-Pending | Void-Pending |
| ----------------------------- | ------------ | -------- | ------------ | ------------ |
| `id`                          | required     | required | required     | required     |
| `debit_account_id`            | required     | required | optional     | optional     |
| `credit_account_id`           | required     | required | optional     | optional     |
| `amount`                      | required     | required | required     | optional     |
| `pending_id`                  | none         | none     | required     | required     |
| `user_data_128`               | optional     | optional | optional     | optional     |
| `user_data_64`                | optional     | optional | optional     | optional     |
| `user_data_32`                | optional     | optional | optional     | optional     |
| `timeout`                     | none         | optional¹| none         | none         |
| `ledger`                      | required     | required | optional     | optional     |
| `code`                        | required     | required | optional     | optional     |
| `flags.linked`                | optional     | optional | optional     | optional     |
| `flags.pending`               | false        | true     | false        | false        |
| `flags.post_pending_transfer` | false        | false    | true         | false        |
| `flags.void_pending_transfer` | false        | false    | false        | true         |
| `flags.balancing_debit`       | optional     | optional | false        | false        |
| `flags.balancing_credit`      | optional     | optional | false        | false        |
| `flags.closing_debit`         | optional     | true     | false        | false        |
| `flags.closing_credit`        | optional     | true     | false        | false        |
| `flags.imported`              | optional     | optional | optional     | optional     |
| `timestamp`                   | none²        | none²    | none²        | none²        |

> _¹ None if `flags.imported` is set._<br/>
  _² Required if `flags.imported` is set._

## Fields

### `id`

This is a unique identifier for the transaction.

Constraints:

- Type is 128-bit unsigned integer (16 bytes)
- Must not be zero or `2^128 - 1`
- Must not conflict with another transfer in the cluster

See the [`id` section in the data modeling doc](../coding/data-modeling.md#id) for more
recommendations on choosing an ID scheme.

Note that transfer IDs are unique for the cluster -- not the ledger. If you want to store a
relationship between multiple transfers, such as indicating that multiple transfers on different
ledgers were part of a single transaction, you should store a transaction ID in one of the
[`user_data`](#user_data_128) fields.

### `debit_account_id`

This refers to the account to debit the transfer's [`amount`](#amount).

Constraints:

- Type is 128-bit unsigned integer (16 bytes)
- When `flags.post_pending_transfer` and `flags.void_pending_transfer` are _not_ set:
  - Must match an existing account
  - Must not be the same as `credit_account_id`
- When `flags.post_pending_transfer` or `flags.void_pending_transfer` are set:
  - If `debit_account_id` is zero, it will be automatically set to the pending transfer's
    `debit_account_id`.
  - If `debit_account_id` is nonzero, it must match the corresponding pending transfer's
    `debit_account_id`.
- When `flags.imported` is set:
  - The matching account's [timestamp](account.md#timestamp) must be less than or equal to the
    transfer's [timestamp](#timestamp).

### `credit_account_id`

This refers to the account to credit the transfer's [`amount`](#amount).

Constraints:

- Type is 128-bit unsigned integer (16 bytes)
- When `flags.post_pending_transfer` and `flags.void_pending_transfer` are _not_ set:
  - Must match an existing account
  - Must not be the same as `debit_account_id`
- When `flags.post_pending_transfer` or `flags.void_pending_transfer` are set:
  - If `credit_account_id` is zero, it will be automatically set to the pending transfer's
    `credit_account_id`.
  - If `credit_account_id` is nonzero, it must match the corresponding pending transfer's
    `credit_account_id`.
- When `flags.imported` is set:
  - The matching account's [timestamp](account.md#timestamp) must be less than or equal to the
    transfer's [timestamp](#timestamp).

### `amount`

This is how much should be debited from the `debit_account_id` account and credited to the
`credit_account_id` account.

Note that this is an unsigned 128-bit integer. You can read more about using
[debits and credits](../coding/data-modeling.md#debits-vs-credits) to represent positive and
negative balances as well as
[fractional amounts and asset scales](../coding/data-modeling.md#fractional-amounts-and-asset-scale).

- When `flags.balancing_debit` is set, this is the maximum amount that will be debited/credited,
  where the actual transfer amount is determined by the debit account's constraints.
- When `flags.balancing_credit` is set, this is the maximum amount that will be debited/credited,
  where the actual transfer amount is determined by the credit account's constraints.
- When `flags.post_pending_transfer` is set, the amount posted will be:
  - the pending transfer's amount, when the posted transfer's `amount` is `AMOUNT_MAX`
  - the posting transfer's amount, when the posted transfer's `amount` is less than or equal to the
    pending transfer's amount.

Constraints:

- Type is 128-bit unsigned integer (16 bytes)
- When `flags.void_pending_transfer` is set:
  - If `amount` is zero, it will be automatically be set to the pending transfer's `amount`.
  - If `amount` is nonzero, it must be equal to the pending transfer's `amount`.
- When `flags.post_pending_transfer` is set:
  - If `amount` is `AMOUNT_MAX` (`2^128 - 1`), it will automatically be set to the pending
    transfer's `amount`.
  - If `amount` is not `AMOUNT_MAX`, it must be less than or equal to the pending transfer's
    `amount`.

<details>
<summary>Client release &lt; 0.16.0</summary>

Additional constraints:

- When `flags.post_pending_transfer` is set:
  - If `amount` is zero, it will be automatically be set to the pending transfer's `amount`.
  - If `amount` is nonzero, it must be less than or equal to the pending transfer's `amount`.
- When `flags.balancing_debit` and/or `flags.balancing_credit` is set, if `amount` is zero, it will
  automatically be set to the maximum amount that does not violate the corresponding account limits.
  (Equivalent to setting `amount = 2^128 - 1`).
- When all of the following flags are not set, `amount` must be nonzero:
  - `flags.post_pending_transfer`
  - `flags.void_pending_transfer`
  - `flags.balancing_debit`
  - `flags.balancing_credit`

</details>

#### Examples

- For representing fractional amounts (e.g. `$12.34`), see
  [Fractional Amounts](../coding/data-modeling.md#fractional-amounts-and-asset-scale).
- For balancing transfers, see [Close Account](../coding/recipes/close-account.md).

### `pending_id`

If this transfer will post or void a pending transfer, `pending_id` references that pending
transfer. If this is not a post or void transfer, it must be zero.

See the section on [Two-Phase Transfers](../coding/two-phase-transfers.md) for more information on
how the `pending_id` is used.

Constraints:

- Type is 128-bit unsigned integer (16 bytes)
- Must be zero if neither void nor pending transfer flag is set
- Must match an existing transfer's [`id`](#id) if non-zero

### `user_data_128`

This is an optional 128-bit secondary identifier to link this transfer to an external entity or
event.

When set to zero, no secondary identifier will be associated with the account, therefore only
non-zero values can be used as [query filter](./query-filter.md).

When set to zero, if
[`flags.post_pending_transfer`](#flagspost_pending_transfer) or
[`flags.void_pending_transfer`](#flagsvoid_pending_transfer) is set, then
it will be automatically set to the pending transfer's `user_data_128`.

As an example, you might generate a
[TigerBeetle Time-Based Identifier](../coding/data-modeling.md#tigerbeetle-time-based-identifiers-recommended)
that ties together a group of transfers.

For more information, see [Data Modeling](../coding/data-modeling.md#user_data).

Constraints:

- Type is 128-bit unsigned integer (16 bytes)

### `user_data_64`

This is an optional 64-bit secondary identifier to link this transfer to an external entity or
event.

When set to zero, no secondary identifier will be associated with the account, therefore only
non-zero values can be used as [query filter](./query-filter.md).

When set to zero, if
[`flags.post_pending_transfer`](#flagspost_pending_transfer) or
[`flags.void_pending_transfer`](#flagsvoid_pending_transfer) is set, then
it will be automatically set to the pending transfer's `user_data_64`.

As an example, you might use this field store an external timestamp.

For more information, see [Data Modeling](../coding/data-modeling.md#user_data).

Constraints:

- Type is 64-bit unsigned integer (8 bytes)

### `user_data_32`

This is an optional 32-bit secondary identifier to link this transfer to an external entity or
event.

When set to zero, no secondary identifier will be associated with the account, therefore only
non-zero values can be used as [query filter](./query-filter.md).

When set to zero, if
[`flags.post_pending_transfer`](#flagspost_pending_transfer) or
[`flags.void_pending_transfer`](#flagsvoid_pending_transfer) is set, then
it will be automatically set to the pending transfer's `user_data_32`.

As an example, you might use this field to store a timezone or locale.

For more information, see [Data Modeling](../coding/data-modeling.md#user_data).

Constraints:

- Type is 32-bit unsigned integer (4 bytes)

### `timeout`

This is the interval in seconds after a [`pending`](#flagspending) transfer's
[arrival at the cluster](#timestamp) that it may be [posted](#flagspost_pending_transfer) or
[voided](#flagsvoid_pending_transfer). Zero denotes absence of timeout.

Non-pending transfers cannot have a timeout.

Imported transfers cannot have a timeout.

TigerBeetle makes a best-effort approach to remove pending balances of expired transfers
automatically:

- Transfers expire _exactly_ at their expiry time ([`timestamp`](#timestamp) _plus_ `timeout`
  converted in nanoseconds).

- Pending balances will never be removed before its expiry.

- Expired transfers cannot be manually posted or voided.

- It is not guaranteed that the pending balance will be removed exactly at its expiry.

  In particular, client requests may observe still-pending balances for expired transfers.

- Pending balances are removed in chronological order by expiry. If multiple transfers expire at the
  same time, then ordered by the transfer's creation [`timestamp`](#timestamp).

  If a transfer `A` has expiry `E₁` and transfer `B` has expiry `E₂`, and `E₁<E₂`, if transfer `B`
  had the pending balance removed, then transfer `A` had the pending balance removed as well.

Constraints:

- Type is 32-bit unsigned integer (4 bytes)
- Must be zero if `flags.pending` is _not_ set
- Must be zero if `flags.imported` is set.

The `timeout` is an interval in seconds rather than an absolute timestamp because this is more
robust to clock skew between the cluster and the application. (Watch this talk on
[Detecting Clock Sync Failure in Highly Available Systems](https://youtu.be/7R-Iz6sJG6Q?si=9sD2TpfD29AxUjOY)
on YouTube for more details.)

### `ledger`

This is an identifier that partitions the sets of accounts that can transact with each other.

See [data modeling](../coding/data-modeling.md#ledgers) for more details about how to think about
setting up your ledgers.

Constraints:

- Type is 32-bit unsigned integer (4 bytes)
- When `flags.post_pending_transfer` or `flags.void_pending_transfer` is set:
  - If `ledger` is zero, it will be automatically be set to the pending transfer's `ledger`.
  - If `ledger` is nonzero, it must match the `ledger` value on the pending transfer's
    `debit_account_id` **and** `credit_account_id`.
- When `flags.post_pending_transfer` and `flags.void_pending_transfer` are not set:
  - `ledger` must not be zero.
  - `ledger` must match the `ledger` value on the accounts referenced in `debit_account_id` **and**
    `credit_account_id`.

### `code`

This is a user-defined enum denoting the reason for (or category of) the transfer.

Constraints:

- Type is 16-bit unsigned integer (2 bytes)
- When `flags.post_pending_transfer` or `flags.void_pending_transfer` is set:
  - If `code` is zero, it will be automatically be set to the pending transfer's `code`.
  - If `code` is nonzero, it must match the pending transfer's `code`.
- When `flags.post_pending_transfer` and `flags.void_pending_transfer` are not set, `code` must not
  be zero.

### `flags`

This specifies (optional) transfer behavior.

Constraints:

- Type is 16-bit unsigned integer (2 bytes)
- Some flags are mutually exclusive; see
  [`flags_are_mutually_exclusive`](./requests/create_transfers.md#flags_are_mutually_exclusive).

#### `flags.linked`

This flag links the result of this transfer to the outcome of the next transfer in the request such
that they will either succeed or fail together.

The last transfer in a chain of linked transfers does **not** have this flag set.

You can read more about [linked events](./requests/README.md#linked-events).

##### Examples

- [Currency Exchange](../coding/recipes/currency-exchange.md)

#### `flags.pending`

Mark the transfer as a
[pending transfer](../coding/two-phase-transfers.md#reserve-funds-pending-transfer).

#### `flags.post_pending_transfer`

Mark the transfer as a
[post-pending transfer](../coding/two-phase-transfers.md#post-pending-transfer).

#### `flags.void_pending_transfer`

Mark the transfer as a
[void-pending transfer](../coding/two-phase-transfers.md#void-pending-transfer).

#### `flags.balancing_debit`

Transfer at most [`amount`](#amount) — automatically transferring less than `amount` as necessary
such that
`debit_account.debits_pending + debit_account.debits_posted ≤ debit_account.credits_posted`.

The `amount` of the recorded transfer is set to the actual amount that was transferred, which is
less than or equal to the amount that was passed to `create_transfers`.

Retrying a balancing transfer will return
[`exists_with_different_amount`](./requests/create_transfers.md#exists_with_different_amount)
only when the maximum amount passed to `create_transfers` is insufficient to fulfill the amount
that was actually transferred.
Otherwise it may return [`exists`](./requests/create_transfers.md#exists) even if the retry amount
differs from the original value.

`flags.balancing_debit` is exclusive with the
`flags.post_pending_transfer`/`flags.void_pending_transfer` flags because posting or voiding a
pending transfer will never exceed/overflow either account's limits.

`flags.balancing_debit` is compatible with (and orthogonal to) `flags.balancing_credit`.

<details>
<summary>Client release &lt; 0.16.0</summary>

Transfer at most [`amount`](#amount) — automatically transferring less than `amount` as necessary
such that
`debit_account.debits_pending + debit_account.debits_posted ≤ debit_account.credits_posted`. If
`amount` is set to `0`, transfer at most `2^64 - 1` (i.e. as much as possible).

If the highest amount transferable is `0`, returns
[`exceeds_credits`](./requests/create_transfers.md#exceeds_credits).

</details>

##### Examples

- [Close Account](../coding/recipes/close-account.md)

#### `flags.balancing_credit`

Transfer at most [`amount`](#amount) — automatically transferring less than `amount` as necessary
such that
`credit_account.credits_pending + credit_account.credits_posted ≤ credit_account.debits_posted`.

The `amount` of the recorded transfer is set to the actual amount that was transferred, which is
less than or equal to the amount that was passed to `create_transfers`.

Retrying a balancing transfer will return
[`exists_with_different_amount`](./requests/create_transfers.md#exists_with_different_amount)
only when the maximum amount passed to `create_transfers` is insufficient to fulfill the amount
that was actually transferred.
Otherwise it may return [`exists`](./requests/create_transfers.md#exists) even if the retry amount
differs from the original value.

`flags.balancing_credit` is exclusive with the
`flags.post_pending_transfer`/`flags.void_pending_transfer` flags because posting or voiding a
pending transfer will never exceed/overflow either account's limits.

`flags.balancing_credit` is compatible with (and orthogonal to) `flags.balancing_debit`.

<details>
<summary>Client release &lt; 0.16.0</summary>

Transfer at most [`amount`](#amount) — automatically transferring less than `amount` as necessary
such that
`credit_account.credits_pending + credit_account.credits_posted ≤ credit_account.debits_posted`. If
`amount` is set to `0`, transfer at most `2^64 - 1` (i.e. as much as possible).

If the highest amount transferable is `0`, returns
[`exceeds_debits`](./requests/create_transfers.md#exceeds_debits).

</details>

##### Examples

- [Close Account](../coding/recipes/close-account.md)

#### `flags.closing_debit`

When set, it will cause the [`Account.flags.closed`](account.md#flagsclosed) flag
of the [debit account](#debit_account_id) to be set if the transfer succeeds.

This flag requires a [two-phase transfer](#modes), so the flag [`flags.pending`](#flagspending)
must also be set. This ensures that closing transfers are reversible by
[voiding](#flagsvoid_pending_transfer) the pending transfer, and requires that the reversal
operation references the corresponding closing transfer, guarding against unexpected interleaving
of close/unclose operations.

#### `flags.closing_credit`

When set, it will cause the [`Account.flags.closed`](account.md#flagsclosed) flag
of the [credit account](#credit_account_id) to be set if the transfer succeeds.

This flag requires a [two-phase transfer](#modes), so the flag [`flags.pending`](#flagspending)
must also be set. This ensures that closing transfers are reversible by
[voiding](#flagsvoid_pending_transfer) the pending transfer, and requires that the reversal
operation references the corresponding closing transfer, guarding against unexpected interleaving
of close/unclose operations.

#### `flags.imported`

When set, allows importing historical `Transfer`s with their original [`timestamp`](#timestamp).

TigerBeetle will not use the [cluster clock](../coding/time.md) to assign the timestamp, allowing
the user to define it, expressing _when_ the transfer was effectively created by an external
event.

To maintain system invariants regarding auditability and traceability, some constraints are
necessary:

- It is not allowed to mix events with the `imported` flag set and _not_ set in the same batch.
  The application must submit batches of imported events separately.

- User-defined timestamps must be **unique** and expressed as nanoseconds since the UNIX epoch.
  No two objects can have the same timestamp, even different objects like an `Account` and a `Transfer` cannot share the same timestamp.

- User-defined timestamps must be a past date, never ahead of the cluster clock at the time the
  request arrives.

- Timestamps must be strictly increasing.

  Even user-defined timestamps that are required to be past dates need to be at least one
  nanosecond ahead of the timestamp of the last transfer committed by the cluster.

  Since the timestamp cannot regress, importing past events can be naturally restrictive without
  coordination, as the last timestamp can be updated using the cluster clock during regular
  cluster activity. Instead, it's recommended to import events only on a fresh cluster or
  during a scheduled maintenance window.

  It's recommended to submit the entire batch as a [linked chain](#flagslinked), ensuring that
  if any transfer fails, none of them are committed, preserving the last timestamp unchanged.
  This approach gives the application a chance to correct failed imported transfers, re-submitting
  the batch again with the same user-defined timestamps.

- Imported transfers cannot have a [`timeout`](#timeout).

  It's possible to import [pending](#flagspending) transfers with a user-defined timestamp,
  but since it's not driven by the cluster clock, it cannot define a
  [`timeout`](#timeout) for automatic expiration.
  In those cases, the [two-phase post or rollback](../coding/two-phase-transfers.md) must be
  done manually.

### `timestamp`

This is the time the transfer was created, as nanoseconds since UNIX epoch.
You can read more about [Time in TigerBeetle](../coding/time.md).

Constraints:

- Type is 64-bit unsigned integer (8 bytes)
- Must be `0` when the `Transfer` is created with [`flags.imported`](#flagsimported) _not_ set

  It is set by TigerBeetle to the moment the transfer arrives at the cluster.

- Must be greater than `0` and less than `2^63` when the `Transfer` is created with
  [`flags.imported`](#flagsimported) set

## Internals

If you're curious and want to learn more, you can find the source code for this struct in
[src/tigerbeetle.zig](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/tigerbeetle.zig).
Search for `const Transfer = extern struct {`.

You can find the source code for creating a transfer in
[src/state_machine.zig](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig).
Search for `fn create_transfer(`.
