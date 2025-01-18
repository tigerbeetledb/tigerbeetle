# `Account`

An `Account` is a record storing the cumulative effect of committed [transfers](./transfer.md).

### Updates

Account fields _cannot be changed by the user_ after creation. However, debits and credits fields
are updated by TigerBeetle as transfers move money to and from an account.

### Deletion

Accounts **cannot be deleted** after creation. This provides a strong guarantee for an audit trail
-- and the account record is only 128 bytes.

If an account is no longer in use, you may want to
[zero out its balance](../coding/recipes/close-account.md).

### Guarantees

- Accounts are immutable. They are never modified once they are successfully created (excluding
  balance fields, which are modified by transfers).
- There is at most one `Account` with a particular [`id`](#id).
- The sum of all accounts' [`debits_pending`](#debits_pending) equals the sum of all accounts'
  [`credits_pending`](#credits_pending).
- The sum of all accounts' [`debits_posted`](#debits_posted) equals the sum of all accounts'
  [`credits_posted`](#credits_posted).

## Fields

### `id`

This is a unique, client-defined identifier for the account.

Constraints:

- Type is 128-bit unsigned integer (16 bytes)
- Must not be zero or `2^128 - 1` (the highest 128-bit unsigned integer)
- Must not conflict with another account in the cluster

See the [`id` section in the data modeling doc](../coding/data-modeling.md#id) for more
recommendations on choosing an ID scheme.

Note that account IDs are unique for the cluster -- not per ledger. If you want to store a
relationship between accounts, such as indicating that multiple accounts on different ledgers belong
to the same user, you should store a user ID in one of the [`user_data`](#user_data_128) fields.

### `debits_pending`

`debits_pending` counts debits reserved by pending transfers. When a pending transfer posts, voids,
or times out, the amount is removed from `debits_pending`.

Money in `debits_pending` is reserved — that is, it cannot be spent until the corresponding pending
transfer resolves.

Constraints:

- Type is 128-bit unsigned integer (16 bytes)
- Must be zero when the account is created

### `debits_posted`

Amount of posted debits.

Constraints:

- Type is 128-bit unsigned integer (16 bytes)
- Must be zero when the account is created

### `credits_pending`

`credits_pending` counts credits reserved by pending transfers. When a pending transfer posts,
voids, or times out, the amount is removed from `credits_pending`.

Money in `credits_pending` is reserved — that is, it cannot be spent until the corresponding pending
transfer resolves.

Constraints:

- Type is 128-bit unsigned integer (16 bytes)
- Must be zero when the account is created

### `credits_posted`

Amount of posted credits.

Constraints:

- Type is 128-bit unsigned integer (16 bytes)
- Must be zero when the account is created

### `user_data_128`

This is an optional 128-bit secondary identifier to link this account to an external entity or
event.

When set to zero, no secondary identifier will be associated with the account, therefore only
non-zero values can be used as [query filter](./query-filter.md).

As an example, you might use a
[ULID](../coding/data-modeling.md#tigerbeetle-time-based-identifiers-recommended) that ties together
a group of accounts.

For more information, see [Data Modeling](../coding/data-modeling.md#user_data).

Constraints:

- Type is 128-bit unsigned integer (16 bytes)

### `user_data_64`

This is an optional 64-bit secondary identifier to link this account to an external entity or event.

When set to zero, no secondary identifier will be associated with the account, therefore only
non-zero values can be used as [query filter](./query-filter.md).

As an example, you might use this field store an external timestamp.

For more information, see [Data Modeling](../coding/data-modeling.md#user_data).

Constraints:

- Type is 64-bit unsigned integer (8 bytes)

### `user_data_32`

This is an optional 32-bit secondary identifier to link this account to an external entity or event.

When set to zero, no secondary identifier will be associated with the account, therefore only
non-zero values can be used as [query filter](./query-filter.md).

As an example, you might use this field to store a timezone or locale.

For more information, see [Data Modeling](../coding/data-modeling.md#user_data).

Constraints:

- Type is 32-bit unsigned integer (4 bytes)

### `reserved`

This space may be used for additional data in the future.

Constraints:

- Type is 4 bytes
- Must be zero

### `ledger`

This is an identifier that partitions the sets of accounts that can transact with each other.

See [data modeling](../coding/data-modeling.md#ledgers) for more details about how to think about
setting up your ledgers.

Constraints:

- Type is 32-bit unsigned integer (4 bytes)
- Must not be zero

### `code`

This is a user-defined enum denoting the category of the account.

As an example, you might use codes `1000`-`3340` to indicate asset accounts in general, where `1001`
is Bank Account and `1002` is Money Market Account and `2003` is Motor Vehicles and so on.

Constraints:

- Type is 16-bit unsigned integer (2 bytes)
- Must not be zero

### `flags`

A bitfield that toggles additional behavior.

Constraints:

- Type is 16-bit unsigned integer (2 bytes)
- Some flags are mutually exclusive; see
  [`flags_are_mutually_exclusive`](./requests/create_accounts.md#flags_are_mutually_exclusive).

#### `flags.linked`

This flag links the result of this account creation to the result of the next one in the request,
such that they will either succeed or fail together.

The last account in a chain of linked accounts does **not** have this flag set.

You can read more about [linked events](./requests/README.md#linked-events).

#### `flags.debits_must_not_exceed_credits`

When set, transfers will be rejected that would cause this account's debits to exceed credits.
Specifically when
`account.debits_pending + account.debits_posted + transfer.amount > account.credits_posted`.

This cannot be set when `credits_must_not_exceed_debits` is also set.

#### `flags.credits_must_not_exceed_debits`

When set, transfers will be rejected that would cause this account's credits to exceed debits.
Specifically when
`account.credits_pending + account.credits_posted + transfer.amount > account.debits_posted`.

This cannot be set when `debits_must_not_exceed_credits` is also set.

#### `flags.history`

When set, the account will retain the history of balances at each transfer.

Note that the [`get_account_balances`](./requests/get_account_balances.md) operation only works for
accounts with this flag set.

#### `flags.imported`

When set, allows importing historical `Account`s with their original [`timestamp`](#timestamp).

TigerBeetle will not use the [cluster clock](../coding/time.md) to assign the timestamp, allowing
the user to define it, expressing _when_ the account was effectively created by an external
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
  nanosecond ahead of the timestamp of the last account committed by the cluster.

  Since the timestamp cannot regress, importing past events can be naturally restrictive without
  coordination, as the last timestamp can be updated using the cluster clock during regular
  cluster activity. Instead, it's recommended to import events only on a fresh cluster or
  during a scheduled maintenance window.

  It's recommended to submit the entire batch as a [linked chain](#flagslinked), ensuring that
  if any account fails, none of them are committed, preserving the last timestamp unchanged.
  This approach gives the application a chance to correct failed imported accounts, re-submitting
  the batch again with the same user-defined timestamps.

#### `flags.closed`

When set, the account will reject further transfers,
except for [voiding two-phase transfers](transfer.md#modes) that are still pending.

- This flag can be set during the account creation.
- This flag can also be set by sending a [two-phase pending transfer](transfer.md#flagspending)
  with the [`Transfer.flags.closing_debit`](transfer.md#flagsclosing_debit)
  and/or [`Transfer.flags.closing_credit`](transfer.md#flagsclosing_credit) flags set.
- This flag can be _unset_ by [voiding](transfer.md#flagsvoid_pending_transfer) the two-phase
  pending transfer that closed the account.

### `timestamp`

This is the time the account was created, as nanoseconds since UNIX epoch.
You can read more about [Time in TigerBeetle](../coding/time.md).

Constraints:

- Type is 64-bit unsigned integer (8 bytes)
- Must be `0` when the `Account` is created with [`flags.imported`](#flagsimported) _not_ set

  It is set by TigerBeetle to the moment the account arrives at the cluster.

- Must be greater than `0` and less than `2^63` when the `Account` is created with
  [`flags.imported`](#flagsimported) set

## Internals

If you're curious and want to learn more, you can find the source code for this struct in
[src/tigerbeetle.zig](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/tigerbeetle.zig).
Search for `const Account = extern struct {`.

You can find the source code for creating an account in
[src/state_machine.zig](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/state_machine.zig).
Search for `fn create_account(`.
