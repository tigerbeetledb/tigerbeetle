# `AccountBalance`

An `AccountBalance` is a record storing the [`Account`](./account.md)'s balance at a given point in
time.

Only Accounts with the flag [`history`](./account.md#flagshistory) set retain
[historical balances](https://docs.tigerbeetle.com/reference/requests/get_account_balances).

## Fields

### `timestamp`

This is the time the account balance was updated, as nanoseconds since UNIX epoch.

The timestamp refers to the same [`Transfer.timestamp`](./transfer.md#timestamp) which changed the
[`Account`](./account.md).

The amounts refer to the account balance recorded _after_ the transfer execution.

Constraints:

- Type is 64-bit unsigned integer (8 bytes)

### `debits_pending`

Amount of [pending debits](./account.md#debits_pending).

Constraints:

- Type is 128-bit unsigned integer (16 bytes)

### `debits_posted`

Amount of [posted debits](./account.md#debits_posted).

Constraints:

- Type is 128-bit unsigned integer (16 bytes)

### `credits_pending`

Amount of [pending credits](./account.md#credits_pending).

Constraints:

- Type is 128-bit unsigned integer (16 bytes)

### `credits_posted`

Amount of [posted credits](./account.md#credits_posted).

Constraints:

- Type is 128-bit unsigned integer (16 bytes)

### `reserved`

This space may be used for additional data in the future.

Constraints:

- Type is 56 bytes
- Must be zero
