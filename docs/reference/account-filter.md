# `AccountFilter`

An `AccountFilter` is a record containing the filter parameters for querying
the [account transfers](./requests/get_account_transfers.md)
and the [account historical balances](./requests/get_account_balances.md).

## Fields

### `account_id`

The unique [identifier](account.md#id) of the account for which the results will be retrieved.

Constraints:

- Type is 128-bit unsigned integer (16 bytes)
- Must not be zero or `2^128 - 1`

### `user_data_128`

Filter the results by the field [`Transfer.user_data_128`](transfer.md#user_data_128).
Optional; set to zero to disable the filter.

Constraints:

- Type is 128-bit unsigned integer (16 bytes)

### `user_data_64`

Filter the results by the field [`Transfer.user_data_64`](transfer.md#user_data_64).
Optional; set to zero to disable the filter.

Constraints:

- Type is 64-bit unsigned integer (8 bytes)

### `user_data_32`

Filter the results by the field [`Transfer.user_data_32`](transfer.md#user_data_32).
Optional; set to zero to disable the filter.

Constraints:

- Type is 32-bit unsigned integer (4 bytes)

### `code`

Filter the results by the [`Transfer.code`](transfer.md#code).
Optional; set to zero to disable the filter.

Constraints:

- Type is 16-bit unsigned integer (2 bytes)

### `reserved`

This space may be used for additional data in the future.

Constraints:

- Type is 58 bytes
- Must be zero

### `timestamp_min`

The minimum [`Transfer.timestamp`](transfer.md#timestamp) from which results will be returned, inclusive range.
Optional; set to zero to disable the lower-bound filter.

Constraints:

- Type is 64-bit unsigned integer (8 bytes)
- Must be less than `2^63`.

### `timestamp_max`

The maximum [`Transfer.timestamp`](transfer.md#timestamp) from which results will be returned, inclusive range.
Optional; set to zero to disable the upper-bound filter.

Constraints:

- Type is 64-bit unsigned integer (8 bytes)
- Must be less than `2^63`.

### `limit`

The maximum number of results that can be returned by this query.

Limited by the [maximum message size](./requests/README.md#batching-events).

Constraints:

- Type is 32-bit unsigned integer (4 bytes)
- Must not be zero

### `flags`

A bitfield that specifies querying behavior.

Constraints:

- Type is 32-bit unsigned integer (4 bytes)

#### `flags.debits`

Whether or not to include results where the field [`debit_account_id`](transfer.md#debit_account_id)
matches the parameter [`account_id`](#account_id).

#### `flags.credits`

Whether or not to include results where the field [`credit_account_id`](transfer.md#credit_account_id)
matches the parameter [`account_id`](#account_id).

#### `flags.reversed`

Whether the results are sorted by timestamp in chronological or reverse-chronological order. If the
flag is not set, the event that happened first (has the smallest timestamp) will come first. If the
flag is set, the event that happened last (has the largest timestamp) will come first.
