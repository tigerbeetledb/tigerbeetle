# `QueryFilter`

A `QueryFilter` is a record containing the filter parameters for
[querying accounts](./requests/query_accounts.md)
and [querying transfers](./requests/query_transfers.md).

## Fields

### `user_data_128`

Filter the results by the field [`Account.user_data_128`](account.md#user_data_128) or
[`Transfer.user_data_128`](transfer.md#user_data_128).
Optional; set to zero to disable the filter.

Constraints:

- Type is 128-bit unsigned integer (16 bytes)

### `user_data_64`

Filter the results by the field [`Account.user_data_64`](account.md#user_data_64) or
[`Transfer.user_data_64`](transfer.md#user_data_64).
Optional; set to zero to disable the filter.

Constraints:

- Type is 64-bit unsigned integer (8 bytes)

### `user_data_32`

Filter the results by the field [`Account.user_data_32`](account.md#user_data_32) or
[`Transfer.user_data_32`](transfer.md#user_data_32).
Optional; set to zero to disable the filter.

Constraints:

- Type is 32-bit unsigned integer (4 bytes)

### `ledger`

Filter the results by the field [`Account.ledger`](account.md#ledger) or
[`Transfer.ledger`](transfer.md#ledger).
Optional; set to zero to disable the filter.

Constraints:

- Type is 32-bit unsigned integer (4 bytes)

### `code`

Filter the results by the field [`Account.code`](account.md#code) or
[`Transfer.code`](transfer.md#code).
Optional; set to zero to disable the filter.

Constraints:

- Type is 16-bit unsigned integer (2 bytes)

### `timestamp_min`

The minimum [`Account.timestamp`](account.md#timestamp) or
[`Transfer.timestamp`](transfer.md#timestamp) from which results will be returned,
inclusive range.
Optional; set to zero to disable the lower-bound filter.

Constraints:

- Type is 64-bit unsigned integer (8 bytes)
- Must not be `2^64 - 1`

### `timestamp_max`

The maximum [`Account.timestamp`](account.md#timestamp) or
[`Transfer.timestamp`](transfer.md#timestamp) from which results will be returned,
inclusive range.
Optional; set to zero to disable the upper-bound filter.

Constraints:

- Type is 64-bit unsigned integer (8 bytes)
- Must not be `2^64 - 1`

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

#### `flags.reversed`

Whether the results are sorted by timestamp in chronological or reverse-chronological order. If the
flag is not set, the event that happened first (has the smallest timestamp) will come first. If the
flag is set, the event that happened last (has the largest timestamp) will come first.

### `reserved`

This space may be used for additional data in the future.

Constraints:

- Type is 6 bytes
- Must be zero
