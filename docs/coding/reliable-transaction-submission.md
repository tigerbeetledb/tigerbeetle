# Reliable Transaction Submission

When making payments or recording transfers, it is important to ensure that they are recorded once
and only once -- even if some parts of the system fail during the transaction.

There are some subtle gotchas to avoid, so this page describes how to submit events -- and
especially transfers -- reliably.

## The App or Browser Should Generate the ID

[`Transfer`s](../reference/transfer.md#id) and [`Account`s](../reference/account.md#id)
carry an `id` field that is used as an idempotency key to ensure the same object is not created
twice.

**The client software, such as your app or web page, that the user interacts with should generate
the `id` (not your API). This `id` should be persisted locally before submission, and the same `id`
should be used for subsequent retries.**

1. User initiates a transfer.
2. Client software (app, web page, etc) [generates the transfer `id`](./data-modeling.md#id).
3. Client software **persists the `id` in the app or browser local storage.**
4. Client software submits the transfer to your [API service](./system-architecture.md).
5. API service includes the transfer in a [request](../reference/requests/README.md).
6. TigerBeetle creates the transfer with the given `id` once and only once.
7. TigerBeetle responds to the API service.
8. The API service responds to the client software.

### Handling Network Failures

The method described above handles various potential network failures. The request may be lost
before it reaches the API service or before it reaches TigerBeetle. Or, the response may be lost on
the way back from TigerBeetle.

Generating the `id` on the client side ensures that transfers can be safely retried. The app must
use the same `id` each time the transfer is resent.

If the transfer was already created before and then retried, TigerBeetle will return the
[`exists`](../reference/requests/create_transfers.md#exists) response code. If the transfer had
not already been created, it will be created and return the
[`ok`](../reference/requests/create_transfers.md#ok).

### Handling Client Software Restarts

The method described above also handles potential restarts of the app or browser while the request
is in flight.

It is important to **persist the `id` to local storage on the client's device before submitting the
transfer**. When the app or web page reloads, it should resubmit the transfer using the same `id`.

This ensures that the operation can be safely retried even if the client app or browser restarts
before receiving the response to the operation. Similar to the case of a network failure,
TigerBeetle will respond with the [`ok`](../reference/requests/create_transfers.md#ok) if a
transfer is newly created and [`exists`](../reference/requests/create_transfers.md#exists) if an
object with the same `id` was already created.
