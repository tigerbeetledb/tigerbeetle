---
sidebar_position: 3
sidebar_label: OLTP and OLGP
---

# OLTP and General Purpose Databases

General purpose databases such as PostgreSQL or MySQL, which we refer to as Online General Purpose
(OLGP) databases, have served us well for the past 30 years. However, they were not built to handle
the extreme write contention and volume of today's OLTP workloads.

TigerBeetle is built from the ground up for realtime business transaction processing. However, there
is still very much a role for general purpose databases, and TigerBeetle works well in combination
with them.

## TigerBeetle in Your System Architecture

TigerBeetle should be used in the data plane, or hot path of transaction processing, while your
general purpose database is used in the control plane and may be used for storing information or
metadata that is updated less frequently.

![TigerBeetle in Your System Architecture](https://github.com/tigerbeetle/tigerbeetle/assets/3262610/c145acd5-ec6e-49f4-9bd0-77cd242d2b8f)

## Separation of Concerns

**App or Website**

- Initiate transactions
- [Generate Transfer and Account IDs](./reliable-transaction-submission.md#the-app-or-browser-should-generate-the-id)

**Stateless API Service**

- Handle authentication and authorization
- Create account records in both the general purpose database and TigerBeetle when users sign up
- [Cache ledger metadata](#ledger-account-and-transfer-types)
- [Batch transfers](../reference/requests/README.md#batching-events)
- Apply exchange rates for [currency exchange](../develop/recipes/currency-exchange.md) transactions

**General Purpose (OLGP) Database**

- Store metadata about ledgers and accounts (such as string names or descriptions)
- Store mappings between [integer type identifiers](#ledger-account-and-transfer-types) used in
  TigerBeetle and string representations used by the app and API

**TigerBeetle (OLTP) Database**

- Record transfers between accounts
- Track balances for accounts
- Enforce balance limits
- Enforce financial consistency through double-entry bookkeeping
- Enforce strict serializability of events
- Optionally store pointers to records or entities in the general purpose database in the
  [`user_data`](../develop/data-modeling.md#user_data) fields

## The TigerBeetle API

TigerBeetle uses a highly efficient binary protocol over TCP, which is implemented by the client
libraries. TigerBeetle does not use SQL.

All requests to TigerBeetle support
[batching events](../reference/requests/README.md#batching-events) for high throughput.

The TigerBeetle data model mainly consists of
[Ledgers, Accounts, and Transfers](../develop/data-modeling.md#accounts-transfers-and-ledgers), and
there are various [request types](../reference/requests/README.md) for creating and querying
accounts and transfers.

You can read more about mapping your application logic to TigerBeetle's schema in the
[Data Modeling guide](../develop/data-modeling.md).

### Mapping Type Identifiers

For performance reasons, TigerBeetle stores the ledger, account, and transfer types as simple
integers. Most likely, you will want these integers to map to enums of type names or strings, along
with other associated metadata.

The mapping from the string representation of these types to the integers used within TigerBeetle
may be hard-coded into your application logic or stored in a general purpose (OLGP) database and
cached by your application. (These mappings should be immutable and append-only, so there is no
concern about cache invalidation.)

⚠️ Importantly, **initiating a transfer should not require fetching metadata from the general
purpose database**. If it does, that database will become the bottleneck and will negate the
performance gains from using TigerBeetle.

Specifically, the types of information that fit into this category include:

| Hard-coded in app or cached                                         | In TigerBeetle                                                                                                 |
| ------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Currency or asset code's string representation (for example, "USD") | [`ledger`](../develop/data-modeling.md#asset-scale) and [asset scale](../develop/data-modeling.md#asset-scale) |
| Account type's string representation (for example, "cash")          | [`code`](../develop/data-modeling.md#code)                                                                     |
| Transfer type's string representation (for example, "refund")       | [`code`](../develop/data-modeling.md#code)                                                                     |

### Authentication

TigerBeetle does not support authentication. You should never allow untrusted users or services to
interact with it directly.

Also, untrusted processes must not be able to access or modify TigerBeetle's on-disk data file.

## Next: High Throughput, Low Latency

We've alluded to TigerBeetle being high performance, but how does it achieve this -- and do you need
such high performance? We'll dive into both of these in the next section:
[High Throughput, Low Latency](./performance.md).
