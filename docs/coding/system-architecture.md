# TigerBeetle in Your System Architecture

TigerBeetle is an Online Transaction Processing (OLTP) database built for safety and performance. It
is not a general purpose database like PostgreSQL or MySQL. Instead, TigerBeetle works alongside
your general purpose database, which we refer to as an Online General Purpose (OLGP) database.

TigerBeetle should be used in the data plane, or hot path of transaction processing, while your
general purpose database is used in the control plane and may be used for storing information or
metadata that is updated less frequently.

![TigerBeetle in Your System Architecture](https://github.com/tigerbeetle/tigerbeetle/assets/3262610/c145acd5-ec6e-49f4-9bd0-77cd242d2b8f)

## Division of Responsibilities

**App or Website**

- Initiate transactions
- [Generate Transfer and Account IDs](./reliable-transaction-submission.md#the-app-or-browser-should-generate-the-id)

**Stateless API Service**

- Handle authentication and authorization
- Create account records in both the general purpose database and TigerBeetle when users sign up
- [Cache ledger metadata](#ledger-account-and-transfer-types)
- [Batch transfers](../reference/requests/README.md#batching-events)
- Apply exchange rates for [currency exchange](./recipes/currency-exchange.md) transactions

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
  [`user_data`](./data-modeling.md#user_data) fields

## Ledger, Account, and Transfer Types

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

| Hard-coded in app or cached                                         | In TigerBeetle                                                                               |
| ------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Currency or asset code's string representation (for example, "USD") | [`ledger`](./data-modeling.md#asset-scale) and [asset scale](./data-modeling.md#asset-scale) |
| Account type's string representation (for example, "cash")          | [`code`](./data-modeling.md#code)                                                            |
| Transfer type's string representation (for example, "refund")       | [`code`](./data-modeling.md#code)                                                            |

## Authentication

TigerBeetle does not support authentication. You should never allow untrusted users or services to
interact with it directly.

Also, untrusted processes must not be able to access or modify TigerBeetle's on-disk data file.
