---
sidebar_position: 2
sidebar_label: Debit / Credit
---

# Debit / Credit: The Schema for OLTP

As discussed in the previous section, OLTP is all about processing business transactions. We saw
that the nuances of OLTP workloads make them tricky to handle at scale.

Now, we'll turn to the data model and see how the specifics of business transactions actually lend
themselves to an incredibly simple schema -- and one that's been in use for centuries!

## The "Who, What, When, Where, Why, and How Much" of OLTP

OLTP and business transactions tend to record the same types of information:

- **Who**: which accounts are transacting?
- **What**: what type of asset or value is moving?
- **When**: when was the transaction initiated or when was it finalized?
- **Where**: where in the world did the transaction take place?
- **Why**: what type of transaction is this or why is it happening?
- **How Much**: what quantity of the asset or items was moved?

## The Language of Business for Centuries

Debit / credit, or double-entry bookkeeping, has been the lingua franca of business and accounting
since at least the 13th century.

The key insight underpinning debit / credit systems is that every transfer records a movement of
value from one or more accounts to one or more accounts. Money never appears from nowhere or
disappears. This simple principle helps ensure that all of a business's money is accounted for.

Debit / credit perfectly captures the who, what, when, where, why, and how much of OLTP while
ensuring financial consistency.

(For a deeper dive on debits and credits, see our primer on
[Financial Accounting](../develop/financial-accounting.md).)

## SQL vs Debit / Credit

While SQL is a great query language for getting data out of a database, OLTP is primarily about
getting data in to the database and this is where SQL falls short.

**Often, a single business transaction requires multiple SQL queries (on the order of 10 SQL queries
per transaction)** and potentially even multiple round-trips from the application to the database.

By designing a database specifically for the schema and needs of OLTP, we can ensure our accounting
logic is enforced correctly while massively increasing performance.

## TigerBeetle Enforces Debit / Credit in the Database

The schema of OLTP is built in to TigerBeetle's data model, and is ready for you to use:

- **Who**: the [`debit_account_id`](../reference/transfer.md#debit_account_id) and
  [`credit_account_id`](../reference/transfer.md#credit_account_id) indicate which accounts are
  transacting.
- **What**: each asset or type of value in TigerBeetle is tracked on a separate
  [ledger](../develop/data-modeling.md#ledgers). The [`ledger`](../reference/transfer.md#ledger)
  field indicates what is being transferred.
- **When**: each transfer has a unique [`timestamp`](../reference/transfer.md#timestamp) for when it
  is processed by the cluster, but you can add another timestamp representing when the transaction
  happened in the real world in the [`user_data_64`](../reference/transfer.md#user_data_64) field.
- **Where**: the [`user_data_32`](../reference/transfer.md#user_data_32) can be used to store the
  locale where the transfer occurred.
- **Why**: the [`code`](../reference/transfer.md#code) field stores the reason a transfer occurred
  and should map to an enum or table of all the possible business events.
- **How Much**: the [`amount`](../reference/transfer.md#amount) indicates how much of the asset or
  item is being transferred.

Furthermore, accounting invariants such as balance limits are enforced within the database to avoid
round-trips between your database and application logic.

## Immutability is Essential

Another critical element of debit / credit systems is immutability: once transfers are recorded,
they cannot be erased. Reversals are implemented with separate transfers to provide a full and
auditable log of business events.

Accidentally dropping rows or tables is bad in any database, but it is unacceptable when it comes to
accounting. Legal compliance and good business practices require that all funds be fully accounted
for, and all history be maintained.

Transfers in TigerBeetle are always immutable, providing you with peace of mind out of the box.
There is no possibility of a malformed query unintentionally deleting data.

## Don't Roll Your Own Ledger

Many companies start out building their own system for recording business transactions. Then, once
their business scales, they realize they need a proper ledger and end up coming back to debits and
credits.

A number of prime examples of this are:

- **Uber**: In 2018, Uber started a 2-year, 40-engineer effort to migrate their collection and
  disbursement payment platform to one based on the principles of double-entry accounting and debits
  and credits.[^1]
- **Airbnb**: From 2012 to 2016, Airbnb used a MySQL-based data pipeline to record all of its
  transactions in an immutable store suitable for reporting. The pipeline became too complex, hard
  to scale, and slow. They ended up building a new financial reporting system based on double-entry
  accounting.[^2]
- **Stripe**: While we don't know when this system initially went into service, Stripe relies on an
  internal system based on double-entry accounting and an immutable log of events to record all of
  the payments they process.[^3]

[^1]:
    Singla, A., & Wu, S. (2020, October 2). _Revolutionizing Money Movements at Scale with Strong
    Data Consistency_. Uber Blog.
    [https://www.uber.com/blog/money-scale-strong-data](https://www.uber.com/blog/money-scale-strong-data).

[^2]:
    Liang, A. (2017, March 16). _Tracking the Money — Scaling Financial Reporting at Airbnb_. The
    Airbnb Tech Blog.
    [https://medium.com/airbnb-engineering/tracking-the-money-scaling-financial-reporting-at-airbnb-6d742b80f040](https://medium.com/airbnb-engineering/tracking-the-money-scaling-financial-reporting-at-airbnb-6d742b80f040).

[^3]:
    Ganelin, I. (2024, February 16). _Ledger: Stripe’s system for tracking and validating money
    movement_. Stripe Blog.
    [https://stripe.com/blog/ledger-stripe-system-for-tracking-and-validating-money-movement](https://stripe.com/blog/ledger-stripe-system-for-tracking-and-validating-money-movement).

## Standardized, Simple, and Scalable

From one perspective, debit / credit may seem like a limited data model. However, it is incredibly
flexible and scalable. Any business event can be recorded as debits and credits -- indeed,
accountants have been doing precisely this for centuries!

Instead of modeling business transactions as a set of ad-hoc tables and relationships, debits and
credits provide a simple and standardized schema that can be used across all product lines, now and
into the future. This avoids the need to add columns, tables, and complex relations between them as
new features are added -- and avoids complex schema migrations.

Debit / credit has been the foundation of business for hundreds of years, and now you can leverage
TigerBeetle's high-performance implementation of it built for OLTP in the 21st century.

## Next: OLTP and General Purpose Databases

So far, we've seen why we need a new database designed for OLTP and how debit / credit provides the
perfect data model for it. Next, we can look at the relationship and separation of concerns between
OLTP and general purpose databases.
