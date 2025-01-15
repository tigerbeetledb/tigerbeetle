---
sidebar_position: 1
sidebar_label: OLTP
---

# Online Transaction Processing (OLTP)

Online Transaction Processing (OLTP) is about **recording business transactions in real-time**. This
could be payments, sales, car sharing rides, game scores, or API usage.

## The World is Becoming More Transactional

For the last 20-30 years, OLTP has been handled by general purpose databases like PostgreSQL, MySQL,
and SQLite, which we refer to as Online General Purpose (OLGP) databases.

General purpose databases, however, struggle to keep up with the extreme write contention of growing
OLTP workloads today, which have increased by 3-4 orders of magnitude in the last 10 years alone.

For example:

- The UPI real-time payments switch in India processed 10 billion payments in 2019. The switch
  processed 12 billion payments in the month of January 2024 alone.
- With the moves to cleaner energy and smart metering, energy is being traded by the Kilowatt-hour
  and billed to customers every 15 or 30 minutes rather than at the end of the month.
- Serverless APIs charge for usage by the second or per-request, rather than per month. (Today,
  serverless billing at scale is often implemented using MapReduce, which makes it difficult or
  impossible to offer customers real-time spending caps.)

**TigerBeetle is built to handle the scale of OLTP workloads today and for the decades to come.** It
works well alongside a general purpose database. The latter holds data that isn't updated very
frequently, such as account metadata, while TigerBeetle can race ahead processing transactions and
giving your system unparalleled latency and throughput.

## Write-Heavy Workloads

A distinguishing characteristic of OLTP is that it is focused on _recording_ business transactions,
and is thus write-heavy. General purpose databases are often designed for workloads that are
balanced between reads and writes or are read-heavy.

TigerBeetle is optimized from the ground up for a write-heavy workload in order to handle the
massively increasing scale of OLTP.

## High Contention on Hot Accounts

Business transactions always involve more than one account. Someone might be getting paid, but then
there are fees, taxes, revenue splits, and other costs to account for.

OLTP systems will often have some accounts that are involved in a high percentage of all
transactions. This is especially true for house accounts, or those that represent the income or
expenses of the business itself. If we use locks to ensure that updates to hot accounts are
consistent, the contention on those accounts will bring the system's performance to a crawl.

TigerBeetle provides strong consistency guarantees without row locks to entirely sidestep the issue
of contention on hot accounts. In fact, due to TigerBeetle's use of the system cache, hot accounts
_increase_ the speed at which transactions can be processed.

## Business Transactions Don't Shard Well

One of the most common ways of scaling systems is to horizontally scale or shard them, such that
different servers process different sets of transactions. Unfortunately, business transactions don't
shard well.

Horizontal scaling works poorly for OLTP because most accounts cannot be neatly partitioned between
shards. Transactions between accounts on different shards become even more complex and slow. Row
locks on hot accounts even worse when the transactions must execute across shards.

Another approach to scaling OLTP systems is to use
[MapReduce](https://en.wikipedia.org/wiki/MapReduce) for billing. Unfortunately, this makes it
almost impossible to provide real-time balance reporting or real-time spending limits, because
balances are only calculated periodically in batches. This is a poor user experience that cannot
readily be fixed after the system is designed.

TigerBeetle intentionally uses a single-core design, along with numerous unique performance
optimizations, to provide high throughput without the downsides of horizontal scaling.

## Bottleneck for Your System

You can only do as much business as your database supports. As such, you need a core OLTP database
capable of handling your transactions today, on your busiest days, and for the decades to come.

TigerBeetle is designed to handle **1 million transactions per second**. For comparison, the entire
Visa network processes an average of around 9,000 transactions per second[^1] and supports a maximum
of around 65,000 transactions per second[^2].

[^1]:
    The
    [Visa Fact Sheet](https://usa.visa.com/dam/VCOM/global/about-visa/documents/aboutvisafactsheet.pdf)
    reports that the network processed 276.3 billion transactions in 2023.

[^2]:
    Visa.
    [_A deep dive on Solana, a high performance blockchain network_](https://usa.visa.com/solutions/crypto/deep-dive-on-solana.html). 2023.

## Next Up: Debit / Credit is the Schema for OLTP

OLTP workloads are massively increasing and we need a system designed from the ground up for extreme
write contention. Since we know we need a new database for this purpose, we can now think about what
the perfect schema and language is for OLTP: [Debit / Credit](./debit-credit.md).
