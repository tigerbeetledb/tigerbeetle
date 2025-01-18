# Built for OLTP

Online Transaction Processing (OLTP) is about recording business transactions in real-time. This
could be payments, sales, car sharing rides, or API usage.

For the last 20-30 years, OLTP has been handled by general purpose databases like PostgreSQL, MySQL,
and SQLite, which we refer to as Online General Purpose (OLGP) databases. However, general purpose
databases struggle to keep up with the extreme write contention of growing OLTP workloads today,
which have increased by 3-4 orders of magnitude in the last 10 years alone.

For example:

- In 2019, the UPI real-time payments switch in India processed 10 billion payments. In the month of
  January 2024 alone, the switch processed 12 billion payments.
- With the moves to cleaner energy and smart metering, energy is being traded by the Kilowatt-hour
  and billed to customers every 15 or 30 minutes rather than at the end of the month.
- Serverless APIs charge for usage by the second or per-request, rather than per month. (Today,
  serverless billing at scale is often implemented using MapReduce, which makes it difficult or
  impossible to offer customers real-time spending caps.)

**TigerBeetle is built to handle the scale of OLTP workloads today and for the decades to come.** It
works well alongside a general purpose database. The latter holds data that isn't updated very
frequently, such as account metadata, while TigerBeetle can race ahead processing transactions and
giving your system unparalleled latency and throughput.

## OLTP Requires High Performance

Transaction processing is the core of most business' software, which makes it the bottleneck for
increasing sales, processing payments, or serving API requests faster.

Because performance is so critical, it is common for companies to put caches like Redis in front of
a general purpose database to try to offload and speed up some types of queries. However, caching
brings its own set of headaches and does not help for write-heavy workloads.

At their core, the majority of general purpose databases use a B-Tree data structure, which is meant
for a combination of reads and writes. But OLTP is write-heavy. Each transaction coming in needs to
be inserted and update state such as an account balance.

Log-Structured Merge (LSM) Trees are optimized for write-heavy workloads. TigerBeetle uses LSM
Trees, along with a number of other building blocks, to create a database that can handle the scale
of OLTP today and for the next decades.

Read more about [TigerBeetle's performance](./performance.md).

### Business Transactions Don't Shard Well

Two of the main ways of scaling OLTP and billing systems today are sharding and MapReduce. However,
both of these present problems.

You could try to separate different groups of accounts into separate database clusters.
Unfortunately, most accounting systems tend to have a few very hot accounts that are involved in a
large number of transactions. For example, those representing the service operator's assets,
liabilities, or income. You can shard accounts, but contention and row locks on the hot accounts
become the bottleneck.

Alternatively, certain types of very high-volume transaction systems like billing for serverless
usage might use MapReduce to total the balances after the fact. However, this makes it extremely
difficult to implement anything like real-time spending caps or live account balance views for
users.

TigerBeetle is designed to squeeze the maximum amount of transaction processing power out of the
leader node, giving you financial consistency at massive scale.

## OLTP Requires Strong Safety and Availability Guarantees

Transaction processing also requires strong safety guarantees to ensure that data cannot be lost,
and high availability to ensure that money is not lost due to database downtime.

TigerBeetle takes a unique approach to safety and availability. It is designed to be run in a
fault-tolerant cluster by default -- without needing a separate add-on or proprietary
implementation. It supports automatic failover in the same package to keep your service running
smoothly.

TigerBeetle is also built in a unique way that sets it apart from other OLTP databases. It is
[written in Zig](./zig.md) instead of C or C++, uses
[static memory allocation](https://tigerbeetle.com/blog/2022-10-12-a-database-without-dynamic-memory), imposes
strict limits on everything it does, and makes heavy use of
[assertions](https://tigerbeetle.com/blog/2023-12-27-it-takes-two-to-contract) and
[simulation testing](https://tigerbeetle.com/blog/2023-07-06-simulation-testing-for-liveness) to
check that code behaves exactly as expected.

Importantly, TigerBeetle is one of the only OLTP databases that was designed to handle storage
faults. Research on how storage faults affect databases only came out in 2018
(["fsyncgate 2018"](https://danluu.com/fsyncgate/)), and TigerBeetle was built from the ground up
with these lessons in mind. TigerBeetle uses hash chains to detect storage faults, it
supports [Protocol-Aware Recovery](https://www.usenix.org/conference/fast18/presentation/alagappan),
and uses a deterministic on-disk format to speed up the cluster's recovery.

Read more about TigerBeetle's approach to [safety](./safety.md).

## The "Who, What, When, Where, Why, and How Much" of OLTP

OLTP and business transactions tend to record the same types of information:

- **Who**: which accounts are transacting?
- **What**: what type of asset or value is moving?
- **When**: when was the transaction initiated or when was it finalized?
- **Where**: where in the world did the transaction take place?
- **Why**: what type of transaction is this or why is it happening?
- **How Much**: what quantity of the asset or items was moved?

While SQL is a great query language for getting data out, OLTP is primarily about getting data in
and this is where SQL falls short. It is **very common for a single business transaction to require
multiple SQL queries (on the order of 10 SQL queries per transaction)** and potentially even
multiple round-trips from the application to the database.

With TigerBeetle, these fields are already built into the schema and ready for you to use:

- **Who**: the [`debit_account_id`](../reference/transfer.md#debit_account_id) and
  [`credit_account_id`](../reference/transfer.md#credit_account_id) indicate which accounts are
  transacting.
- **What**: each asset or type of value in TigerBeetle is tracked on a separate
  [ledger](../coding/data-modeling.md#ledgers). The [`ledger`](../reference/transfer.md#ledger)
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

For centuries, this information has been modeled as debits and credits, and double-entry bookkeeping
has been the language of business since it was invented over 1000 years ago. TigerBeetle takes this
tried-and-true schema and adds the performance and safety we need for OLTP in the 21st century.

By implementing double-entry accounting primitives directly in the database, TigerBeetle gives you
unmatched financial consistency (on top of data consistency). Every transaction comes from one
account and goes to another, ensuring that money is always accounted for. Furthermore, TigerBeetle
keeps an immutable record of business events and entities, ensuring that transactions and balances
are fully auditable.

## TigerBeetle in the Hot Path

You can think about the relationship between TigerBeetle and a general purpose database as the
difference between the _data plane_ and _control plane_.

In routing internet packets, the data plane refers to everything involved in moving IP packets in
real-time. The control plane determines the network topology and makes decisions for how packets
should be routed. The data plane must be as fast and efficient as possible, whereas the control
plane does not need to be touched as frequently.

TigerBeetle is designed to be in the hot path of your system, processing every transaction in
real-time. You can have a [stateless API service](../coding/system-architecture.md) construct and
batch transfers to send to TigerBeetle.

Metadata, such as account details or infrequently changed ledger details, can be stored in a general
purpose database. Note, however, that it is important not to build your system such that it loads
the metadata on every transfer, because that would put the slower-moving general purpose database
back in the hot path.

## Conclusion: Accelerating OLTP for the Next Decades

Building your application on TigerBeetle gives you unmatched transaction processing power.
TigerBeetle provides a fixed schema that maps naturally on to the who, what, when, where, why, and
how much of business transactions. And it is built to handle write-heavy and high-contention
workloads at high performance and with strong safety guarantees. TigerBeetle can help you build your
application correctly today, and it can handle the scale as your business grows.
