# Talks

## February 26, 2024: The FASTEST and SAFEST Database

Joran Dirk Greef gives an entertaining and informative overview of the problems TigerBeetle
solves, technical details about how it works, and shows a demo of the simulator at the end.
ThePrimeagen was blown away.

https://youtu.be/sC1B3d9C_sI?si=Pn6OfKSkyLoHc2Z5

## February 26, 2024: TigerBeetle Release Pipeline (Behind the Scenes!)

In this impromptu talk, matklad discusses build and release process of TigerBeetle.

https://www.youtube.com/watch?v=eFTQzhfO6nc

## October 2, 2023: Redesigning OLTP for a New Order of Magnitude at QCon San Francisco 2024

Joran Dirk Greef discusses how the world is becoming more transactional, why Online
Transaction Processing (OLTP) needs to be redesigned for today's massive scale, and
he goes into technical detail about how TigerBeetle achieves this.

https://www.infoq.com/presentations/redesign-oltp/

## March 27, 2023: A New Era for Database Design with TigerBeetle at QCon London 2023

Joran Dirk Greef discusses pivotal moments in database design and how
they influenced the design decisions for TigerBeetle, a distributed
financial accounting database.

https://www.youtube.com/watch?v=ehYcCTHRyFs

## March 9, 2023: How to Thought Lead and the Double Entry Accounting Database

> I had a great chat with Joran of Tigerbeetle, the hottest animal
> themed database on the market, about Zig, distributed database design,
> and exchanging notes on how to Cross the Chasm with a brand new
> database.

https://www.youtube.com/watch?v=QwXddaB8tj0

## January 18, 2023: Why Accounting Needs Its Own Database with Joran Greef of Tiger Beetle on The datastack Show

This week on The Data Stack Show, Eric and Kostas chatted with Joran
Greef, Founder & CEO of Tiger Beetle. During the episode, Joran
discusses his journey from accounting to coding, why double-entry
accounting is important for databases, safety issues in financial
software, the need for low latency and high throughput, and more.

https://datastackshow.com/podcast/why-accounting-needs-its-own-database-with-joran-greef-of-tiger-beetle/

## November 23, 2022: TigerBeetle: Magical Memory Tour! on CMU Database Group - ¡Databases! – A Database Seminar Series

TigerBeetle is an open source distributed financial accounting
database designed for mission critical safety and performance to track
financial transactions at scale. TigerBeetle is coded to NASA’s Power
of Ten Rules for Safety Critical Code. All memory is statically
allocated at startup for predictable and efficient resource
usage. Function arguments and return values are verified at runtime by
over three thousand assertions. Deterministic Simulation Testing
accelerates the maturation process of TigerBeetle’s VSR consensus
protocol and LSM storage engine, through fault injection of network
faults as well as storage faults such as misdirected or corrupt reads
and writes. TigerBeetle is being designed to process a million journal
entries per second on commodity hardware, using io_uring for high
performance I/O, and Direct I/O and fixed-size cache line aligned data
structures for zero-copy and zero-deserialization. TigerBeetle is
written in Andrew Kelley’s Zig.

https://www.youtube.com/watch?v=FyGukn77gqA

## November 12, 2022: TigerBeetle, a Financial Accounting Database for Interledger

https://www.youtube.com/watch?v=Whp4RfW3K_U&t=6568s

## November 5, 2022: Tornow Talks TigerBeetle!

The legendary Principal Engineer from Temporal, Dominik Tornow, joins
us on set to interview Joran about TigerBeetle.

The history, mission, primitives, invariants, distributed database
design, and deterministic simulation testing, plus a deep dive into
the two-phase commit protocol, the right place in the distributed
systems stack to handle errors (or make an apology!) and an exciting
new conference!

https://www.youtube.com/watch?v=ZW_emZ4683A

## October 28, 2022: Zig's I/O and Concurrency Story at Software You Can Love 2022

Async I/O and concurrency have come to be features expected from any
general purpose language. Zig is in a unique spot where it has
interesting ideas but hasn’t settled on any yet. With the rise of
multi-core CPUs and io_uring, the landscape for concurrent programming
is shifting and it’s important that Zig adapts. Instead of providing
solutions, let’s explore what the options are and discuss the status
quo, the Future (no pun intended), and what it all means for a
language and ecosystem in development.

https://www.youtube.com/watch?v=Ul8OO4vQMTw

## October 17, 2022: Building a Database with Joran Dirk Greef on Software Unscripted

Richard and Joran Greef talk about the TigerBeetle database, an
impressive feat of engineering effort which Joran has been building to
solve real-world problems his team has encountered at work.

https://podcasts.apple.com/us/podcast/building-a-database-with-joran-greef/id1602572955?i=1000582870854

## September 29, 2022: Ledgers at Scale! With Chris Riccomini

In which we unpack the past, present (and future!) of ledgers.

With Chris Riccomini — a Distinguished Engineer, author, investor and
formerly of WePay, LinkedIn and PayPal—who's built some of the
scaliest of them!

https://www.youtube.com/watch?v=xQ7Gmkb9zts

## August 30, 2022: Twitter Spaces Recording: Coil x TigerBeetle: Databases of the Future

Audio Recording from 8-25-22: Joran Dirk Greef from TigerBeetle
interviewed by Adrian Hope-Bailie (Fynbos) from Coil's Twitter Spaces

We are very excited to have Joran from the TigerBeetle team together
with Adrian from Fynbos discussing…lessons from building TigerBeetle,
a high-performance database which handles 1 million transactions per
second.

We’re going to talk about why TigerBeetle is a distributed database
that is specifically built for financial accounting, and what it
brings to an already vibrant landscape of open source databases.

## August 15: 2022: VOPR'izing TigerBeetle

Sarah takes us deep into the Matrix, where “VOPR” bots run TigerBeetle
clusters in thousands of simulated worlds—to find, classify and report
correctness and liveness bugs to GitHub as issues, all without human
intervention.

https://www.youtube.com/watch?v=0esGaX5XekM

## August 6, 2022: A New Era for Databases With Alex Gallego (Redpanda)

A deep dive into distributed database design with Alex Gallego,
founder and CEO of Redpanda:

- How have hardware trends changed database design?
- What exactly is a database?
- How do we build and test distributed databases?
- And how do we package them into a product that people love?

https://www.youtube.com/watch?v=jC_803mW448

## July 13, 2022: TigerBeetle - A Million Financial Transactions per Second in Zig on Zig SHOWTIME

https://www.youtube.com/watch?v=BH2jvJ74npM

## June 7, 2022: Let's Remix Distributed Database Design at Recurse Center

When we talk about consensus protocols and storage engines, it's often
in isolation. So the distributed systems community will talk about
protocols like Viewstamped Replication, Paxos, or RAFT, and the
storage community will talk about storage faults and engines like
LevelDB and RocksDB. Today, let's talk about both—and remix
distributed database design!

https://www.youtube.com/watch?v=rNmZZLant9o

## May 1, 2022: TigerBeetle's LSM-Forest at HYTRADBOI '22

A fast-paced introduction to TigerBeetle's mission, architecture,
global consensus, local storage engine, and our thinking on
LSM-trees—how to make them fast, fault-tolerant, and above all, fun to
test!

https://www.youtube.com/watch?v=yBBpUMR8dHw

## January 11, 2022: Introducing TigerBeetle's LSM-Forest

Why and how to implement a distributed deterministic LSM-Tree storage
engine, and then grow this into an LSM-Forest!

Simulation testing, storage faults, persistent read snapshots that
survive crashes, new IO APIs like io_uring, reduced read/write
amplification, incremental/pipelined compaction, static allocation,
optimal memory usage and more...

https://www.youtube.com/watch?v=LikJDDhwmXA

## October 3, 2021: Paper #74. Viewstamped Replication Revisited on DistSys Reading Group

In the 74th meeting we discussed yet another foundational paper --
Viewstamped Replication. In particular, we focused on the 2012
revisited version of the paper: "Viewstamped Replication Revisited."

https://www.youtube.com/watch?v=Wii1LX_ltIs

## September 18, 2021: Revisiting Viewstamped Replication with Brian Oki and James Cowling on Zig SHOWTIME

https://www.youtube.com/watch?v=ps106zjmjhw

## September 17, 2021: Viewstamped Replication Made Famous on Zig SHOWTIME

https://www.youtube.com/watch?v=qeWyc8G-lq4

## May 22, 2021: PaPoC '21 Lightning Talk - Viewstamped Replication Made Famous

A 3-minute, 1-slide lightning talk given by Joran Dirk Greef on 26
April 2020 at the 8th Workshop on Principles and Practice of
Consistency for Distributed Data (part of EuroSys '21), on
TigerBeetle's production implementation of Viewstamped Replication
Revisited and Protocol Aware Recovery for Consensus-Based Storage.

https://www.youtube.com/watch?v=tlz0FbzGGGo
