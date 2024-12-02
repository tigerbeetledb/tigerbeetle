# Coding

This section is aimed at programmers building applications on top of TigerBeetle. It is organized as
a series of loosely connected guides which can be read in any order.

- [System Architecture](./system-architecture.md) paints the big picture.
- [Data Modeling](./data-modeling.md) shows how to map business-level entities to the primitives
  provided by TigerBeetle
- [Reliable Transfer Submission](./idempotency.md) explains the end-to-end principle and how it
  helps to avoid double spending.
- [Time](./time.md) lists the guarantees provided by TigerBeetle cluster clock.
- [Two-Phase Transfers](./two-phase-transfers.md) introduces pending transfer, one of the most
  powerful primitives built into TigerBeetle.
- [Linked Transfers](./linked-transfers.md) explains how a sequence of simple transfers can be
  combined into a larger indivisible transaction that either fails or succeeds as a single unit.
- [Recipes](./recipes/) is a library of ready-made solutions for common business requirements such
  as a currency exchange.
- [Clients](./clients/) shows how to use TigerBeetle from the comfort of .Net, Go, Java, Node.js, or
  Python.
