# Architecture

In theory, TigerBeetle is a replicated state machine that **takes an initial starting state**
(account opening balances), and **applies a set of input events** (transfers) in deterministic
order, after first replicating these input events safely, to **arrive at a final state** (account
closing balances).

In practice, TigerBeetle is based on the [LMAX Exchange
Architecture](https://www.infoq.com/presentations/LMAX/) and makes a few improvements.

We take the same three classic LMAX steps:

1. journal incoming events safely to disk, and replicate to backup nodes, then
2. apply these events to the in-memory state, then
3. ACK to the client

And then we introduce something new:

4. delete the local journalling step entirely, and
5. replace it with parallel replication to 3/5 distributed replicas.

Our architecture then becomes three easy steps:

1. replicate incoming events safely to a quorum of distributed replicas, then
2. apply these events to the in-memory state, then
3. ACK to the client

That's how TigerBeetle **eliminates gray failure in the leader's local disk**, and how TigerBeetle
**eliminates gray failure in the network links to the replication nodes**.

Like LMAX, TigerBeetle uses a thread-per-core design for optimal performance, with strict
single-threading to enforce the single-writer principle and to avoid the costs of multi-threaded
coordinated access to data.

## Data Structures

The best way to understand TigerBeetle is through the data structures it provides. All data
structures are **fixed-size** for performance and simplicity, and there are two main kinds of data
structures, **events** and **states**.

### Events

Events are **immutable data structures** that **instantiate or mutate state data structures**:

- Events cannot be changed, not even by other events.
- Events cannot be derived and must therefore be recorded before execution.
- Events must be executed one after another –in deterministic order– to ensure replayability.
- Events may depend on past events (should they choose).
- Events cannot depend on future events.
- Events may depend on states being at an exact version (should they choose).
- Events may succeed or fail, but the result of an event is never stored in the event; it is stored
  in the state instantiated or mutated by the event.
- Events can only have one immutable version, which can be referenced directly by the event's id.
- Events should be retained for auditing purposes. However, events may be drained into a separate
  cold storage system once their effect has been captured in a state snapshot to compact the journal
  and improve startup times.

**create_transfer**: Create a transfer between accounts (maps to a "prepare"). We group fields in
descending order of size to avoid unnecessary struct padding in C implementations.

```
          create_transfer {
                      id: 16 bytes (128-bit)
        debit_account_id: 16 bytes (128-bit)
       credit_account_id: 16 bytes (128-bit)
                  amount: 16 bytes (128-bit) [required, an unsigned integer in the unit of value of the debit and credit accounts, which must be the same for both accounts]
              pending_id: 16 bytes (128-bit) [optional, required to post or void an existing but pending transfer]
           user_data_128: 16 bytes (128-bit) [optional, e.g. opaque third-party identifier to link this transfer (many-to-one) to an external entity]
            user_data_64:  8 bytes ( 64-bit) [optional, e.g. opaque third-party identifier to link this transfer (many-to-one) to an external entity]
            user_data_32:  4 bytes ( 32-bit) [optional, e.g. opaque third-party identifier to link this transfer (many-to-one) to an external entity]
                 timeout:  4 bytes ( 32-bit) [optional, required only for a pending transfer, a quantity of time, i.e. an offset in seconds from timestamp]
                  ledger:  4 bytes ( 32-bit) [required, to enforce isolation by ensuring that all transfers are between accounts of the same ledger]
                    code:  2 bytes ( 16-bit) [required, an opaque chart of accounts code describing the reason for the transfer, e.g. deposit, settlement]
                   flags:  2 bytes ( 16-bit) [optional, to modify the usage of the reserved field and for future feature expansion]
               timestamp:  8 bytes ( 64-bit) [reserved, assigned by the leader before journalling]
} = 128 bytes (2 CPU cache lines)
```

**create_account**: Create an account.

- We use the terms `credit` and `debit` instead of "payable" or "receivable" since the meaning of a
  credit balance depends on whether the account is an asset or liability or equity, income or
  expense.
- A `posted` amount refers to an amount posted by a transfer.
- A `pending` amount refers to an inflight amount yet-to-be-posted by a two-phase transfer only,
  where the transfer is still pending, and the transfer timeout has not yet fired. In other words,
  the transfer amount has been reserved in the pending account balance (to avoid double-spending)
  but not yet posted to the posted balance. The reserved amount will rollback if the transfer
  ultimately fails. By default, transfers post automatically, but being able to reserve the amount
  as pending and then post the amount only later can sometimes be convenient, for example, when
  switching credit card payments.
- The debit balance of an account is given by adding `debits_posted` plus `debits_pending`,
  likewise, for the credit balance of an account.
- The total balance of an account can be derived by subtracting the total credit balance from the
  total debit balance.
- We keep both sides of the ledger (debit and credit) separate to avoid having to deal with signed
  numbers and to preserve more information about the nature of an account. For example, two accounts
  could have the same balance of 0, but one account could have 1,000,000 units on both sides of the
  ledger, whereas another account could have 1 unit on both sides, both balancing out to 0.
- Once created, an account may be changed only through transfer events to keep an immutable paper
  trail for auditing.

```
           create_account {
                      id: 16 bytes (128-bit)
          debits_pending: 16 bytes (128-bit)
           debits_posted: 16 bytes (128-bit)
         credits_pending: 16 bytes (128-bit)
          credits_posted: 16 bytes (128-bit)
           user_data_128: 16 bytes (128-bit) [optional, opaque third-party identifier to link this account (many-to-one) to an external entity]
            user_data_64:  8 bytes ( 64-bit) [optional, opaque third-party identifier to link this account (many-to-one) to an external entity]
            user_data_32:  4 bytes ( 32-bit) [optional, opaque third-party identifier to link this account (many-to-one) to an external entity]
                reserved:  4 bytes ( 32-bit) [reserved for future accounting policy primitives]
                  ledger:  4 bytes ( 32-bit) [required, to enforce isolation by ensuring that all transfers are between accounts of the same ledger]
                    code:  2 bytes ( 16-bit) [required, an opaque chart of accounts code describing the reason for the transfer, e.g. deposit, settlement]
                   flags:  2 bytes ( 16-bit) [optional, net balance limits: e.g. debits_must_not_exceed_credits or credits_must_not_exceed_debits]
               timestamp:  8 bytes ( 64-bit) [reserved]
} = 128 bytes (2 CPU cache lines)
```

### States

States are **data structures** that capture the results of events:

- States can always be derived by replaying all events.

TigerBeetle provides **exactly one state data structure**:

- **Account**: An account showing the effect of all transfers.

To simplify, to reduce memory copies and to reuse the wire format of event data structures as much
as possible, we reuse our `create_account` event data structure to instantiate the corresponding
state data structure.

## Protocol

The current TCP wire protocol is:

- a fixed-size header that can be used for requests or responses,
- followed by variable-length data.

```
HEADER (128 bytes)
16 bytes CHECKSUM (of remaining HEADER)
16 bytes CHECKSUM BODY
[...see src/vsr.zig for the rest of the Header definition...]
DATA (multiples of 64 bytes)
................................................................................
................................................................................
................................................................................
```

The `DATA` in **the request** for a `create_transfer` command looks like this:

```
{ create_transfer event struct }, { create_transfer event struct } etc.
```

- All event structures are appended one after the other in the `DATA`.

The `DATA` in **the response** to a `create_transfer` command looks like this:

```
{ index: integer, error: integer }, { index: integer, error: integer }, etc.
```

- Only failed `create_transfer` events emit an `error` struct in the response. We do this to
  optimize the common case where most `create_transfer` events succeed.
- The `error` struct includes the `index` into the batch of the `create_transfer` event that failed
  and a TigerBeetle `error` return code indicating why.
- All other `create_transfer` events succeeded.
- This `error` struct response strategy is the same for `create_account` events.

### Protocol Design Decisions

The header is a multiple of 128 bytes because we want to keep the subsequent data aligned to 64-byte
cache line boundaries. We don't want any structure to straddle multiple cache lines unnecessarily
for the sake of simplicity with respect to struct alignment and because this can have a performance
impact through false sharing.

We order the header struct as we do to keep any C protocol implementations padding-free.

We use AEGIS-128L as our checksum, designed to fully exploit the parallelism and built-in AES
support of recent Intel and ARM CPUs.

The reason we use two checksums instead of only a single checksum across header and data is that we
need a reliable way to know the size of the data to expect before we start receiving the data.

Here is an example showing the risk of a single checksum for the recipient:

1. We receive a header with a single checksum protecting both header and data.
2. We extract the SIZE of the data from the header (4 GiB in this case).
3. We cannot tell if this SIZE value is corrupt until we receive the data.
4. We wait for 4 GiB of data to arrive before calculating/comparing checksums.
5. Except the SIZE was corrupted in transit from 16 MiB to 4 GiB (2-bit flips).
6. We never detect the corruption, the connection times out, and we miss our SLA.
