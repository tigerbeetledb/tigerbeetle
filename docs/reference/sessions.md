# Client Sessions

A _client session_ is a sequence of [requests](./requests/README.md) and replies sent between a
client and a cluster.

A client session may have **at most one in-flight request** — i.e. at most one unique request on the
network for which a reply has not been received. This simplifies consistency and allows the cluster
to statically guarantee capacity in its incoming message queue. Additional requests from the
application are queued by the client, to be dequeued and sent when their preceding request receives
a reply.

Similar to other databases, TigerBeetle has a [hard limit](#eviction) on the number of concurrent
client sessions. To maximize throughput, users are encouraged to minimize the number of concurrent
clients and [batch](./requests/README.md#batching-events) as many events as possible per request.

## Lifecycle

A client session begins when a client registers itself with the cluster.

- Each client session has a unique identifier ("client id") — an ephemeral random 128-bit id.
- The client sends a special "register" message which is committed by the cluster, at which point
  the client is "registered" — once it receives the reply, it may begin sending requests.
- Client registration is handled automatically by the TigerBeetle client implementation when the
  client is initialized, before it sends its first request.
- When a client restarts (for example, the application service running the TigerBeetle client is
  restarted) it does not resume its old session — it starts a new session, with a new (random)
  client id.

A client session ends when either:

- the client session is [evicted](#eviction), or
- the client terminates

— whichever occurs first.

## Eviction

When a client session is registering and the number of active sessions in the cluster is already at
the cluster's concurrent client session
[limit](https://tigerbeetle.com/blog/a-database-without-dynamic-memory/) (`config.clients_max`, 64
by default), an existing client session must be evicted to make space for the new session.

- After a session is evicted by the cluster, no future requests from that session will ever execute.
- The evicted session is chosen as the session that committed a request the longest time ago.

The cluster sends a message to notify the evicted session that it has ended. Typically the evicted
client is no longer active (already terminated), but if it is active, the eviction message causes it
to self-terminate, bubbling up to the application as an `session evicted` error.

If active clients are terminating with `session evicted` errors, it most likely indicates that the
application is trying to run too many concurrent clients. For performance reasons, it is recommended
to [batch](./requests/README.md#batching-events) as many events as possible into each request sent
by each client.

## Retries

A client session will automatically retry a request until either:

- the client receives a corresponding reply from the cluster, or
- the client is terminated.

Unlike most database or RPC clients:

- the TigerBeetle client will never time out
- the TigerBeetle client has no retry limits
- the TigerBeetle client does not surface network errors

With TigerBeetle's strict consistency model, surfacing these errors at the client/application level
would be misleading. An error would imply that a request did not execute, when that is not known:

- A request delayed by the network could execute after its timeout.
- A reply delayed by the network could execute before its timeout.

## Guarantees

- A client session may have at most one in-flight [request](./requests/README.md).
- A client session [reads its own writes](https://jepsen.io/consistency/models/read-your-writes),
  meaning that read operations that happen after a given write operation will observe the effects of
  the write.
- A client session observes writes in the order that they occur on the cluster.
- A client session observes [`debits_posted`](./account.md#debits_posted) and
  [`credits_posted`](./account.md#credits_posted) as monotonically increasing. That is, a client
  session will never see `credits_posted` or `debits_posted` decrease.
- A client session never observes uncommitted updates.
- A client session never observes a broken invariant (e.g.
  [`flags.credits_must_not_exceed_debits`](./account.md#flagscredits_must_not_exceed_debits) or
  [`flags.linked`](./transfer.md#flagslinked)).
- Multiple client sessions may receive replies out of order relative to one another. For example, if
  two clients submit requests around the same time, the client whose request is committed first
  might receive the reply later.
- A client session can consider a request executed when it receives a reply for the request.
- If a client session is terminated and restarts, it is guaranteed to see the effects of updates for
  which the corresponding reply was received prior to termination.
- If a client session is terminated and restarts, it is _not_ guaranteed to see the effects of
  updates for which the corresponding reply was _not_ received prior to the restart. Those updates
  may occur at any point in the future, or never. Handling application crash recovery safely
  requires [using `id`s to idempotently retry events](../coding/idempotency.md).
