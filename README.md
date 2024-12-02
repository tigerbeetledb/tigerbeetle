# tigerbeetle

*TigerBeetle is the financial transactions database designed for mission critical safety and performance to power the next 30 years of [OLTP](https://docs.tigerbeetle.com/concepts/oltp).*

## Documentation

* <https://docs.tigerbeetle.com>
* [The Primeagen](https://www.youtube.com/watch?v=sC1B3d9C_sI) video introduction to our
  design decisions regarding performance, safety, and debit/credit primitives.
* [Redesigning OLTP for a New Order of Magnitude (QCon SF)](https://www.infoq.com/presentations/redesign-oltp/)
  talk with a deeper dive into TigerBeetle’s local storage engine and global consensus protocol.
* [TIGER_STYLE.md](./docs/internals/TIGER_STYLE.md), the engineering methodology behind TigerBeetle.
* [Slack](https://join.slack.com/t/tigerbeetle/shared_invite/zt-21xk62q7l-p~~G7~H01zQb88rQn7tWfQ), say hello!

## Start

Run a single-replica cluster on Linux (or [other platforms](https://docs.tigerbeetle.com/start)):

```console
$ curl -Lo tigerbeetle.zip https://linux.tigerbeetle.com && unzip tigerbeetle.zip && ./tigerbeetle version
$ ./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 --development 0_0.tigerbeetle
$ ./tigerbeetle start --addresses=3000 --development 0_0.tigerbeetle
```

Connect to the cluster and make a transfer:

```console
$ ./tigerbeetle repl --cluster=0 --addresses=3000
> create_accounts id=1 code=10 ledger=700,
                  id=2 code=10 ledger=700;
> create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
> lookup_accounts id=1, id=2;
{
  "id": "1",
  "user_data": "0",
  "ledger": "700",
  "code": "10",
  "flags": "",
  "debits_pending": "0",
  "debits_posted": "10",
  "credits_pending": "0",
  "credits_posted": "0"
}
{
  "id": "2",
  "user_data": "0",
  "ledger": "700",
  "code": "10",
  "flags": "",
  "debits_pending": "0",
  "debits_posted": "0",
  "credits_pending": "0",
  "credits_posted": "10"
}
```

Want to learn more? See <https://docs.tigerbeetle.com>.
