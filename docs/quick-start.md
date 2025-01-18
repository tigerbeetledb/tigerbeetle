# Quick Start

This page will guide you through downloading TigerBeetle, setting up a single- or multi-node
cluster, and creating some accounts and transfers using the REPL.

## 1. Download TigerBeetle

TigerBeetle is a single, small, statically-linked binary.

### Latest Release

```console
# macOS
curl -Lo tigerbeetle.zip https://mac.tigerbeetle.com && unzip tigerbeetle.zip && ./tigerbeetle version
```

```console
# Linux
curl -Lo tigerbeetle.zip https://linux.tigerbeetle.com && unzip tigerbeetle.zip && ./tigerbeetle version
```

```console
# Windows
powershell -command "curl.exe -Lo tigerbeetle.zip https://windows.tigerbeetle.com; Expand-Archive tigerbeetle.zip .; .\tigerbeetle version"
```

#### Build from Source

To build TigerBeetle from source, clone the repo, install Zig and run `zig build`:

```console
git clone https://github.com/tigerbeetle/tigerbeetle && cd tigerbeetle
./zig/download.sh # .bat if you're on Windows.
./zig/zig build
./tigerbeetle version
```

Notes:
- Building from source is not recommended for production deployments.
- If you build TigerBeetle from source, it is only compatible with clients that were also built from source.

#### Direct Download

You can download prebuilt binaries here:

|         | Linux                           | Windows                          | MacOS                             |
| :------ | :------------------------------ | :------------------------------- | :-------------------------------- |
| x86_64  | [tigerbeetle-x86_64-linux.zip]  | [tigerbeetle-x86_64-windows.zip] | [tigerbeetle-universal-macos.zip] |
| aarch64 | [tigerbeetle-aarch64-linux.zip] | N/A                              | [tigerbeetle-universal-macos.zip] |

[tigerbeetle-aarch64-linux.zip]:
  https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-aarch64-linux.zip
[tigerbeetle-universal-macos.zip]:
  https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-universal-macos.zip
[tigerbeetle-x86_64-linux.zip]:
  https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-x86_64-linux.zip
[tigerbeetle-x86_64-windows.zip]:
  https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-x86_64-windows.zip

#### Docker

You can find instructions on using TigerBeetle with Docker [here](./operating/docker.md).

## 2. Create the Data File

Each TigerBeetle node uses a single data file to store its state. Create the data file using the
`format` command:

```console
./tigerbeetle format --cluster=0 --replica=0 --replica-count=1 --development 0_0.tigerbeetle
```

```console
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

## 3. Start Your Cluster

Now we'll run the TigerBeetle server.

```console
./tigerbeetle start --addresses=3000 --development 0_0.tigerbeetle
```

```console
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 127.0.0.1:3000
```

## 4. Connect to the REPL

Now that we have TigerBeetle running, we can connect to it via the REPL to create some accounts and
transfers!

```console
./tigerbeetle repl --cluster=0 --addresses=3000
```

```console
TigerBeetle Client
  Hit enter after a semicolon to run a command.

Examples:
  create_accounts id=1 code=10 ledger=700 flags=linked | history,
                  id=2 code=10 ledger=700;
  create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
  lookup_accounts id=1;
  lookup_accounts id=1, id=2;
```

## 5. Create Accounts

In the REPL, create two accounts on the same ledger using the command:

```console
create_accounts id=1 code=10 ledger=700,
                id=2 code=10 ledger=700;
```

```console
info(message_bus): connected to replica 0
```

## 6. Create a Transfer

Now create a transfer of `10` (of some amount/currency) between the two accounts.

```console
create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
```

Now, the amount of `10` has been credited to account `2` and debited from account `1`.

## 7. Look Up Accounts

Let's query TigerBeetle for these two accounts to verify the transfer we made!

```console
lookup_accounts id=1, id=2;
```

```json
{
  "id": "1",
  "user_data": "0",
  "ledger": "700",
  "code": "10",
  "flags": [],
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

And indeed you can see that account `1` has `debits_posted` as `10` and account `2` has
`credits_posted` as `10`. The `10` amount is fully accounted for!

You can take a look at the [`Accounts` reference](./reference/account.md) to understand all of the
fields on the accounts.

You can also take a look at the [Request Types](./reference/requests/README.md) to see what else you
can do with the REPL.

## Optional: Run a Multi-Node Cluster

Up to this point, we have only shown you how to run a single-node TigerBeetle cluster. In
production, TigerBeetle is intended to be run with [6 nodes](./operating/deploy.md).

Here, we will show you how to run a 3-node cluster (the idea is the same for 6 nodes):

First, create the data files for each node:

```console
./tigerbeetle format --cluster=0 --replica=0 --replica-count=3 --development 0_0.tigerbeetle
./tigerbeetle format --cluster=0 --replica=1 --replica-count=3 --development 0_1.tigerbeetle
./tigerbeetle format --cluster=0 --replica=2 --replica-count=3 --development 0_2.tigerbeetle
```

Note that the data file stores which replica in the cluster the file belongs to.

Start each server in a new terminal window:

```console
./tigerbeetle start --addresses=127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 --development 0_0.tigerbeetle
```

```console
./tigerbeetle start --addresses=127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 --development 0_1.tigerbeetle
```

```console
./tigerbeetle start --addresses=127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 --development 0_2.tigerbeetle
```

TigerBeetle uses the `--replica` that's stored in the data file as an index into the `--addresses`
provided.

You can connect to the REPL as described above try creating accounts and transfers in this cluster.

You can also read more about [deploying TigerBeetle in production](./operating/deploy.md).

## Next: Designing for TigerBeetle

Now that you've created some accounts and transfers, you may want to read about
[how TigerBeetle fits into your system architecture](./coding/system-architecture.md) and dig into
the [data model](./coding/data-modeling.md).
