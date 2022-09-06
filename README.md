# tigerbeetle

*TigerBeetle is a financial accounting database designed for mission critical safety and performance to power the future of financial services.*

**Take part in TigerBeetle's $20k consensus challenge: [Viewstamped Replication Made Famous](https://github.com/tigerbeetledb/viewstamped-replication-made-famous)**

Watch an introduction to TigerBeetle on [Zig SHOWTIME](https://www.youtube.com/watch?v=BH2jvJ74npM) for our design decisions regarding performance, safety, and financial accounting primitives:

[![A million financial transactions per second in Zig](https://img.youtube.com/vi/BH2jvJ74npM/0.jpg)](https://www.youtube.com/watch?v=BH2jvJ74npM)

Read more about the [history](./docs/HISTORY.md) of TigerBeetle, the problem of balance tracking at scale and the solution of a purpose-built financial accounting database.

## TigerBeetle (under active development)

TigerBeetle is not yet production-ready. The production version of **TigerBeetle is now under active development**. Our [DESIGN doc](docs/DESIGN.md) provides an overview of TigerBeetle's data structures and our [project board](https://github.com/tigerbeetledb/tigerbeetle/projects?type=classic) provides a glimpse of where we want to go.

## QuickStart

This section assumes you have Docker.

First provision TigerBeetle's data directory.

```
$ docker run -v $(pwd)/data:/data ghcr.io/tigerbeetledb/tigerbeetle format --cluster=0 --replica=0 /data/0_0.tigerbeetle
```

Then run the server:

```
$ docker run -p 3000:3000 -v $(pwd):/data ghcr.io/tigerbeetledb/tigerbeetle start --addresses=0.0.0.0:3000 /data/0_0.tigerbeetle
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 0.0.0.0:3000

... and so on ...
```

If you want, you can try entering in some accounts and transfers with the Node client and the Node CLI.

```
$ yarn add tigerbeetle-node # or npm install
$ node
Welcome to Node.js v16.14.0.
Type ".help" for more information.
> let { createClient } = require('tigerbeetle-node');
> let client = createClient({ cluster_id: 0, replica_addresses: ['3000'] });
info(message_bus): connected to replica 0
> let errors = await client.createAccounts([
  {
    id: 137n, // u128, primary key generated by the client program
    user_data: 0n, // u128, opaque third-party identifier to link this account to an external entity
    reserved: Buffer.alloc(48, 0), // [48]u8
    ledger: 1,   // u32, ledger value
    code: 718, // u16, a chart of accounts code describing the type of account (e.g. clearing, settlement)
    flags: 0,  // u16
    debits_pending: 0n,  // u64
    debits_posted: 0n,  // u64
    credits_pending: 0n, // u64
    credits_posted: 0n, // u64
    timestamp: 0n, // u64, Reserved: This will be set by the server.
  },
]);
```

For further reading:

* [Running a 3-node cluster locally with docker-compose](./docs/DOCKER_COMPOSE.md)

## Clients

* For Node.js: [tigerbeetle-node](https://github.com/tigerbeetledb/tigerbeetle-node)
* For Golang: [tigerbeetle-go](https://github.com/tigerbeetledb/tigerbeetle-go)

## Community

[Join the TigerBeetle community in Discord.](https://discord.com/invite/uWCGp46uG5)

*If you encounter any benchmark errors, please send us the resulting `benchmark.log`.*

## Performance Demos

Along the way, we also put together a series of performance demos and sketches to get you comfortable building TigerBeetle, show how low-level code can sometimes be easier than high-level code, help you understand some of the key components within TigerBeetle, and enable back-of-the-envelope calculations to motivate design decisions.

You may be interested in:

* [demos/protobeetle](./demos/protobeetle), how batching changes everything.
* [demos/bitcast](./demos/bitcast), how Zig makes zero-overhead network deserialization easy, fast and safe.
* [demos/io_uring](./demos/io_uring), how ring buffers can eliminate kernel syscalls, reduce server hardware requirements by a factor of two, and change the way we think about event loops.
* [demos/hash_table](./demos/hash_table), how linear probing compares with cuckoo probing, and what we look for in a hash table that needs to scale to millions (and billions) of account transfers.

## License

Copyright 2020-2022 Coil Technologies, Inc

Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
