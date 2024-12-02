# Hacking on TigerBeetle

**Prerequisites:** TigerBeetle makes use of certain fairly new technologies, such as
[io_uring](https://kernel.dk/io_uring.pdf) or advanced CPU instructions for cryptography. As such,
it requires a fairly modern kernel (≥ 5.6) and CPU. While at the moment only Linux is supported for
production deployments, TigerBeetle also works on Windows and MacOS.

## Building

```console
git clone https://github.com/tigerbeetle/tigerbeetle.git
cd tigerbeetle
./zig/download.sh # .bat if you're on Windows.
./zig/zig build -Drelease
./tigerbeetle version
```

See the [Quick Start](./quick-start.md) for how to use a freshly-built TigerBeetle.

## Testing

TigerBeetle has several layers of tests. The Zig unit tests can be run as:

```console
./zig/zig build test
```

To run a single test, pass its name after `--`:

```console
./zig/zig build test -- parse_addresses
```

The entry point for various "minor" fuzzers is:

```console
./zig/zig build fuzz -- smoke
```

See [/src/fuzz_tests.zig](/src/fuzz_tests.zig) for the menu of available fuzzers.

### Simulation Tests

To run TigerBeetle's long-running simulation, called *The VOPR*:

```console
zig/zig build vopr
```

To run the VOPR using a specific seed:

```console
zig/zig build vopr -- 123
```

*The VOPR* stands for *The Viewstamped Operation Replicator* and was inspired by the movie WarGames,
by our love of fuzzing over the years, by [Dropbox's Nucleus
testing](https://dropbox.tech/infrastructure/-testing-our-new-sync-engine), and by [FoundationDB's
deterministic simulation testing](https://www.youtube.com/watch?v=OJb8A6h9jQQ).

*The VOPR* is [a deterministic simulator](/src/vopr.zig) that can fuzz many clusters of
TigerBeetle servers and clients interacting through TigerBeetle's Viewstamped Replication consensus
protocol, but all within a single developer machine process, with [a network
simulator](/src/testing/packet_simulator.zig) to simulate all kinds of network faults, and with an
in-memory [storage simulator](/src/testing/storage.zig) to simulate all kinds of storage faults, to
explore and test TigerBeetle against huge state spaces in a short amount of time, by literally
speeding up the passing of time within the simulation itself.

Beyond being a deterministic simulator, *The VOPR* also features [a state
checker](/src/testing/cluster/state_checker.zig) that can hook into all the replicas, and check all
their state transitions the instant they take place, using hash chaining to prove
causality and check that all interim state transitions are valid, based on any of the set of
inflight client requests at the time, without divergent states, and then check for convergence to
the highest state at the end of the simulation, to distinguish between correctness or liveness bugs.

Check out TigerBeetle's [Viewstamped Replication Made
Famous](https://github.com/coilhq/viewstamped-replication-made-famous#how-can-i-run-the-implementation-how-many-batteries-are-included-do-you-mean-i-can-even-run-the-vopr)
bug bounty challenge repository for more details on how to run *The VOPR* and interpret its output.

## Hacking on Clients

Each client is built using language-specific tooling (`npm`, `maven`, `dotnet`, and `go`), and links
to a native library built with Zig. The general pattern is

```console
./zig/zig build client_lang
cd src/clients/lang
lang_package_manager test
```

See `src/clients/$LANG/ci.zig` scripts for exact commands we use on CI to build clients.

### Testing Client Libraries

Each language client is tested by a mixture of unit-tests written using language-specific test
frameworks, and integration tests which run sample projects against a real `tigerbeetle` process.
Everything is orchestrated by [ci.zig](/src/scripts/ci.zig) script:

```console
./zig/zig build scripts -- ci --language=go
```

## Other Useful Commands

Build & immediately run TigerBeetle:

```console
./zig/zig build run -- format ...
```

Quickly check if the code compiles without spending time to generate the binary:

```console
./zig/zig build check
```

Reformat the code according to style:

```
./zig/zig fmt .
```

Run lint checks:

```
./zig/zig build test -- tidy
```

Run macro benchmark:

```
./zig/zig build -Drelease run -- benchmark
```

See comments at the top of
[/src/tigerbeetle/benchmark_load.zig](/src/tigerbeetle/benchmark_load.zig)
for details of benchmarking.

## Docs

Developer-oriented documentation is at
[/docs/about/internals/README.md](/docs/about/internals/README.md)

## Getting In Touch

Say hello in our [Slack](https://slack.tigerbeetle.com/invite)!

## Pull Requests

When submitting pull request, _assign_ a single person to be its reviewer. Unpacking:

* GitHub supports both "assign" and "request review". The difference between them is that "request"
  is "edge triggered" (it is cleared after a round of review), while "assign" is "level triggered"
  (it won't go away until the PR is merged or closed). We use "assign", because the reviewer is
  co-responsible for making sure that the PR doesn't stall, and is eventually completed.

* Only a single person is assigned to any particular pull request, to avoid diffusion of
  responsibility and the bystander effect.

* Pull request author chooses the reviewer. The author has the most context about who is the best
  person to request review from. When picking a reviewer, think about sharing knowledge, balancing
  review load, and maximizing correctness of the code.

After pull request is approved, the author makes the final call to merge by clicking "merge when
ready" button on GitHub. To reduce the number of round-trips, "merge when ready" can be engaged
before the review is completed: a PR will then be merged automatically once an approving review is
submitted.
