# Deterministic Simulation Testing

Deterministic Simulation Testing (DST) is one of our favorite parts about TigerBeetle, and it is a
key way that we improve the system's reliability.

Simulation testing enables us to run the production TigerBeetle code under a wide variety of
conditions to ensure that the cluster behaves properly. Because our simulator is deterministic based
on a _seed_ number and the Git commit, we can perfectly reproduce any bugs discovered in testing for
easy local debugging. Crucially, VOPR can speed up time arbitrarily. One minute of VOPR time is
equivalent to days of real-world testing.

## Live Simulator in the Browser

You can see the simulator in action at <https://sim.tigerbeetle.com>!

The three modes show TigerBeetle handling different types of network and hardware conditions -- and
you can inject different faults yourself 🔨🧊⚡.

## The VOPR

_The VOPR_, or The Viewstamped Operation Replicator, is our name for our deterministic simulator.
(The name was inspired by the AI supercomputer in the 1983 movie
[WarGames](https://www.imdb.com/title/tt0086567/), which was called the War Operation Plan Response
or WOPR, which constantly simulated scenarios in order to learn.)

The key purpose of the VOPR is to test TigerBeetle's safety and liveness, and it focuses on
consensus and the cluster's recovery mechanisms.

In the simulator, all non-deterministic parts of the system are stubbed out. This includes the
clock, network, and disk operations.

The VOPR uses a random seed to tune parameters for injecting different types of faults into the
simulation. For example, it may drop and reorder packets, partition the network, or corrupt reads
and writes to the "disk".

Using those conditions, the simulator commits several hundred batches of operations and checks that
they are applied as expected.

When a simulation causes any type of failure, the seed and Git commit hash can be used to replay
back the exact simulation and bug. If you are interested in understanding how we debug and fix
failures discovered in simulation, you can watch this
[IronBeetle episode where @matklad live debugs a real simulator failure](https://youtu.be/kZ3xVeO0vBw?si=gaHgOzrN-X86CAmi).

Using the same deterministic simulation infrastructure, we also test for
[specific cases](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/vsr/replica_test.zig) that
are hard or slow to replicate through random simulation.

## Assertions and Checkers

Simulation testing pairs particularly well with TigerBeetle's heavy use of assertions. Throughout
the code base there are thousands of assertions checking that all manner of invariants hold true.

TigerBeetle is somewhat unique in that it keeps these assertions on, even in production. The logic
is that it is far better to stop operating than to continue operating in an incorrect state.

Assertions are a force multiplier when used with simulation testing and fuzzing. If any assertion is
broken under a specific set of circumstances, the simulation will crash and we debug that failure.

On top of the assertions in the code, the simulator also includes a variety of additional checkers
that verify the correctness of the cluster's state. For example, TigerBeetle replicas' data files
are designed to be byte-for-byte identical across caught-up nodes in the cluster. Some of the
storage checkers verify that this is the case across simulations.

## Inspiration

TigerBeetle's approach to DST was heavily inspired by the work of
[FoundationDB](https://apple.github.io/foundationdb/testing.html) and
[Antithesis](https://www.antithesis.com/solutions/problems_we_solve/).

## Learn More

- [Simulation Testing for Liveness (Blog)](https://tigerbeetle.com/blog/2023-07-06-simulation-testing-for-liveness)
- [Deterministic Simulation Testing (Video)](https://youtu.be/el-LqUTv00M?si=ltKilzPSW8c7nKVQ)
