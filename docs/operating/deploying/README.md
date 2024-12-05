# Deploying

TigerBeetle is a single, statically linked binary without external dependencies, so the overall
deployment procedure is simple:

- Get the `tigerbeetle` binary onto each of the cluster's machines (see
  [Installing](../installing.md)).
- Format the data files, specifying cluster id, replica count, and replica index.
- Start replicas, specifying path to the datafile and addresses of all replicas in the cluster.

Here's how to deploy a three replica cluster running on a single machine:

```console
curl -Lo tigerbeetle.zip https://linux.tigerbeetle.com && unzip tigerbeetle.zip && ./tigerbeetle version
./tigerbeetle format --cluster=0 --replica-count=3 --replica=0 ./0_0.tigerbeetle
./tigerbeetle format --cluster=0 --replica-count=3 --replica=1 ./0_1.tigerbeetle
./tigerbeetle format --cluster=0 --replica-count=3 --replica=2 ./0_2.tigerbeetle

./tigerbeetle start --addresses=127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 ./0_0.tigerbeetle &
./tigerbeetle start --addresses=127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 ./0_1.tigerbeetle &
./tigerbeetle start --addresses=127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002 ./0_2.tigerbeetle &
```

Here what the arguments mean:

* `--cluster` specifies a globally unique 128 bit cluster ID. It is recommended to use a random
  number for a cluster id.
* `--replica-count` specifies the size of the cluster. In the current version of TigerBeetle,
  cluster size can not be changed after creation, but this limitation will be lifted in the future.
* `--replica` is a zero-based index of the current replica. While `--cluster` and `--replica-count`
  arguments must match across all replicas of the cluster, `--replica` arguments must be unique.
* `./0_0.tigerbeetle` is a path to the data file. It doesn't matter how you name it, but the
  suggested naming schema is `${CLUSTER_ID}_${REPLICA_INDEX}.tigerbeetle`.
* `--addresses` specify IP addresses of all the replicas in the cluster. **The order of addresses
  must correspond to the order of replicas**. In particular,  the `--addresses` argument must be the
  same for all replicas and all clients.

Production deployment differs in three aspects (see [Cluster Recommendations](../cluster.md)):

- Each replica runs on a dedicated machine.
- Six replicas are used rather than three.
- There's a supervisor process to restart a replica process after a crash.

## Deployment Recipes

We have recipes for some commonly used deployment tool:

- [systemd](./systemd.md)
- [Docker](./docker.md)
- [Managed](./managed-service.md)
