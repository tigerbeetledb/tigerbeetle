# Docker

TigerBeetle can be run using Docker. However, it is not recommended.

TigerBeetle is distributed as a single, small, statically-linked binary. It
should be easy to run directly on the target machine. Using Docker as an
abstraction adds complexity while providing relatively little in this case.

## Image

The Docker image is available from the Github Container Registry:

<https://github.com/tigerbeetle/tigerbeetle/pkgs/container/tigerbeetle>

## Format the Data File

When using Docker, the data file must be mounted as a volume:

```shell
docker run --security-opt seccomp=unconfined \
     -v $(pwd)/data:/data ghcr.io/tigerbeetle/tigerbeetle \
    format --cluster=0 --replica=0 --replica-count=1 /data/0_0.tigerbeetle
```

```console
info(io): creating "0_0.tigerbeetle"...
info(io): allocating 660.140625MiB...
```

## Run the Server

```console
docker run -it --security-opt seccomp=unconfined \
    -p 3000:3000 -v $(pwd)/data:/data ghcr.io/tigerbeetle/tigerbeetle \
    start --addresses=0.0.0.0:3000 /data/0_0.tigerbeetle
```

```console
info(io): opening "0_0.tigerbeetle"...
info(main): 0: cluster=0: listening on 0.0.0.0:3000
```

## Run a Multi-Node Cluster Using Docker Compose

Format the data file for each replica:

```console
docker run --security-opt seccomp=unconfined -v $(pwd)/data:/data ghcr.io/tigerbeetle/tigerbeetle format --cluster=0 --replica=0 --replica-count=3 /data/0_0.tigerbeetle
docker run --security-opt seccomp=unconfined -v $(pwd)/data:/data ghcr.io/tigerbeetle/tigerbeetle format --cluster=0 --replica=1 --replica-count=3 /data/0_1.tigerbeetle
docker run --security-opt seccomp=unconfined -v $(pwd)/data:/data ghcr.io/tigerbeetle/tigerbeetle format --cluster=0 --replica=2 --replica-count=3 /data/0_2.tigerbeetle
```

Note that the data file stores which replica in the cluster the file belongs to.

Then, create a docker-compose.yml file:

```docker-compose
version: "3.7"

##
# Note: this example might only work with linux + using `network_mode:host` because of 2 reasons:
#
# 1. When specifying an internal docker network, other containers are only available using dns based routing:
#    e.g. from tigerbeetle_0, the other replicas are available at `tigerbeetle_1:3002` and
#    `tigerbeetle_2:3003` respectively.
#
# 2. Tigerbeetle performs some validation of the ip address provided in the `--addresses` parameter
#    and won't let us specify a custom domain name.
#
# The workaround for now is to use `network_mode:host` in the containers instead of specifying our
# own internal docker network
##

services:
  tigerbeetle_0:
    image: ghcr.io/tigerbeetle/tigerbeetle
    command: "start --addresses=0.0.0.0:3001,0.0.0.0:3002,0.0.0.0:3003 /data/0_0.tigerbeetle"
    network_mode: host
    volumes:
      - ./data:/data
    security_opt:
      - "seccomp=unconfined"

  tigerbeetle_1:
    image: ghcr.io/tigerbeetle/tigerbeetle
    command: "start --addresses=0.0.0.0:3001,0.0.0.0:3002,0.0.0.0:3003 /data/0_1.tigerbeetle"
    network_mode: host
    volumes:
      - ./data:/data
    security_opt:
      - "seccomp=unconfined"

  tigerbeetle_2:
    image: ghcr.io/tigerbeetle/tigerbeetle
    command: "start --addresses=0.0.0.0:3001,0.0.0.0:3002,0.0.0.0:3003 /data/0_2.tigerbeetle"
    network_mode: host
    volumes:
      - ./data:/data
    security_opt:
      - "seccomp=unconfined"
```

And run it:

```console
docker-compose up
```

```console
docker-compose up
Starting tigerbeetle_0   ... done
Starting tigerbeetle_2   ... done
Recreating tigerbeetle_1 ... done
Attaching to tigerbeetle_0, tigerbeetle_2, tigerbeetle_1
tigerbeetle_1    | info(io): opening "0_1.tigerbeetle"...
tigerbeetle_2    | info(io): opening "0_2.tigerbeetle"...
tigerbeetle_0    | info(io): opening "0_0.tigerbeetle"...
tigerbeetle_0    | info(main): 0: cluster=0: listening on 0.0.0.0:3001
tigerbeetle_2    | info(main): 2: cluster=0: listening on 0.0.0.0:3003
tigerbeetle_1    | info(main): 1: cluster=0: listening on 0.0.0.0:3002
tigerbeetle_0    | info(message_bus): connected to replica 1
tigerbeetle_0    | info(message_bus): connected to replica 2
tigerbeetle_1    | info(message_bus): connected to replica 2
tigerbeetle_1    | info(message_bus): connection from replica 0
tigerbeetle_2    | info(message_bus): connection from replica 0
tigerbeetle_2    | info(message_bus): connection from replica 1
tigerbeetle_0    | info(clock): 0: system time is 83ns ahead
tigerbeetle_2    | info(clock): 2: system time is 83ns ahead
tigerbeetle_1    | info(clock): 1: system time is 78ns ahead

... and so on ...
```

## Troubleshooting

### `error: PermissionDenied`

If you see this error at startup, it is likely because you are running Docker
25.0.0 or newer, which blocks io_uring by default. Set
`--security-opt seccomp=unconfined` to fix it.

### `exited with code 137`

If you see this error without any logs from TigerBeetle, it is likely that the
Linux OOMKiller is killing the process. If you are running Docker inside a
virtual machine (such as is required on Docker or Podman for macOS), try
increasing the virtual machine memory limit.

Alternatively, in a development environment, you can lower the size of the cache
so TigerBeetle uses less memory. For example, set `--cache-grid=256MiB` when
running `tigerbeetle start`.

### Debugging panics

If TigerBeetle panics and you can reproduce the panic, you can get a better
stack trace by switching to a debug image (by using the `:debug` Docker image
tag).

```console
docker run -p 3000:3000 -v $(pwd)/data:/data ghcr.io/tigerbeetle/tigerbeetle:debug \
    start --addresses=0.0.0.0:3000 /data/0_0.tigerbeetle
```

### On MacOS

#### `error: SystemResources`

If you get `error: SystemResources` when running TigerBeetle in Docker on macOS,
the container may be blocking TigerBeetle from locking memory, which is necessary both for io_uring
and to prevent the kernel's use of swap from bypassing TigerBeetle's storage fault tolerance.

#### Allowing MEMLOCK

To raise the memory lock limits under Docker, execute one of the following:

1. Run `docker run` with `--cap-add IPC_LOCK`
2. Run `docker run` with `--ulimit memlock=-1:-1`
3. Or modify the defaults in `$HOME/.docker/daemon.json` and restart the Docker
   for Mac application:

```json
{
  ... other settings ...
  "default-ulimits": {
    "memlock": {
      "Hard": -1,
      "Name": "memlock",
      "Soft": -1
    }
  },
  ... other settings ...
}
```

If you are running TigerBeetle with Docker Compose, you will need to add the
`IPC_LOCK` capability like this:

```yaml
... rest of docker-compose.yml ...

services:
  tigerbeetle_0:
    image: ghcr.io/tigerbeetle/tigerbeetle
    command: "start --addresses=0.0.0.0:3001,0.0.0.0:3002,0.0.0.0:3003 /data/0_0.tigerbeetle"
    network_mode: host
    cap_add:       # HERE
      - IPC_LOCK   # HERE
    volumes:
      - ./data:/data

... rest of docker-compose.yml ...
```

See https://github.com/tigerbeetle/tigerbeetle/issues/92 for discussion.
