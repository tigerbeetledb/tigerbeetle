# Deploying with systemd

The following includes an example systemd unit for running TigerBeetle with Linux systems that use
systemd. The unit is configured to start a single-node cluster, so you may need to adjust it for
other cluster configurations.

### **tigerbeetle.service**
```
[Unit]
Description=TigerBeetle Replica
Documentation=https://docs.tigerbeetle.com/
After=network-online.target
Wants=network-online.target systemd-networkd-wait-online.service

[Service]
AmbientCapabilities=CAP_IPC_LOCK

Environment=TIGERBEETLE_CACHE_GRID_SIZE=1GiB
Environment=TIGERBEETLE_ADDRESSES=3001
Environment=TIGERBEETLE_REPLICA_COUNT=1
Environment=TIGERBEETLE_REPLICA_INDEX=0
Environment=TIGERBEETLE_CLUSTER_ID=0
Environment=TIGERBEETLE_DATA_FILE=%S/tigerbeetle/0_0.tigerbeetle

DevicePolicy=closed
DynamicUser=true
LockPersonality=true
ProtectClock=true
ProtectControlGroups=true
ProtectHome=true
ProtectHostname=true
ProtectKernelLogs=true
ProtectKernelModules=true
ProtectKernelTunables=true
ProtectProc=noaccess
ProtectSystem=strict
RestrictAddressFamilies=AF_INET AF_INET6
RestrictNamespaces=true
RestrictRealtime=true
RestrictSUIDSGID=true

StateDirectory=tigerbeetle
StateDirectoryMode=700

Type=exec
ExecStart=/usr/local/bin/tigerbeetle start --cache-grid=${TIGERBEETLE_CACHE_GRID_SIZE} --addresses=${TIGERBEETLE_ADDRESSES} ${TIGERBEETLE_DATA_FILE}

[Install]
WantedBy=multi-user.target
```

## Adjusting

You can adjust multiple aspects of this systemd service.
Each specific adjustment is listed below with instructions.

It is not recommended to adjust some values directly in the service file.
When this is the case, the instructions will ask you to instead use systemd's drop-in file support.
Here's how to do that:

1. Install the service unit in systemd (usually by adding it to `/etc/systemd/system`).
2. Create a drop-in file to override the environment variables.
   Run `systemctl edit tigerbeetle.service`.
   This will bring you to an editor with instructions.
3. Add your overrides.
   Example:
   ```
   [Service]
   Environment=TIGERBEETLE_CACHE_GRID_SIZE=4GiB
   Environment=TIGERBEETLE_ADDRESSES=0.0.0.0:3001
   ```

### Pre-start script

You can place the following script in `/usr/local/bin`.
This script is responsible for ensuring that a replica data file exists.
It will create a data file if it doesn't exist.

#### **tigerbeetle-pre-start.sh**
```bash
#!/bin/sh
set -eu

if ! test -e "${TIGERBEETLE_DATA_FILE}"; then
  /usr/local/bin/tigerbeetle format --cluster="${TIGERBEETLE_CLUSTER_ID}" --replica="${TIGERBEETLE_REPLICA_INDEX}" --replica-count="${TIGERBEETLE_REPLICA_COUNT}" "${TIGERBEETLE_DATA_FILE}"
fi
```

The script assumes that `/bin/sh` exists and points to a POSIX-compliant shell, and the `test` utility is either built-in or in the script's search path.
If this is not the case, adjust the script's shebang.

Add the following line to `tigerbeetle.service` before `ExecStart`.

```
ExecStartPre=/usr/local/bin/tigerbeetle-pre-start.sh
```

The service then executes the `tigerbeetle-pre-start.sh` script before starting TigerBeetle.

### TigerBeetle executable

The `tigerbeetle` executable is assumed to be installed in `/usr/local/bin`.
If this is not the case, adjust both `tigerbeetle.service` and `tigerbeetle-pre-start.sh` to use the correct location.

### Environment variables

This service uses environment variables to provide default values for a simple single-node cluster.
To configure a different cluster structure, or a cluster with different values, adjust the values in the environment variables.
It is **not recommended** to change these default values directly in the service file, because it may be important to revert to the default behavior later.
Instead, use systemd's drop-in file support.

### State directory and replica data file path

This service configures a state directory, which means that systemd will make sure the directory is created before the service starts, and the directory will have the correct permissions.
This is especially important because the service uses systemd's dynamic user capabilities.
systemd forces the state directory to be in `/var/lib`, which means that this service will have its replica data file at `/var/lib/tigerbeetle/`.
It is **not recommended** to adjust the state directory directly in the service file, because it may be important to revert to the default behavior later.
Instead, use systemd's drop-in file support.
If you do so, remember to also adjust the `TIGERBEETLE_DATA_FILE` environment variable, because it also hardcodes the `tigerbeetle` state directory value.

Due to systemd's dynamic user capabilities, the replica data file path will not be owned by any existing user of the system.

### Hardening configurations

Some hardening configurations are enabled for added security when running the service.
It is **not recommended** to change these, since they have additional implications on all other configurations and values defined in this service file.
If you wish to change those, you are expected to understand those implications and make any other adjustments accordingly.

### Development mode

The service was created assuming it'll be used in a production scenario.

In case you want to use this service for development as well, you may need to adjust the `ExecStart` line to include the `--development` flag if your development environment doesn't support Direct IO, or if you require smaller cache sizes and/or batch sizes due to memory constraints.

### Memory Locking

TigerBeetle requires `RLIMIT_MEMLOCK` to be set high enough to:

1. initialize io_uring, which requires memory shared with the kernel to be locked, as well as
2. lock all allocated memory, and so prevent the kernel from swapping any pages to disk, which would not only affect performance but also bypass TigerBeetle's storage fault-tolerance.

If the required memory cannot be locked, then the environment should be modified either by (in order of preference):

1. giving the local `tigerbeetle` binary the `CAP_IPC_LOCK` capability (`sudo setcap "cap_ipc_lock=+ep" ./tigerbeetle`), or
2. raising the global `memlock` value under `/etc/security/limits.conf`, or else
3. disabling swap (io_uring may still require an RLIMIT increase).

Memory locking is disabled for development environments when using the `--development` flag.

For Linux running under Docker, refer to [Allowing MEMLOCK](./docker.md#allowing-memlock).
