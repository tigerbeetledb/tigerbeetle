# Upgrading

TigerBeetle guarantees storage stability and provides forward upgradeability. In other words, data
files created by a particular past version of TigerBeetle can be migrated to any future version of
TigerBeetle.

Migration is automatic and the upgrade process is usually as simple as:
* Upgrade the replicas, by replacing the `./tigerbeetle` binary with a newer version on each
  replica (they will restart automatically when needed).
* Upgrade the clients, by updating the corresponding client libraries, recompiling and redeploying
  as usual.

There's no need to stop the cluster for upgrades, and the client upgrades can be rolled out
gradually as any change to the client code might.

NOTE: if you are upgrading from 0.15.3 (the first stable version), the upgrade procedure is more
involved, see the [release notes for 0.15.4](https://github.com/tigerbeetle/tigerbeetle/releases/tag/0.15.4).

## Planning for upgrades
When upgrading TigerBeetle, each release specifies two important versions:
* the oldest release that can be upgraded from and,
* the oldest supported client version.

It's critical to make sure that the release you intend to upgrade from is supported by the release
you're upgrading to. This is a hard requirement, but also a hard guarantee: if you wish to upgrade
to `0.15.20` which says it supports down to `0.15.5`, `0.15.5` _will_ work and `0.15.4` _will not_.
You will have to perform multiple upgrades in this case.

The upgrade process involves first upgrading the replicas, followed by upgrading the clients. The
client version *cannot* be newer than the replica version, and will fail with an error message if
so. Provided the supported version ranges overlap, coordinating the upgrade between clients and
replicas is not required.

Upgrading causes a short period of unavailability as the replicas restart. This is on the order of
5 seconds, and will show up as a latency spike on requests. The TigerBeetle clients will internally
retry any requests during the period.

Even though this period is short, scheduling a maintenance windows for upgrades is still
recommended, for an extra layer of safety.

Any special instructions, like that when upgrading from 0.15.3 to 0.15.4, will be explicitly
mentioned in the [changelog](https://github.com/tigerbeetle/tigerbeetle/blob/main/CHANGELOG.md)
and [release notes](https://github.com/tigerbeetle/tigerbeetle/releases).

Additionally, subscribe to [this tracking issue](https://github.com/tigerbeetle/tigerbeetle/issues/2231)
to be notified when there are breaking API/behavior changes that are visible to the client.

## Upgrading binary-based installations
If TigerBeetle is installed under `/usr/bin/tigerbeetle`, and you wish to upgrade to `0.15.4`:
```bash
# SSH to each replica, in no particular order:
cd /tmp
wget https://github.com/tigerbeetle/tigerbeetle/releases/download/0.15.4/tigerbeetle-x86_64-linux.zip
unzip tigerbeetle-x86_64-linux.zip

# Put the binary on the same file system as the target, so mv is atomic.
mv tigerbeetle /usr/bin/tigerbeetle-new

mv /usr/bin/tigerbeetle /usr/bin/tigerbeetle-old
mv /usr/bin/tigerbeetle-new /usr/bin/tigerbeetle

# Restart TigerBeetle. Only required when upgrading from 0.15.3.
# Otherwise, it will detect new versions are available and coordinate the upgrade itself.
systemctl restart tigerbeetle # or, however you are managing TigerBeetle.
```

## Upgrading Docker-based installations
If you're running TigerBeetle inside Kubernetes or Docker, update the tag that is pointed to to the
release you wish to upgrade to. Before beginning, it's strongly recommend to have a rolling deploy
strategy set up.

For example:
```
image: ghcr.io/tigerbeetle/tigerbeetle:0.15.3
```

becomes
```
image: ghcr.io/tigerbeetle/tigerbeetle:0.15.4
```

Due to the way upgrades work internally, this will restart with the new binary available, but still
running the older version. TigerBeetle will then coordinate the actual upgrade when all replicas
are ready and have the latest version available.

## Upgrading clients
Update your language's specific package management, to reference the same version of the
TigerBeetle client:

### .NET
```
dotnet add package tigerbeetle --version 0.15.4
```

### Go
```
go mod edit -require github.com/tigerbeetle/tigerbeetle-go@v0.15.4
```

### Java
Edit your `pom.xml`:

```
    <dependency>
        <groupId>com.tigerbeetle</groupId>
        <artifactId>tigerbeetle-java</artifactId>
        <version>0.15.4</version>
    </dependency>
```

### Node.js
```
npm install --save-exact tigerbeetle-node@0.15.4
```

### Python
```
pip install tigerbeetle==0.15.4
```

## Troubleshooting
### Upgrading to a newer version with incompatible clients
If a release of TigerBeetle no longer supports the client version you're using, it's still possible
to upgrade, with two options:
* Upgrade the replicas to the latest version. In this case, the clients will stop working for the
  duration of the upgrade and unavailability will be extended.
* Upgrade the replicas to the latest release that supports the client version in use, then upgrade
  the clients to that version. Repeat this until you're on the latest release.
