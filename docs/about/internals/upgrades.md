# Upgrades

Upgrades in TigerBeetle are handled by bundling multiple underlying TigerBeetle binaries of
different versions, into a single binary, known as "Multiversion Binaries".

The idea behind multiversion binaries is to give operators a great experience when upgrading
TigerBeetle clusters:

Upgrades should be simple, involve minimal downtime and be robust, while not requiring external
coordination.

Multiple versions in a single binary are required for two reasons:
* It allows a replica to crash after the binary has been upgraded, and still come back online.
  * It also allows for deployments, like Docker, where the binary is immutable and the process
    has to be terminated to learn about new versions from itself.
* It allows for migrations over a range to happen easily without having to manually jump from
  version to version.

The upgrade instructions look something like:

```
# SSH to each replica, in no particular order:
cd /tmp
wget https://github.com/tigerbeetle/tigerbeetle/releases/download/0.15.4/tigerbeetle-x86_64-linux.zip
unzip tigerbeetle-x86_64-linux.zip

# Put the binary on the same file system as the target, so mv is atomic.
mv tigerbeetle /usr/bin/tigerbeetle-new

mv /usr/bin/tigerbeetle /usr/bin/tigerbeetle-old
mv /usr/bin/tigerbeetle-new /usr/bin/tigerbeetle
```

When the primary determines that enough[^1] replicas have the new binary, it'll [coordinate the
 upgrade](https://github.com/tigerbeetle/tigerbeetle/pull/1670).

[^1]: Currently the total number of replicas, less one.

There are three main parts to multiversion binaries: building, monitoring and executing, with
platform specific parts in each.

## Building
Physically, multiversion binaries are regular TigerBeetle ELF / PE / MachO[^2] files that have two
extra sections[^3] embedded into them - marked as `noload` so that they're not memory mapped:
* `.tb_mvh` or TigerBeetleMultiVersionHeader - a header struct containing information on past
  versions embedded as well as offsets, sizes, checksums and the like.
* `.tb_mvb` or TigerBeetleMultiVersionBody - a concatenated pack of binaries. The offsets in
  `.tb_mvh` refer into here.

[^2]: MachO binaries are constructed as fat binaries, using unused, esoteric CPU identifiers to
signal the header and body, for both x86_64 and arm64.

[^3]: The short names are for compatibility with Windows: PE supports up to 8 characters for
section names without getting more complicated.

These are added by an explicit objcopy step in the release process, _after_ the regular build is
done. After the epoch, the build process only needs to pull the last TigerBeetle release from
GitHub, to access its embedded pack to build its own.

### Bootstrapping
0.15.3 is considered the epoch release, but it doesn't know about any future versions of
TigerBeetle or how to read the metadata yet. This means that if the build process pulled in that
exact release, when running on a 0.15.3 data file, 0.15.3 would be executed and nothing further
would happen. There is a [special backport
release](https://github.com/tigerbeetle/tigerbeetle/pull/1935), that embeds the fact that 0.15.4 is
available to solve this problem. The release code for 0.15.4 builds this version for 0.15.3,
instead of downloading it from GitHub.

Additionally, since 0.15.3 can't read its own binary (see Monitoring below), restarting the replica
manually after copying it in is needed.

Once 0.15.4 is running, no more special cases are needed.

## Monitoring
On a 1 second timer, TigerBeetle `stat`s its binary file, looking for changes. Should anything
differ (besides `atime`) it'll re-read the binary into memory, verify checksums and metadata, and
start advertising new versions without requiring a restart.

This optimization allows skipping a potentially expensive WAL replay when upgrading: the previous
version is what will checkpoint to the new version, at which point the exec happens.

## Executing
The final step is executing into the new version of TigerBeetle. On Linux, this is handled by
`execveat` which allows executing from a `memfd`. If executing the latest release, `exec_current`
re-execs the `memfd` as-is. If executing an older release, `exec_release` copies it out of the
pack, verifies its checksum, and then executes it.

One key point is that the newest version is always what starts up and determines what version to
run.
