# Installing

## Quick Install

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

## Latest Release

You can download prebuilt binaries for the latest release here:

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

## Past Releases

The releases page lists all past and current releases:

<https://github.com/tigerbeetle/tigerbeetle/releases>

TigerBeetle can be upgraded without downtime, this is documented in [Upgrading](./upgrading.md).

## Building from Source

Building from source is easy, but is not recommended for production deployments, as extra care is
needed to ensure compatibility with clients and upgradability.

To build TigerBeetle from source, clone the repo, install the required version Zig using the
provided script, and run `zig build`:

```console
git clone https://github.com/tigerbeetle/tigerbeetle && cd tigerbeetle
./zig/download.sh # .bat if you're on Windows.
./zig/zig build
./tigerbeetle version
```

## Client Libraries

Client libraries for .Net, Go, Java, Node, and Python are published to the respective package
repositories, see [Clients](../coding/clients/). 
