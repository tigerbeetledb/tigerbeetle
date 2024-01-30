const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;
const net = std.net;

const vsr = @import("vsr");
const flags = vsr.flags;
const constants = vsr.constants;
const tigerbeetle = vsr.tigerbeetle;
const data_file_size_min = vsr.superblock.data_file_size_min;
const Grid = vsr.GridType(vsr.storage.Storage);
const StateMachine = vsr.state_machine.StateMachineType(
    vsr.storage.Storage,
    constants.state_machine_config,
);

const CliArgs = union(enum) {
    format: struct {
        cluster: u128,
        replica: u8,
        replica_count: u8,

        positional: struct {
            path: [:0]const u8,
        },
    },

    start: struct {
        addresses: []const u8,
        limit_storage: flags.ByteSize = .{ .bytes = constants.storage_size_limit_max },
        cache_accounts: flags.ByteSize = .{ .bytes = constants.cache_accounts_size_default },
        cache_transfers: flags.ByteSize = .{ .bytes = constants.cache_transfers_size_default },
        cache_transfers_posted: flags.ByteSize =
            .{ .bytes = constants.cache_transfers_posted_size_default },
        cache_grid: flags.ByteSize = .{ .bytes = constants.grid_cache_size_default },

        positional: struct {
            path: [:0]const u8,
        },
    },

    version: struct {
        verbose: bool = false,
    },

    client: struct {
        addresses: []const u8,
        cluster: u128,
        verbose: bool = false,
        command: []const u8 = "",
    },

    // TODO Document --cache-accounts, --cache-transfers, --cache-transfers-posted, --limit-storage
    pub const help = fmt.comptimePrint(
        \\Usage:
        \\
        \\  tigerbeetle [-h | --help]
        \\
        \\  tigerbeetle format --cluster=<integer> --replica=<index> --replica-count=<integer> <path>
        \\
        \\  tigerbeetle start --addresses=<addresses> [--cache-grid=<size><KB|MB|GB>] <path>
        \\
        \\  tigerbeetle version [--version]
        \\
        \\  tigerbeetle client --cluster=<integer> --addresses=<addresses>
        \\
        \\Commands:
        \\
        \\  format   Create a TigerBeetle replica data file at <path>.
        \\           The --cluster and --replica arguments are required.
        \\           Each TigerBeetle replica must have its own data file.
        \\
        \\  start    Run a TigerBeetle replica from the data file at <path>.
        \\
        \\  version  Print the TigerBeetle build version and the compile-time config values.
        \\
        \\  client   Enter the TigerBeetle client REPL.
        \\
        \\Options:
        \\
        \\  -h, --help
        \\        Print this help message and exit.
        \\
        \\  --cluster=<integer>
        \\        Set the cluster ID to the provided 128-bit unsigned decimal integer.
        \\
        \\  --replica=<index>
        \\        Set the zero-based index that will be used for the replica process.
        \\        An index greater than or equal to "replica-count" makes the replica a standby.
        \\        The value of this argument will be interpreted as an index into the --addresses array.
        \\
        \\  --replica-count=<integer>
        \\        Set the number of replicas participating in replication.
        \\
        \\  --addresses=<addresses>
        \\        Set the addresses of all replicas in the cluster.
        \\        Accepts a comma-separated list of IPv4/IPv6 addresses with port numbers.
        \\        Either the address or port number (but not both) may be omitted,
        \\        in which case a default of {[default_address]s} or {[default_port]d}
        \\        will be used.
        \\        "addresses[i]" corresponds to replica "i".
        \\
        \\  --cache-grid=<size><KB|MB|GB>
        \\        Set the grid cache size. The grid cache acts like a page cache for TigerBeetle,
        \\        and should be set as large as possible.
        \\        On a machine running only TigerBeetle, this is somewhere around
        \\        (Total RAM) - 3GB (TigerBeetle) - 1GB (System), eg 12GB for a 16GB machine.
        \\        Defaults to 1GB.
        \\
        \\  --verbose
        \\        Print compile-time configuration along with the build version.
        \\
        \\Examples:
        \\
        \\  tigerbeetle format --cluster=0 --replica=0 --replica-count=3 0_0.tigerbeetle
        \\  tigerbeetle format --cluster=0 --replica=1 --replica-count=3 0_1.tigerbeetle
        \\  tigerbeetle format --cluster=0 --replica=2 --replica-count=3 0_2.tigerbeetle
        \\
        \\  tigerbeetle start --addresses=127.0.0.1:3003,127.0.0.1:3001,127.0.0.1:3002 0_0.tigerbeetle
        \\  tigerbeetle start --addresses=3003,3001,3002 0_1.tigerbeetle
        \\  tigerbeetle start --addresses=3003,3001,3002 0_2.tigerbeetle
        \\
        \\  tigerbeetle start --addresses=192.168.0.1,192.168.0.2,192.168.0.3 0_0.tigerbeetle
        \\
        \\  tigerbeetle start --addresses='[::1]:3003,[::1]:3001,[::1]:3002'  0_0.tigerbeetle
        \\
        \\  tigerbeetle version --verbose
        \\
        \\  tigerbeetle client --addresses=3003,3002,3001 --cluster=0
        \\
    , .{
        .default_address = constants.address,
        .default_port = constants.port,
    });
};

pub const Command = union(enum) {
    pub const Start = struct {
        args_allocated: std.process.ArgIterator,
        addresses: []net.Address,
        cache_accounts: u32,
        cache_transfers: u32,
        cache_transfers_posted: u32,
        storage_size_limit: u64,
        cache_grid_blocks: u32,
        path: [:0]const u8,
    };

    pub const Repl = struct {
        addresses: []net.Address,
        cluster: u128,
        verbose: bool,
        statements: []const u8,
    };

    format: struct {
        args_allocated: std.process.ArgIterator,
        cluster: u128,
        replica: u8,
        replica_count: u8,
        path: [:0]const u8,
    },
    start: Start,
    version: struct {
        args_allocated: std.process.ArgIterator,
        verbose: bool,
    },
    repl: Repl,

    pub fn deinit(command: *Command, allocator: std.mem.Allocator) void {
        switch (command.*) {
            .start => |*start| allocator.free(start.addresses),
            else => {},
        }
        switch (command.*) {
            .repl => {},
            inline else => |*cmd| cmd.args_allocated.deinit(),
        }
        command.* = undefined;
    }
};

/// Parse the command line arguments passed to the `tigerbeetle` binary.
/// Exits the program with a non-zero exit code if an error is found.
pub fn parse_args(allocator: std.mem.Allocator) !Command {
    // This iterator owns the arguments and is passed to the caller as a part of `Command`.
    var args = try std.process.argsWithAllocator(allocator);
    errdefer args.deinit(allocator);

    // Skip argv[0] which is the name of this executable
    assert(args.skip());

    const cli_args = flags.parse(&args, CliArgs);

    switch (cli_args) {
        .version => |version| {
            return Command{
                .version = .{
                    .args_allocated = args,
                    .verbose = version.verbose,
                },
            };
        },
        .format => |format| {
            if (format.replica_count == 0) {
                flags.fatal("--replica-count: value needs to be greater than zero", .{});
            }
            if (format.replica_count > constants.replicas_max) {
                flags.fatal("--replica-count: value is too large ({}), at most {} is allowed", .{
                    format.replica_count,
                    constants.replicas_max,
                });
            }

            if (format.replica >= constants.standbys_max + format.replica_count) {
                flags.fatal("--replica: value is too large ({}), at most {} is allowed", .{
                    format.replica,
                    constants.standbys_max + format.replica_count - 1,
                });
            }

            return Command{
                .format = .{
                    .args_allocated = args,
                    .cluster = format.cluster, // just an ID, any value is allowed
                    .replica = format.replica,
                    .replica_count = format.replica_count,
                    .path = format.positional.path,
                },
            };
        },
        .start => |start| {
            const groove_config = StateMachine.Forest.groove_config;
            const AccountsValuesCache = groove_config.accounts.ObjectsCache.Cache;
            const TransfersValuesCache = groove_config.transfers.ObjectsCache.Cache;
            const PostedValuesCache = groove_config.posted.ObjectsCache.Cache;

            const addresses = parse_addresses(allocator, start.addresses);

            const storage_size_limit = start.limit_storage.bytes;
            const storage_size_limit_min = data_file_size_min;
            const storage_size_limit_max = constants.storage_size_limit_max;
            if (storage_size_limit > storage_size_limit_max) {
                flags.fatal("--storage-size-limit: size {} exceeds maximum: {}", .{
                    storage_size_limit,
                    storage_size_limit_max,
                });
            }
            if (storage_size_limit < storage_size_limit_min) {
                flags.fatal("--storage-size-limit: size {} is below minimum: {}", .{
                    storage_size_limit,
                    storage_size_limit_min,
                });
            }
            if (storage_size_limit % constants.sector_size != 0) {
                flags.fatal(
                    "--storage-size-limit: size {} must be a multiple of sector size ({})",
                    .{ storage_size_limit, constants.sector_size },
                );
            }

            return Command{
                .start = .{
                    .args_allocated = args,
                    .addresses = addresses,
                    .storage_size_limit = storage_size_limit,
                    .cache_accounts = parse_cache_size_to_count(
                        tigerbeetle.Account,
                        AccountsValuesCache,
                        start.cache_accounts,
                    ),
                    .cache_transfers = parse_cache_size_to_count(
                        tigerbeetle.Transfer,
                        TransfersValuesCache,
                        start.cache_transfers,
                    ),
                    .cache_transfers_posted = parse_cache_size_to_count(
                        StateMachine.PostedGrooveValue,
                        PostedValuesCache,
                        start.cache_transfers_posted,
                    ),
                    .cache_grid_blocks = parse_cache_size_to_count(
                        [constants.block_size]u8,
                        Grid.Cache,
                        start.cache_grid,
                    ),
                    .path = start.positional.path,
                },
            };
        },
        .client => |repl| {
            const addresses = parse_addresses(allocator, repl.addresses);

            return Command{
                .repl = .{
                    .addresses = addresses,
                    .cluster = repl.cluster,
                    .verbose = repl.verbose,
                    .statements = repl.command,
                },
            };
        },
    }
}

/// Parse and allocate the addresses returning a slice into that array.
fn parse_addresses(allocator: std.mem.Allocator, raw_addresses: []const u8) []net.Address {
    return vsr.parse_addresses(
        allocator,
        raw_addresses,
        constants.members_max,
    ) catch |err| switch (err) {
        error.AddressHasTrailingComma => flags.fatal("--addresses: invalid trailing comma", .{}),
        error.AddressLimitExceeded => {
            flags.fatal("--addresses: too many addresses, at most {d} are allowed", .{
                constants.members_max,
            });
        },
        error.AddressHasMoreThanOneColon => {
            flags.fatal("--addresses: invalid address with more than one colon", .{});
        },
        error.PortOverflow => flags.fatal("--addresses: port exceeds 65535", .{}),
        error.PortInvalid => flags.fatal("--addresses: invalid port", .{}),
        error.AddressInvalid => flags.fatal("--addresses: invalid IPv4 or IPv6 address", .{}),
        error.OutOfMemory => flags.fatal("out of memory", .{}),
    };
}

/// Given a limit like `10GiB`, a SetAssociativeCache and T return the largest `value_count_max`
/// that can fit in the limit.
fn parse_cache_size_to_count(
    comptime T: type,
    comptime SetAssociativeCache: type,
    size: flags.ByteSize,
) u32 {
    const value_count_max_multiple = SetAssociativeCache.value_count_max_multiple;

    const count_limit = @divFloor(size.bytes, @sizeOf(T));
    const count_rounded = @divFloor(
        count_limit,
        value_count_max_multiple,
    ) * value_count_max_multiple;

    const result = @as(u32, @intCast(count_rounded)); // TODO: better error message on overflow
    assert(@as(u64, result) * @sizeOf(T) <= size.bytes);

    return result;
}
