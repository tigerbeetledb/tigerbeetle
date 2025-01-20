//! Verify Journal/WAL properties.
const std = @import("std");
const assert = std.debug.assert;
const log = std.log.scoped(.journal_checker);

const TestStorage = @import("../storage.zig").Storage;

pub fn JournalCheckerType(comptime Replica: type) type {
    return struct {
        pub fn check(replica: *const Replica) void {
            const replica_index = replica.replica;
            const replica_storage = replica.superblock.storage;
            comptime assert(@TypeOf(replica_storage) == *TestStorage);

            if (replica.journal.writes.executing() == 0) {
                // Sanity-check: Where the journal is clean and not being written to:
                // - Redundant headers exactly match their corresponding prepares.
                // - The WAL's content matches `journal.headers`.
                // - There are no zeroed entries (representing faulty journal entries) in the
                //   redundant WAL headers.
                var wal_header_errors: u32 = 0;
                for (
                    replica_storage.wal_headers(),
                    replica_storage.wal_prepares(),
                    replica.journal.headers,
                    0..,
                ) |*wal_header, *wal_prepare, *journal_header, slot| {
                    if (!replica.journal.dirty.bit(.{ .index = slot })) {
                        if (journal_header.operation == .reserved) {
                            // Ignore reserved headers -- when Journal.remove_entries_from()
                            // truncates the log, it cleans the in-memory journal without writing to
                            // the WAL.
                        } else {
                            if (wal_header.checksum == 0) {
                                log.err("{}: check: slot={} checksum=0", .{ replica_index, slot });
                                wal_header_errors += 1;
                            } else {
                                assert(wal_header.checksum == wal_prepare.header.checksum);
                                assert(wal_header.checksum == journal_header.checksum);
                            }
                        }
                    }
                }
                assert(wal_header_errors == 0);
            }
        }
    };
}
