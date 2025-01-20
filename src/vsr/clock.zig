//! Cluster-wide synchronized clock, aggregating timing information from all replicas.
//!
//! Time plays a central role in TigerBeetle data model. Because it is so important, TigerBeetle
//! defines its own time. In other words, we don't use time to drive consensus, we use consensus to
//! drive time!
//!
//! Time is important for the domain of accounting (e.g., pending transfers can expire with time),
//! but it can't be supplied by the client, as its clock can be unreliable. For this reason,
//! TigerBeetle needs to expose a "time service" to the state machine logic.
//!
//! Additionally, TigerBeetle needs to assign some kind of a sequence number to every event in the
//! system, to make it easy to say whether A happened before B or vice versa.
//!
//! Finally, to maintain indices, the LSM tree could benefit from a compact synthetic primary key.
//!
//! Time solves _all_ of these problems at once: each object in TigerBeetle gets tagged with a u64
//! nanosecond-precision creation timestamp. These timestamps are unique across all objects (an
//! Account and a Transfer can never have the same timestamp), consistent with linearization order
//! of the events (earlier events get smaller timestamps), and closely match the real wall-clock
//! time. Timestamps are used as internal synthetic primary keys instead of user-supplied random
//! u128 ids because they are smaller and also expose temporal locality.
//!
//! Implementation:
//!
//! The ultimate source of timestamps is each replica's operating system. This time is backed by a
//! replica-local drifty hardware clock which is periodically synchronized through NTP with high
//! quality clocks elsewhere. Using system time directly as a source of TigerBeetle timestamps
//! doesn't work:
//!
//! First, system time differs across replicas. To solve this problem, only the primary assigns
//! timestamps. Specifically, when the primary converts a request to a prepare, it assigns its
//! current time to the prepare. The state machine then assigns `prepare_timestamp + object_index`
//! as the creation timestamp for each object in a batch.
//!
//! Second, system time is not monotonic: due to NTP it can easily go backwards. To solve this
//! problem, the primary just takes the max between the current time and the previous timestamp
//! used. Notably, this ends up preserving monotonicity across restarts --- it is when replaying
//! past prepares from the WAL that a replica learns about the latest timestamp before restart.
//!
//! Third, replica's system time lacks high availability: if a primary is isolated from NTP servers
//! its local clock can drift significantly. Another problematic scenario is an operator error
//! which incorrectly adjusts primary's local clock to be far in the future, which, due to
//! monotonicity requirement, could render the cluster completely unusable.
//!
//! To solve the last problem, the primary aggregates clock information from the entire cluster and
//! calculates a timestamp value which is consistent with clocks on at least half of the replicas.
//!
//! Sketch of the algorithm:
//!
//! Assume you have six different clocks. Each clock shows a different time. Most are close, but
//! there could be outliers. How do you estimate the "true" time?
//!
//! The key insight is to think in intervals, rather than points. If a clock shows time t and
//! claims error margin Δ, it means the true time is in the [t-Δ;t+Δ] interval. If you have two
//! clocks, you can intersect their intervals to narrow down the true time interval. If the
//! intervals are disjoint, that means that at least one of the clocks is malfunctioning. This gives
//! an algorithm for identifying cluster time --- collect clock measurements from all replicas
//! together with the respective error margins and find an interval which is consistent with at
//! least half of the clocks.
//!
//! The first problem with the above plan is that clocks' error margins are not known. To solve
//! this, flip the problem around and find the smallest error margin that still allows for half of
//! the clocks' intervals to intersect. If this minimal error margin still ends up too large,
//! declare that the clocks are unsynchronized and wait for NTP to fix things up.
//!
//! The second problem with the plan is that a replica can only read its own clock. To learn other
//! replica's clock, the following algorithm is used:
//!
//! - A sends a ping message to B, including A's current time.
//! - B replies with a pong message, which includes a copy of the original ping timestamp, as well
//!   as B's current time.
//! - When A receives a pong, it uses the attached ping time to estimate the network delay and infer
//!   the clock offset from that.
//!
//! Further reading:
//!
//! [Three Clocks are Better than One](https://tigerbeetle.com/blog/2021-08-30-three-clocks-are-better-than-one)
//!
//! And watching:
//!
//! [Detecting Clock Sync Failure in Highly Available Systems](https://youtu.be/7R-Iz6sJG6Q?si=9sD2TpfD29AxUjOY)
const std = @import("std");
const assert = std.debug.assert;
const fmt = std.fmt;

const stdx = @import("../stdx.zig");
const log = stdx.log.scoped(.clock);
const constants = @import("../constants.zig");

const clock_offset_tolerance_max: u64 =
    constants.clock_offset_tolerance_max_ms * std.time.ns_per_ms;
const epoch_max: u64 = constants.clock_epoch_max_ms * std.time.ns_per_ms;
const window_min: u64 = constants.clock_synchronization_window_min_ms * std.time.ns_per_ms;
const window_max: u64 = constants.clock_synchronization_window_max_ms * std.time.ns_per_ms;

const Marzullo = @import("marzullo.zig").Marzullo;

pub fn ClockType(comptime Time: type) type {
    return struct {
        const Clock = @This();

        const Sample = struct {
            /// The relative difference between our wall clock reading and that of the remote clock
            //source.
            clock_offset: i64,
            one_way_delay: u64,
        };

        const Epoch = struct {
            /// The best clock offset sample per remote clock source (with minimum one way delay)
            /// collected over the course of a window period of several seconds.
            sources: []?Sample,

            /// The total number of samples learned while synchronizing this epoch.
            samples: usize,

            /// The monotonic clock timestamp when this epoch began. We use this to measure elapsed
            /// time.
            monotonic: u64,

            /// The wall clock timestamp when this epoch began. We add the elapsed monotonic time to
            /// this plus the synchronized clock offset to arrive at a synchronized realtime
            /// timestamp. We capture this realtime when starting the epoch, before we take any
            /// samples, to guard against any jumps in the system's realtime clock from impacting
            /// our measurements.
            realtime: i64,

            /// Once we have enough source clock offset samples in agreement, the epoch is
            /// synchronized. We then have lower and upper bounds on the true cluster time, and can
            /// install this epoch for subsequent clock readings. This epoch is then valid for
            /// several seconds, while clock drift has not had enough time to accumulate into any
            /// significant clock skew, and while we collect samples for the next epoch to refresh
            /// and replace this one.
            synchronized: ?Marzullo.Interval,

            /// A guard to prevent synchronizing too often without having learned any new samples.
            learned: bool = false,

            fn elapsed(epoch: *Epoch, clock: *Clock) u64 {
                return clock.monotonic() - epoch.monotonic;
            }

            fn reset(epoch: *Epoch, clock: *Clock) void {
                @memset(epoch.sources, null);
                // A replica always has zero clock offset and network delay to its own system time
                // reading:
                epoch.sources[clock.replica] = Sample{
                    .clock_offset = 0,
                    .one_way_delay = 0,
                };
                epoch.samples = 1;
                epoch.monotonic = clock.monotonic();
                epoch.realtime = clock.realtime();
                epoch.synchronized = null;
                epoch.learned = false;
            }

            fn sources_sampled(epoch: *Epoch) usize {
                var count: usize = 0;
                for (epoch.sources) |sampled| {
                    if (sampled != null) count += 1;
                }
                return count;
            }
        };

        /// The index of the replica using this clock to provide synchronized time.
        replica: u8,
        /// Minimal number of distinct clock sources required for synchronization.
        quorum: u8,

        /// The underlying time source for this clock (system time or deterministic time).
        time: *Time,

        /// An epoch from which the clock can read synchronized clock timestamps within safe bounds.
        /// At least `constants.clock_synchronization_window_min_ms` is needed for this to be ready
        /// to use.
        epoch: Epoch,

        /// The next epoch (collecting samples and being synchronized) to replace the current epoch.
        window: Epoch,

        /// A static allocation to convert window samples into tuple bounds for Marzullo's
        /// algorithm.
        marzullo_tuples: []Marzullo.Tuple,

        /// A kill switch to revert to unsynchronized realtime.
        synchronization_disabled: bool,

        pub fn init(
            allocator: std.mem.Allocator,
            time: *Time,
            options: struct {
                /// The size of the cluster, i.e. the number of clock sources (including this
                /// replica).
                replica_count: u8,
                replica: u8,
                quorum: u8,
            },
        ) !Clock {
            assert(options.replica_count > 0);
            assert(options.replica < options.replica_count);
            assert(options.quorum > 0);
            assert(options.quorum <= options.replica_count);
            if (options.replica_count > 1) assert(options.quorum > 1);

            var epoch: Epoch = undefined;
            epoch.sources = try allocator.alloc(?Sample, options.replica_count);
            errdefer allocator.free(epoch.sources);

            var window: Epoch = undefined;
            window.sources = try allocator.alloc(?Sample, options.replica_count);
            errdefer allocator.free(window.sources);

            // There are two Marzullo tuple bounds (lower and upper) per source clock offset sample:
            const marzullo_tuples = try allocator.alloc(Marzullo.Tuple, options.replica_count * 2);
            errdefer allocator.free(marzullo_tuples);

            var self = Clock{
                .replica = options.replica,
                .quorum = options.quorum,
                .time = time,
                .epoch = epoch,
                .window = window,
                .marzullo_tuples = marzullo_tuples,
                // A cluster of one cannot synchronize.
                .synchronization_disabled = options.replica_count == 1,
            };

            // Reset the current epoch to be unsynchronized,
            self.epoch.reset(&self);
            // and open a new epoch window to start collecting samples...
            self.window.reset(&self);

            return self;
        }

        pub fn deinit(self: *Clock, allocator: std.mem.Allocator) void {
            allocator.free(self.epoch.sources);
            allocator.free(self.window.sources);
            allocator.free(self.marzullo_tuples);
        }

        /// Called by `Replica.on_pong()` with:
        /// * the index of the `replica` that has replied to our ping with a pong,
        /// * our monotonic timestamp `m0` embedded in the ping we sent, carried over
        ///   into this pong,
        /// * the remote replica's `realtime()` timestamp `t1`, and
        /// * our monotonic timestamp `m2` as captured by our `Replica.on_pong()` handler.
        pub fn learn(self: *Clock, replica: u8, m0: u64, t1: i64, m2: u64) void {
            assert(replica != self.replica);

            if (self.synchronization_disabled) return;

            // Our m0 and m2 readings should always be monotonically increasing if not equal.
            // Crucially, it is possible for a very fast network to have m0 == m2, especially where
            // `constants.tick_ms` is at a more course granularity. We must therefore tolerate RTT=0
            // or otherwise we would have a liveness bug simply because we would be throwing away
            // perfectly good clock samples.
            // This condition should never be true. Reject this as a bad sample:
            if (m0 > m2) {
                log.warn("{}: learn: m0={} > m2={}", .{ self.replica, m0, m2 });
                return;
            }

            // The window was reset between a ping and the corresponding pong.
            if (m0 < self.window.monotonic) {
                log.debug("{}: learn: m0={} < window.monotonic={}", .{
                    self.replica,
                    m0,
                    self.window.monotonic,
                });
                return;
            }
            assert(m2 >= self.window.monotonic); // Guaranteed by monotonicity of our local Time.

            const elapsed: u64 = m2 - self.window.monotonic;
            if (elapsed > window_max) {
                log.warn("{}: learn: elapsed={} > window_max={}", .{
                    self.replica,
                    elapsed,
                    window_max,
                });
                return;
            }

            const round_trip_time: u64 = m2 - m0;
            const one_way_delay: u64 = round_trip_time / 2;
            const t2: i64 = self.window.realtime + @as(i64, @intCast(elapsed));
            const clock_offset: i64 = t1 + @as(i64, @intCast(one_way_delay)) - t2;
            const asymmetric_delay = self.estimate_asymmetric_delay(
                replica,
                one_way_delay,
                clock_offset,
            );
            const clock_offset_corrected = clock_offset + asymmetric_delay;

            log.debug("{}: learn: replica={} m0={} t1={} m2={} t2={} one_way_delay={} " ++
                "asymmetric_delay={} clock_offset={}", .{
                self.replica,
                replica,
                m0,
                t1,
                m2,
                t2,
                one_way_delay,
                asymmetric_delay,
                clock_offset_corrected,
            });

            // The less network delay, the more likely we have an accurate clock offset measurement:
            self.window.sources[replica] = minimum_one_way_delay(
                self.window.sources[replica],
                Sample{
                    .clock_offset = clock_offset_corrected,
                    .one_way_delay = one_way_delay,
                },
            );

            self.window.samples += 1;

            // We decouple calls to `synchronize()` so that it's not triggered by these network
            // events. Otherwise, excessive duplicate network packets would burn the CPU.
            self.window.learned = true;
        }

        /// Called by `Replica.on_ping_timeout()` to provide `m0` when we decide to send a ping.
        /// Called by `Replica.on_pong()` to provide `m2` when we receive a pong.
        /// Called by `Replica.on_commit_message_timeout()` to allow backups to discard
        /// duplicate/misdirected heartbeats.
        pub fn monotonic(self: *Clock) u64 {
            return self.time.monotonic();
        }

        /// Called by `Replica.on_ping()` when responding to a ping with a pong.
        /// This should never be used by the state machine, only for measuring clock offsets.
        pub fn realtime(self: *Clock) i64 {
            return self.time.realtime();
        }

        /// Called by `Replica.on_request()` when the primary wants to timestamp a batch. If the
        /// primary's clock is not synchronized with the cluster, it must wait until it is.
        /// it is.
        /// Returns the system time clamped to be within our synchronized lower and upper bounds.
        /// This is complementary to NTP and allows clusters with very accurate time to make use of
        /// it, while providing guard rails for when NTP is partitioned or unable to correct quickly
        /// enough.
        pub fn realtime_synchronized(self: *Clock) ?i64 {
            if (self.synchronization_disabled) {
                return self.realtime();
            } else if (self.epoch.synchronized) |interval| {
                const elapsed = @as(i64, @intCast(self.epoch.elapsed(self)));
                return std.math.clamp(
                    self.realtime(),
                    self.epoch.realtime + elapsed + interval.lower_bound,
                    self.epoch.realtime + elapsed + interval.upper_bound,
                );
            } else {
                return null;
            }
        }

        pub fn round_trip_time_median_ns(self: *const Clock) ?u64 {
            // +1 to allow for the standby.
            var one_way_delays = stdx.BoundedArrayType(u64, constants.replicas_max + 1){};
            for (self.window.sources, 0..) |source, replica_index| {
                if (self.replica != replica_index) {
                    if (source) |sampled| {
                        one_way_delays.append_assume_capacity(sampled.one_way_delay);
                    }
                }
            }

            if (one_way_delays.count() < self.quorum) {
                return null;
            } else {
                std.mem.sort(u64, one_way_delays.slice(), {}, std.sort.asc(u64));
                const one_way_delay_median =
                    one_way_delays.get(@divFloor(one_way_delays.count(), 2));
                return one_way_delay_median * 2;
            }
        }

        pub fn tick(self: *Clock) void {
            self.time.tick();

            if (self.synchronization_disabled) return;
            self.synchronize();
            // Expire the current epoch if successive windows failed to synchronize:
            // Gradual clock drift prevents us from using an epoch for more than a few seconds.
            if (self.epoch.elapsed(self) >= epoch_max) {
                log.err(
                    "{}: no agreement on cluster time (partitioned or too many clock faults)",
                    .{self.replica},
                );
                self.epoch.reset(self);
            }
        }

        /// Estimates the asymmetric delay for a sample compared to the previous window,
        /// according to Algorithm 1 from Section 4.2,
        /// "A System for Clock Synchronization in an Internet of Things".
        ///
        /// Note that it is impossible to estimate persistent asymmetric delay, as these two
        /// situations are indistinguishable:
        /// - A and B have synchronized clocks and a 50ms symmetrical delay.
        /// - B's clock is 50ms ahead, A → B delay is 0ms, B → A delay is 100ms.
        ///
        /// In both of these cases, A and B observe that a ping-pong round trip takes 100ms and that
        /// a pong's timestamp is 50ms ahead of ping's timestamp.
        ///
        /// Instead, the model here is of a one-time delay --- a particular ping or pong message
        /// got delayed because it had a large prepare message in front of it in the send queue, a
        /// network packet got lost, or a pigeon got eaten by a cat.
        ///
        /// The delay happened either for the ping (forward path) or for the pong (reverse path)
        /// message. Assuming that the minimum RTT seen before is a no-delay situation, the
        /// magnitude of a delay for the current sample can be estimated as RTT - min(RTT), and the
        /// direction (forward/reverse) distinguished by comparing unadjusted clock offsets.
        ///
        /// Previous window is used to determine min(RTT).
        fn estimate_asymmetric_delay(
            self: *Clock,
            replica: u8,
            one_way_delay: u64,
            clock_offset: i64,
        ) i64 {
            // Note that `one_way_delay` may be 0 for very fast networks.

            const error_margin = 10 * std.time.ns_per_ms;

            if (self.epoch.sources[replica]) |epoch| {
                if (one_way_delay <= epoch.one_way_delay) {
                    return 0;
                } else if (clock_offset > epoch.clock_offset + error_margin) {
                    // The asymmetric error is on the forward network path.
                    return 0 - @as(i64, @intCast(one_way_delay - epoch.one_way_delay));
                } else if (clock_offset < epoch.clock_offset - error_margin) {
                    // The asymmetric error is on the reverse network path.
                    return 0 + @as(i64, @intCast(one_way_delay - epoch.one_way_delay));
                } else {
                    return 0;
                }
            } else {
                return 0;
            }
        }

        fn synchronize(self: *Clock) void {
            assert(self.window.synchronized == null);

            // Wait until the window has enough accurate samples:
            const elapsed = self.window.elapsed(self);
            if (elapsed < window_min) return;
            if (elapsed >= window_max) {
                // We took too long to synchronize the window, expire stale samples...
                const sources_sampled = self.window.sources_sampled();
                if (sources_sampled <= @divTrunc(self.window.sources.len, 2)) {
                    log.warn("{}: synchronization failed, partitioned (sources={} samples={})", .{
                        self.replica,
                        sources_sampled,
                        self.window.samples,
                    });
                } else {
                    log.warn("{}: synchronization failed, no agreement (sources={} samples={})", .{
                        self.replica,
                        sources_sampled,
                        self.window.samples,
                    });
                }
                self.window.reset(self);
                return;
            }

            if (!self.window.learned) return;
            // Do not reset `learned` any earlier than this (before we have attempted to
            // synchronize).
            self.window.learned = false;

            // Starting with the most clock offset tolerance, while we have a quorum, find the best
            // smallest interval with the least clock offset tolerance, reducing tolerance at each
            // step:
            var tolerance: u64 = clock_offset_tolerance_max;
            var terminate = false;
            var rounds: usize = 0;
            // Do at least one round if tolerance=0 and cap the number of rounds to avoid runaway
            // loops.
            while (!terminate and rounds < 64) : (tolerance /= 2) {
                if (tolerance == 0) terminate = true;
                rounds += 1;

                const interval = Marzullo.smallest_interval(self.window_tuples(tolerance));
                if (interval.sources_true < self.quorum) break;

                // The new interval may reduce the number of `sources_true` while also decreasing
                // error. In other words, provided we maintain a quorum, we prefer tighter tolerance
                // bounds.
                self.window.synchronized = interval;
            }

            // Wait for more accurate samples or until we timeout the window for lack of quorum:
            if (self.window.synchronized == null) return;

            // Transitioning from not being synchronized to being synchronized - log out a message
            // for the operator, as the counterpoint to `no agreement on cluster time`.
            if (self.epoch.synchronized == null and self.window.synchronized != null) {
                const new_interval = self.window.synchronized.?;
                log.info("{}: synchronized: accuracy={}", .{
                    self.replica,
                    fmt.fmtDurationSigned(new_interval.upper_bound - new_interval.lower_bound),
                });
            }

            var new_window = self.epoch;
            new_window.reset(self);
            self.epoch = self.window;
            self.window = new_window;

            self.after_synchronization();
        }

        fn after_synchronization(self: *Clock) void {
            const new_interval = self.epoch.synchronized.?;

            log.debug("{}: synchronized: truechimers={}/{} clock_offset={}..{} accuracy={}", .{
                self.replica,
                new_interval.sources_true,
                self.epoch.sources.len,
                fmt.fmtDurationSigned(new_interval.lower_bound),
                fmt.fmtDurationSigned(new_interval.upper_bound),
                fmt.fmtDurationSigned(new_interval.upper_bound - new_interval.lower_bound),
            });

            const elapsed: i64 = @intCast(self.epoch.elapsed(self));
            const system = self.realtime();
            const lower = self.epoch.realtime + elapsed + new_interval.lower_bound;
            const upper = self.epoch.realtime + elapsed + new_interval.upper_bound;
            const cluster = std.math.clamp(system, lower, upper);

            if (system == cluster) {} else if (system < lower) {
                const delta = lower - system;
                if (delta < std.time.ns_per_ms) {
                    log.debug("{}: system time is {} behind", .{
                        self.replica,
                        fmt.fmtDurationSigned(delta),
                    });
                } else {
                    log.warn(
                        "{}: system time is {} behind, clamping system time to cluster time",
                        .{
                            self.replica,
                            fmt.fmtDurationSigned(delta),
                        },
                    );
                }
            } else {
                const delta = system - upper;
                if (delta < std.time.ns_per_ms) {
                    log.debug("{}: system time is {} ahead", .{
                        self.replica,
                        fmt.fmtDurationSigned(delta),
                    });
                } else {
                    log.warn("{}: system time is {} ahead, clamping system time to cluster time", .{
                        self.replica,
                        fmt.fmtDurationSigned(delta),
                    });
                }
            }
        }

        fn window_tuples(self: *Clock, tolerance: u64) []Marzullo.Tuple {
            assert(self.window.sources[self.replica].?.clock_offset == 0);
            assert(self.window.sources[self.replica].?.one_way_delay == 0);
            var count: usize = 0;
            for (self.window.sources, 0..) |sampled, source| {
                if (sampled) |sample| {
                    self.marzullo_tuples[count] = Marzullo.Tuple{
                        .source = @intCast(source),
                        .offset = sample.clock_offset -
                            @as(i64, @intCast(sample.one_way_delay + tolerance)),
                        .bound = .lower,
                    };
                    count += 1;
                    self.marzullo_tuples[count] = Marzullo.Tuple{
                        .source = @intCast(source),
                        .offset = sample.clock_offset +
                            @as(i64, @intCast(sample.one_way_delay + tolerance)),
                        .bound = .upper,
                    };
                    count += 1;
                }
            }
            return self.marzullo_tuples[0..count];
        }

        fn minimum_one_way_delay(a: ?Sample, b: ?Sample) ?Sample {
            if (a == null) return b;
            if (b == null) return a;
            if (a.?.one_way_delay < b.?.one_way_delay) return a;
            // Choose B if B's one way delay is less or the same (we assume B is the newer sample):
            return b;
        }
    };
}

const testing = std.testing;
const OffsetType = @import("../testing/time.zig").OffsetType;
const DeterministicTime = @import("../testing/time.zig").Time;
const DeterministicClock = ClockType(DeterministicTime);

const ClockUnitTestContainer = struct {
    time: DeterministicTime,
    clock: DeterministicClock,
    rtt: u64 = 300 * std.time.ns_per_ms,
    owd: u64 = 150 * std.time.ns_per_ms,
    learn_interval: u64 = 5,

    pub fn init(
        self: *ClockUnitTestContainer,
        allocator: std.mem.Allocator,
        offset_type: OffsetType,
        offset_coefficient_A: i64,
        offset_coefficient_B: i64,
    ) !void {
        self.* = .{
            .time = .{
                .resolution = std.time.ns_per_s / 2,
                .offset_type = offset_type,
                .offset_coefficient_A = offset_coefficient_A,
                .offset_coefficient_B = offset_coefficient_B,
            },
            .clock = try DeterministicClock.init(allocator, &self.time, .{
                .replica_count = 3,
                .replica = 0,
                .quorum = 2,
            }),
        };
    }

    pub fn run_till_tick(self: *ClockUnitTestContainer, tick: u64) void {
        while (self.clock.time.ticks < tick) {
            self.clock.time.tick();

            if (@mod(self.clock.time.ticks, self.learn_interval) == 0) {
                const on_pong_time = self.clock.monotonic();
                const m0 = on_pong_time - self.rtt;
                const t1: i64 = @intCast(on_pong_time - self.owd);

                self.clock.learn(1, m0, t1, on_pong_time);
                self.clock.learn(2, m0, t1, on_pong_time);
            }

            self.clock.synchronize();
        }
    }

    const AssertionPoint = struct {
        tick: u64,
        expected_offset: i64,
    };
    pub fn ticks_to_perform_assertions(self: *ClockUnitTestContainer) [3]AssertionPoint {
        var ret: [3]AssertionPoint = undefined;
        switch (self.clock.time.offset_type) {
            .linear => {
                // For the first (OWD/drift per tick) ticks, the offset < OWD. This means that the
                // Marzullo interval is [0,0] (the offset and OWD are 0 for a replica w.r.t.
                // itself). Therefore the offset of `clock.realtime_synchronised` will be the
                // analytically prescribed offset at the start of the window.
                // Beyond this, the offset > OWD and the Marzullo interval will be from replica 1
                // and replica 2. The `clock.realtime_synchronized` will be clamped to the lower
                // bound. Therefore the `clock.realtime_synchronized` will be offset by the OWD.
                const threshold = self.owd /
                    @as(u64, @intCast(self.clock.time.offset_coefficient_A));
                ret[0] = .{
                    .tick = threshold,
                    .expected_offset = self.clock.time.offset(threshold - self.learn_interval),
                };
                ret[1] = .{
                    .tick = threshold + 100,
                    .expected_offset = @intCast(self.owd),
                };
                ret[2] = .{
                    .tick = threshold + 200,
                    .expected_offset = @intCast(self.owd),
                };
            },
            .periodic => {
                ret[0] = .{
                    .tick = @intCast(@divTrunc(self.clock.time.offset_coefficient_B, 4)),
                    .expected_offset = @intCast(self.owd),
                };
                ret[1] = .{
                    .tick = @intCast(@divTrunc(self.clock.time.offset_coefficient_B, 2)),
                    .expected_offset = 0,
                };
                ret[2] = .{
                    .tick = @intCast(@divTrunc(self.clock.time.offset_coefficient_B * 3, 4)),
                    .expected_offset = -@as(i64, @intCast(self.owd)),
                };
            },
            .step => {
                ret[0] = .{
                    .tick = @intCast(self.clock.time.offset_coefficient_B - 10),
                    .expected_offset = 0,
                };
                ret[1] = .{
                    .tick = @intCast(self.clock.time.offset_coefficient_B + 10),
                    .expected_offset = -@as(i64, @intCast(self.owd)),
                };
                ret[2] = .{
                    .tick = @intCast(self.clock.time.offset_coefficient_B + 10),
                    .expected_offset = -@as(i64, @intCast(self.owd)),
                };
            },
            .non_ideal => unreachable, // use ideal clocks for the unit tests
        }

        return ret;
    }
};

test "ideal clocks get clamped to cluster time" {
    // Silence all clock logs.
    const level = std.testing.log_level;
    std.testing.log_level = std.log.Level.err;
    defer std.testing.log_level = level;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var ideal_constant_drift_clock: ClockUnitTestContainer = undefined;
    try ideal_constant_drift_clock.init(
        allocator,
        OffsetType.linear,
        std.time.ns_per_ms, // loses 1ms per tick
        0,
    );
    const linear_clock_assertion_points = ideal_constant_drift_clock.ticks_to_perform_assertions();
    for (linear_clock_assertion_points) |point| {
        ideal_constant_drift_clock.run_till_tick(point.tick);
        try testing.expectEqual(
            point.expected_offset,
            @as(i64, @intCast(ideal_constant_drift_clock.clock.monotonic())) -
                ideal_constant_drift_clock.clock.realtime_synchronized().?,
        );
    }

    var ideal_periodic_drift_clock: ClockUnitTestContainer = undefined;
    try ideal_periodic_drift_clock.init(
        allocator,
        OffsetType.periodic,
        std.time.ns_per_s, // loses up to 1s
        200, // period of 200 ticks
    );
    const ideal_periodic_drift_clock_assertion_points =
        ideal_periodic_drift_clock.ticks_to_perform_assertions();
    for (ideal_periodic_drift_clock_assertion_points) |point| {
        ideal_periodic_drift_clock.run_till_tick(point.tick);
        try testing.expectEqual(
            point.expected_offset,
            @as(i64, @intCast(ideal_periodic_drift_clock.clock.monotonic())) -
                ideal_periodic_drift_clock.clock.realtime_synchronized().?,
        );
    }

    var ideal_jumping_clock: ClockUnitTestContainer = undefined;
    try ideal_jumping_clock.init(
        allocator,
        OffsetType.step,
        -5 * std.time.ns_per_day, // jumps 5 days ahead.
        49, // after 49 ticks
    );
    const ideal_jumping_clock_assertion_points = ideal_jumping_clock.ticks_to_perform_assertions();
    for (ideal_jumping_clock_assertion_points) |point| {
        ideal_jumping_clock.run_till_tick(point.tick);
        try testing.expectEqual(
            point.expected_offset,
            @as(i64, @intCast(ideal_jumping_clock.clock.monotonic())) -
                ideal_jumping_clock.clock.realtime_synchronized().?,
        );
    }
}

const PacketSimulatorOptions = @import("../testing/packet_simulator.zig").PacketSimulatorOptions;
const PacketSimulatorType = @import("../testing/packet_simulator.zig").PacketSimulatorType;
const Path = @import("../testing/packet_simulator.zig").Path;
const Command = @import("../vsr.zig").Command;
const ClockSimulator = struct {
    const Packet = struct {
        m0: u64,
        t1: ?i64,
        clock_simulator: *ClockSimulator,

        pub fn clone(packet: *const Packet) Packet {
            return packet.*;
        }

        /// PacketSimulator requires this function, but we don't actually have anything to deinit.
        pub fn deinit(packet: *const Packet) void {
            _ = packet;
        }

        pub fn command(_: *const Packet) Command {
            return .ping; // Value doesn't matter.
        }
    };

    const Options = struct {
        ping_timeout: u32,
        clock_count: u8,
        network_options: PacketSimulatorOptions,
    };

    allocator: std.mem.Allocator,
    options: Options,
    ticks: u64 = 0,
    network: PacketSimulatorType(Packet),
    times: []DeterministicTime,
    clocks: []DeterministicClock,
    prng: std.rand.DefaultPrng,

    pub fn init(allocator: std.mem.Allocator, options: Options) !ClockSimulator {
        var network = try PacketSimulatorType(Packet).init(allocator, options.network_options);
        errdefer network.deinit(allocator);

        var times = try allocator.alloc(DeterministicTime, options.clock_count);
        errdefer allocator.free(times);

        var clocks = try allocator.alloc(DeterministicClock, options.clock_count);
        errdefer allocator.free(clocks);

        var prng = std.rand.DefaultPrng.init(options.network_options.seed);

        for (clocks, 0..) |*clock, replica| {
            errdefer for (clocks[0..replica]) |*c| c.deinit(allocator);

            const amplitude = prng.random().intRangeAtMost(i64, -10, 10) * std.time.ns_per_s;
            const phase = prng.random().intRangeAtMost(i64, 100, 1000) +
                @as(i64, @intFromFloat(prng.random().floatNorm(f64) * 50));
            times[replica] = .{
                .resolution = std.time.ns_per_s / 2, // delta_t = 0.5s
                .offset_type = OffsetType.non_ideal,
                .offset_coefficient_A = amplitude,
                .offset_coefficient_B = phase,
                .offset_coefficient_C = 10,
            };

            clock.* = try DeterministicClock.init(allocator, &times[replica], .{
                .replica_count = options.clock_count,
                .replica = @intCast(replica),
                .quorum = @divFloor(options.clock_count, 2) + 1,
            });
            errdefer clock.deinit(allocator);
        }
        errdefer for (clocks) |*clock| clock.deinit(allocator);

        return ClockSimulator{
            .allocator = allocator,
            .options = options,
            .network = network,
            .times = times,
            .clocks = clocks,
            .prng = prng,
        };
    }

    pub fn deinit(self: *ClockSimulator) void {
        for (self.clocks) |*clock| clock.deinit(self.allocator);
        self.allocator.free(self.clocks);
        self.allocator.free(self.times);
        self.network.deinit(self.allocator);
    }

    pub fn tick(self: *ClockSimulator) void {
        self.ticks += 1;
        self.network.tick();
        for (self.clocks) |*clock| {
            clock.tick();
        }

        for (self.clocks) |*clock| {
            if (clock.time.ticks % self.options.ping_timeout == 0) {
                const m0 = clock.monotonic();
                for (self.clocks, 0..) |_, target| {
                    if (target != clock.replica) {
                        self.network.submit_packet(
                            .{
                                .m0 = m0,
                                .t1 = null,
                                .clock_simulator = self,
                            },
                            ClockSimulator.handle_packet,
                            .{
                                .source = clock.replica,
                                .target = @intCast(target),
                            },
                        );
                    }
                }
            }
        }
    }

    fn handle_packet(packet: Packet, path: Path) void {
        const self = packet.clock_simulator;
        const target = &self.clocks[path.target];

        if (packet.t1) |t1| {
            target.learn(
                path.source,
                packet.m0,
                t1,
                target.monotonic(),
            );
        } else {
            self.network.submit_packet(
                .{
                    .m0 = packet.m0,
                    .t1 = target.realtime(),
                    .clock_simulator = self,
                },
                ClockSimulator.handle_packet,
                .{
                    // send the packet back to where it came from.
                    .source = path.target,
                    .target = path.source,
                },
            );
        }
    }
};

test "clock: fuzz test" {
    // Silence all clock logs.
    const level = std.testing.log_level;
    std.testing.log_level = std.log.Level.err;
    defer std.testing.log_level = level;

    const ticks_max: u64 = 1_000_000;
    const clock_count: u8 = 3;
    const SystemTime = @import("../testing/time.zig").Time;
    var system_time = SystemTime{
        .resolution = constants.tick_ms * std.time.ns_per_ms,
        .offset_type = .linear,
        .offset_coefficient_A = 0,
        .offset_coefficient_B = 0,
    };
    const seed: u64 = @intCast(system_time.realtime());
    var min_sync_error: u64 = 1_000_000_000;
    var max_sync_error: u64 = 0;
    var max_clock_offset: u64 = 0;
    var min_clock_offset: u64 = 1_000_000_000;
    var simulator = try ClockSimulator.init(std.testing.allocator, .{
        .network_options = .{
            .node_count = clock_count,
            .client_count = 0,
            .seed = seed,

            .one_way_delay_mean = 25,
            .one_way_delay_min = 10,
            .packet_loss_probability = 10,
            .path_maximum_capacity = 20,
            .path_clog_duration_mean = 200,
            .path_clog_probability = 2,
            .packet_replay_probability = 2,

            .partition_mode = .isolate_single,
            .partition_probability = 25,
            .unpartition_probability = 5,
            .partition_stability = 100,
            .unpartition_stability = 10,
        },
        .clock_count = clock_count,
        .ping_timeout = 20,
    });
    defer simulator.deinit();

    var clock_ticks_without_synchronization = [_]u32{0} ** clock_count;
    while (simulator.ticks < ticks_max) {
        simulator.tick();

        for (simulator.clocks, 0..) |*clock, index| {
            const offset = clock.time.offset(simulator.ticks);
            const abs_offset: u64 = if (offset >= 0) @intCast(offset) else @intCast(-offset);
            max_clock_offset = if (abs_offset > max_clock_offset) abs_offset else max_clock_offset;
            min_clock_offset = if (abs_offset < min_clock_offset) abs_offset else min_clock_offset;

            const synced_time = clock.realtime_synchronized() orelse {
                clock_ticks_without_synchronization[index] += 1;
                continue;
            };

            for (simulator.clocks, 0..) |*other_clock, other_clock_index| {
                if (index == other_clock_index) continue;
                const other_clock_sync_time = other_clock.realtime_synchronized() orelse {
                    continue;
                };
                const err: i64 = synced_time - other_clock_sync_time;
                const abs_err: u64 = if (err >= 0) @intCast(err) else @intCast(-err);
                max_sync_error = if (abs_err > max_sync_error) abs_err else max_sync_error;
                min_sync_error = if (abs_err < min_sync_error) abs_err else min_sync_error;
            }
        }
    }

    log.info("seed={}, max ticks={}, clock count={}\n", .{
        seed,
        ticks_max,
        clock_count,
    });
    log.info("absolute clock offsets with respect to test time:\n", .{});
    log.info("maximum={}\n", .{fmt.fmtDurationSigned(@as(i64, @intCast(max_clock_offset)))});
    log.info("minimum={}\n", .{fmt.fmtDurationSigned(@as(i64, @intCast(min_clock_offset)))});
    log.info("\nabsolute synchronization errors between clocks:\n", .{});
    log.info("maximum={}\n", .{fmt.fmtDurationSigned(@as(i64, @intCast(max_sync_error)))});
    log.info("minimum={}\n", .{fmt.fmtDurationSigned(@as(i64, @intCast(min_sync_error)))});
    log.info("clock ticks without synchronization={d}\n", .{
        clock_ticks_without_synchronization,
    });
}
