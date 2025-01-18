# Production Ready

TigerBeetle is ready for production use on Linux as of version 0.15.3, which provides protocol and
storage stability, and a forward upgrade path, without requiring data migration.

TigerBeetle releases are tested in a deterministic simulator to apply model checking techniques but
on the actual code. For example, fault injection simulates network partitions, packet loss, crashes,
and even high levels of disk corruption, while time is accelerated in the simulated world by a
factor of up to seven hundred times to find bugs faster.

Beyond this level of testing, TigerBeetle follows NASA's Power of Ten Rules for Safety-Critical
Code, with 6000+ assertions to verify correctness at runtime, so that as far as possible,
TigerBeetle either runs correctly in production or else shuts down safely.

Please be aware that certain APIs, for example, how to query TigerBeetle to lookup accounts or
transfers, may be replaced by TigerBeetle's new Query engine when it ships. These features will be
simple to update in code, and will first be deprecated, with time to update between versions.

We are happy to provide assistance with operating TigerBeetle in production. Please contact our CEO,
Joran Dirk Greef, at <joran@tigerbeetle.com> if your company would like professional support.
