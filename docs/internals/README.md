# TigerBeetle Internals

Welcome, wanderer! You are looking at the TigerBeetle's internal documentation. If you want to _use_
TigerBeetle, you don't need to read this and could head straight to our user-level docs at

<https://docs.tigerbeetle.com>

If you want to lear how TigerBeetle works inside, here's what we got:

- [TIGER_STYLE](./TIGER_STYLE.md) is _the_ style guide, and more. This is the philosophy underlining
  all the code here!
- [HACKING](./HACKING.md) gets you up to speed with building the codebase and running the tests.
- [DESIGN](./DESIGN.md) is a one-page technical intro into TigerBeetle. If you are learning the
  codebase, start here.
- [Data File](./data_file.md) is a good second read. Following Fred Brooks' advice, it explains what
  data is stored where and why.
- [VSR](./vsr.md) explains the upper consensus half of TigerBeetle.
  - [Sync](./sync.md) covers state synchronization for lagging replicas,
  - [Upgrades](./upgrades.md) are just sooo cool, you must read this!
- [LSM](./lsm.md) covers the lower storage half.
- [Releases](./releases.md) is our release process. 
- [Talks](./talks.md) is the list of talks about TigerBeetle so far!
- [VOPR](./vopr.md) and [testing](./testing.md) cover the simulator.
