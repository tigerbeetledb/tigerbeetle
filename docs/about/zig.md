# Zig

Coming from C, Zig is the language we've always wanted -- and the perfect language in which to write
a database focused on performance and safety.

We want:

- **C ABI compatibility** to embed the TigerBeetle leader library or TigerBeetle network client
  directly into any language, to match the portability and ease of use of the [SQLite
  library](https://www.sqlite.org/index.html), the most used database engine in the world.
- Bounds checking and checked arithmetic for greater memory safety than C.
- **Control the memory layout, alignment, and padding of data structures** to avoid cache misses and
  unaligned accesses and allow zero-copy parsing of data structures from off the network.
- **Explicit static memory allocation** from the network all the way to the disk with **no hidden
  memory allocations**. Zig also has a rich choice of allocators and test allocators.
- **OOM safety** as the TigerBeetle leader library needs to manage GiBs of in-memory state without
  crashing.
- Direct access to **io_uring** for fast, simple networking and file system operations.
- Direct access to **fast CPU instructions** such as `POPCNT`, which are essential for the hash
  table implementation we want to use.
- Direct access to **existing C libraries** without the overhead of FFI.
- **Strict single-threaded control flow** to eliminate data races by design and to enforce the
  single writer principle.
- **Compiler support for error sets** to enforce [fine-grained error
  handling](https://www.eecg.utoronto.ca/~yuan/papers/failure_analysis_osdi14.pdf).
- A developer-friendly and fast build system.
- [Comptime](https://zig.guide/language-basics/comptime/) is a game changer.

Zig retains C ABI interoperability, offers relief from undefined behavior and makefiles, and
provides an order of magnitude improvement in runtime safety and fine-grained error handling. Zig is
a good fit with its emphasis on explicit memory allocation and OOM safety.

Since Zig is pre-1.0.0 we plan to use only stable language features. It's an excellent time for
TigerBeetle to adopt Zig since our stable roadmaps will probably coincide. We wanted to invest for
the next 20 years and didn't want to be stuck with C/C++ or compiler/language complexity and pay a
tax for the lifetime of the project.
