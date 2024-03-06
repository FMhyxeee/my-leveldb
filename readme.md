# My-levelDB
this project is a practice method about to learning database and rust.
It's one by one copy project from leveldb-rs.

**Goal：** A fully compatible implementation of levelDB in Rust.

The implementation is very close to the original; often, you can see the same
algorithm translated 1:1, and class (struct) and methods names are similar or the same

## Status

* User-facing methods exist: Read/Write/Delete; snapshots; iteration
* Compaction is supported, but no manual ones.
* Fully synchronous: Efficiency gains by using non-atomic types, but writes may
  occasionally block during a compaction.
* Compatibility: Not yet assessed.

## Goals

Some of the goals of this project are

* A few copies of data as possible; most of the time, slices of bytes (`&[u8]`)
  are used. Owned memory is represented as `Vec<u8>` (and then possibly borrowed
  as slice)
* Correctness  -- self-checking implementation, good test coverage, etc. Just
  like the original implementation.
* Clarity; commented code, clear structure (hopefully doing a better job than
  the original implementation).
* Coming close-ish to the original implementation; clarifying the translation of
  typical C++ constructs to Rust.
