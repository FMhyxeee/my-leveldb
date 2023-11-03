# My-levelDB
this project is a practice method about to learning database and rust.
It's one by one copy project from leveldb-rs.

## Status

In development; most of the infrastructure exists, btu the actual database logic
has not yet been implemented.

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
