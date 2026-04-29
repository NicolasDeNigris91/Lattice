# The write ahead log

This is the chapter the rest of the database leans on. Every other component
exists to make reads fast or storage cheap, but if the WAL is wrong the
database forgets things, which is the worst sin a database can commit.

## The contract

A `put` or `delete` returns when the change is durable. Durable means, if
the process is killed at any moment after the call returns, a fresh `open`
on the same directory observes the change.

The unit of durability is the record. Half-written records do not count.
The reader of the WAL must be able to tell a torn write from a complete one
and stop replay before applying anything that might be wrong.

## The naive approach

The first thing one tries is to skip the log entirely and write the
memtable directly to disk on every mutation. That works exactly until you
notice that updating a `BTreeMap` on disk, in place, with crash safety, is
the problem we are trying to solve in the first place. Append-only logs
exist because appending is the only filesystem operation that is reliably
atomic at a useful granularity.

## The format

Records are little-endian. The header is eight bytes followed by a
variable-length payload:

```text
| crc32 (u32) | length (u32) | payload (length bytes) |
```

`crc32` covers the payload only. `length` is the size of the payload in
bytes, capped at four gigabytes per record. The payload is a `LogEntry`
encoded with `bincode` using the standard configuration:

```rust
pub(crate) enum LogEntry {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}
```

The full code for the format and the replay loop lives in
[`crates/lattice-core/src/wal.rs`][wal].

## Append

Each call to `Wal::append` writes the eight-byte header, writes the
payload, flushes the `BufWriter`, and calls `sync_data` on the underlying
file. By the time `append` returns, the bytes are on the storage medium.
This is "synchronous" durability. Group commit is a tunable Phase 5 will
reopen, not a Phase 1 problem.

## Replay

`Wal::open` reads records sequentially. For each record it reads eight
bytes of header, then `length` bytes of payload, then verifies the CRC. It
stops cleanly on three signals:

1. **EOF before a full header.** Treated as the natural end of the log.
2. **EOF inside the payload.** Treated as a torn write at the tail.
3. **CRC mismatch.** Also treated as a torn write at the tail.

The first two cases are common. The third happens when a sector was
half-written and the OS gave us back partial data. In all three cases the
correct behaviour is the same, return every record up to the failure and
discard everything after it.

A test in [`tests/integration_roundtrip.rs`][test] proves this by writing
a few entries, then appending thirty-two bytes of `0xFF` to the WAL, and
asserting the original entries are still readable on the next `open`.

## Trade-offs

`fsync` per write is durable, slow, and simple. Group commit batches many
writes into one `fsync`. RocksDB and PostgreSQL both group-commit by
default. Lattice does not, because Phase 1 is about getting the contract
right, not the throughput. We measure the cost in chapter seven and
revisit it in the "what is not yet implemented" appendix.

The CRC is `crc32fast` (IEEE polynomial). It is fine for catching torn
writes. It would not be fine as a defence against a hostile filesystem,
but defence against a hostile filesystem is a different project.

[wal]: https://github.com/NicolasDeNigris91/Lattice/blob/main/crates/lattice-core/src/wal.rs
[test]: https://github.com/NicolasDeNigris91/Lattice/blob/main/crates/lattice-core/tests/integration_roundtrip.rs
