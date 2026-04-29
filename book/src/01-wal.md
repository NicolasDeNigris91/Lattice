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
This is "synchronous" durability. It is also the default for `db.put` so
that callers carried over from v1.0.x see no behaviour change.

Phases 1 through v1.0 use only this path. The v1.1 group commit story
adds a second mode that the next section walks through.

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

## Group commit (v1.1)

The synchronous path costs one `fsync` per call. On the development
machine that ceiling is roughly one thousand puts per second. For
workloads that can tolerate losing the last few writes after an
unclean shutdown, that ceiling is wasteful.

The v1.1 milestone adds a second append path that callers reach via
`WriteOptions { durable: false }`:

```rust
use lattice_core::{Lattice, WriteOptions};
let mut db = Lattice::open("./data")?;
let opts = WriteOptions { durable: false };
for record in batch {
    db.put_with(&record.key, &record.value, opts)?;
}
db.flush_wal()?; // one fsync covers the whole batch
```

Internally the WAL grows two more methods next to `append`:

- `append_pending` writes header and payload into the `BufWriter`
  but skips both the buffer flush and the `fsync`.
- `sync_pending` flushes the buffer to the OS and `fsync`s the file.

`db.put` still routes through `append`, so the v1.0.x guarantee is
unchanged. `db.put_with(.., WriteOptions { durable: false })` routes
through `append_pending`. A counter on the engine triggers a
`sync_pending` automatically once the configured `commit_batch`
threshold (default 64) is crossed, and `Drop` calls `flush_wal` so
that a graceful close loses nothing.

The honest trade-off is pinned by a test that calls `mem::forget` on
the engine after a single non-durable put: the bytes never leave the
user-space buffer, the next `open` does not see them, and the test
asserts that the engine has not silently turned non-durable into
durable. That is the contract.

The throughput payoff is measurable and large. On the same machine
that benches the synchronous path at ten seconds for ten thousand
puts, the amortised path takes about a hundred and sixty
milliseconds: roughly sixty times faster. Chapter seven walks through
the criterion output.

A future "commit window" knob (`LatticeBuilder::commit_window`) is
already on the builder for source compatibility, but it is wired up
in M2 because the timer needs a background thread and a shared WAL
that M2's concurrency rework will set up. Until then only the batch
threshold and explicit `flush_wal` calls trigger an `fsync`.

## Trade-offs

The synchronous default is the safe choice and stays the default. The
amortised path is the opt-in for callers that value throughput and
will checkpoint themselves. Both share the same on-disk format, so
nothing changes for the replay loop.

The CRC is `crc32fast` (IEEE polynomial). It is fine for catching torn
writes. It would not be fine as a defence against a hostile filesystem,
but defence against a hostile filesystem is a different project.

[wal]: https://github.com/NicolasDeNigris91/Lattice/blob/main/crates/lattice-core/src/wal.rs
[test]: https://github.com/NicolasDeNigris91/Lattice/blob/main/crates/lattice-core/tests/integration_roundtrip.rs
