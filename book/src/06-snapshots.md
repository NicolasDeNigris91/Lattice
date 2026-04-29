# Snapshots

A snapshot is a read-only view of the database, frozen at the moment
the snapshot was taken. Subsequent writes, flushes, and compactions on
the parent do not change what the snapshot sees. This chapter is short
because the implementation, in [`snapshot.rs`][snapshot], leans hard on
work the previous phases already did.

## What you can do with one

The motivating use case is iteration that must look coherent even while
writes are landing. A `scan` that returns "all keys at this moment"
needs to ignore writes that begin after it starts, otherwise iterators
return inconsistent prefixes. Backups have the same shape: dump the
state at one instant.

```rust
let snap = db.snapshot();
// Concurrent writes happening on `db` are invisible to `snap`.
for (k, v) in snap.scan(None)? {
    write_to_backup(&k, &v)?;
}
```

## The design

A `Snapshot` holds two things, both cheap:

1. A **clone of the memtable** at the moment of `snapshot()`. The
   memtable already sits behind a `BTreeMap`, so cloning is allocation
   per node but not I/O.
2. A **vector of `Arc<SSTableReader>`** copies. The parent already
   holds its readers as `Arc`, so a snapshot's clone bumps refcounts,
   not bytes.

`Snapshot::get` walks its frozen memtable first, exactly like
`Lattice::get` walks its live one. On `Lookup::Absent`, it falls
through to the snapshot's `Vec<Arc<SSTableReader>>`, newest first.

`Snapshot::scan` runs the same newest-source-wins merge that
`Lattice::scan` runs, but on the snapshot's frozen sources.

## Concurrency boundaries

A snapshot is `Send + Clone` and has no link back to the parent. You
can clone it, ship it across threads, and outlive the parent if you
hold the last `Arc` to its readers.

While a snapshot is alive, the parent is free to:

- Accept `put` and `delete` calls. Snapshot's memtable is unchanged.
- Run `flush`. The new `SSTable` joins the parent's list. The
  snapshot's list still holds references only to the older readers.
- Run `compact`. The parent's list is replaced with a single new
  reader. The snapshot's `Arc<SSTableReader>` instances keep the old
  readers alive, even after their files are removed from the live set.

## The Windows quirk

POSIX lets you delete a file while it is open: the inode lingers until
the last file descriptor closes. Windows, by default, refuses to
delete a file held open by any process. Lattice opens its `SSTable`
files with the standard library's default share modes, which do not
include `FILE_SHARE_DELETE`.

Concrete consequence: while a snapshot is alive on Windows, a
compaction's attempt to remove the obsolete `.sst` files fails with
`ACCESS_DENIED`. The compaction logs a warning and moves on. The
snapshot keeps reading correctly because its `Arc<SSTableReader>`
holds the file open. The leftover files are cleaned by the orphan
sweep on the next `Lattice::open`. Data integrity is unaffected; only
disk usage is briefly bloated.

## What snapshots do not give you

- Not a transaction. There is no commit, no rollback, no isolation
  for writes.
- Not a replica. The snapshot has no independent durability story.
  If the parent's directory is deleted, the snapshot still reads from
  its open file handles, but a fresh `open` would not see anything.
- Not free. Cloning the memtable allocates per-key memory.

[snapshot]: https://github.com/NicolasDeNigris91/Lattice/blob/main/crates/lattice-core/src/snapshot.rs
