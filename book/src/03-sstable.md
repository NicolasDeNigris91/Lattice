# Sorted string tables

Phase 1 left every byte in RAM. That works until you remember computers
have less RAM than disk, and that the WAL grows without bound until
something flushes it. The sorted string table is the immutable on-disk
container that takes a memtable, freezes it, and lets the engine forget
the corresponding WAL records.

## The problem an SSTable solves

The memtable wants to be small enough to fit in RAM. The WAL wants to be
short enough that replay on `open` is fast. Both wishes argue for moving
old writes out of the in-memory log and into a structure that:

1. Stores a fixed snapshot of keys and values, sorted by key.
2. Supports point lookups in roughly logarithmic time.
3. Supports a range scan with one sequential read.
4. Compresses well, because keys and values written in the same window
   often share structure.

A sorted, blocked, compressed file does all four. Lattice writes one such
file every flush.

## File layout

The shape below is the v2 on-disk layout used by the released code.
Phase 3 (chapter 4) introduced the bloom block; the original v1
layout shipped in this phase had no bloom and a 32-byte footer. The
diagram is updated so the rest of the book agrees with what `cargo
doc` and the source actually do.

```text
+------------------------------+
| data block 0  (lz4 compressed)|
| data block 1  (lz4 compressed)|
|              ...              |
| data block N  (lz4 compressed)|
+------------------------------+
| bloom filter block            |   <- added in Phase 3
+------------------------------+
| index block (uncompressed)    |
+------------------------------+
| footer (48 bytes)             |
+------------------------------+
```

Data blocks come first because they dominate the file size. The index
sits at the end so a writer that streams data blocks knows their
offsets and lengths only after it has written them all. The footer is
a fixed 48 bytes, so the reader can compute its position from the
file size with no hunting.

### A data block

Inside a data block, entries are concatenated:

```text
| flags: u8 | key_len: u32 | key | value_len: u32 | value |
```

`flags` is `0` for a put, `1` for a tombstone. Tombstones store `value_len = 0`
and no value bytes. Inside one block, keys are strictly ascending. The
target block size is four kilobytes after compression. We compress with
LZ4 because it gives meaningful ratios on string-shaped data and decodes
fast enough that the read path is dominated by I/O, not the codec.

### The index block

One entry per data block:

```text
| key_len: u32 | first_key | offset: u64 | compressed_len: u32 | uncompressed_len: u32 |
```

This is a sparse index. It does not list every key, only the first key of
each block. To find a target key, the reader binary-searches the index
for the rightmost block whose first key is less than or equal to the
target, then loads that one block and linearly scans inside it.

Sparse rather than dense, because an entry per block keeps the index
small enough to hold in RAM. A dense index would double the file size in
the worst case.

### The footer

```text
| bloom_offset: u64 | bloom_length: u64 | index_offset: u64 | index_length: u64 | magic: u64 | version: u32 | reserved: u32 |
```

The magic is `0x4C415454_49434530` ("LATTICE0" in big-endian, stored
little-endian on disk). The version is `2`. A reader that opens a
file with a wrong magic, or a future version it does not understand,
refuses to load it instead of guessing. The four reserved bytes exist
to give us room to add fields in version `3` without changing the
offset of any existing field. The `bloom_offset` and `bloom_length`
slots are zero-initialised in v1 files (which the released engine no
longer reads).

## The flush procedure

`Lattice::flush` does five things in this order:

1. Drains the memtable into a sorted vector of `(key, optional value)`
   pairs.
2. Streams those pairs into an `SSTableWriter` writing to a temporary
   file ending in `.sst.tmp`.
3. Calls `finish` on the writer, which writes the trailing block, the
   index, the footer, then `fsync`s.
4. Renames the temporary file to its final name (`000123.sst`). On a
   POSIX-style filesystem this rename is atomic. On Windows it is
   atomic with respect to the source path; either the final file is
   present or it is absent.
5. Truncates the WAL to zero, since every record it held is now durable
   in the new SSTable.

Steps 4 and 5 are the durability boundary. If a crash happens between
them, the file is on disk but the WAL still has its records. Replay on
the next `open` re-applies them on top of a fresh memtable. The keys end
up in the SSTable plus the memtable, but the read path consults the
memtable first, so the answer is the same.

If a crash happens between steps 3 and 4, the partial `.sst.tmp` is
visible on disk but the SSTable list does not include it. Phase 4
(chapter 5) introduces an explicit cleanup pass on `open` that
removes any leftover `*.sst.tmp` and any `*.sst` whose sequence
number is absent from the manifest. The released code does this; the
"future" qualifier is gone.

## The mixed read path

`Lattice::get(key)` consults sources in newest-first order:

1. The memtable. Three answers possible:
   - `Found(value)` — return.
   - `Tombstoned` — return `None` without reading SSTables. This is the
     reason `MemTable::lookup` returns `Lookup`, not `Option<&[u8]>`.
   - `Absent` — fall through.
2. SSTables, iterated newest to oldest. Each gives the same three
   answers. First non-`Absent` wins.

`Lattice::scan` does a one-pass merge into a `BTreeMap` accumulator. The
memtable goes in first (newest source, including tombstones). Then every
SSTable in newest-to-oldest order, with `entry.or_insert` so that
already-seen keys are not overwritten. Tombstones survive in the
accumulator until the final pass strips them.

This is `O(total entries)` per scan in Phase 2, which is fine for a few
SSTables. Phase 4's tiered compaction keeps the count of live SSTables
bounded so this stays cheap. Phase 5 introduces a streaming iterator
that does not allocate the whole accumulator up front.

## Trade-offs

Every flush writes a new SSTable, and an overwritten or deleted key
lives once per SSTable that ever held it. This is **write amplification**.
Tiered compaction in Phase 4 merges old SSTables into one, dropping
duplicates and tombstones, and is what keeps space and read amplification
in check. For now, multi-flush workloads have N SSTables and the read
path checks them all on a miss. Phase 3 mitigates that with bloom
filters.
