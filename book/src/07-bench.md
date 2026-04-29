# Benchmarks

Numbers in this chapter come from `cargo bench -p lattice-core`,
backed by Criterion at
[`benches/put_get.rs`][bench]. They are not a comparison against
RocksDB or sled; they are a sanity check that each layer of the engine
is doing what it set out to do.

## The machine

- Windows 10 x64, MSVC toolchain
- SATA SSD
- Rust 1.95 stable
- One run per measurement, default Criterion sampling reduced to keep
  this chapter honest about the wall-clock cost of the bench itself.

The numbers below are the median of `(low, mid, high)` Criterion
estimates. Take them with a generous error bar.

## What was measured

Four micro-benchmarks, each on a database holding ten thousand keys
of eight bytes plus an eleven-byte fixed value:

| Benchmark               | What it does                                            |
|-------------------------|---------------------------------------------------------|
| `sequential_write_10k`  | 10 000 sequential `put`s on a fresh database, no flush  |
| `random_read_hits_10k`  | 10 000 random `get`s, all keys present (post-flush)     |
| `random_read_misses_10k`| 10 000 random `get`s, all keys absent (post-flush)      |
| `scan_all_10k`          | One `scan(None)` on the populated database              |

## What we saw

| Benchmark                | Total time | Per operation |
|--------------------------|------------|---------------|
| `sequential_write_10k`   | 9.94 s     | 994 µs        |
| `random_read_hits_10k`   | 147.3 ms   | 14.7 µs       |
| `random_read_misses_10k` | 342.7 µs   | 34 ns         |
| `scan_all_10k`           | 2.24 ms    | 224 ns/entry  |

## What that means

**`sequential_write_10k` at 994 µs per write.** This is dominated by
`fsync` per put. The WAL is doing exactly what its contract says: a
single write that survives a power loss costs roughly one disk
synchronisation. A real-world workload with group commit (Phase 5+)
would amortise this; Phase 1 chose the simpler durability story and is
paying for it transparently.

**`random_read_hits_10k` at 14.7 µs per hit.** After the flush, the
memtable is empty and every probe lands on the SSTable. The cost is
a bloom probe, an index binary search, one compressed-block read, an
LZ4 decompress, and a linear scan inside the block. Fourteen
microseconds for that whole pipeline says the block size and bloom
sizing are reasonable.

**`random_read_misses_10k` at 34 ns per miss.** This is the bloom
filter's headline number. A miss does not touch the index, the file,
or the decompressor. The read path computes one `xxh3` digest, probes
seven bits, and returns `Absent`. Two orders of magnitude faster than
a hit. A workload where most queries miss (cache-style reads, lookup
by primary key on a long-tailed distribution) gets this for free.

**`scan_all_10k` at 224 ns per entry.** `scan` reads every block,
decompresses each, walks every entry, and pushes pairs into the
result vector. The number is small because the data is contiguous on
disk and LZ4 decompresses gigabytes per second per core. The
allocator and the merge accumulator are most of the time here.

## What this does not measure

- Heavy concurrency. Lattice is single-writer in v1.0.
- Large values. Eleven bytes per value is the easy case for both
  compression and block layout.
- Workloads that exercise compaction. The bench creates the data once
  and reads against the post-flush state.
- Crash recovery. Replay performance is observable but not benched
  here; it depends mostly on WAL size, which Phase 4 keeps short.

A future version of this chapter compares numbers against `sled` and
`fjall` on identical hardware. For v1.0 the goal is just to show that
each component pays for itself.

[bench]: https://github.com/NicolasDeNigris91/Lattice/blob/main/benches/put_get.rs
