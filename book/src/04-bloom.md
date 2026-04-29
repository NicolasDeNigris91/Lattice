# Bloom filters

Phase 2 made the engine able to forget things. The cost is a slow miss.
A `get` for a key that lives in none of the SSTables walks every one of
them, paying for an index lookup, a block read, a decompression, and a
linear scan inside the block, on every single SSTable. This chapter
adds the cheapest possible "no" to the front of each SSTable read.

## The problem

A Bloom filter answers two questions:

- "Is this key definitely not in the set?"
- "Is this key possibly in the set?"

It never answers "yes, definitely". The set you ask about is the keys
written into one SSTable. If the answer is "definitely not", we skip
that SSTable entirely. The "possibly" answer is wrong with a small
configurable probability, the **false positive rate**, and on a false
positive we read the SSTable as before, no harm to correctness.

## The shape of the filter

A Bloom filter is an `m`-bit array and `k` hash functions. To insert a
key, hash it `k` times into positions `0..m`, set those bits. To check,
hash again, check those bits. A clean bit anywhere proves the key was
never inserted. All bits set means probably yes.

The math says, given `n` keys at `k` hashes in `m` bits, the false
positive probability is approximately `(1 - e^(-kn/m))^k`. The optimal
`k` for a target `m/n` ratio is `(m/n) * ln 2`. Lattice picks
`m/n = 10` bits per key, which gives `k = 7` hashes and a theoretical
false positive rate of about 0.82%.

## One digest per key

Computing seven different hash functions per key is wasteful. The
standard trick (Kirsch and Mitzenmacher, 2006) is to compute two
independent hashes `h1` and `h2`, then derive the `i`-th position as
`(h1 + i * h2) mod m`. The false positive rate of this scheme matches
the rate of seven truly independent hashes, while paying for only one
hash computation.

Lattice computes one 128-bit `xxh3` digest per key and splits it into a
high half (`h1`) and a low half (`h2`). The hot path in
[`bloom.rs`][bloom] looks like:

```rust
let (h1, h2) = double_hash(key);
for i in 0..self.num_hashes {
    let pos = h1.wrapping_add(u64::from(i).wrapping_mul(h2)) % self.num_bits;
    let word = (pos / 64) as usize;
    let bit = pos % 64;
    self.bits[word] |= 1u64 << bit;
}
```

A 64-bit-wide word array means a hot insert is one digest, seven
modulos, seven word indexes, seven bit-or operations. Modern CPUs do
this in a handful of nanoseconds.

## Where the bloom lives in an SSTable

The file format gained one section. Going from bottom to top, an
SSTable now holds:

```text
[Data Block 0..N]
[Bloom block]      <-- new
[Index block]
[Footer (48 bytes)]
```

The footer grew from 32 to 48 bytes, holding `bloom_offset` and
`bloom_length` alongside the existing `index_offset` and `index_length`.
The format version moved from 1 to 2. SSTables produced by Phase 2 do
not open in Phase 3, since the engine refuses an unknown version. This
is a deliberate pre-1.0 break.

The writer accumulates keys into the bloom as they arrive, so the bloom
is fully built at `finish` time and gets flushed to disk in one
contiguous write before the index. The reader loads the bloom into RAM
on `open`, since the whole filter is small (about 10 bits per key, so a
million-key SSTable carries roughly 1.2 MiB of bloom).

## What the read path does

`SSTableReader::get(key)` now starts with one bloom probe. If the
filter says "definitely not", the function returns `Absent` without
touching the index, the file, or the decompressor. If the bloom says
"possibly", everything proceeds as before.

Across multiple SSTables, the saving compounds. A `get` for a missing
key in a database with five SSTables used to do five index searches and
five block reads. Now, with a 1% false positive rate, the expected work
is `5 * 0.01 ≈ 0.05` block reads. Two orders of magnitude.

## Trade-offs and pitfalls

**Memory.** Every open SSTable keeps its bloom in RAM. Ten bits per key
is tiny per key but adds up. Lattice does not yet unload cold blooms or
move them off-heap. Phase 5 measures whether this matters in practice.

**Hash quality.** A bad hash makes the false positive rate balloon.
`xxh3` is fast and high quality. We do not use the default `Hasher`
trait because its API is awkward for keying with raw bytes and its
quality, while fine, is slightly worse than `xxh3` on short keys.

**Tombstones.** A tombstoned key is in the bloom too. The bloom only
tells us "the key is in this SSTable in some form". The block scan
distinguishes put from tombstone. So a bloom probe alone cannot tell
you "this key was deleted", only "we may need to look".

[bloom]: https://github.com/NicolasDeNigris91/Lattice/blob/main/crates/lattice-core/src/bloom.rs
