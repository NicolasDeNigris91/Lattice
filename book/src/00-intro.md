# Introduction

Lattice is a key-value storage engine I wrote from scratch in Rust. The goal
was never to compete with `sled`, `fjall`, or RocksDB. The goal was to
understand them by reproducing the parts that matter, in code small enough to
read in an afternoon, with prose explaining the choices.

This book walks through every component of Lattice, in the order I built
them. It is meant to be read alongside the source. Wherever a chapter
references a piece of behavior, you can find the corresponding type in
`crates/lattice-core/src/`.

## How to read

Each chapter answers four questions in roughly this order, what problem the
component solves, what naive solution would fail and why, what the actual
implementation looks like, and what the trade-offs are. When a chapter ends
with a benchmark, the numbers are real, taken from `cargo bench` on a
specific machine, with the configuration spelled out.

## What this is not

Lattice is not production grade. It is single-process, single-threaded by
default, has no transactions, and does not survive disk corruption beyond
basic checksum verification. The chapter on what is not yet implemented
catalogues the gaps, with a sentence on why each one was deferred.

## Acknowledgements

The shape of this engine owes a lot to two papers and one book, the original
LSM-tree paper from 1996 by O'Neil et al., the Bigtable paper from Google in
2006, and Alex Petrov's _Database Internals_. Where I borrowed an idea I
will say so in the chapter that uses it.
