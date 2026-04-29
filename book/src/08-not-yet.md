# What is not yet implemented

A short catalogue of features intentionally left out of v1.0, with a sentence
on why each was deferred.

- **Transactions and isolation levels.** Out of scope for a study engine. A
  separate project would be needed to do this honestly.
- **Leveled compaction.** Tiered is enough to demonstrate the principle and
  is simpler to reason about.
- **Multi-thread writes.** Single-writer simplifies invariants. A read-write
  separation is plausible later.
- **Replication.** A distributed log is a different project entirely.
- **Encryption at rest.** Cleanly composable on top of the WAL and SSTable
  formats, but adds a layer of choices that distract from the storage path.
