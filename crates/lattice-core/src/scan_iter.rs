//! Streaming scan iterator.
//!
//! `Lattice::scan` materialises every visible `(key, value)` pair
//! into a `Vec` before returning. Callers that only need to walk the
//! keyspace once (export, replication, prefix sweep) pay for the
//! whole result up front. v1.12 adds `Lattice::scan_iter`, which
//! exposes the same merge-and-dedupe logic behind an `Iterator` so
//! callers process one entry at a time and the engine only holds the
//! merge frontier (one entry per source) plus the current block of
//! whichever `SSTable` is being walked.
//!
//! ## Algorithm
//!
//! The scan composes one source iterator per LSM tier:
//!
//! - The active memtable (snapshot taken at `scan_iter` call time;
//!   the memtable is bounded by the flush threshold so the snapshot
//!   is small).
//! - The frozen memtable, if one is mid-flush.
//! - One iterator per `SSTable`, walked newest-first within each
//!   level and shallowest-first across levels.
//!
//! Each source yields its own entries in key order. A
//! [`BinaryHeap`] holds one peeked entry per source; the merge pops
//! the smallest key, and on tie the smallest source index (= newest
//! source) wins. Entries from older sources at that same key are
//! drained and discarded so the caller observes the newest write
//! per key, exactly matching the resolution rule used by `get`.
//!
//! Tombstones (`None` values) are filtered after the dedupe so a
//! deletion in a newer tier hides an older live value, as expected.
//!
//! ## Memory
//!
//! - One peeked entry per source in the heap (`O(num_sources)`).
//! - One decoded block per `SSTable` source (`O(block_size)`,
//!   default 4 KiB).
//! - The active and frozen memtables snapshotted upfront
//!   (`O(memtable_size)`, bounded by the flush threshold).
//!
//! No per-entry growth; the cost is independent of the total number
//! of keys in the database.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::Arc;

use crate::error::Result;
use crate::memtable::MemTable;
use crate::sstable::SSTableReader;

/// One entry in the merge frontier. The `source_id` is the index of
/// the source iterator inside `ScanIter::sources`; smaller
/// `source_id` means a newer LSM tier.
#[derive(Debug)]
struct HeapEntry {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    source_id: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source_id == other.source_id
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Sort by key ascending, then by source_id ascending so the
        // newest source wins on ties. Wrapped in `Reverse` at the
        // heap-push site so `BinaryHeap`'s max-heap behaviour pops
        // the smallest key first.
        self.key
            .cmp(&other.key)
            .then_with(|| self.source_id.cmp(&other.source_id))
    }
}

/// Boxed source iterator. Each LSM tier produces one of these. The
/// items are `(key, optional value)` so tombstones can flow through
/// the merge and shadow older live values before being filtered out
/// at the public-API boundary.
type SourceIter = Box<dyn Iterator<Item = Result<(Vec<u8>, Option<Vec<u8>>)>> + Send>;

/// Streaming scan iterator.
///
/// Yields visible `(key, value)` pairs in strictly increasing key
/// order, with the newest write per key winning and tombstones
/// filtered out. Tombstones from newer tiers hide older live
/// values, matching the resolution rule used by `Lattice::get`.
///
/// Created via [`crate::Lattice::scan_iter`]. The iterator owns
/// snapshots of the memtables and `Arc`s to the `SSTable` readers,
/// so it is `Send` and safe to move across threads. Errors from
/// block reads or parse failures surface as `Some(Err(...))`
/// items; callers can choose to abort or continue.
pub struct ScanIter {
    sources: Vec<SourceIter>,
    heap: BinaryHeap<Reverse<HeapEntry>>,
    prefix: Option<Vec<u8>>,
    /// Set to `Some(err)` once a source returns an error. The next
    /// `next()` yields the error and the iterator is then exhausted.
    error: Option<crate::error::Error>,
}

impl std::fmt::Debug for ScanIter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanIter")
            .field("sources", &self.sources.len())
            .field("frontier_size", &self.heap.len())
            .field("prefix_len", &self.prefix.as_ref().map(Vec::len))
            .finish_non_exhaustive()
    }
}

impl ScanIter {
    /// Build a scan iterator over the supplied tiers. The tier order
    /// is significant: index 0 is the newest tier (active memtable),
    /// later indices are older. The merge resolves ties by picking
    /// the smaller index, so callers must hand the tiers in this
    /// exact order.
    pub(crate) fn new(
        active: &MemTable,
        frozen: Option<&MemTable>,
        sstables_newest_first: Vec<Arc<SSTableReader>>,
        prefix: Option<&[u8]>,
    ) -> Self {
        let prefix_owned = prefix.map(<[u8]>::to_vec);
        let mut sources: Vec<SourceIter> = Vec::with_capacity(2 + sstables_newest_first.len());

        sources.push(Box::new(memtable_source(active, prefix)));
        if let Some(frozen) = frozen {
            sources.push(Box::new(memtable_source(frozen, prefix)));
        }
        for sst in sstables_newest_first {
            sources.push(Box::new(SsTableSource::new(sst, prefix_owned.clone())));
        }

        let mut heap = BinaryHeap::with_capacity(sources.len());
        let mut error = None;

        // Prime the heap with one entry per source.
        for (idx, source) in sources.iter_mut().enumerate() {
            match source.next() {
                Some(Ok((key, value))) => {
                    heap.push(Reverse(HeapEntry {
                        key,
                        value,
                        source_id: idx,
                    }));
                }
                Some(Err(err)) => {
                    error = Some(err);
                    break;
                }
                None => {}
            }
        }

        Self {
            sources,
            heap,
            prefix: prefix_owned,
            error,
        }
    }

    /// Refill the heap by pulling the next entry from `source_id`.
    /// Records any error in `self.error` so `next()` can surface it
    /// before exhausting the iterator.
    fn refill(&mut self, source_id: usize) {
        match self.sources[source_id].next() {
            Some(Ok((key, value))) => {
                self.heap.push(Reverse(HeapEntry {
                    key,
                    value,
                    source_id,
                }));
            }
            Some(Err(err)) if self.error.is_none() => {
                self.error = Some(err);
            }
            Some(Err(_)) | None => {}
        }
    }
}

impl Iterator for ScanIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(err) = self.error.take() {
                return Some(Err(err));
            }

            let Reverse(winner) = self.heap.pop()?;
            self.refill(winner.source_id);

            // Drain every other entry in the heap that has the same
            // key. The smallest source_id won and is held as
            // `winner`; older sources are discarded.
            while self
                .heap
                .peek()
                .is_some_and(|Reverse(top)| top.key == winner.key)
            {
                let Reverse(stale) = self.heap.pop().expect("peek said Some");
                self.refill(stale.source_id);
            }

            // Honour the prefix filter (memtable sources already
            // pre-filter; SSTable sources pre-filter inside their
            // block walk; this guards the API contract).
            if let Some(p) = &self.prefix
                && !winner.key.starts_with(p.as_slice())
            {
                continue;
            }

            // Tombstones drop out of the public iterator.
            let Some(value) = winner.value else {
                continue;
            };

            return Some(Ok((winner.key, value)));
        }
    }
}

/// Snapshot a memtable into an owned-pair iterator. The collect is
/// load-bearing: the underlying `iter_all()` borrows the memtable,
/// and the returned iterator must outlive that borrow (the caller
/// stores it in `ScanIter::sources` for the rest of the scan). The
/// snapshot allocates one `Vec` per visible entry; memtables are
/// bounded by `flush_threshold_bytes` (default 2 MiB), so the cost
/// is small.
#[allow(clippy::needless_collect)]
fn memtable_source(
    memtable: &MemTable,
    prefix: Option<&[u8]>,
) -> impl Iterator<Item = Result<(Vec<u8>, Option<Vec<u8>>)>> + Send + use<> {
    let prefix_owned = prefix.map(<[u8]>::to_vec);
    let snapshot: Vec<(Vec<u8>, Option<Vec<u8>>)> = memtable
        .iter_all()
        .filter_map(|(k, v)| {
            if let Some(p) = &prefix_owned
                && !k.starts_with(p.as_slice())
            {
                return None;
            }
            Some((k.to_vec(), v.map(<[u8]>::to_vec)))
        })
        .collect();
    snapshot.into_iter().map(Ok)
}

/// Lazy block-by-block iterator over an `SSTable`. Holds an `Arc` to
/// the reader so the file handle stays open for the iterator's
/// lifetime. Reads at most one block ahead of the caller.
struct SsTableSource {
    reader: Arc<SSTableReader>,
    next_block: usize,
    block_entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pos: usize,
    prefix: Option<Vec<u8>>,
    finished: bool,
}

impl SsTableSource {
    const fn new(reader: Arc<SSTableReader>, prefix: Option<Vec<u8>>) -> Self {
        Self {
            reader,
            next_block: 0,
            block_entries: Vec::new(),
            pos: 0,
            prefix,
            finished: false,
        }
    }
}

impl Iterator for SsTableSource {
    type Item = Result<(Vec<u8>, Option<Vec<u8>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.finished {
                return None;
            }

            // Yield from the buffered block if it has anything left.
            while self.pos < self.block_entries.len() {
                let entry = self.block_entries[self.pos].clone();
                self.pos += 1;
                if let Some(p) = &self.prefix
                    && !entry.0.starts_with(p.as_slice())
                {
                    continue;
                }
                return Some(Ok(entry));
            }

            if self.next_block >= self.reader.block_count() {
                self.finished = true;
                return None;
            }

            // Pull the next block. Errors propagate up; once an
            // error fires the source goes silent.
            match self.reader.block_entries_at(self.next_block) {
                Ok(entries) => {
                    self.block_entries = entries;
                    self.pos = 0;
                    self.next_block += 1;
                }
                Err(err) => {
                    self.finished = true;
                    return Some(Err(err));
                }
            }
        }
    }
}
