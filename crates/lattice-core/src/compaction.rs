//! Tiered compaction of `SSTable`s into a single replacement.
//!
//! INVARIANT: `compact_all` produces an `SSTable` whose contents are the
//! newest-source-wins merge of the inputs, with tombstones removed.
//! Tombstones are safe to drop here because the output is the bottom of
//! the LSM after replacement: there is no older layer for a tombstone to
//! shadow.

use std::collections::BTreeMap;
use std::path::Path;

use tracing::{debug, info};

use crate::error::Result;
use crate::sstable::{SSTableReader, SSTableWriter};

/// Merge every entry from `readers` (in oldest-to-newest order) into a
/// single new `SSTable` written at `output`. Tombstones are dropped.
pub(crate) fn compact_all(readers: &[&SSTableReader], output: &Path) -> Result<usize> {
    let mut accumulator: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

    for reader in readers {
        let mut count_in = 0usize;
        for (key, value) in reader.iter_all(None)? {
            accumulator.insert(key, value);
            count_in += 1;
        }
        debug!(seq = reader.seq(), entries = count_in, "merged input table");
    }

    let live_count = accumulator.values().filter(|v| v.is_some()).count();
    let mut writer = SSTableWriter::create(output, live_count.max(1))?;
    for (key, value) in accumulator {
        if let Some(value) = value {
            writer.append(key, Some(value))?;
        }
    }
    writer.finish()?;
    info!(live = live_count, output = %output.display(), "compaction wrote new sstable");
    Ok(live_count)
}
