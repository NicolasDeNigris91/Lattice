//! Compaction of `SSTable`s into a single replacement.
//!
//! INVARIANT: `compact_all` produces an `SSTable` whose contents are the
//! newest-source-wins merge of the inputs. Tombstones are dropped only
//! when `drop_tombstones` is `true`, which the engine sets only when the
//! output lands at the bottom of the LSM (no older layer exists below
//! it for a tombstone to shadow). Leveled compaction keeps tombstones
//! while pushing a level toward a deeper level that may still hold the
//! pre-delete value.

use std::collections::BTreeMap;
use std::path::Path;

use tracing::{debug, info};

use crate::error::Result;
use crate::sstable::{SSTableReader, SSTableWriter};

/// Merge every entry from `readers` (in oldest-to-newest order) into a
/// single new `SSTable` written at `output`. When `drop_tombstones` is
/// `true` deletions are dropped from the output; when `false` they are
/// preserved so they can shadow older data still resident in deeper
/// levels.
pub(crate) fn compact_all(
    readers: &[&SSTableReader],
    output: &Path,
    drop_tombstones: bool,
) -> Result<usize> {
    let mut accumulator: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

    for reader in readers {
        let mut count_in = 0usize;
        for (key, value) in reader.iter_all(None)? {
            accumulator.insert(key, value);
            count_in += 1;
        }
        debug!(seq = reader.seq(), entries = count_in, "merged input table");
    }

    let entries_to_write: Vec<(Vec<u8>, Option<Vec<u8>>)> = if drop_tombstones {
        accumulator
            .into_iter()
            .filter(|(_, v)| v.is_some())
            .collect()
    } else {
        accumulator.into_iter().collect()
    };
    let live_count = entries_to_write.iter().filter(|(_, v)| v.is_some()).count();

    let mut writer = SSTableWriter::create(output, entries_to_write.len().max(1))?;
    for (key, value) in entries_to_write {
        writer.append(key, value)?;
    }
    writer.finish()?;
    info!(live = live_count, output = %output.display(), "compaction wrote new sstable");
    Ok(live_count)
}
