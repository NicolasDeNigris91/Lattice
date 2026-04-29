//! Persistent manifest tracking the live set of `SSTable`s.
//!
//! INVARIANT: a `Manifest` saved via [`Manifest::save`] is durable on
//! disk before the call returns, and the rename from the temp path is
//! atomic with respect to crashes. Any `SSTable` file in the directory
//! whose sequence number is not listed in the loaded manifest is an
//! orphan from a crash mid-compaction and is safe to delete.

use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

use bincode::config::Configuration;

use crate::error::{Error, Result};

const BINCODE_CONFIG: Configuration = bincode::config::standard();
const MANIFEST_FILE: &str = "MANIFEST";
const MANIFEST_TMP: &str = "MANIFEST.tmp";

/// On-disk format version produced by the current code. v1 (a flat
/// `table_seqs: Vec<u64>`) is still readable; `load` upgrades it to
/// v2 in memory by placing every sequence in `levels[0]`.
pub(crate) const MANIFEST_VERSION: u32 = 2;

/// On-disk record of the engine's table set, partitioned by LSM
/// level. Index 0 is L0 (may overlap by key range, written by the
/// memtable flush); index 1+ is L1 onward (non-overlapping within
/// the level, written by leveled compaction).
#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub(crate) struct Manifest {
    pub(crate) version: u32,
    pub(crate) next_seq: u64,
    pub(crate) levels: Vec<Vec<u64>>,
}

/// Legacy decode-only shape for manifests written by v1.0 through
/// v1.2. Bincode encodes structs by field order, so as long as this
/// matches the historical layout the decoder can recover the data.
#[derive(bincode::Decode)]
struct ManifestV1 {
    /// Always 1 on disk; kept here so bincode consumes the byte and
    /// `next_seq` lines up at the right offset. Read by the version
    /// dispatcher in `Manifest::load`.
    #[allow(dead_code)]
    version: u32,
    next_seq: u64,
    table_seqs: Vec<u64>,
}

impl ManifestV1 {
    /// Migrate a v1 manifest into the v2 in-memory shape. Every live
    /// `SSTable` lands in L0 so the leveled algorithm is forced to
    /// normalise them on its first compaction tick. This is safer
    /// than guessing they were already non-overlapping (true for the
    /// merge-everything-to-one path of v1 in practice, but not part
    /// of the v1 contract).
    fn into_v2(self) -> Manifest {
        Manifest {
            version: MANIFEST_VERSION,
            next_seq: self.next_seq,
            levels: vec![self.table_seqs],
        }
    }
}

/// Peek at the version byte before deciding which struct to decode.
#[derive(bincode::Decode)]
struct VersionPeek {
    version: u32,
}

impl Manifest {
    pub(crate) fn manifest_path(dir: &Path) -> PathBuf {
        dir.join(MANIFEST_FILE)
    }

    /// Read the manifest from `dir`. Returns `Ok(None)` if no manifest
    /// exists yet, distinct from a malformed manifest which is an error.
    /// Manifests written by v1 (`version = 1`) are upgraded in memory
    /// to the v2 shape; the on-disk file is rewritten the next time the
    /// engine calls `save`.
    pub(crate) fn load(dir: &Path) -> Result<Option<Self>> {
        let path = Self::manifest_path(dir);
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(Error::Io(err)),
        };
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let (peek, _) = bincode::decode_from_slice::<VersionPeek, _>(&buf, BINCODE_CONFIG)?;
        match peek.version {
            1 => {
                let (v1, _) = bincode::decode_from_slice::<ManifestV1, _>(&buf, BINCODE_CONFIG)?;
                Ok(Some(v1.into_v2()))
            }
            2 => {
                let (v2, _) = bincode::decode_from_slice::<Self, _>(&buf, BINCODE_CONFIG)?;
                Ok(Some(v2))
            }
            _ => Err(Error::MalformedFormat("unsupported manifest version")),
        }
    }

    /// Flatten every sequence number across every level into one
    /// vector, oldest level first, oldest sequence first inside a
    /// level. Used by the engine while M3 is partway through: callers
    /// that still treat the table set as a flat list go through this
    /// helper, the leveled-aware code paths use `levels` directly.
    pub(crate) fn flat_table_seqs(&self) -> Vec<u64> {
        self.levels.iter().flatten().copied().collect()
    }

    /// Persist the manifest atomically.
    pub(crate) fn save(&self, dir: &Path) -> Result<()> {
        let final_path = Self::manifest_path(dir);
        let tmp_path = dir.join(MANIFEST_TMP);
        let bytes = bincode::encode_to_vec(self, BINCODE_CONFIG)?;
        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            file.write_all(&bytes)?;
            file.sync_data()?;
        }
        fs::rename(&tmp_path, &final_path)?;
        crate::sync_dir(dir)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn manifest_v2_round_trip_through_disk() {
        // Pin the new on-disk shape: `levels: Vec<Vec<u64>>` with one
        // entry per LSM level. The L0 vec may have overlapping key
        // ranges; L1+ are non-overlapping. The manifest stores only
        // sequence numbers; key ranges live in the SSTable footers.
        let dir = tempdir().unwrap();
        let original = Manifest {
            version: MANIFEST_VERSION,
            next_seq: 42,
            levels: vec![vec![1, 2, 3], vec![10, 11], vec![20]],
        };
        original.save(dir.path()).unwrap();

        let loaded = Manifest::load(dir.path()).unwrap().unwrap();
        assert_eq!(loaded, original);
        assert_eq!(loaded.levels.len(), 3);
        assert_eq!(loaded.levels[0], vec![1, 2, 3]);
        assert_eq!(loaded.levels[1], vec![10, 11]);
        assert_eq!(loaded.levels[2], vec![20]);
    }

    #[test]
    fn manifest_v1_on_disk_upgrades_to_v2_with_everything_in_l0() {
        // A directory written by v1.0..v1.2 has a manifest version=1
        // with a flat `table_seqs: Vec<u64>`. M3 reads it and surfaces
        // it as v2 with every sequence in `levels[0]`. Putting them
        // in L0 forces the leveled algorithm to normalise them on its
        // first compaction tick rather than trusting the legacy layout
        // to satisfy the L1+ non-overlapping invariant.
        #[derive(bincode::Encode)]
        struct ManifestV1Shape {
            version: u32,
            next_seq: u64,
            table_seqs: Vec<u64>,
        }

        let dir = tempdir().unwrap();
        let v1 = ManifestV1Shape {
            version: 1,
            next_seq: 5,
            table_seqs: vec![1, 2, 3, 4],
        };
        let bytes = bincode::encode_to_vec(v1, BINCODE_CONFIG).unwrap();
        std::fs::write(dir.path().join(MANIFEST_FILE), bytes).unwrap();

        let loaded = Manifest::load(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.version, MANIFEST_VERSION);
        assert_eq!(loaded.next_seq, 5);
        assert_eq!(
            loaded.levels,
            vec![vec![1, 2, 3, 4]],
            "v1 table_seqs should land in levels[0] on upgrade"
        );
    }
}
