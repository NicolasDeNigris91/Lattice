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
const MANIFEST_VERSION: u32 = 1;

/// On-disk record of the engine's table set.
#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub(crate) struct Manifest {
    pub(crate) version: u32,
    pub(crate) next_seq: u64,
    /// Sequence numbers of every live `SSTable`, sorted ascending.
    pub(crate) table_seqs: Vec<u64>,
}

impl Manifest {
    pub(crate) fn manifest_path(dir: &Path) -> PathBuf {
        dir.join(MANIFEST_FILE)
    }

    /// Read the manifest from `dir`. Returns `Ok(None)` if no manifest
    /// exists yet, distinct from a malformed manifest which is an error.
    pub(crate) fn load(dir: &Path) -> Result<Option<Self>> {
        let path = Self::manifest_path(dir);
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(Error::Io(err)),
        };
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let (manifest, _consumed) = bincode::decode_from_slice::<Self, _>(&buf, BINCODE_CONFIG)?;
        if manifest.version != MANIFEST_VERSION {
            return Err(Error::MalformedFormat("unsupported manifest version"));
        }
        Ok(Some(manifest))
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
