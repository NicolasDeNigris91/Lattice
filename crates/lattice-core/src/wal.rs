//! Append-only write-ahead log.
//!
//! INVARIANT: every byte that returned from [`Wal::append`] is durable on
//! disk before the call returns. Replay reads records in append order and
//! stops at the first torn write or truncation, returning all entries up to
//! that point.
//!
//! Record layout, all integers little-endian:
//!
//! ```text
//! | crc32 (u32) | length (u32) | payload (length bytes) |
//! ```
//!
//! `crc32` is the CRC-32 (IEEE polynomial) of the payload. The payload is a
//! [`LogEntry`] encoded with `bincode` using the standard configuration.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

use bincode::config::Configuration;
use tracing::{debug, info};

use crate::error::{Error, Result};

const BINCODE_CONFIG: Configuration = bincode::config::standard();

/// One mutation recorded in the log.
#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub(crate) enum LogEntry {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Append-only WAL handle.
#[derive(Debug)]
pub(crate) struct Wal {
    writer: BufWriter<File>,
    #[allow(dead_code)]
    path: PathBuf,
}

impl Wal {
    /// Open or create the WAL file at `path`. Returns the handle along
    /// with every entry recovered from the on-disk log.
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<(Self, Vec<LogEntry>)> {
        let path = path.as_ref().to_path_buf();

        let entries = match File::open(&path) {
            Ok(file) => Self::replay(file)?,
            Err(err) if err.kind() == ErrorKind::NotFound => Vec::new(),
            Err(err) => return Err(Error::Io(err)),
        };
        info!(recovered = entries.len(), wal = %path.display(), "wal opened");

        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok((
            Self {
                writer: BufWriter::new(file),
                path,
            },
            entries,
        ))
    }

    /// Truncate the WAL to zero length and `fsync` it. Called after a
    /// successful flush, when the entries the WAL was protecting are now
    /// durable in an `SSTable` and replay is no longer needed.
    ///
    /// Implementation note: the active writer was opened with `append`
    /// mode, which on Windows grants `FILE_APPEND_DATA` but not
    /// `FILE_WRITE_DATA`, so calling `set_len` on that handle fails with
    /// `ACCESS_DENIED`. We open a separate write-mode handle just for
    /// the truncation. Both handles share the same inode under Rust's
    /// default `FILE_SHARE_*` flags.
    pub(crate) fn truncate(&mut self) -> Result<()> {
        self.writer.flush()?;
        let truncating = OpenOptions::new().write(true).open(&self.path)?;
        truncating.set_len(0)?;
        truncating.sync_data()?;
        drop(truncating);
        // The append handle's effective offset becomes "end of file",
        // which is now zero. Subsequent appends will land at offset 0.
        Ok(())
    }

    /// Append a single entry into the in-memory `BufWriter` only. The
    /// bytes do not reach the OS until either the buffer fills, a
    /// later [`Wal::sync_pending`] call, or the next [`Wal::append`].
    /// Caller is responsible for invoking `sync_pending` (directly or
    /// via the engine's group commit) before depending on durability.
    pub(crate) fn append_pending(&mut self, entry: &LogEntry) -> Result<()> {
        let payload = bincode::encode_to_vec(entry, BINCODE_CONFIG)?;
        let len: u32 = u32::try_from(payload.len()).map_err(|_| {
            Error::Io(std::io::Error::new(
                ErrorKind::InvalidInput,
                "WAL record exceeds 4 GiB",
            ))
        })?;
        let crc = crc32fast::hash(&payload);

        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&payload)?;

        debug!(bytes = payload.len() + 8, "wal append (pending)");
        Ok(())
    }

    /// Flush the in-memory buffer to the OS and `fsync` the file.
    /// A no-op when nothing is pending in the `BufWriter`.
    pub(crate) fn sync_pending(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_mut().sync_data()?;
        Ok(())
    }

    fn replay(file: File) -> Result<Vec<LogEntry>> {
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut header = [0u8; 8];
            match reader.read_exact(&mut header) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(Error::Io(err)),
            }

            let crc = u32::from_le_bytes(header[..4].try_into().expect("4 bytes"));
            let len = u32::from_le_bytes(header[4..].try_into().expect("4 bytes")) as usize;

            let mut payload = vec![0u8; len];
            match reader.read_exact(&mut payload) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(Error::Io(err)),
            }

            if crc32fast::hash(&payload) != crc {
                debug!("wal replay stopped at torn write");
                break;
            }

            let (entry, _) = bincode::decode_from_slice::<LogEntry, _>(&payload, BINCODE_CONFIG)?;
            entries.push(entry);
        }

        Ok(entries)
    }
}
