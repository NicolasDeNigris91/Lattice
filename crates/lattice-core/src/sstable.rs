//! Immutable on-disk sorted string tables.
//!
//! INVARIANT: an `SSTable` produced by [`SSTableWriter::finish`] is
//! self-describing: its footer carries the magic number, format version,
//! and the offset and length of the index block. The index block lists
//! one entry per data block, recording the first key and the on-disk
//! span of that block.
//!
//! File layout, integers little-endian:
//!
//! ```text
//! +------------------------------+
//! | data block 0 (lz4 compressed)|
//! | data block 1 (lz4 compressed)|
//! |              ...             |
//! | data block N (lz4 compressed)|
//! +------------------------------+
//! | index block (uncompressed)   |
//! +------------------------------+
//! | footer (32 bytes)            |
//! +------------------------------+
//! ```
//!
//! Data block entry, repeated until block end:
//!
//! ```text
//! | flags: u8 | key_len: u32 | key | value_len: u32 | value |
//! ```
//!
//! `flags` is `0` for a put, `1` for a tombstone. Tombstones store no
//! value bytes (`value_len == 0`).
//!
//! Index entry, repeated for each data block:
//!
//! ```text
//! | key_len: u32 | first_key | offset: u64 | compressed_len: u32 | uncompressed_len: u32 |
//! ```
//!
//! Footer is fixed 32 bytes:
//!
//! ```text
//! | index_offset: u64 | index_length: u64 | magic: u64 | version: u32 | reserved: u32 |
//! ```

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use tracing::debug;

use crate::bloom::BloomFilter;
use crate::error::{Error, Result};

/// One on-disk entry: a key and either a value (put) or `None` (tombstone).
type Entry = (Vec<u8>, Option<Vec<u8>>);

const BLOCK_TARGET_SIZE: usize = 4 * 1024;
const MAGIC: u64 = 0x4C41_5454_4943_4530; // "LATTICE0"
const FORMAT_VERSION: u32 = 2;
const FOOTER_SIZE: usize = 48;
const FLAG_PUT: u8 = 0;
const FLAG_TOMBSTONE: u8 = 1;

/// Result of looking up a key in a single `SSTable`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SsLookup {
    Found(Vec<u8>),
    Tombstoned,
    Absent,
}

#[derive(Debug, Clone)]
struct IndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    compressed_len: u32,
    uncompressed_len: u32,
}

/// Build an `SSTable` by streaming `(key, optional value)` pairs in key
/// order.
#[derive(Debug)]
pub(crate) struct SSTableWriter {
    writer: BufWriter<File>,
    pending: Vec<Entry>,
    pending_size: usize,
    index: Vec<IndexEntry>,
    bytes_written: u64,
    bloom: BloomFilter,
}

impl SSTableWriter {
    pub(crate) fn create(path: &Path, expected_keys: usize) -> Result<Self> {
        let file = OpenOptions::new().create_new(true).write(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            pending: Vec::new(),
            pending_size: 0,
            index: Vec::new(),
            bytes_written: 0,
            bloom: BloomFilter::with_capacity(expected_keys),
        })
    }

    /// Append an entry. Caller must supply keys in ascending order.
    pub(crate) fn append(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        self.bloom.insert(&key);
        let entry_size = 1 + 4 + key.len() + 4 + value.as_ref().map_or(0, Vec::len);
        self.pending.push((key, value));
        self.pending_size = self.pending_size.saturating_add(entry_size);
        if self.pending_size >= BLOCK_TARGET_SIZE {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut uncompressed = Vec::with_capacity(self.pending_size);
        let first_key = self.pending[0].0.clone();
        for (key, value) in self.pending.drain(..) {
            let flags = if value.is_some() {
                FLAG_PUT
            } else {
                FLAG_TOMBSTONE
            };
            uncompressed.push(flags);
            let key_len = u32::try_from(key.len()).map_err(|_| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "sstable key too large",
                ))
            })?;
            uncompressed.extend_from_slice(&key_len.to_le_bytes());
            uncompressed.extend_from_slice(&key);
            let val_bytes: &[u8] = value.as_deref().unwrap_or_default();
            let val_len = u32::try_from(val_bytes.len()).map_err(|_| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "sstable value too large",
                ))
            })?;
            uncompressed.extend_from_slice(&val_len.to_le_bytes());
            uncompressed.extend_from_slice(val_bytes);
        }
        let compressed = lz4_flex::compress(&uncompressed);

        let offset = self.bytes_written;
        self.writer.write_all(&compressed)?;
        let compressed_len = u32::try_from(compressed.len()).map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "sstable block too large",
            ))
        })?;
        let uncompressed_len = u32::try_from(uncompressed.len()).map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "sstable block uncompressed too large",
            ))
        })?;
        self.bytes_written = self.bytes_written.saturating_add(u64::from(compressed_len));

        self.index.push(IndexEntry {
            first_key,
            offset,
            compressed_len,
            uncompressed_len,
        });

        self.pending_size = 0;
        Ok(())
    }

    /// Finish the file: flushes the trailing block, writes the bloom
    /// filter block, writes the index block, writes the footer, and
    /// `fsync`s. Consumes self.
    pub(crate) fn finish(mut self) -> Result<()> {
        self.flush_block()?;

        let bloom_bytes = self.bloom.serialize();
        let bloom_offset = self.bytes_written;
        self.writer.write_all(&bloom_bytes)?;
        let bloom_length = u64::try_from(bloom_bytes.len()).map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "sstable bloom too large",
            ))
        })?;
        self.bytes_written = self.bytes_written.saturating_add(bloom_length);

        let index_offset = self.bytes_written;
        let mut index_buf = Vec::new();
        for entry in &self.index {
            let key_len = u32::try_from(entry.first_key.len()).map_err(|_| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "sstable index key too large",
                ))
            })?;
            index_buf.extend_from_slice(&key_len.to_le_bytes());
            index_buf.extend_from_slice(&entry.first_key);
            index_buf.extend_from_slice(&entry.offset.to_le_bytes());
            index_buf.extend_from_slice(&entry.compressed_len.to_le_bytes());
            index_buf.extend_from_slice(&entry.uncompressed_len.to_le_bytes());
        }
        self.writer.write_all(&index_buf)?;
        let index_length = u64::try_from(index_buf.len()).map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "sstable index too large",
            ))
        })?;

        let mut footer = [0u8; FOOTER_SIZE];
        footer[0..8].copy_from_slice(&bloom_offset.to_le_bytes());
        footer[8..16].copy_from_slice(&bloom_length.to_le_bytes());
        footer[16..24].copy_from_slice(&index_offset.to_le_bytes());
        footer[24..32].copy_from_slice(&index_length.to_le_bytes());
        footer[32..40].copy_from_slice(&MAGIC.to_le_bytes());
        footer[40..44].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        // bytes 44..48 are reserved zeros.
        self.writer.write_all(&footer)?;
        self.writer.flush()?;
        self.writer.get_mut().sync_all()?;
        debug!(blocks = self.index.len(), "sstable finished");
        Ok(())
    }
}

/// Read-only handle to a finished `SSTable`.
#[derive(Debug)]
pub(crate) struct SSTableReader {
    file: Mutex<File>,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    seq: u64,
    path: PathBuf,
}

impl SSTableReader {
    pub(crate) fn open(path: &Path, seq: u64) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < FOOTER_SIZE as u64 {
            return Err(Error::MalformedFormat("file shorter than footer"));
        }

        file.seek(SeekFrom::Start(file_len - FOOTER_SIZE as u64))?;
        let mut footer = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer)?;
        let bloom_offset = u64::from_le_bytes(footer[0..8].try_into().expect("8"));
        let bloom_length = u64::from_le_bytes(footer[8..16].try_into().expect("8"));
        let index_offset = u64::from_le_bytes(footer[16..24].try_into().expect("8"));
        let index_length = u64::from_le_bytes(footer[24..32].try_into().expect("8"));
        let magic = u64::from_le_bytes(footer[32..40].try_into().expect("8"));
        let version = u32::from_le_bytes(footer[40..44].try_into().expect("4"));
        if magic != MAGIC {
            return Err(Error::MalformedFormat("bad magic in footer"));
        }
        if version != FORMAT_VERSION {
            return Err(Error::MalformedFormat("unsupported sstable version"));
        }

        file.seek(SeekFrom::Start(bloom_offset))?;
        let mut bloom_bytes = vec![
            0u8;
            usize::try_from(bloom_length)
                .map_err(|_| Error::MalformedFormat("bloom too large"))?
        ];
        file.read_exact(&mut bloom_bytes)?;
        let bloom = BloomFilter::deserialize(&bloom_bytes)?;

        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_bytes = vec![
            0u8;
            usize::try_from(index_length)
                .map_err(|_| Error::MalformedFormat("index too large"))?
        ];
        file.read_exact(&mut index_bytes)?;
        let index = parse_index(&index_bytes)?;

        Ok(Self {
            file: Mutex::new(file),
            index,
            bloom,
            seq,
            path: path.to_path_buf(),
        })
    }

    pub(crate) const fn seq(&self) -> u64 {
        self.seq
    }

    #[allow(dead_code)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Look up a single key.
    pub(crate) fn get(&self, key: &[u8]) -> Result<SsLookup> {
        if !self.bloom.might_contain(key) {
            return Ok(SsLookup::Absent);
        }
        let Some(entry) = self.candidate_block(key) else {
            return Ok(SsLookup::Absent);
        };
        let block = self.read_block(entry)?;
        let entries = parse_block(&block)?;
        for (k, v) in entries {
            match k.as_slice().cmp(key) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    return Ok(v.map_or(SsLookup::Tombstoned, SsLookup::Found));
                }
                std::cmp::Ordering::Greater => return Ok(SsLookup::Absent),
            }
        }
        Ok(SsLookup::Absent)
    }

    /// Number of compressed blocks in this `SSTable`. Used by the
    /// streaming scan iterator to walk blocks one at a time without
    /// materialising the whole table into memory.
    pub(crate) fn block_count(&self) -> usize {
        self.index.len()
    }

    /// Read the compressed block at `idx`, decompress it, and parse
    /// the entries in order. Used by the streaming scan iterator;
    /// production reads through `get` go through `candidate_block`
    /// and read at most one block per lookup.
    pub(crate) fn block_entries_at(&self, idx: usize) -> Result<Vec<Entry>> {
        let entry = self
            .index
            .get(idx)
            .ok_or(Error::MalformedFormat("block index out of range"))?;
        let block = self.read_block(entry)?;
        parse_block(&block)
    }

    /// Iterate every entry (including tombstones) in key order, skipping
    /// any whose key does not start with `prefix` if given. Returns
    /// owned `(key, optional value)` pairs.
    pub(crate) fn iter_all(&self, prefix: Option<&[u8]>) -> Result<Vec<Entry>> {
        let mut out = Vec::new();
        for entry in &self.index {
            let block = self.read_block(entry)?;
            let entries = parse_block(&block)?;
            for (k, v) in entries {
                if let Some(p) = prefix {
                    if !k.starts_with(p) {
                        continue;
                    }
                }
                out.push((k, v));
            }
        }
        Ok(out)
    }

    fn candidate_block(&self, key: &[u8]) -> Option<&IndexEntry> {
        let pos = self
            .index
            .partition_point(|entry| entry.first_key.as_slice() <= key);
        if pos == 0 {
            return None;
        }
        Some(&self.index[pos - 1])
    }

    fn read_block(&self, entry: &IndexEntry) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; entry.compressed_len as usize];
        {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(entry.offset))?;
            file.read_exact(&mut buf)?;
        }
        let uncompressed = lz4_flex::decompress(&buf, entry.uncompressed_len as usize)?;
        Ok(uncompressed)
    }
}

fn parse_index(bytes: &[u8]) -> Result<Vec<IndexEntry>> {
    let mut out = Vec::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        let key_len = read_u32_le(bytes, cursor)?;
        cursor += 4;
        let key_end = cursor + key_len as usize;
        if key_end > bytes.len() {
            return Err(Error::MalformedFormat("index key truncated"));
        }
        let first_key = bytes[cursor..key_end].to_vec();
        cursor = key_end;
        if cursor + 8 + 4 + 4 > bytes.len() {
            return Err(Error::MalformedFormat("index trailer truncated"));
        }
        let offset = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().expect("8"));
        cursor += 8;
        let compressed_len = read_u32_le(bytes, cursor)?;
        cursor += 4;
        let uncompressed_len = read_u32_le(bytes, cursor)?;
        cursor += 4;
        out.push(IndexEntry {
            first_key,
            offset,
            compressed_len,
            uncompressed_len,
        });
    }
    Ok(out)
}

fn parse_block(bytes: &[u8]) -> Result<Vec<Entry>> {
    let mut out = Vec::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if cursor + 1 > bytes.len() {
            return Err(Error::MalformedFormat("block flags truncated"));
        }
        let flags = bytes[cursor];
        cursor += 1;
        let key_len = read_u32_le(bytes, cursor)?;
        cursor += 4;
        let key_end = cursor + key_len as usize;
        if key_end > bytes.len() {
            return Err(Error::MalformedFormat("block key truncated"));
        }
        let key = bytes[cursor..key_end].to_vec();
        cursor = key_end;
        let value_len = read_u32_le(bytes, cursor)?;
        cursor += 4;
        let value_end = cursor + value_len as usize;
        if value_end > bytes.len() {
            return Err(Error::MalformedFormat("block value truncated"));
        }
        let value = if flags == FLAG_TOMBSTONE {
            None
        } else if flags == FLAG_PUT {
            Some(bytes[cursor..value_end].to_vec())
        } else {
            return Err(Error::MalformedFormat("unknown block entry flags"));
        };
        cursor = value_end;
        out.push((key, value));
    }
    Ok(out)
}

fn read_u32_le(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = offset + 4;
    if end > bytes.len() {
        return Err(Error::MalformedFormat("u32 truncated"));
    }
    Ok(u32::from_le_bytes(
        bytes[offset..end].try_into().expect("4 bytes"),
    ))
}
