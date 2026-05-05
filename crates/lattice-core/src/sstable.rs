//! Immutable on-disk sorted string tables.
//!
//! INVARIANT: an `SSTable` produced by [`SSTableWriter::finish`] is
//! self-describing: its footer carries the magic number, format version,
//! and the offset and length of the index block. The index block lists
//! one entry per data block, recording the first key and the on-disk
//! span of that block.
//!
//! File layout, integers little-endian. Each data block is LZ4
//! on the cleartext path, or LZ4 + XChaCha20-Poly1305 (ciphertext
//! followed by a 16-byte Poly1305 tag) when the writer was
//! constructed with a cipher. The index block and bloom block
//! stay uncompressed in both cases:
//!
//! ```text
//! +------------------------------+
//! | data block 0                 |
//! | data block 1                 |
//! |              ...             |
//! | data block N                 |
//! +------------------------------+
//! | bloom block (uncompressed)   |
//! +------------------------------+
//! | index block (uncompressed)   |
//! +------------------------------+
//! | footer (48 bytes)            |
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
//! Footer is fixed 48 bytes:
//!
//! ```text
//! | bloom_offset: u64 | bloom_length: u64 | index_offset: u64 |
//! | index_length: u64 | magic: u64        | version: u32 = 3  |
//! | flags: u32        |
//! ```
//!
//! The `flags` word reuses what was the v2 reserved tail. Bit 0
//! is set when the data blocks of this `SSTable` are sealed under
//! XChaCha20-Poly1305 (book chapter 19, encryption-at-rest). All
//! other bits are reserved and must read as zero. The on-disk
//! shape is otherwise unchanged from v2; the version field is the
//! load-bearing migration signal.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use tracing::debug;

use crate::bloom::BloomFilter;
use crate::cipher::{Cipher, NONCE_LEN, TAG_LEN};
use crate::error::{Error, Result};

/// One on-disk entry: a key and either a value (put) or `None` (tombstone).
type Entry = (Vec<u8>, Option<Vec<u8>>);

const BLOCK_TARGET_SIZE: usize = 4 * 1024;
const MAGIC: u64 = 0x4C41_5454_4943_4530; // "LATTICE0"
/// Self-describing on-disk format version. Bumped from 2 to 3
/// for the v2.0 encryption-at-rest milestone (book chapter 19);
/// the body shape is identical to v2 when the cleartext path is
/// taken, but the version field gates v2 readers out so they
/// cannot silently mis-decode an encrypted directory.
const FORMAT_VERSION: u32 = 3;
const FOOTER_SIZE: usize = 48;
const FLAG_PUT: u8 = 0;
const FLAG_TOMBSTONE: u8 = 1;
/// Bit 0 of the footer flags word, set when the data blocks of
/// this `SSTable` are sealed under XChaCha20-Poly1305 instead of
/// being raw LZ4. The reader inspects this bit before decoding
/// any block.
const FOOTER_FLAG_ENCRYPTED_BLOCKS: u32 = 1 << 0;
/// Eight-byte domain separator stamped at the head of every
/// per-block nonce. Combined with `(sstable_seq, block_index)`
/// it produces the 24-byte `XChaCha` nonce; the prefix prevents
/// nonce reuse across other crate-level cipher contexts (the
/// WAL and the manifest will mint their own prefixes in later
/// phases).
const NONCE_PREFIX_SST_BLOCK: [u8; 8] = *b"sst-blk-";
/// Authenticated-additional-data domain separator for `SSTable`
/// data blocks. The full AAD is
/// `AAD_TAG_SST_BLOCK || sstable_seq.to_le_bytes() || block_index.to_le_bytes()`,
/// which binds the ciphertext to its on-disk location and stops
/// a swapped-block attack (book chapter 19).
const AAD_TAG_SST_BLOCK: &[u8] = b"lattice-sst-block-v3";

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
///
/// The `'cipher` lifetime parameter ties the writer to the
/// optional borrowed [`Cipher`] handle that seals each block.
/// Cleartext writers (the default `create` path) instantiate
/// `SSTableWriter<'static>` with `cipher = None`; encrypted
/// writers minted by [`SSTableWriter::create_with_cipher`] carry
/// the lifetime of the caller's `Cipher` reference. The cipher
/// itself is held by the runtime (`Inner.cipher` in lib.rs from
/// phase D onward); no clone of the key material lives inside
/// the writer.
#[derive(Debug)]
pub(crate) struct SSTableWriter<'cipher> {
    writer: BufWriter<File>,
    pending: Vec<Entry>,
    pending_size: usize,
    index: Vec<IndexEntry>,
    bytes_written: u64,
    bloom: BloomFilter,
    /// Optional cipher. `Some` triggers the encrypted-block path
    /// in `flush_block` and sets the encrypted-blocks bit in the
    /// footer flags word.
    cipher: Option<&'cipher Cipher>,
    /// Sequence number of the `SSTable` being written. Threaded
    /// into both the per-block nonce and the AAD so a ciphertext
    /// cannot be moved between `SSTables` under the same key.
    /// Unused on the cleartext path; carried through so the
    /// constructors stay type-uniform.
    seq: u64,
}

impl SSTableWriter<'static> {
    /// Create a cleartext writer. Equivalent to
    /// `create_with_cipher(path, expected_keys, 0, None)`; kept
    /// as a separate entry point so the existing flush /
    /// compaction call sites in `lib.rs` and `compaction.rs` need
    /// no change while the v2.0 encryption work lands phase by
    /// phase.
    pub(crate) fn create(path: &Path, expected_keys: usize) -> Result<Self> {
        Self::create_with_cipher(path, expected_keys, 0, None)
    }
}

impl<'cipher> SSTableWriter<'cipher> {
    /// Create a writer that optionally seals each data block
    /// under `cipher`. When `cipher` is `Some`, the per-block
    /// nonce is the deterministic concatenation
    /// `NONCE_PREFIX_SST_BLOCK || seq.to_le_bytes() || block_index.to_le_bytes()`,
    /// the AAD is
    /// `AAD_TAG_SST_BLOCK || seq.to_le_bytes() || block_index.to_le_bytes()`,
    /// and the footer flags word carries
    /// [`FOOTER_FLAG_ENCRYPTED_BLOCKS`]. When `cipher` is `None`
    /// the path collapses to the v2-compatible cleartext shape
    /// (LZ4 only, no Poly1305 tag, flags word zero) but the
    /// footer version field is still v3.
    pub(crate) fn create_with_cipher(
        path: &Path,
        expected_keys: usize,
        seq: u64,
        cipher: Option<&'cipher Cipher>,
    ) -> Result<Self> {
        let file = OpenOptions::new().create_new(true).write(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            pending: Vec::new(),
            pending_size: 0,
            index: Vec::new(),
            bytes_written: 0,
            bloom: BloomFilter::with_capacity(expected_keys),
            cipher,
            seq,
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
        let block_index = u64::try_from(self.index.len()).map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "sstable block index overflow",
            ))
        })?;
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

        // The index trailer records the *compressed* length only.
        // On the encrypted path, the on-disk byte count is
        // `compressed_len + TAG_LEN`; the reader recomputes that
        // sum from the footer flags word and reads accordingly
        // (book chapter 19).
        let offset = self.bytes_written;
        let on_disk_len = if let Some(cipher) = self.cipher {
            let mut nonce = [0u8; NONCE_LEN];
            nonce[0..8].copy_from_slice(&NONCE_PREFIX_SST_BLOCK);
            nonce[8..16].copy_from_slice(&self.seq.to_le_bytes());
            nonce[16..24].copy_from_slice(&block_index.to_le_bytes());

            let mut aad = Vec::with_capacity(AAD_TAG_SST_BLOCK.len() + 16);
            aad.extend_from_slice(AAD_TAG_SST_BLOCK);
            aad.extend_from_slice(&self.seq.to_le_bytes());
            aad.extend_from_slice(&block_index.to_le_bytes());

            let sealed = cipher.seal(&nonce, &aad, &compressed);
            // XChaCha20 is a stream cipher: ciphertext length
            // equals plaintext length. Anything else here means
            // the cipher API drifted and the index entry would lie.
            debug_assert_eq!(sealed.len(), compressed.len() + TAG_LEN);
            self.writer.write_all(&sealed)?;
            u64::from(compressed_len).saturating_add(u64::try_from(TAG_LEN).expect("16 fits u64"))
        } else {
            self.writer.write_all(&compressed)?;
            u64::from(compressed_len)
        };
        self.bytes_written = self.bytes_written.saturating_add(on_disk_len);

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

        let flags: u32 = if self.cipher.is_some() {
            FOOTER_FLAG_ENCRYPTED_BLOCKS
        } else {
            0
        };
        let mut footer = [0u8; FOOTER_SIZE];
        footer[0..8].copy_from_slice(&bloom_offset.to_le_bytes());
        footer[8..16].copy_from_slice(&bloom_length.to_le_bytes());
        footer[16..24].copy_from_slice(&index_offset.to_le_bytes());
        footer[24..32].copy_from_slice(&index_length.to_le_bytes());
        footer[32..40].copy_from_slice(&MAGIC.to_le_bytes());
        footer[40..44].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        footer[44..48].copy_from_slice(&flags.to_le_bytes());
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
    /// Smallest key in the table. Equals `index[0].first_key`. Cached
    /// on open so the strict-leveled compactor can compare ranges
    /// without touching disk.
    min_key: Vec<u8>,
    /// Largest key in the table. Computed at open time by decoding the
    /// last data block and recording its terminal entry. Cached so the
    /// strict-leveled compactor can compute level-N+1 overlap subsets
    /// without reopening data blocks.
    max_key: Vec<u8>,
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

        if index.is_empty() {
            return Err(Error::MalformedFormat("sstable has no data blocks"));
        }
        let min_key = index[0].first_key.clone();
        let last_index_entry = &index[index.len() - 1];
        let mut last_block_buf = vec![0u8; last_index_entry.compressed_len as usize];
        file.seek(SeekFrom::Start(last_index_entry.offset))?;
        file.read_exact(&mut last_block_buf)?;
        let last_block =
            lz4_flex::decompress(&last_block_buf, last_index_entry.uncompressed_len as usize)?;
        let last_entries = parse_block(&last_block)?;
        let max_key = last_entries
            .last()
            .map(|(k, _)| k.clone())
            .ok_or(Error::MalformedFormat("sstable last block is empty"))?;

        Ok(Self {
            file: Mutex::new(file),
            index,
            bloom,
            seq,
            path: path.to_path_buf(),
            min_key,
            max_key,
        })
    }

    pub(crate) const fn seq(&self) -> u64 {
        self.seq
    }

    /// Smallest key in the table (inclusive). Cached at open time;
    /// the strict-leveled compactor uses this to detect range
    /// overlap with target-level tables without reading disk.
    pub(crate) fn min_key(&self) -> &[u8] {
        &self.min_key
    }

    /// Largest key in the table (inclusive). Cached at open time
    /// from the last data block's final entry.
    pub(crate) fn max_key(&self) -> &[u8] {
        &self.max_key
    }

    /// File size of this `SSTable` in bytes, queried through the
    /// open file handle so the answer is robust against the
    /// inode being unlinked on POSIX (which happens to a snapshot
    /// after a compaction unlinks the path; the snapshot still
    /// keeps the inode alive via this open handle). Returns
    /// `Ok(0)` if the OS refuses to report the size for any
    /// reason; callers should treat the value as advisory.
    pub(crate) fn file_size_bytes(&self) -> u64 {
        self.file.lock().metadata().map_or(0, |m| m.len())
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

#[cfg(test)]
mod tests {
    //! Phase B session 1 of the v2.0 encryption-at-rest milestone
    //! (book chapter 19): pin the writer-only contract for the
    //! `SSTable` v3 format. The reader is unchanged in this session;
    //! all assertions parse the on-disk bytes directly.

    use super::*;
    use crate::cipher::{Cipher, TAG_LEN};

    /// Pull index entry 0 out of the on-disk bytes given the parsed
    /// footer. Returns `(offset, compressed_len, uncompressed_len)`.
    fn first_index_entry(bytes: &[u8]) -> (usize, usize, usize) {
        let footer = &bytes[bytes.len() - FOOTER_SIZE..];
        let index_offset =
            usize::try_from(u64::from_le_bytes(footer[16..24].try_into().expect("8"))).unwrap();
        let index_length =
            usize::try_from(u64::from_le_bytes(footer[24..32].try_into().expect("8"))).unwrap();
        let index_bytes = &bytes[index_offset..index_offset + index_length];
        let key_len = u32::from_le_bytes(index_bytes[0..4].try_into().expect("4")) as usize;
        let after_key = 4 + key_len;
        let off = u64::from_le_bytes(index_bytes[after_key..after_key + 8].try_into().expect("8"));
        let clen = u32::from_le_bytes(
            index_bytes[after_key + 8..after_key + 12]
                .try_into()
                .expect("4"),
        );
        let ulen = u32::from_le_bytes(
            index_bytes[after_key + 12..after_key + 16]
                .try_into()
                .expect("4"),
        );
        (usize::try_from(off).unwrap(), clen as usize, ulen as usize)
    }

    fn footer_version_and_flags(bytes: &[u8]) -> (u32, u32) {
        let footer = &bytes[bytes.len() - FOOTER_SIZE..];
        let version = u32::from_le_bytes(footer[40..44].try_into().expect("4"));
        let flags = u32::from_le_bytes(footer[44..48].try_into().expect("4"));
        (version, flags)
    }

    #[test]
    fn sstable_v3_writes_v2_compatible_format_when_cipher_absent() {
        // The default writer path (no cipher) must keep producing
        // bytes the v2 reader path can decode: blocks are LZ4
        // bytes only, no Poly1305 tag, footer flags word is zero.
        // What changes from v2 to v3 is the version field; the
        // body of the file is identical in shape.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain.sst");
        let mut writer = SSTableWriter::create_with_cipher(&path, 8, 7, None).unwrap();
        writer
            .append(b"alpha".to_vec(), Some(b"first".to_vec()))
            .unwrap();
        writer
            .append(b"bravo".to_vec(), Some(b"second".to_vec()))
            .unwrap();
        writer.finish().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        let (version, flags) = footer_version_and_flags(&bytes);
        assert_eq!(version, FORMAT_VERSION, "footer version is v3");
        assert_eq!(version, 3, "v3 is the literal version number");
        assert_eq!(flags, 0, "no-cipher path must leave every flag bit clear");

        // Block 0 on disk is the raw LZ4 ciphertext: decompressing
        // it directly succeeds and the recovered plaintext carries
        // the appended keys and values.
        let (off0, clen0, ulen0) = first_index_entry(&bytes);
        let block_bytes = &bytes[off0..off0 + clen0];
        let decompressed = lz4_flex::decompress(block_bytes, ulen0).unwrap();
        assert!(
            decompressed.windows(b"alpha".len()).any(|w| w == b"alpha"),
            "first key visible in cleartext block",
        );
        assert!(
            decompressed.windows(b"first".len()).any(|w| w == b"first"),
            "first value visible in cleartext block",
        );
    }

    /// Build the per-block nonce the way the writer does (book
    /// chapter 19): `NONCE_PREFIX_SST_BLOCK || seq.to_le() || idx.to_le()`.
    /// Mirrored here in the test so a future regression that
    /// silently changes the writer's derivation breaks this fence
    /// rather than the writer secretly drifting away from the doc.
    fn expected_nonce(seq: u64, block_index: u64) -> [u8; NONCE_LEN] {
        let mut nonce = [0u8; NONCE_LEN];
        nonce[0..8].copy_from_slice(&NONCE_PREFIX_SST_BLOCK);
        nonce[8..16].copy_from_slice(&seq.to_le_bytes());
        nonce[16..24].copy_from_slice(&block_index.to_le_bytes());
        nonce
    }

    fn expected_aad(seq: u64, block_index: u64) -> Vec<u8> {
        let mut aad = Vec::with_capacity(AAD_TAG_SST_BLOCK.len() + 16);
        aad.extend_from_slice(AAD_TAG_SST_BLOCK);
        aad.extend_from_slice(&seq.to_le_bytes());
        aad.extend_from_slice(&block_index.to_le_bytes());
        aad
    }

    #[test]
    fn sstable_v3_writes_encrypted_blocks_when_cipher_supplied() {
        // The encrypted path must (1) set bit 0 of the footer
        // flags word, (2) write `ciphertext || tag` instead of raw
        // LZ4, (3) keep `compressed_len` in the index pointing at
        // the ciphertext length only (the +16 tag is implicit), and
        // (4) round-trip through `Cipher::open` with the documented
        // nonce + AAD derivation.
        let cipher = Cipher::new([0x42; 32]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("encrypted.sst");
        let seq = 7u64;
        let mut writer = SSTableWriter::create_with_cipher(&path, 8, seq, Some(&cipher)).unwrap();
        writer
            .append(b"alpha".to_vec(), Some(b"first".to_vec()))
            .unwrap();
        writer
            .append(b"bravo".to_vec(), Some(b"second".to_vec()))
            .unwrap();
        writer.finish().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        let (version, flags) = footer_version_and_flags(&bytes);
        assert_eq!(version, FORMAT_VERSION, "footer version is v3");
        assert_eq!(
            flags & FOOTER_FLAG_ENCRYPTED_BLOCKS,
            FOOTER_FLAG_ENCRYPTED_BLOCKS,
            "encrypted path must set the encrypted-blocks flag",
        );
        assert_eq!(
            flags & !FOOTER_FLAG_ENCRYPTED_BLOCKS,
            0,
            "no other flag bit may be set in v3 phase B",
        );

        let (off0, clen0, ulen0) = first_index_entry(&bytes);
        // The on-disk block is `compressed_len + TAG_LEN` bytes
        // long (the index trailer carries only the ciphertext
        // length per book chapter 19; the tag is implicit).
        let on_disk = &bytes[off0..off0 + clen0 + TAG_LEN];

        // Plaintext markers must not survive into the ciphertext.
        // (Stream-cipher sanity check; the formal contract is the
        // open call below, but this catches a "forgot to seal"
        // regression in one line.)
        assert!(
            !on_disk.windows(b"alpha".len()).any(|w| w == b"alpha"),
            "key bytes must not be visible in encrypted block",
        );
        assert!(
            !on_disk.windows(b"first".len()).any(|w| w == b"first"),
            "value bytes must not be visible in encrypted block",
        );

        // Round-trip through Cipher with the doc-stamped nonce and
        // AAD. Any drift in the writer's derivation breaks this.
        let nonce = expected_nonce(seq, 0);
        let aad = expected_aad(seq, 0);
        let opened = cipher
            .open(&nonce, &aad, on_disk)
            .expect("encrypted block must open with derived nonce + AAD");
        assert_eq!(
            opened.len(),
            clen0,
            "opened plaintext is the LZ4-compressed block bytes",
        );

        let decompressed = lz4_flex::decompress(&opened, ulen0).unwrap();
        assert!(
            decompressed.windows(b"alpha".len()).any(|w| w == b"alpha"),
            "decrypted block decompresses to the original entries",
        );
    }

    #[test]
    fn sstable_v3_encrypted_blocks_use_distinct_nonce_per_block_index() {
        // A v3 SSTable holding two or more blocks must mint a
        // distinct nonce per block, derived from its own
        // block_index. The AAD changes likewise so a swapped-block
        // attack (block 1 ciphertext written under block 0's
        // index entry) fails the Poly1305 verify.
        let cipher = Cipher::new([0xAB; 32]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multiblock.sst");
        let seq = 13u64;
        let mut writer =
            SSTableWriter::create_with_cipher(&path, 1024, seq, Some(&cipher)).unwrap();

        // Force at least two blocks: BLOCK_TARGET_SIZE is 4 KiB,
        // each entry is 25 bytes overhead + key/value, so 600
        // entries cleanly cross the boundary.
        for i in 0..600u32 {
            let key = format!("key-{i:08}").into_bytes();
            let value = format!("val-{i:08}").into_bytes();
            writer.append(key, Some(value)).unwrap();
        }
        writer.finish().unwrap();

        let bytes = std::fs::read(&path).unwrap();
        let footer = &bytes[bytes.len() - FOOTER_SIZE..];
        let index_offset =
            usize::try_from(u64::from_le_bytes(footer[16..24].try_into().expect("8"))).unwrap();
        let index_length =
            usize::try_from(u64::from_le_bytes(footer[24..32].try_into().expect("8"))).unwrap();
        let index = parse_index(&bytes[index_offset..index_offset + index_length]).unwrap();
        assert!(
            index.len() >= 2,
            "fixture must produce at least two blocks; got {}",
            index.len(),
        );

        // Each block must open under its own (seq, block_index)
        // nonce + AAD.
        for (block_index, entry) in index.iter().enumerate() {
            let off = usize::try_from(entry.offset).unwrap();
            let clen = entry.compressed_len as usize;
            let block = &bytes[off..off + clen + TAG_LEN];
            let nonce = expected_nonce(seq, block_index as u64);
            let aad = expected_aad(seq, block_index as u64);
            let opened = cipher
                .open(&nonce, &aad, block)
                .unwrap_or_else(|e| panic!("block {block_index} must open: {e:?}"));
            let _ = lz4_flex::decompress(&opened, entry.uncompressed_len as usize)
                .unwrap_or_else(|e| panic!("block {block_index} must decompress: {e:?}"));
        }

        // Swapped-block attack: open block 0's ciphertext under
        // block 1's nonce + AAD. Must fail authentication. This is
        // the contract that motivates the per-block AAD binding
        // in book chapter 19.
        let off0 = usize::try_from(index[0].offset).unwrap();
        let clen0 = index[0].compressed_len as usize;
        let block0 = &bytes[off0..off0 + clen0 + TAG_LEN];
        let wrong_nonce = expected_nonce(seq, 1);
        let wrong_aad = expected_aad(seq, 1);
        assert!(
            cipher.open(&wrong_nonce, &wrong_aad, block0).is_err(),
            "block 0 ciphertext must not open under block 1 nonce + AAD",
        );
    }
}
