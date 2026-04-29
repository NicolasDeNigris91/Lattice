//! Error type for `lattice-core`.

/// Convenience alias for results returned by this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// All errors surfaced by the storage engine.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error from the OS or filesystem.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// A binary encoding error from the WAL or `SSTable` codecs.
    #[error("encode error: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    /// A binary decoding error from the WAL or `SSTable` codecs.
    #[error("decode error: {0}")]
    Decode(#[from] bincode::error::DecodeError),

    /// LZ4 decompression of an `SSTable` block failed.
    #[error("decompress error: {0}")]
    Decompress(#[from] lz4_flex::block::DecompressError),

    /// An `SSTable` file is missing required structure (footer, magic,
    /// supported format version, well-formed index).
    #[error("malformed sstable: {0}")]
    MalformedSstable(&'static str),
}
