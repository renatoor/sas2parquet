use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("not a SAS7BDAT file (magic number mismatch)")]
    BadMagic,

    #[error("unexpected end of data while reading {0}")]
    Truncated(&'static str),

    #[error("invalid byte order flag {0:#04x}")]
    ByteOrder(u8),

    #[error("unknown page type {code:#06x} on page {page}")]
    PageType { code: u16, page: usize },

    #[error("unknown column type {0:#04x}")]
    ColumnType(u8),

    #[error("unsupported character encoding: {0} (use --encoding to override)")]
    Encoding(String),

    #[error("corrupt compressed row: {0}")]
    Decompress(&'static str),

    #[error("invalid metadata: {0}")]
    Metadata(String),

    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
}

pub type Result<T> = std::result::Result<T, Error>;
