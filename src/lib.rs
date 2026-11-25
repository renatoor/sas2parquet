//! Reader for SAS7BDAT files that produces Apache Arrow record batches, plus a
//! parallel Parquet writer on top of it.
//!
//! SAS7BDAT is an undocumented binary format. The layout handled here follows the
//! community reverse-engineering work in [readstat](https://github.com/WizardMac/ReadStat)
//! and [pandas](https://github.com/pandas-dev/pandas/blob/main/pandas/io/sas/sas7bdat.py).
//!
//! ```no_run
//! use sas2parquet::{ReadOptions, SasReader};
//!
//! let bytes = std::fs::read("data.sas7bdat")?;
//! let reader = SasReader::new(&bytes, &ReadOptions::default())?;
//! for i in 0..reader.num_batches() {
//!     let batch = reader.read_batch(i)?;
//!     println!("{} rows", batch.num_rows());
//! }
//! # Ok::<(), sas2parquet::Error>(())
//! ```

pub mod bytes;
mod decompress;
pub mod encoding;
mod error;
pub mod header;
pub mod metadata;
mod page;
mod reader;
mod writer;

pub use error::{Error, Result};
pub use reader::{ReadOptions, SasReader};
pub use writer::write_parquet;
