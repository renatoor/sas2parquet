//! Parallel conversion to Parquet.

use crate::error::Result;
use crate::reader::SasReader;
use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use std::io::Write;

/// Decodes batches in parallel and writes them in file order.
///
/// Batches are decoded a window at a time (a few per thread), so memory stays bounded
/// by the window size rather than the file size, while every core stays busy decoding.
/// Parquet encoding and compression happen in `ArrowWriter` on the calling thread.
pub fn write_parquet<W: Write + Send>(
    reader: &SasReader<'_>,
    out: W,
    properties: WriterProperties,
) -> Result<usize> {
    let mut writer = ArrowWriter::try_new(out, reader.schema(), Some(properties))?;
    let window = rayon::current_num_threads() * 2;
    let total = reader.num_batches();
    let mut rows = 0;

    let mut start = 0;
    while start < total {
        let end = (start + window).min(total);
        let batches: Vec<RecordBatch> = (start..end)
            .into_par_iter()
            .map(|i| reader.read_batch(i))
            .collect::<Result<_>>()?;
        for batch in &batches {
            rows += batch.num_rows();
            writer.write(batch)?;
        }
        start = end;
    }

    writer.close()?;
    Ok(rows)
}
