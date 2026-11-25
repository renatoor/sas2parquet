//! Parallel conversion to Parquet.
//!
//! Decoding is only half of the work: Parquet encoding and compression cost as much or
//! more, and `ArrowWriter` does them on a single thread. Here each row group is built in
//! two parallel steps, both on the rayon pool:
//!
//! 1. decode the row group's batches (independent of each other);
//! 2. encode every column chunk with its own `ArrowColumnWriter`.
//!
//! The finished chunks are then appended to the file in order. When a dataset has fewer
//! columns than there are threads, several row groups are processed at once so that
//! narrow tables still use every core.

use crate::error::Result;
use crate::reader::SasReader;
use arrow_array::RecordBatch;
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowRowGroupWriterFactory, compute_leaves,
};
use parquet::arrow::{ArrowSchemaConverter, add_encoded_arrow_schema_to_metadata};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use rayon::prelude::*;
use std::io::Write;
use std::ops::Range;
use std::sync::Arc;

/// Row groups are closed at whichever limit comes first.
const ROW_GROUP_ROWS: usize = 1 << 20;
const ROW_GROUP_BYTES: usize = 128 << 20;
/// Upper bound on row groups held in memory at once.
const MAX_ROW_GROUPS_IN_FLIGHT: usize = 8;

/// Converts the whole file and returns the number of rows written.
pub fn write_parquet<W: Write + Send>(
    reader: &SasReader<'_>,
    out: W,
    mut properties: WriterProperties,
) -> Result<usize> {
    let schema = reader.schema();
    // Store the Arrow schema (field metadata, exact types) like ArrowWriter does.
    add_encoded_arrow_schema_to_metadata(&schema, &mut properties);
    let properties = Arc::new(properties);
    let parquet_schema = ArrowSchemaConverter::new()
        .with_coerce_types(properties.coerce_types())
        .convert(&schema)?;
    let mut file =
        SerializedFileWriter::new(out, parquet_schema.root_schema_ptr(), properties.clone())?;
    let factory = ArrowRowGroupWriterFactory::try_new(&file, schema.clone())?;

    let groups = row_groups(reader);
    let columns = schema.fields().len().max(1);
    let in_flight = (rayon::current_num_threads() / columns).clamp(1, MAX_ROW_GROUPS_IN_FLIGHT);

    let mut rows = 0;
    for (window_index, window) in groups.chunks(in_flight).enumerate() {
        let first_index = window_index * in_flight;

        // Step 1: decode every batch of every row group in the window.
        let decoded: Vec<Vec<RecordBatch>> = window
            .par_iter()
            .map(|group| {
                group
                    .clone()
                    .into_par_iter()
                    .map(|i| reader.read_batch(i))
                    .collect()
            })
            .collect::<Result<_>>()?;

        // Step 2: encode every (row group, column) pair.
        let mut jobs = Vec::new();
        for (offset, batches) in decoded.iter().enumerate() {
            let writers = factory.create_column_writers(first_index + offset)?;
            jobs.extend(
                writers
                    .into_iter()
                    .enumerate()
                    .map(|(column, writer)| (batches, column, writer)),
            );
        }
        let mut chunks: Vec<ArrowColumnChunk> = jobs
            .into_par_iter()
            .map(|(batches, column, writer)| encode_column(&schema, batches, column, writer))
            .collect::<parquet::errors::Result<_>>()?;

        // Step 3: append the chunks in order, one row group at a time.
        for batches in &decoded {
            let mut row_group = file.next_row_group()?;
            for chunk in chunks.drain(..schema.fields().len()) {
                chunk.append_to_row_group(&mut row_group)?;
            }
            row_group.close()?;
            rows += batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        }
    }

    file.close()?;
    Ok(rows)
}

fn encode_column(
    schema: &arrow_schema::Schema,
    batches: &[RecordBatch],
    column: usize,
    mut writer: ArrowColumnWriter,
) -> parquet::errors::Result<ArrowColumnChunk> {
    let field = schema.field(column);
    for batch in batches {
        for leaf in compute_leaves(field, batch.column(column))? {
            writer.write(&leaf)?;
        }
    }
    writer.close()
}

/// Splits the reader's batches into row groups of about `ROW_GROUP_ROWS` rows or
/// `ROW_GROUP_BYTES` bytes of SAS row data, whichever comes first.
fn row_groups(reader: &SasReader<'_>) -> Vec<Range<usize>> {
    let row_length = reader.metadata().row_length.max(1);
    let mut groups = Vec::new();
    let (mut start, mut rows) = (0, 0);
    for i in 0..reader.num_batches() {
        rows += reader.batch_row_count(i);
        if rows >= ROW_GROUP_ROWS || rows.saturating_mul(row_length) >= ROW_GROUP_BYTES {
            groups.push(start..i + 1);
            start = i + 1;
            rows = 0;
        }
    }
    if start < reader.num_batches() {
        groups.push(start..reader.num_batches());
    }
    groups
}
