//! Turns the pages of a SAS7BDAT file into Arrow record batches.
//!
//! Reading happens in two phases:
//!
//! 1. A cheap sequential pass over page headers builds a *plan*: which byte ranges hold
//!    rows, in file order. Nothing is decoded or copied here.
//! 2. Batches of consecutive plan entries are decoded independently, so callers can
//!    decode many batches in parallel. Uncompressed rows are read in place from the
//!    input buffer (normally a memory-mapped file); only compressed rows are expanded,
//!    into one arena per batch.

use crate::decompress;
use crate::encoding::TextDecoder;
use crate::error::{Error, Result};
use crate::header::Header;
use crate::metadata::{self, Column, Compression, Metadata, ValueType, is_row_subheader};
use crate::page::{Page, PageType, SubheaderPointer};
use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, Date32Array, Float64Array, RecordBatch, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

/// Days between the SAS epoch (1960-01-01) and the Unix epoch (1970-01-01).
const SAS_EPOCH_OFFSET_DAYS: f64 = 3653.0;
const SAS_EPOCH_OFFSET_SECONDS: f64 = SAS_EPOCH_OFFSET_DAYS * 86_400.0;

/// Batches are also cut by size, so wide rows still give the writer several batches
/// per row group to decode in parallel.
const MAX_BATCH_BYTES: usize = 16 << 20;

#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// Overrides the encoding declared in the file (a WHATWG label such as "windows-1252").
    pub encoding: Option<String>,
    /// Target number of rows per record batch. Batches are cut at page boundaries.
    pub batch_rows: usize,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            encoding: None,
            batch_rows: 64 * 1024,
        }
    }
}

/// Where the rows of one page live.
#[derive(Debug)]
enum PageRows {
    /// `count` fixed-width rows stored back to back starting at `offset`.
    Contiguous {
        page: usize,
        offset: usize,
        count: usize,
    },
    /// One row per subheader, possibly compressed (compressed files only).
    Subheaders {
        page: usize,
        pointers: Vec<SubheaderPointer>,
    },
}

impl PageRows {
    fn len(&self) -> usize {
        match self {
            Self::Contiguous { count, .. } => *count,
            Self::Subheaders { pointers, .. } => pointers.len(),
        }
    }
}

pub struct SasReader<'a> {
    data: &'a [u8],
    header: Header,
    decoder: TextDecoder,
    metadata: Metadata,
    schema: SchemaRef,
    plan: Vec<PageRows>,
    batches: Vec<Range<usize>>,
}

impl<'a> SasReader<'a> {
    pub fn new(data: &'a [u8], options: &ReadOptions) -> Result<Self> {
        let header = Header::parse(data)?;
        let layout = header.layout;
        let decoder = TextDecoder::new(header.encoding_code, options.encoding.as_deref())?;
        let pages = (0..header.page_count).map(|i| Page::parse(layout, i, header.page(data, i)));
        let metadata = metadata::parse(layout, pages, &decoder)?;
        check_row_length(&header, &metadata)?;

        let schema = Arc::new(build_schema(&header, &decoder, &metadata));
        // Parquet (and Arrow IPC) cannot represent rows without columns, so a dataset
        // with no variables reads as empty, whatever row count it declares.
        let plan = if metadata.columns.is_empty() {
            Vec::new()
        } else {
            build_plan(data, &header, &metadata)?
        };
        let batches = group_batches(&plan, options.batch_rows.max(1), metadata.row_length);

        Ok(Self {
            data,
            header,
            decoder,
            metadata,
            schema,
            plan,
            batches,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn decoder(&self) -> &TextDecoder {
        &self.decoder
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Number of rows that will be read (the declared row count, unless the file holds fewer).
    pub fn row_count(&self) -> usize {
        self.plan.iter().map(PageRows::len).sum()
    }

    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Number of rows batch `index` will decode to.
    pub fn batch_row_count(&self, index: usize) -> usize {
        self.plan[self.batches[index].clone()]
            .iter()
            .map(PageRows::len)
            .sum()
    }

    /// Decodes batch `index`. Batches are independent, so this can run on many threads.
    pub fn read_batch(&self, index: usize) -> Result<RecordBatch> {
        let entries = &self.plan[self.batches[index].clone()];
        let row_count: usize = entries.iter().map(PageRows::len).sum();

        let arena = self.decompress_rows(entries)?;
        let rows = self.row_slices(entries, &arena, row_count);
        let columns = self
            .metadata
            .columns
            .iter()
            .map(|column| self.column(column, &rows))
            .collect();
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    /// Expands every compressed row of the batch into one contiguous buffer.
    fn decompress_rows(&self, entries: &[PageRows]) -> Result<Vec<u8>> {
        let row_length = self.metadata.row_length;
        let compressed = |p: &&SubheaderPointer| p.len < row_length;
        let count: usize = entries
            .iter()
            .map(|e| match e {
                PageRows::Subheaders { pointers, .. } => pointers.iter().filter(compressed).count(),
                PageRows::Contiguous { .. } => 0,
            })
            .sum();
        let mut arena = vec![0u8; count * row_length];
        let mut slots = arena.chunks_exact_mut(row_length);

        for entry in entries {
            let PageRows::Subheaders { page, pointers } = entry else {
                continue;
            };
            let bytes = self.header.page(self.data, *page);
            for pointer in pointers.iter().filter(compressed) {
                let input = &bytes[pointer.offset..pointer.offset + pointer.len];
                let out = slots.next().expect("arena sized for every compressed row");
                match self.metadata.compression {
                    Compression::Rle => decompress::rle(input, out)?,
                    Compression::Rdc => decompress::rdc(input, out)?,
                    Compression::None => {
                        unreachable!("plan has subheader rows only when compressed")
                    }
                }
            }
        }
        Ok(arena)
    }

    /// One slice per row, pointing into the input buffer or into the decompression arena.
    fn row_slices<'b>(
        &'b self,
        entries: &[PageRows],
        arena: &'b [u8],
        count: usize,
    ) -> Vec<&'b [u8]> {
        let row_length = self.metadata.row_length;
        let mut rows = Vec::with_capacity(count);
        let mut expanded = arena.chunks_exact(row_length);
        for entry in entries {
            match entry {
                PageRows::Contiguous {
                    page,
                    offset,
                    count,
                } => {
                    let bytes = self.header.page(self.data, *page);
                    rows.extend(
                        bytes[*offset..*offset + count * row_length].chunks_exact(row_length),
                    );
                }
                PageRows::Subheaders { page, pointers } => {
                    let bytes = self.header.page(self.data, *page);
                    for pointer in pointers {
                        if pointer.len >= row_length {
                            rows.push(&bytes[pointer.offset..pointer.offset + row_length]);
                        } else {
                            rows.push(expanded.next().expect("one arena slot per compressed row"));
                        }
                    }
                }
            }
        }
        rows
    }

    fn column(&self, column: &Column, rows: &[&[u8]]) -> ArrayRef {
        let range = column.offset..column.offset + column.width;
        let e = self.header.layout.endianness;
        // Metadata validation guarantees `range` lies within every row.
        let numbers = || rows.iter().map(|row| e.numeric(&row[range.clone()]));

        match column.value_type {
            // SAS encodes all missing values (., .A-.Z, ._) as NaN payloads.
            ValueType::Number => Arc::new(Float64Array::from_iter(
                numbers().map(|v| (!v.is_nan()).then_some(v)),
            )),
            ValueType::Date => Arc::new(Date32Array::from_iter(numbers().map(|v| {
                let days = (v - SAS_EPOCH_OFFSET_DAYS).floor();
                (days.is_finite() && days.abs() <= f64::from(i32::MAX)).then_some(days as i32)
            }))),
            ValueType::DateTime => {
                Arc::new(TimestampMillisecondArray::from_iter(numbers().map(|v| {
                    let ms = ((v - SAS_EPOCH_OFFSET_SECONDS) * 1000.0).round();
                    (ms.is_finite() && ms.abs() < 9.2e18).then_some(ms as i64)
                })))
            }
            ValueType::Text => {
                let mut builder =
                    StringBuilder::with_capacity(rows.len(), rows.len() * column.width.min(16));
                for row in rows {
                    // SAS has no null strings: a blank value is its "missing" value.
                    let value = self.decoder.decode_field(&row[range.clone()]);
                    if value.is_empty() {
                        builder.append_null();
                    } else {
                        builder.append_value(value);
                    }
                }
                Arc::new(builder.finish())
            }
        }
    }
}

fn build_schema(header: &Header, decoder: &TextDecoder, metadata: &Metadata) -> Schema {
    let fields: Vec<Field> = metadata
        .columns
        .iter()
        .map(|c| {
            let data_type = match c.value_type {
                ValueType::Number => DataType::Float64,
                ValueType::Date => DataType::Date32,
                ValueType::DateTime => DataType::Timestamp(TimeUnit::Millisecond, None),
                ValueType::Text => DataType::Utf8,
            };
            let mut field_metadata = HashMap::new();
            if !c.label.is_empty() {
                field_metadata.insert("sas.label".to_string(), c.label.clone());
            }
            if !c.format.is_empty() {
                field_metadata.insert("sas.format".to_string(), c.format.clone());
            }
            Field::new(&c.name, data_type, true).with_metadata(field_metadata)
        })
        .collect();

    let mut schema_metadata = HashMap::new();
    let name = metadata::decode_padded(decoder, &header.dataset_name);
    if !name.is_empty() {
        schema_metadata.insert("sas.dataset_name".to_string(), name);
    }
    schema_metadata.insert("sas.encoding".to_string(), decoder.name().to_string());
    Schema::new_with_metadata(fields, schema_metadata)
}

/// Rejects row lengths that no valid file could have, before anything is allocated from them.
fn check_row_length(header: &Header, metadata: &Metadata) -> Result<()> {
    let row_length = metadata.row_length;
    if metadata.columns.is_empty() {
        return Ok(());
    }
    let limit = match metadata.compression {
        // Uncompressed rows never span pages.
        Compression::None => header.page_len,
        // A compressed row lives in a single subheader, so it cannot expand past this.
        _ => header.page_len.saturating_mul(decompress::MAX_EXPANSION),
    };
    if row_length == 0 || row_length > limit {
        return Err(Error::Metadata(format!(
            "row length {row_length} is invalid for {} byte pages",
            header.page_len
        )));
    }
    Ok(())
}

fn build_plan(data: &[u8], header: &Header, metadata: &Metadata) -> Result<Vec<PageRows>> {
    let layout = header.layout;
    let row_length = metadata.row_length;
    let compressed = metadata.compression != Compression::None;
    let mut remaining = metadata.row_count;
    let mut plan = Vec::new();

    for index in 0..header.page_count {
        if remaining == 0 {
            break;
        }
        let page = Page::parse(layout, index, header.page(data, index))?;
        let entry = match page.page_type {
            PageType::Comp => continue,
            PageType::Meta | PageType::Amd if compressed => {
                let mut pointers = Vec::new();
                for pointer in page.pointers(layout) {
                    let pointer = pointer?;
                    if is_row_subheader(layout, &pointer, page.subheader(&pointer))? {
                        if pointer.len < row_length
                            && row_length > pointer.len.saturating_mul(decompress::MAX_EXPANSION)
                        {
                            return Err(Error::Decompress(
                                "row is too small to expand to the declared row length",
                            ));
                        }
                        pointers.push(pointer);
                    }
                }
                pointers.truncate(remaining);
                if pointers.is_empty() {
                    continue;
                }
                PageRows::Subheaders {
                    page: index,
                    pointers,
                }
            }
            PageType::Meta | PageType::Amd => continue,
            PageType::Data | PageType::Mix => {
                let offset = page.row_data_offset(layout);
                let declared = if page.page_type == PageType::Data {
                    page.block_count
                } else {
                    let blocks = page.block_count.saturating_sub(page.subheader_count);
                    if blocks == 0 {
                        metadata.mix_page_row_count
                    } else {
                        blocks.min(metadata.mix_page_row_count)
                    }
                };
                let fits = page
                    .bytes
                    .len()
                    .saturating_sub(offset)
                    .checked_div(row_length)
                    .unwrap_or(declared);
                let count = declared.min(fits).min(remaining);
                if count == 0 {
                    continue;
                }
                PageRows::Contiguous {
                    page: index,
                    offset,
                    count,
                }
            }
        };
        remaining -= entry.len();
        plan.push(entry);
    }
    Ok(plan)
}

/// Groups consecutive pages into batches of about `target` rows (cut at page boundaries),
/// or fewer when rows are wide enough to reach `MAX_BATCH_BYTES` first.
fn group_batches(plan: &[PageRows], target: usize, row_length: usize) -> Vec<Range<usize>> {
    let mut batches = Vec::new();
    let (mut start, mut rows) = (0, 0);
    for (i, entry) in plan.iter().enumerate() {
        rows += entry.len();
        if rows >= target || rows.saturating_mul(row_length) >= MAX_BATCH_BYTES {
            batches.push(start..i + 1);
            start = i + 1;
            rows = 0;
        }
    }
    if start < plan.len() {
        batches.push(start..plan.len());
    }
    batches
}
