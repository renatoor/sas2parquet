//! Dataset metadata (row layout, columns, labels, formats) from the metadata subheaders.

use crate::encoding::TextDecoder;
use crate::error::{Error, Result};
use crate::header::Layout;
use crate::page::{
    Page, PageType, SUBHEADER_COMPRESSED, SUBHEADER_TYPE_ROW, SUBHEADER_UNCOMPRESSED,
    SubheaderPointer,
};

const SIG_ROW_SIZE: u32 = 0xF7F7_F7F7;
const SIG_COLUMN_SIZE: u32 = 0xF6F6_F6F6;
const SIG_COLUMN_TEXT: u32 = 0xFFFF_FFFD;
const SIG_COLUMN_NAME: u32 = 0xFFFF_FFFF;
const SIG_COLUMN_ATTRS: u32 = 0xFFFF_FFFC;
const SIG_FORMAT_AND_LABEL: u32 = 0xFFFF_FBFE;

const RLE_LITERAL: &[u8] = b"SASYZCRL";
const RDC_LITERAL: &[u8] = b"SASYZCR2";

/// Formats that mark a numeric column as a date (days since 1960-01-01). Same list as pandas.
const DATE_FORMATS: &[&str] = &[
    "DATE", "DAY", "DDMMYY", "DOWNAME", "JULDAY", "JULIAN", "MMDDYY", "MMYY", "MMYYC", "MMYYD",
    "MMYYP", "MMYYS", "MMYYN", "MONNAME", "MONTH", "MONYY", "QTR", "QTRR", "NENGO", "WEEKDATE",
    "WEEKDATX", "WEEKDAY", "WEEKV", "WORDDATE", "WORDDATX", "YEAR", "YYMM", "YYMMC", "YYMMD",
    "YYMMP", "YYMMS", "YYMMN", "YYMON", "YYMMDD", "YYQ", "YYQC", "YYQD", "YYQP", "YYQS", "YYQN",
    "YYQR", "YYQRC", "YYQRD", "YYQRP", "YYQRS", "YYQRN", "YYMMDDP", "YYMMDDC", "E8601DA",
    "YYMMDDN", "MMDDYYC", "MMDDYYS", "MMDDYYD", "YYMMDDS", "B8601DA", "DDMMYYN", "YYMMDDD",
    "DDMMYYB", "DDMMYYP", "MMDDYYP", "YYMMDDB", "MMDDYYN", "DDMMYYC", "DDMMYYD", "DDMMYYS",
    "MINGUO",
];

/// Formats that mark a numeric column as a datetime (seconds since 1960-01-01).
const DATETIME_FORMATS: &[&str] = &[
    "DATETIME", "DTWKDATX", "B8601DN", "B8601DT", "B8601DX", "B8601DZ", "B8601LX", "E8601DN",
    "E8601DT", "E8601DX", "E8601DZ", "E8601LX", "DATEAMPM", "DTDATE", "DTMONYY", "DTYEAR", "TOD",
    "MDYAMPM",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Rle,
    Rdc,
}

/// Logical type of a column after applying its SAS format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Number,
    Date,
    DateTime,
    Text,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub label: String,
    pub format: String,
    /// Byte offset of the value inside a row.
    pub offset: usize,
    pub width: usize,
    pub value_type: ValueType,
}

#[derive(Debug)]
pub struct Metadata {
    pub row_length: usize,
    pub row_count: usize,
    /// Maximum number of rows stored on a Mix page.
    pub mix_page_row_count: usize,
    pub compression: Compression,
    pub columns: Vec<Column>,
}

const KNOWN_SIGNATURES: &[u32] = &[
    SIG_ROW_SIZE,
    SIG_COLUMN_SIZE,
    0xFFFF_FC00, // subheader counts
    SIG_COLUMN_TEXT,
    SIG_COLUMN_NAME,
    SIG_COLUMN_ATTRS,
    SIG_FORMAT_AND_LABEL,
    0xFFFF_FFFE, // column list
];

/// True when a subheader holds a row rather than metadata. This only happens in
/// compressed files: compressed rows are flagged as such, while rows that did not
/// compress well are stored raw with the "row" type and no known signature.
pub fn is_row_subheader(layout: Layout, pointer: &SubheaderPointer, sub: &[u8]) -> Result<bool> {
    if pointer.compression == SUBHEADER_COMPRESSED {
        return Ok(true);
    }
    Ok(pointer.compression == SUBHEADER_UNCOMPRESSED
        && pointer.kind == SUBHEADER_TYPE_ROW
        && !KNOWN_SIGNATURES.contains(&signature(layout, sub)?))
}

/// Reference into a column text subheader.
#[derive(Debug, Clone, Copy)]
struct TextRef {
    index: usize,
    offset: usize,
    len: usize,
}

impl TextRef {
    fn read(layout: Layout, buf: &[u8], at: usize) -> Result<Self> {
        let e = layout.endianness;
        Ok(Self {
            index: e.u16(buf, at, "text reference")?.into(),
            offset: e.u16(buf, at + 2, "text reference")?.into(),
            len: e.u16(buf, at + 4, "text reference")?.into(),
        })
    }
}

struct RowSize {
    row_length: usize,
    row_count: usize,
    mix_page_row_count: usize,
    compression: TextRef,
}

struct Attr {
    offset: usize,
    width: usize,
    numeric: bool,
}

#[derive(Default)]
struct Builder<'a> {
    texts: Vec<&'a [u8]>,
    row_size: Option<RowSize>,
    column_count: Option<usize>,
    names: Vec<TextRef>,
    attrs: Vec<Attr>,
    formats: Vec<(TextRef, TextRef)>,
}

fn signature(layout: Layout, sub: &[u8]) -> Result<u32> {
    let e = layout.endianness;
    let sig = e.u32(sub, 0, "subheader signature")?;
    // In big-endian 64-bit files the 32-bit signature sits in the low half of the word,
    // preceded by zero or sign-extension bytes.
    if layout.wide && e == crate::bytes::Endianness::Big && (sig == 0 || sig == u32::MAX) {
        return e.u32(sub, 4, "subheader signature");
    }
    Ok(sig)
}

impl<'a> Builder<'a> {
    fn subheader(&mut self, layout: Layout, sub: &'a [u8]) -> Result<()> {
        let w = layout.word_len();
        match signature(layout, sub)? {
            SIG_ROW_SIZE => {
                let tail = sub
                    .len()
                    .checked_sub(118)
                    .ok_or(Error::Truncated("row size subheader"))?;
                self.row_size = Some(RowSize {
                    row_length: layout.word(sub, 5 * w, "row length")?,
                    row_count: layout.word(sub, 6 * w, "row count")?,
                    mix_page_row_count: layout.word(sub, 15 * w, "mix page row count")?,
                    compression: TextRef::read(layout, sub, tail)?,
                });
            }
            SIG_COLUMN_SIZE => {
                self.column_count = Some(layout.word(sub, w, "column count")?);
            }
            SIG_COLUMN_TEXT => {
                self.texts
                    .push(sub.get(w..).ok_or(Error::Truncated("column text"))?);
            }
            SIG_COLUMN_NAME => {
                let count = sub.len().saturating_sub(2 * w + 12) / 8;
                for i in 0..count {
                    self.names.push(TextRef::read(layout, sub, w + 8 + i * 8)?);
                }
            }
            SIG_COLUMN_ATTRS => {
                let entry = w + 8;
                let count = sub.len().saturating_sub(2 * w + 12) / entry;
                for i in 0..count {
                    let at = entry + i * entry;
                    let kind = *sub
                        .get(at + w + 6)
                        .ok_or(Error::Truncated("column attributes"))?;
                    self.attrs.push(Attr {
                        offset: layout.word(sub, at, "column offset")?,
                        width: layout.endianness.u32(sub, at + w, "column width")? as usize,
                        numeric: match kind {
                            0x01 => true,
                            0x02 => false,
                            other => return Err(Error::ColumnType(other)),
                        },
                    });
                }
            }
            SIG_FORMAT_AND_LABEL => {
                let at = 22 + 3 * w;
                self.formats.push((
                    TextRef::read(layout, sub, at)?,
                    TextRef::read(layout, sub, at + 6)?,
                ));
            }
            // Counts, column list and undocumented subheaders carry nothing we need.
            _ => {}
        }
        Ok(())
    }

    fn text(&self, r: TextRef) -> &'a [u8] {
        if r.len == 0 || self.texts.is_empty() {
            return b"";
        }
        // Like pandas, clamp out-of-range indexes to the last text block.
        let block = self.texts[r.index.min(self.texts.len() - 1)];
        let start = r.offset.min(block.len());
        let end = (r.offset + r.len).min(block.len());
        &block[start..end]
    }

    fn finish(self, decoder: &TextDecoder) -> Result<Metadata> {
        let row_size = self
            .row_size
            .as_ref()
            .ok_or_else(|| Error::Metadata("missing row size subheader".into()))?;

        let literal = self.text(row_size.compression);
        let first_text = self.texts.first().copied().unwrap_or_default();
        let contains = |hay: &[u8], needle: &[u8]| hay.windows(needle.len()).any(|w| w == needle);
        let compression = if literal == RLE_LITERAL || contains(first_text, RLE_LITERAL) {
            Compression::Rle
        } else if literal == RDC_LITERAL || contains(first_text, RDC_LITERAL) {
            Compression::Rdc
        } else {
            Compression::None
        };

        let count = self.column_count.unwrap_or(self.attrs.len());
        if self.names.len() < count || self.attrs.len() < count {
            return Err(Error::Metadata(format!(
                "expected {count} columns, found {} names and {} attribute entries",
                self.names.len(),
                self.attrs.len()
            )));
        }

        let decode = |r: TextRef| decoder.decode_field(self.text(r)).trim().to_string();
        let mut columns = Vec::with_capacity(count);
        for i in 0..count {
            let attr = &self.attrs[i];
            let (format, label) = match self.formats.get(i) {
                Some(&(f, l)) => (decode(f), decode(l)),
                None => (String::new(), String::new()),
            };
            let value_type = if !attr.numeric {
                ValueType::Text
            } else {
                let upper = format.to_ascii_uppercase();
                if DATE_FORMATS.contains(&upper.as_str()) {
                    ValueType::Date
                } else if DATETIME_FORMATS.contains(&upper.as_str()) {
                    ValueType::DateTime
                } else {
                    ValueType::Number
                }
            };

            let fits = attr
                .offset
                .checked_add(attr.width)
                .is_some_and(|end| end <= row_size.row_length);
            let valid_width = if attr.numeric {
                (1..=8).contains(&attr.width)
            } else {
                true
            };
            if !fits || !valid_width {
                return Err(Error::Metadata(format!(
                    "column {i} (offset {}, width {}) does not fit a {} byte row",
                    attr.offset, attr.width, row_size.row_length
                )));
            }

            columns.push(Column {
                name: decode(self.names[i]),
                label,
                format,
                offset: attr.offset,
                width: attr.width,
                value_type,
            });
        }

        Ok(Metadata {
            row_length: row_size.row_length,
            row_count: row_size.row_count,
            mix_page_row_count: row_size.mix_page_row_count,
            compression,
            columns,
        })
    }
}

/// Reads metadata from the pages before the first Data page.
pub fn parse<'a>(
    layout: Layout,
    pages: impl Iterator<Item = Result<Page<'a>>>,
    decoder: &TextDecoder,
) -> Result<Metadata> {
    let mut builder = Builder::default();
    for page in pages {
        let page = page?;
        if page.page_type == PageType::Data {
            break;
        }
        if !page.page_type.has_subheaders() {
            continue;
        }
        for pointer in page.pointers(layout) {
            let pointer = pointer?;
            let sub = page.subheader(&pointer);
            if is_row_subheader(layout, &pointer, sub)? {
                continue;
            }
            builder.subheader(layout, sub)?;
        }
    }
    builder.finish(decoder)
}

/// Decodes a fixed-width, blank-padded header field such as the dataset name.
pub fn decode_padded(decoder: &TextDecoder, bytes: &[u8]) -> String {
    decoder.decode_field(bytes).trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes::Endianness;

    #[test]
    fn big_endian_wide_signatures_use_low_word() {
        let layout = Layout {
            wide: true,
            endianness: Endianness::Big,
        };
        assert_eq!(
            signature(layout, &[0, 0, 0, 0, 0xF7, 0xF7, 0xF7, 0xF7]).unwrap(),
            SIG_ROW_SIZE
        );
        let name = [0xFF; 8];
        assert_eq!(signature(layout, &name).unwrap(), SIG_COLUMN_NAME);
        let text = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFD];
        assert_eq!(signature(layout, &text).unwrap(), SIG_COLUMN_TEXT);
    }

    #[test]
    fn little_endian_signatures_use_first_four_bytes() {
        let layout = Layout {
            wide: true,
            endianness: Endianness::Little,
        };
        let text = [0xFD, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        assert_eq!(signature(layout, &text).unwrap(), SIG_COLUMN_TEXT);
    }
}
