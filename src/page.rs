//! Pages and the subheader pointer table at the start of each page.

use crate::error::{Error, Result};
use crate::header::Layout;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    /// Only subheaders (metadata, or rows when the file is compressed).
    Meta,
    /// Only uncompressed rows.
    Data,
    /// Subheaders followed by uncompressed rows.
    Mix,
    /// Metadata pages written when a dataset is amended; parsed like `Meta`.
    Amd,
    /// Compression bookkeeping; never holds rows or metadata we need.
    Comp,
}

impl PageType {
    fn from_code(code: u16, page: usize) -> Result<Self> {
        if code == 0x9000 {
            return Ok(Self::Comp);
        }
        // The high bits carry flags (e.g. 0x4000 for a second kind of meta page);
        // the page kind lives in bits 8-11.
        match code & 0x0F00 {
            0x0000 => Ok(Self::Meta),
            0x0100 => Ok(Self::Data),
            0x0200 => Ok(Self::Mix),
            0x0400 => Ok(Self::Amd),
            _ => Err(Error::PageType { code, page }),
        }
    }

    pub fn has_subheaders(self) -> bool {
        matches!(self, Self::Meta | Self::Mix | Self::Amd)
    }
}

/// How a subheader's bytes are stored, from the pointer table.
pub const SUBHEADER_UNCOMPRESSED: u8 = 0x00;
pub const SUBHEADER_TRUNCATED: u8 = 0x01;
pub const SUBHEADER_COMPRESSED: u8 = 0x04;

/// Subheader pointers with this type in a compressed file point at row data.
pub const SUBHEADER_TYPE_ROW: u8 = 0x01;

#[derive(Debug, Clone, Copy)]
pub struct SubheaderPointer {
    pub offset: usize,
    pub len: usize,
    pub compression: u8,
    pub kind: u8,
}

#[derive(Debug)]
pub struct Page<'a> {
    pub index: usize,
    pub bytes: &'a [u8],
    pub page_type: PageType,
    pub block_count: usize,
    pub subheader_count: usize,
}

impl<'a> Page<'a> {
    pub fn parse(layout: Layout, index: usize, bytes: &'a [u8]) -> Result<Self> {
        let base = layout.page_header_offset();
        let e = layout.endianness;
        let code = e.u16(bytes, base, "page type")?;
        Ok(Self {
            index,
            bytes,
            page_type: PageType::from_code(code, index)?,
            block_count: e.u16(bytes, base + 2, "page block count")?.into(),
            subheader_count: e.u16(bytes, base + 4, "page subheader count")?.into(),
        })
    }

    /// The subheader pointers of this page, skipping empty and truncated entries.
    pub fn pointers(&self, layout: Layout) -> impl Iterator<Item = Result<SubheaderPointer>> + '_ {
        let table = layout.page_header_offset() + 8;
        let word = layout.word_len();
        (0..self.subheader_count).filter_map(move |i| {
            let at = table + i * layout.pointer_len();
            let read = || -> Result<SubheaderPointer> {
                let pointer = SubheaderPointer {
                    offset: layout.word(self.bytes, at, "subheader offset")?,
                    len: layout.word(self.bytes, at + word, "subheader length")?,
                    compression: *self
                        .bytes
                        .get(at + 2 * word)
                        .ok_or(Error::Truncated("subheader compression"))?,
                    kind: *self
                        .bytes
                        .get(at + 2 * word + 1)
                        .ok_or(Error::Truncated("subheader type"))?,
                };
                if pointer.offset.saturating_add(pointer.len) > self.bytes.len() {
                    return Err(Error::Truncated("subheader (points past end of page)"));
                }
                Ok(pointer)
            };
            match read() {
                Ok(p) if p.len == 0 || p.compression == SUBHEADER_TRUNCATED => None,
                other => Some(other),
            }
        })
    }

    pub fn subheader(&self, pointer: &SubheaderPointer) -> &'a [u8] {
        // Bounds were validated when the pointer was read.
        &self.bytes[pointer.offset..pointer.offset + pointer.len]
    }

    /// Offset of the first uncompressed row on Data and Mix pages.
    pub fn row_data_offset(&self, layout: Layout) -> usize {
        let offset = layout.page_header_offset() + 8 + self.subheader_count * layout.pointer_len();
        // Same alignment rule as readstat and pandas.
        offset + offset % 8
    }
}
