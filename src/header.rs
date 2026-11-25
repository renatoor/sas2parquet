//! The fixed-layout file header at the start of every SAS7BDAT file.

use crate::bytes::{Endianness, slice};
use crate::error::{Error, Result};

const MAGIC: [u8; 32] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc2, 0xea, 0x81, 0x60,
    0xb3, 0x14, 0x11, 0xcf, 0xbd, 0x92, 0x08, 0x00, 0x09, 0xc7, 0x31, 0x8c, 0x18, 0x1f, 0x10, 0x11,
];

/// Size of the header fields we read, before alignment padding.
const MIN_HEADER_LEN: usize = 288;

/// Sizes that differ between files written by 32-bit and 64-bit SAS.
#[derive(Debug, Clone, Copy)]
pub struct Layout {
    pub wide: bool,
    pub endianness: Endianness,
}

impl Layout {
    /// Width of integers and offsets: 8 bytes in 64-bit files, 4 in 32-bit ones.
    pub fn word_len(self) -> usize {
        if self.wide { 8 } else { 4 }
    }

    /// Offset of the page header inside each page.
    pub fn page_header_offset(self) -> usize {
        if self.wide { 32 } else { 16 }
    }

    /// Size of each subheader pointer in a page's pointer table.
    pub fn pointer_len(self) -> usize {
        if self.wide { 24 } else { 12 }
    }

    pub fn word(self, buf: &[u8], offset: usize, what: &'static str) -> Result<usize> {
        self.endianness.word(buf, offset, self.wide, what)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Unix,
    Windows,
    Unknown,
}

#[derive(Debug)]
pub struct Header {
    pub layout: Layout,
    pub platform: Platform,
    pub encoding_code: u8,
    pub dataset_name: Vec<u8>,
    /// Seconds since 1960-01-01, as stored by SAS.
    pub created: f64,
    pub modified: f64,
    pub header_len: usize,
    pub page_len: usize,
    pub page_count: usize,
    pub sas_release: Vec<u8>,
    pub host: Vec<u8>,
}

impl Header {
    pub fn parse(buf: &[u8]) -> Result<Self> {
        if buf.len() < MIN_HEADER_LEN || buf[..32] != MAGIC {
            return Err(Error::BadMagic);
        }

        // Byte 32 flags 64-bit files, which have 4 extra bytes of padding after offset 204.
        // Byte 35 flags another 4 bytes of padding after offset 164.
        let wide = buf[32] == 0x33;
        let align2 = if wide { 4 } else { 0 };
        let align1 = if buf[35] == 0x33 { 4 } else { 0 };
        let total_align = align1 + align2;

        let endianness = match buf[37] {
            0x00 => Endianness::Big,
            0x01 => Endianness::Little,
            other => return Err(Error::ByteOrder(other)),
        };
        let layout = Layout { wide, endianness };

        let platform = match buf[39] {
            b'1' => Platform::Unix,
            b'2' => Platform::Windows,
            _ => Platform::Unknown,
        };

        let e = endianness;
        let header = Self {
            layout,
            platform,
            encoding_code: buf[70],
            dataset_name: slice(buf, 92, 64, "dataset name")?.to_vec(),
            created: e.f64(buf, 164 + align1, "creation date")?,
            modified: e.f64(buf, 172 + align1, "modification date")?,
            header_len: e.u32(buf, 196 + align1, "header length")? as usize,
            page_len: e.u32(buf, 200 + align1, "page length")? as usize,
            page_count: layout.word(buf, 204 + align1, "page count")?,
            sas_release: slice(buf, 216 + total_align, 8, "SAS release")?.to_vec(),
            host: slice(buf, 224 + total_align, 16, "host")?.to_vec(),
        };

        if header.page_len == 0 {
            return Err(Error::Metadata("page length is zero".into()));
        }
        let expected_len = header
            .page_count
            .checked_mul(header.page_len)
            .and_then(|pages| pages.checked_add(header.header_len))
            .ok_or(Error::Truncated("page table"))?;
        if buf.len() < expected_len {
            return Err(Error::Truncated(
                "pages (file is shorter than its header declares)",
            ));
        }

        Ok(header)
    }

    /// Bytes of the page at `index`. The caller guarantees `index < page_count`.
    pub fn page<'a>(&self, buf: &'a [u8], index: usize) -> &'a [u8] {
        let start = self.header_len + index * self.page_len;
        &buf[start..start + self.page_len]
    }
}
