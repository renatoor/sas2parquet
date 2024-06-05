mod decoder;

use arrow::datatypes::{DataType, Field, Schema};
use chrono::{prelude::*, TimeDelta};
use decoder::Decoder;
use memmap2::Mmap;
use rayon::prelude::*;
use std::borrow::Cow;
use std::convert::From;
use std::env;
use std::fs::File;

#[derive(Debug)]
enum Endianness {
    BigEndian,
    LittleEndian,
}

impl TryFrom<u8> for Endianness {
    type Error = &'static str;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            0x00 => Ok(Self::BigEndian),
            0x01 => Ok(Self::LittleEndian),
            _ => Err("Invalid byte order"),
        }
    }
}

impl Endianness {
    pub fn read_u16(&self, bytes: &[u8]) -> u16 {
        match self {
            Self::BigEndian => u16::from_be_bytes(bytes[..2].try_into().unwrap()),
            Self::LittleEndian => u16::from_le_bytes(bytes[..2].try_into().unwrap()),
        }
    }

    pub fn read_u32(&self, bytes: &[u8]) -> u32 {
        match self {
            Self::BigEndian => u32::from_be_bytes(bytes[..4].try_into().unwrap()),
            Self::LittleEndian => u32::from_le_bytes(bytes[..4].try_into().unwrap()),
        }
    }

    pub fn read_u64(&self, bytes: &[u8]) -> u64 {
        match self {
            Self::BigEndian => u64::from_be_bytes(bytes[..8].try_into().unwrap()),
            Self::LittleEndian => u64::from_le_bytes(bytes[..8].try_into().unwrap()),
        }
    }

    pub fn read_f64(&self, bytes: &[u8]) -> f64 {
        unsafe {
            std::mem::transmute(match self {
                Endianness::BigEndian => {
                    (0..bytes.len()).fold(0, |value, index| (value << 8) | bytes[index] as u64)
                        << (8 - bytes.len()) * 8
                }
                Endianness::LittleEndian => {
                    (0..bytes.len())
                        .rev()
                        .fold(0, |value, index| (value << 8) | bytes[index] as u64)
                        << (8 - bytes.len()) * 8
                }
            })
        }
    }
}

#[derive(Debug)]
enum Platform {
    Unix,
    Windows,
    Unknown,
}

impl From<u8> for Platform {
    fn from(byte: u8) -> Self {
        match byte {
            0x31 => Self::Unix,
            0x32 => Self::Windows,
            _ => Self::Unknown,
        }
    }
}

fn convert_sas_timestamp(timestamp: f64) -> DateTime<Utc> {
    let sas_date_start = Utc.with_ymd_and_hms(1960, 1, 1, 0, 0, 0).unwrap();
    let milliseconds = (timestamp * 1000.0) as i64;
    let time_delta = TimeDelta::milliseconds(milliseconds);
    sas_date_start + time_delta
}

#[derive(Debug)]
struct TextRef {
    index: usize,
    offset: usize,
    length: usize,
}

impl TextRef {
    pub fn new(ctx: &ParserContext, buffer: &[u8]) -> Self {
        let index = ctx.endianness.read_u16(&buffer[0..2]).into();
        let offset = ctx.endianness.read_u16(&buffer[2..4]).into();
        let length = ctx.endianness.read_u16(&buffer[4..6]).into();

        Self {
            index,
            offset,
            length,
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
enum PageType {
    Meta,
    Data,
    Mix,
    Amd,
    Comp,
}

impl TryFrom<u16> for PageType {
    type Error = &'static str;

    fn try_from(byte: u16) -> Result<Self, Self::Error> {
        match byte {
            0x0000 => Ok(Self::Meta),
            0x0100 => Ok(Self::Data),
            0x0200 => Ok(Self::Mix),
            0x0400 => Ok(Self::Amd),
            0x4000 => Ok(Self::Meta),
            0x9000 => Ok(Self::Comp),
            _ => Err("Invalid page type"),
        }
    }
}

struct Header<'a> {
    buffer: &'a [u8],
    is_64_bits: bool,
    align1: usize,
    total_align: usize,
    endianness: Endianness,
    decoder: Decoder,
}

impl<'a> Header<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        const HEADER_SIZE: usize = 288;

        let (align2, is_64_bits) = if buffer[32] == 0x33 {
            (4, true)
        } else {
            (0, false)
        };

        let align1 = if buffer[35] == 0x33 { 4 } else { 0 };
        let total_align = align1 + align2;
        let total_header_size = HEADER_SIZE + align1 + align2;
        let header_buffer = &buffer[0..total_header_size];
        let endianness = buffer[37].try_into().unwrap();
        let decoder = buffer[70].into();

        Self {
            buffer: header_buffer,
            is_64_bits,
            align1,
            total_align,
            endianness,
            decoder,
        }
    }

    pub fn check_magic_number(&self) {
        const MAGIC_NUMBER: [u8; 32] = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc2, 0xea,
            0x81, 0x60, 0xb3, 0x14, 0x11, 0xcf, 0xbd, 0x92, 0x08, 0x00, 0x09, 0xc7, 0x31, 0x8c,
            0x18, 0x1f, 0x10, 0x11,
        ];

        let magic_number = &self.buffer[0..32];

        assert_eq!(magic_number, MAGIC_NUMBER, "Invalid magic number");
    }

    pub fn endianness(&self) -> &Endianness {
        &self.endianness
    }

    pub fn platform(&self) -> Platform {
        self.buffer[39].into()
    }

    pub fn decoder(&self) -> &Decoder {
        &self.decoder
    }

    pub fn dataset_name(&self) -> Cow<'a, str> {
        self.decoder.decode(&self.buffer[92..156])
    }

    pub fn date_created(&self) -> DateTime<Utc> {
        convert_sas_timestamp(
            self.endianness
                .read_f64(&self.buffer[164 + self.align1..172 + self.align1]),
        )
    }

    pub fn date_modified(&self) -> DateTime<Utc> {
        convert_sas_timestamp(
            self.endianness
                .read_f64(&self.buffer[172 + self.align1..180 + self.align1]),
        )
    }

    pub fn header_length(&self) -> usize {
        self.endianness
            .read_u32(&self.buffer[196 + self.align1..200 + self.align1])
            .try_into()
            .unwrap()
    }

    pub fn page_length(&self) -> usize {
        self.endianness
            .read_u32(&self.buffer[200 + self.align1..204 + self.align1])
            .try_into()
            .unwrap()
    }

    pub fn page_count(&self) -> usize {
        if self.is_64_bits {
            self.endianness
                .read_u64(&self.buffer[204 + self.align1..208 + self.total_align])
                .try_into()
                .unwrap()
        } else {
            self.endianness
                .read_u32(&self.buffer[204 + self.align1..208 + self.total_align])
                .try_into()
                .unwrap()
        }
    }

    pub fn release(&self) -> Cow<'a, str> {
        self.decoder
            .decode(&self.buffer[216 + self.total_align..224 + self.total_align])
    }

    pub fn host(&self) -> Cow<'a, str> {
        self.decoder
            .decode(&self.buffer[224 + self.total_align..240 + self.total_align])
    }
}

#[derive(Debug)]
struct ParserContext<'a> {
    is_64_bits: bool,
    endianness: &'a Endianness,
    decoder: &'a Decoder,
}

impl<'a> ParserContext<'a> {
    pub fn new(is_64_bits: bool, endianness: &'a Endianness, decoder: &'a Decoder) -> Self {
        Self {
            is_64_bits,
            endianness,
            decoder,
        }
    }
}

#[derive(Debug)]
struct PageHeader {
    page_type: PageType,
    data_block_count: usize,
    subheader_pointers_count: usize,
}

impl PageHeader {
    pub fn new(ctx: &ParserContext, align: usize, buffer: &[u8]) -> Self {
        let page_type = ctx
            .endianness
            .read_u16(&buffer[align..align + 2])
            .try_into()
            .unwrap();

        let data_block_count = ctx
            .endianness
            .read_u16(&buffer[align + 2..align + 4])
            .into();

        let subheader_pointers_count = ctx
            .endianness
            .read_u16(&buffer[align + 4..align + 6])
            .into();

        Self {
            page_type,
            data_block_count,
            subheader_pointers_count,
        }
    }
}

#[derive(Debug, PartialEq)]
enum Compression {
    Uncompressed,
    Truncated,
    Compressed,
}

impl TryFrom<u8> for Compression {
    type Error = &'static str;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            0x00 => Ok(Self::Uncompressed),
            0x01 => Ok(Self::Truncated),
            0x04 => Ok(Self::Compressed),
            _ => Err("Invalid compression"),
        }
    }
}

#[derive(Debug)]
enum CompressionType {
    RunLengthEncoding,
    RossDataCompression,
    None,
}

impl From<&str> for CompressionType {
    fn from(compression_type: &str) -> Self {
        match compression_type {
            "SASYZCRL" => Self::RunLengthEncoding,
            "SASYZCR2" => Self::RossDataCompression,
            _ => Self::None,
        }
    }
}

#[derive(Debug)]
struct PageSubheaderPointer {
    offset: usize,
    length: usize,
    compression: Compression,
    is_compressed: bool,
}

impl PageSubheaderPointer {
    pub fn new(ctx: &ParserContext, buffer: &[u8]) -> Self {
        let (offset, length, compression, is_compressed) = if ctx.is_64_bits {
            (
                ctx.endianness.read_u64(&buffer[0..8]).try_into().unwrap(),
                ctx.endianness.read_u64(&buffer[8..16]).try_into().unwrap(),
                buffer[16].try_into().unwrap(),
                buffer[17] == 1,
            )
        } else {
            (
                ctx.endianness.read_u32(&buffer[0..4]).try_into().unwrap(),
                ctx.endianness.read_u32(&buffer[4..8]).try_into().unwrap(),
                buffer[8].try_into().unwrap(),
                buffer[9] == 1,
            )
        };

        Self {
            offset,
            length,
            compression,
            is_compressed,
        }
    }
}

#[derive(Debug)]
struct RowSizeSubheader {
    row_length: usize,
    total_row_count: usize,
    compression_ref: TextRef,
}

impl RowSizeSubheader {
    pub fn new(ctx: &ParserContext, buffer: &[u8]) -> Self {
        let (row_length, total_row_count) = if ctx.is_64_bits {
            (
                ctx.endianness.read_u64(&buffer[40..48]).try_into().unwrap(),
                ctx.endianness.read_u64(&buffer[48..56]).try_into().unwrap(),
            )
        } else {
            (
                ctx.endianness.read_u32(&buffer[20..24]).try_into().unwrap(),
                ctx.endianness.read_u32(&buffer[24..28]).try_into().unwrap(),
            )
        };

        let offset = buffer.len() - 118;
        let compression_ref = TextRef::new(ctx, &buffer[offset..offset + 6]);

        Self {
            row_length,
            total_row_count,
            compression_ref,
        }
    }
}

#[derive(Debug)]
struct ColumnSizeSubheader {
    columns_count: usize,
}

impl ColumnSizeSubheader {
    pub fn new(ctx: &ParserContext, buffer: &[u8]) -> Self {
        let columns_count = if ctx.is_64_bits {
            ctx.endianness.read_u64(&buffer[8..16]).try_into().unwrap()
        } else {
            ctx.endianness.read_u32(&buffer[4..8]).try_into().unwrap()
        };

        Self { columns_count }
    }
}

#[derive(Debug)]
struct TextSubheader<'a> {
    ctx: &'a ParserContext<'a>,
    length: usize,
    text_buffer: &'a [u8],
}

impl<'a> TextSubheader<'a> {
    pub fn new(ctx: &'a ParserContext, buffer: &'a [u8]) -> Self {
        let (length, text_buffer) = if ctx.is_64_bits {
            (ctx.endianness.read_u16(&buffer[8..10]).into(), &buffer[8..])
        } else {
            (ctx.endianness.read_u16(&buffer[4..6]).into(), &buffer[4..])
        };

        Self {
            ctx,
            length,
            text_buffer,
        }
    }

    pub fn text_from_ref(&self, text_ref: &TextRef) -> Cow<'a, str> {
        self.ctx
            .decoder
            .decode(&self.text_buffer[text_ref.offset..text_ref.offset + text_ref.length])
    }
}

#[derive(Debug)]
struct ColumnNameSubheader<'a> {
    ctx: &'a ParserContext<'a>,
    buffer: &'a [u8],
    cmax: usize,
    align: usize,
}

impl<'a> ColumnNameSubheader<'a> {
    pub fn new(ctx: &'a ParserContext, buffer: &'a [u8]) -> Self {
        let (_remaining_length, cmax, align) = if ctx.is_64_bits {
            (
                ctx.endianness.read_u16(&buffer[8..10]),
                (buffer.len() - 28) / 8,
                16,
            )
        } else {
            (
                ctx.endianness.read_u16(&buffer[4..6]),
                (buffer.len() - 20) / 8,
                12,
            )
        };

        Self {
            ctx,
            buffer,
            cmax,
            align,
        }
    }

    pub fn column_name_pointers(&self) -> Vec<TextRef> {
        (0..self.cmax)
            .map(|index| {
                let offset = self.align + index * 8;
                TextRef::new(self.ctx, &self.buffer[offset..offset + 6])
            })
            .collect()
    }
}

#[derive(Debug, PartialEq)]
enum ColumnType {
    Numeric,
    Character,
}

impl TryFrom<u8> for ColumnType {
    type Error = &'static str;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            0x01 => Ok(Self::Numeric),
            0x02 => Ok(Self::Character),
            _ => Err("Invalid column type"),
        }
    }
}

#[derive(Debug)]
struct ColumnAttrVector {
    offset: usize,
    width: usize,
    length: usize,
    column_type: ColumnType,
}

impl ColumnAttrVector {
    pub fn new(offset: usize, width: usize, length: usize, column_type: ColumnType) -> Self {
        Self {
            offset,
            width,
            length,
            column_type,
        }
    }
}

#[derive(Debug)]
struct ColumnAttrsSubheader<'a> {
    ctx: &'a ParserContext<'a>,
    buffer: &'a [u8],
    cmax: usize,
    lcav: usize,
}

impl<'a> ColumnAttrsSubheader<'a> {
    pub fn new(ctx: &'a ParserContext, buffer: &'a [u8]) -> Self {
        let (_remaining_length, cmax, lcav) = if ctx.is_64_bits {
            (
                ctx.endianness.read_u16(&buffer[8..10]),
                (buffer.len() - 28) / 16,
                16,
            )
        } else {
            (
                ctx.endianness.read_u16(&buffer[4..6]),
                (buffer.len() - 20) / 12,
                12,
            )
        };

        Self {
            ctx,
            buffer,
            cmax,
            lcav,
        }
    }

    pub fn column_attr_vectors(&self) -> Vec<ColumnAttrVector> {
        (0..self.cmax)
            .map(|index| {
                let attr_offset = self.lcav + index * self.lcav;

                let (offset, align) = if self.ctx.is_64_bits {
                    (
                        self.ctx
                            .endianness
                            .read_u64(&self.buffer[attr_offset..attr_offset + 8])
                            .try_into()
                            .unwrap(),
                        attr_offset + 8,
                    )
                } else {
                    (
                        self.ctx
                            .endianness
                            .read_u32(&self.buffer[attr_offset..attr_offset + 4])
                            .try_into()
                            .unwrap(),
                        attr_offset + 4,
                    )
                };

                let width = self
                    .ctx
                    .endianness
                    .read_u32(&self.buffer[align..align + 4])
                    .try_into()
                    .unwrap();

                let length = self
                    .ctx
                    .endianness
                    .read_u16(&self.buffer[align + 4..align + 6])
                    .into();

                let column_type = self.buffer[align + 6].try_into().unwrap();

                ColumnAttrVector::new(offset, width, length, column_type)
            })
            .collect()
    }
}

#[derive(Debug)]
struct ColumnFormatSubheader<'a> {
    ctx: &'a ParserContext<'a>,
    buffer: &'a [u8],
    align: usize,
}

impl<'a> ColumnFormatSubheader<'a> {
    pub fn new(ctx: &'a ParserContext, buffer: &'a [u8]) -> Self {
        let align = if ctx.is_64_bits { 46 } else { 34 };

        Self { ctx, buffer, align }
    }

    pub fn format_ref(&self) -> TextRef {
        TextRef::new(self.ctx, &self.buffer[self.align..self.align + 6])
    }

    pub fn label_ref(&self) -> TextRef {
        let offset = self.align + 6;
        TextRef::new(self.ctx, &self.buffer[offset..offset + 6])
    }
}

#[derive(Debug)]
enum Command {
    Copy64,
    Copy64Plus4096,
    Copy96,
    InsertByte18,
    InsertAt17,
    InsertBlank17,
    InsertZero17,
    Copy1,
    Copy17,
    Copy33,
    Copy49,
    InsertByte3,
    InsertAt2,
    InsertBlank2,
    InsertZero2,
}

impl TryFrom<u8> for Command {
    type Error = &'static str;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            0x0 => Ok(Self::Copy64),
            0x1 => Ok(Self::Copy64Plus4096),
            0x2 => Ok(Self::Copy96),
            0x4 => Ok(Self::InsertByte18),
            0x5 => Ok(Self::InsertAt17),
            0x6 => Ok(Self::InsertBlank17),
            0x7 => Ok(Self::InsertZero17),
            0x8 => Ok(Self::Copy1),
            0x9 => Ok(Self::Copy17),
            0xA => Ok(Self::Copy33),
            0xB => Ok(Self::Copy49),
            0xC => Ok(Self::InsertByte3),
            0xD => Ok(Self::InsertAt2),
            0xE => Ok(Self::InsertBlank2),
            0xF => Ok(Self::InsertZero2),
            _ => Err("Invalid compression command"),
        }
    }
}

#[derive(Debug)]
struct CompressedBinaryDataSubheader<'a> {
    buffer: &'a [u8],
}

impl<'a> CompressedBinaryDataSubheader<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        Self { buffer }
    }

    pub fn decompress(&self, metadata: &Metadata) -> Vec<u8> {
        match metadata.compression_type {
            CompressionType::RunLengthEncoding => self.decompress_rle(metadata),
            CompressionType::RossDataCompression => self.decompress_rdc(metadata),
            CompressionType::None => Vec::from(self.buffer),
        }
    }

    fn decompress_rle(&self, metadata: &Metadata) -> Vec<u8> {
        let mut bytes: Vec<u8> = vec![0; metadata.row_length];
        let mut offset = 0;
        let mut result_offset = 0;

        while offset < self.buffer.len() {
            let mut copy_length = 0;
            let mut insert_length = 0;
            let mut insert_byte = 0;

            let control = self.buffer[offset];
            let command = ((control & 0xF0) >> 4).try_into().unwrap();
            let length = usize::from(control & 0x0F);

            offset += 1;

            match command {
                Command::Copy64 => {
                    copy_length = usize::from(self.buffer[offset]) + 64 + length * 256;
                    offset += 1;
                }
                Command::Copy64Plus4096 => {
                    copy_length = usize::from(self.buffer[offset]) + 64 + length * 256 + 4096;
                    offset += 1;
                }
                Command::Copy96 => copy_length = length + 96,
                Command::InsertByte18 => {
                    insert_length = usize::from(self.buffer[offset]) + 18 + length * 256;
                    offset += 1;
                    insert_byte = self.buffer[offset];
                    offset += 1;
                }
                Command::InsertAt17 => {
                    insert_length = usize::from(self.buffer[offset]) + 17 + length * 256;
                    offset += 1;
                    insert_byte = b'@';
                }
                Command::InsertBlank17 => {
                    insert_length = usize::from(self.buffer[offset]) + 17 + length * 256;
                    offset += 1;
                    insert_byte = b' ';
                }
                Command::InsertZero17 => {
                    insert_length = usize::from(self.buffer[offset]) + 17 + length * 256;
                    offset += 1;
                    insert_byte = b'\0';
                }
                Command::Copy1 => copy_length = length + 1,
                Command::Copy17 => copy_length = length + 17,
                Command::Copy33 => copy_length = length + 33,
                Command::Copy49 => copy_length = length + 49,
                Command::InsertByte3 => {
                    insert_byte = self.buffer[offset];
                    offset += 1;
                    insert_length = length + 3;
                }
                Command::InsertAt2 => {
                    insert_byte = b'@';
                    insert_length = length + 2;
                }
                Command::InsertBlank2 => {
                    insert_byte = b' ';
                    insert_length = length + 2;
                }
                Command::InsertZero2 => {
                    insert_byte = b'\0';
                    insert_length = length + 2;
                }
            }

            if copy_length > 0 {
                bytes[result_offset..result_offset + copy_length]
                    .clone_from_slice(&self.buffer[offset..offset + copy_length]);

                offset += copy_length;
                result_offset += copy_length;
            }

            if insert_length > 0 {
                bytes[result_offset..result_offset + insert_length]
                    .clone_from_slice(&vec![insert_byte; insert_length]);

                result_offset += insert_length;
            }
        }

        bytes
    }

    // https://web.archive.org/web/20150409042848/http://collaboration.cmc.ec.gc.ca/science/rpn/biblio/ddj/Website/articles/CUJ/1992/9210/ross/list2.htm
    fn decompress_rdc(&self, metadata: &Metadata) -> Vec<u8> {
        let mut bytes: Vec<u8> = vec![0; metadata.row_length];
        let mut offset = 0;
        let mut result_offset = 0;

        let mut control_bits = 0;
        let mut control_mask = 0;

        while offset < self.buffer.len() {
            control_mask = control_mask >> 1;

            if control_mask == 0 {
                control_bits =
                    (u16::from(self.buffer[offset]) << 8) + u16::from(self.buffer[offset + 1]);
                offset += 2;
                control_mask = 0x8000;
            }

            if (control_bits & control_mask) == 0 {
                bytes[result_offset] = self.buffer[offset];
                result_offset += 1;
                offset += 1;
                continue;
            }

            let cmd = usize::from((self.buffer[offset] >> 4) & 0x0F);
            let mut cnt = usize::from(self.buffer[offset] & 0x0F);
            offset += 1;

            match cmd {
                0 => {
                    cnt += 3;

                    bytes[result_offset..result_offset + cnt].clone_from_slice(&vec![
                        self.buffer
                            [offset];
                        cnt
                    ]);

                    result_offset += cnt;
                    offset += 1;
                }
                1 => {
                    cnt += usize::from(u16::from(self.buffer[offset]) << 4);
                    cnt += 19;
                    offset += 1;

                    bytes[result_offset..result_offset + cnt].clone_from_slice(&vec![
                        self.buffer
                            [offset];
                        cnt
                    ]);

                    result_offset += cnt;
                    offset += 1;
                }
                2 => {
                    let mut ofs = cnt + 3;
                    ofs += usize::from(u16::from(self.buffer[offset]) << 4);
                    offset += 1;
                    cnt = usize::from(self.buffer[offset]);
                    offset += 1;
                    cnt += 16;

                    bytes.copy_within(
                        result_offset - ofs..result_offset - ofs + cnt,
                        result_offset,
                    );

                    result_offset += cnt;
                }
                _ => {
                    let mut ofs = cnt + 3;
                    ofs += usize::from(u16::from(self.buffer[offset]) << 4);
                    offset += 1;

                    bytes.copy_within(
                        result_offset - ofs..result_offset - ofs + cmd,
                        result_offset,
                    );

                    result_offset += cmd;
                }
            }
        }

        bytes
    }
}

#[derive(Debug)]
enum PageSubheaderType<'a> {
    RowSize(RowSizeSubheader),
    ColumnSize(ColumnSizeSubheader),
    Counts,
    Text(TextSubheader<'a>),
    ColumnName(ColumnNameSubheader<'a>),
    ColumnAttrs(ColumnAttrsSubheader<'a>),
    ColumnFormat(ColumnFormatSubheader<'a>),
    ColumnList,
    CompressedBinaryData(CompressedBinaryDataSubheader<'a>),
}

#[derive(Debug)]
struct PageSubheader<'a> {
    subheader_type: PageSubheaderType<'a>,
}

impl<'a> PageSubheader<'a> {
    pub fn new(ctx: &'a ParserContext, pointer: &PageSubheaderPointer, buffer: &'a [u8]) -> Self {
        let subheader_type = match pointer.compression {
            Compression::Compressed => {
                PageSubheaderType::CompressedBinaryData(CompressedBinaryDataSubheader::new(&buffer))
            }
            _ => {
                let signature = if ctx.is_64_bits {
                    ctx.endianness.read_u64(&buffer[0..8])
                } else {
                    ctx.endianness.read_u32(&buffer[0..4]).into()
                };

                match signature {
                    0xF7F7F7F7 | 0xF7F7F7F700000000 | 0xF7F7F7F7FFFFFBFE => {
                        PageSubheaderType::RowSize(RowSizeSubheader::new(ctx, buffer))
                    }
                    0xF6F6F6F6 | 0xF6F6F6F600000000 | 0xF6F6F6F6FFFFFBFE => {
                        PageSubheaderType::ColumnSize(ColumnSizeSubheader::new(ctx, buffer))
                    }
                    0xFFFFFC00 | 0xFFFFFFFFFFFFFC00 => PageSubheaderType::Counts,
                    0xFFFFFFFD | 0xFFFFFFFFFFFFFFFD => {
                        PageSubheaderType::Text(TextSubheader::new(ctx, buffer))
                    }
                    0xFFFFFFFF | 0xFFFFFFFFFFFFFFFF => {
                        PageSubheaderType::ColumnName(ColumnNameSubheader::new(ctx, buffer))
                    }
                    0xFFFFFFFC | 0xFFFFFFFFFFFFFFFC => {
                        PageSubheaderType::ColumnAttrs(ColumnAttrsSubheader::new(ctx, buffer))
                    }
                    0xFFFFFBFE | 0xFFFFFFFFFFFFFBFE => {
                        PageSubheaderType::ColumnFormat(ColumnFormatSubheader::new(ctx, buffer))
                    }
                    0xFFFFFFFE | 0xFFFFFFFFFFFFFFFE => PageSubheaderType::ColumnList,
                    _ => panic!("Invalid page subheader signature"),
                }
            }
        };

        Self { subheader_type }
    }
}

#[derive(Debug)]
struct Metadata {
    row_length: usize,
    total_row_count: usize,
    column_count: usize,
    compression_type: CompressionType,
    column_names: Vec<String>,
    column_attrs: Vec<ColumnAttrVector>,
    formats: Vec<String>,
    labels: Vec<String>,
}

impl Metadata {
    pub fn new(row_length: usize, total_row_count: usize, column_count: usize) -> Self {
        Self {
            row_length,
            total_row_count,
            column_count,
            compression_type: CompressionType::None,
            column_names: Vec::new(),
            column_attrs: Vec::new(),
            formats: Vec::new(),
            labels: Vec::new(),
        }
    }
}

impl From<&Vec<Page<'_>>> for Metadata {
    fn from(pages: &Vec<Page>) -> Self {
        let subheaders = pages
            .iter()
            .take_while(|page| page.page_type() != PageType::Data)
            .filter(|page| page.page_type() != PageType::Comp)
            .flat_map(|page| page.subheaders())
            .collect::<Vec<_>>();

        let text_subheaders = subheaders
            .iter()
            .filter_map(|subheader| match &subheader.subheader_type {
                PageSubheaderType::Text(subheader) => Some(subheader),
                _ => None,
            })
            .collect::<Vec<_>>();

        subheaders
            .iter()
            .fold(Metadata::new(0, 0, 0), |mut metadata, subheader| {
                match &subheader.subheader_type {
                    PageSubheaderType::RowSize(subheader) => {
                        let text_subheader = text_subheaders[subheader.compression_ref.index];

                        metadata.row_length = subheader.row_length;
                        metadata.total_row_count = subheader.total_row_count;
                        metadata.compression_type = text_subheader
                            .text_from_ref(&subheader.compression_ref)
                            .as_ref()
                            .into();
                    }
                    PageSubheaderType::ColumnSize(subheader) => {
                        metadata.column_count = subheader.columns_count;
                    }
                    PageSubheaderType::ColumnName(subheader) => {
                        for column_name_pointer in subheader.column_name_pointers() {
                            let text_subheader = &text_subheaders[column_name_pointer.index];

                            metadata.column_names.push(
                                text_subheader
                                    .text_from_ref(&column_name_pointer)
                                    .trim()
                                    .to_string(),
                            );
                        }
                    }
                    PageSubheaderType::ColumnAttrs(subheader) => {
                        metadata
                            .column_attrs
                            .extend(subheader.column_attr_vectors());
                    }
                    PageSubheaderType::ColumnFormat(subheader) => {
                        let format_ref = subheader.format_ref();
                        let text_subheader = &text_subheaders[format_ref.index];

                        metadata
                            .formats
                            .push(text_subheader.text_from_ref(&format_ref).trim().to_string());

                        let label_ref = subheader.label_ref();
                        let text_subheader = &text_subheaders[label_ref.index];

                        metadata
                            .labels
                            .push(text_subheader.text_from_ref(&label_ref).trim().to_string());
                    }
                    _ => {}
                }

                metadata
            })
    }
}

impl From<&Metadata> for Schema {
    fn from(metadata: &Metadata) -> Self {
        let fields = (0..metadata.column_count)
            .map(|index| {
                let name = &metadata.column_names[index];
                let column_type = &metadata.column_attrs[index].column_type;

                match column_type {
                    ColumnType::Character => Field::new(name, DataType::Utf8, true),
                    ColumnType::Numeric => Field::new(name, DataType::Float64, true),
                }
            })
            .collect::<Vec<_>>();

        Self::new(fields)
    }
}

#[derive(Debug)]
struct Page<'a> {
    ctx: &'a ParserContext<'a>,
    subheader_pointer_length: usize,
    align: usize,
    buffer: &'a [u8],
    header: PageHeader,
    page_subheaders: Option<Vec<PageSubheader<'a>>>,
}

impl<'a> Page<'a> {
    pub fn new(ctx: &'a ParserContext, buffer: &'a [u8]) -> Self {
        let (align, subheader_pointer_length) = if ctx.is_64_bits { (32, 24) } else { (16, 12) };
        let header = PageHeader::new(ctx, align, &buffer[0..align + 6]);

        Self {
            ctx,
            align,
            subheader_pointer_length,
            buffer,
            header,
            page_subheaders: None,
        }
    }

    pub fn page_type(&self) -> PageType {
        self.header.page_type
    }

    fn parse_data(&self, metadata: &Metadata) {
        if self.page_type() != PageType::Comp {
            match metadata.compression_type {
                CompressionType::None => self.parse_uncompressed_data(metadata),
                _ => self.parse_compressed_data(metadata),
            }
        }
    }

    fn parse_uncompressed_data(&self, metadata: &Metadata) {
        let rows_count = self.header.data_block_count - self.header.subheader_pointers_count;
        let data_buffer = {
            let offset = self.align
                + 8
                + self.header.subheader_pointers_count * self.subheader_pointer_length;
            let align_offset = offset + (offset % 8);
            &self.buffer[align_offset..]
        };

        for i in 0..rows_count {
            print!("ROW {} ", i);
            for column_attr in &metadata.column_attrs {
                let offset = column_attr.offset + i * metadata.row_length;
                let width = column_attr.width;

                match column_attr.column_type {
                    ColumnType::Character => {
                        let value = self
                            .ctx
                            .decoder
                            .decode(&data_buffer[offset..offset + width]);
                        print!("{:?} ", value.trim());
                    }
                    ColumnType::Numeric => {
                        let value = self
                            .ctx
                            .endianness
                            .read_f64(&data_buffer[offset..offset + width]);
                        print!("{:?} ", value);
                    }
                }
            }
            println!("");
        }
    }

    fn parse_compressed_data(&self, metadata: &Metadata) {
        let uncompressed_data = self
            .subheaders()
            .par_iter()
            .filter_map(|subheader| match &subheader.subheader_type {
                PageSubheaderType::CompressedBinaryData(subheader) => Some(subheader),
                _ => None,
            })
            .map(|subheader| subheader.decompress(metadata))
            .collect::<Vec<_>>();

        for (i, data) in uncompressed_data.iter().enumerate() {
            print!("ROW {} ", i);
            for column_attr in &metadata.column_attrs {
                let offset = column_attr.offset;
                let width = column_attr.width;

                match column_attr.column_type {
                    ColumnType::Character => {
                        let value = self.ctx.decoder.decode(&data[offset..offset + width]);
                        print!("{:?} ", value.trim());
                    }
                    ColumnType::Numeric => {
                        let value = self.ctx.endianness.read_f64(&data[offset..offset + width]);
                        print!("{:?} ", value);
                    }
                }
            }
            println!("");
        }
    }

    pub fn subheaders(&self) -> Vec<PageSubheader> {
        (0..self.header.subheader_pointers_count)
            .map(|index| {
                let pointer_offset = self.align + 8 + index * self.subheader_pointer_length;
                let pointer_buffer =
                    &self.buffer[pointer_offset..pointer_offset + self.subheader_pointer_length];
                PageSubheaderPointer::new(self.ctx, pointer_buffer)
            })
            .filter(|pointer| pointer.length > 0 && pointer.compression != Compression::Truncated)
            .map(|pointer| {
                let subheader_buffer =
                    &self.buffer[pointer.offset..pointer.offset + pointer.length];
                PageSubheader::new(self.ctx, &pointer, subheader_buffer)
            })
            .collect()
    }
}

struct Parser<'a> {
    buffer: &'a [u8],
}

impl<'a> Parser<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        Self { buffer }
    }

    pub fn parse(&self) {
        let header = Header::new(self.buffer);

        header.check_magic_number();

        let header_length = header.header_length();
        let page_length = header.page_length();
        let is_64_bits = header.is_64_bits;
        let endianness = header.endianness();
        let decoder = header.decoder();
        let ctx = ParserContext::new(is_64_bits, endianness, decoder);

        //println!("{:?}", ctx);

        let pages = (0..header.page_count())
            .map(|index| {
                let offset = header_length + index * page_length;
                Page::new(&ctx, &self.buffer[offset..offset + page_length])
            })
            .collect::<Vec<_>>();

        let metadata = Metadata::from(&pages);

        println!("{:?}", metadata);

        pages.iter().for_each(|page| page.parse_data(&metadata));
    }
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let file = File::open(&args[1])?;
    let mmap = unsafe { Mmap::map(&file)? };
    let parser = Parser::new(&mmap[..]);

    parser.parse();

    Ok(())
}
