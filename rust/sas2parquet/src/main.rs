mod decoder;

use arrow::datatypes::{DataType, Field, Schema};
use chrono::{prelude::*, TimeDelta};
use decoder::Decoder;
use memmap2::Mmap;
use std::borrow::Cow;
use std::convert::From;
use std::env;
use std::fs::File;

#[derive(Debug)]
enum Endianness {
    BigEndian,
    LittleEndian,
}

impl From<u8> for Endianness {
    fn from(byte: u8) -> Self {
        match byte {
            0x00 => Self::BigEndian,
            0x01 => Self::LittleEndian,
            _ => panic!("ERROW"),
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
    let seconds = timestamp.floor() as i64;
    let time_delta = TimeDelta::seconds(seconds);
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

#[derive(Debug, PartialEq)]
enum PageType {
    Meta,
    Data,
    Mix,
    Amd,
    Comp,
}

impl From<u16> for PageType {
    fn from(byte: u16) -> Self {
        match byte {
            0x0000 => Self::Meta,
            0x0100 => Self::Data,
            0x0200 => Self::Mix,
            0x0400 => Self::Amd,
            0x4000 => Self::Meta,
            0x9000 => Self::Comp,
            _ => panic!("Invalid page type"),
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
        let endianness = Endianness::from(buffer[37]);
        let decoder = Decoder::from(buffer[70]);

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
        Platform::from(self.buffer[39])
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
struct PageHeader<'a> {
    ctx: &'a ParserContext<'a>,
    align: usize,
    buffer: &'a [u8],
}

impl<'a> PageHeader<'a> {
    pub fn new(ctx: &'a ParserContext, align: usize, buffer: &'a [u8]) -> Self {
        Self { ctx, align, buffer }
    }

    pub fn page_type(&self) -> PageType {
        PageType::from(
            self.ctx
                .endianness
                .read_u16(&self.buffer[self.align..self.align + 2]),
        )
    }

    pub fn data_block_count(&self) -> usize {
        self.ctx
            .endianness
            .read_u16(&self.buffer[self.align + 2..self.align + 4])
            .into()
    }

    pub fn subheader_pointers_count(&self) -> usize {
        self.ctx
            .endianness
            .read_u16(&self.buffer[self.align + 4..self.align + 6])
            .into()
    }
}

#[derive(Debug, PartialEq)]
enum Compression {
    Uncompressed,
    Truncated,
    Compressed,
}

impl From<u8> for Compression {
    fn from(byte: u8) -> Self {
        match byte {
            0x00 => Self::Uncompressed,
            0x01 => Self::Truncated,
            0x04 => Self::Compressed,
            _ => panic!("Invalid compression"),
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
                Compression::from(buffer[16]),
                buffer[17] == 1,
            )
        } else {
            (
                ctx.endianness.read_u32(&buffer[0..4]).try_into().unwrap(),
                ctx.endianness.read_u32(&buffer[4..8]).try_into().unwrap(),
                Compression::from(buffer[8]),
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

        Self {
            row_length,
            total_row_count,
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

impl From<u8> for ColumnType {
    fn from(byte: u8) -> Self {
        match byte {
            0x01 => Self::Numeric,
            0x02 => Self::Character,
            _ => panic!("Invalid column type"),
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

                let column_type = ColumnType::from(self.buffer[align + 6]);

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
enum PageSubheaderType<'a> {
    RowSize(RowSizeSubheader),
    ColumnSize(ColumnSizeSubheader),
    Counts,
    Text(TextSubheader<'a>),
    ColumnName(ColumnNameSubheader<'a>),
    ColumnAttrs(ColumnAttrsSubheader<'a>),
    ColumnFormat(ColumnFormatSubheader<'a>),
    ColumnList,
    CompressedBinaryData,
}

#[derive(Debug)]
struct PageSubheader<'a> {
    subheader_type: PageSubheaderType<'a>,
}

impl<'a> PageSubheader<'a> {
    pub fn new(ctx: &'a ParserContext, pointer: &PageSubheaderPointer, buffer: &'a [u8]) -> Self {
        let subheader_type = match pointer.compression {
            Compression::Compressed => PageSubheaderType::CompressedBinaryData,
            _ => {
                let signature = if ctx.is_64_bits {
                    ctx.endianness.read_u64(&buffer[0..8])
                } else {
                    u64::from(ctx.endianness.read_u32(&buffer[0..4]))
                };

                match signature {
                    0xF7F7F7F7 | 0xF7F7F7F700000000 => {
                        PageSubheaderType::RowSize(RowSizeSubheader::new(ctx, buffer))
                    }
                    0xF6F6F6F6 | 0xF6F6F6F600000000 => {
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
    column_names: Vec<String>,
    column_attrs: Vec<ColumnAttrVector>,
    formats: Vec<String>,
    labels: Vec<String>,
}

impl Metadata {
    pub fn new(pages: &Vec<Page>) -> Self {
        let subheaders = pages
            .iter()
            .take_while(|page| page.page_type != PageType::Data)
            .filter(|page| page.page_type != PageType::Comp)
            .flat_map(|page| page.subheaders())
            .collect::<Vec<_>>();

        let text_subheaders = subheaders
            .iter()
            .filter_map(|subheader| match &subheader.subheader_type {
                PageSubheaderType::Text(subheader) => Some(subheader),
                _ => None,
            })
            .collect::<Vec<_>>();

        let mut row_length = 0;
        let mut total_row_count = 0;
        let mut column_count = 0;
        let mut column_names = Vec::new();
        let mut column_attrs = Vec::new();
        let mut formats = Vec::new();
        let mut labels = Vec::new();

        for subheader in &subheaders {
            match &subheader.subheader_type {
                PageSubheaderType::RowSize(subheader) => {
                    row_length = subheader.row_length;
                    total_row_count = subheader.total_row_count;
                }
                PageSubheaderType::ColumnSize(subheader) => {
                    column_count = subheader.columns_count;
                }
                PageSubheaderType::ColumnName(subheader) => {
                    for column_name_pointer in subheader.column_name_pointers() {
                        let text_subheader = &text_subheaders[column_name_pointer.index];
                        column_names.push(
                            text_subheader
                                .text_from_ref(&column_name_pointer)
                                .trim()
                                .to_string(),
                        );
                    }
                }
                PageSubheaderType::ColumnAttrs(subheader) => {
                    column_attrs.extend(subheader.column_attr_vectors());
                }
                PageSubheaderType::ColumnFormat(subheader) => {
                    let format_ref = subheader.format_ref();
                    let text_subheader = &text_subheaders[format_ref.index];

                    formats.push(text_subheader.text_from_ref(&format_ref).trim().to_string());

                    let label_ref = subheader.label_ref();
                    let text_subheader = &text_subheaders[label_ref.index];

                    labels.push(text_subheader.text_from_ref(&label_ref).trim().to_string());
                }
                _ => {}
            }
        }

        Self {
            row_length,
            total_row_count,
            column_count,
            column_names,
            column_attrs,
            formats,
            labels,
        }
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
    subheader_pointers_count: usize,
    align: usize,
    buffer: &'a [u8],
    header: PageHeader<'a>,
    page_type: PageType,
}

impl<'a> Page<'a> {
    pub fn new(ctx: &'a ParserContext, buffer: &'a [u8]) -> Self {
        let (align, subheader_pointer_length) = if ctx.is_64_bits { (32, 24) } else { (16, 12) };
        let header = PageHeader::new(ctx, align, &buffer[0..align + 6]);
        let page_type = header.page_type();
        let subheader_pointers_count = header.subheader_pointers_count();

        Self {
            ctx,
            align,
            subheader_pointer_length,
            subheader_pointers_count,
            buffer,
            header,
            page_type,
        }
    }

    fn parse_data(&self, metadata: &Metadata) {
        let rows_count = self.header.data_block_count() - self.subheader_pointers_count;
        let data_buffer = {
            let offset =
                self.align + 8 + self.subheader_pointers_count * self.subheader_pointer_length;
            let offset = offset + (offset % 8);
            &self.buffer[offset..]
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

    pub fn subheaders(&self) -> Vec<PageSubheader> {
        (0..self.subheader_pointers_count)
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

        println!("{:?}", ctx);

        let pages = (0..header.page_count())
            .map(|index| {
                let offset = header_length + index * page_length;
                Page::new(&ctx, &self.buffer[offset..offset + page_length])
            })
            .collect::<Vec<_>>();

        let metadata = Metadata::new(&pages);

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
