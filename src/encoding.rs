//! Character encodings declared in the SAS7BDAT header.

use crate::error::{Error, Result};
use encoding_rs::Encoding;
use std::borrow::Cow;

/// SAS name of the encoding stored at header offset 70.
pub fn sas_name(code: u8) -> &'static str {
    match code {
        0 => "default (WLATIN1)",
        20 => "UTF-8",
        28 => "US-ASCII",
        29 => "ISO-8859-1",
        30 => "ISO-8859-2",
        31 => "ISO-8859-3",
        32 => "ISO-8859-4",
        33 => "ISO-8859-5",
        34 => "ISO-8859-6",
        35 => "ISO-8859-7",
        36 => "ISO-8859-8",
        37 => "ISO-8859-9",
        39 => "ISO-8859-11",
        40 => "ISO-8859-15",
        41 => "CP437",
        42 => "CP850",
        43 => "CP852",
        44 => "CP857",
        45 => "CP858",
        46 => "CP862",
        47 => "CP864",
        48 => "CP865",
        49 => "CP866",
        50 => "CP869",
        51 => "CP874",
        52 => "CP921",
        53 => "CP922",
        54 => "CP1129",
        55 => "CP720",
        56 => "CP737",
        57 => "CP775",
        58 => "CP860",
        59 => "CP863",
        60 => "WINDOWS-1250",
        61 => "WINDOWS-1251",
        62 => "WINDOWS-1252",
        63 => "WINDOWS-1253",
        64 => "WINDOWS-1254",
        65 => "WINDOWS-1255",
        66 => "WINDOWS-1256",
        67 => "WINDOWS-1257",
        68 => "WINDOWS-1258",
        69 => "MACROMAN",
        70 => "MACARABIC",
        71 => "MACHEBREW",
        72 => "MACGREEK",
        73 => "MACTHAI",
        75 => "MACTURKISH",
        76 => "MACUKRAINE",
        118 => "CP950",
        119 => "EUC-TW",
        123 => "BIG-5",
        125 => "GB18030",
        126 => "WINDOWS-936",
        128 => "CP1381",
        134 => "EUC-JP",
        136 => "CP949",
        137 => "CP942",
        138 => "CP932",
        140 => "EUC-KR",
        141 => "CP949",
        142 => "CP949",
        163 => "MACICELAND",
        167 => "ISO-2022-JP",
        168 => "ISO-2022-KR",
        169 => "ISO-2022-CN",
        172 => "ISO-2022-CN-EXT",
        204 => "WINDOWS-1252",
        205 => "GB18030",
        227 => "ISO-8859-14",
        242 => "ISO-8859-13",
        245 => "MACCROATIAN",
        246 => "MACCYRILLIC",
        247 => "MACROMANIA",
        248 => "SHIFT_JISX0213",
        _ => "unknown",
    }
}

/// WHATWG label (as understood by `encoding_rs`) for a SAS encoding code, when one exists.
fn whatwg_label(code: u8) -> Option<&'static str> {
    Some(match code {
        // 0 means "SAS session default", which is WLATIN1 (Windows-1252) in practice.
        0 | 62 | 204 => "windows-1252",
        20 => "utf-8",
        // WHATWG maps both of these to Windows-1252, a superset of each.
        28 | 29 => "windows-1252",
        30 => "iso-8859-2",
        31 => "iso-8859-3",
        32 => "iso-8859-4",
        33 => "iso-8859-5",
        34 => "iso-8859-6",
        35 => "iso-8859-7",
        36 => "iso-8859-8",
        37 => "windows-1254",
        39 | 51 => "windows-874",
        40 => "iso-8859-15",
        49 => "ibm866",
        60 => "windows-1250",
        61 => "windows-1251",
        63 => "windows-1253",
        64 => "windows-1254",
        65 => "windows-1255",
        66 => "windows-1256",
        67 => "windows-1257",
        68 => "windows-1258",
        69 => "macintosh",
        76 | 246 => "x-mac-cyrillic",
        118 | 123 => "big5",
        125 | 205 => "gb18030",
        126 => "gbk",
        134 => "euc-jp",
        136 | 140 | 141 | 142 => "euc-kr",
        138 => "shift_jis",
        167 => "iso-2022-jp",
        227 => "iso-8859-14",
        242 => "iso-8859-13",
        _ => return None,
    })
}

#[derive(Debug, Clone, Copy)]
pub struct TextDecoder {
    encoding: &'static Encoding,
}

impl TextDecoder {
    /// Picks the decoder for the file's encoding code, or for an explicit label override.
    pub fn new(code: u8, override_label: Option<&str>) -> Result<Self> {
        let label = match override_label {
            Some(label) => label,
            None => whatwg_label(code)
                .ok_or_else(|| Error::Encoding(format!("{} (code {code})", sas_name(code))))?,
        };
        let encoding = Encoding::for_label(label.as_bytes())
            .ok_or_else(|| Error::Encoding(label.to_string()))?;
        Ok(Self { encoding })
    }

    pub fn name(&self) -> &'static str {
        self.encoding.name()
    }

    /// Decodes without copying when the bytes are already valid in the target encoding.
    #[inline]
    pub fn decode<'a>(&self, bytes: &'a [u8]) -> Cow<'a, str> {
        self.encoding.decode_without_bom_handling(bytes).0
    }

    /// Decodes a fixed-width SAS character field, dropping the trailing blank and NUL padding.
    #[inline]
    pub fn decode_field<'a>(&self, bytes: &'a [u8]) -> Cow<'a, str> {
        self.decode(trim_padding(bytes))
    }
}

/// Removes trailing spaces and NULs. This is safe on raw bytes for every supported
/// encoding: none of them use 0x20 or 0x00 as a trail byte of a multi-byte sequence.
#[inline]
pub fn trim_padding(bytes: &[u8]) -> &[u8] {
    let end = bytes
        .iter()
        .rposition(|&b| b != b' ' && b != 0)
        .map_or(0, |i| i + 1);
    &bytes[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trims_blank_and_nul_padding() {
        assert_eq!(trim_padding(b"abc  \0\0"), b"abc");
        assert_eq!(trim_padding(b"  a b "), b"  a b");
        assert_eq!(trim_padding(b"   "), b"");
    }

    #[test]
    fn default_encoding_is_windows_1252() {
        let decoder = TextDecoder::new(0, None).unwrap();
        assert_eq!(decoder.decode(b"caf\xe9"), "café");
    }

    #[test]
    fn unsupported_encoding_is_an_error_unless_overridden() {
        assert!(TextDecoder::new(41, None).is_err()); // CP437
        assert!(TextDecoder::new(41, Some("windows-1252")).is_ok());
    }
}
