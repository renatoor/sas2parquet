use encoding_rs::Encoding;
use lazy_static::lazy_static;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::convert::From;

const SAS_DEFAULT_ENCODING: &str = "WINDOWS-1252";

lazy_static! {
    static ref ENCODINGS: BTreeMap<u8, &'static str> = [
        (0, SAS_DEFAULT_ENCODING),
        (20, "UTF-8"),
        (28, "US-ASCII"),
        (29, "ISO-8859-1"),
        (30, "ISO-8859-2"),
        (31, "ISO-8859-3"),
        (32, "ISO-8859-4"),
        (33, "ISO-8859-5"),
        (34, "ISO-8859-6"),
        (35, "ISO-8859-7"),
        (36, "ISO-8859-8"),
        (37, "ISO-8859-9"),
        (39, "ISO-8859-11"),
        (40, "ISO-8859-15"),
        (41, "CP437"),
        (42, "CP850"),
        (43, "CP852"),
        (44, "CP857"),
        (45, "CP858"),
        (46, "CP862"),
        (47, "CP864"),
        (48, "CP865"),
        (49, "CP866"),
        (50, "CP869"),
        (51, "CP874"),
        (52, "CP921"),
        (53, "CP922"),
        (54, "CP1129"),
        (55, "CP720"),
        (56, "CP737"),
        (57, "CP775"),
        (58, "CP860"),
        (59, "CP863"),
        (60, "WINDOWS-1250"),
        (61, "WINDOWS-1251"),
        (62, "WINDOWS-1252"),
        (63, "WINDOWS-1253"),
        (64, "WINDOWS-1254"),
        (65, "WINDOWS-1255"),
        (66, "WINDOWS-1256"),
        (67, "WINDOWS-1257"),
        (68, "WINDOWS-1258"),
        (69, "MACROMAN"),
        (70, "MACARABIC"),
        (71, "MACHEBREW"),
        (72, "MACGREEK"),
        (73, "MACTHAI"),
        (75, "MACTURKISH"),
        (76, "MACUKRAINE"),
        (118, "CP950"),
        (119, "EUC-TW"),
        (123, "BIG-5"),
        (125, "GB18030"),
        (126, "WINDOWS-936"),
        (128, "CP1381"),
        (134, "EUC-JP"),
        (136, "CP949"),
        (137, "CP942"),
        (138, "CP932"),
        (140, "EUC-KR"),
        (141, "CP949"),
        (142, "CP949"),
        (163, "MACICELAND"),
        (167, "ISO-2022-JP"),
        (168, "ISO-2022-KR"),
        (169, "ISO-2022-CN"),
        (172, "ISO-2022-CN-EXT"),
        (204, SAS_DEFAULT_ENCODING),
        (205, "GB18030"),
        (227, "ISO-8859-14"),
        (242, "ISO-8859-13"),
        (245, "MACCROATIAN"),
        (246, "MACCYRILLIC"),
        (247, "MACROMANIA"),
        (248, "SHIFT_JISX0213"),
    ]
    .iter()
    .copied()
    .collect();
}

#[derive(Debug)]
pub struct Decoder {
    encoding: &'static Encoding,
}

impl From<u8> for Decoder {
    fn from(byte: u8) -> Self {
        match ENCODINGS.get(&byte) {
            Some(encoding) => Self::new(encoding),
            None => Self::new(SAS_DEFAULT_ENCODING),
        }
    }
}

impl Decoder {
    pub fn new(encoding: &str) -> Self {
        let encoding = Encoding::for_label(encoding.as_bytes()).unwrap();

        Decoder { encoding }
    }

    pub fn decode<'a>(&self, bytes: &'a [u8]) -> Cow<'a, str> {
        let (cow, _encoding, _had_errors) = self.encoding.decode(bytes);
        cow
    }
}
