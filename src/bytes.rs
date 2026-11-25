//! Bounds-checked primitive reads over the raw file bytes.

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endianness {
    Big,
    Little,
}

/// Returns `buf[offset..offset + len]`, or a `Truncated` error naming `what`.
#[inline]
pub fn slice<'a>(buf: &'a [u8], offset: usize, len: usize, what: &'static str) -> Result<&'a [u8]> {
    offset
        .checked_add(len)
        .and_then(|end| buf.get(offset..end))
        .ok_or(Error::Truncated(what))
}

#[inline]
fn array<const N: usize>(buf: &[u8], offset: usize, what: &'static str) -> Result<[u8; N]> {
    let bytes = slice(buf, offset, N, what)?;
    // `slice` returned exactly N bytes, so the conversion cannot fail.
    Ok(bytes.try_into().expect("slice has length N"))
}

impl Endianness {
    pub fn u16(self, buf: &[u8], offset: usize, what: &'static str) -> Result<u16> {
        let b = array(buf, offset, what)?;
        Ok(match self {
            Self::Big => u16::from_be_bytes(b),
            Self::Little => u16::from_le_bytes(b),
        })
    }

    pub fn u32(self, buf: &[u8], offset: usize, what: &'static str) -> Result<u32> {
        let b = array(buf, offset, what)?;
        Ok(match self {
            Self::Big => u32::from_be_bytes(b),
            Self::Little => u32::from_le_bytes(b),
        })
    }

    pub fn u64(self, buf: &[u8], offset: usize, what: &'static str) -> Result<u64> {
        let b = array(buf, offset, what)?;
        Ok(match self {
            Self::Big => u64::from_be_bytes(b),
            Self::Little => u64::from_le_bytes(b),
        })
    }

    pub fn f64(self, buf: &[u8], offset: usize, what: &'static str) -> Result<f64> {
        Ok(self.numeric(slice(buf, offset, 8, what)?))
    }

    /// Reads an integer that is 8 bytes wide in 64-bit files and 4 bytes wide in 32-bit files.
    pub fn word(self, buf: &[u8], offset: usize, wide: bool, what: &'static str) -> Result<usize> {
        let value = if wide {
            self.u64(buf, offset, what)?
        } else {
            u64::from(self.u32(buf, offset, what)?)
        };
        usize::try_from(value).map_err(|_| Error::Truncated(what))
    }

    /// Decodes a SAS numeric value stored in 1 to 8 bytes.
    ///
    /// SAS can store doubles truncated to fewer than 8 bytes; the stored bytes are
    /// always the most significant ones, so the missing low-order bytes are zero.
    #[inline]
    pub fn numeric(self, bytes: &[u8]) -> f64 {
        let width = bytes.len();
        debug_assert!((1..=8).contains(&width));
        let mut full = [0u8; 8];
        match self {
            Self::Little => {
                full[8 - width..].copy_from_slice(bytes);
                f64::from_le_bytes(full)
            }
            Self::Big => {
                full[..width].copy_from_slice(bytes);
                f64::from_be_bytes(full)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_width_doubles() {
        let x = 1234.5678_f64;
        assert_eq!(Endianness::Little.numeric(&x.to_le_bytes()), x);
        assert_eq!(Endianness::Big.numeric(&x.to_be_bytes()), x);
    }

    #[test]
    fn truncated_doubles_keep_high_order_bytes() {
        // 3.0 is 0x4008_0000_0000_0000, so 3 bytes are enough to represent it exactly.
        let le = 3.0_f64.to_le_bytes();
        let be = 3.0_f64.to_be_bytes();
        assert_eq!(Endianness::Little.numeric(&le[5..]), 3.0);
        assert_eq!(Endianness::Big.numeric(&be[..3]), 3.0);
    }

    #[test]
    fn out_of_bounds_reads_are_errors() {
        let buf = [0u8; 4];
        assert!(Endianness::Little.u64(&buf, 0, "test").is_err());
        assert!(Endianness::Little.u16(&buf, 3, "test").is_err());
        assert!(slice(&buf, usize::MAX, 2, "test").is_err());
    }
}
