//! Row decompression for the two schemes SAS uses: RLE ("SASYZCRL") and RDC ("SASYZCR2").
//!
//! Both decoders write into a caller-provided buffer of exactly one row, so a batch of
//! rows can be decompressed into a single reusable arena without per-row allocations.
//! Malformed input returns an error instead of panicking.

use crate::error::{Error, Result};

/// Upper bound on output bytes per input byte for either scheme. The most expansive
/// command is RLE's 2-byte "insert 17+ blanks/zeros/@" run, which emits up to 4112 bytes.
pub const MAX_EXPANSION: usize = 2056;

#[inline]
fn byte(input: &[u8], pos: &mut usize) -> Result<u8> {
    let b = *input
        .get(*pos)
        .ok_or(Error::Decompress("input ended mid-command"))?;
    *pos += 1;
    Ok(b)
}

#[inline]
fn fill(out: &mut [u8], at: &mut usize, len: usize, value: u8) -> Result<()> {
    let dst = out
        .get_mut(*at..*at + len)
        .ok_or(Error::Decompress("output overflows the row"))?;
    dst.fill(value);
    *at += len;
    Ok(())
}

#[inline]
fn copy(input: &[u8], pos: &mut usize, out: &mut [u8], at: &mut usize, len: usize) -> Result<()> {
    let src = input
        .get(*pos..*pos + len)
        .ok_or(Error::Decompress("copy reads past the input"))?;
    let dst = out
        .get_mut(*at..*at + len)
        .ok_or(Error::Decompress("output overflows the row"))?;
    dst.copy_from_slice(src);
    *pos += len;
    *at += len;
    Ok(())
}

/// SAS run-length encoding. Each control byte holds a command in the high nibble and a
/// length component in the low nibble; commands either copy literal bytes or repeat one.
pub fn rle(input: &[u8], out: &mut [u8]) -> Result<()> {
    let (mut pos, mut at) = (0, 0);
    while pos < input.len() {
        let control = byte(input, &mut pos)?;
        let low = usize::from(control & 0x0F);
        match control >> 4 {
            0x0 => {
                let len = usize::from(byte(input, &mut pos)?) + 64 + low * 256;
                copy(input, &mut pos, out, &mut at, len)?;
            }
            0x1 => {
                let len = usize::from(byte(input, &mut pos)?) + 64 + low * 256 + 4096;
                copy(input, &mut pos, out, &mut at, len)?;
            }
            0x2 => copy(input, &mut pos, out, &mut at, low + 96)?,
            0x4 => {
                let len = usize::from(byte(input, &mut pos)?) + 18 + low * 256;
                let value = byte(input, &mut pos)?;
                fill(out, &mut at, len, value)?;
            }
            cmd @ 0x5..=0x7 => {
                let len = usize::from(byte(input, &mut pos)?) + 17 + low * 256;
                let value = [b'@', b' ', 0][usize::from(cmd - 0x5)];
                fill(out, &mut at, len, value)?;
            }
            0x8 => copy(input, &mut pos, out, &mut at, low + 1)?,
            0x9 => copy(input, &mut pos, out, &mut at, low + 17)?,
            0xA => copy(input, &mut pos, out, &mut at, low + 33)?,
            0xB => copy(input, &mut pos, out, &mut at, low + 49)?,
            0xC => {
                let value = byte(input, &mut pos)?;
                fill(out, &mut at, low + 3, value)?;
            }
            cmd @ 0xD..=0xF => {
                let value = [b'@', b' ', 0][usize::from(cmd - 0xD)];
                fill(out, &mut at, low + 2, value)?;
            }
            _ => return Err(Error::Decompress("unknown RLE command")),
        }
    }
    // Rows shorter than the declared length are zero-padded, as SAS and readstat do.
    out[at..].fill(0);
    Ok(())
}

/// Ross Data Compression: a 16-bit control word says, for each of the next 16 items,
/// whether it is a literal byte or a command (run, or back-reference into the output).
pub fn rdc(input: &[u8], out: &mut [u8]) -> Result<()> {
    let (mut pos, mut at) = (0, 0);
    let mut control_bits: u16 = 0;
    let mut control_mask: u16 = 0;

    while pos < input.len() {
        control_mask >>= 1;
        if control_mask == 0 {
            let hi = byte(input, &mut pos)?;
            let lo = byte(input, &mut pos)?;
            control_bits = u16::from_be_bytes([hi, lo]);
            control_mask = 0x8000;
            if pos >= input.len() {
                break;
            }
        }

        if control_bits & control_mask == 0 {
            let value = byte(input, &mut pos)?;
            fill(out, &mut at, 1, value)?;
            continue;
        }

        let command = byte(input, &mut pos)?;
        let cmd = usize::from(command >> 4);
        let cnt = usize::from(command & 0x0F);
        match cmd {
            // Short run: repeat the next byte cnt + 3 times.
            0 => {
                let value = byte(input, &mut pos)?;
                fill(out, &mut at, cnt + 3, value)?;
            }
            // Long run: repeat the next byte cnt + (next << 4) + 19 times.
            1 => {
                let len = cnt + (usize::from(byte(input, &mut pos)?) << 4) + 19;
                let value = byte(input, &mut pos)?;
                fill(out, &mut at, len, value)?;
            }
            // Long back-reference: copy 16 + next bytes from `ofs` bytes back.
            2 => {
                let ofs = cnt + 3 + (usize::from(byte(input, &mut pos)?) << 4);
                let len = usize::from(byte(input, &mut pos)?) + 16;
                back_reference(out, &mut at, ofs, len)?;
            }
            // Short back-reference: the command itself (3..=15) is the length.
            _ => {
                let ofs = cnt + 3 + (usize::from(byte(input, &mut pos)?) << 4);
                back_reference(out, &mut at, ofs, cmd)?;
            }
        }
    }
    out[at..].fill(0);
    Ok(())
}

/// Copies `len` bytes starting `ofs` bytes behind the write position. The source and
/// destination may overlap (ofs < len), in which case the copy must run byte by byte so
/// that freshly written bytes are repeated, as in LZ77. `copy_within` would not do that.
#[inline]
fn back_reference(out: &mut [u8], at: &mut usize, ofs: usize, len: usize) -> Result<()> {
    let start = at
        .checked_sub(ofs)
        .ok_or(Error::Decompress("back-reference before start of row"))?;
    if *at + len > out.len() {
        return Err(Error::Decompress("output overflows the row"));
    }
    if ofs >= len {
        out.copy_within(start..start + len, *at);
    } else {
        for i in 0..len {
            out[*at + i] = out[start + i];
        }
    }
    *at += len;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rle_copies_and_runs() {
        // 0x82: copy 3 literal bytes; 0xE1: 3 blanks; 0xC0 'x': 3 x's; 0xF0: 2 NULs.
        let input = [0x82, b'a', b'b', b'c', 0xE1, 0xC0, b'x', 0xF0];
        let mut out = [0xFFu8; 13];
        rle(&input, &mut out).unwrap();
        assert_eq!(&out, b"abc   xxx\0\0\0\0");
    }

    #[test]
    fn rle_rejects_overflow() {
        let input = [0xC5, b'x']; // 8 x's into a 4 byte row
        assert!(rle(&input, &mut [0u8; 4]).is_err());
    }

    #[test]
    fn rle_rejects_truncated_input() {
        assert!(rle(&[0x85, b'a'], &mut [0u8; 16]).is_err());
    }

    #[test]
    fn rdc_literals_and_runs() {
        // Control word 0b0100_0000_0000_0000: item 0 literal, item 1 command, item 2 literal.
        // Command 0x01 'z' = short run of 1 + 3 = 4 'z'.
        let input = [0x40, 0x00, b'a', 0x01, b'z', b'b'];
        let mut out = [0u8; 6];
        rdc(&input, &mut out).unwrap();
        assert_eq!(&out, b"azzzzb");
    }

    #[test]
    fn rdc_overlapping_back_reference_repeats_pattern() {
        // Literals "abc", then command 0x60 0x00: copy 6 bytes from 3 bytes back.
        // Source and destination overlap, so the pattern must repeat: "abcabcabc".
        let input = [0x10, 0x00, b'a', b'b', b'c', 0x60, 0x00];
        let mut out = [0u8; 9];
        rdc(&input, &mut out).unwrap();
        assert_eq!(&out, b"abcabcabc");
    }

    #[test]
    fn rdc_rejects_back_reference_before_start() {
        let input = [0x80, 0x00, 0x30, 0x00];
        assert!(rdc(&input, &mut [0u8; 8]).is_err());
    }
}
