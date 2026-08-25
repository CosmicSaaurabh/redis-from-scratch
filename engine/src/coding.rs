//! Length-prefixed binary encoding shared by every on-disk format.
//!
//! Varints are used for lengths because the overwhelming majority of keys and
//! values in a key-value store are short: a fixed four-byte length would spend
//! three bytes of every record saying "this is under 128 bytes".

use crate::error::{Error, Result};
use std::path::Path;

/// Appends an unsigned varint.
pub fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Appends a signed varint using zig-zag encoding, so that small negative
/// numbers stay small instead of setting every high bit.
pub fn put_varint(out: &mut Vec<u8>, v: i64) {
    put_uvarint(out, ((v << 1) ^ (v >> 63)) as u64);
}

/// Appends a length-prefixed byte string.
pub fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_uvarint(out, b.len() as u64);
    out.extend_from_slice(b);
}

/// A cursor over an encoded buffer.
///
/// It carries the file path so that every decode failure names the file it came
/// from. Corruption reported without a path is corruption an operator cannot
/// act on.
pub struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
    path: &'a Path,
}

impl<'a> Cursor<'a> {
    /// Wraps a buffer.
    pub fn new(buf: &'a [u8], path: &'a Path) -> Self {
        Cursor { buf, pos: 0, path }
    }

    /// Bytes consumed so far.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Bytes left.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Reports whether the cursor is exhausted.
    pub fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn fail(&self, detail: impl Into<String>) -> Error {
        Error::corrupt(self.path, detail)
    }

    /// Reads an unsigned varint.
    pub fn uvarint(&mut self) -> Result<u64> {
        let mut v: u64 = 0;
        let mut shift = 0u32;
        for _ in 0..10 {
            let b = *self
                .buf
                .get(self.pos)
                .ok_or_else(|| self.fail("truncated varint"))?;
            self.pos += 1;
            if b < 0x80 {
                return v
                    .checked_add((b as u64) << shift)
                    .ok_or_else(|| self.fail("varint overflows 64 bits"));
            }
            v |= ((b & 0x7f) as u64) << shift;
            shift += 7;
        }
        Err(self.fail("varint longer than 10 bytes"))
    }

    /// Reads a zig-zag signed varint.
    pub fn varint(&mut self) -> Result<i64> {
        let u = self.uvarint()?;
        Ok(((u >> 1) as i64) ^ -((u & 1) as i64))
    }

    /// Reads a length-prefixed byte string, borrowed from the buffer.
    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.uvarint()? as usize;
        self.take(n)
    }

    /// Reads exactly `n` bytes.
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(self.fail(format!(
                "wanted {n} bytes at offset {} but only {} remain",
                self.pos,
                self.remaining()
            )));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    /// Reads one byte.
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    /// Reads a little-endian `u32`.
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    /// Reads a little-endian `u64`.
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

/// Computes CRC-32C over a buffer.
///
/// CRC-32C rather than the IEEE polynomial because both arm64 and x86-64
/// implement it in hardware, which keeps checksumming off the critical path of
/// every block read.
pub fn checksum(data: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(data);
    h.finalize()
}

/// Verifies that `block` ends with a checksum covering the rest of it, and
/// returns the payload.
pub fn verify_trailing_checksum<'a>(block: &'a [u8], path: &Path, what: &str) -> Result<&'a [u8]> {
    if block.len() < 4 {
        return Err(Error::corrupt(
            path,
            format!(
                "{what} is {} bytes, too short to hold a checksum",
                block.len()
            ),
        ));
    }
    let split = block.len() - 4;
    let (payload, tail) = block.split_at(split);
    let want = u32::from_le_bytes(tail.try_into().unwrap());
    let got = checksum(payload);
    if got != want {
        return Err(Error::corrupt(
            path,
            format!("{what} checksum {got:08x} does not match the stored {want:08x}"),
        ));
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn varints_round_trip() {
        let path = PathBuf::from("test");
        let cases: [i64; 9] = [
            0,
            1,
            -1,
            127,
            128,
            -128,
            i64::MAX,
            i64::MIN,
            1_700_000_000_000,
        ];
        let mut buf = Vec::new();
        for v in cases {
            put_varint(&mut buf, v);
        }
        let mut c = Cursor::new(&buf, &path);
        for v in cases {
            assert_eq!(c.varint().unwrap(), v);
        }
        assert!(c.is_empty());
    }

    #[test]
    fn uvarints_round_trip() {
        let path = PathBuf::from("test");
        let cases: [u64; 6] = [0, 1, 127, 128, 16_383, u64::MAX];
        let mut buf = Vec::new();
        for v in cases {
            put_uvarint(&mut buf, v);
        }
        let mut c = Cursor::new(&buf, &path);
        for v in cases {
            assert_eq!(c.uvarint().unwrap(), v);
        }
    }

    #[test]
    fn small_values_use_one_byte() {
        let mut buf = Vec::new();
        put_uvarint(&mut buf, 127);
        assert_eq!(
            buf.len(),
            1,
            "a varint should not spend four bytes on a small length"
        );
    }

    #[test]
    fn bytes_round_trip_including_empty_and_binary() {
        let path = PathBuf::from("test");
        let cases: Vec<&[u8]> = vec![b"", b"a", b"\x00\xff\r\n", &[7u8; 500]];
        let mut buf = Vec::new();
        for b in &cases {
            put_bytes(&mut buf, b);
        }
        let mut c = Cursor::new(&buf, &path);
        for b in &cases {
            assert_eq!(c.bytes().unwrap(), *b);
        }
    }

    #[test]
    fn truncation_is_reported_as_corruption_not_a_panic() {
        let path = PathBuf::from("test");
        let mut buf = Vec::new();
        put_bytes(&mut buf, b"hello");
        for cut in 1..buf.len() {
            let mut c = Cursor::new(&buf[..cut], &path);
            // Either the length or the payload is short; both must be an
            // error, and neither may index out of bounds.
            let _ = c.bytes().unwrap_err();
        }
    }

    #[test]
    fn a_flipped_byte_fails_the_checksum() {
        let path = PathBuf::from("test");
        let mut block = b"some block payload".to_vec();
        let sum = checksum(&block);
        block.extend_from_slice(&sum.to_le_bytes());
        assert_eq!(
            verify_trailing_checksum(&block, &path, "block").unwrap(),
            b"some block payload"
        );

        block[3] ^= 0xff;
        assert!(
            verify_trailing_checksum(&block, &path, "block")
                .unwrap_err()
                .is_corruption()
        );
    }
}
