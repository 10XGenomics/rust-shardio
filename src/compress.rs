//! Abstract over different compressors that could be used.
//!
//! For the time being, we support lz4 and zstd, but since we want a given
//! shard file to internally specify which compressor was used, we do not
//! plan to support extensible compressor support. This could be changed in the
//! future through the use of a compile-time plugin registry system like the
//! inventory crate.

use std::io::{BufReader, Read, Write};

use anyhow::{bail, Result};
use zstd::zstd_safe;

/// The compressors supported by shardio.
#[derive(Clone, Copy)]
pub enum Compressor {
    /// lz4 compression at level 2
    /// this appears to be a good general trade-off of speed and compression ratio.
    /// note see these benchmarks for useful numers:
    /// http://quixdb.github.io/squash-benchmark/#ratio-vs-compression
    /// important note: lz4f (not lz4) is the relevant mode in those charts.
    Lz4,
    /// zstd compression using --fast=1
    /// Roughly the same speed as lz4 level 2 but possibly 40% smaller for certain
    /// workloads.
    Zstd,
}

pub type MagicNumber = [u8; 4];

impl Compressor {
    const LZ4_MAGIC_NUMBER: &'static MagicNumber = b"\x18\x4D\x22\x04";
    const ZSTD_MAGIC_NUMBER: &'static MagicNumber = b"\xFD\x2F\xB5\x28";
    const ZSTD_COMPRESSION_LEVEL: i32 = -1;

    /// Return a fixed-width identifier for this compressor, for encoding into a shard file.
    pub fn to_magic_number(&self) -> MagicNumber {
        match self {
            Self::Lz4 => *Self::LZ4_MAGIC_NUMBER,
            Self::Zstd => *Self::ZSTD_MAGIC_NUMBER,
        }
    }

    /// Read a magic number from a reader and match it to a known compressor.
    pub fn from_magic_number(mut r: impl Read) -> Result<Self> {
        let mut id = [0u8; 4];
        r.read_exact(&mut id)?;
        match &id {
            Self::LZ4_MAGIC_NUMBER => Ok(Self::Lz4),
            Self::ZSTD_MAGIC_NUMBER => Ok(Self::Zstd),
            unknown => bail!("unknown compressor: {unknown:?}"),
        }
    }

    /// Return the amount of memory in bytes used by an instance of this decompressor.
    pub fn decompressor_mem_size_bytes(&self) -> usize {
        match self {
            // 8KB BufReader
            // + 32KB of lz4-rs internal buffer
            // + 64KB of lz4-sys default blockSize (x2)
            // + 128KB lz4-sys default blockLinked
            // + 4 lz4-sys checksum
            Self::Lz4 => 8192 + 32_768 + 65_536 * 2 + 131_072 + 4,
            Self::Zstd => {
                // Safety: only unsafe due to FFI
                let window_log = unsafe {
                    zstd_sys::ZSTD_getCParams(Self::ZSTD_COMPRESSION_LEVEL, 0, 0).windowLog as u32
                };
                let window_size = 1usize << window_log;
                // Safety: only unsafe due to FFI
                let zstd_internal_size = unsafe { zstd_sys::ZSTD_estimateDStreamSize(window_size) };
                let rust_read_buf_size = zstd_safe::DCtx::in_size();
                zstd_internal_size + rust_read_buf_size
            }
        }
    }

    /// Encode a buffer of data using this compression strategy.
    /// Write it to the provided writer.
    pub fn encode(&self, data: &[u8], w: impl Write) -> Result<()> {
        match self {
            Self::Lz4 => {
                let mut encoder = lz4::EncoderBuilder::new().level(2).build(w)?;
                encoder.write_all(data)?;
                let (_, result) = encoder.finish();
                result?;
            }
            Self::Zstd => {
                let mut encoder = zstd::Encoder::new(w, Self::ZSTD_COMPRESSION_LEVEL)?;
                encoder.write_all(data)?;
                encoder.finish()?;
            }
        }
        Ok(())
    }

    /// Create a decoder for this compressor, wrapping the provided reader.
    /// This decoder should handle input buffering.
    pub fn decoder<R: Read>(&self, r: R) -> Result<Decoder<R>> {
        Ok(match self {
            Self::Lz4 => Decoder::Lz4(lz4::Decoder::new(BufReader::new(r))?),
            Self::Zstd => Decoder::Zstd(zstd::Decoder::new(r)?),
        })
    }
}

/// Wrap up decoding using different compression strategies.
pub enum Decoder<R: Read> {
    Lz4(lz4::Decoder<BufReader<R>>),
    Zstd(zstd::Decoder<'static, BufReader<R>>),
}

impl<R: Read> Read for Decoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Lz4(d) => d.read(buf),
            Self::Zstd(d) => d.read(buf),
        }
    }
}

impl<R: Read> Decoder<R> {
    /// Finish the current decode and retrieve the inner reader and the compressor.
    pub fn finish(self) -> (R, Compressor) {
        match self {
            Self::Lz4(d) => {
                // The only error condition possible here is if we didn't finish
                // reading all of the data from the compressed stream, but that
                // is most likely intended behavior, since we may be done reading
                // items from a shard partway through.
                let (r, _result) = d.finish();
                (r.into_inner(), Compressor::Lz4)
            }
            Self::Zstd(d) => (d.finish().into_inner(), Compressor::Zstd),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Compressor;

    /// This test mostly serves as documentation for the actual memory overhead
    /// of the two compressors.
    #[test]
    fn test_decompressor_mem_size() {
        assert_eq!(1144627, Compressor::Zstd.decompressor_mem_size_bytes());
        assert_eq!(303108, Compressor::Lz4.decompressor_mem_size_bytes());
    }
}
