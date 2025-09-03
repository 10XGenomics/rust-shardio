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

impl Compressor {
    const LZ4_MAGIC_NUMBER: u32 = 0x184D2204;
    const ZSTD_MAGIC_NUMBER: u32 = 0xFD2FB528;

    /// Return a fixed-width identifier for this compressor, for encoding into a shard file.
    pub fn to_magic_number(&self) -> u32 {
        match self {
            Self::Lz4 => Self::LZ4_MAGIC_NUMBER,
            Self::Zstd => Self::ZSTD_MAGIC_NUMBER,
        }
    }

    /// Match a fixed-width identifier to a known compressor.
    pub fn from_magic_number(id: u32) -> Result<Self> {
        match id {
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
            // Whatever zstd's buffer size is,
            // FIXME there's gotta be more in there
            Self::Zstd => zstd_safe::DCtx::in_size(),
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
                let mut encoder = zstd::Encoder::new(w, -1)?;
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
