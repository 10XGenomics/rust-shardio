//! Abstract over different compressors that could be used.
//!
//! For the time being, we support lz4 and zstd, but since we want a given
//! shard file to internally specify which compressor was used, we do not
//! plan to support extensible compressor support. This could be changed in the
//! future through the use of a compile-time plugin registry system like the
//! inventory crate.

use std::io::{Read, Write};

use anyhow::{bail, Result};

/// The compressors supported by shardio.
pub enum Compressor {
    /// lz4 compression at level 2
    /// this appears to be a good general trad-off of speed and compression ratio.
    /// note see these benchmarks for useful numers:
    /// http://quixdb.github.io/squash-benchmark/#ratio-vs-compression
    /// important note: lz4f (not lz4) is the relevant mode in those charts.
    Lz4,
}

impl Compressor {
    const LZ4_ID: &'static [u8; 8] = b"lz4     ";

    /// Return a fixed-width identifier for this compressor, for encoding into a shard file.
    pub fn to_id(&self) -> u64 {
        match self {
            Self::Lz4 => u64::from_be_bytes(*Self::LZ4_ID),
        }
    }

    /// Match a fixed-width identifier to a known compressor.
    pub fn from_id(id: u64) -> Result<Self> {
        match &id.to_be_bytes() {
            Self::LZ4_ID => Ok(Self::Lz4),
            unknown => bail!("unknown compressor: {unknown:?}"),
        }
    }

    /// Return the amount of memory in bytes used by an instance of this decompressor.
    pub fn decompressor_mem_size_bytes(&self) -> usize {
        match self {
            // 32KB of lz4-rs internal buffer
            // + 64KB of lz4-sys default blockSize (x2)
            // + 128KB lz4-sys default blockLinked
            // + 4 lz4-sys checksum
            Self::Lz4 => 32_768 + 65_536 * 2 + 131_072 + 4,
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
                Ok(())
            }
        }
    }

    /// Create a decoder for this compressor, wrapping the provided reader.
    pub fn decoder<R: Read>(&self, r: R) -> Result<Decoder<R>> {
        match self {
            Self::Lz4 => Ok(Decoder::Lz4(lz4::Decoder::new(r)?)),
        }
    }
}

/// Wrap up decoding using different compression strategies.
pub enum Decoder<R: Read> {
    Lz4(lz4::Decoder<R>),
}

impl<R: Read> Read for Decoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Lz4(d) => d.read(buf),
        }
    }
}
