#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate criterion;
extern crate bincode;
extern crate failure;
extern crate fxhash;
extern crate lz4;
extern crate shardio;
extern crate tempfile;

use criterion::Criterion;
use failure::Error;
use shardio::*;
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::Hasher;
use std::io::BufWriter;
use std::io::Write;
use std::time::Duration;

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Debug, PartialOrd, Ord)]
struct T1 {
    a: u64,
    b: u32,
    c: u16,
    d: u8,
}

#[macro_use]
extern crate lazy_static;

lazy_static! {
    static ref DATA: Vec<u8> = {
        let size = 1 << 24;
        let mut buf = Vec::with_capacity(size);
        for i in 0..size {
            let mut hasher = fxhash::FxHasher::default();
            hasher.write_usize(i);
            buf.push((hasher.finish() % 8) as u8);
        }
        buf
    };
    static ref D2: Vec<T1> = {
        let size = 1 << 20;
        let mut buf = Vec::with_capacity(size);
        for i in 0..size {
            let tt = T1 {
                a: ((i / 2) + (i * 10) % 128 + (i * 6) % 64) as u64,
                b: i as u32,
                c: (i * 2) as u16,
                d: i as u8,
            };
            buf.push(tt);
        }
        buf
    };
}

fn main() {
    fn check_round_trip(
        disk_chunk_size: usize,
        producer_chunk_size: usize,
        buffer_size: usize,
        n_items: usize,
    ) -> Result<(), Error> {
        let tmp = tempfile::NamedTempFile::new()?;

        // Write and close file
        let true_items = {
            let manager: ShardWriter<T1> = ShardWriter::new(
                tmp.path(),
                producer_chunk_size,
                disk_chunk_size,
                buffer_size,
            )?;
            let mut true_items = Vec::new();

            // Sender must be closed
            {
                let mut sender = manager.get_sender();

                for i in 0..n_items {
                    let tt = T1 {
                        a: ((i / 2) + (i * 10) % 128 + (i * 6) % 64) as u64,
                        b: i as u32,
                        c: (i * 2) as u16,
                        d: i as u8,
                    };
                    sender.send(tt);
                    true_items.push(tt);
                }
            }
            true_items.sort();
            true_items
        };

        // Open finished file
        let reader = ShardReader::<T1>::open(tmp.path())?;

        let mut all_items = Vec::new();
        reader.read_range(&Range::all(), &mut all_items);

        if !(true_items == all_items) {
            println!("true len: {:?}", true_items.len());
            println!("round trip len: {:?}", all_items.len());
            assert!(false);
        }

        Ok(())
    }

    fn test_shard_round_trip_big() {
        check_round_trip(256, 32, 1 << 12, 1 << 14);
    }

    fn benchmark_roundtrip(c: &mut Criterion) {
        c.bench_function("rt", |b| b.iter(|| test_shard_round_trip_big()));
    }

    fn getf() -> impl Write {
        BufWriter::new(File::create("test1").unwrap())
    }

    fn direct(size: usize) -> Result<(), Error> {
        let mut tf = getf();
        tf.write(&DATA[0..size]);
        Ok(())
    }

    fn lz4_only(size: usize) -> Result<(), Error> {
        let mut tf = getf();
        let mut fo = lz4::EncoderBuilder::new().build(tf)?;
        fo.write(&DATA[0..size]);
        fo.finish().1?;
        Ok(())
    }

    fn bincode_only(size: usize) -> Result<(), Error> {
        let mut tf = getf();
        let n = size / std::mem::size_of::<T1>();
        bincode::serialize_into(tf, &D2[0..n]);
        Ok(())
    }

    fn bincode_lz4(size: usize) -> Result<(), Error> {
        let tf = tempfile::tempfile()?;
        let fo = lz4::EncoderBuilder::new().build(tf)?;
        let n = size / std::mem::size_of::<T1>();
        bincode::serialize_into(fo, &D2[0..n]);
        Ok(())
    }

    fn bincode_buf_lz4(size: usize) -> Result<(), Error> {
        let tf = tempfile::tempfile()?;
        let mut fo = lz4::EncoderBuilder::new().build(tf)?;
        let n = size / std::mem::size_of::<T1>();
        let mut buf = Vec::new();
        bincode::serialize_into(&mut buf, &D2[0..n]);

        fo.write(&buf);
        Ok(())
    }

    const KB: usize = 1024;
    static v: [usize; 4] = [256 * KB, 512 * KB, 1024 * KB, 2048 * KB];

    let mut crit = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2))
        .sample_size(20)
        .noise_threshold(0.15);

    crit.bench_function_over_inputs(
        "direct",
        |b, &&size| {
            b.iter(|| direct(size));
        },
        &v,
    ).bench_function_over_inputs(
            "lz4_only",
            |b, &&size| {
                b.iter(|| lz4_only(size));
            },
            &v,
        )
        .bench_function_over_inputs(
            "bincode_only",
            |b, &&size| {
                b.iter(|| bincode_only(size));
            },
            &v,
        )
        .bench_function_over_inputs(
            "bincode_lz4",
            |b, &&size| {
                b.iter(|| bincode_lz4(size));
            },
            &v,
        )
        .bench_function_over_inputs(
            "bincode_buf_lz4",
            |b, &&size| {
                b.iter(|| bincode_buf_lz4(size));
            },
            &v,
        );
    //criterion_group!(benches, criterion_benchmark);
    //criterion_main!(benches);
}
