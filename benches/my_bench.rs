use std::hash::Hasher;

use std::time::Duration;

use anyhow::Error;
use criterion::Criterion;
use serde::{Deserialize, Serialize};
use shardio::{Compressor, ShardReader, ShardWriter, UnsortedShardReader};

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Debug, PartialOrd, Ord)]
struct T1 {
    a: u64,
    b: u32,
    c: u16,
    d: u8,
    e: u64,
    f: u128,
    g: u128,
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
                e: (i as u64) * 100,
                f: (i as u128) * 123 + 100,
                g: (i as u128) * 100123 + 123123412,
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
        unsorted_read: bool,
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
            let mut true_items = Vec::with_capacity(n_items);

            // Sender must be closed
            {
                let mut sender = manager.get_sender();

                for i in 0..n_items {
                    let tt = T1 {
                        a: ((i / 2) + (i * 10) % 128 + (i * 6) % 64) as u64,
                        b: i as u32,
                        c: (i * 2) as u16,
                        d: i as u8,
                        e: (i as u64) * 100,
                        f: (i as u128) * 123 + 100,
                        g: (i as u128) * 100123 + 123123412,
                    };
                    sender.send(tt).expect("failed to send");
                    true_items.push(tt);
                }
            }
            //true_items.sort();
            true_items
        };

        // Open finished file
        let all_items = if unsorted_read {
            UnsortedShardReader::<T1>::open(tmp.path()).collect::<Result<_, _>>()?
        } else {
            let reader = ShardReader::<T1>::open(tmp.path())?;

            let mut items: Vec<T1> = vec![];
            for r in reader.iter()? {
                items.push(r?);
            }
            items
        };

        if true_items.len() != all_items.len() {
            println!("true len: {:?}", true_items.len());
            println!("round trip len: {:?}", all_items.len());
            panic!("fail");
        }

        Ok(())
    }

    fn test_shard_round_trip_big(unsorted_read: bool) {
        check_round_trip(2048, 32, 1 << 13, 1 << 18, unsorted_read).unwrap();
    }

    let mut crit = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(15))
        .sample_size(10)
        .noise_threshold(0.1);

    crit.bench_function("round-trip", |b| {
        b.iter(|| test_shard_round_trip_big(false))
    });

    crit.bench_function("round-trip-unsorted", |b| {
        b.iter(|| test_shard_round_trip_big(true))
    });

    //criterion_group!(benches, criterion_benchmark);
    //criterion_main!(benches);
}
