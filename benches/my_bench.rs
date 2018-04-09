

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate criterion;
extern crate tempfile;
extern crate shardio;

use criterion::Criterion;

use shardio::*;

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Debug, PartialOrd, Ord)]
struct T1 {
    a: u64,
    b: u32,
    c: u16,
    d: u8,
}


fn check_round_trip(disk_chunk_size: usize, producer_chunk_size: usize, buffer_size: usize, n_items: usize) {
    
    let tmp = tempfile::NamedTempFile::new().unwrap();

    // Write and close file
    let true_items = {
        let manager: ShardWriter<T1, T1> = ShardWriter::new(tmp.path(), producer_chunk_size, disk_chunk_size, buffer_size);
        let mut true_items = Vec::new();

        // Sender must be closed
        {
            let mut sender = manager.get_sender();

            for i in 0..n_items {
                let tt = T1 {
                    a: ((i/2) + (i*10) % 128 + (i*6) % 64) as u64,
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
    let reader = ShardReader::<T1, T1>::open(tmp.path());

    let mut all_items = Vec::new();
    let mut buf = Vec::new();
    reader.read_range(&Range::all(), &mut all_items, &mut buf);

    if !(true_items == all_items) {
        println!("true len: {:?}", true_items.len());
        println!("round trip len: {:?}", all_items.len());
        assert!(false);
    }
}

fn test_shard_round_trip_big() {
    check_round_trip(256, 32,  1<<12,  1<<14);
}


fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("rt", |b| b.iter(|| test_shard_round_trip_big()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);