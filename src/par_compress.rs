//! shardio buffer handler than uses a thread pool to serialize and compress.

use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;

use anyhow::Context;
use bincode::serialize_into;
use pariter::IteratorExt;
use serde::Serialize;

use crate::{write_index, Compressor, ShardRecord, SortKey, INITIAL_WRITE_CURSOR_OFFSET};

/// Process an iterator of sorted buffers into a shard file.
///
/// Each buffer is split up into chunks of maximum size chunk_size.
/// Serialization/compression are delegated to a thread pool of size worker_count.
///
/// The function returns when the input iterator has been fully consumed, all
/// chunks have been written, the shard index has been written, and the file
/// closed.
pub fn process_sorted_bufs<T, S>(
    sorted_buf_iter: impl Iterator<Item = Vec<T>> + 'static,
    chunk_size: usize,
    worker_count: usize,
    compressor: Compressor,
    path: &Path,
) -> anyhow::Result<()>
where
    T: Send + Sync + Serialize + 'static,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone + Serialize + Send + 'static,
{
    assert!(chunk_size > 0);

    let mut file = File::create(path).with_context(|| path.to_string_lossy().to_string())?;

    let mut shard_index = vec![];

    // Current write location in the file.
    let mut cursor = INITIAL_WRITE_CURSOR_OFFSET;

    // NOTE: parallel_map_custom clones this for each worker thread.
    // We actually have worker_count number of these.
    let mut serialize_buf = vec![];

    // A cross-thread memory pool of buffers to write compressed data into.
    let compress_pool = MemPool::new(1 + worker_count * 2, Vec::new);

    for result in sorted_buf_iter
        .flat_map(move |buf| {
            // Break up the input buffer into chunks by ranges, sharing access
            // to the input via Arc.
            let buf_len = buf.len();
            let chunks = (0..buf_len)
                .step_by(chunk_size)
                .map(move |start| start..(start + chunk_size).min(buf_len));
            let wrapped = Arc::new(buf);
            chunks.map(move |chunk_range| (wrapped.clone(), chunk_range))
        })
        // Inject the buffer to use for compression of this chunk.
        .map(move |(buf, chunk_range)| (buf, chunk_range, compress_pool.fetch()))
        // Spread serialization/compression across worker pool.
        // This preserves order of the input chunks.
        .parallel_map_custom(
            |o| o.threads(worker_count),
            move |(buf, chunk_range, mut compress_buf)| {
                let items = &buf[chunk_range];

                let bounds = (
                    S::sort_key(&items[0]).into_owned(),
                    S::sort_key(&items[items.len() - 1]).into_owned(),
                );

                serialize_buf.clear();
                compress_buf.clear();
                for item in items {
                    serialize_into(&mut serialize_buf, item)?;
                }
                compressor.encode(&serialize_buf, compress_buf.deref_mut())?;

                let reg = ShardRecord {
                    offset: 0, // this will be set when processed by the writer below
                    start_key: bounds.0,
                    end_key: bounds.1,
                    len_bytes: compress_buf.len(),
                    len_items: items.len(),
                };

                anyhow::Ok((compress_buf, reg))
            },
        )
    {
        // Write the compressed buffer into the file, and update the index.
        let (compressed_buf, mut shard_record) = result?;
        shard_record.offset = cursor;
        shard_index.push(shard_record);
        let cur_offset = cursor;
        cursor += compressed_buf.len();
        file.write_all_at(&compressed_buf, cur_offset as u64)?;
    }
    // Finalize by writing the shard index into the file.
    write_index::<T, S, <S as SortKey<T>>::Key>(&mut file, cursor, &shard_index, compressor)?;
    Ok(())
}

/// A thread-safe, fixed-size pool of T.
/// This type can be used to provide reusable allocations that can be passed
/// between threads, such as in-memory buffers.
///
/// The pool will never allocate more than the specified number of members,
/// and requesting a member from the pool will block until one is available.
struct MemPool<T: Send> {
    recv: Receiver<T>,
    load: SyncSender<T>,
}

impl<T: Send> MemPool<T> {
    /// Initialize a new pool of size count.
    /// Calls init repeatedly to produce the items.
    pub fn new<F: FnMut() -> T>(count: usize, mut init: F) -> Self {
        let (load, recv) = sync_channel(count);
        for _ in 0..count {
            load.send(init()).unwrap();
        }
        Self { recv, load }
    }

    /// Fetch a member from the pool, blocking until one is available.
    pub fn fetch(&self) -> PoolMember<T> {
        PoolMember {
            val: Some(self.recv.recv().unwrap()),
            return_to_pool: self.load.clone(),
        }
    }
}

/// A value of type T that will be returned to the source memory pool when dropped.
struct PoolMember<T: Send> {
    val: Option<T>,
    return_to_pool: SyncSender<T>,
}

impl<T: Send> Drop for PoolMember<T> {
    fn drop(&mut self) {
        // This can only fail if the entire pool itself has been dropped.
        let _ = self.return_to_pool.send(self.val.take().unwrap());
    }
}

impl<T: Send> Deref for PoolMember<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.val.as_ref().unwrap()
    }
}

impl<T: Send> DerefMut for PoolMember<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.val.as_mut().unwrap()
    }
}
