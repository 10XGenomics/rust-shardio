// Copyright (c) 2018 10x Genomics, Inc. All rights reserved.

//! Serialize large streams of `Serialize`-able structs to disk from multiple threads, with a customizable on-disk sort order.
//! Data is written to sorted chunks. When reading shardio will merge the data on the fly into a single sorted view. You can
//! also procss disjoint subsets of sorted data.
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use shardio::*;
//! use std::fs::File;
//! use failure::Error;
//!
//! #[derive(Clone, Eq, PartialEq, Serialize, Deserialize, PartialOrd, Ord, Debug)]
//! struct DataStruct {
//!     a: u64,
//!     b: u32,
//! }
//!
//! fn main() -> Result<(), Error>
//! {
//!     let filename = "test.shardio";
//!     {
//!         // Open a shardio output file
//!         // Parameters here control buffering, and the size of disk chunks
//!         // which affect how many disk chunks need to be read to
//!         // satisfy a range query when reading.
//!         // In this example the 'built-in' sort order given by #[derive(Ord)]
//!         // is used.
//!         let mut writer: ShardWriter<DataStruct> =
//!             ShardWriter::new(filename, 64, 256, 1<<16)?;
//!
//!         // Get a handle to send data to the file
//!         let mut sender = writer.get_sender();
//!
//!         // Generate some test data
//!         for i in 0..(2 << 16) {
//!             sender.send(DataStruct { a: (i%25) as u64, b: (i%100) as u32 });
//!         }
//!
//!         // done sending items
//!         sender.finished();
//!
//!         // Write errors are accessible by calling the finish() method
//!         writer.finish()?;
//!     }
//!
//!     // Open finished file & test chunked reads
//!     let reader = ShardReader::<DataStruct>::open(filename)?;
//!
//!     
//!     let mut all_items = Vec::new();
//!
//!     // Shardio will divide the key space into 5 roughly equally sized chunks.
//!     // These chunks can be processed serially, in parallel in different threads,
//!     // or on different machines.
//!     let chunks = reader.make_chunks(5, &Range::all());
//!
//!     for c in chunks {
//!         // Iterate over all the data in chunk c.
//!         let mut range_iter = reader.iter_range(&c)?;
//!         for i in range_iter {
//!             all_items.push(i?);
//!         }
//!     }
//!
//!     // Data will be return in sorted order
//!     let mut all_items_sorted = all_items.clone();
//!     all_items.sort();
//!     assert_eq!(all_items, all_items_sorted);
//!     std::fs::remove_file(filename)?;
//!     Ok(())
//! }
//! ```

#![deny(warnings)]
#![deny(missing_docs)]
use std::any::type_name;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, Seek, SeekFrom};
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use std::thread;

use bincode::serialize_into;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use failure::{format_err, Error};
use log::warn;
use lz4;
use min_max_heap::MinMaxHeap;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Represent a range of key space
pub mod range;

/// Helper methods
pub mod helper;

pub use crate::range::Range;
use range::Rorder;

/// The size (in bytes) of a ShardIter object (mostly buffers)
// ? sizeof(T)
// + 8 usize items_remaining
// + 8 &bincode::Config
// + 8KB BufReader
// + 32KB of lz4-rs internal buffer
// + 64KB of lz4-sys default blockSize (x2)
// + 128KB lz4-sys default blockLinked
// + 4 lz4-sys checksum
pub const SHARD_ITER_SZ: usize = 8 + 8 + 8_192 + 32_768 + 65_536 * 2 + 131_072 + 4;

// this number is chosen such that, for the expected size of a ShardIter above,
//   represents at least 1GiB of memory (1024^3 / 303124 =~ 3542.3)
const WARN_ACTIVE_SHARDS: usize = 3_543;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
/// A group of `len_items` items, from shard `shard`, stored at position `offset`, using `block_size` bytes on-disk,
/// with sort keys covering the interval [`start_key`, `end_key`]
struct ShardRecord<K> {
    start_key: K,
    end_key: K,
    offset: usize,
    len_bytes: usize,
    len_items: usize,
}

/// Specify a key function from data items of type `T` to a sort key of type `Key`.
/// Impelment this trait to create a custom sort order.
/// The function `sort_key` returns a `Cow` so that we abstract over Owned or
/// Borrowed data.
/// ```rust
///
/// extern crate shardio;
/// use shardio::*;
/// use std::borrow::Cow;
///
/// // The default sort order for DataStruct will be by field1.
/// #[derive(Ord, PartialOrd, Eq, PartialEq)]
/// struct DataStruct {
///     field1: usize,
///     field2: String,
/// }
///
/// // Define a marker struct for your new sort order
/// struct Field2SortKey;
///
/// // Define the new sort key by extracting field2 from DataStruct
/// impl SortKey<DataStruct> for Field2SortKey {
///     type Key = String;
///
///     fn sort_key(t: &DataStruct) -> Cow<String> {
///         Cow::Borrowed(&t.field2)
///     }
/// }
/// ```
pub trait SortKey<T> {
    /// The type of the key that will be sorted.
    type Key: Ord + Clone;

    /// Compute the sort key for value `t`.
    fn sort_key(t: &T) -> Cow<'_, Self::Key>;
}

/// Marker struct for sorting types that implement `Ord` in the order defined by their `Ord` impl.
/// This sort order is used by default when writing, unless an alternative sort order is provided.
pub struct DefaultSort;
impl<T> SortKey<T> for DefaultSort
where
    T: Ord + Clone,
{
    type Key = T;
    fn sort_key(t: &T) -> Cow<'_, T> {
        Cow::Borrowed(t)
    }
}

/// Write a stream data items of type `T` to disk, in the sort order defined by `S`.
///
/// Data is buffered up to `item_buffer_size` items, then sorted, block compressed and written to disk.
/// When the ShardWriter is dropped or has `finish()` called on it, it will
/// flush remaining items to disk, write an index of the chunk data and close the file.
///
/// The `get_sender()` methods returns a `ShardSender` that must be used to send items to the writer.
/// You must close each ShardSender by dropping it or calling its `finish()` method, or data may be lost.
/// The `ShardSender` must be dropped/finished prior to callinng `SharWriter::finish` or dropping the shard writer.
///
/// # Sorting
/// Items are sorted according to the `Ord` implementation of type `S::Key`. Type `S`, implementing the `SortKey` trait
/// maps items of type `T` to their sort key of type `S::Key`. By default the sort key is the data item itself, and the
/// the `DefaultSort` implementation of `SortKey` is the identity function.
pub struct ShardWriter<T, S = DefaultSort>
where
    T: 'static + Send + Serialize,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: 'static + Send + Ord + Serialize + Clone,
{
    inner: Option<Arc<ShardWriterInner<T, S>>>,
    sort: PhantomData<S>,
}

impl<T, S> ShardWriter<T, S>
where
    T: 'static + Send + Serialize,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: 'static + Send + Ord + Serialize + Clone,
{
    /// Create a writer for storing data items of type `T`.
    /// # Arguments
    /// * `path` - Path to newly created output file
    /// * `sender_buffer_size` - number of items to buffer on the sending thread before transferring data to the writer
    /// * `disk_chunk_size` - number of items to store in chunk on disk. Controls the tradeoff between indexing overhead and the granularity
    ///                       can be read at.
    /// * `item_buffer_size` - number of items to buffer before writing data to disk. More buffering causes the data for a given interval to be
    ///                        spread over fewer disk chunks, but requires more memory.
    pub fn new<P: AsRef<Path>>(
        path: P,
        sender_buffer_size: usize,
        disk_chunk_size: usize,
        item_buffer_size: usize,
    ) -> Result<ShardWriter<T, S>, Error> {
        assert!(disk_chunk_size >= 1);
        assert!(item_buffer_size >= 1);
        assert!(sender_buffer_size >= 1);
        assert!(item_buffer_size >= sender_buffer_size);

        let inner =
            ShardWriterInner::new(disk_chunk_size, item_buffer_size, sender_buffer_size, path)?;

        Ok(ShardWriter {
            inner: Some(Arc::new(inner)),
            sort: PhantomData,
        })
    }

    /// Get a `ShardSender`. It can be sent to another thread that is generating data.
    pub fn get_sender(&self) -> ShardSender<T, S> {
        ShardSender::new(self.inner.as_ref().unwrap().clone())
    }

    /// Call finish if you want to detect errors in the writer IO.
    pub fn finish(&mut self) -> Result<usize, Error> {
        if let Some(inner_arc) = self.inner.take() {
            match Arc::try_unwrap(inner_arc) {
                Ok(inner) => inner.close(),
                Err(_) => {
                    if !std::thread::panicking() {
                        panic!("ShardSenders are still active. They must all be out of scope or finished() before ShardWriter is closed");
                    } else {
                        return Ok(0);
                    }
                }
            }
        } else {
            Ok(0)
        }
    }
}

impl<T, S> Drop for ShardWriter<T, S>
where
    S: SortKey<T>,
    <S as SortKey<T>>::Key: 'static + Send + Ord + Serialize + Clone,
    T: Send + Serialize,
{
    fn drop(&mut self) {
        let _e = self.finish();
    }
}

/// Coordinate a pair of buffers that need to added to from multiple threads and processed/flushed when full.
/// Coordinates acces to `buffer_state`, passing full buffers to `handler` when they are full.
struct BufferStateMachine<T, H> {
    sender_buffer_size: usize,
    buffer_state: Mutex<BufStates<T>>,
    handler: Mutex<H>,
    closed: AtomicBool,
}

/// There are always two item buffers held by a ShardWriter. A buffer can be in the process of being filled,
/// waiting for use, or being processed & written to disk.  In the first two states, the buffer will be held
/// by this enum.  In the processing state it will be held on the stack of the thread processing the buffer.
#[derive(PartialEq, Eq)]
enum BufStates<T> {
    /// Fill the first buffer, using the second as backup
    FillAndWait(Vec<T>, Vec<T>),
    /// Fill the buffer, the other buffer is being written out
    FillAndBusy(Vec<T>),
    /// Both buffers are busy, try again later
    BothBusy,
    /// Placeholder variable, should never be observed.
    Dummy,
}

/// Outcome of trying to add items to the buffer.
enum BufAddOutcome<T> {
    /// Items added successfully
    Done,
    /// No free buffers, operation must be re-tried
    Retry,
    /// Items added, but a buffer was filled and must be processed.
    /// Buffer to be processed is attached.
    Process(Vec<T>),
}

use BufAddOutcome::*;
use BufStates::*;

use std::fmt;

impl<T> fmt::Debug for BufStates<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BufStates::FillAndWait(a, b) => write!(f, "FillAndWait({}, {})", a.len(), b.len()),
            BufStates::FillAndBusy(a) => write!(f, "FillAndBusy({})", a.len()),
            BufStates::BothBusy => write!(f, "BothBusy"),
            BufStates::Dummy => write!(f, "Dummy"),
        }
    }
}

impl<T, H: BufHandler<T>> BufferStateMachine<T, H> {
    /// Move `items` into the buffer & it. If a buffer is filled by this operation
    /// then the calling thread will be used to process the full buffer.
    fn add_items(&self, items: &mut Vec<T>) -> Result<(), Error> {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            panic!("tried to add items to ShardSender after ShardWriter was closed");
        }

        loop {
            // get mutex on buffers
            let mut buffer_state = self.buffer_state.lock().unwrap();
            let mut current_state = Dummy;
            std::mem::swap(buffer_state.deref_mut(), &mut current_state);

            // determine new state and any follow-up work
            let (mut new_state, outcome) = match current_state {
                FillAndWait(mut f, w) => {
                    f.extend(items.drain(..));
                    if f.len() + self.sender_buffer_size >= f.capacity() {
                        (FillAndBusy(w), Process(f))
                    } else {
                        (FillAndWait(f, w), Done)
                    }
                }
                FillAndBusy(mut f) => {
                    f.extend(items.drain(..));
                    if f.len() + self.sender_buffer_size >= f.capacity() {
                        (BothBusy, Process(f))
                    } else {
                        (FillAndBusy(f), Done)
                    }
                }
                BothBusy => (BothBusy, Retry),
                Dummy => unreachable!(),
            };

            // Fill in the new state.
            std::mem::swap(buffer_state.deref_mut(), &mut new_state);

            // drop the mutex
            drop(buffer_state);

            // outcome could be to process a full vec, retry, or be done.
            match outcome {
                Process(mut buf_to_process) => {
                    // process the buffer & return it to the pool.
                    self.process_buffer(&mut buf_to_process)?;
                    self.return_buffer(buf_to_process);
                    break;
                }
                Retry => {
                    // take a break before trying again.
                    thread::yield_now();
                    continue;
                }
                Done => break,
            }
        }

        Ok(())
    }

    /// shut down the buffer machinery, processing any remaining items.
    /// calls to `add_items` after `close` will panic.
    pub fn close(self) -> Result<H, Error> {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        let mut bufs_processed = 0;

        loop {
            if bufs_processed == 2 {
                break;
            }

            let mut buffer_state = self.buffer_state.lock().unwrap();
            let mut current_state = BufStates::Dummy;
            std::mem::swap(buffer_state.deref_mut(), &mut current_state);

            let (mut new_state, to_process) = match current_state {
                BufStates::FillAndWait(f, w) => (BufStates::FillAndBusy(w), Some(f)),
                BufStates::FillAndBusy(f) => (BufStates::BothBusy, Some(f)),
                // if both buffers are busy, we yield and try again.
                BufStates::BothBusy => {
                    thread::yield_now();
                    continue;
                }
                BufStates::Dummy => unreachable!(),
            };

            // fill in the new state.
            std::mem::swap(buffer_state.deref_mut(), &mut new_state);

            if let Some(mut buf) = to_process {
                bufs_processed += 1;
                self.process_buffer(&mut buf)?;
            } else {
                unreachable!();
            }
        }

        Ok(self.handler.into_inner().unwrap())
    }

    /// put a buffer back into service after processing
    fn return_buffer(&self, buf: Vec<T>) {
        let mut buffer_state = self.buffer_state.lock().unwrap();
        let mut current_state = BufStates::Dummy;
        std::mem::swap(buffer_state.deref_mut(), &mut current_state);

        let mut new_state = match current_state {
            BufStates::FillAndWait(_, _) => panic!("there are 3 buffers in use!!"),
            BufStates::FillAndBusy(f) => BufStates::FillAndWait(f, buf),
            BufStates::BothBusy => BufStates::FillAndBusy(buf),
            BufStates::Dummy => unreachable!(),
        };

        std::mem::swap(buffer_state.deref_mut(), &mut new_state);
    }

    /// process `buf`
    pub fn process_buffer(&self, buf: &mut Vec<T>) -> Result<(), Error> {
        // prepare the buffer for process - this doesn't require
        // a lock on handler
        H::prepare_buf(buf);

        // process the buf -- this requires the handler mutex
        let mut handler = self.handler.lock().unwrap();
        handler.process_buf(buf)?;
        buf.clear();
        Ok(())
    }
}

/// Two-part handler for a buffer needing processing.
/// `prepare_buf` is generic and needs no state. Used for sorting.
/// `process_buf` needs exclusive access to the handler, so can do IO.
trait BufHandler<T> {
    fn prepare_buf(v: &mut Vec<T>);
    fn process_buf(&mut self, v: &mut Vec<T>) -> Result<(), Error>;
}

struct SortAndWriteHandler<T, S>
where
    T: Send + Serialize,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone + Serialize,
{
    // Current start position of next chunk
    cursor: usize,
    // Record of chunks written
    regions: Vec<ShardRecord<<S as SortKey<T>>::Key>>,
    // File handle
    file: File,
    // Size of disk chunks
    chunk_size: usize,
    // Buffers for use when writing
    serialize_buffer: Vec<u8>,
    compress_buffer: Vec<u8>,
}

impl<T, S> BufHandler<T> for SortAndWriteHandler<T, S>
where
    T: Send + Serialize,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone + Serialize,
{
    /// Sort items according to `S`.
    fn prepare_buf(buf: &mut Vec<T>) {
        buf.sort_by(|x, y| S::sort_key(x).cmp(&S::sort_key(y)));
    }

    /// Write items to disk chunks
    fn process_buf(&mut self, buf: &mut Vec<T>) -> Result<(), Error> {
        // Write out the buffer chunks
        for c in buf.chunks(self.chunk_size) {
            self.write_chunk(c)?;
        }

        Ok(())
    }
}

impl<T, S> SortAndWriteHandler<T, S>
where
    T: Send + Serialize,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone + Serialize,
{
    pub fn new<P: AsRef<Path>>(
        chunk_size: usize,
        path: P,
    ) -> Result<SortAndWriteHandler<T, S>, Error> {
        let file = File::create(path)?;

        Ok(SortAndWriteHandler {
            cursor: 4096,
            regions: Vec::new(),
            file,
            serialize_buffer: Vec::new(),
            compress_buffer: Vec::new(),
            chunk_size,
        })

        // FIXME: write a magic string to the start of the file,
        // and maybe some metadata about the types being stored and the
        // compression scheme.
        // FIXME: write to a TempFile that will be destroyed unless
        // writing completes successfully.
    }

    fn write_chunk(&mut self, items: &[T]) -> Result<usize, Error> {
        self.serialize_buffer.clear();
        self.compress_buffer.clear();

        assert!(items.len() > 0);

        let bounds = (
            S::sort_key(&items[0]).into_owned(),
            S::sort_key(&items[items.len() - 1]).into_owned(),
        );

        for item in items {
            serialize_into(&mut self.serialize_buffer, item)?;
        }

        {
            use std::io::Write;
            let mut encoder = lz4::EncoderBuilder::new()
                .level(2) // this appears to be a good general trad-off of speed and compression ratio.
                          // note see these benchmarks for useful numers:
                          // http://quixdb.github.io/squash-benchmark/#ratio-vs-compression
                          // important note: lz4f (not lz4) is the relevant mode in those charts.
                .build(&mut self.compress_buffer)?;

            encoder.write(&self.serialize_buffer)?;
            let (_, result) = encoder.finish();
            result?;
        }

        let cur_offset = self.cursor;
        let reg = ShardRecord {
            offset: self.cursor,
            start_key: bounds.0,
            end_key: bounds.1,
            len_bytes: self.compress_buffer.len(),
            len_items: items.len(),
        };

        self.regions.push(reg);
        self.cursor += self.compress_buffer.len();
        let l = self
            .file
            .write_at(&self.compress_buffer, cur_offset as u64)?;

        Ok(l)
    }

    /// Write out the shard positioning data
    fn write_index(&mut self) -> Result<(), Error> {
        let mut buf = Vec::new();

        serialize_into(&mut buf, &(type_name::<T>(), type_name::<S>()))?;
        serialize_into(&mut buf, &self.regions)?;

        let index_block_position = self.cursor;
        let index_block_size = buf.len();

        self.file
            .write_at(buf.as_slice(), index_block_position as u64)?;

        self.file.seek(SeekFrom::Start(
            (index_block_position + index_block_size) as u64,
        ))?;
        self.file.write_u64::<BigEndian>(0 as u64)?;
        self.file
            .write_u64::<BigEndian>(index_block_position as u64)?;
        self.file.write_u64::<BigEndian>(index_block_size as u64)?;
        Ok(())
    }
}

/// Sort buffered items, break large buffer into chunks.
/// Serialize, compress and write the each chunk. The
/// file manager maintains the index data.
struct ShardWriterInner<T, S>
where
    T: Send + Serialize,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone + Serialize,
{
    sender_buffer_size: usize,
    state_machine: BufferStateMachine<T, SortAndWriteHandler<T, S>>,
}

impl<T, S> ShardWriterInner<T, S>
where
    T: Send + Serialize,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone + Serialize,
{
    fn new(
        chunk_size: usize,
        buf_size: usize,
        sender_buffer_size: usize,
        path: impl AsRef<Path>,
    ) -> Result<ShardWriterInner<T, S>, Error> {
        let handler = SortAndWriteHandler::new(chunk_size, path)?;

        let bufs = BufStates::FillAndWait(
            Vec::with_capacity(buf_size / 2),
            Vec::with_capacity(buf_size / 2),
        );

        let state_machine = BufferStateMachine {
            buffer_state: Mutex::new(bufs),
            handler: Mutex::new(handler),
            closed: AtomicBool::new(false),
            sender_buffer_size: sender_buffer_size,
        };

        Ok(ShardWriterInner {
            sender_buffer_size,
            state_machine,
        })
    }

    fn send_items(&self, items: &mut Vec<T>) -> Result<(), Error> {
        self.state_machine.add_items(items)
    }

    fn close(self) -> Result<usize, Error> {
        let mut handler = self.state_machine.close()?;
        let nitems = handler.regions.iter().map(|r| r.len_items).sum();
        handler.write_index()?;
        Ok(nitems)
    }
}

/// A handle that is used to send data to a `ShardWriter`. Each thread that is producing data
/// needs it's own ShardSender. A `ShardSender` can be obtained with the `get_sender` method of
/// `ShardWriter`.  ShardSender implement clone.
pub struct ShardSender<T, S = DefaultSort>
where
    T: Send + Serialize,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone + Serialize,
{
    writer: Option<Arc<ShardWriterInner<T, S>>>,
    buffer: Vec<T>,
    buf_size: usize,
}

impl<T: Send, S> ShardSender<T, S>
where
    T: Send + Serialize,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone + Serialize,
{
    fn new(writer: Arc<ShardWriterInner<T, S>>) -> ShardSender<T, S> {
        let buffer = Vec::with_capacity(writer.sender_buffer_size);
        let buf_size = writer.sender_buffer_size;

        ShardSender {
            writer: Some(writer),
            buffer,
            buf_size,
        }
    }

    /// Send an item to the shard file
    pub fn send(&mut self, item: T) -> Result<(), Error> {
        let send = {
            self.buffer.push(item);
            self.buffer.len() == self.buf_size
        };

        if send {
            self.writer
                .as_ref()
                .expect("ShardSender is already closed")
                .send_items(&mut self.buffer)?;
        }

        Ok(())
    }

    /// Signal that you've finished sending items to this `ShardSender`. `finished` will called
    /// if the `ShardSender` is dropped. You must call `finished()` or drop the `ShardSender`
    /// prior to calling `ShardWriter::finish` or dropping the ShardWriter, or you will get a panic.
    pub fn finished(&mut self) -> Result<(), Error> {
        // send any remaining items and drop the Arc<ShardWriterInner>
        if let Some(inner) = self.writer.take() {
            if self.buffer.len() > 0 {
                inner.send_items(&mut self.buffer)?;
            }
        }

        assert!(self.writer.is_none());
        Ok(())
    }
}

impl<T, S> Clone for ShardSender<T, S>
where
    T: Send + Serialize,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone + Serialize,
{
    fn clone(&self) -> Self {
        let buffer = Vec::with_capacity(self.buf_size);

        ShardSender {
            writer: self.writer.clone(),
            buffer,
            buf_size: self.buf_size,
        }
    }
}

impl<T: Send, S: SortKey<T>> Drop for ShardSender<T, S>
where
    T: Send + Serialize,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone + Serialize,
{
    fn drop(&mut self) {
        let _e = self.finished();
    }
}

/// Read a shardio file
struct ShardReaderSingle<T, S = DefaultSort>
where
    S: SortKey<T>,
{
    file: File,
    index: Vec<ShardRecord<<S as SortKey<T>>::Key>>,
    binconfig: bincode::Config,
    p1: PhantomData<T>,
}

impl<T, S> ShardReaderSingle<T, S>
where
    T: DeserializeOwned,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Clone + Ord + DeserializeOwned,
{
    /// Open a shard file that stores `T` items.
    fn open<P: AsRef<Path>>(path: P) -> Result<ShardReaderSingle<T, S>, Error> {
        let f = File::open(path).unwrap();

        let mut binconfig = bincode::config();
        // limit ourselves to decoding no more than 268MB at a time
        binconfig.limit(1 << 28);

        let (mut index, f) = Self::read_index_block(f, &binconfig)?;
        index.sort();

        Ok(ShardReaderSingle {
            file: f,
            index,
            binconfig,
            p1: PhantomData,
        })
    }

    /// Read shard index
    fn read_index_block(
        mut file: File,
        binconfig: &bincode::Config,
    ) -> Result<(Vec<ShardRecord<<S as SortKey<T>>::Key>>, File), Error> {
        let _ = file.seek(SeekFrom::End(-24))?;
        let _num_shards = file.read_u64::<BigEndian>()? as usize;
        let index_block_position = file.read_u64::<BigEndian>()?;
        let _ = file.read_u64::<BigEndian>()?;
        file.seek(SeekFrom::Start(index_block_position as u64))?;
        let (t_typ, s_typ): (String, String) = binconfig.deserialize_from(&mut file)?;
        // if compiler version is the same, type misnaming is an error
        if t_typ != type_name::<T>() || s_typ != type_name::<S>() {
            return Err(format_err!(
                "Expected type {} with sort order {}, but got type {} with sort order {}",
                type_name::<T>(),
                type_name::<S>(),
                t_typ,
                s_typ,
            ));
        }
        let recs = binconfig.deserialize_from(&mut file)?;
        Ok((recs, file))
    }

    /// Return an iterator over the items in the given `range` of keys.
    pub fn iter_range(
        &self,
        range: &Range<<S as SortKey<T>>::Key>,
    ) -> Result<RangeIter<'_, T, S>, Error> {
        RangeIter::new(&self, range.clone())
    }

    fn iter_shard(
        &self,
        rec: &ShardRecord<<S as SortKey<T>>::Key>,
    ) -> Result<ShardIter<'_, T, S>, Error> {
        ShardIter::new(self, rec.clone())
    }

    /// Total number of values held by this reader
    pub fn len(&self) -> usize {
        self.index.iter().map(|x| x.len_items).sum()
    }

    /// Estimate an upper bound on the total number of values held by a range
    pub fn est_len_range(&self, range: &Range<<S as SortKey<T>>::Key>) -> usize {
        self.index
            .iter()
            .map(|x| {
                if range.intersects_shard(x) {
                    x.len_items
                } else {
                    0
                }
            })
            .sum::<usize>()
    }
}

use std::io::BufReader;
use std::io::Read;

struct ReadAdapter<'a> {
    file: &'a File,
    offset: usize,
    bytes_remaining: usize,
}

impl<'a> ReadAdapter<'a> {
    fn new(file: &'a File, offset: usize, len: usize) -> Self {
        ReadAdapter {
            file,
            offset,
            bytes_remaining: len,
        }
    }
}

impl<'a> Read for ReadAdapter<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read_len = std::cmp::min(buf.len(), self.bytes_remaining);

        let buf_slice = &mut buf[0..read_len];
        let actual_read = self.file.read_at(buf_slice, self.offset as u64)?;
        self.offset += actual_read;
        self.bytes_remaining -= actual_read;

        Ok(actual_read)
    }
}

struct ShardIter<'a, T, S>
where
    S: SortKey<T>,
{
    next_item: Option<T>,
    decoder: lz4::Decoder<BufReader<ReadAdapter<'a>>>,
    items_remaining: usize,
    binconfig: &'a bincode::Config,
    phantom_s: PhantomData<S>,
}

impl<'a, T, S> ShardIter<'a, T, S>
where
    T: DeserializeOwned,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone,
{
    pub(crate) fn new(
        reader: &'a ShardReaderSingle<T, S>,
        rec: ShardRecord<<S as SortKey<T>>::Key>,
    ) -> Result<Self, Error> {
        let adp_reader = ReadAdapter::new(&reader.file, rec.offset, rec.len_bytes);
        let buf_reader = BufReader::new(adp_reader);
        let mut lz4_reader = lz4::Decoder::new(buf_reader)?;

        let binconfig = &reader.binconfig;
        let first_item: T = binconfig.deserialize_from(&mut lz4_reader)?;
        let items_remaining = rec.len_items - 1;

        Ok(ShardIter {
            next_item: Some(first_item),
            decoder: lz4_reader,
            items_remaining,
            binconfig,
            phantom_s: PhantomData,
        })
    }

    pub(crate) fn current_key(&self) -> Cow<'_, <S as SortKey<T>>::Key> {
        self.next_item.as_ref().map(|a| S::sort_key(a)).unwrap()
    }

    pub(crate) fn pop(mut self) -> Result<(T, Option<Self>), Error> {
        let item = self.next_item.unwrap();
        if self.items_remaining == 0 {
            Ok((item, None))
        } else {
            self.next_item = Some(self.binconfig.deserialize_from(&mut self.decoder)?);
            self.items_remaining -= 1;
            Ok((item, Some(self)))
        }
    }
}

use std::cmp::Ordering;

impl<'a, T, S> Ord for ShardIter<'a, T, S>
where
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone,
{
    fn cmp(&self, other: &ShardIter<'a, T, S>) -> Ordering {
        let key_self = self.next_item.as_ref().map(|a| S::sort_key(a));
        let key_other = other.next_item.as_ref().map(|b| S::sort_key(b));
        key_self.cmp(&key_other)
    }
}

impl<'a, T, S> Eq for ShardIter<'a, T, S>
where
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone,
{
}

impl<'a, T, S> PartialOrd for ShardIter<'a, T, S>
where
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone,
{
    fn partial_cmp(&self, other: &ShardIter<'a, T, S>) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

impl<'a, T, S> PartialEq for ShardIter<'a, T, S>
where
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone,
{
    fn eq(&self, other: &ShardIter<'a, T, S>) -> bool {
        let key_self = self.next_item.as_ref().map(|a| S::sort_key(a));
        let key_other = other.next_item.as_ref().map(|b| S::sort_key(b));
        key_self == key_other
    }
}

/// Iterator of items from a single shardio reader
pub struct RangeIter<'a, T, S>
where
    T: 'a,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: 'a + Ord + Clone,
{
    reader: &'a ShardReaderSingle<T, S>,
    range: Range<<S as SortKey<T>>::Key>,
    active_queue: MinMaxHeap<ShardIter<'a, T, S>>,
    waiting_queue: MinMaxHeap<&'a ShardRecord<<S as SortKey<T>>::Key>>,
    warn_limit: usize,
}

impl<'a, T, S> RangeIter<'a, T, S>
where
    T: DeserializeOwned,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: 'a + Clone + Ord + DeserializeOwned,
{
    fn new(
        reader: &'a ShardReaderSingle<T, S>,
        range: Range<<S as SortKey<T>>::Key>,
    ) -> Result<RangeIter<'a, T, S>, Error> {
        let shards: Vec<&ShardRecord<_>> = reader
            .index
            .iter()
            .filter(|x| range.intersects_shard(x))
            .collect();

        let min_item = shards.iter().map(|x| &x.start_key).min();
        let mut active_queue = MinMaxHeap::new();
        let mut waiting_queue = MinMaxHeap::new();

        for s in shards {
            if Some(&s.start_key) == min_item {
                active_queue.push(reader.iter_shard(s)?);
            } else {
                waiting_queue.push(s);
            }
        }

        let mut iter = RangeIter {
            reader,
            range,
            active_queue,
            waiting_queue,
            warn_limit: WARN_ACTIVE_SHARDS,
        };
        iter.warn_active_shards();

        Ok(iter)
    }

    /// What key is next among the active set
    fn peek_active_next(&self) -> Option<Cow<'_, <S as SortKey<T>>::Key>> {
        let n = self.active_queue.peek_min();
        n.map(|v| v.current_key())
    }

    /// Restore all the shards that start at or before this item, or the next usable shard
    fn activate_shards(&mut self) -> Result<(), Error> {
        while self.waiting_queue.peek_min().map_or(false, |shard| {
            Some(Cow::Borrowed(&shard.start_key)) <= self.peek_active_next()
        }) || (self.peek_active_next().is_none() && self.waiting_queue.len() > 0)
        {
            let shard = self.waiting_queue.pop_min().unwrap();
            let iter = self.reader.iter_shard(shard)?;
            self.active_queue.push(iter);
        }
        self.warn_active_shards();
        Ok(())
    }

    /// Get the next item to return among active chunks
    fn next_active(&mut self) -> Option<Result<T, Error>> {
        self.active_queue.pop_min().map(|vec| {
            let (item, new_vec) = vec.pop()?;

            match new_vec {
                Some(v) => self.active_queue.push(v),
                _ => (),
            }
            Ok(item)
        })
    }

    /// Warn if the memory usage will be relevant
    fn warn_active_shards(&mut self) {
        let queue_len = self.active_queue.len();
        if queue_len >= self.warn_limit {
            let mem_est_gib = (SHARD_ITER_SZ * queue_len) as f64 / 1024f64.powi(3);
            warn!(
                "{} active shards! Memory usage may be impacted by at least {} GiB",
                queue_len, mem_est_gib
            );
            // raise the warn limit by 10% of observed so we don't spam the log
            self.warn_limit = queue_len * 11 / 10;
        }
    }
}

fn transpose<T, E>(v: Option<Result<T, E>>) -> Result<Option<T>, E> {
    match v {
        Some(Ok(v)) => Ok(Some(v)),
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

impl<'a, T, S> Iterator for RangeIter<'a, T, S>
where
    T: DeserializeOwned,
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Clone + Ord + DeserializeOwned,
{
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Result<T, Error>> {
        loop {
            let a = self.activate_shards();
            if a.is_err() {
                return Some(Err(a.unwrap_err()));
            }

            match self.next_active() {
                Some(Ok(v)) => {
                    let c = self.range.cmp(&S::sort_key(&v));
                    match c {
                        Rorder::Before => (),
                        Rorder::Intersects => return Some(Ok(v)),
                        Rorder::After => return None,
                    }
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
}

struct SortableItem<T, S> {
    item: T,
    phantom_s: PhantomData<S>,
}

impl<T, S> SortableItem<T, S> {
    fn new(item: T) -> SortableItem<T, S> {
        SortableItem {
            item,
            phantom_s: PhantomData,
        }
    }
}

impl<T, S> Ord for SortableItem<T, S>
where
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone,
{
    fn cmp(&self, other: &SortableItem<T, S>) -> Ordering {
        S::sort_key(&self.item).cmp(&S::sort_key(&other.item))
    }
}

impl<T, S> PartialOrd for SortableItem<T, S>
where
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone,
{
    fn partial_cmp(&self, other: &SortableItem<T, S>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T, S> PartialEq for SortableItem<T, S>
where
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone,
{
    fn eq(&self, other: &SortableItem<T, S>) -> bool {
        S::sort_key(&self.item) == S::sort_key(&other.item)
    }
}

impl<T, S> Eq for SortableItem<T, S>
where
    S: SortKey<T>,
    <S as SortKey<T>>::Key: Ord + Clone,
{
}

/// Iterator over merged shardio files
pub struct MergeIterator<'a, T: 'a, S: 'a>
where
    S: SortKey<T>,
    <S as SortKey<T>>::Key: 'a + Ord + Clone,
{
    iterators: Vec<RangeIter<'a, T, S>>,
    merge_heap: MinMaxHeap<(SortableItem<T, S>, usize)>,
    phantom_s: PhantomData<S>,
}

impl<'a, T, S> MergeIterator<'a, T, S>
where
    S: SortKey<T>,
    <S as SortKey<T>>::Key: 'a + Ord + Clone,
    RangeIter<'a, T, S>: Iterator<Item = Result<T, Error>>,
{
    fn new(mut iterators: Vec<RangeIter<'a, T, S>>) -> Result<MergeIterator<'a, T, S>, Error> {
        let mut merge_heap = MinMaxHeap::new();

        for (idx, itr) in iterators.iter_mut().enumerate() {
            let item = transpose(itr.next())?;

            // If we have a next item, add it's key to the heap
            item.map(|ii| {
                let sortable_item = SortableItem::new(ii);
                merge_heap.push((sortable_item, idx));
            });
        }

        Ok(MergeIterator {
            iterators,
            merge_heap,
            phantom_s: PhantomData,
        })
    }
}

use std::iter::Iterator;

impl<'a, T: 'a, S: 'a> Iterator for MergeIterator<'a, T, S>
where
    S: SortKey<T>,
    <S as SortKey<T>>::Key: 'a + Ord + Clone,
    RangeIter<'a, T, S>: Iterator<Item = Result<T, Error>>,
{
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Result<T, Error>> {
        let next_itr = self.merge_heap.pop_min();

        next_itr.map(|(item, i)| {
            // Get next-next value for this iterator
            let next = transpose(self.iterators[i].next())?;

            // Push the next-next key onto the heap
            match next {
                Some(next_item) => self.merge_heap.push((SortableItem::new(next_item), i)),
                _ => (),
            };

            Ok(item.item)
        })
    }
}

/// Read from a collection of shardio files. The input data is merged to give
/// a single sorted view of the combined dataset. The input files must
/// be created with the same sort order `S` as they are read with.
pub struct ShardReader<T, S = DefaultSort>
where
    S: SortKey<T>,
{
    readers: Vec<ShardReaderSingle<T, S>>,
}

impl<T, S> ShardReader<T, S>
where
    T: DeserializeOwned,
    <S as SortKey<T>>::Key: Clone + Ord + DeserializeOwned,
    S: SortKey<T>,
{
    /// Open a single shard files into reader
    pub fn open<P: AsRef<Path>>(shard_file: P) -> Result<ShardReader<T, S>, Error> {
        let mut readers = Vec::new();
        let reader = ShardReaderSingle::open(shard_file)?;
        readers.push(reader);

        Ok(ShardReader { readers })
    }

    /// Open a set of shard files into an aggregated reader
    pub fn open_set<P: AsRef<Path>>(shard_files: &[P]) -> Result<ShardReader<T, S>, Error> {
        let mut readers = Vec::new();

        for p in shard_files {
            let reader = ShardReaderSingle::open(p)?;
            readers.push(reader);
        }

        Ok(ShardReader { readers })
    }

    /// Read data from the given `range` into `data` buffer. The `data` buffer is cleared before adding items.
    pub fn read_range(
        &self,
        range: &Range<<S as SortKey<T>>::Key>,
        data: &mut Vec<T>,
    ) -> Result<(), Error> {
        data.clear();
        for item in self.iter_range(range)? {
            data.push(item?);
        }
        Ok(())
    }

    /// Iterate over items in the given `range`
    pub fn iter_range<'a>(
        &'a self,
        range: &Range<<S as SortKey<T>>::Key>,
    ) -> Result<MergeIterator<'a, T, S>, Error> {
        let mut iters = Vec::new();
        for r in self.readers.iter() {
            let iter = r.iter_range(range)?;
            iters.push(iter);
        }

        MergeIterator::new(iters)
    }

    /// Iterate over all items
    pub fn iter<'a>(&'a self) -> Result<MergeIterator<'a, T, S>, Error> {
        self.iter_range(&Range::all())
    }

    /// Total number of items
    pub fn len(&self) -> usize {
        self.readers.iter().map(|r| r.len()).sum()
    }

    /// Generate `num_chunks` ranges covering the give `range`, each with a roughly equal numbers of elements.
    /// The ranges can be fed to `iter_range`
    pub fn make_chunks(
        &self,
        num_chunks: usize,
        range: &Range<<S as SortKey<T>>::Key>,
    ) -> Vec<Range<<S as SortKey<T>>::Key>> {
        assert!(num_chunks > 0);

        // Enumerate all the in-range known start locations in the dataset
        let mut starts = BTreeSet::new();
        for r in &self.readers {
            for block in &r.index {
                if range.contains(&block.start_key) {
                    starts.insert(block.start_key.clone());
                }
            }
        }
        let starts = starts.into_iter().collect::<Vec<_>>();

        // Divide the known start locations into chunks, and
        // use setup start positions.
        let chunk_starts = if starts.len() <= num_chunks {
            starts.into_iter().map(|x| Some(x)).collect::<Vec<_>>()
        } else {
            let n = starts.len();
            (0..num_chunks)
                .map(|i| {
                    if i == 0 {
                        range.start.clone()
                    } else {
                        let idx = ((n * i) as f64 / num_chunks as f64).round() as usize;
                        Some(starts[idx].clone())
                    }
                })
                .collect::<Vec<_>>()
        };

        let mut chunks = Vec::new();
        for (i, start) in chunk_starts.iter().enumerate() {
            let end = if i + 1 == chunk_starts.len() {
                range.end.clone()
            } else {
                chunk_starts[i + 1].clone()
            };

            chunks.push(Range {
                start: start.clone(),
                end,
            })
        }

        chunks
    }

    /// Estimate an upper bound on the total number of values held by a range
    #[allow(dead_code)]
    pub fn est_len_range(&self, range: &Range<<S as SortKey<T>>::Key>) -> usize {
        self.readers
            .iter()
            .map(|x| x.est_len_range(range))
            .sum::<usize>()
    }
}

#[cfg(test)]
mod shard_tests {
    use super::*;
    use is_sorted::IsSorted;
    use pretty_assertions::assert_eq;
    use quickcheck::{Arbitrary, Gen, QuickCheck, StdThreadGen};
    use std::collections::HashSet;
    use std::fmt::Debug;
    use std::hash::Hash;
    use std::iter::{repeat, FromIterator};
    use std::u8;
    use tempfile;

    #[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Debug, PartialOrd, Ord, Hash)]
    struct T1 {
        a: u64,
        b: u32,
        c: u16,
        d: u8,
    }

    impl Arbitrary for T1 {
        fn arbitrary<G: Gen>(g: &mut G) -> T1 {
            T1 {
                a: u64::arbitrary(g),
                b: u32::arbitrary(g),
                c: u16::arbitrary(g),
                d: u8::arbitrary(g),
            }
        }
    }

    struct FieldDSort;
    impl SortKey<T1> for FieldDSort {
        type Key = u8;
        fn sort_key(item: &T1) -> Cow<'_, u8> {
            Cow::Borrowed(&item.d)
        }
    }

    fn rand_items(n: usize, g: &mut impl Gen) -> Vec<T1> {
        let mut items = Vec::new();
        for _ in 0..n {
            let tt = T1::arbitrary(g);
            items.push(tt);
        }
        items
    }

    fn rand_item_chunks(n_chunks: usize, items_per_chunk: usize, g: &mut impl Gen) -> Vec<Vec<T1>> {
        let mut chunks = Vec::new();
        for _ in 0..n_chunks {
            chunks.push(rand_items(items_per_chunk, g));
        }
        chunks
    }

    // Some tests are configured to only run with the "full-test" feature enabled.
    // They are too slow to run in debug mode, so you should use release mode.
    #[cfg(feature = "full-test")]
    #[test]
    fn test_read_write_at() {
        let tmp = tempfile::NamedTempFile::new().unwrap();

        let n = (1 << 31) - 1000;
        let mut buf = Vec::with_capacity(n);
        for i in 0..n {
            buf.push((i % 254) as u8);
        }

        let written = tmp.as_file().write_at(&buf, 0).unwrap();
        assert_eq!(n, written);

        for i in 0..n {
            buf[i] = 0;
        }

        let read = tmp.as_file().read_at(&mut buf, 0).unwrap();
        assert_eq!(n, read);

        for i in 0..n {
            assert_eq!(buf[i], (i % 254) as u8)
        }
    }

    #[test]
    fn test_shard_round_trip() {
        // Test different buffering configurations
        check_round_trip(5, 3, 12, 1 << 9);
        check_round_trip(5, 4, 12, 1 << 9);
        check_round_trip(5, 5, 13, 1 << 9);
        check_round_trip(10, 20, 40, 1 << 8);
        check_round_trip(1024, 16, 2 << 14, 1 << 16);
        check_round_trip(4096, 8, 2048, 1 << 16);
        check_round_trip(128, 4, 1024, 1 << 12);
        check_round_trip(50, 2, 256, 1 << 16);
        check_round_trip(10, 20, 40, 1 << 14);
    }

    #[cfg(feature = "full-test")]
    #[test]
    fn test_shard_round_trip_big_chunks() {
        // Test different buffering configurations
        check_round_trip(1 << 18, 64, 1 << 20, 1 << 21);
    }

    #[test]
    fn test_shard_round_trip_sort_key() -> Result<(), Error> {
        // Test different buffering configurations
        check_round_trip_sort_key(10, 10, 10, 256, true)?;
        check_round_trip_sort_key(10, 2, 3, 512, true)?;
        check_round_trip_sort_key(10, 20, 40, 256, true)?;
        check_round_trip_sort_key(1024, 16, 2 << 14, 1 << 16, true)?;
        check_round_trip_sort_key(4096, 8, 2048, 1 << 16, true)?;
        check_round_trip_sort_key(128, 4, 1024, 1 << 12, true)?;
        check_round_trip_sort_key(50, 2, 256, 1 << 16, true)?;
        check_round_trip_sort_key(10, 20, 40, 1 << 14, true)?;

        check_round_trip_sort_key(64, 16, 1 << 17, 1 << 16, true)?;
        check_round_trip_sort_key(128, 16, 1 << 16, 1 << 16, true)?;
        check_round_trip_sort_key(128, 16, 1 << 15, 1 << 16, true)?;

        Ok(())
    }

    // Only run this test in release mode for perf testing
    #[cfg(feature = "full-test")]
    #[test]
    fn test_shard_round_trip_big() {
        // Play with these settings to test perf.
        check_round_trip_opt(1024, 64, 1 << 18, 1 << 20, false).unwrap();
    }

    #[test]
    fn test_shard_one_key() -> Result<(), Error> {
        let n_items = 1 << 16;
        let tmp = tempfile::NamedTempFile::new().unwrap();

        // Write and close file
        let true_items = {
            let manager: ShardWriter<T1> = ShardWriter::new(tmp.path(), 16, 64, 1 << 10).unwrap();

            let mut g = StdThreadGen::new(10);
            let true_items = repeat(rand_items(1, &mut g)[0])
                .take(n_items)
                .collect::<Vec<_>>();

            // Sender must be closed
            {
                for chunk in true_items.chunks(n_items / 8) {
                    let mut sender = manager.get_sender();
                    for item in chunk {
                        sender.send(*item)?;
                    }
                }
            }
            true_items
        };

        // Open finished file
        let reader = ShardReader::<T1>::open(tmp.path())?;

        let _all_items: Result<_, _> = reader.iter_range(&Range::all())?.collect();
        let all_items: Vec<_> = _all_items?;
        set_compare(&true_items, &all_items);

        if !(true_items == all_items) {
            println!("true len: {:?}", true_items.len());
            println!("round trip len: {:?}", all_items.len());
            assert_eq!(&true_items, &all_items);
        }

        for rc in [1, 3, 8, 15, 32, 63, 128, 255, 512, 1095].iter() {
            // Open finished file & test chunked reads
            let set_reader = ShardReader::<T1>::open(&tmp.path())?;
            let mut all_items_chunks = Vec::new();

            // Read in chunks
            let chunks = set_reader.make_chunks(*rc, &Range::all());
            assert_eq!(1, chunks.len());
            for c in chunks {
                let itr = set_reader.iter_range(&c)?;
                for i in itr {
                    all_items_chunks.push(i?);
                }
            }
            assert_eq!(&true_items, &all_items_chunks);
        }

        Ok(())
    }

    // newtype for generating Vec<Vec<T>>, with a not-too-big number of outer vectors
    #[derive(Clone, Debug)]
    struct MultiSlice<T>(Vec<Vec<T>>);

    impl<T: Arbitrary> Arbitrary for MultiSlice<T> {
        fn arbitrary<G: Gen>(g: &mut G) -> MultiSlice<T> {
            let slices = (g.next_u32() % 32) as usize;

            let mut data = Vec::new();

            for _ in 0..slices {
                let slice = Vec::<T>::arbitrary(g);
                data.push(slice);
            }

            MultiSlice(data)
        }
    }

    fn test_multi_slice<T, S>(
        items: MultiSlice<T>,
        disk_chunk_size: usize,
        producer_chunk_size: usize,
        buffer_size: usize,
    ) -> Result<Vec<T>, Error>
    where
        T: 'static + Serialize + DeserializeOwned + Clone + Send,
        S: SortKey<T>,
        <S as SortKey<T>>::Key: 'static + Send + Serialize + DeserializeOwned,
    {
        let mut files = Vec::new();

        for item_chunk in &items.0 {
            let tmp = tempfile::NamedTempFile::new()?;

            let writer: ShardWriter<T, S> = ShardWriter::new(
                tmp.path(),
                producer_chunk_size,
                disk_chunk_size,
                buffer_size,
            )?;

            let mut sender = writer.get_sender();
            for item in item_chunk {
                sender.send(item.clone())?;
            }

            files.push(tmp);
        }

        let reader = ShardReader::<T, S>::open_set(&files)?;
        let mut out_items = Vec::new();

        for r in reader.iter()? {
            out_items.push(r?);
        }

        Ok(out_items)
    }

    #[test]
    fn multi_slice_correctness_quickcheck() {
        fn check_t1(v: MultiSlice<T1>) -> bool {
            let sorted = test_multi_slice::<T1, FieldDSort>(v.clone(), 1024, 16, 1 << 17).unwrap();

            let mut vall = Vec::new();
            for chunk in v.0 {
                vall.extend(chunk);
            }

            if sorted.len() != vall.len() {
                return false;
            }
            if !set_compare(&sorted, &vall) {
                return false;
            }
            IsSorted::is_sorted_by_key(&mut sorted.iter(), |x| FieldDSort::sort_key(x).into_owned())
        }

        QuickCheck::with_gen(StdThreadGen::new(500000))
            .tests(4)
            .quickcheck(check_t1 as fn(MultiSlice<T1>) -> bool);
    }

    fn check_round_trip(
        disk_chunk_size: usize,
        producer_chunk_size: usize,
        buffer_size: usize,
        n_items: usize,
    ) {
        check_round_trip_opt(
            disk_chunk_size,
            producer_chunk_size,
            buffer_size,
            n_items,
            true,
        )
        .unwrap();
    }

    struct ThreadSender<T, S> {
        t: PhantomData<T>,
        s: PhantomData<S>,
    }

    impl<T, S> ThreadSender<T, S>
    where
        T: 'static + Send + Serialize + Clone,
        S: 'static + SortKey<T>,
        <S as SortKey<T>>::Key: Ord + Clone + Serialize + Send,
    {
        fn send_from_threads(
            chunks: Vec<Vec<T>>,
            sender: ShardSender<T, S>,
        ) -> Result<Vec<T>, Error> {
            let barrier = Arc::new(std::sync::Barrier::new(chunks.len()));
            let mut handles = Vec::new();

            let mut all_items = Vec::new();

            for chunk in chunks {
                let mut s = sender.clone();
                all_items.extend(chunk.iter().cloned());
                let b = barrier.clone();

                let h = std::thread::spawn(move || -> Result<(), Error> {
                    b.wait();
                    for item in chunk {
                        s.send(item)?;
                    }
                    Ok(())
                });

                handles.push(h);
            }

            for h in handles {
                h.join().unwrap()?;
            }

            Ok(all_items)
        }
    }

    fn check_round_trip_opt(
        disk_chunk_size: usize,
        producer_chunk_size: usize,
        buffer_size: usize,
        n_items: usize,
        do_read: bool,
    ) -> Result<(), Error> {
        println!(
            "test round trip: disk_chunk_size: {}, producer_chunk_size: {}, n_items: {}",
            disk_chunk_size, producer_chunk_size, n_items
        );

        let tmp = tempfile::NamedTempFile::new()?;

        // Write and close file
        let true_items = {
            let mut writer: ShardWriter<T1> = ShardWriter::new(
                tmp.path(),
                producer_chunk_size,
                disk_chunk_size,
                buffer_size,
            )?;

            let mut g = StdThreadGen::new(10);
            let send_chunks = rand_item_chunks(4, n_items / 4, &mut g);
            let mut true_items = ThreadSender::send_from_threads(send_chunks, writer.get_sender())?;

            writer.finish()?;
            true_items.sort();
            true_items
        };

        if do_read {
            // Open finished file
            let reader = ShardReader::<T1>::open(tmp.path())?;
            let iter = reader.iter_range(&Range::all())?;

            let all_items_res: Result<Vec<_>, Error> = iter.collect();
            let all_items = all_items_res?;
            set_compare(&true_items, &all_items);

            if !(true_items == all_items) {
                println!("true len: {:?}", true_items.len());
                println!("round trip len: {:?}", all_items.len());
                assert_eq!(&true_items, &all_items);
            }

            for rc in [1, 3, 8, 15, 27].iter() {
                // Open finished file & test chunked reads
                let set_reader = ShardReader::<T1>::open(&tmp.path())?;
                let mut all_items_chunks = Vec::new();

                // Read in chunks
                let chunks = set_reader.make_chunks(*rc, &Range::all());
                assert!(
                    chunks.len() <= *rc,
                    "chunks > req: ({} > {})",
                    chunks.len(),
                    *rc
                );

                for c in chunks {
                    let itr = set_reader.iter_range(&c)?;

                    for i in itr {
                        let i = i?;
                        assert!(c.contains(&i));
                        all_items_chunks.push(i);
                    }
                }
                assert_eq!(&true_items, &all_items_chunks);
            }
        }
        Ok(())
    }

    fn check_round_trip_sort_key(
        disk_chunk_size: usize,
        producer_chunk_size: usize,
        buffer_size: usize,
        n_items: usize,
        do_read: bool,
    ) -> Result<(), Error> {
        println!(
            "test round trip: disk_chunk_size: {}, producer_chunk_size: {}, n_items: {}",
            disk_chunk_size, producer_chunk_size, n_items
        );

        let tmp = tempfile::NamedTempFile::new().unwrap();

        // Write and close file
        let true_items = {
            let mut writer: ShardWriter<T1, FieldDSort> = ShardWriter::new(
                tmp.path(),
                producer_chunk_size,
                disk_chunk_size,
                buffer_size,
            )?;

            let mut g = StdThreadGen::new(10);
            let send_chunks = rand_item_chunks(4, n_items / 4, &mut g);
            let mut true_items = ThreadSender::send_from_threads(send_chunks, writer.get_sender())?;

            writer.finish()?;

            true_items.sort_by_key(|x| x.d);
            true_items
        };

        if do_read {
            // Open finished file
            let reader = ShardReader::<T1, FieldDSort>::open(tmp.path())?;

            let all_items_res: Result<_, _> = reader.iter_range(&Range::all())?.collect();
            let all_items: Vec<_> = all_items_res?;
            set_compare(&true_items, &all_items);

            // Open finished file & test chunked reads
            let set_reader = ShardReader::<T1, FieldDSort>::open(tmp.path())?;
            let mut all_items_chunks = Vec::new();

            for rc in &[1, 4, 7, 12, 15, 21, 65, 8192] {
                all_items_chunks.clear();
                let chunks = set_reader.make_chunks(*rc, &Range::all());
                assert!(
                    chunks.len() <= *rc,
                    "chunks > req: ({} > {})",
                    chunks.len(),
                    *rc
                );
                assert!(
                    chunks.len() <= u8::max_value() as usize,
                    "chunks > |T1.d| ({} > {})",
                    chunks.len(),
                    u8::max_value()
                );
                for c in chunks {
                    let itr = set_reader.iter_range(&c)?;

                    for i in itr {
                        let i = i?;
                        let key = FieldDSort::sort_key(&i);
                        assert!(c.contains(&key));
                        all_items_chunks.push(i);
                    }
                }

                set_compare(&true_items, &all_items_chunks);
            }
        }

        Ok(())
    }

    fn set_compare<T: Eq + Debug + Clone + Hash>(s1: &Vec<T>, s2: &Vec<T>) -> bool {
        let s1: HashSet<T> = HashSet::from_iter(s1.iter().cloned());
        let s2: HashSet<T> = HashSet::from_iter(s2.iter().cloned());

        if s1 == s2 {
            return true;
        } else {
            let s1_minus_s2 = s1.difference(&s2);
            println!("in s1, but not s2: {:?}", s1_minus_s2);

            let s2_minus_s1 = s2.difference(&s1);
            println!("in s2, but not s1: {:?}", s2_minus_s1);

            return false;
        }
    }

    #[test]
    fn test_io_error() -> Result<(), Error> {
        let disk_chunk_size = 1 << 20;
        let producer_chunk_size = 1 << 4;
        let buffer_size = 1 << 16;
        let n_items = 1 << 19;

        let tmp = tempfile::NamedTempFile::new()?;

        // Write and close file
        let _true_items = {
            let manager: ShardWriter<T1> = ShardWriter::new(
                tmp.path(),
                producer_chunk_size,
                disk_chunk_size,
                buffer_size,
            )?;

            let mut g = StdThreadGen::new(10);
            let mut true_items = rand_items(n_items, &mut g);

            // Sender must be closed
            {
                for chunk in true_items.chunks(n_items / 8) {
                    let mut sender = manager.get_sender();
                    for item in chunk {
                        sender.send(*item)?;
                    }
                }
            }
            true_items.sort();
            true_items
        };

        // Open finished file
        let reader = ShardReader::<T1>::open(tmp.path())?;

        // Truncate the file, so we create a read error.
        let file = tmp.reopen().unwrap();
        let sz = file.metadata().unwrap().len();
        file.set_len(3 * sz / 4).unwrap();

        // make sure we get a read err due to the truncated file.
        let iter = reader.iter_range(&Range::all());

        // Could get the error here
        if iter.is_err() {
            return Ok(());
        }

        for i in iter? {
            // or could get the error here
            if i.is_err() {
                return Ok(());
            }
        }

        Err(format_err!("didn't shardio IO error correctly"))
    }

    #[derive(Copy, Clone, Serialize, Deserialize, Debug)]
    struct T2(u16);

    #[derive(Copy, Clone, Serialize, Deserialize, Debug)]
    struct T3(u16);

    struct S2 {}
    struct S3 {}

    impl SortKey<T2> for S2 {
        type Key = u16;
        fn sort_key(v: &T2) -> Cow<u16> {
            Cow::Owned(v.0)
        }
    }

    impl SortKey<T3> for S2 {
        type Key = u16;
        fn sort_key(v: &T3) -> Cow<u16> {
            Cow::Owned(v.0)
        }
    }

    impl SortKey<T2> for S3 {
        type Key = u16;
        fn sort_key(v: &T2) -> Cow<u16> {
            Cow::Owned(v.0)
        }
    }

    #[test]
    fn test_ttyp_error() -> Result<(), Error> {
        let disk_chunk_size = 1 << 20;
        let producer_chunk_size = 1 << 4;
        let buffer_size = 1 << 16;

        let tmp = tempfile::NamedTempFile::new()?;

        {
            let manager: ShardWriter<T2, S2> = ShardWriter::new(
                tmp.path(),
                producer_chunk_size,
                disk_chunk_size,
                buffer_size,
            )?;
            let mut sender = manager.get_sender();
            sender.send(T2(0))?;
        }
        {
            let reader = ShardReader::<T3, S2>::open(tmp.path());
            assert!(reader.is_err());
            if let Err(err) = reader {
                println!("{}", err);
            }
        }
        tmp.close()?;
        Ok(())
    }

    #[test]
    fn test_styp_error() -> Result<(), Error> {
        let disk_chunk_size = 1 << 20;
        let producer_chunk_size = 1 << 4;
        let buffer_size = 1 << 16;

        let tmp = tempfile::NamedTempFile::new()?;

        {
            let manager: ShardWriter<T2, S2> = ShardWriter::new(
                tmp.path(),
                producer_chunk_size,
                disk_chunk_size,
                buffer_size,
            )?;
            let mut sender = manager.get_sender();
            sender.send(T2(0))?;
        }
        {
            let reader = ShardReader::<T2, S3>::open(tmp.path());
            assert!(reader.is_err());
            if let Err(err) = reader {
                println!("{}", err);
            }
        }
        tmp.close()?;
        Ok(())
    }
}
