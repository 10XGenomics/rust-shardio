// Copyright (c) 2018 10x Genomics, Inc. All rights reserved.

//! Efficiently write Rust structs to shard files from multiple threads.
//! You can process different subsets of your data in different threads, different processes.
//! When reading shardio will merge the data on the fly into a single sorted view.
//!
//! ```rust
//! #[macro_use]
//! extern crate serde_derive;
//!
//! extern crate shardio;
//! use shardio::*;
//! use std::fs::File;
//!
//! #[derive(Clone, Eq, PartialEq, Serialize, Deserialize, PartialOrd, Ord, Debug)]
//! struct Test {
//!     a: u64,
//!     b: u32,
//! }
//!
//! fn main()
//! {
//!     let filename = "test.shardio";
//!
//!     {
//!         // Open a shardio output file
//!         // Parameters here  buffering, and the size of disk chunks
//!         // which affect how many disk chunks need to be read to
//!         // satisfy a range query when reading.
//!         let manager: ShardWriter<Test, Test> =
//!             ShardWriter::new(filename, 64, 256, 1<<16).unwrap();
//!
//!         // Get a handle to send data to the file
//!         let mut sender = manager.get_sender();
//!
//!         // Generate some test data
//!         for i in 0..(2 << 16) {
//!             let item = Test { a: (i%25) as u64, b: (i%100) as u32 };
//!             sender.send(item);
//!         }
//!     }
//!
//!     // Open finished file & test chunked reads
//!     let reader = ShardReader::<Test, Test>::open(filename);
//!
//!     // Read back data
//!     let mut all_items = Vec::new();
//!
//!     let chunks = reader.make_chunks(5, &Range::all());
//!     for c in chunks {
//!         let mut range_iter = reader.iter_range(&c);
//!         all_items.extend(range_iter);
//!     }
//!
//!     // Data will be return in sorted order
//!     let mut all_items_sorted = all_items.clone();
//!     all_items.sort();
//!     assert_eq!(all_items, all_items_sorted);
//! }
//! ```

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate pretty_assertions;

extern crate bincode;
extern crate byteorder;
extern crate libc;
extern crate serde;

#[macro_use]
extern crate failure;
extern crate crossbeam_channel;
extern crate flate2;
extern crate min_max_heap;

#[cfg(feature = "lz4")]
extern crate lz4;

#[cfg(test)]
extern crate tempfile;

use std::fs::File;
use std::io::{self, Seek, SeekFrom};
use std::os::unix::io::{AsRawFd, RawFd};

use crossbeam_channel::{bounded, Receiver, Sender};
use std::path::Path;

use min_max_heap::MinMaxHeap;
use std::marker::PhantomData;
use std::thread;
use std::thread::JoinHandle;

use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use bincode::{deserialize_from, serialize_into};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use libc::{c_void, off_t, pread, pwrite, size_t, ssize_t};

use failure::Error;

pub mod pmap;
pub mod range;
use range::Rorder;
pub mod helper;

pub use range::Range;

fn err(e: ssize_t) -> io::Result<usize> {
    if e == -1 as ssize_t {
        Err(io::Error::last_os_error())
    } else {
        Ok(e as usize)
    }
}

fn read_at(fd: &RawFd, pos: u64, buf: &mut [u8]) -> io::Result<usize> {
    err(unsafe {
        pread(
            *fd,
            buf.as_mut_ptr() as *mut c_void,
            buf.len() as size_t,
            pos as off_t,
        )
    })
}

fn write_at(fd: &RawFd, pos: u64, buf: &[u8]) -> io::Result<usize> {
    err(unsafe {
        pwrite(
            *fd,
            buf.as_ptr() as *const c_void,
            buf.len() as size_t,
            pos as off_t,
        )
    })
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
/// A group of `len_items` items, from shard `shard`, sorted at position `offset`, using `block_size` bytes on-disk.
struct ShardRecord<K> {
    start_key: K,
    end_key: K,
    offset: usize,
    len_bytes: usize,
    len_items: usize,
}

/// Log of shard chunks written into this file.
struct FileManager<K> {
    // Current start position of next chunk
    cursor: usize,

    // Record of chunks written
    regions: Vec<ShardRecord<K>>,
    file: File,
}

impl<K: Ord + Serialize> FileManager<K> {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<FileManager<K>, Error> {
        let file = File::create(path)?;

        Ok(FileManager {
            cursor: 4096,
            regions: Vec::new(),
            file,
        })
    }

    fn write_block(&mut self, range: (K, K), n_items: usize, data: &[u8]) -> Result<usize, Error> {
        let cur_offset = self.cursor;
        let reg = ShardRecord {
            offset: self.cursor,
            start_key: range.0,
            end_key: range.1,
            len_bytes: data.len(),
            len_items: n_items,
        };

        self.regions.push(reg);
        self.cursor += data.len();
        let l = write_at(&self.file.as_raw_fd(), cur_offset as u64, data)?;

        Ok(l)
    }
}

/// Specify a key function from data items of type `T` to a sort key of type `K`.
/// Implment this trait to create a custom sort order.
pub trait SortKey<T, K: Ord> {
    fn sort_key(t: &T) -> K;
}

/// Marker struct for sorting types that implement `Ord` in their 'natural' order.
pub struct DefaultSort;
impl<T> SortKey<T, T> for DefaultSort
where
    T: Ord + Clone,
{
    fn sort_key(t: &T) -> T {
        t.clone()
    }
}

/// Write a stream data items of type `T` to disk.
///
/// Data is sorted, block compressed and written to disk when the buffer fills up. When the ShardWriter is dropped, it will
/// flush remaining items to disk, write the inded and close the file.  NOTE: you may loose data if you don't close shutdown ShardSenders
/// prior to dropping the ShardWriter.
///
/// # Sorting
/// Items are sorted according to the `Ord` implementation of type `K`. Type `S`, implementing the `SortKey` trait
/// maps items of type `T` to their sort key of type `K`. By default the sort key _is_ the data item itself, and the
/// the `DefaultSort` implementation of `SortKey` is the identity function.
pub struct ShardWriter<T, K = T, S = DefaultSort> {
    helper: ShardWriterHelper<T>,
    sender_buffer_size: usize,
    p1: PhantomData<K>,
    p2: PhantomData<S>,
    closed: bool,
}

struct ShardWriterHelper<T> {
    tx: Sender<Option<Vec<T>>>,
    buffer_thread: Option<JoinHandle<()>>,
    writer_thread: Option<JoinHandle<Result<usize, Error>>>,
}

impl<T, K, S> ShardWriter<T, K, S>
where
    T: 'static + Send + Serialize,
    K: 'static + Send + Ord + Serialize,
    S: SortKey<T, K>,
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
    ) -> Result<ShardWriter<T, K, S>, Error> {
        let (send, recv) = bounded(4);

        // Ping-pong of the 2 item buffer between the thread the accumulates the items,
        // and the thread that does the IO. Need 2 slots to avoid a deadlock.
        let (to_writer_send, to_writer_recv) = bounded(2);
        let (to_buffer_send, to_buffer_recv) = bounded(2);

        // Divide buffer size by 2 -- the get swaped between buffer thread and writer thread
        let mut sbt = ShardBufferThread::<T>::new(
            sender_buffer_size,
            item_buffer_size >> 1,
            recv,
            to_writer_send,
            to_buffer_recv,
        );
        let p2 = path.as_ref().to_owned();

        let buffer_thread = thread::spawn(move || sbt.process());

        let writer = FileManager::<K>::new(p2)?;
        let writer_thread = thread::spawn(move || {
            let mut swt = ShardWriterThread::<_, _, S>::new(
                disk_chunk_size,
                writer,
                to_writer_recv,
                to_buffer_send,
            );
            swt.process()
        });

        Ok(ShardWriter {
            helper: ShardWriterHelper {
                tx: send,
                // These need to be options to work correctly with drop().
                buffer_thread: Some(buffer_thread),
                writer_thread: Some(writer_thread),
            },
            sender_buffer_size,
            closed: false,
            p1: PhantomData,
            p2: PhantomData,
        })
    }

    /// Get a `ShardSender`. It can be sent to another thread that is generating data.
    pub fn get_sender(&self) -> ShardSender<T> {
        ShardSender::new(self)
    }

    /// Call finish if you want to detect errors in the writer IO.
    pub fn finish(&mut self) -> Result<usize, Error> {
        if self.closed {
            return Err(format_err!("ShardWriter closed twice"));
        }

        self.closed = true;
        let _ = self.helper.tx.send(None);
        self.helper.buffer_thread.take().map(|x| x.join());
        let join_handle = self
            .helper
            .writer_thread
            .take()
            .expect("called finish twice");
        convert_thread_panic(join_handle.join())
    }
}

fn convert_thread_panic<T>(thread_result: thread::Result<Result<T, Error>>) -> Result<T, Error> {
    match thread_result {
        Ok(v) => v,
        Err(e) => {
            if let Some(str_slice) = e.downcast_ref::<&'static str>() {
                Err(format_err!("thread panic: {}", str_slice))
            } else if let Some(string) = e.downcast_ref::<String>() {
                Err(format_err!("thread panic: {}", string))
            } else {
                Err(format_err!("thread panic: Box<Any>"))
            }
        }
    }
}

impl<T, K, S> Drop for ShardWriter<T, K, S> {
    fn drop(&mut self) {
        if !self.closed {
            self.helper.tx.send(None).unwrap();
            self.helper.buffer_thread.take().map(|x| x.join());
            self.helper.writer_thread.take().map(|x| x.join());
        }
    }
}

/// Manage receive and buffer items from ShardSenders
/// Two buffers will be created & passed back and forth
/// with the ShardWriter thread.
struct ShardBufferThread<T>
where
    T: Send,
{
    chunk_size: usize,
    buffer_size: usize,
    rx: Receiver<Option<Vec<T>>>,
    buf_tx: Sender<(Vec<T>, bool)>,
    buf_rx: Receiver<Vec<T>>,
    second_buf: bool,
}

impl<T> ShardBufferThread<T>
where
    T: Send,
{
    fn new(
        chunk_size: usize,
        buffer_size: usize,
        rx: Receiver<Option<Vec<T>>>,
        buf_tx: Sender<(Vec<T>, bool)>,
        buf_rx: Receiver<Vec<T>>,
    ) -> ShardBufferThread<T> {
        ShardBufferThread {
            chunk_size,
            buffer_size,
            rx,
            buf_rx,
            buf_tx,
            second_buf: false,
        }
    }

    // Add new items to the buffer, send full buffers to the
    // writer thread.
    fn process(&mut self) {
        let mut buf = Vec::with_capacity(self.buffer_size);

        loop {
            match self.rx.recv() {
                Ok(Some(v)) => {
                    buf.extend(v);
                    if buf.len() + self.chunk_size > self.buffer_size {
                        buf = self.send_buf(buf, false);
                    }
                }
                Ok(None) => {
                    self.send_buf(buf, true);
                    break;
                }
                Err(_) => break,
            }
        }
    }

    fn send_buf(&mut self, buf: Vec<T>, done: bool) -> Vec<T> {
        let r = self.buf_rx.try_recv();
        let _ = self.buf_tx.send((buf, done));

        match r {
            Ok(r) => return r,
            _ => (),
        };

        if !self.second_buf {
            self.second_buf = true;
            Vec::with_capacity(self.buffer_size)
        } else if !done {
            self.buf_rx.recv().unwrap()
        } else {
            Vec::new()
        }
    }
}

/// Sort buffered items, break large buffer into chunks.
/// Serialize, compress and write the each chunk. The
/// file manager maintains the index data.
struct ShardWriterThread<T, K, S> {
    chunk_size: usize,
    writer: FileManager<K>,
    buf_rx: Receiver<(Vec<T>, bool)>,
    buf_tx: Sender<Vec<T>>,
    serialize_buffer: Vec<u8>,
    compress_buffer: Vec<u8>,
    phantom: PhantomData<S>,
}

impl<T, K, S> ShardWriterThread<T, K, S>
where
    T: Send + Serialize,
    K: Ord + Serialize,
    S: SortKey<T, K>,
{
    fn new(
        chunk_size: usize,
        writer: FileManager<K>,
        buf_rx: Receiver<(Vec<T>, bool)>,
        buf_tx: Sender<Vec<T>>,
    ) -> ShardWriterThread<T, K, S> {
        ShardWriterThread {
            chunk_size,
            writer,
            buf_rx,
            buf_tx,
            serialize_buffer: Vec::new(),
            compress_buffer: Vec::new(),
            phantom: PhantomData,
        }
    }

    fn process(&mut self) -> Result<usize, Error> {
        let mut n_items = 0;

        loop {
            // Get the next buffer to process
            let (mut buf, done) = self.buf_rx.recv()?;
            n_items += buf.len();

            // Sort by sort key
            buf.sort_by_key(S::sort_key);

            // Write out the buffer chunks
            for c in buf.chunks(self.chunk_size) {
                self.write_chunk(c)?;
            }

            // Done with all the items
            buf.clear();

            // Send the buffer back to be reused
            if done {
                break;
            } else {
                // The receiver may have hung up, don't worry.
                let _ = self.buf_tx.send(buf);
            }
        }

        self.write_index_block()?;
        Ok(n_items)
    }

    #[cfg(feature = "lz4")]
    fn get_encoder(buffer: &mut Vec<u8>) -> Result<lz4::Encoder<&mut Vec<u8>>, Error> {
        let buf = lz4::EncoderBuilder::new().build(buffer)?;
        Ok(buf)
    }

    #[cfg(not(feature = "lz4"))]
    fn get_encoder(buffer: &mut Vec<u8>) -> flate2::write::ZlibEncoder<&mut Vec<u8>> {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        ZlibEncoder::new(buffer, Compression::Fast).unwrap()
    }

    fn write_chunk(&mut self, items: &[T]) -> Result<usize, Error> {
        self.serialize_buffer.clear();
        self.compress_buffer.clear();
        let bounds = (S::sort_key(&items[0]), S::sort_key(&items[items.len() - 1]));

        serialize_into(&mut self.serialize_buffer, items)?;

        {
            use std::io::Write;
            let mut encoder = Self::get_encoder(&mut self.compress_buffer)?;
            encoder.write(&self.serialize_buffer)?;
            encoder.finish();
        }

        self.writer
            .write_block(bounds, items.len(), &self.compress_buffer)
    }

    /// Write out the shard positioning data
    fn write_index_block(&mut self) -> Result<(), Error> {
        let mut buf = Vec::new();

        serialize_into(&mut buf, &self.writer.regions)?;

        let index_block_position = self.writer.cursor;
        let index_block_size = buf.len();

        write_at(
            &self.writer.file.as_raw_fd(),
            index_block_position as u64,
            buf.as_slice(),
        )?;

        self.writer.file.seek(SeekFrom::Start(
            (index_block_position + index_block_size) as u64,
        ))?;
        self.writer.file.write_u64::<BigEndian>(0 as u64)?;
        self.writer
            .file
            .write_u64::<BigEndian>(index_block_position as u64)?;
        self.writer
            .file
            .write_u64::<BigEndian>(index_block_size as u64)?;
        Ok(())
    }
}

/// A handle that is used to send data to a `ShardWriter`. Each thread that is producing data
/// needs it's own ShardSender. A `ShardSender` can be obtained with the `get_sender` method of
/// `ShardWriter`.  ShardSender implement clone.
pub struct ShardSender<T: Send> {
    tx: Sender<Option<Vec<T>>>,
    buffer: Vec<T>,
    buf_size: usize,
}

impl<T: Send> ShardSender<T> {
    fn new<K, S>(writer: &ShardWriter<T, K, S>) -> ShardSender<T> {
        let new_tx = writer.helper.tx.clone();
        let buffer = Vec::with_capacity(writer.sender_buffer_size);

        ShardSender {
            tx: new_tx,
            buffer,
            buf_size: writer.sender_buffer_size,
        }
    }

    /// Send an item to the shard file
    pub fn send(&mut self, item: T) {
        let send = {
            self.buffer.push(item);
            self.buffer.len() == self.buf_size
        };

        if send {
            let mut send_buf = Vec::with_capacity(self.buf_size);
            std::mem::swap(&mut send_buf, &mut self.buffer);
            self.tx.send(Some(send_buf)).unwrap();
        }
    }

    /// Signal that you've finished sending items to this `ShardSender`. `finished` will called
    /// if the `ShardSender` is dropped.
    pub fn finished(&mut self) {
        if !self.buffer.is_empty() {
            let mut send_buf = Vec::new();
            std::mem::swap(&mut send_buf, &mut self.buffer);
            self.tx.send(Some(send_buf)).unwrap();
        }
    }
}

impl<T: Send + Serialize> Clone for ShardSender<T> {
    fn clone(&self) -> Self {
        let new_tx = self.tx.clone();
        let buffer = Vec::with_capacity(self.buf_size);

        ShardSender {
            tx: new_tx,
            buffer,
            buf_size: self.buf_size,
        }
    }
}

impl<T: Send> Drop for ShardSender<T> {
    fn drop(&mut self) {
        self.finished();
    }
}

/// Read a shardio file
struct ShardReaderSingle<T, K, S = DefaultSort> {
    file: File,
    index: Vec<ShardRecord<K>>,
    p1: PhantomData<T>,
    p2: PhantomData<S>,
}

impl<T, K, S> ShardReaderSingle<T, K, S>
where
    T: DeserializeOwned,
    K: Clone + Ord + DeserializeOwned,
    S: SortKey<T, K>,
{
    /// Open a shard file that stores `T` items.
    fn open<P: AsRef<Path>>(path: P) -> ShardReaderSingle<T, K, S> {
        let mut f = File::open(path).unwrap();

        let mut index = Self::read_index_block(&mut f);
        index.sort();

        ShardReaderSingle {
            file: f,
            index,
            p1: PhantomData,
            p2: PhantomData,
        }
    }

    /// Read shard index
    fn read_index_block(file: &mut File) -> Vec<ShardRecord<K>> {
        let _ = file.seek(SeekFrom::End(-24)).unwrap();
        let _num_shards = file.read_u64::<BigEndian>().unwrap() as usize;
        let index_block_position = file.read_u64::<BigEndian>().unwrap();
        let _ = file.read_u64::<BigEndian>().unwrap();
        file.seek(SeekFrom::Start(index_block_position as u64))
            .unwrap();
        deserialize_from(file).unwrap()
    }

    #[cfg(feature = "lz4")]
    fn get_decoder(buffer: &mut Vec<u8>) -> lz4::Decoder<&[u8]> {
        lz4::Decoder::new(buffer.as_slice()).unwrap()
    }

    #[cfg(not(feature = "lz4"))]
    fn get_decoder(buffer: &mut Vec<u8>) -> flate2::read::ZlibDecoder<&[u8]> {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        ZlibDecoder::new(buffer.as_slice())
    }

    pub fn read_range(&self, range: &Range<K>, data: &mut Vec<T>, buf: &mut Vec<u8>) {
        for rec in self
            .index
            .iter()
            .cloned()
            .filter(|x| range.intersects_shard(x))
        {
            buf.resize(rec.len_bytes, 0);
            let read_len = read_at(
                &self.file.as_raw_fd(),
                rec.offset as u64,
                buf.as_mut_slice(),
            ).unwrap();
            assert_eq!(read_len, rec.len_bytes);

            let mut decoder = Self::get_decoder(buf);
            let r: Vec<T> = deserialize_from(&mut decoder).unwrap();
            data.extend(r.into_iter().filter(|x| range.contains(&S::sort_key(x))));
        }

        data.sort_by_key(S::sort_key);
    }

    pub fn iter_range(&self, range: &Range<K>) -> ShardItemIter<T, K, S> {
        ShardItemIter::new(&self, range.clone())
    }

    fn read_shard(&self, rec: &ShardRecord<K>, buf: &mut Vec<u8>) -> Vec<T> {
        buf.resize(rec.len_bytes, 0);
        let read_len = read_at(
            &self.file.as_raw_fd(),
            rec.offset as u64,
            buf.as_mut_slice(),
        ).unwrap();
        assert_eq!(read_len, rec.len_bytes);

        let mut decoder = Self::get_decoder(buf);
        deserialize_from(&mut decoder).unwrap()
    }

    pub fn len(&self) -> usize {
        self.index.iter().map(|x| x.len_items).sum()
    }
}

struct MergeVec<T, K, S> {
    current_key: K,
    items: Vec<T>,
    phantom: PhantomData<S>,
}

impl<T, K, S> MergeVec<T, K, S>
where
    S: SortKey<T, K>,
    K: Ord,
{
    pub fn new(items: Vec<T>) -> MergeVec<T, K, S> {
        assert!(items.len() > 0);

        MergeVec {
            current_key: S::sort_key(&items[items.len() - 1]),
            items: items,
            phantom: PhantomData,
        }
    }

    pub fn pop(mut self) -> (T, Option<MergeVec<T, K, S>>) {
        let item = self.items.pop().unwrap();

        if self.items.len() == 0 {
            (item, None)
        } else {
            (
                item,
                Some(MergeVec {
                    current_key: S::sort_key(&self.items[self.items.len() - 1]),
                    items: self.items,
                    phantom: PhantomData,
                }),
            )
        }
    }
}

impl<T, K, S> Ord for MergeVec<T, K, S>
where
    K: Ord,
{
    fn cmp(&self, other: &MergeVec<T, K, S>) -> Ordering {
        self.current_key.cmp(&other.current_key)
    }
}

impl<T, K, S> PartialOrd for MergeVec<T, K, S>
where
    K: Ord,
{
    fn partial_cmp(&self, other: &MergeVec<T, K, S>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T, K, S> PartialEq for MergeVec<T, K, S>
where
    K: Ord,
{
    fn eq(&self, other: &MergeVec<T, K, S>) -> bool {
        self.current_key == other.current_key
    }
}

impl<T, K, S> Eq for MergeVec<T, K, S>
where
    K: Ord,
{
}

use std::cmp::Ordering;

/// Iterator of items from a single shardio reader
pub struct ShardItemIter<'a, T: 'a, K: 'a, S: 'a> {
    reader: &'a ShardReaderSingle<T, K, S>,
    buf: Vec<u8>,
    range: Range<K>,
    active_queue: MinMaxHeap<MergeVec<T, K, S>>,
    waiting_queue: MinMaxHeap<&'a ShardRecord<K>>,
}

impl<'a, T, K, S> ShardItemIter<'a, T, K, S>
where
    T: DeserializeOwned,
    K: Clone + Ord + DeserializeOwned,
    S: SortKey<T, K>,
{
    fn new(reader: &'a ShardReaderSingle<T, K, S>, range: Range<K>) -> ShardItemIter<'a, T, K, S> {
        let mut buf = Vec::new();

        let shards: Vec<&ShardRecord<K>> = reader
            .index
            .iter()
            .filter(|x| range.intersects_shard(x))
            .collect();

        let min_item = shards.iter().map(|x| x.start_key.clone()).min();
        let mut active_queue = MinMaxHeap::new();
        let mut waiting_queue = MinMaxHeap::new();

        for s in shards {
            if Some(s.start_key.clone()) == min_item {
                let mut items = reader.read_shard(s, &mut buf);
                items.reverse();
                let vec = MergeVec::new(items);
                active_queue.push(vec);
            } else {
                waiting_queue.push(s);
            }
        }

        ShardItemIter {
            reader,
            buf,
            range,
            active_queue,
            waiting_queue,
        }
    }

    /// What key is next among the active set
    fn peek_active_next(&self) -> Option<K> {
        let n = self.active_queue.peek_min();
        n.map(|v| v.current_key.clone())
    }

    /// Activate any relevant
    fn activate_shards(&mut self) {
        // What key would be next?
        let mut possible_next = self.peek_active_next();

        // Restore all the shards that start at or before this item, or the next usable shard
        while self.waiting_queue.peek_min().map_or(false, |shard| {
            Some(shard.start_key.clone()) <= possible_next
        }) || (possible_next.is_none() && self.waiting_queue.len() > 0)
        {
            let shard = self.waiting_queue.pop_min().unwrap();
            let mut items = self.reader.read_shard(shard, &mut self.buf);
            items.reverse();
            let vec = MergeVec::new(items);
            self.active_queue.push(vec);

            possible_next = self.peek_active_next();
        }
    }

    // Get the next item to return among active vecs
    fn next_active(&mut self) -> Option<T> {
        self.active_queue.pop_min().map(|vec| {
            let (item, new_vec) = vec.pop();

            match new_vec {
                Some(v) => self.active_queue.push(v),
                _ => (),
            }
            item
        })
    }
}

impl<'a, T, K, S> Iterator for ShardItemIter<'a, T, K, S>
where
    T: DeserializeOwned,
    K: Clone + Ord + DeserializeOwned,
    S: SortKey<T, K>,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        loop {
            self.activate_shards();
            match self.next_active() {
                Some(v) => {
                    let c = self.range.cmp(&S::sort_key(&v));
                    match c {
                        Rorder::Before => (),
                        Rorder::Intersects => return Some(v),
                        Rorder::After => return None,
                    }
                }
                None => return None,
            }
        }
    }
}

/// Iterator over merged shardio files
pub struct MergeIterator<'a, T: 'a, K: 'a, S: 'a> {
    iterators: Vec<ShardItemIter<'a, T, K, S>>,
    next_values: Vec<Option<T>>,
    merge_heap: MinMaxHeap<(K, usize)>,
    phantom_k: PhantomData<K>,
    phantom_s: PhantomData<S>,
}

impl<'a, T, K, S> MergeIterator<'a, T, K, S>
where
    K: Clone + Ord,
    S: SortKey<T, K>,
    ShardItemIter<'a, T, K, S>: Iterator<Item = T>,
{
    fn new(mut iterators: Vec<ShardItemIter<'a, T, K, S>>) -> MergeIterator<'a, T, K, S> {
        let mut next_values = Vec::new();
        let mut merge_heap = MinMaxHeap::new();

        for (idx, itr) in iterators.iter_mut().enumerate() {
            let item = itr.next();

            // If we have a next item, add it's key to the heap
            item.as_ref().map(|ii| {
                let k = S::sort_key(ii);
                merge_heap.push((k, idx));
            });

            // Add current item of iterator to next_values
            next_values.push(item);
        }

        MergeIterator {
            iterators,
            next_values,
            merge_heap,
            phantom_k: PhantomData,
            phantom_s: PhantomData,
        }
    }
}

use std::iter::Iterator;

impl<'a, T: 'a, K: 'a, S: 'a> Iterator for MergeIterator<'a, T, K, S>
where
    K: Clone + Ord,
    S: SortKey<T, K>,
    ShardItemIter<'a, T, K, S>: Iterator<Item = T>,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        let next_itr = self.merge_heap.pop_min();

        next_itr.map(|(_, i)| {
            assert!(self.next_values[i].is_some());

            // Get next-next value for this iterator
            let mut next = self.iterators[i].next();

            // Push the next-next key onto the heap
            match next {
                Some(ref v) => self.merge_heap.push((S::sort_key(v), i)),
                _ => (),
            };

            // Swap the value to return with the next-next value
            std::mem::swap(&mut self.next_values[i], &mut next);

            // Return. Heap should not have pointers to exhausted iterators.
            next.unwrap()
        })
    }
}

/// Read from a collection of shardio files. The input data is merged to give
/// a single sorted view of the combined dataset. The input files must
/// be created with the same sort order `S` as they are read with.
pub struct ShardReader<T, K, S = DefaultSort> {
    readers: Vec<ShardReaderSingle<T, K, S>>,
}

impl<T, K, S> ShardReader<T, K, S>
where
    T: DeserializeOwned,
    K: Clone + Ord + DeserializeOwned,
    S: SortKey<T, K>,
{
    /// Open a single shard files into reader
    pub fn open<P: AsRef<Path>>(shard_file: P) -> ShardReader<T, K, S> {
        let mut readers = Vec::new();
        let reader = ShardReaderSingle::open(shard_file);
        readers.push(reader);

        ShardReader { readers }
    }

    /// Open a set of shard files into an aggregated reader
    pub fn open_set<P: AsRef<Path>>(shard_files: &[P]) -> ShardReader<T, K, S> {
        let mut readers = Vec::new();

        for p in shard_files {
            let reader = ShardReaderSingle::open(p);
            readers.push(reader);
        }

        ShardReader { readers }
    }

    /// Read data from the given `range` into `data` buffer. The `data` buffer is not cleared before adding items.
    pub fn read_range(&self, range: &Range<K>, data: &mut Vec<T>) {
        let mut buf = Vec::new();
        for r in &self.readers {
            r.read_range(range, data, &mut buf)
        }
    }

    /// Iterate over items in the given `range`
    pub fn iter_range<'a>(&'a self, range: &Range<K>) -> MergeIterator<'a, T, K, S> {
        let iters: Vec<_> = self.readers.iter().map(|r| r.iter_range(range)).collect();
        MergeIterator::new(iters)
    }

    /// Iterate over all items
    pub fn iter<'a>(&'a self) -> MergeIterator<'a, T, K, S> {
        self.iter_range(&Range::all())
    }

    /// Total number of items
    pub fn len(&self) -> usize {
        self.readers.iter().map(|r| r.len()).sum()
    }

    /// Generate `num_chunks` ranges covering the give `range`, each with a roughly equal numbers of elements.
    /// The ranges can be fed to `iter_range`
    pub fn make_chunks(&self, num_chunks: usize, range: &Range<K>) -> Vec<Range<K>> {
        assert!(num_chunks > 0);

        // Enumerate all the in-range known start locations in the dataset
        let mut starts = Vec::new();
        for r in &self.readers {
            for block in &r.index {
                if range.contains(&block.start_key) {
                    starts.push(block.start_key.clone());
                }
            }
        }
        starts.sort();

        // Divide the known start locations into chunks, and
        // use setup start positions.
        let mut chunk_starts = Vec::new();
        let chunks = starts.chunks(std::cmp::max(1, starts.len() / num_chunks));
        for (i, c) in chunks.enumerate() {
            let start = if i == 0 {
                range.start.clone()
            } else {
                Some(c[0].clone())
            };
            chunk_starts.push(start);
        }

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
}

#[cfg(test)]
mod shard_tests {
    use super::*;
    use std::collections::HashSet;
    use std::fmt::Debug;
    use std::hash::Hash;
    use std::iter::FromIterator;
    use tempfile;

    #[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Debug, PartialOrd, Ord, Hash)]
    struct T1 {
        a: u64,
        b: u32,
        c: u16,
        d: u8,
    }

    struct FieldDSort;
    impl SortKey<T1, u8> for FieldDSort {
        fn sort_key(item: &T1) -> u8 {
            item.d
        }
    }

    fn rand_items(n: usize) -> Vec<T1> {
        let mut items = Vec::new();

        for i in 0..n {
            let tt = T1 {
                a: (((i / 2) + (i * 10)) % 3 + (i * 5) % 2) as u64,
                b: i as u32,
                c: (i * 2) as u16,
                d: ((i % 5) * 10 + (if i % 10 > 7 { i / 10 } else { 0 })) as u8,
            };
            items.push(tt);
        }

        items
    }

    #[test]
    fn test_shard_round_trip() {
        // Test different buffering configurations
        check_round_trip(10, 20, 0, 1 << 8);
        check_round_trip(10, 20, 40, 1 << 8);
        check_round_trip(1024, 16, 2 << 14, 1 << 18);
        check_round_trip(4096, 8, 2048, 1 << 18);
        check_round_trip(128, 4, 1024, 1 << 12);
        check_round_trip(50, 2, 256, 1 << 16);
        check_round_trip(10, 20, 40, 1 << 14);
    }

    #[test]
    fn test_shard_round_trip_sort_key() {
        // Test different buffering configurations
        check_round_trip_sort_key(10, 20, 0, 256, true);
        check_round_trip_sort_key(10, 20, 40, 256, true);
        check_round_trip_sort_key(1024, 16, 2 << 14, 1 << 18, true);
        check_round_trip_sort_key(4096, 8, 2048, 1 << 18, true);
        check_round_trip_sort_key(128, 4, 1024, 1 << 12, true);
        check_round_trip_sort_key(50, 2, 256, 1 << 16, true);
        check_round_trip_sort_key(10, 20, 40, 1 << 14, true);

        check_round_trip_sort_key(64, 16, 1 << 17, 1 << 18, true);
        check_round_trip_sort_key(128, 16, 1 << 17, 1 << 18, true);
        check_round_trip_sort_key(64, 16, 1 << 16, 1 << 18, true);
        check_round_trip_sort_key(128, 16, 1 << 16, 1 << 18, true);
        check_round_trip_sort_key(64, 16, 1 << 15, 1 << 18, true);
        check_round_trip_sort_key(128, 16, 1 << 15, 1 << 18, true);
    }

    #[test]
    fn test_shard_round_trip_big() {
        // Play with these settings to test perf.
        check_round_trip_opt(1024, 64, 1 << 18, 1 << 20, false);
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
    }

    fn check_round_trip_opt(
        disk_chunk_size: usize,
        producer_chunk_size: usize,
        buffer_size: usize,
        n_items: usize,
        do_read: bool,
    ) {
        println!(
            "test round trip: disk_chunk_size: {}, producer_chunk_size: {}, n_items: {}",
            disk_chunk_size, producer_chunk_size, n_items
        );

        let tmp = tempfile::NamedTempFile::new().unwrap();

        // Write and close file
        let true_items = {
            let manager: ShardWriter<T1, T1> = ShardWriter::new(
                tmp.path(),
                producer_chunk_size,
                disk_chunk_size,
                buffer_size,
            ).unwrap();
            let mut true_items = rand_items(n_items);

            // Sender must be closed
            {
                for chunk in true_items.chunks(n_items / 8) {
                    let mut sender = manager.get_sender();
                    for item in chunk {
                        sender.send(*item);
                    }
                }
            }
            true_items.sort();
            true_items
        };

        if do_read {
            // Open finished file
            let reader = ShardReader::<T1, T1>::open(tmp.path());

            let all_items = reader.iter_range(&Range::all()).collect();
            set_compare(&true_items, &all_items);

            if !(true_items == all_items) {
                println!("true len: {:?}", true_items.len());
                println!("round trip len: {:?}", all_items.len());
                assert_eq!(&true_items, &all_items);
            }

            for rc in [1, 3, 8, 15].iter() {
                // Open finished file & test chunked reads
                let set_reader = ShardReader::<T1, T1>::open(&tmp.path());
                let mut all_items_chunks = Vec::new();

                // Read in chunks
                let chunks = set_reader.make_chunks(*rc, &Range::all());
                for c in chunks {
                    let itr = set_reader.iter_range(&c);
                    all_items_chunks.extend(itr);
                }
                assert_eq!(&true_items, &all_items_chunks);
            }
        }
    }

    fn check_round_trip_sort_key(
        disk_chunk_size: usize,
        producer_chunk_size: usize,
        buffer_size: usize,
        n_items: usize,
        do_read: bool,
    ) {
        println!(
            "test round trip: disk_chunk_size: {}, producer_chunk_size: {}, n_items: {}",
            disk_chunk_size, producer_chunk_size, n_items
        );

        let tmp = tempfile::NamedTempFile::new().unwrap();

        // Write and close file
        let true_items = {
            let manager: ShardWriter<T1, u8, FieldDSort> = ShardWriter::new(
                tmp.path(),
                producer_chunk_size,
                disk_chunk_size,
                buffer_size,
            ).unwrap();
            let mut true_items = rand_items(n_items);

            // Sender must be closed
            {
                for chunk in true_items.chunks(n_items / 8) {
                    let mut sender = manager.get_sender();
                    for item in chunk {
                        sender.send(*item);
                    }
                }
            }
            true_items.sort_by_key(|x| x.d);
            true_items
        };

        if do_read {
            // Open finished file
            let reader = ShardReader::<T1, u8, FieldDSort>::open(tmp.path());

            let all_items = reader.iter_range(&Range::all()).collect();
            set_compare(&true_items, &all_items);

            // Open finished file & test chunked reads
            let set_reader = ShardReader::<T1, u8, FieldDSort>::open(tmp.path());
            let mut all_items_chunks = Vec::new();

            for rc in &[1, 4, 7, 12, 15, 21, 65] {
                all_items_chunks.clear();
                let chunks = set_reader.make_chunks(*rc, &Range::all());
                for c in chunks {
                    let itr = set_reader.iter_range(&c);
                    all_items_chunks.extend(itr);
                }
                set_compare(&true_items, &all_items_chunks);
            }
        }
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
}
