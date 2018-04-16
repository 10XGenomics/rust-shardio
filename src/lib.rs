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
//!         let manager: ShardWriter<Test, Test> = ShardWriter::new(filename, 64, 256, 1<<16);
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
//!     let reader = ShardReaderSet::<Test, Test>::open(&vec![filename]);
//! 
//!     // Read back data
//!     let mut all_items = Vec::new();
//! 
//!     let chunks = reader.make_chunks(5);
//!     for c in chunks {
//!         reader.read_range(&c, &mut all_items);
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

extern crate byteorder;
extern crate libc;
extern crate bincode;
extern crate serde;
extern crate failure;
extern crate flate2;
extern crate crossbeam_channel;

#[cfg(feature = "lz4")]
extern crate lz4;

#[cfg(test)]
extern crate tempfile;



use std::fs::File;
use std::io::{self, Seek, SeekFrom};
use std::os::unix::io::{RawFd, AsRawFd};

use crossbeam_channel::{bounded, Sender, Receiver};
use std::path::{Path};

use std::thread;
use std::thread::JoinHandle;
use std::marker::PhantomData;

use serde::ser::Serialize;
use serde::de::{DeserializeOwned};

use bincode::{serialize_into, deserialize_from};
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};

use libc::{pread, pwrite, c_void, off_t, size_t, ssize_t};

use failure::Error;

pub mod pmap;
pub mod range;
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
        pread(*fd, buf.as_mut_ptr() as *mut c_void, buf.len() as size_t, pos as off_t)
    })
}


fn write_at(fd: &RawFd, pos: u64, buf: &[u8]) -> io::Result<usize> {
    err(unsafe {
        pwrite(*fd, buf.as_ptr() as *const c_void, buf.len() as size_t, pos as off_t)
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


impl<K: Ord + Clone> ShardRecord<K> {
    fn range(&self) -> Range<K> {
        Range::new(self.start_key.clone(), self.end_key.clone())
    }
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
    pub fn new<P: AsRef<Path>>(path: P) -> FileManager<K> {

        let file = File::create(path).unwrap();

        FileManager {
            cursor: 4096,
            regions: Vec::new(),
            file,
        }
    }

    fn write_block(&mut self, range: (K, K), n_items: usize, data: &[u8]) -> Result<usize, Error> {

        let cur_offset = self.cursor;
        let reg = 
            ShardRecord {
                offset: self.cursor,
                start_key: range.0,
                end_key: range.1,
                len_bytes: data.len(),
                len_items: n_items 
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
impl<T> SortKey<T,T> for DefaultSort where T:Ord + Clone {
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
pub struct ShardWriter<T,K=T,S=DefaultSort> {
    helper: ShardWriterHelper<T>,
    sender_buffer_size: usize,
    p1: PhantomData<K>,
    p2: PhantomData<S>,
}

struct ShardWriterHelper<T> {
    tx: Sender<Option<Vec<T>>>,
    err_rx: Receiver<Error>,
    h1: Option<JoinHandle<()>>,
    h2: Option<JoinHandle<()>>,
}


impl<T: 'static + Send + Serialize, K: Ord + Serialize, S: SortKey<T,K>> ShardWriter<T,K,S> {
    /// Create a writer for storing data items of type `T`.
    /// # Arguments
    /// * `path` - Path to newly created output file
    /// * `sender_buffer_size` - number of items to buffer on the sending thread before transferring data to the writer
    /// * `disk_chunk_size` - number of items to store in chunk on disk. Controls the tradeoff between indexing overhead and the granularity 
    ///                       can be read at.
    /// * `item_buffer_size` - number of items to buffer before writing data to disk. More buffering causes the data for a given interval to be 
    ///                        spread over fewer disk chunks, but requires more memory.
    pub fn new<P: AsRef<Path>>(path: P, sender_buffer_size: usize, disk_chunk_size: usize, item_buffer_size: usize) -> ShardWriter<T, K, S> {
        let (send, recv) = bounded(4);

        // Ping-pong of the 2 item buffer between the thread the accumulates the items, 
        // and the thread that does the IO. Need 2 slots to avoid a deadlock.
        let (to_writer_send, to_writer_recv) = bounded(2);
        let (to_buffer_send, to_buffer_recv) = bounded(2);
        let (err_tx, err_rx) = bounded(16);

        // Divide buffer size by 2 -- the get swaped between buffer thread and writer thread
        let mut sbt = ShardBufferThread::<T>::new(sender_buffer_size, item_buffer_size>>1, recv, to_writer_send, to_buffer_recv);
        let p2 = path.as_ref().to_owned();

        let h1 = thread::spawn(move || { sbt.process() });
        let h2 = thread::spawn(
            move || { 
                let writer = FileManager::<K>::new(p2);
                let mut swt = ShardWriterThread::<_,_,S>::new(disk_chunk_size, writer, err_tx, to_writer_recv, to_buffer_send);
                swt.process() });

        ShardWriter { 
            helper: ShardWriterHelper {tx: send, err_rx: err_rx, h1: Some(h1), h2: Some(h2) }, 
            sender_buffer_size,
            p1: PhantomData,
            p2: PhantomData 
        }
    }

    /// Get a `ShardSender`. It can be sent to another thread that is generating data.
    pub fn get_sender(&self) -> ShardSender<T>
    {
        ShardSender::new(self)
    }
}

impl<T, K, S> Drop for ShardWriter<T,K,S> 
{
    fn drop(&mut self) {
        self.helper.tx.send(None).unwrap();
        self.helper.h1.take().map(|x| x.join());
        self.helper.h2.take().map(|x| x.join());
    }
}

/// Manage the buffering and writing of items for a subset of the shard space.
struct ShardBufferThread<T> where T: Send {
    chunk_size: usize,
    buffer_size: usize,
    rx: Receiver<Option<Vec<T>>>,
    buf_tx: Sender<(Vec<T>, bool)>,
    buf_rx: Receiver<Vec<T>>,
    second_buf: bool,
}


impl<T> ShardBufferThread<T> where T: Send {
    fn new(
        chunk_size: usize,
        buffer_size: usize,
        rx: Receiver<Option<Vec<T>>>,
        buf_tx: Sender<(Vec<T>, bool)>,
        buf_rx: Receiver<Vec<T>>) -> ShardBufferThread<T>{

        ShardBufferThread {
            chunk_size,
            buffer_size, 
            rx, buf_rx, buf_tx,
            second_buf: false,
        }
    }

    fn process(&mut self) {

        let mut buf = Vec::with_capacity(self.buffer_size);

        loop {
            match self.rx.recv() {
                Ok(Some(v)) => {
                    buf.extend(v);
                    if buf.len() + self.chunk_size > self.buffer_size {
                        buf = self.send_buf(buf, false);
                    }
                },
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
            _ => ()
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


/// Manage the buffering and writing of items for a subset of the shard space.
struct ShardWriterThread<T, K, S> {
    chunk_size: usize,
    writer: FileManager<K>,
    buf_rx: Receiver<(Vec<T>, bool)>,
    buf_tx: Sender<Vec<T>>,
    err_tx: Sender<Error>,
    write_buffer: Vec<u8>,
    phantom: PhantomData<S>,
}


impl<T, K, S> ShardWriterThread<T, K, S> where T: Send + Serialize, K: Ord + Serialize, S: SortKey<T, K> {

    fn new(
        chunk_size: usize,   
        writer: FileManager<K>,
        err_tx: Sender<Error>,
        buf_rx: Receiver<(Vec<T>, bool)>,
        buf_tx: Sender<Vec<T>>) -> ShardWriterThread<T, K, S>{

        ShardWriterThread {
            chunk_size,
            writer,
            err_tx,
            buf_rx,
            buf_tx,
            write_buffer: Vec::new(),
            phantom: PhantomData,
        }
    }

    fn process(&mut self) {
        let v = self._process();
        match v {
            Ok(_) => (),
            Err(e) => { println!("{}", e); let _ = self.err_tx.send(e); }
        }
    }

    fn _process(&mut self) -> Result<usize, Error> {
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
    fn get_encoder(buffer: &mut Vec<u8>) -> Result<lz4::Encoder<&mut Vec<u8>>, Error>
    {
        let buf = lz4::EncoderBuilder::new().build(buffer)?;
        Ok(buf)
    }

    #[cfg(not(feature = "lz4"))]
    fn get_encoder(buffer: &mut Vec<u8>) -> flate2::write::ZlibEncoder<&mut Vec<u8>>
    {
        use flate2::Compression;
        use flate2::write::ZlibEncoder;
        ZlibEncoder::new(buffer, Compression::Fast).unwrap()
    }

    fn write_chunk(&mut self, items: &[T]) -> Result<usize, Error>
    {
        self.write_buffer.clear();
        let bounds = (S::sort_key(&items[0]), S::sort_key(&items[items.len() - 1]));

        {
            let mut encoder = Self::get_encoder(&mut self.write_buffer)?;
            serialize_into(&mut encoder, items)?;
            encoder.finish();
        }

        self.writer.write_block(bounds, items.len(), &self.write_buffer)
    }


    /// Write out the shard positioning data
    fn write_index_block(&mut self) -> Result<(), Error> {

        let mut buf = Vec::new();

        serialize_into(&mut buf, &self.writer.regions)?;

        let index_block_position = self.writer.cursor;
        let index_block_size = buf.len();

        write_at(&self.writer.file.as_raw_fd(), index_block_position as u64, buf.as_slice())?;

        self.writer.file.seek(SeekFrom::Start((index_block_position + index_block_size) as u64))?;
        self.writer.file.write_u64::<BigEndian>(0 as u64)?;
        self.writer.file.write_u64::<BigEndian>(index_block_position as u64)?;
        self.writer.file.write_u64::<BigEndian>(index_block_size as u64)?;
        Ok(())
    }
}


/// A handle that is used to send data to the shard file. Each thread
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
            buffer: buffer,
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

    /// Signal that you've finished sending items to this `ShardSender`. Also called 
    /// if the `ShardSender` is dropped.
    pub fn finished(&mut self) {
        if self.buffer.len() > 0 {
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

        ShardSender{
            tx: new_tx,
            buffer: buffer,
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
pub struct ShardReader<T, K, S = DefaultSort> {
    file: File,
    index: Vec<ShardRecord<K>>,
    p1: PhantomData<T>,
    p2: PhantomData<S>,
}

impl<T, K, S> ShardReader<T, K, S> 
    where T: DeserializeOwned, K: Clone + Ord + DeserializeOwned, S: SortKey<T,K> {
    /// Open a shard file that stores `T` items.
    pub fn open<P: AsRef<Path>>(path: P) -> ShardReader<T, K, S> {
        let mut f = File::open(path).unwrap();

        let mut index = Self::read_index_block(&mut f);
        index.sort();

        ShardReader {
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
        file.seek(SeekFrom::Start(index_block_position as u64)).unwrap();
        deserialize_from(file).unwrap()
    }


    #[cfg(feature = "lz4")]
    fn get_decoder(buffer: &mut Vec<u8>) -> lz4::Decoder<&[u8]>
    {
        lz4::Decoder::new(buffer.as_slice()).unwrap()
    }

    #[cfg(not(feature = "lz4"))]
    fn get_decoder(buffer: &mut Vec<u8>) -> flate2::read::ZlibDecoder<&[u8]>
    {
        use flate2::Compression;
        use flate2::write::ZlibEncoder;
        ZlibDecoder::new(buffer.as_slice())
    }


    pub fn read_range(&self, range: &Range<K>, data: &mut Vec<T>, buf: &mut Vec<u8>) {

        for rec in self.index.iter().cloned().filter(|x| x.range().intersects(range)) {
            buf.resize(rec.len_bytes, 0);
            let read_len = read_at(&self.file.as_raw_fd(), rec.offset as u64, buf.as_mut_slice()).unwrap();
            assert_eq!(read_len, rec.len_bytes);
                    
            let mut decoder = Self::get_decoder(buf);
            let r: Vec<T> = deserialize_from(&mut decoder).unwrap();
            data.extend(r.into_iter().filter(|x| range.contains(&S::sort_key(x))));
        }

        data.sort_by_key(S::sort_key);
    }

    pub fn data_range(&self) -> Range<K> {
        let start = self.index.iter().map(|x| x.start_key.clone()).min();
        let end = self.index.iter().map(|x| x.end_key.clone()).max();
        Range { start, end }
    }

    pub fn len(&self) -> usize {
        self.index.iter().map(|x| x.len_items).sum()
    }
}

/// Read from a collection of shard io files.
pub struct ShardReaderSet<T, K, S = DefaultSort> {
    readers: Vec<ShardReader<T, K, S>>
}


impl<T, K, S> ShardReaderSet<T, K, S> 
 where T: DeserializeOwned, K: Clone + Ord + DeserializeOwned, S: SortKey<T,K> {
    /// Open a set of shard files into a aggregated reader
    pub fn open<P: AsRef<Path>>(shard_files: &[P]) -> ShardReaderSet<T, K, S> {
        let mut readers = Vec::new();

        for p in shard_files {
            let reader = ShardReader::open(p);
            readers.push(reader);
        }

        ShardReaderSet{ 
            readers
        }
    }

    /// Read data the given `range` into `data` buffer
    pub fn read_range(&self, range: &Range<K>, data: &mut Vec<T>) {
        let mut buf = Vec::new();
        for r in &self.readers {
            r.read_range(range, data, &mut buf)
        }
    }

    /// Total number of items
    pub fn len(&self) -> usize {
        self.readers.iter().map(|r| r.len()).sum()
    }

    /// Generate `num_chunks` ranges with roughly equal numbers of elements.
    pub fn make_chunks(&self, num_chunks: usize) -> Vec<Range<K>> {
        let mut starts = Vec::new();
        for r in &self.readers {
            for block in r.index.iter() {
                let r = block.range();
                match r.start { Some(ref v) => starts.push(v.clone()), _ => () };
            }
        }
        starts.sort();

        let mut chunk_starts = Vec::new();
        let chunks = starts.chunks(std::cmp::max(1, starts.len() / num_chunks));
        for (i, c) in chunks.enumerate() {
            let start = 
                if i == 0 {
                    None
                } else {
                    Some(c[0].clone())
                };
            chunk_starts.push(start);
        }

        let mut chunks = Vec::new();
        for (i, start) in chunk_starts.iter().enumerate() {
            let end = 
                if i+1 == chunk_starts.len() {
                    None
                } else {
                    chunk_starts[i+1].clone()
                };

            chunks.push(Range { start: start.clone(), end: end })
        }

        chunks
    }
}

#[cfg(test)] 
mod shard_tests {
    use tempfile;
    use super::*;
    use std::collections::HashSet;
    use std::fmt::Debug;
    use std::iter::FromIterator;
    use std::hash::Hash;

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
                a: ((i/2) + (i*10) % 128 + (i*6) % 64) as u64,
                b: i as u32,
                c: (i * 2) as u16,
                d: i as u8,
            }; 
            items.push(tt);
        }

        items
    }

    #[test]
    fn test_shard_round_trip() {

        // Test different buffering configurations
        check_round_trip(10,   20,    40,  1<<8);
        check_round_trip(1024, 16, 2<<14,  1<<18);
        check_round_trip(4096,  8,  2048,  1<<18);
        check_round_trip(128,   4,  1024,  1<<12);
        check_round_trip(50,    2,   256,  1<<16);
        check_round_trip(10,   20,    40,  1<<14);
    }

    #[test]
    fn test_shard_round_trip_sort_key() {

        // Test different buffering configurations
        check_round_trip_sort_key(10,   20,    40,  256, true);
        check_round_trip_sort_key(1024, 16, 2<<14,  1<<18, true);
        check_round_trip_sort_key(4096,  8,  2048,  1<<18, true);
        check_round_trip_sort_key(128,   4,  1024,  1<<12, true);
        check_round_trip_sort_key(50,    2,   256,  1<<16, true);
        check_round_trip_sort_key(10,   20,    40,  1<<14, true);
    }


    #[test]
    fn test_shard_round_trip_big() {
        // Play with these settings to test perf.
        check_round_trip_opt(1024, 64,  2<<20,  1<<20, false);
    }

    fn check_round_trip(disk_chunk_size: usize, producer_chunk_size: usize, buffer_size: usize, n_items: usize) {
          check_round_trip_opt(disk_chunk_size, producer_chunk_size, buffer_size, n_items, true) 
    }
  
    fn check_round_trip_opt(disk_chunk_size: usize, producer_chunk_size: usize, buffer_size: usize, n_items: usize, do_read: bool) {
        
        println!("test round trip: disk_chunk_size: {}, producer_chunk_size: {}, n_items: {}", disk_chunk_size, producer_chunk_size, n_items);

        let tmp = tempfile::NamedTempFile::new().unwrap();

        // Write and close file
        let true_items = {
            let manager: ShardWriter<T1, T1> = ShardWriter::new(tmp.path(), producer_chunk_size, disk_chunk_size, buffer_size);
            let mut true_items = rand_items(n_items);

            // Sender must be closed
            {
                let mut sender = manager.get_sender();
                for item in true_items.iter() {
                    sender.send(*item);
                }
            }
            true_items.sort();
            true_items
        };

        if do_read {
            // Open finished file
            let reader = ShardReader::<T1, T1>::open(tmp.path());

            let mut all_items = Vec::new();
            let mut buf = Vec::new();
            reader.read_range(&Range::all(), &mut all_items, &mut buf);

            if !(true_items == all_items) {
                println!("true len: {:?}", true_items.len());
                println!("round trip len: {:?}", all_items.len());
                assert_eq!(&true_items, &all_items);
            }


            // Open finished file & test chunked reads
            let set_reader = ShardReaderSet::<T1, T1>::open(&vec![tmp.path()]);
            let mut all_items_chunks = Vec::new();

            let chunks = set_reader.make_chunks(5);
            for c in chunks {
                set_reader.read_range(&c, &mut all_items_chunks);
            }

            assert_eq!(&true_items, &all_items_chunks);
        }
    }

    fn check_round_trip_sort_key(disk_chunk_size: usize, producer_chunk_size: usize, buffer_size: usize, n_items: usize, do_read: bool) {
        
        println!("test round trip: disk_chunk_size: {}, producer_chunk_size: {}, n_items: {}", disk_chunk_size, producer_chunk_size, n_items);

        let tmp = tempfile::NamedTempFile::new().unwrap();

        // Write and close file
        let true_items = {
            let manager: ShardWriter<T1, u8, FieldDSort> = ShardWriter::new(tmp.path(), producer_chunk_size, disk_chunk_size, buffer_size);
            let mut true_items = rand_items(n_items);

            // Sender must be closed
            {
                let mut sender = manager.get_sender();
                for item in true_items.iter() {
                    sender.send(*item);
                }
            }
            true_items.sort_by_key(|x| x.d);
            true_items
        };

        if do_read {
            // Open finished file
            let reader = ShardReader::<T1, u8, FieldDSort>::open(tmp.path());

            let mut all_items = Vec::new();
            let mut buf = Vec::new();
            reader.read_range(&Range::all(), &mut all_items, &mut buf);
            set_compare(&true_items, &all_items);


            // Open finished file & test chunked reads
            let set_reader = ShardReaderSet::<T1, u8, FieldDSort>::open(&vec![tmp.path()]);
            let mut all_items_chunks = Vec::new();

            let chunks = set_reader.make_chunks(5);
            for c in chunks {
                set_reader.read_range(&c, &mut all_items_chunks);
            }

            set_compare(&true_items, &all_items_chunks);
        }
    }

    fn set_compare<T: Eq + Debug + Clone + Hash>(s1: &Vec<T>, s2: &Vec<T>) -> bool {
        let s1 : HashSet<T> = HashSet::from_iter(s1.iter().cloned());
        let s2 : HashSet<T> = HashSet::from_iter(s2.iter().cloned());

        if s1 == s2 {
            return true
        } else {
            let s1_minus_s2 = s1.difference(&s2);
            println!("in s1, but not s2: {:?}", s1_minus_s2);

            let s2_minus_s1 = s2.difference(&s1);
            println!("in s2, but not s1: {:?}", s2_minus_s1);

            return false;
        }
    }
}