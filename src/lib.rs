//! Efficiently write Rust structs to shard files from multiple threads.

#[macro_use]
extern crate serde_derive;

extern crate byteorder;
extern crate libc;
extern crate bincode;
extern crate serde;
extern crate lz4;

#[cfg(test)]
extern crate tempfile;

use std::fs::File;
use std::io::{Write, BufWriter, BufReader, Read};
use std::collections::HashMap;
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::{SyncSender, Receiver, RecvError};
use std::path::{Path, PathBuf};

use std::thread;
use std::thread::JoinHandle;
use std::slice;
use std::mem;

use std::fs::create_dir;
use std::fs::{self, DirEntry};
use std::io;

use std::marker::PhantomData;

pub mod shard;

/// 
pub struct ThreadProxyIterator<T: Send> {
    rx: Receiver<Option<T>>,
    done: bool
}

impl<T: Send> Iterator for ThreadProxyIterator<T> {
    type Item=T;

    fn next(&mut self) -> Option<T> {
        if self.done {
            return None
        }

        match self.rx.recv() {
            Ok(Some(v)) => Some(v),
            Ok(None) => {
                self.done = true;
                None
            },
            Err(_) => None
        }
    }
}

impl<T: 'static + Send> ThreadProxyIterator<T> {
    pub fn new<I: 'static + Send + Iterator<Item=T>>(itr: I, buf: usize) -> ThreadProxyIterator<T> {
        let (tx, rx) = sync_channel::<Option<T>>(buf);
        let handle = thread::spawn(move || {
            for item in itr {
                match tx.send(Some(item)) {
                    Err(_) => return,
                    _ => (),
                }
            } 

            tx.send(None).unwrap();
        });

        ThreadProxyIterator {
            rx: rx,
            done: false
        }     
    }
}



pub struct ThreadProxyWriter<T: Send + Write> {
    buf_size: usize,
    buf: Vec<u8>,
    thread_handle: Option<JoinHandle<Result<usize, RecvError>>>,
    tx: SyncSender<Option<Vec<u8>>>,
    phantom: PhantomData<T>,
}

impl<T: 'static + Send + Write> ThreadProxyWriter<T> {
    pub fn new(mut writer: T, buffer_size: usize) -> ThreadProxyWriter<T> {
        let (tx, rx) = sync_channel::<Option<Vec<u8>>>(10);

        let handle = thread::spawn(move || {
            let mut total = 0;
            loop {
                match rx.recv() {
                    Ok(Some(data)) => {
                        writer.write(data.as_slice());
                        total += data.len();
                    },
                    Ok(None) => break,
                    Err(e) => return Err(e),
                }
            }

            Ok(total)
        });

        ThreadProxyWriter {
            buf_size: buffer_size,
            buf: Vec::with_capacity(buffer_size),
            thread_handle: Some(handle),
            tx: tx,
            phantom: PhantomData,
        }
    }
}

impl<T: Send + Write> Write for ThreadProxyWriter<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() + self.buf.len() > self.buf.capacity() {
            let old_buf = std::mem::replace(&mut self.buf, Vec::with_capacity(self.buf_size));
            self.tx.send(Some(old_buf));
        }

        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let old_buf = std::mem::replace(&mut self.buf, Vec::with_capacity(self.buf_size));
        self.tx.send(Some(old_buf));
        Ok(())
    }
}

impl<T: Send + Write> Drop for ThreadProxyWriter<T> {
    fn drop(&mut self) {
        self.flush();
        self.tx.send(None);
        self.thread_handle.take().map(|th| th.join());
    }
}



// one possible implementation of fs::walk_dir only visiting files
fn visit_dirs(dir: &Path, cb: &Fn(&DirEntry)) -> io::Result<()> {
    if try!(fs::metadata(dir)).is_dir() {
        for entry in try!(fs::read_dir(dir)) {
            let entry = try!(entry);
            if try!(fs::metadata(entry.path())).is_dir() {
                try!(visit_dirs(&entry.path(), cb));
            } else {
                cb(&entry);
            }
        }
    }
    Ok(())
}


#[cfg(test)]
mod pod_tests {
    use std::path::Path;
    use std::io::{Read, Write};
    use std::fs::{File, remove_dir_all};
    use tempfile;

    #[derive(Copy, Clone, Eq, PartialEq)]
    struct T1 {
        a: u64,
        b: u32,
        c: u16,
        d: u8,
    }

    #[test]
    fn thread_iterate_test() {

        let it1 = (0..100).map(|x| x*x);

        let it2 = (0..100).map(|x| x*x);
        let thread_it2 = super::ThreadProxyIterator::new(it2, 10);

        let res1 = it1.collect::<Vec<_>>();
        let res2 = thread_it2.collect::<Vec<_>>();

        assert_eq!(res1, res2);
    }


    #[test]
    fn thread_write_test() {

        let tmp1 = tempfile::NamedTempFile::new().unwrap();
        let tmp2 = tempfile::NamedTempFile::new().unwrap();

        {
            let mut w1 = File::create(tmp1.path()).unwrap();

            let mut w2 = File::create(tmp2.path()).unwrap();
            let mut p2 = super::ThreadProxyWriter::new(w2, 4096);

            for i in 0 .. 1000000 {
                let cc = format!("a: {}, b: {}\n", i, i+1);
                w1.write(cc.as_bytes());
                p2.write(cc.as_bytes());            
            }
        }

        let mut f1 = File::open(tmp1.path()).unwrap();
        let mut v1 = Vec::new();
        f1.read_to_end(&mut v1);

        let mut f2 = File::open(tmp2.path()).unwrap();
        let mut v2 = Vec::new();
        f2.read_to_end(&mut v2);

        assert_eq!(v1, v2);
    }
}