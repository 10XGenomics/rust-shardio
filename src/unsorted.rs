use anyhow::Error;
use bincode::deserialize_from;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::cmp::min;
use std::fs::File;
use std::marker::PhantomData;

use std::path::Path;
use std::path::PathBuf;

use crate::compress::Decoder;
use crate::Compressor;
use crate::DefaultSort;
use crate::ReadAdapter;
use crate::ShardReaderSingle;
use crate::SortKey;

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
/// A group of `len_items` items, from shard `shard`, stored at position `offset`, using `len_bytes` bytes on-disk.
/// Similar to ShardRecord, just that we don't store the key. Used for `UnsortedShardReader`
struct KeylessShardRecord {
    offset: usize,
    len_bytes: usize,
    len_items: usize,
}

/// Read from a collection of shardio files in the order in which items are written without
/// considering the sort order.
///
/// Useful if you just want to iterate over the items irrespective of the ordering.
pub struct UnsortedShardReader<T, S = DefaultSort>
where
    S: SortKey<T>,
{
    /// Iterator over the shard files in the set, opening readers.
    shard_reader_iter:
        Box<dyn Iterator<Item = Result<UnsortedShardFileReader<T, S>, Error>> + Send + Sync>,

    /// Iterator over the current shard file.
    active_shard_reader: Option<UnsortedShardFileReader<T, S>>,
}

impl<T, S> UnsortedShardReader<T, S>
where
    T: DeserializeOwned,
    <S as SortKey<T>>::Key: Clone + Ord + DeserializeOwned + Send + Sync + 'static,
    S: SortKey<T>,
{
    /// Open a single shard file.
    pub fn open<P: AsRef<Path>>(shard_file: P) -> Self {
        Self::open_set(&[shard_file])
    }

    /// Open a set of shard files.
    pub fn open_set<P: AsRef<Path>>(shard_files: &[P]) -> Self {
        let reader_iter = shard_files
            .iter()
            .map(|f| f.as_ref().into())
            .collect::<Vec<_>>()
            .into_iter()
            .filter_map(|path: PathBuf| UnsortedShardFileReader::new(&path).transpose());
        Self {
            shard_reader_iter: Box::new(reader_iter),
            active_shard_reader: None,
        }
    }

    /// Compute the total number of elements in this set of shard files.
    pub fn len<P: AsRef<Path>>(shard_files: &[P]) -> Result<usize, Error> {
        // Create a set reader, and consume all the files, just getting counts.
        let files_reader = Self::open_set(shard_files);
        let mut count = 0;
        for file in files_reader.shard_reader_iter {
            let file = file?;
            count += file.len();
        }
        Ok(count)
    }

    /// Skip the first count items that would be returned by iteration.
    /// This avoids reading anything into memory besides the indexes for each
    /// file, and the first chunk that wouldn't be skipped.
    ///
    /// Returns the number of items skipped. This will only ever be less than
    /// count if we exhausted all shard files.
    pub fn skip_lazy(&mut self, count: usize) -> Result<usize, Error> {
        let mut skipped = 0;
        loop {
            let Some(mut file_reader) = self.take_active_reader().transpose()? else {
                // Ran out of files.
                break;
            };
            let SkipResult {
                skipped: this_file_skipped,
                exhausted,
            } = file_reader.skip(count - skipped)?;
            skipped += this_file_skipped;
            if !exhausted {
                self.active_shard_reader = Some(file_reader);
                // If we didn't exhaust an entire file, we should be done.
                assert!(skipped == count);
                break;
            }
        }
        Ok(skipped)
    }

    /// Take the current active shard reader, or advance to the next.
    fn take_active_reader(&mut self) -> Option<Result<UnsortedShardFileReader<T, S>, Error>> {
        let reader = if let Some(reader) = self.active_shard_reader.take() {
            reader
        } else {
            // Advance to the next reader.
            match self.shard_reader_iter.next() {
                None => {
                    // Exhausted all files.
                    return None;
                }
                Some(Err(err)) => {
                    // Index read error.
                    return Some(Err(err));
                }
                Some(Ok(reader)) => reader,
            }
        };
        Some(Ok(reader))
    }
}

impl<T, S> Iterator for UnsortedShardReader<T, S>
where
    T: DeserializeOwned,
    <S as SortKey<T>>::Key: Clone + Ord + DeserializeOwned + Send + Sync + 'static,
    S: SortKey<T>,
{
    type Item = Result<T, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut reader = match self.take_active_reader() {
                Some(Ok(reader)) => reader,
                Some(Err(err)) => {
                    return Some(Err(err));
                }
                None => {
                    return None;
                }
            };
            match reader.next() {
                Ok(Some(item)) => {
                    // The reader wasn't exhausted, put it back for the next iteration.
                    self.active_shard_reader = Some(reader);
                    return Some(Ok(item));
                }
                Ok(None) => {
                    // Reader is exhausted, loop to next file.
                }
                Err(err) => {
                    // File read error, return it.
                    return Some(Err(err));
                }
            }
        }
    }
}

/// Read from a single shardio file in the order in which items are written without
/// considering the sort order.
struct UnsortedShardFileReader<T, S = DefaultSort>
where
    S: SortKey<T>,
{
    count: usize,
    file_index_iter: Box<dyn Iterator<Item = KeylessShardRecord> + Send + Sync>,
    shard_iter: Option<UnsortedShardIter<T>>,
    phantom: PhantomData<S>,
}

impl<T, S> UnsortedShardFileReader<T, S>
where
    T: DeserializeOwned,
    <S as SortKey<T>>::Key: Clone + Ord + DeserializeOwned + Send + Sync + 'static,
    S: SortKey<T>,
{
    /// Create a unsorted reader for a single shard file.
    /// Return Ok(None) if the specified shard file is empty.
    pub fn new(path: &Path) -> Result<Option<Self>, Error> {
        let reader = ShardReaderSingle::<T, S>::open(path)?;
        let count = reader.len();
        let mut file_index_iter = reader.index.into_iter().map(|r| KeylessShardRecord {
            offset: r.offset,
            len_bytes: r.len_bytes,
            len_items: r.len_items,
        });
        let Some(first_index_entry) = file_index_iter.next() else {
            // File is empty.
            return Ok(None);
        };
        Ok(Some(Self {
            count,
            shard_iter: Some(UnsortedShardIter::new(
                reader.file,
                first_index_entry,
                reader.compressor,
            )?),
            file_index_iter: Box::new(file_index_iter),
            phantom: Default::default(),
        }))
    }

    /// Return the total number of items in this shard file.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Get the next item out of this file.
    pub fn next(&mut self) -> Result<Option<T>, Error> {
        loop {
            // Get the next item if we still have a shard iterator.
            let Some(mut shard_iter) = self.shard_iter.take() else {
                return Ok(None);
            };
            if let Some(item) = shard_iter.next()? {
                self.shard_iter = Some(shard_iter);
                return Ok(Some(item));
            }

            // Advance to the next shard, if there is one.
            let Some(next_index_record) = self.file_index_iter.next() else {
                return Ok(None);
            };

            self.shard_iter = Some(shard_iter.reset(next_index_record)?);
        }
    }

    /// Skip the next N items in this file.
    /// Any full shards that can be skipped will not be read.
    pub fn skip(&mut self, count: usize) -> Result<SkipResult, Error> {
        let mut skipped = 0;
        loop {
            let Some(mut shard_iter) = self.shard_iter.take() else {
                // Ran out of shards.
                return Ok(SkipResult {
                    skipped,
                    exhausted: true,
                });
            };
            let SkipResult {
                skipped: this_shard_skipped,
                exhausted,
            } = shard_iter.skip(count - skipped)?;
            skipped += this_shard_skipped;
            if !exhausted {
                self.shard_iter = Some(shard_iter);
                // If we didn't exhaust an entire shard iterator, we should be done.
                assert!(skipped == count);
                return Ok(SkipResult {
                    skipped,
                    exhausted: false,
                });
            } else {
                // Advance to the next shard, if there is one.
                if let Some(next_index_record) = self.file_index_iter.next() {
                    self.shard_iter = Some(shard_iter.reset(next_index_record)?);
                }
            }
        }
    }
}

/// A direct iterator over the items in a single shard in a single file.
struct UnsortedShardIter<T> {
    decoder: Decoder<ReadAdapter<File, File>>,
    items_remaining: usize,
    phantom_s: PhantomData<T>,
}

impl<T> UnsortedShardIter<T>
where
    T: DeserializeOwned,
{
    pub(crate) fn new(
        file: File,
        rec: KeylessShardRecord,
        compressor: Compressor,
    ) -> Result<Self, Error> {
        Ok(Self {
            decoder: compressor.decoder(ReadAdapter::new(file, rec.offset, rec.len_bytes))?,
            items_remaining: rec.len_items,
            phantom_s: PhantomData,
        })
    }

    pub(crate) fn reset(self, rec: KeylessShardRecord) -> Result<Self, Error> {
        let (read_adapter, compressor) = self.decoder.finish();
        Self::new(read_adapter.file, rec, compressor)
    }

    /// Return the next item in the iterator.
    ///
    /// If this method returns an error, the iterator should be discarded.
    pub(crate) fn next(&mut self) -> Result<Option<T>, Error> {
        if self.items_remaining == 0 {
            return Ok(None);
        }
        let next_item = deserialize_from(&mut self.decoder)?;
        self.items_remaining -= 1;
        Ok(Some(next_item))
    }

    /// Skip the specified number of items. If the requested number of items to
    /// skip is equal to or larger than the number of remaining items, this
    /// operation performs no I/O.
    ///
    /// If this method returns an error, the iterator should be discarded.
    #[allow(unused)]
    pub(crate) fn skip(&mut self, count: usize) -> Result<SkipResult, Error> {
        let can_skip = min(count, self.items_remaining);
        if can_skip == self.items_remaining {
            self.items_remaining = 0;
            return Ok(SkipResult {
                skipped: can_skip,
                exhausted: true,
            });
        }
        // If we're not skipping the entire rest of the iterator, load and discard
        // the requested number of items.
        for i in 0..can_skip {
            self.next()?.unwrap_or_else(|| panic!(
                "inconsistent item count; expected to skip {can_skip} but iteration ended at {}th item", i
            ));
        }
        Ok(SkipResult {
            skipped: can_skip,
            exhausted: false,
        })
    }
}

struct SkipResult {
    skipped: usize,
    exhausted: bool,
}

/// Ensure that readers are Sync.
#[allow(dead_code)]
const fn assert_readers_are_sync() {
    const fn takes_sync<T: Sync>() {}
    takes_sync::<UnsortedShardFileReader<usize, DefaultSort>>();
    takes_sync::<UnsortedShardReader<usize, DefaultSort>>();
}
