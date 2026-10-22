// Copyright (c) 2018 10x Genomics, Inc. All rights reserved.

//! Upper-bound estimates of the memory needed to read a key range in sorted order.

use std::mem::size_of;

use crate::range::Range;
use crate::ShardRecord;

/// An upper-bound estimate of the memory one process needs to open a set of shardio files
/// with [`ShardReader::open_set`](crate::ShardReader::open_set) and iterate a single key range
/// in sorted order with [`ShardReader::iter_range`](crate::ShardReader::iter_range).
///
/// Produced by [`ShardReader::memory_estimate`](crate::ShardReader::memory_estimate) for any
/// range, or by [`ShardReader::make_chunks_with_memory_estimate`](crate::ShardReader::make_chunks_with_memory_estimate)
/// together with the ranges.
/// The struct holds only quantities that shardio knows from the file indexes and the compressor.
/// The in-memory sizes of an item and of a key depend on your data, so they are supplied when
/// converting to bytes with [`MemoryEstimate::total_bytes`].
///
/// # Memory model
///
/// A shardio file is a sequence of compressed *chunks*, each holding up to `disk_chunk_size`
/// items, followed by an index with one record per chunk. A writer sorts items in batches of
/// `item_buffer_size / 2` and writes each sorted batch (a *run*) as consecutive chunks. Chunks
/// from the same run never interleave in key space; chunks from different runs overlap wherever
/// the runs share keys.
///
/// Sorted reading of a range merges the chunks that intersect the range. For each file, a
/// `RangeIter` keeps:
/// * a *waiting* min-heap with one reference per not-yet-opened chunk, ordered by `start_key`, and
/// * an *active* min-heap of open chunks. Each open chunk owns a decompressor (the dominant cost,
///   roughly 296 KiB for lz4 and 1.09 MiB for zstd) plus one decoded item and its key.
///
/// A chunk is opened only when its `start_key` is strictly smaller than the smallest pending key
/// among the open chunks (or when nothing is open), and it is freed as soon as its last item is
/// returned. Each `RangeIter` also keeps its own copy of the range, so a bounded range costs one
/// or two keys per file. Across files, a `MergeIterator` holds one decoded item and key per file.
/// While it advances, the item it is about to return and the item an open chunk is replacing are
/// alive together with the newly decoded item, so one more item and key is live at the peak.
///
/// Putting this together, the peak memory is bounded by
///
/// ```text
/// total_bytes =  index_records   × (index_record_fixed_bytes + 2 × key_bytes)  // all indexes
///              + waiting_chunks  × size_of::<usize>()                          // waiting heaps
///              + max_open_chunks × (decoder_bytes + item_bytes + key_bytes)    // open chunks
///              + files           × (file_fixed_bytes + item_bytes + key_bytes   // per file
///                                   + range_bound_keys × key_bytes)               // range copies
///              + (item_bytes + key_bytes)                    // in flight, if files > 0
/// ```
///
/// ## How `max_open_chunks` is bounded
///
/// The number of chunks open at once is computed from the index alone, without decompressing
/// anything and without knowing which chunk belongs to which run. The activation rule above
/// implies that chunks open together pairwise overlap as half-open intervals
/// `[start_key, end_key)`, and that at most one chunk with `start_key == end_key` can be open at a
/// time. So for each file the bound is the maximum, over all keys `x`, of the number of in-range
/// chunks with `start_key <= x < end_key`, plus one if any in-range chunk has
/// `start_key == end_key` (and at least one if any chunk is in range). The per-file bounds are
/// summed.
///
/// In practice:
/// * With random keys, every run spans the whole key space, so the bound is close to the number
///   of runs, about `items / (item_buffer_size / 2)` per file. Splitting into narrower ranges does
///   not reduce it much, because every range still crosses one chunk per run.
/// * With sorted or clustered input, runs overlap little and the bound is small.
/// * Many chunks sharing a single repeated key do not increase it.
/// * `disk_chunk_size` affects `index_records` and `waiting_chunks`, not `max_open_chunks`.
///
/// # Choosing `item_bytes` and `key_bytes`
///
/// Use in-memory sizes including owned heap data, for example `size_of::<T>()` plus the capacity
/// of any `String` or `Vec` fields. Both must be at least the inline size of the type, because
/// the fixed-bytes fields only cover what the record or entry adds around the item and key. For
/// variable-sized data, use a realistic maximum. With
/// [`DefaultSort`](crate::DefaultSort) the key is a clone of the whole item, so
/// `key_bytes == item_bytes`.
///
/// # Scope
///
/// * The estimate is for one process that opens all of the reader's files and iterates one range.
///   The `index_records` term is the same for every range, because `open_set` loads every file's
///   full index regardless of the range read. If several ranges are instead iterated by threads
///   sharing one `ShardReader`, the index is loaded once, so summing the per-range totals
///   over-counts it.
/// * When the totals of several readers are added, the in-flight item is counted once per reader,
///   which over-counts slightly.
/// * The index, the waiting and merge heaps, and the per-file iterators are allocated at their
///   exact sizes, so the estimate counts their full allocations, padding included.
/// * The active heap grows as chunks are opened, so it can have spare slots: up to
///   `max(max_open_chunks, 4)` of them. A spare slot holds only the inline part of an open chunk
///   (`size_of` the item, the key and the decoder handle, usually a few hundred bytes) and none of
///   their heap data, so this is not counted. It matters only for items with a large inline size.
/// * Not included: allocator overhead, the OS page cache, file handles, the temporary key set
///   built by `make_chunks`, and whatever the caller does with the items it receives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct MemoryEstimate {
    /// Number of files opened by the reader. The `MergeIterator` holds at most one decoded item
    /// and its key per file.
    pub files: usize,

    /// Total number of chunk records in the indexes of all files. Each record holds a
    /// `start_key`, an `end_key` and three `usize` fields, and all indexes stay in memory for the
    /// lifetime of the reader. This does not depend on the range.
    pub index_records: usize,

    /// Number of chunks, summed over files, that intersect this range. Each is referenced by one
    /// pointer in a waiting heap until it is opened.
    pub waiting_chunks: usize,

    /// Upper bound, summed over files, on the number of chunks open at the same time while
    /// iterating this range. Each open chunk holds a decompressor plus one decoded item and key.
    /// See the type-level documentation for how the bound is computed.
    pub max_open_chunks: usize,

    /// Bytes for one open chunk's decompressor and bookkeeping, excluding the decoded item and
    /// key. Equal to [`ShardReader::shard_iter_size`](crate::ShardReader::shard_iter_size),
    /// taking the largest value if files use different compressors.
    pub decoder_bytes: usize,

    /// Number of bounds of the range (0, 1 or 2) that are set. Each file's `RangeIter` keeps its
    /// own copy of the range, so this many keys are held per file. The inline part of these keys
    /// is also in `file_fixed_bytes`, so it is counted twice; this over-counts by at most
    /// `2 × size_of` the key per file.
    pub range_bound_keys: usize,

    /// Inline bytes of one index record other than its two keys: three `usize` fields plus any
    /// padding. Taken from the record's actual layout, so it is exact for the reader's key type.
    pub index_record_fixed_bytes: usize,

    /// Inline bytes held per file other than the merged item and key: the file's `RangeIter`
    /// (its copy of the range and its heap headers) and, in its merge heap entry, the file number
    /// plus any padding. Taken from the actual layouts.
    pub file_fixed_bytes: usize,
}

impl MemoryEstimate {
    /// Upper bound on the bytes needed to iterate the range, given the in-memory size of one
    /// item (`item_bytes`) and of one sort key (`key_bytes`), each including owned heap data.
    /// See the type-level documentation for the formula.
    pub fn total_bytes(&self, item_bytes: usize, key_bytes: usize) -> usize {
        let item_and_key = item_bytes + key_bytes;
        let in_flight = if self.files > 0 { item_and_key } else { 0 };

        self.index_records * (self.index_record_fixed_bytes + 2 * key_bytes)
            + self.waiting_chunks * size_of::<usize>()
            + self.max_open_chunks * (self.decoder_bytes + item_and_key)
            + self.files
                * (self.file_fixed_bytes + item_and_key + self.range_bound_keys * key_bytes)
            + in_flight
    }
}

/// For one file's index, return an upper bound on the number of chunks a `RangeIter` over `range`
/// holds open at once, and the number of chunks that intersect `range`.
pub(crate) fn open_chunks_bound<K: Ord + Clone>(
    index: &[ShardRecord<K>],
    range: &Range<K>,
) -> (usize, usize) {
    let mut in_range = 0;
    let mut has_point_chunk = false;
    let mut events: Vec<(&K, isize)> = Vec::new();

    for rec in index.iter().filter(|rec| range.intersects_shard(rec)) {
        in_range += 1;
        if rec.start_key < rec.end_key {
            events.push((&rec.start_key, 1));
            events.push((&rec.end_key, -1));
        } else {
            has_point_chunk = true;
        }
    }

    if in_range == 0 {
        return (0, 0);
    }

    // Intervals are half-open, so at equal keys the -1 (end) events sort before the +1 (start)
    // events and two chunks that only touch at a key are not counted as overlapping.
    events.sort_unstable();
    let mut depth: isize = 0;
    let mut max_depth: isize = 0;
    for (_, delta) in events {
        depth += delta;
        max_depth = max_depth.max(depth);
    }

    let bound = max_depth as usize + usize::from(has_point_chunk);
    (bound.max(1), in_range)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(start_key: u32, end_key: u32) -> ShardRecord<u32> {
        ShardRecord {
            start_key,
            end_key,
            offset: 0,
            len_bytes: 0,
            len_items: 1,
        }
    }

    #[test]
    fn bound_two_interleaved_runs() {
        // Run 1: [3, 19], [42, 88]. Run 2: [12, 33], [50, 95].
        let index = vec![rec(3, 19), rec(42, 88), rec(12, 33), rec(50, 95)];
        assert_eq!(open_chunks_bound(&index, &Range::all()), (2, 4));
    }

    #[test]
    fn bound_single_repeated_key() {
        let index = vec![rec(7, 7), rec(7, 7), rec(7, 7), rec(7, 7)];
        assert_eq!(open_chunks_bound(&index, &Range::all()), (1, 4));
    }

    #[test]
    fn bound_touching_chunks_do_not_overlap() {
        let index = vec![rec(1, 5), rec(5, 8), rec(8, 9)];
        assert_eq!(open_chunks_bound(&index, &Range::all()), (1, 3));
    }

    #[test]
    fn bound_point_chunk_inside_interval() {
        // [5, 9] then [5, 5]: both can be open together.
        let index = vec![rec(5, 9), rec(5, 5)];
        assert_eq!(open_chunks_bound(&index, &Range::all()), (2, 2));
    }

    #[test]
    fn bound_respects_range() {
        let index = vec![rec(3, 19), rec(42, 88), rec(12, 33), rec(50, 95)];
        assert_eq!(open_chunks_bound(&index, &Range::new(34, 41)), (0, 0));
        assert_eq!(open_chunks_bound(&index, &Range::new(20, 45)), (1, 2));
        assert_eq!(open_chunks_bound(&index, &Range::starts_at(60)), (2, 2));
    }

    #[test]
    fn total_bytes_formula() {
        let est = MemoryEstimate {
            files: 2,
            index_records: 10,
            waiting_chunks: 6,
            max_open_chunks: 3,
            decoder_bytes: 1000,
            range_bound_keys: 2,
            index_record_fixed_bytes: 24,
            file_fixed_bytes: 8,
        };
        let (item, key) = (40, 8);
        let expected = 10 * (24 + 2 * key)
            + 6 * 8
            + 3 * (1000 + item + key)
            + 2 * (8 + item + key + 2 * key)
            + (item + key);
        assert_eq!(est.total_bytes(item, key), expected);
    }
}
