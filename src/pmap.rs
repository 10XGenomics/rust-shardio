use std::thread;
use std::collections::HashMap;
use crossbeam_channel::{bounded, Sender, Receiver};
use std::sync::Arc;


pub struct StreamMapper<I, U, T> {
    // All the other threads will steal from this
    iter: I,
    return_buffer: HashMap<usize, T>,
    proc_channel: Sender<(usize, U)>,
    done_channel: Receiver<(usize, T)>,
    items_emitted: usize,
    items_read: usize,
    read_ahead: usize, 
    total_items: Option<usize>,
}

struct WorkerThread<F, U, T> {
    f: Arc<F>,
    proc_channel: Receiver<(usize, U)>,
    done_channel: Sender<(usize, T)>,
}

impl<F, U, T> WorkerThread<F, U, T> where
    F: Fn(U) -> T,
    U: Send,
    T: Send
{
    /// Process items & shutdown when the sender hangs up.
    fn thread_func(&self) {
        while let Ok((id, input)) = self.proc_channel.recv() {
                let output = (self.f)(input);
                let _ = self.done_channel.send((id, output));
        }
    }
}

impl<I, U, T> StreamMapper<I, U, T> where
    I: Iterator<Item=U>, 
    U: Send + 'static,
    T: Send + 'static,
{
    pub fn new<F: Fn(U) -> T + Send + Sync + 'static>(iter: I, read_ahead: usize, threads: usize, f: F) -> StreamMapper<I, U, T> {

        // Channel to exchange items to be mapped and results
        let (proc_channel_tx, proc_channel_rx) = bounded(read_ahead);
        let (done_channel_tx, done_channel_rx) = bounded(read_ahead);

        let f = Arc::new(f);

        let m = StreamMapper {
            iter,
            return_buffer: HashMap::new(),
            proc_channel: proc_channel_tx,
            done_channel: done_channel_rx,
            items_emitted: 0,
            items_read: 0,
            read_ahead,
            total_items: None,
        };

        // spawn threads
        for _ in 0 .. threads {
            let rx = proc_channel_rx.clone();
            let tx = done_channel_tx.clone();
            let th = WorkerThread { f: f.clone(), proc_channel: rx, done_channel: tx };
            thread::spawn(move || {
                th.thread_func();
            });
        }

        m
    }
}

impl<I,U,T> Iterator for StreamMapper<I, U, T> where 
    I: Iterator<Item=U>,
    U: Send,
    T: Send,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {

        // Make sure we've queued at least read_ahead items
        while self.total_items.is_none() && self.items_read - self.items_emitted < self.read_ahead {
            match self.iter.next() {
                // New item: push it onto Deque.
                Some(item) => { 
                    self.proc_channel.send((self.items_read, item)).unwrap(); 
                    self.items_read += 1; },

                // We've read all the items 
                None => self.total_items = Some(self.items_read),
            }
        }

        // loop & wait for the output item until it's available
        while self.total_items.map_or(true, |ti| self.items_emitted < ti) {

            // pull all available return items
            loop {
                match self.done_channel.try_recv() {
                    Ok((idx, t)) => { self.return_buffer.insert(idx, t); },
                    Err(_) => break,
                }
            }

            // Try to return the next item
            match self.return_buffer.remove(&self.items_emitted) {
                Some(v) => {
                    self.items_emitted += 1;
                    return Some(v);
                },

                // If we don't have an output ready, sleep for a while
                // then try again.
                None => thread::yield_now(),
            }
        }

        // We're done.
        None
    }
}

#[cfg(test)]
mod pmap_tests {
    use super::*;

    #[test]
    fn pmap_test() {

        let rng = 0..1000000;
        let sq = StreamMapper::new(rng.clone().into_iter(), 2, 2, |x| x*x);
        let res : Vec<usize> = sq.collect();

        let true_res: Vec<usize> = rng.into_iter().map(|x| x*x).collect();
        assert_eq!(res, true_res);
    }
}
