use std::sync::mpsc::sync_channel;
use std::thread;

use anyhow::{Context, Result};
use rayon::ThreadPoolBuilder;
use rayon::prelude::*;

pub fn resolve_thread_count(requested_thread_count: usize, work_items: usize) -> usize {
    requested_thread_count.min(work_items.max(1))
}

pub fn collect_parallel<I, O, F>(items: &[I], thread_count: usize, worker: F) -> Result<Vec<O>>
where
    I: Sync,
    O: Send,
    F: Fn(&I) -> O + Sync + Send,
{
    let pool = ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .context("failed to build the Rayon thread pool")?;

    Ok(pool.install(|| items.par_iter().map(worker).collect()))
}

pub fn stream_parallel<I, O, F, C>(
    items: Vec<I>,
    thread_count: usize,
    worker: F,
    mut consumer: C,
) -> Result<()>
where
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> O + Send + Sync + 'static,
    C: FnMut(O) -> Result<()>,
{
    let pool = ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .context("failed to build the Rayon thread pool")?;
    let queue_depth = thread_count.saturating_mul(2).max(1);
    let (sender, receiver) = sync_channel::<O>(queue_depth);

    let worker = thread::spawn(move || {
        pool.install(|| {
            items
                .into_par_iter()
                .for_each_with(sender, |result_sender, item| {
                    let result = worker(item);
                    let _ = result_sender.send(result);
                });
        });
    });

    let mut consumer_error = None;
    for result in receiver {
        if consumer_error.is_none() {
            if let Err(error) = consumer(result) {
                consumer_error = Some(error);
            }
        }
    }

    worker
        .join()
        .map_err(|_| anyhow::anyhow!("parallel worker thread panicked"))?;

    if let Some(error) = consumer_error {
        return Err(error);
    }

    Ok(())
}
