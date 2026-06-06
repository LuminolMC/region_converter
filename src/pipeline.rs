use std::collections::VecDeque;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
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
    let items = Arc::new(Mutex::new(VecDeque::from(items)));
    let worker = Arc::new(worker);
    let (sender, receiver) = channel::<O>();
    let mut workers = Vec::with_capacity(thread_count);

    for _ in 0..thread_count {
        let items = Arc::clone(&items);
        let worker = Arc::clone(&worker);
        let sender = sender.clone();
        workers.push(thread::spawn(move || {
            loop {
                let item = {
                    let mut items = items.lock().expect("parallel work queue mutex poisoned");
                    items.pop_front()
                };

                let Some(item) = item else {
                    break;
                };

                let result = worker(item);
                if sender.send(result).is_err() {
                    break;
                }
            }
        }));
    }
    drop(sender);

    let mut consumer_error = None;
    for result in receiver {
        if consumer_error.is_none()
            && let Err(error) = consumer(result)
        {
            consumer_error = Some(error);
        }
    }

    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("parallel worker thread panicked"))?;
    }

    if let Some(error) = consumer_error {
        return Err(error);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use super::*;

    fn record_max(maximum: &AtomicUsize, value: usize) {
        let mut current = maximum.load(Ordering::SeqCst);
        while value > current {
            match maximum.compare_exchange(current, value, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    #[test]
    fn stream_parallel_uses_multiple_workers_from_dynamic_queue() -> Result<()> {
        let active = Arc::new(AtomicUsize::new(0));
        let maximum_active = Arc::new(AtomicUsize::new(0));
        let worker_active = Arc::clone(&active);
        let worker_maximum_active = Arc::clone(&maximum_active);
        let mut outputs = Vec::new();

        stream_parallel(
            (0..9).collect(),
            3,
            move |item| {
                let active = worker_active.fetch_add(1, Ordering::SeqCst) + 1;
                record_max(&worker_maximum_active, active);
                thread::sleep(Duration::from_millis(20));
                worker_active.fetch_sub(1, Ordering::SeqCst);
                item
            },
            |result| {
                outputs.push(result);
                Ok(())
            },
        )?;

        assert_eq!(outputs.len(), 9);
        assert!(
            maximum_active.load(Ordering::SeqCst) >= 2,
            "expected multiple workers to run concurrently"
        );
        Ok(())
    }

    #[test]
    fn slow_consumer_does_not_prevent_workers_from_finishing_queue() -> Result<()> {
        let completed = Arc::new(AtomicUsize::new(0));
        let last_worker_done = Arc::new(Mutex::new(None));
        let worker_completed = Arc::clone(&completed);
        let worker_last_done = Arc::clone(&last_worker_done);
        let started_at = Instant::now();

        stream_parallel(
            (0..12).collect(),
            2,
            move |_| {
                thread::sleep(Duration::from_millis(5));
                if worker_completed.fetch_add(1, Ordering::SeqCst) + 1 == 12 {
                    *worker_last_done.lock().expect("test mutex poisoned") =
                        Some(started_at.elapsed());
                }
            },
            |_| {
                thread::sleep(Duration::from_millis(25));
                Ok(())
            },
        )?;

        let last_worker_done = last_worker_done
            .lock()
            .expect("test mutex poisoned")
            .expect("all workers should have completed");
        assert!(
            last_worker_done < Duration::from_millis(120),
            "workers were likely blocked by result consumption for {last_worker_done:?}"
        );
        Ok(())
    }

    #[test]
    fn consumer_error_is_returned_after_workers_finish() {
        let result = stream_parallel(
            vec![1, 2, 3],
            2,
            |item| item,
            |item| {
                if item == 2 {
                    anyhow::bail!("consumer failed");
                }
                Ok(())
            },
        );

        assert!(result.is_err());
        assert!(format!("{:#}", result.unwrap_err()).contains("consumer failed"));
    }

    #[test]
    fn worker_panic_is_reported() {
        let result = stream_parallel(
            vec![1],
            1,
            |_| -> usize {
                panic!("worker failed");
            },
            |_| Ok(()),
        );

        assert!(result.is_err());
        assert!(format!("{:#}", result.unwrap_err()).contains("parallel worker thread panicked"));
    }
}
