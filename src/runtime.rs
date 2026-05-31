use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

const MIN_MEMORY_JOB_BYTES: u64 = 32 * 1024 * 1024;
const MEMORY_BYTES_PER_THREAD: u64 = 96 * 1024 * 1024;
const MIN_MEMORY_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone)]
pub struct RuntimeResources {
    io: Arc<Semaphore>,
    memory: Arc<MemoryLimiter>,
    memory_budget_bytes: u64,
}

#[derive(Debug, Default)]
pub struct ResourceWait {
    pub memory: Duration,
    pub io_decode: Duration,
    pub io_write: Duration,
}

pub struct PermitGuard {
    semaphore: Arc<Semaphore>,
    permits: usize,
}

pub struct MemoryGuard {
    limiter: Arc<MemoryLimiter>,
    bytes: u64,
}

impl RuntimeResources {
    pub fn for_thread_count(thread_count: usize) -> Self {
        let io_permits = thread_count.clamp(1, 4);
        let memory_budget_bytes =
            MIN_MEMORY_BUDGET_BYTES.max(thread_count as u64 * MEMORY_BYTES_PER_THREAD);

        Self {
            io: Arc::new(Semaphore::new(io_permits)),
            memory: Arc::new(MemoryLimiter::new(memory_budget_bytes)),
            memory_budget_bytes,
        }
    }

    pub fn acquire_decode_io(&self) -> PermitGuard {
        self.io.acquire(1)
    }

    pub fn acquire_write_io(&self) -> PermitGuard {
        self.io.acquire(1)
    }

    pub fn acquire_memory_for_job(&self, estimated_size_bytes: u64) -> MemoryGuard {
        let requested = estimated_size_bytes
            .saturating_mul(4)
            .max(MIN_MEMORY_JOB_BYTES)
            .min(self.memory_budget_bytes);
        self.memory.acquire(requested)
    }

    pub fn memory_budget_bytes(&self) -> u64 {
        self.memory_budget_bytes
    }
}

struct Semaphore {
    state: Mutex<usize>,
    available: Condvar,
}

impl Semaphore {
    fn new(permits: usize) -> Self {
        Self {
            state: Mutex::new(permits),
            available: Condvar::new(),
        }
    }

    fn acquire(self: &Arc<Self>, permits: usize) -> PermitGuard {
        let mut available = self.state.lock().expect("semaphore mutex poisoned");
        while *available < permits {
            available = self
                .available
                .wait(available)
                .expect("semaphore mutex poisoned while waiting");
        }
        *available -= permits;
        PermitGuard {
            semaphore: Arc::clone(self),
            permits,
        }
    }

    fn release(&self, permits: usize) {
        let mut available = self.state.lock().expect("semaphore mutex poisoned");
        *available += permits;
        self.available.notify_all();
    }
}

impl Drop for PermitGuard {
    fn drop(&mut self) {
        self.semaphore.release(self.permits);
    }
}

struct MemoryLimiter {
    state: Mutex<u64>,
    available: Condvar,
    budget: u64,
}

impl MemoryLimiter {
    fn new(budget: u64) -> Self {
        Self {
            state: Mutex::new(budget),
            available: Condvar::new(),
            budget,
        }
    }

    fn acquire(self: &Arc<Self>, bytes: u64) -> MemoryGuard {
        let request = bytes.min(self.budget);
        let mut available = self.state.lock().expect("memory limiter mutex poisoned");
        while *available < request {
            available = self
                .available
                .wait(available)
                .expect("memory limiter mutex poisoned while waiting");
        }
        *available -= request;
        MemoryGuard {
            limiter: Arc::clone(self),
            bytes: request,
        }
    }

    fn release(&self, bytes: u64) {
        let mut available = self.state.lock().expect("memory limiter mutex poisoned");
        *available = (*available + bytes).min(self.budget);
        self.available.notify_all();
    }
}

impl Drop for MemoryGuard {
    fn drop(&mut self) {
        self.limiter.release(self.bytes);
    }
}
