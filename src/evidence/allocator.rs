use serde::Serialize;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::atomic::{AtomicU64, Ordering},
};

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static DEALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static THREAD_ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    static THREAD_DEALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    static THREAD_LIVE_BYTES: Cell<u64> = const { Cell::new(0) };
}

pub struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        THREAD_ALLOCATIONS.with(|allocations| allocations.set(allocations.get() + 1));
        THREAD_LIVE_BYTES.with(|bytes| bytes.set(bytes.get() + layout.size() as u64));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        THREAD_DEALLOCATIONS.with(|deallocations| deallocations.set(deallocations.get() + 1));
        THREAD_LIVE_BYTES.with(|bytes| bytes.set(bytes.get().saturating_sub(layout.size() as u64)));
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct AllocationSnapshot {
    pub allocations: u64,
    pub deallocations: u64,
    pub live_bytes: u64,
}

impl AllocationSnapshot {
    pub fn delta_since(self, earlier: Self) -> Self {
        Self {
            allocations: self.allocations.saturating_sub(earlier.allocations),
            deallocations: self.deallocations.saturating_sub(earlier.deallocations),
            live_bytes: self.live_bytes.saturating_sub(earlier.live_bytes),
        }
    }
}

pub fn reset_allocations() {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    DEALLOCATIONS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    reset_thread_allocations();
}

pub fn allocation_snapshot() -> AllocationSnapshot {
    AllocationSnapshot {
        allocations: ALLOCATIONS.load(Ordering::Relaxed),
        deallocations: DEALLOCATIONS.load(Ordering::Relaxed),
        live_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
    }
}

pub fn reset_thread_allocations() {
    THREAD_ALLOCATIONS.with(|allocations| allocations.set(0));
    THREAD_DEALLOCATIONS.with(|deallocations| deallocations.set(0));
    THREAD_LIVE_BYTES.with(|bytes| bytes.set(0));
}

pub fn thread_allocation_snapshot() -> AllocationSnapshot {
    AllocationSnapshot {
        allocations: THREAD_ALLOCATIONS.with(Cell::get),
        deallocations: THREAD_DEALLOCATIONS.with(Cell::get),
        live_bytes: THREAD_LIVE_BYTES.with(Cell::get),
    }
}
