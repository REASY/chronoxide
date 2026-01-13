use std::alloc::{GlobalAlloc, Layout, System};
use std::ffi::c_void;
use std::sync::atomic::{AtomicUsize, Ordering};

static REQ_ALLOC_TOTAL: AtomicUsize = AtomicUsize::new(0);
static REQ_DEALLOC_TOTAL: AtomicUsize = AtomicUsize::new(0);
static USABLE_ALLOC_TOTAL: AtomicUsize = AtomicUsize::new(0);
static USABLE_DEALLOC_TOTAL: AtomicUsize = AtomicUsize::new(0);

static ALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);
static REALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn malloc_usable_size(ptr: *const c_void) -> usize;
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn malloc_size(ptr: *const c_void) -> usize;
}

#[cfg(windows)]
unsafe extern "C" {
    fn _msize(ptr: *const c_void) -> usize;
}

#[cfg(target_os = "linux")]
#[inline]
fn usable_size(ptr: *const u8, _fallback: usize) -> usize {
    if ptr.is_null() {
        return 0;
    }
    unsafe { malloc_usable_size(ptr as *const c_void) }
}

#[cfg(target_os = "macos")]
#[inline]
fn usable_size(ptr: *const u8, _fallback: usize) -> usize {
    if ptr.is_null() {
        return 0;
    }
    unsafe { malloc_size(ptr as *const c_void) }
}

#[cfg(windows)]
#[inline]
fn usable_size(ptr: *const u8, _fallback: usize) -> usize {
    if ptr.is_null() {
        return 0;
    }
    unsafe { _msize(ptr as *const c_void) }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
#[inline]
fn usable_size(ptr: *const u8, fallback: usize) -> usize {
    if ptr.is_null() {
        return 0;
    }
    fallback
}

#[derive(Clone, Copy, Debug)]
pub struct AllocationStats {
    pub requested_total: usize,
    pub requested_freed_total: usize,
    pub requested_current: usize,
    pub usable_total: usize,
    pub usable_freed_total: usize,
    pub usable_current: usize,
    pub internal_frag_bytes: usize,
    pub internal_frag_percent: f64,
    pub alloc_calls: usize,
    pub dealloc_calls: usize,
    pub realloc_calls: usize,
}

pub fn reset_allocation_counters() {
    REQ_ALLOC_TOTAL.store(0, Ordering::Relaxed);
    REQ_DEALLOC_TOTAL.store(0, Ordering::Relaxed);
    USABLE_ALLOC_TOTAL.store(0, Ordering::Relaxed);
    USABLE_DEALLOC_TOTAL.store(0, Ordering::Relaxed);
    ALLOC_CALLS.store(0, Ordering::Relaxed);
    DEALLOC_CALLS.store(0, Ordering::Relaxed);
    REALLOC_CALLS.store(0, Ordering::Relaxed);
}

pub fn allocation_stats() -> AllocationStats {
    let requested_total = REQ_ALLOC_TOTAL.load(Ordering::Relaxed);
    let requested_freed_total = REQ_DEALLOC_TOTAL.load(Ordering::Relaxed);
    let usable_total = USABLE_ALLOC_TOTAL.load(Ordering::Relaxed);
    let usable_freed_total = USABLE_DEALLOC_TOTAL.load(Ordering::Relaxed);

    let requested_current = requested_total.saturating_sub(requested_freed_total);
    let usable_current = usable_total.saturating_sub(usable_freed_total);

    let internal_frag_bytes = usable_current.saturating_sub(requested_current);
    let internal_frag_percent = if usable_current == 0 {
        0.0
    } else {
        internal_frag_bytes as f64 * 100.0 / usable_current as f64
    };

    AllocationStats {
        requested_total,
        requested_freed_total,
        requested_current,
        usable_total,
        usable_freed_total,
        usable_current,
        internal_frag_bytes,
        internal_frag_percent,
        alloc_calls: ALLOC_CALLS.load(Ordering::Relaxed),
        dealloc_calls: DEALLOC_CALLS.load(Ordering::Relaxed),
        realloc_calls: REALLOC_CALLS.load(Ordering::Relaxed),
    }
}

/// Global allocator that tracks both requested allocation sizes and the allocator's usable sizes.
///
/// - `requested_*` counters are based on `Layout::size()` / `new_size`.
/// - `usable_*` counters use `malloc_usable_size` (Linux), `malloc_size` (macOS), or `_msize`
///   (Windows) where available. On other platforms, usable sizes fall back to requested sizes.
///
/// The difference `usable_current - requested_current` approximates allocator internal
/// fragmentation / size-class rounding.
pub struct TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
            let req = layout.size();
            REQ_ALLOC_TOTAL.fetch_add(req, Ordering::Relaxed);
            let usable = usable_size(ptr, req);
            USABLE_ALLOC_TOTAL.fetch_add(usable, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if !ptr.is_null() {
            DEALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
            let req = layout.size();
            REQ_DEALLOC_TOTAL.fetch_add(req, Ordering::Relaxed);
            let usable = usable_size(ptr, req);
            USABLE_DEALLOC_TOTAL.fetch_add(usable, Ordering::Relaxed);
        }
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let old_req = layout.size();
        let old_usable = if ptr.is_null() {
            0
        } else {
            usable_size(ptr, old_req)
        };

        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            REALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
            REQ_ALLOC_TOTAL.fetch_add(new_size, Ordering::Relaxed);
            REQ_DEALLOC_TOTAL.fetch_add(old_req, Ordering::Relaxed);

            let new_usable = usable_size(new_ptr, new_size);
            USABLE_ALLOC_TOTAL.fetch_add(new_usable, Ordering::Relaxed);
            USABLE_DEALLOC_TOTAL.fetch_add(old_usable, Ordering::Relaxed);
        }
        new_ptr
    }
}
