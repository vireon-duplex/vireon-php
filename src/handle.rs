//! Opaque pointer (`isize`) ↔ `Arc` conversion helpers.

use std::sync::Arc;

/// Store an `Arc<T>` as an opaque `isize` handle.
pub(crate) fn arc_to_handle<T>(arc: Arc<T>) -> isize {
    Arc::into_raw(arc) as isize
}

/// Borrow the inner value from a handle (no refcount change).
///
/// # Safety
/// `ptr` must be a valid handle produced by [`arc_to_handle`]`::<T>` and the
/// backing `Arc` must still be alive.
pub(crate) unsafe fn handle_to_ref<'a, T>(ptr: isize) -> &'a T {
    unsafe { &*(ptr as *const T) }
}

/// Reclaim (decrement) the `Arc` behind a handle. Called from `close()` /
/// `Dispose()`.
///
/// # Safety
/// `ptr` must be a valid handle produced by [`arc_to_handle`]`::<T>`, must not
/// have been reclaimed already, and must not be `0`.
pub(crate) unsafe fn reclaim_handle<T>(ptr: isize) {
    if ptr != 0 {
        unsafe {
            let _ = Arc::from_raw(ptr as *const T);
        }
    }
}
