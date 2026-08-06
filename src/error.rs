//! Thread-local last error — C# checks the return code, then reads the message.

use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Store an error message for the current thread.
pub(crate) fn set_last_error(msg: &str) {
    let cstr = CString::new(msg).unwrap_or_else(|_| {
        CString::new("error message contained null byte").unwrap()
    });
    LAST_ERROR.with(|cell| *cell.borrow_mut() = Some(cstr));
}

/// Return a raw pointer to the last error, or null if none.
pub(crate) fn last_error_ptr() -> *const c_char {
    LAST_ERROR.with(|cell| {
        cell.borrow()
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(ptr::null())
    })
}
