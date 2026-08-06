//! C string (`*const c_char`) → Rust `String` / `Option<String>` helpers.

use std::ffi::CStr;
use std::os::raw::c_char;

/// Convert a null-terminated C string to a Rust `String`.
/// Returns empty string if ptr is null.
pub(crate) fn cstr_to_string(ptr: *const c_char) -> String {
    if ptr.is_null() {
        return String::new();
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned()
}

/// Convert a nullable C string to `Option<String>`.
pub(crate) fn cstr_to_option(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    Some(cstr_to_string(ptr))
}
