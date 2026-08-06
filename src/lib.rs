#![allow(
    clippy::unwrap_used, clippy::expect_used, clippy::panic,
    clippy::todo, clippy::unimplemented, clippy::unreachable,
    clippy::dbg_macro, clippy::print_stdout, clippy::print_stderr,
    clippy::missing_safety_doc, clippy::not_unsafe_ptr_arg_deref,
)]

mod builder;
mod client;
mod config;
mod error;
mod handle;
mod message;
mod pool;
mod runtime;
mod stream;
mod string_h;
mod subscription;

use std::os::raw::c_char;

/// Initialize the global tokio runtime. Call once at startup.
/// Returns 0 on success, -1 if initialization failed.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_init() -> i32 {
    runtime::init();
    0
}

/// Return a pointer to the last error message (thread-local, UTF-8, null-terminated).
/// Returns null if no error is set. Valid until the next vireon call on this thread.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_last_error() -> *const c_char {
    error::last_error_ptr()
}
