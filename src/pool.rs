//! ClientPool C ABI — len, member, publish, tryPublish, pendingBytes, close.

use std::os::raw::c_char;
use std::sync::Arc;

use vireon_sdk::{Client, ClientPool as SdkClientPool};

use crate::error::set_last_error;
use crate::handle::{arc_to_handle, handle_to_ref, reclaim_handle};
use crate::runtime::RUNTIME;
use crate::string_h::cstr_to_string;

/// C ABI: pool member count.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_pool_len(handle: isize) -> i32 {
    let pool = unsafe { handle_to_ref::<SdkClientPool>(handle) };
    pool.len() as i32
}

/// C ABI: get pool member by index. Returns a Client handle (caller must close).
#[unsafe(no_mangle)]
pub extern "C" fn vireon_pool_member(handle: isize, idx: i32) -> isize {
    let pool = unsafe { handle_to_ref::<SdkClientPool>(handle) };
    let idx = idx as usize;
    if idx >= pool.len() {
        return 0;
    }
    let client: Client = pool.member(idx).clone();
    arc_to_handle(Arc::new(client))
}

/// C ABI: pool publish (blocking, round-robin). Returns 0 on success, -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_pool_publish(
    handle: isize,
    topic: *const c_char,
    payload: *const u8,
    payload_len: usize,
) -> i32 {
    let pool = unsafe { handle_to_ref::<SdkClientPool>(handle) };
    let topic = cstr_to_string(topic);
    let payload = unsafe { std::slice::from_raw_parts(payload, payload_len) }.to_vec();
    let runtime = RUNTIME.get().unwrap();
    match runtime.block_on(pool.publish(&topic, payload)) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("PublishError: {e}"));
            -1
        }
    }
}

/// C ABI: pool try_publish (fire-and-forget, round-robin).
#[unsafe(no_mangle)]
pub extern "C" fn vireon_pool_try_publish(
    handle: isize,
    topic: *const c_char,
    payload: *const u8,
    payload_len: usize,
) -> i32 {
    let pool = unsafe { handle_to_ref::<SdkClientPool>(handle) };
    let topic = cstr_to_string(topic);
    let payload = unsafe { std::slice::from_raw_parts(payload, payload_len) };
    match pool.try_publish(&topic, payload) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("PublishError: {e}"));
            -1
        }
    }
}

/// C ABI: total pending bytes across all pool members.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_pool_pending_bytes(handle: isize) -> u64 {
    let pool = unsafe { handle_to_ref::<SdkClientPool>(handle) };
    pool.pending_bytes() as u64
}

/// C ABI: close pool + reclaim handle. Returns 0 on success.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_pool_close(handle: isize) -> i32 {
    let pool = unsafe { handle_to_ref::<SdkClientPool>(handle) };
    let runtime = RUNTIME.get().unwrap();
    let rc = runtime.block_on(pool.close());
    unsafe { reclaim_handle::<SdkClientPool>(handle) };
    match rc {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("ConnectError: {e}"));
            -1
        }
    }
}
