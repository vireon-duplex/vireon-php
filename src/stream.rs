//! StreamHandle C ABI — recv, recvBatch, publish, tryPublish, streamId, close.

use std::os::raw::c_char;
use tokio::sync::Mutex;

use vireon_sdk::StreamHandle;

use crate::error::set_last_error;
use crate::handle::{handle_to_ref, reclaim_handle};
use crate::message::{sdk_msg_to_c, VireonMessage, VireonMsgBatch};
use crate::runtime::RUNTIME;
use crate::string_h::cstr_to_string;

type StreamInner = Mutex<Option<StreamHandle>>;
const BATCH_CAP: usize = 256;

/// C ABI: stream receive next message (blocking).
/// Returns 0 if msg received, 1 if closed, -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_stream_recv(
    handle: isize,
    out_msg: *mut VireonMessage,
) -> i32 {
    let inner = unsafe { handle_to_ref::<StreamInner>(handle) };
    let runtime = RUNTIME.get().unwrap();
    let msg = runtime.block_on(async {
        let mut guard = inner.lock().await;
        match guard.as_mut() {
            Some(stream) => match stream.recv().await {
                Some(msg) => Some(msg),
                None => {
                    *guard = None;
                    None
                }
            },
            None => None,
        }
    });
    match msg {
        Some(m) => {
            if !out_msg.is_null() {
                unsafe { *out_msg = sdk_msg_to_c(m) };
            }
            0
        }
        None => 1,
    }
}

/// C ABI: stream receive batch.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_stream_recv_batch(
    handle: isize,
    max_count: i32,
    out_batch: *mut VireonMsgBatch,
) -> i32 {
    let inner = unsafe { handle_to_ref::<StreamInner>(handle) };
    let max = (max_count as usize).min(BATCH_CAP).max(1);
    let runtime = RUNTIME.get().unwrap();
    let msgs = runtime.block_on(async {
        let mut guard = inner.lock().await;
        match guard.as_mut() {
            Some(stream) => {
                let first = stream.recv().await;
                if first.is_none() {
                    *guard = None;
                    return Vec::new();
                }
                let mut batch = Vec::with_capacity(max);
                batch.push(first.unwrap());
                for _ in 1..max {
                    match stream.try_recv() {
                        Some(m) => batch.push(m),
                        None => break,
                    }
                }
                batch
            }
            None => Vec::new(),
        }
    });

    if msgs.is_empty() {
        return 1;
    }
    if !out_batch.is_null() {
        let count = msgs.len();
        let c_msgs: Vec<VireonMessage> = msgs.into_iter().map(sdk_msg_to_c).collect();
        let mut c_msgs = std::mem::ManuallyDrop::new(c_msgs);
        let ptr = c_msgs.as_mut_ptr();
        unsafe {
            *out_batch = VireonMsgBatch { msgs: ptr, count };
        }
    }
    0
}

/// C ABI: stream publish (blocking). Returns 0 on success, -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_stream_publish(
    handle: isize,
    topic: *const c_char,
    payload: *const u8,
    payload_len: usize,
) -> i32 {
    let inner = unsafe { handle_to_ref::<StreamInner>(handle) };
    let topic = cstr_to_string(topic);
    let payload = unsafe { std::slice::from_raw_parts(payload, payload_len) };
    let runtime = RUNTIME.get().unwrap();
    let rc = runtime.block_on(async {
        let guard = inner.lock().await;
        match guard.as_ref() {
            Some(stream) => stream.publish(&topic, payload).await,
            None => Err(vireon_sdk::PublishError::NotConnected),
        }
    });
    match rc {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("PublishError: {e}"));
            -1
        }
    }
}

/// C ABI: stream try_publish (fire-and-forget). Returns 0 on success, -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_stream_try_publish(
    handle: isize,
    topic: *const c_char,
    payload: *const u8,
    payload_len: usize,
) -> i32 {
    let inner = unsafe { handle_to_ref::<StreamInner>(handle) };
    let topic = cstr_to_string(topic);
    let payload = unsafe { std::slice::from_raw_parts(payload, payload_len) };
    let runtime = RUNTIME.get().unwrap();
    let rc = runtime.block_on(async {
        let guard = inner.lock().await;
        match guard.as_ref() {
            Some(stream) => stream.try_publish(&topic, payload),
            None => Err(vireon_sdk::PublishError::NotConnected),
        }
    });
    match rc {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("PublishError: {e}"));
            -1
        }
    }
}

/// C ABI: stream id. Returns the QUIC stream ID.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_stream_id(handle: isize) -> u64 {
    let inner = unsafe { handle_to_ref::<StreamInner>(handle) };
    let runtime = RUNTIME.get().unwrap();
    runtime.block_on(async {
        let guard = inner.lock().await;
        guard.as_ref().map(|s| s.stream_id()).unwrap_or(0)
    })
}

/// C ABI: stream pending bytes.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_stream_pending_bytes(handle: isize) -> u64 {
    let inner = unsafe { handle_to_ref::<StreamInner>(handle) };
    let runtime = RUNTIME.get().unwrap();
    runtime.block_on(async {
        let guard = inner.lock().await;
        guard.as_ref().map(|s| s.pending_bytes()).unwrap_or(0) as u64
    })
}

/// C ABI: close + reclaim stream handle.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_stream_close(handle: isize) {
    unsafe { reclaim_handle::<StreamInner>(handle) };
}
