//! Subscription + GroupSubscription C ABI — recv, recvBatch, close.

use tokio::sync::Mutex;

use vireon_sdk::{GroupSubscription, Subscription};

use crate::handle::{arc_to_handle, handle_to_ref, reclaim_handle};
use crate::message::{sdk_msg_to_c, VireonMessage, VireonMsgBatch};
use crate::runtime::RUNTIME;

type SubInner = Mutex<Option<Subscription>>;
type GroupSubInner = Mutex<Option<GroupSubscription>>;

const BATCH_CAP: usize = 256;

// ── Subscription recv ─────────────────────────────────────────────────

/// C ABI: receive next message (blocking).
/// Returns 0 if msg received, 1 if closed, -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_sub_recv(
    handle: isize,
    out_msg: *mut VireonMessage,
) -> i32 {
    let inner = unsafe { handle_to_ref::<SubInner>(handle) };
    let runtime = RUNTIME.get().unwrap();
    let msg = runtime.block_on(async {
        let mut guard = inner.lock().await;
        match guard.as_mut() {
            Some(sub) => match sub.recv().await {
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

/// C ABI: receive batch (blocking for first, drains rest via try_recv).
/// Returns 0 on success (check batch.count), 1 if closed, -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_sub_recv_batch(
    handle: isize,
    max_count: i32,
    out_batch: *mut VireonMsgBatch,
) -> i32 {
    let inner = unsafe { handle_to_ref::<SubInner>(handle) };
    let max = (max_count as usize).min(BATCH_CAP).max(1);
    let runtime = RUNTIME.get().unwrap();
    let msgs = runtime.block_on(async {
        let mut guard = inner.lock().await;
        match guard.as_mut() {
            Some(sub) => {
                let first = sub.recv().await;
                if first.is_none() {
                    *guard = None;
                    return Vec::new();
                }
                let mut batch = Vec::with_capacity(max);
                batch.push(first.unwrap());
                for _ in 1..max {
                    match sub.try_recv() {
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
            *out_batch = VireonMsgBatch {
                msgs: ptr,
                count,
            };
        }
    }
    0
}

/// C ABI: close + reclaim subscription handle.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_sub_close(handle: isize) {
    unsafe { reclaim_handle::<SubInner>(handle) };
}

// ── GroupSubscription recv ───────────────────────────────────────────

/// C ABI: group subscription receive next message (blocking).
#[unsafe(no_mangle)]
pub extern "C" fn vireon_group_sub_recv(
    handle: isize,
    out_msg: *mut VireonMessage,
) -> i32 {
    let inner = unsafe { handle_to_ref::<GroupSubInner>(handle) };
    let runtime = RUNTIME.get().unwrap();
    let msg = runtime.block_on(async {
        let mut guard = inner.lock().await;
        match guard.as_mut() {
            Some(sub) => match sub.recv().await {
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

/// C ABI: group subscription receive batch.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_group_sub_recv_batch(
    handle: isize,
    max_count: i32,
    out_batch: *mut VireonMsgBatch,
) -> i32 {
    let inner = unsafe { handle_to_ref::<GroupSubInner>(handle) };
    let max = (max_count as usize).min(BATCH_CAP).max(1);
    let runtime = RUNTIME.get().unwrap();
    let msgs = runtime.block_on(async {
        let mut guard = inner.lock().await;
        match guard.as_mut() {
            Some(sub) => {
                let first = sub.recv().await;
                if first.is_none() {
                    *guard = None;
                    return Vec::new();
                }
                let mut batch = Vec::with_capacity(max);
                batch.push(first.unwrap());
                for _ in 1..max {
                    match sub.try_recv() {
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
            *out_batch = VireonMsgBatch {
                msgs: ptr,
                count,
            };
        }
    }
    0
}

/// C ABI: close + reclaim group subscription handle.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_group_sub_close(handle: isize) {
    unsafe { reclaim_handle::<GroupSubInner>(handle) };
}

// Silence unused import: arc_to_handle used by client.rs not here, but
// the type aliases are shared.
#[allow(dead_code)]
fn _ensure_link(_h: isize) {
    let _ = arc_to_handle::<SubInner>;
}
