//! Client C ABI methods — publish, subscribe, openStream, etc.

use std::os::raw::c_char;
use std::sync::Arc;
use tokio::sync::Mutex;

use vireon_sdk::{
    Client, GroupSubscription, StreamHandle, StreamSpec,
    Subscription,
};
use vireon_sdk::Message as SdkMessage;

use crate::error::set_last_error;
use crate::handle::{arc_to_handle, handle_to_ref, reclaim_handle};
use crate::message::{sdk_msg_to_c, VireonMessage};
use crate::runtime::RUNTIME;
use crate::string_h::cstr_to_string;
use crate::config::ordinal_to_policy;

type SubInner = Mutex<Option<Subscription>>;
type GroupSubInner = Mutex<Option<GroupSubscription>>;
type StreamInner = Mutex<Option<StreamHandle>>;

// ── Publish ──────────────────────────────────────────────────────────

/// C ABI: publish (blocking). Returns 0 on success, -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_client_publish(
    handle: isize,
    topic: *const c_char,
    payload: *const u8,
    payload_len: usize,
) -> i32 {
    let client = unsafe { handle_to_ref::<Client>(handle) };
    let topic = cstr_to_string(topic);
    let payload = unsafe { std::slice::from_raw_parts(payload, payload_len) }.to_vec();
    let runtime = RUNTIME.get().unwrap();
    match runtime.block_on(client.publish(&topic, payload)) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("PublishError: {e}"));
            -1
        }
    }
}

/// C ABI: try_publish (fire-and-forget). Returns 0 on success, -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_client_try_publish(
    handle: isize,
    topic: *const c_char,
    payload: *const u8,
    payload_len: usize,
) -> i32 {
    let client = unsafe { handle_to_ref::<Client>(handle) };
    let topic = cstr_to_string(topic);
    let payload = unsafe { std::slice::from_raw_parts(payload, payload_len) };
    match client.try_publish(&topic, payload) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("PublishError: {e}"));
            -1
        }
    }
}

// ── Subscribe / Unsubscribe ──────────────────────────────────────────

/// C ABI: subscribe. Returns subscription handle, or 0 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_client_subscribe(
    handle: isize,
    pattern: *const c_char,
) -> isize {
    let client = unsafe { handle_to_ref::<Client>(handle) };
    let pattern = cstr_to_string(pattern);
    let runtime = RUNTIME.get().unwrap();
    match runtime.block_on(client.subscribe(&pattern)) {
        Ok(sub) => arc_to_handle(Arc::new(SubInner::new(Some(sub)))),
        Err(e) => {
            set_last_error(&format!("SubscribeError: {e}"));
            0
        }
    }
}

/// C ABI: unsubscribe. Returns 0 on success, -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_client_unsubscribe(
    handle: isize,
    pattern: *const c_char,
) -> i32 {
    let client = unsafe { handle_to_ref::<Client>(handle) };
    let pattern = cstr_to_string(pattern);
    let runtime = RUNTIME.get().unwrap();
    match runtime.block_on(client.unsubscribe(&pattern)) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("SubscribeError: {e}"));
            -1
        }
    }
}

// ── Stream ───────────────────────────────────────────────────────────

/// C ABI: open dedicated stream. Returns stream handle, or 0 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_client_open_stream(
    handle: isize,
    policy_ordinal: i32,
    topic: *const c_char,
) -> isize {
    let client = unsafe { handle_to_ref::<Client>(handle) };
    let policy = ordinal_to_policy(policy_ordinal);
    let mut spec = StreamSpec::new(policy);
    let topic_str = cstr_to_string(topic);
    if !topic_str.is_empty() {
        spec = spec.with_topic(topic_str);
    }
    let runtime = RUNTIME.get().unwrap();
    match runtime.block_on(client.open_stream(spec)) {
        Ok(stream) => arc_to_handle(Arc::new(StreamInner::new(Some(stream)))),
        Err(e) => {
            set_last_error(&format!("StreamError: {e}"));
            0
        }
    }
}

// ── Consumer Group ───────────────────────────────────────────────────

/// C ABI: subscribe to consumer group. Returns handle, or 0 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_client_subscribe_group(
    handle: isize,
    topic: *const c_char,
    group: *const c_char,
    consumer: *const c_char,
) -> isize {
    let client = unsafe { handle_to_ref::<Client>(handle) };
    let topic = cstr_to_string(topic);
    let group = cstr_to_string(group);
    let consumer = cstr_to_string(consumer);
    let runtime = RUNTIME.get().unwrap();
    match runtime.block_on(client.subscribe_group(&topic, &group, &consumer)) {
        Ok(gs) => arc_to_handle(Arc::new(GroupSubInner::new(Some(gs)))),
        Err(e) => {
            set_last_error(&format!("GroupError: {e}"));
            0
        }
    }
}

/// C ABI: leave consumer group. Returns 0 on success, -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_client_leave_group(
    handle: isize,
    topic: *const c_char,
    group: *const c_char,
    consumer: *const c_char,
) -> i32 {
    let client = unsafe { handle_to_ref::<Client>(handle) };
    let topic = cstr_to_string(topic);
    let group = cstr_to_string(group);
    let consumer = cstr_to_string(consumer);
    let runtime = RUNTIME.get().unwrap();
    match runtime.block_on(client.leave_group(&topic, &group, &consumer)) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("GroupError: {e}"));
            -1
        }
    }
}

// ── RPC ──────────────────────────────────────────────────────────────

/// C ABI: RPC request/reply. Writes result to out_msg.
/// Returns 0 on success, 1 on timeout/closed, -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_client_rpc(
    handle: isize,
    req_topic: *const c_char,
    payload: *const u8,
    payload_len: usize,
    reply_topic: *const c_char,
    timeout_secs: f64,
    out_msg: *mut VireonMessage,
) -> i32 {
    let client = unsafe { handle_to_ref::<Client>(handle) };
    let req_topic = cstr_to_string(req_topic);
    let reply_topic = cstr_to_string(reply_topic);
    let payload = unsafe { std::slice::from_raw_parts(payload, payload_len) }.to_vec();
    let timeout = std::time::Duration::from_secs_f64(timeout_secs.max(0.001));
    let runtime = RUNTIME.get().unwrap();
    match runtime.block_on(client.rpc(&req_topic, payload, &reply_topic, timeout)) {
        Ok(msg) => {
            if !out_msg.is_null() {
                unsafe {
                    *out_msg = sdk_msg_to_c(msg);
                }
            }
            0
        }
        Err(e) => {
            set_last_error(&format!("RpcError: {e}"));
            -1
        }
    }
}

// ── Connection Management ────────────────────────────────────────────

/// C ABI: close + reclaim client handle. Returns 0 on success.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_client_close(handle: isize) -> i32 {
    let client = unsafe { handle_to_ref::<Client>(handle) };
    let runtime = RUNTIME.get().unwrap();
    let rc = runtime.block_on(client.close());
    unsafe { reclaim_handle::<Client>(handle) };
    match rc {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("ConnectError: {e}"));
            -1
        }
    }
}

/// C ABI: migrate connection. Returns 0 on success, -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_client_migrate(
    handle: isize,
    bind_addr: *const c_char,
) -> i32 {
    let client = unsafe { handle_to_ref::<Client>(handle) };
    let bind_addr = cstr_to_string(bind_addr);
    let runtime = RUNTIME.get().unwrap();
    match runtime.block_on(client.migrate(&bind_addr)) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&format!("ConnectError: {e}"));
            -1
        }
    }
}

/// C ABI: pending bytes. Returns the count (always succeeds).
#[unsafe(no_mangle)]
pub extern "C" fn vireon_client_pending_bytes(handle: isize) -> u64 {
    let client = unsafe { handle_to_ref::<Client>(handle) };
    client.pending_bytes() as u64
}

// ── Suppress unused import warning for SdkMessage ────────────────────
#[allow(dead_code)]
fn _use_message(_m: SdkMessage) {}
