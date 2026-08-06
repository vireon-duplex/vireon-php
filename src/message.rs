//! C-compatible `VireonMessage` struct + batch + memory management.
//!
//! Rust allocates topic/payload; C# copies to managed memory then calls
//! `vireon_msg_free` / `vireon_batch_free`.

use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;

/// C-compatible message struct.
#[repr(C)]
pub struct VireonMessage {
    /// UTF-8 null-terminated topic (CString::into_raw).
    pub topic: *const c_char,
    /// Payload bytes (Box<[u8]>::into_raw).
    pub payload: *const u8,
    /// Payload length.
    pub payload_len: usize,
    /// Per-stream sequence number.
    pub seq: u64,
    /// Logical stream id.
    pub stream_id: u64,
}

impl Default for VireonMessage {
    fn default() -> Self {
        Self {
            topic: ptr::null(),
            payload: ptr::null(),
            payload_len: 0,
            seq: 0,
            stream_id: 0,
        }
    }
}

/// C-compatible batch result for recvBatch.
#[repr(C)]
pub struct VireonMsgBatch {
    /// Pointer to array of VireonMessage.
    pub msgs: *mut VireonMessage,
    /// Number of messages.
    pub count: usize,
}

impl Default for VireonMsgBatch {
    fn default() -> Self {
        Self {
            msgs: ptr::null_mut(),
            count: 0,
        }
    }
}

/// Convert an SDK Message into a C-compatible struct.
/// Caller MUST free via `vireon_msg_free`.
pub(crate) fn sdk_msg_to_c(msg: vireon_sdk::Message) -> VireonMessage {
    let topic = CString::new(String::from_utf8_lossy(&msg.topic).into_owned())
        .unwrap_or_default();
    let topic_ptr = topic.into_raw();

    let payload_len = msg.payload.len();
    let payload_ptr = if payload_len > 0 {
        let boxed: Box<[u8]> = msg.payload.to_vec().into_boxed_slice();
        Box::into_raw(boxed) as *const u8
    } else {
        ptr::null()
    };

    VireonMessage {
        topic: topic_ptr,
        payload: payload_ptr,
        payload_len,
        seq: msg.seq,
        stream_id: msg.stream_id,
    }
}

/// C ABI: free a VireonMessage's allocations.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_msg_free(msg: *mut VireonMessage) {
    if msg.is_null() {
        return;
    }
    unsafe {
        let m = &mut *msg;
        if !m.topic.is_null() {
            let _ = CString::from_raw(m.topic as *mut c_char);
            m.topic = ptr::null();
        }
        if !m.payload.is_null() && m.payload_len > 0 {
            let slice = std::slice::from_raw_parts_mut(m.payload as *mut u8, m.payload_len);
            let _ = Box::from_raw(slice as *mut [u8]);
            m.payload = ptr::null();
            m.payload_len = 0;
        }
    }
}

/// C ABI: free a VireonMsgBatch (frees each message, then the array).
#[unsafe(no_mangle)]
pub extern "C" fn vireon_batch_free(batch: *mut VireonMsgBatch) {
    if batch.is_null() {
        return;
    }
    unsafe {
        let b = &mut *batch;
        for i in 0..b.count {
            let msg_ptr = b.msgs.add(i);
            vireon_msg_free(msg_ptr);
        }
        if !b.msgs.is_null() && b.count > 0 {
            let _ = Vec::from_raw_parts(b.msgs, b.count, b.count);
        }
        b.msgs = ptr::null_mut();
        b.count = 0;
    }
}
