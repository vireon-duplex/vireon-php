//! `vireon_connect` + `vireon_pool_connect` — build SDK ClientBuilder from
//! C parameters, connect, return handle.

use std::os::raw::c_char;
use std::sync::Arc;
use std::time::Duration;

use vireon_sdk::{ClientBuilder as SdkClientBuilder, ClientPool as SdkClientPool};

use crate::config::{build_identity, build_reconnect, build_tls_verify};
use crate::error::set_last_error;
use crate::handle::arc_to_handle;
use crate::runtime::RUNTIME;
use crate::string_h::{cstr_to_option, cstr_to_string};

/// Shared builder logic: convert C params → SDK ClientBuilder.
fn build_sdk_builder(
    addr: *const c_char,
    tls_mode: i32,
    tls_path: *const c_char,
    sni: *const c_char,
    max_msg_size: u64,
    subscriber_buffer: u64,
    cmd_channel_cap: u64,
    idle_timeout_secs: f64,
    reconnect_enabled: i32,
    reconnect_max_attempts: i32,
    reconnect_initial_secs: f64,
    reconnect_max_secs: f64,
    identity_cert: *const c_char,
    identity_key: *const c_char,
) -> SdkClientBuilder {
    let addr = cstr_to_string(addr);
    let tls_path = cstr_to_option(tls_path);
    let sni = cstr_to_option(sni);
    let tls_verify = build_tls_verify(tls_mode, tls_path);
    let identity = build_identity(cstr_to_option(identity_cert), cstr_to_option(identity_key));
    let reconnect = build_reconnect(
        reconnect_enabled != 0,
        reconnect_max_attempts,
        reconnect_initial_secs,
        reconnect_max_secs,
    );

    let mut builder = SdkClientBuilder::new(&addr);
    if let Some(ref s) = sni {
        builder = builder.sni(s.clone());
    }
    builder = builder.tls_verify(tls_verify);
    if let Some(id) = identity {
        builder = builder.client_identity(id);
    }
    builder = builder.reconnect(reconnect);
    if max_msg_size > 0 {
        builder = builder.max_message_size(max_msg_size as usize);
    }
    if subscriber_buffer > 0 {
        builder = builder.subscriber_buffer(subscriber_buffer as usize);
    }
    if cmd_channel_cap > 0 {
        builder = builder.cmd_channel_cap(cmd_channel_cap as usize);
    }
    if idle_timeout_secs > 0.0 {
        builder = builder.max_idle_timeout(Duration::from_secs_f64(idle_timeout_secs));
    }
    builder
}

/// C ABI: connect to a Vireon server. Returns handle (>0) on success, 0 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_connect(
    addr: *const c_char,
    tls_mode: i32,
    tls_path: *const c_char,
    sni: *const c_char,
    max_msg_size: u64,
    subscriber_buffer: u64,
    cmd_channel_cap: u64,
    idle_timeout_secs: f64,
    reconnect_enabled: i32,
    reconnect_max_attempts: i32,
    reconnect_initial_secs: f64,
    reconnect_max_secs: f64,
    identity_cert: *const c_char,
    identity_key: *const c_char,
) -> isize {
    let builder = build_sdk_builder(
        addr, tls_mode, tls_path, sni, max_msg_size, subscriber_buffer,
        cmd_channel_cap, idle_timeout_secs, reconnect_enabled,
        reconnect_max_attempts, reconnect_initial_secs, reconnect_max_secs,
        identity_cert, identity_key,
    );
    let runtime = RUNTIME.get().unwrap();
    match runtime.block_on(builder.connect()) {
        Ok(client) => arc_to_handle(Arc::new(client)),
        Err(e) => {
            set_last_error(&format!("ConnectError: {e}"));
            0
        }
    }
}

/// C ABI: connect a pool of N clients. Returns pool handle, or 0 on error.
#[unsafe(no_mangle)]
pub extern "C" fn vireon_pool_connect(
    addr: *const c_char,
    tls_mode: i32,
    tls_path: *const c_char,
    sni: *const c_char,
    max_msg_size: u64,
    subscriber_buffer: u64,
    cmd_channel_cap: u64,
    idle_timeout_secs: f64,
    reconnect_enabled: i32,
    reconnect_max_attempts: i32,
    reconnect_initial_secs: f64,
    reconnect_max_secs: f64,
    identity_cert: *const c_char,
    identity_key: *const c_char,
    n: i32,
) -> isize {
    let builder = build_sdk_builder(
        addr, tls_mode, tls_path, sni, max_msg_size, subscriber_buffer,
        cmd_channel_cap, idle_timeout_secs, reconnect_enabled,
        reconnect_max_attempts, reconnect_initial_secs, reconnect_max_secs,
        identity_cert, identity_key,
    );
    let n = n.max(1) as usize;
    let runtime = RUNTIME.get().unwrap();
    match runtime.block_on(SdkClientPool::connect(builder, n)) {
        Ok(pool) => arc_to_handle(Arc::new(pool)),
        Err(e) => {
            set_last_error(&format!("ConnectError: {e}"));
            0
        }
    }
}
