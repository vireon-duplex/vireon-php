//! Configuration helpers: DeliveryPolicy ordinal → SDK enum, TLS mode →
//! `SdkTlsVerify`, reconnect params → `SdkReconnectPolicy`.

use std::path::PathBuf;
use std::time::Duration;

use vireon_sdk::{ClientIdentity, DeliveryPolicy as SdkDeliveryPolicy, ReconnectPolicy, TlsVerify};

/// Map the C# `DeliveryPolicy` ordinal to the SDK enum.
pub(crate) fn ordinal_to_policy(ord: i32) -> SdkDeliveryPolicy {
    match ord {
        0 => SdkDeliveryPolicy::ReliableOrdered,
        1 => SdkDeliveryPolicy::ReliableUnordered,
        2 => SdkDeliveryPolicy::RealtimeDropOld,
        _ => SdkDeliveryPolicy::LatestOnly,
    }
}

pub(crate) const TLS_DANGER_ACCEPT_INVALID: i32 = 1;
pub(crate) const TLS_STRICT: i32 = 2;
pub(crate) const TLS_PINNED: i32 = 3;

/// Build the SDK `TlsVerify` from the C# mode + optional path.
pub(crate) fn build_tls_verify(mode: i32, path: Option<String>) -> TlsVerify {
    match mode {
        TLS_STRICT => TlsVerify::Strict {
            ca: PathBuf::from(path.unwrap_or_default()),
        },
        TLS_PINNED => TlsVerify::Pinned {
            cert_der: path
                .map(|p| std::fs::read(&p).unwrap_or_default())
                .unwrap_or_default(),
        },
        TLS_DANGER_ACCEPT_INVALID => TlsVerify::DangerAcceptInvalid,
        _ => TlsVerify::Tofu,
    }
}

/// Build the SDK `ReconnectPolicy` from C# parameters.
pub(crate) fn build_reconnect(
    enabled: bool,
    max_attempts: i32,
    initial_secs: f64,
    max_secs: f64,
) -> ReconnectPolicy {
    if !enabled {
        return ReconnectPolicy::disabled();
    }
    ReconnectPolicy {
        max_attempts: max_attempts.max(1) as u32,
        initial_backoff: Duration::from_secs_f64(initial_secs.max(0.001)),
        max_backoff: Duration::from_secs_f64(max_secs.max(0.001)),
        resubscribe: true,
    }
}

/// Build the SDK `ClientIdentity` from C# file paths.
pub(crate) fn build_identity(cert: Option<String>, key: Option<String>) -> Option<ClientIdentity> {
    match (cert, key) {
        (Some(c), Some(k)) => Some(ClientIdentity {
            cert: PathBuf::from(c),
            key: PathBuf::from(k),
        }),
        _ => None,
    }
}
