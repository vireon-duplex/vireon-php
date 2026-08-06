//! Global tokio runtime — initialized once via `vireon_init()`.

use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;

pub(crate) static RUNTIME: OnceCell<Runtime> = OnceCell::new();

pub(crate) fn init() {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to create vireon-csharp tokio runtime")
    });
}
