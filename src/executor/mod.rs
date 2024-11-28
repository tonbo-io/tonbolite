#[cfg(feature = "tokio")]
pub(crate) mod tokio;
#[cfg(feature = "wasm")]
pub(crate) mod wasm;

use std::future::Future;

#[cfg(not(feature = "wasm"))]
pub type SQLiteExecutor = tokio::TokioExecutor;
#[cfg(feature = "wasm")]
pub type SQLiteExecutor = wasm::WasmExecutor;

pub trait BlockOnExecutor: tonbo::executor::Executor {
    fn block_on<F: Future>(&self, future: F) -> F::Output;
}
