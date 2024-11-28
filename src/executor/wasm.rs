use crate::executor::BlockOnExecutor;
use fusio::MaybeSend;
use tonbo::executor::Executor;

use std::future::Future;

#[derive(Clone, Default)]
pub struct WasmExecutor;

impl Executor for WasmExecutor {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + MaybeSend + 'static,
    {
        wasm_bindgen_futures::spawn_local(future)
    }
}

impl BlockOnExecutor for WasmExecutor {
    fn block_on<F: Future>(&self, _future: F) -> F::Output {
        todo!()
    }
}

impl WasmExecutor {
    pub fn new() -> Self {
        Default::default()
    }
}
