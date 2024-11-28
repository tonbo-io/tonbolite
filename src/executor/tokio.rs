use crate::executor::BlockOnExecutor;
use fusio::MaybeSend;
use std::future::Future;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tonbo::executor::Executor;

#[derive(Clone)]
pub struct TokioExecutor {
    pub(crate) runtime: Arc<Runtime>,
}

impl Executor for TokioExecutor {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + MaybeSend + 'static,
    {
        self.runtime.spawn(future);
    }
}

impl BlockOnExecutor for TokioExecutor {
    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }
}

impl TokioExecutor {
    pub fn new() -> Self {
        let runtime = Arc::new(
            Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .unwrap(),
        );
        Self { runtime }
    }
}
