pub(crate) mod tokio;

use std::future::Future;

pub trait BlockOnExecutor: tonbo::executor::Executor {
    fn block_on<F: Future>(&self, future: F) -> F::Output;
}
