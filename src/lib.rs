use std::collections::Bound;
use std::ffi::c_int;
use std::marker::PhantomData;
use std::sync::Arc;
use flume::{Receiver, Sender};
use futures_util::StreamExt;
use rusqlite::{ffi, Error};
use rusqlite::types::Null;
use rusqlite::vtab::{Context, IndexInfo, VTab, VTabConnection, VTabCursor, Values};
use tokio::runtime::Runtime;
use tonbo::DB;
use tonbo::executor::tokio::TokioExecutor;
use tonbo::record::Record;
use tonbo_macros::Record;

#[derive(Record, Debug)]
pub struct Music {
    #[record(primary_key)]
    id: i64,
    name: Option<String>,
    like: i64,
}

#[repr(C)]
pub struct MusicTable {
    base: ffi::sqlite3_vtab,
    runtime: Runtime,
    database: Arc<DB<Music, TokioExecutor>>,
}

unsafe impl<'vtab> VTab<'vtab> for MusicTable {
    type Aux = Arc<DB<Music, TokioExecutor>>;
    type Cursor = MusicCursor<'vtab>;

    fn connect(db: &mut VTabConnection, aux: Option<&Self::Aux>, args: &[&[u8]]) -> rusqlite::Result<(String, Self)> {
        todo!()
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        todo!()
    }

    fn open(&'vtab mut self) -> rusqlite::Result<Self::Cursor> {
        let (req_tx, req_rx): (Sender<MusicRequest>, _) = flume::bounded(1);
        let (tuple_tx, tuple_rx): (Sender<Option<Music>>, _) = flume::bounded(10);
        let database = self.database.clone();

        self.runtime.spawn(async move {
            while let Ok(req) = req_rx.recv() {
                let transaction = database.transaction().await;

                match req {
                    MusicRequest::Query((lower, upper)) => {
                        let mut stream = transaction.scan((lower.as_ref(), upper.as_ref())).take().await.unwrap();

                        while let Some(result) = stream.next().await {
                            let entry = result.unwrap();

                            let item = Music {
                                id: entry.value().unwrap().id,
                                name: Some(entry.value().unwrap().name.unwrap().to_string()),
                                like: entry.value().unwrap().like.unwrap(),
                            };

                            let _ = tuple_tx.send(Some(item));
                        }
                        let _ = tuple_tx.send(None);
                    }
                    MusicRequest::Update(item) => {
                        todo!()
                    }
                    MusicRequest::Insert(item) => {
                        todo!()
                    }
                    MusicRequest::Delete(item) => {
                        todo!()
                    }
                }
            }
        });

        Ok(MusicCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            req_tx,
            tuple_rx,
            buf: None,
            _p: Default::default(),
        })
    }
}

pub(crate) enum MusicRequest {
    Query((Bound<<Music as Record>::Key>, Bound<<Music as Record>::Key>)),
    Update(Music),
    Insert(Music),
    Delete(Music),
}

#[repr(C)]
pub struct MusicCursor<'vtab> {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab_cursor,
    req_tx: Sender<MusicRequest>,
    tuple_rx: Receiver<Option<Music>>,
    buf: Option<Music>,
    _p: PhantomData<&'vtab MusicTable>,
}

impl MusicCursor<'_> {
    fn vtab(&self) -> &MusicTable {
        unsafe { &*(self.base.pVtab as *const MusicTable) }
    }
}

unsafe impl VTabCursor for MusicCursor<'_> {
    fn filter(&mut self, idx_num: c_int, idx_str: Option<&str>, args: &Values<'_>) -> rusqlite::Result<()> {
        self.req_tx.send(MusicRequest::Query((Bound::Unbounded, Bound::Unbounded))).unwrap();
        self.next()?;

        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        self.buf = self.tuple_rx.recv().unwrap();

        Ok(())
    }

    fn eof(&self) -> bool {
        self.buf.is_none()
    }

    fn column(&self, ctx: &mut Context, i: c_int) -> rusqlite::Result<()> {
        if i > 2 {
            return Err(Error::ModuleError(format!(
                "column index out of bounds: {i}"
            )))
        }
        if let Some(item) = &self.buf {
            match i {
                0 => ctx.set_result(&item.id)?,
                1 => {
                    if let Some(like) = &item.name {
                        ctx.set_result(like)?;
                    } else {
                        ctx.set_result(&Null)?;
                    }
                },
                2 => ctx.set_result(&item.name)?,
                _ => return Err(Error::ModuleError(format!(
                    "column index out of bounds: {i}"
                ))),
            }
        }
        Ok(())
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.buf.as_ref().unwrap().id)
    }
}

fn main() {

}
