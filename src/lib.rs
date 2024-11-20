use flume::{Receiver, Sender};
use fusio::path::Path;
use futures_util::StreamExt;
use rusqlite::types::{Null, ValueRef};
use rusqlite::vtab::{
    update_module, Context, CreateVTab, IndexInfo, UpdateVTab, VTab, VTabConnection, VTabCursor,
    VTabKind, Values,
};
use rusqlite::{ffi, Connection, Error};
use std::collections::Bound;
use std::ffi::c_int;
use std::fs;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tonbo::executor::tokio::TokioExecutor;
use tonbo::record::Record;
use tonbo::{DbOption, DB};
use tonbo_macros::Record;

pub fn load_module(conn: &Connection) -> rusqlite::Result<()> {
    let _ = fs::create_dir_all("./db_path/music");
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let database = runtime.block_on(async {
        let options = DbOption::from(Path::from_filesystem_path("./db_path/music").unwrap());
        DB::new(options, TokioExecutor::default())
            .await
            .map_err(|err| Error::ModuleError(err.to_string()))
    })?;

    let aux = Some(Arc::new(DbState {
        runtime,
        database: Arc::new(database),
    }));
    conn.create_module("tonbo", update_module::<MusicTable>(), aux)
}

#[derive(Record, Debug)]
pub struct Music {
    #[record(primary_key)]
    id: i64,
    name: Option<String>,
    like: i64,
}

pub struct DbState {
    runtime: Runtime,
    database: Arc<DB<Music, TokioExecutor>>,
}

#[repr(C)]
pub struct MusicTable {
    base: ffi::sqlite3_vtab,
    state: Arc<DbState>,
}

impl MusicTable {
    fn connect_create(
        _: &mut VTabConnection,
        aux: Option<&Arc<DbState>>,
        _: &[&[u8]],
        _: bool,
    ) -> rusqlite::Result<(String, Self)> {
        Ok((
            "CREATE TABLE music(id int, name varchar, like int)".to_string(),
            Self {
                base: ffi::sqlite3_vtab::default(),
                state: aux.unwrap().clone(),
            },
        ))
    }
}

unsafe impl<'vtab> VTab<'vtab> for MusicTable {
    type Aux = Arc<DbState>;
    type Cursor = MusicCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, Self)> {
        Self::connect_create(db, aux, args, false)
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(500.);
        info.set_estimated_rows(500);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<Self::Cursor> {
        let (req_tx, req_rx): (Sender<MusicRequest>, _) = flume::bounded(1);
        let (tuple_tx, tuple_rx): (Sender<Option<Music>>, _) = flume::bounded(10);
        let database = self.state.database.clone();

        self.state.runtime.spawn(async move {
            while let Ok(req) = req_rx.recv() {
                let transaction = database.transaction().await;

                match req {
                    MusicRequest::Query((lower, upper)) => {
                        let mut stream = transaction
                            .scan((lower.as_ref(), upper.as_ref()))
                            .take()
                            .await
                            .unwrap();

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

impl CreateVTab<'_> for MusicTable {
    const KIND: VTabKind = VTabKind::Default;

    fn create(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, Self)> {
        Self::connect_create(db, aux, args, true)
    }

    fn destroy(&self) -> rusqlite::Result<()> {
        Ok(())
    }
}

pub(crate) enum MusicRequest {
    Query((Bound<<Music as Record>::Key>, Bound<<Music as Record>::Key>)),
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

impl UpdateVTab<'_> for MusicTable {
    fn delete(&mut self, _: ValueRef<'_>) -> rusqlite::Result<()> {
        todo!()
    }

    fn insert(&mut self, args: &Values<'_>) -> rusqlite::Result<i64> {
        let mut args = args.iter();

        let _ = args.next();
        let _ = args.next();

        let id = args.next().unwrap().as_i64().unwrap();
        let name = args
            .next()
            .unwrap()
            .as_str_or_null()
            .unwrap()
            .map(str::to_string);
        let like = args.next().unwrap().as_i64().unwrap();

        self.state
            .runtime
            .block_on(async {
                self.state
                    .database
                    .insert(Music { id, name, like })
                    .await
                    .map(|_| id)
            })
            .map_err(|err| Error::ModuleError(err.to_string()))
    }

    fn update(&mut self, _: &Values<'_>) -> rusqlite::Result<()> {
        todo!()
    }
}

unsafe impl VTabCursor for MusicCursor<'_> {
    fn filter(&mut self, _: c_int, _: Option<&str>, _: &Values<'_>) -> rusqlite::Result<()> {
        self.req_tx
            .send(MusicRequest::Query((Bound::Unbounded, Bound::Unbounded)))
            .unwrap();
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
            )));
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
                }
                2 => ctx.set_result(&item.name)?,
                _ => {
                    return Err(Error::ModuleError(format!(
                        "column index out of bounds: {i}"
                    )))
                }
            }
        }
        Ok(())
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.buf.as_ref().unwrap().id)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use rusqlite::Connection;

    #[test]
    fn test_load_module() -> rusqlite::Result<()> {
        let db = Connection::open_in_memory()?;
        super::load_module(&db)?;

        db.execute_batch("CREATE VIRTUAL TABLE temp.music USING tonbo();")?;
        db.execute(
            "INSERT INTO music (id, name, like) VALUES (0, 'lol', 0)",
            [],
        )?;
        let mut stmt = db.prepare("SELECT * FROM music;")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            println!("{:#?}", row);
        }

        Ok(())
    }
}
