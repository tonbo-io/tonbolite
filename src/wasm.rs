use crate::executor::SQLiteExecutor;
use crate::utils::{parse_create_sql_params, set_result, value_trans};
use flume::{Receiver, Sender};
use fusio::path::Path;
use fusio_dispatch::FsOptions;
use futures_util::StreamExt;
use rusqlite::types::ValueRef;
use rusqlite::vtab::{
    parse_boolean, Context, CreateVTab, IndexInfo, UpdateVTab, VTab, VTabConnection, VTabCursor,
    VTabKind, ValueIter, Values,
};
use rusqlite::{ffi, vtab, Error};
use std::ffi::c_int;
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::Arc;
use tonbo::record::{Column, ColumnDesc, DynRecord};
use tonbo::{DbOption, DB};

pub(crate) enum Task {
    Insert {
        sender: Sender<()>,
        record: DynRecord,
    },
    Select {
        sender: Sender<()>,
        range: (Bound<Column>, Bound<Column>),
    },
    Delete {
        sender: Sender<rusqlite::Result<()>>,
        key: Column,
    },
    Flush {
        sender: Sender<rusqlite::Result<()>>,
    },
}

#[derive(Default)]
pub struct DbState;

impl DbState {
    pub(crate) fn new() -> Self {
        Default::default()
    }
}

#[repr(C)]
pub struct TonboTable {
    base: ffi::sqlite3_vtab,
    state: Arc<DbState>,
    primary_key_index: usize,
    column_desc: Vec<ColumnDesc>,
    req_tx: Sender<Task>,
    tuple_rx: Receiver<Option<(Vec<Column>, usize)>>,
    handle: wasm_thread::JoinHandle<()>,
}

enum FsType {
    Local,
    S3,
}

impl TonboTable {
    fn connect_create(
        _: &mut VTabConnection,
        aux: Option<&Arc<DbState>>,
        args: &[&[u8]],
        _: bool,
    ) -> rusqlite::Result<(String, Self)> {
        let mut primary_key_index = None;
        let mut descs = Vec::new();
        let mut fields = Vec::with_capacity(args.len());
        let mut schema = None;
        let mut fs_option = FsType::Local;
        // Local
        let mut path = None;
        // S3
        let mut s3_url = None;
        let mut bucket = None;
        let mut key_id = None;
        let mut secret_key = None;
        let mut token = None;
        let mut endpoint = None;
        let mut region = None;
        let mut sign_payload = None;
        let mut checksum = None;

        let args = &args[3..];
        for c_slice in args.iter() {
            let (param, value) = vtab::parameter(c_slice)?;

            match param {
                "create_sql" => {
                    if schema.is_some() {
                        return Err(Error::ModuleError("`create_sql` duplicate".to_string()));
                    }
                    schema = Some(value.to_string());

                    (descs, primary_key_index) = parse_create_sql_params(value)?;
                }
                "fs" => match value {
                    "local" => fs_option = FsType::Local,
                    "s3" => fs_option = FsType::S3,
                    _ => {
                        return Err(Error::ModuleError(format!(
                            "unrecognized fs type '{param}'"
                        )))
                    }
                },
                "s3_url" => s3_url = Some(value.to_string()),
                "key_id" => key_id = Some(value.to_string()),
                "secret_key" => secret_key = Some(value.to_string()),
                "token" => token = Some(value.to_string()),
                "bucket" => bucket = Some(value.to_string()),
                "path" => path = Some(value.to_string()),
                "endpoint" => endpoint = Some(value.to_string()),
                "region" => region = Some(value.to_string()),
                "sign_payload" => sign_payload = parse_boolean(value),
                "checksum" => checksum = parse_boolean(value),
                _ => {
                    return Err(Error::ModuleError(format!(
                        "unrecognized parameter '{param}'"
                    )));
                }
            }

            fields.push(format!("{} {}", param, value));
        }
        let primary_key_index = primary_key_index.ok_or_else(|| {
            Error::ModuleError("the primary key must exist and only one is allowed".to_string())
        })?;
        let path = Path::from_opfs_path(
            path.ok_or_else(|| Error::ModuleError("`path` not found".to_string()))?,
        )
        .map_err(|err| Error::ModuleError(format!("path parser error: {err}")))?;
        let fs_option = match fs_option {
            FsType::Local => FsOptions::Local,
            FsType::S3 => {
                let mut credential = None;
                if key_id.is_some() || secret_key.is_some() || token.is_some() {
                    credential = Some(fusio::remotes::aws::AwsCredential {
                        key_id: key_id
                            .ok_or_else(|| Error::ModuleError("`key_id` not found".to_string()))?,
                        secret_key: secret_key.ok_or_else(|| {
                            Error::ModuleError("`secret_key` not found".to_string())
                        })?,
                        token,
                    });
                }
                FsOptions::S3 {
                    bucket: bucket
                        .ok_or_else(|| Error::ModuleError("`bucket` not found".to_string()))?,
                    credential,
                    endpoint,
                    region,
                    sign_payload,
                    checksum,
                }
            }
        };

        let mut options = DbOption::with_path(
            path,
            descs[primary_key_index].name.clone(),
            primary_key_index,
        );
        // .disable_wal();
        if matches!(fs_option, FsOptions::S3 { .. }) {
            let url = s3_url.ok_or_else(|| Error::ModuleError("`s3_url` not found".to_string()))?;
            let url = Path::from_url_path(url)
                .map_err(|err| Error::ModuleError(format!("path parser error: {err}")))?;
            options = options
                // TODO: We can add an option user to set all SSTables to use the same URL.
                .level_path(0, url.clone(), fs_option.clone())
                .unwrap()
                .level_path(1, url.clone(), fs_option.clone())
                .unwrap()
                .level_path(2, url.clone(), fs_option.clone())
                .unwrap()
                .level_path(3, url.clone(), fs_option.clone())
                .unwrap()
                .level_path(4, url.clone(), fs_option.clone())
                .unwrap()
                .level_path(5, url.clone(), fs_option.clone())
                .unwrap()
                .level_path(6, url, fs_option.clone())
                .unwrap();
        }
        let descs_clone = descs.clone();
        let (req_tx, req_rx): (Sender<Task>, _) = flume::bounded(100);

        let (tuple_tx, tuple_rx): (Sender<Option<(Vec<Column>, usize)>>, _) = flume::bounded(10);
        let handle = wasm_thread::Builder::new()
            .spawn(move || async move {
                let executor = SQLiteExecutor::new();
                let database = DB::with_schema(options, executor, descs_clone, primary_key_index)
                    .await
                    .map_err(|err| Error::ModuleError(err.to_string()))
                    .unwrap();

                while let Ok(task) = req_rx.recv() {
                    match task {
                        Task::Insert { sender, record } => {
                            database.insert(record).await.unwrap();

                            sender.send(()).unwrap();
                        }
                        Task::Select {
                            sender,
                            range: (lower, upper),
                        } => {
                            let transaction = database.transaction().await;

                            let mut stream = transaction
                                .scan((lower.as_ref(), upper.as_ref()))
                                .take()
                                .await
                                .unwrap();

                            sender.send(()).unwrap();

                            while let Some(result) = stream.next().await {
                                let entry = result.unwrap();
                                if let Some(value) = entry.value() {
                                    let _ =
                                        tuple_tx.send(Some((value.columns, value.primary_index)));
                                }
                            }
                            let _ = tuple_tx.send(None);
                        }
                        Task::Delete { sender, key } => {
                            match database.remove(key).await {
                                Ok(_) => sender.send(Ok(())).unwrap(),
                                Err(err) => sender
                                    .send(Err(Error::ModuleError(err.to_string())))
                                    .unwrap(),
                            };
                        }
                        Task::Flush { sender } => {
                            database.flush().await.unwrap();
                            sender.send(Ok(())).unwrap();
                        }
                    }
                }
            })
            .unwrap();

        Ok((
            schema.unwrap(),
            Self {
                base: ffi::sqlite3_vtab::default(),
                state: aux.unwrap().clone(),
                primary_key_index,
                column_desc: descs,
                req_tx,
                tuple_rx,
                handle,
            },
        ))
    }

    fn _remove(&mut self, pk: i64) -> Result<(), Error> {
        let desc = &self.column_desc[self.primary_key_index];
        let key = Column::new(
            desc.datatype,
            desc.name.clone(),
            Arc::new(pk),
            desc.is_nullable,
        );

        let (sender, receiver) = flume::bounded(1);

        self.req_tx.send(Task::Delete { sender, key }).unwrap();
        receiver.recv().unwrap()?;
        Ok(())
    }

    fn _insert(&mut self, args: ValueIter) -> Result<i64, Error> {
        let mut id = None;
        let mut values = Vec::with_capacity(self.column_desc.len());

        for (i, (desc, value)) in self.column_desc.iter().zip(args).enumerate() {
            if i == self.primary_key_index {
                id = Some(value);
            }
            values.push(Column::new(
                desc.datatype,
                desc.name.clone(),
                value_trans(value, &desc.datatype, desc.is_nullable)?,
                desc.is_nullable,
            ));
        }
        let record = DynRecord::new(values.clone(), self.primary_key_index);
        let record2 = DynRecord::new(values.clone(), self.primary_key_index);
        let record3 = DynRecord::new(values.clone(), self.primary_key_index);
        let record4 = DynRecord::new(values, self.primary_key_index);

        let (sender, receiver) = flume::bounded(1);

        self.req_tx.send(Task::Insert { sender, record }).unwrap();
        receiver.recv().unwrap();
        let (sender, receiver) = flume::bounded(1);

        self.req_tx
            .send(Task::Insert {
                sender,
                record: record2,
            })
            .unwrap();
        receiver.recv().unwrap();
        let (sender, receiver) = flume::bounded(1);

        self.req_tx
            .send(Task::Insert {
                sender,
                record: record3,
            })
            .unwrap();
        receiver.recv().unwrap();
        let (sender, receiver) = flume::bounded(1);

        self.req_tx
            .send(Task::Insert {
                sender,
                record: record4,
            })
            .unwrap();
        receiver.recv().unwrap();
        Ok(id.unwrap().as_i64()?)
    }
}

unsafe impl<'vtab> VTab<'vtab> for TonboTable {
    type Aux = Arc<DbState>;
    type Cursor = RecordCursor<'vtab>;

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
        Ok(RecordCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            req_tx: self.req_tx.clone(),
            tuple_rx: self.tuple_rx.clone(),
            buf: None,
            _p: Default::default(),
        })
    }

    fn integrity(&'vtab mut self, _flag: usize) -> rusqlite::Result<()> {
        let (sender, receiver) = flume::bounded(1);

        self.req_tx.send(Task::Flush { sender }).unwrap();
        receiver.recv().unwrap()
    }
}

impl CreateVTab<'_> for TonboTable {
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

#[repr(C)]
pub struct RecordCursor<'vtab> {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab_cursor,
    req_tx: Sender<Task>,
    tuple_rx: Receiver<Option<(Vec<Column>, usize)>>,
    buf: Option<(Vec<Column>, usize)>,
    _p: PhantomData<&'vtab TonboTable>,
}

impl RecordCursor<'_> {
    #[allow(unused)]
    fn vtab(&self) -> &TonboTable {
        unsafe { &*(self.base.pVtab as *const TonboTable) }
    }
}

impl UpdateVTab<'_> for TonboTable {
    fn delete(&mut self, arg: ValueRef<'_>) -> rusqlite::Result<()> {
        let desc = &self.column_desc[self.primary_key_index];
        let key = Column::new(
            desc.datatype,
            desc.name.clone(),
            Arc::new(arg.as_i64().unwrap()),
            desc.is_nullable,
        );
        let (sender, receiver) = flume::bounded(1);

        self.req_tx.send(Task::Delete { sender, key }).unwrap();
        receiver.recv().unwrap()?;

        Ok(())
    }

    fn insert(&mut self, args: &Values<'_>) -> rusqlite::Result<i64> {
        let mut args = args.iter();

        let _ = args.next();
        let _ = args.next();

        self._insert(args)
    }

    fn update(&mut self, args: &Values<'_>) -> rusqlite::Result<()> {
        let mut args = args.iter();
        let _ = args.next();
        let Some(old_pk) = args.next().map(|v| v.as_i64().unwrap()) else {
            return Ok(());
        };
        let new_pk = self._insert(args)?;
        if new_pk != old_pk {
            self._remove(old_pk)?;
        }

        Ok(())
    }
}

unsafe impl VTabCursor for RecordCursor<'_> {
    fn filter(&mut self, _: c_int, _: Option<&str>, _: &Values<'_>) -> rusqlite::Result<()> {
        let (sender, receiver) = flume::bounded(1);

        self.req_tx
            .send(Task::Select {
                sender,
                range: (Bound::Unbounded, Bound::Unbounded),
            })
            .unwrap();
        receiver.recv().unwrap();

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
        if let Some((columns, _)) = &self.buf {
            set_result(ctx, &columns[i as usize])?;
        }
        Ok(())
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        let (columns, pk_i) = self.buf.as_ref().unwrap();

        Ok(*columns[*pk_i].value.downcast_ref().unwrap())
    }
}
