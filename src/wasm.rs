use crate::executor::SQLiteExecutor;
use crate::utils::{parse_type, set_result, value_trans};
use flume::{Receiver, Sender};
use fusio::path::Path;
use futures_util::StreamExt;
use rusqlite::types::ValueRef;
use rusqlite::vtab::{
    Context, CreateVTab, IndexInfo, UpdateVTab, VTab, VTabConnection, VTabCursor, VTabKind,
    ValueIter, Values,
};
use rusqlite::{ffi, vtab, Error};
use std::ffi::c_int;
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::Arc;
use tonbo::record::runtime::Datatype;
use tonbo::record::{Column, ColumnDesc, DynRecord};
use tonbo::transaction::CommitError;
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

        let mut i = 0;
        for c_slice in args.iter() {
            let Ok((param, value)) = vtab::parameter(c_slice) else {
                continue;
            };
            let (ty, is_nullable, is_primary_key) = parse_type(value)?;

            if is_primary_key {
                if primary_key_index.is_some() {
                    return Err(Error::ModuleError(
                        "the primary key must exist and only one is allowed".to_string(),
                    ));
                }
                primary_key_index = Some(i)
            }

            fields.push(format!("{} {}", param, value));
            descs.push(ColumnDesc::new(param.to_string(), ty, is_nullable));
            i += 1;
        }
        let primary_key_index = primary_key_index.ok_or_else(|| {
            Error::ModuleError("the primary key must exist and only one is allowed".to_string())
        })?;
        if descs[primary_key_index].datatype != Datatype::Int64 {
            return Err(Error::ModuleError(
                "the primary key must be of int type".to_string(),
            ));
        }
        let descs_clone = descs.clone();
        let (req_tx, req_rx): (Sender<Task>, _) = flume::bounded(1);

        let (tuple_tx, tuple_rx): (Sender<Option<(Vec<Column>, usize)>>, _) = flume::bounded(10);
        wasm_thread::Builder::new()
            .spawn(move || async move {
                let options = DbOption::with_path(
                    Path::from_opfs_path("db_path/tonbo").unwrap(),
                    descs_clone[primary_key_index].name.clone(),
                    primary_key_index,
                );

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
                    }
                }
            })
            .unwrap();

        Ok((
            format!("CREATE TABLE tonbo({})", fields.join(", ")),
            Self {
                base: ffi::sqlite3_vtab::default(),
                state: aux.unwrap().clone(),
                primary_key_index,
                column_desc: descs,
                req_tx,
                tuple_rx,
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
        let record = DynRecord::new(values, self.primary_key_index);

        let (sender, receiver) = flume::bounded(1);

        self.req_tx.send(Task::Insert { sender, record }).unwrap();
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
