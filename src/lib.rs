use flume::{Receiver, Sender};
use fusio::path::Path;
use futures_util::StreamExt;
use rusqlite::types::ValueRef;
use rusqlite::vtab::{
    update_module, Context, CreateVTab, IndexInfo, UpdateVTab, VTab, VTabConnection, VTabCursor,
    VTabKind, Values,
};
use rusqlite::{ffi, vtab, Connection, Error};
use std::any::Any;
use std::collections::Bound;
use std::ffi::c_int;
use std::fs;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tonbo::executor::tokio::TokioExecutor;
use tonbo::record::runtime::Datatype;
use tonbo::record::{Column, ColumnDesc, DynRecord, Record};
use tonbo::{DbOption, DB};

macro_rules! value_trans {
    ($is_nullable:expr, $value:expr) => {
        if $is_nullable {
            Arc::new(Some($value))
        } else {
            Arc::new($value)
        }
    };
}

macro_rules! set_result {
    ($context:expr, $column:expr, $ty:ty) => {
        if $column.is_nullable {
            $context.set_result($column.value.as_ref().downcast_ref::<Option<$ty>>().unwrap())?;
        } else {
            $context.set_result($column.value.as_ref().downcast_ref::<$ty>().unwrap())?;
        }
    };
}

struct Tuple {
    pk_i: usize,
    values: Vec<Column>,
}

pub fn load_module(conn: &Connection) -> rusqlite::Result<()> {
    let _ = fs::create_dir_all("./db_path/tonbo");
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let aux = Some(Arc::new(DbState { runtime }));
    conn.create_module("tonbo", update_module::<TonboTable>(), aux)
}

pub struct DbState {
    runtime: Runtime,
}

#[repr(C)]
pub struct TonboTable {
    base: ffi::sqlite3_vtab,
    state: Arc<DbState>,
    database: Arc<DB<DynRecord, TokioExecutor>>,
    primary_key_index: usize,
    column_desc: Vec<ColumnDesc>,
}

impl TonboTable {
    fn parse_type(input: &str) -> rusqlite::Result<(Datatype, bool, bool)> {
        let input = input.trim();

        let is_nullable = input.contains("nullable");
        let is_primary_key = input.contains("primary key");

        let mut type_str = input.to_string();
        if is_nullable {
            type_str = type_str.replace("nullable", "");
        }
        if is_primary_key {
            type_str = type_str.replace("primary key", "");
        }
        let ty = match type_str.trim() {
            "int" => Datatype::Int64,
            "varchar" => Datatype::String,
            _ => {
                return Err(Error::ModuleError(format!(
                    "unrecognized parameter '{input}'"
                )));
            }
        };

        Ok((ty, is_nullable, is_primary_key))
    }

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
            let (ty, is_nullable, is_primary_key) = Self::parse_type(value)?;

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
        let database = aux.unwrap().runtime.block_on(async {
            let options = DbOption::with_path(
                Path::from_filesystem_path("./db_path/tonbo").unwrap(),
                descs[primary_key_index].name.clone(),
                primary_key_index,
            );

            DB::with_schema(
                options,
                TokioExecutor::default(),
                descs.clone(),
                primary_key_index,
            )
            .await
            .map_err(|err| Error::ModuleError(err.to_string()))
        })?;
        
        Ok((
            format!("CREATE TABLE tonbo({})", fields.join(", ")),
            Self {
                base: ffi::sqlite3_vtab::default(),
                state: aux.unwrap().clone(),
                database: Arc::new(database),
                primary_key_index,
                column_desc: descs,
            },
        ))
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
        let (req_tx, req_rx): (
            Sender<(
                Bound<<DynRecord as Record>::Key>,
                Bound<<DynRecord as Record>::Key>,
            )>,
            _,
        ) = flume::bounded(1);
        let (tuple_tx, tuple_rx): (Sender<Option<Tuple>>, _) = flume::bounded(10);
        let database = self.database.clone();

        self.state.runtime.spawn(async move {
            while let Ok((lower, upper)) = req_rx.recv() {
                let transaction = database.transaction().await;

                let mut stream = transaction
                    .scan((lower.as_ref(), upper.as_ref()))
                    .take()
                    .await
                    .unwrap();

                while let Some(result) = stream.next().await {
                    let entry = result.unwrap();
                    let value = entry.value().unwrap();

                    let _ = tuple_tx.send(Some(Tuple {
                        pk_i: value.primary_index,
                        values: value.columns,
                    }));
                }
                let _ = tuple_tx.send(None);
            }
        });

        Ok(RecordCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            req_tx,
            tuple_rx,
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
    req_tx: Sender<(
        Bound<<DynRecord as Record>::Key>,
        Bound<<DynRecord as Record>::Key>,
    )>,
    tuple_rx: Receiver<Option<Tuple>>,
    buf: Option<Tuple>,
    _p: PhantomData<&'vtab TonboTable>,
}

impl RecordCursor<'_> {
    fn vtab(&self) -> &TonboTable {
        unsafe { &*(self.base.pVtab as *const TonboTable) }
    }
}

impl UpdateVTab<'_> for TonboTable {
    fn delete(&mut self, _: ValueRef<'_>) -> rusqlite::Result<()> {
        todo!()
    }

    fn insert(&mut self, args: &Values<'_>) -> rusqlite::Result<i64> {
        let mut args = args.iter();

        let _ = args.next();
        let _ = args.next();

        let mut id = None;
        let values = self
            .column_desc
            .iter()
            .zip(args)
            .enumerate()
            .map(|(i, (desc, value))| {
                if i == self.primary_key_index {
                    id = Some(value);
                }
                Column::new(
                    desc.datatype,
                    desc.name.clone(),
                    value_trans(value, &desc.datatype, desc.is_nullable),
                    desc.is_nullable,
                )
            })
            .collect();

        self.state
            .runtime
            .block_on(async {
                self.database
                    .insert(DynRecord::new(values, self.primary_key_index))
                    .await
            })
            .map_err(|err| Error::ModuleError(err.to_string()))?;
        Ok(id.unwrap().as_i64()?)
    }

    fn update(&mut self, _: &Values<'_>) -> rusqlite::Result<()> {
        todo!()
    }
}

unsafe impl VTabCursor for RecordCursor<'_> {
    fn filter(&mut self, _: c_int, _: Option<&str>, _: &Values<'_>) -> rusqlite::Result<()> {
        self.req_tx
            .send((Bound::Unbounded, Bound::Unbounded))
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
        if let Some(tuple) = &self.buf {
            set_result(ctx, &tuple.values[i as usize])?;
        }
        Ok(())
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        let Tuple { values, pk_i } = self.buf.as_ref().unwrap();

        Ok(*values[*pk_i].value.downcast_ref().unwrap())
    }
}

fn value_trans(value: ValueRef<'_>, ty: &Datatype, is_nullable: bool) -> Arc<dyn Any> {
    match value {
        ValueRef::Null => {
            todo!()
            // match ty {
            //     Datatype::UInt8 => Arc::new(Option::<u8>::None),
            //     Datatype::UInt16 => Arc::new(Option::<u16>::None),
            //     Datatype::UInt32 => Arc::new(Option::<u32>::None),
            //     Datatype::UInt64 => Arc::new(Option::<u64>::None),
            //     Datatype::Int8 => Arc::new(Option::<i8>::None),
            //     Datatype::Int16 => Arc::new(Option::<i16>::None),
            //     Datatype::Int32 => Arc::new(Option::<i32>::None),
            //     Datatype::Int64 => Arc::new(Option::<i64>::None),
            //     Datatype::String => Arc::new(Option::<String>::None),
            //     Datatype::Boolean => Arc::new(Option::<bool>::None),
            //     Datatype::Bytes => Arc::new(Option::<Vec<u8>>::None),
            // }
        }
        ValueRef::Integer(v) => value_trans!(is_nullable, v),
        ValueRef::Real(v) => value_trans!(is_nullable, v),
        ValueRef::Text(v) => value_trans!(is_nullable, v),
        ValueRef::Blob(v) => value_trans!(is_nullable, v),
    }
}

fn set_result(ctx: &mut Context, col: &Column) -> rusqlite::Result<()> {
    match &col.datatype {
        Datatype::UInt8 => set_result!(ctx, col, u8),
        Datatype::UInt16 => set_result!(ctx, col, u16),
        Datatype::UInt32 => set_result!(ctx, col, u32),
        Datatype::UInt64 => set_result!(ctx, col, u64),
        Datatype::Int8 => set_result!(ctx, col, i8),
        Datatype::Int16 => set_result!(ctx, col, i16),
        Datatype::Int32 => set_result!(ctx, col, i32),
        Datatype::Int64 => set_result!(ctx, col, i64),
        Datatype::String => set_result!(ctx, col, String),
        Datatype::Boolean => set_result!(ctx, col, bool),
        Datatype::Bytes => set_result!(ctx, col, Vec<u8>),
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use rusqlite::Connection;

    #[test]
    fn test_load_module() -> rusqlite::Result<()> {
        let db = Connection::open_in_memory()?;
        super::load_module(&db)?;

        db.execute_batch(
            "CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
                    id='int primary key',
                    name='varchar nullable',
                    like='int nullable'
                    );",
        )?;
        db.execute(
            "INSERT INTO tonbo (id, name, like) VALUES (0, 'lol', 0)",
            [],
        )?;
        let mut stmt = db.prepare("SELECT * FROM tonbo;")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            println!("{:#?}", row);
        }

        Ok(())
    }
}
