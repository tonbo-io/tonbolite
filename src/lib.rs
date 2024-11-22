mod executor;

use crate::executor::tokio::TokioExecutor;
use crate::executor::BlockOnExecutor;
use flume::{Receiver, Sender};
use fusio::path::Path;
use fusio_dispatch::FsOptions;
use futures_util::StreamExt;
use rusqlite::types::ValueRef;
use rusqlite::vtab::{
    parse_boolean, update_module, Context, CreateVTab, IndexInfo, UpdateVTab, VTab, VTabConnection,
    VTabCursor, VTabKind, Values,
};
use rusqlite::{ffi, vtab, Connection, Error};
use sqlparser::ast::{ColumnOption, DataType, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::any::Any;
use std::collections::Bound;
use std::ffi::c_int;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::runtime::Builder;
use tonbo::executor::Executor;
use tonbo::record::runtime::Datatype;
use tonbo::record::{Column, ColumnDesc, DynRecord, Record};
use tonbo::{DbOption, DB};

pub fn load_module(conn: &Connection) -> rusqlite::Result<()> {
    let runtime = Arc::new(
        Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap(),
    );

    let aux = Some(Arc::new(DbState {
        executor: TokioExecutor { runtime },
    }));
    conn.create_module("tonbo", update_module::<TonboTable>(), aux)
}

pub struct DbState {
    executor: TokioExecutor,
}

#[repr(C)]
pub struct TonboTable {
    base: ffi::sqlite3_vtab,
    state: Arc<DbState>,
    database: Arc<DB<DynRecord, TokioExecutor>>,
    primary_key_index: usize,
    column_desc: Vec<ColumnDesc>,
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
        let dialect = MySqlDialect {};
        let mut primary_key_index = None;
        let mut descs = Vec::new();

        let mut schema = None;
        let mut fs_option = None;
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
        for (i, c_slice) in args.iter().enumerate() {
            let (param, value) = vtab::parameter(c_slice)?;
            match param {
                "create_sql" => {
                    if schema.is_some() {
                        return Err(Error::ModuleError("`create_sql` duplicate".to_string()));
                    }
                    schema = Some(value.to_string());
                    if let Statement::CreateTable(create_table) =
                        &Parser::parse_sql(&dialect, value)
                            .map_err(|err| Error::ModuleError(err.to_string()))?[0]
                    {
                        for column_def in create_table.columns.iter() {
                            let name = column_def.name.value.to_ascii_lowercase();
                            let datatype = type_trans(&column_def.data_type);
                            let mut is_not_nullable = column_def
                                .options
                                .iter()
                                .any(|option| matches!(option.option, ColumnOption::NotNull));
                            let is_primary_key = column_def.options.iter().any(|option| {
                                matches!(
                                    option.option,
                                    ColumnOption::Unique {
                                        is_primary: true,
                                        ..
                                    }
                                )
                            });
                            if is_primary_key {
                                if primary_key_index.is_some() {
                                    return Err(Error::ModuleError(
                                        "the primary key must exist and only one is allowed"
                                            .to_string(),
                                    ));
                                }
                                is_not_nullable = true;
                                primary_key_index = Some(i)
                            }
                            descs.push(ColumnDesc {
                                datatype,
                                is_nullable: !is_not_nullable,
                                name,
                            })
                        }
                    } else {
                        return Err(Error::ModuleError(format!(
                            "`CreateTable` SQL syntax error: '{value}'"
                        )));
                    }
                }
                "fs" => match value {
                    "local" => fs_option = Some(FsType::Local),
                    "s3" => fs_option = Some(FsType::S3),
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
        }
        let primary_key_index = primary_key_index.ok_or_else(|| {
            Error::ModuleError("the primary key must exist and only one is allowed".to_string())
        })?;
        if descs[primary_key_index].datatype != Datatype::Int64 {
            return Err(Error::ModuleError(
                "the primary key must be of `bigint` type".to_string(),
            ));
        }
        let path = Path::from_filesystem_path(
            path.ok_or_else(|| Error::ModuleError("`path` not found".to_string()))?,
        )
        .map_err(|err| Error::ModuleError(format!("path parser error: {err}")))?;
        let fs_option = match fs_option.unwrap_or(FsType::Local) {
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
        let executor = aux.unwrap().executor.clone();
        let database = aux.unwrap().executor.block_on(async {
            DB::with_schema(options, executor, descs.clone(), primary_key_index)
                .await
                .map_err(|err| Error::ModuleError(err.to_string()))
        })?;

        Ok((
            schema.unwrap(),
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
        let (tuple_tx, tuple_rx): (Sender<Option<(Vec<Column>, usize)>>, _) = flume::bounded(10);
        let database = self.database.clone();

        self.state.executor.spawn(async move {
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

                    let _ = tuple_tx.send(Some((value.columns, value.primary_index)));
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
    tuple_rx: Receiver<Option<(Vec<Column>, usize)>>,
    buf: Option<(Vec<Column>, usize)>,
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
            .executor
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

// TODO: Value Cast
fn value_trans(value: ValueRef<'_>, _ty: &Datatype, is_nullable: bool) -> Arc<dyn Any> {
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
        ValueRef::Integer(v) => {
            if is_nullable {
                Arc::new(Some(v))
            } else {
                Arc::new(v)
            }
        }
        ValueRef::Real(v) => {
            if is_nullable {
                Arc::new(Some(v))
            } else {
                Arc::new(v)
            }
        }
        ValueRef::Text(v) => {
            if is_nullable {
                Arc::new(Some(String::from_utf8(v.to_vec()).unwrap()))
            } else {
                Arc::new(String::from_utf8(v.to_vec()).unwrap())
            }
        }
        ValueRef::Blob(v) => {
            if is_nullable {
                Arc::new(Some(v.to_vec()))
            } else {
                Arc::new(v.to_vec())
            }
        }
    }
}

fn set_result(ctx: &mut Context, col: &Column) -> rusqlite::Result<()> {
    match &col.datatype {
        Datatype::UInt8 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<u8>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<u8>().unwrap())?;
            }
        }
        Datatype::UInt16 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<u16>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<u16>().unwrap())?;
            }
        }
        Datatype::UInt32 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<u32>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<u32>().unwrap())?;
            }
        }
        Datatype::UInt64 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<u64>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<u64>().unwrap())?;
            }
        }
        Datatype::Int8 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<i8>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<i8>().unwrap())?;
            }
        }
        Datatype::Int16 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<i16>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<i16>().unwrap())?;
            }
        }
        Datatype::Int32 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<i32>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<i32>().unwrap())?;
            }
        }
        Datatype::Int64 => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<i64>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<i64>().unwrap())?;
            }
        }
        Datatype::String => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<String>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<String>().unwrap())?;
            }
        }
        Datatype::Boolean => {
            if col.is_nullable {
                ctx.set_result(col.value.as_ref().downcast_ref::<Option<bool>>().unwrap())?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<bool>().unwrap())?;
            }
        }
        Datatype::Bytes => {
            if col.is_nullable {
                ctx.set_result(
                    col.value
                        .as_ref()
                        .downcast_ref::<Option<Vec<u8>>>()
                        .unwrap(),
                )?;
            } else {
                ctx.set_result(col.value.as_ref().downcast_ref::<Vec<u8>>().unwrap())?;
            }
        }
    }
    Ok(())
}

fn type_trans(ty: &DataType) -> Datatype {
    match ty {
        DataType::Int8(_) => Datatype::Int8,
        DataType::Int16 | DataType::SmallInt(_) => Datatype::Int16,
        DataType::Int(_) | DataType::Int32 | DataType::Integer(_) => Datatype::Int32,
        DataType::Int64 | DataType::BigInt(_) => Datatype::Int64,
        DataType::UnsignedInt(_) | DataType::UInt32 | DataType::UnsignedInteger(_) => {
            Datatype::UInt32
        }
        DataType::UInt8 | DataType::UnsignedInt8(_) => Datatype::UInt8,
        DataType::UInt16 => Datatype::UInt16,
        DataType::UInt64 | DataType::UnsignedBigInt(_) => Datatype::UInt64,
        DataType::Bool | DataType::Boolean => Datatype::Boolean,
        DataType::Bytes(_) => Datatype::Bytes,
        DataType::Varchar(_) => Datatype::String,
        _ => todo!(),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use rusqlite::Connection;
    use std::fs;

    #[test]
    fn test_load_module() -> rusqlite::Result<()> {
        let _ = fs::create_dir_all("./db_path/test");

        let db = Connection::open_in_memory()?;
        super::load_module(&db)?;

        db.execute_batch(
            "CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
                    create_sql='create table tonbo(id bigint primary key, name varchar, like bigint)',
                    path = './db_path/test',
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

    #[test]
    fn test_load_module_on_s3() -> rusqlite::Result<()> {
        let _ = fs::create_dir_all("./db_path/test_s3");

        let db = Connection::open_in_memory()?;
        super::load_module(&db)?;

        db.execute_batch(
            "CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
                    create_sql='create table tonbo(id bigint primary key, name varchar, like bigint)',
                    path = './db_path/test_s3',
                    bucket = 'data',
                    key_id = 'user',
                    secret_key = 'password',
                    endpoint = 'http://localhost:9000',
            );",
        )?;
        let num = 100000;
        for i in 0..num {
            db.execute(
                &format!("INSERT INTO tonbo (id, name, like) VALUES ({i}, 'lol', {i})"),
                [],
            )?;
        }

        let mut stmt = db.prepare("SELECT * FROM tonbo limit 10;")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            println!("{:#?}", row);
        }

        Ok(())
    }
}
