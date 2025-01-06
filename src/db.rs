use crate::executor::{BlockOnExecutor, SQLiteExecutor};
use crate::utils::type_trans;
use crate::utils::{set_result, value_trans};
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
use sqlparser::ast::ColumnOption;
use sqlparser::ast::Statement;
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;
use std::collections::Bound;
use std::ffi::c_int;
use std::marker::PhantomData;
use std::sync::Arc;
use tonbo::executor::Executor;
use tonbo::record::{DynRecord, DynSchema};
use tonbo::{DbOption, DB};

const MAX_LEVEL: usize = 7;

pub struct DbState {
    executor: SQLiteExecutor,
}

impl DbState {
    pub(crate) fn new() -> Self {
        Self {
            executor: SQLiteExecutor::new(),
        }
    }
}

#[repr(C)]
pub struct TonboTable {
    base: ffi::sqlite3_vtab,
    descs: Vec<tonbo::record::ValueDesc>,
    pk_index: usize,
    req_tx: Sender<Request>,
}

enum Request {
    Scan {
        lower: Bound<tonbo::record::Value>,
        upper: Bound<tonbo::record::Value>,
        tuple_tx: Sender<Option<(Vec<tonbo::record::Value>, usize)>>,
    },
    Insert((Sender<rusqlite::Result<()>>, DynRecord)),
    Remove((Sender<rusqlite::Result<()>>, tonbo::record::Value)),
    Flush(Sender<rusqlite::Result<()>>),
}

impl TonboTable {
    async fn handle_request(db: DB<DynRecord, SQLiteExecutor>, req_rx: Receiver<Request>) {
        while let Ok(req) = req_rx.recv() {
            match req {
                Request::Scan {
                    lower,
                    upper,
                    tuple_tx,
                } => {
                    let transaction = db.transaction().await;

                    let mut stream = transaction
                        .scan((lower.as_ref(), upper.as_ref()))
                        .take()
                        .await
                        .unwrap();

                    while let Some(result) = stream.next().await {
                        let entry = result.unwrap();
                        if let Some(value) = entry.value() {
                            let _ = tuple_tx.send(Some((value.columns, value.primary_index)));
                        }
                    }
                    let _ = tuple_tx.send(None);
                }
                Request::Insert((tx, record)) => {
                    db.insert(record).await.unwrap();
                    tx.send(Ok(())).unwrap()
                }
                Request::Remove((tx, key)) => {
                    db.remove(key).await.unwrap();
                    tx.send(Ok(())).unwrap()
                }
                Request::Flush(sender) => {
                    let result = db
                        .flush()
                        .await
                        .map_err(|err| Error::ModuleError(err.to_string()));

                    sender.send(result).unwrap();
                }
            }
        }
    }

    fn connect_create(
        _: &mut VTabConnection,
        aux: Option<&Arc<DbState>>,
        args: &[&[u8]],
        _: bool,
    ) -> rusqlite::Result<(String, Self)> {
        let mut create_sql = None;
        let mut primary_key_index = None;
        let mut descs = Vec::new();
        let mut is_local = true;
        // Local
        let mut path = None;
        // S3
        let mut bucket = None;
        let mut key_id = None;
        let mut secret_key = None;
        let mut token = None;
        let mut endpoint = None;
        let mut region = None;
        let mut sign_payload = None;
        let mut checksum = None;
        let mut table_name = None;
        let mut level = 2;

        let args = &args[3..];
        for c_slice in args.iter() {
            let (param, value) = vtab::parameter(c_slice)?;
            match param {
                "create_sql" => {
                    if create_sql.is_some() {
                        return Err(Error::ModuleError("`create_sql` duplicate".to_string()));
                    }
                    create_sql = Some(value.to_string());
                    (table_name, descs, primary_key_index) = parse_create_sql(value)?;
                }
                "fs" => match value {
                    "local" => is_local = true,
                    "s3" => is_local = false,
                    _ => {
                        return Err(Error::ModuleError(format!(
                            "unrecognized fs type '{param}'"
                        )))
                    }
                },
                "level" => level = value.parse::<usize>().unwrap(),
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
        let pk_index = primary_key_index.ok_or_else(|| {
            Error::ModuleError("primary key not found on `create_sql`".to_string())
        })?;
        let table_name =
            table_name.ok_or_else(|| Error::ModuleError("`create_sql` not found".to_string()))?;

        let (req_tx, req_rx): (_, Receiver<Request>) = flume::bounded(1);
        let fs_option = if is_local {
            FsOptions::Local
        } else {
            let mut credential = None;
            if key_id.is_some() || secret_key.is_some() || token.is_some() {
                credential = Some(fusio::remotes::aws::AwsCredential {
                    key_id: key_id
                        .ok_or_else(|| Error::ModuleError("`key_id` not found".to_string()))?,
                    secret_key: secret_key
                        .ok_or_else(|| Error::ModuleError("`secret_key` not found".to_string()))?,
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
        };
        let schema = DynSchema::new(descs.clone(), pk_index);

        #[cfg(feature = "wasm")]
        wasm_thread::Builder::new()
            .spawn(move || async move {
                let path = Path::from_opfs_path(path.expect("`path` not found")).unwrap();
                let mut option = DbOption::new(path, &schema);
                if !is_local {
                    option = level_path(option, fs_option, &table_name, level);
                }
                let db = DB::new(option, SQLiteExecutor::new(), schema)
                    .await
                    .unwrap();
                Self::handle_request(db, req_rx).await;
            })
            .expect("initialize web worker failed.");
        #[cfg(not(feature = "wasm"))]
        {
            let path = Path::from_filesystem_path(
                path.ok_or_else(|| Error::ModuleError("`path` not found".to_string()))?,
            )
            .map_err(|err| Error::ModuleError(format!("path parser error: {err}")))?;
            let executor = aux.unwrap().executor.clone();
            let db = aux.unwrap().executor.block_on(async {
                let mut option = DbOption::new(path, &schema);
                if !is_local {
                    option = level_path(option, fs_option, &table_name, level);
                }

                DB::<DynRecord, SQLiteExecutor>::new(option, executor, schema)
                    .await
                    .unwrap()
            });
            aux.unwrap().executor.spawn(async move {
                Self::handle_request(db, req_rx).await;
            });
        }

        Ok((
            create_sql.ok_or_else(|| Error::ModuleError("`create_sql` not found".to_string()))?,
            Self {
                base: ffi::sqlite3_vtab::default(),
                req_tx,
                descs,
                pk_index,
            },
        ))
    }

    fn _remove(&mut self, pk: i64) -> Result<(), Error> {
        let desc: &tonbo::record::ValueDesc = &self.descs[self.pk_index];
        let value = tonbo::record::Value::new(
            desc.datatype,
            desc.name.clone(),
            Arc::new(pk),
            desc.is_nullable,
        );
        let (remove_tx, remove_rx) = flume::bounded(1);

        self.req_tx
            .send(Request::Remove((remove_tx, value)))
            .unwrap();
        let _ = remove_rx.recv().unwrap();
        Ok(())
    }

    fn _insert(&mut self, args: ValueIter) -> Result<i64, Error> {
        let mut id = None;
        let mut values = Vec::with_capacity(self.descs.len());

        for (i, (desc, value)) in self.descs.iter().zip(args).enumerate() {
            if i == self.pk_index {
                id = Some(value);
            }
            values.push(tonbo::record::Value::new(
                desc.datatype,
                desc.name.clone(),
                value_trans(value, &desc.datatype, desc.is_nullable)?,
                desc.is_nullable,
            ));
        }
        let (insert_tx, insert_rx) = flume::bounded(1);

        self.req_tx
            .send(Request::Insert((
                insert_tx,
                DynRecord::new(values, self.pk_index),
            )))
            .unwrap();
        let _ = insert_rx.recv().unwrap();

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
            tuple_rx: None,
            buf: None,
            _p: Default::default(),
        })
    }

    fn integrity(&'vtab mut self, _flag: usize) -> rusqlite::Result<()> {
        let (flush_tx, flush_rx) = flume::bounded(1);

        self.req_tx.send(Request::Flush(flush_tx)).unwrap();
        flush_rx.recv().unwrap()
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
    req_tx: Sender<Request>,
    tuple_rx: Option<Receiver<Option<(Vec<tonbo::record::Value>, usize)>>>,
    buf: Option<(Vec<tonbo::record::Value>, usize)>,
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
        self._remove(arg.as_i64().unwrap())?;

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
    fn filter(&mut self, _: c_int, _: Option<&str>, v: &Values<'_>) -> rusqlite::Result<()> {
        let (tuple_tx, tuple_rx) = flume::bounded(5);

        self.req_tx
            .send(Request::Scan {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                tuple_tx,
            })
            .unwrap();
        self.tuple_rx = Some(tuple_rx);
        self.next()?;

        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        self.buf = self
            .tuple_rx
            .as_mut()
            .expect("`filter` was not called")
            .recv()
            .unwrap();

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
        let (columns, primary_index) = self.buf.as_ref().unwrap();

        Ok(*columns[*primary_index].value.downcast_ref().unwrap())
    }
}

fn parse_create_sql(
    sql: &str,
) -> rusqlite::Result<(Option<String>, Vec<tonbo::record::ValueDesc>, Option<usize>)> {
    let dialect = SQLiteDialect {};
    let mut primary_key_index = None;
    let mut descs = Vec::new();
    let mut table_name = None;
    if let Statement::CreateTable(create_table) =
        &Parser::parse_sql(&dialect, sql).map_err(|err| Error::ModuleError(err.to_string()))?[0]
    {
        table_name = Some(create_table.name.to_string());
        for (i, column_def) in create_table.columns.iter().enumerate() {
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
                        "the primary key must exist and only one is allowed".to_string(),
                    ));
                }
                is_not_nullable = true;
                primary_key_index = Some(i)
            }
            descs.push(tonbo::record::ValueDesc {
                datatype,
                is_nullable: !is_not_nullable,
                name,
            })
        }
    } else {
        return Err(Error::ModuleError(format!(
            "`CreateTable` SQL syntax error: '{sql}'"
        )));
    }
    Ok((table_name, descs, primary_key_index))
}

fn level_path(
    mut option: DbOption,
    fs_options: FsOptions,
    table_name: &str,
    level: usize,
) -> DbOption {
    for i in level..MAX_LEVEL {
        option = option
            .level_path(
                i,
                Path::from_url_path(format!("/{}/{}", table_name, i)).unwrap(),
                fs_options.clone(),
            )
            .unwrap();
    }

    option
}

#[cfg(test)]
pub(crate) mod tests {
    use rusqlite::Connection;
    use std::fs;

    #[test]
    fn test_load_module() -> rusqlite::Result<()> {
        let _ = fs::create_dir_all("./db_path/test");

        let db = Connection::open_in_memory()?;
        crate::load_module(&db)?;

        db.execute_batch(
            "CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
                    create_sql = 'create table tonbo(id bigint primary key, name varchar, like int)',
                    path = 'db_path/test'
            );"
        )?;
        for i in 0..3 {
            db.execute(
                &format!("INSERT INTO tonbo (id, name, like) VALUES ({i}, 'lol', {i})"),
                [],
            )?;
        }
        let mut stmt = db.prepare("SELECT * FROM tonbo;")?;
        let mut rows = stmt.query([])?;
        let row = rows.next()?.unwrap();
        assert_eq!(row.get_ref_unwrap(0).as_i64().unwrap(), 0);
        assert_eq!(row.get_ref_unwrap(1).as_str().unwrap(), "lol");
        assert_eq!(row.get_ref_unwrap(2).as_i64().unwrap(), 0);
        let row = rows.next()?.unwrap();
        assert_eq!(row.get_ref_unwrap(0).as_i64().unwrap(), 1);
        assert_eq!(row.get_ref_unwrap(1).as_str().unwrap(), "lol");
        assert_eq!(row.get_ref_unwrap(2).as_i64().unwrap(), 1);
        let row = rows.next()?.unwrap();
        assert_eq!(row.get_ref_unwrap(0).as_i64().unwrap(), 2);
        assert_eq!(row.get_ref_unwrap(1).as_str().unwrap(), "lol");
        assert_eq!(row.get_ref_unwrap(2).as_i64().unwrap(), 2);
        assert!(rows.next()?.is_none());

        db.execute(
            "UPDATE tonbo SET name = ?1, like = ?2, id = 4 WHERE id = ?3",
            ["ioi", "9", "0"],
        )?;
        let mut stmt = db.prepare("SELECT * FROM tonbo where id = ?1 or id = ?2;")?;
        let mut rows = stmt.query(["0", "4"])?;

        let row = rows.next()?.unwrap();
        assert_eq!(row.get_ref_unwrap(0).as_i64().unwrap(), 4);
        assert_eq!(row.get_ref_unwrap(1).as_str().unwrap(), "ioi");
        assert_eq!(row.get_ref_unwrap(2).as_i64().unwrap(), 9);
        assert!(rows.next()?.is_none());

        db.execute("DELETE from tonbo WHERE id = ?1", ["2"])?;
        db.pragma(None, "quick_check", "tonbo", |_r| -> rusqlite::Result<()> {
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn test_load_module_on_s3() -> rusqlite::Result<()> {
        let _ = fs::create_dir_all("./db_path/test_s3");

        let db = Connection::open_in_memory()?;
        crate::load_module(&db)?;

        db.execute_batch(
            "CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
                    create_sql='create table tonbo(id bigint primary key, name varchar, like int)',
                    path = './db_path/test_s3',
                    level = '0',
                    fs = 's3',
                    bucket = 'data',
                    key_id = 'user',
                    secret_key = 'password',
                    endpoint = 'http://localhost:9000',
            );",
        )?;
        let num = 100;
        for i in 0..num {
            db.execute(
                &format!("INSERT INTO tonbo (id, name, like) VALUES ({i}, 'lol', {i})"),
                [],
            )?;
        }
        db.pragma(None, "quick_check", "tonbo", |_r| -> rusqlite::Result<()> {
            Ok(())
        })?;
        let mut stmt = db.prepare("SELECT * FROM tonbo limit 10;")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            println!("{:#?}", row);
        }

        Ok(())
    }
}
