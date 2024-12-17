use crate::executor::{BlockOnExecutor, SQLiteExecutor};
use crate::utils::{set_result, value_trans};
use flume::{Receiver, Sender};
use futures_util::StreamExt;
use rusqlite::types::ValueRef;
use rusqlite::vtab::{
    Context, CreateVTab, IndexInfo, UpdateVTab, VTab, VTabConnection, VTabCursor, VTabKind,
    ValueIter, Values,
};
use rusqlite::{ffi, vtab, Error};
use sqlparser::dialect::MySqlDialect;
use std::collections::Bound;
use std::ffi::c_int;
use std::marker::PhantomData;
use std::sync::Arc;
use tonbo::executor::Executor;
use tonbo::record::{Column, DynRecord, Record};
use tonbo_net_client::client::{TonboClient, TonboSchema};
use tonbo_net_client::proto::{tonbo_rpc_client, ColumnDesc, Datatype};

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
    schema: Option<TonboSchema>,
    req_tx: Sender<Request>,
}

enum FsType {
    Local,
    S3,
}

enum Request {
    Scan {
        lower: Bound<Column>,
        upper: Bound<Column>,
        tuple_tx: Sender<Option<DynRecord>>,
    },
    Insert(DynRecord),
    Remove(Column),
    Schema(Sender<Result<TonboSchema, Error>>),
    Flush(Sender<rusqlite::Result<()>>),
}

impl TonboTable {
    async fn handle_request(mut client: TonboClient, req_rx: Receiver<Request>) {
        while let Ok(req) = req_rx.recv() {
            match req {
                Request::Scan {
                    lower,
                    upper,
                    tuple_tx,
                } => {
                    let mut stream = client.scan(lower, upper).await.unwrap();

                    while let Some(result) = stream.next().await {
                        let entry = result.unwrap();
                        let _ = tuple_tx.send(Some(entry));
                    }
                    let _ = tuple_tx.send(None);
                }
                Request::Insert(record) => client.insert(record).await.unwrap(),
                Request::Remove(key) => client.remove(key).await.unwrap(),
                Request::Schema(schema_tx) => {
                    let schema = client
                        .schema()
                        .await
                        .map_err(|err| Error::ModuleError(err.to_string()));
                    schema_tx.send(schema).unwrap();
                }
                Request::Flush(sender) => {
                    let result = client
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
        let dialect = MySqlDialect {};

        let mut addr = None;
        let mut table_name = None;

        let args = &args[3..];
        for (i, c_slice) in args.iter().enumerate() {
            let (param, value) = vtab::parameter(c_slice)?;
            match param {
                "addr" => {
                    if addr.is_some() {
                        return Err(Error::ModuleError("`addr` duplicate".to_string()));
                    }
                    addr = Some(value.to_string());
                    println!("{:#?}", addr);
                }
                "table_name" => {
                    if table_name.is_some() {
                        return Err(Error::ModuleError("`table_name` duplicate".to_string()));
                    }
                    table_name = Some(value.to_string());
                }
                _ => {
                    return Err(Error::ModuleError(format!(
                        "unrecognized parameter '{param}'"
                    )));
                }
            }
        }
        let table_name =
            table_name.ok_or_else(|| Error::ModuleError("`table_name` not found".to_string()))?;

        let (req_tx, req_rx): (_, Receiver<Request>) = flume::bounded(1);

        #[cfg(feature = "wasm")]
        wasm_thread::Builder::new()
            .spawn(move || async move {
                let client = TonboClient::connect(
                    addr.ok_or_else(|| Error::ModuleError("`addr` not found".to_string()))
                        .unwrap(),
                )
                .await
                .map_err(|err| Error::ModuleError(err.to_string()))
                .unwrap();

                Self::handle_request(client, req_rx).await;
            })
            .expect("initialize web worker failed.");
        #[cfg(not(feature = "wasm"))]
        aux.unwrap().executor.spawn(async move {
            let client = TonboClient::connect(
                addr.ok_or_else(|| Error::ModuleError("`addr` not found".to_string()))
                    .unwrap(),
            )
            .await
            .map_err(|err| Error::ModuleError(err.to_string()))
            .unwrap();

            Self::handle_request(client, req_rx).await;
        });

        let create_table_sql =
            "CREATE TABLE IF NOT EXISTS tonbo(id bigint primary key, name varchar, like int)"
                .to_string();

        Ok((
            create_table_sql,
            Self {
                base: ffi::sqlite3_vtab::default(),
                req_tx,
                schema: None,
            },
        ))
    }

    fn schema(&mut self) -> Result<&TonboSchema, Error> {
        if self.schema.is_none() {
            let (schema_tx, schema_rx): (_, Receiver<Result<TonboSchema, Error>>) =
                flume::bounded(1);

            self.req_tx.send(Request::Schema(schema_tx)).unwrap();
            let schema = schema_rx.recv().unwrap()?;

            self.schema = Some(schema);
        }

        Ok(self.schema.as_ref().unwrap())
    }

    fn _remove(&mut self, pk: i64) -> Result<(), Error> {
        let desc: &ColumnDesc = &self.schema()?.primary_key_desc();
        let value = Column::new(
            tonbo::record::Datatype::from(
                Datatype::try_from(desc.ty).map_err(|err| Error::ModuleError(err.to_string()))?,
            ),
            desc.name.clone(),
            Arc::new(pk),
            desc.is_nullable,
        );

        self.req_tx.send(Request::Remove(value)).unwrap();
        Ok(())
    }

    fn _insert(&mut self, args: ValueIter) -> Result<i64, Error> {
        let mut id = None;
        let schema = self.schema()?;
        let mut values = Vec::with_capacity(schema.len());

        let primary_key_index = schema.primary_key_index;
        for (i, (desc, value)) in schema.desc.iter().zip(args).enumerate() {
            if i == primary_key_index {
                id = Some(value);
            }
            let datatype = tonbo::record::Datatype::from(
                Datatype::try_from(desc.ty).map_err(|err| Error::ModuleError(err.to_string()))?,
            );
            values.push(Column::new(
                datatype,
                desc.name.clone(),
                value_trans(value, &datatype, desc.is_nullable)?,
                desc.is_nullable,
            ));
        }

        self.req_tx
            .send(Request::Insert(DynRecord::new(values, primary_key_index)))
            .unwrap();
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
    tuple_rx: Option<Receiver<Option<DynRecord>>>,
    buf: Option<DynRecord>,
    _p: PhantomData<&'vtab TonboTable>,
}

impl RecordCursor<'_> {
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
    fn filter(&mut self, _: c_int, _: Option<&str>, _: &Values<'_>) -> rusqlite::Result<()> {
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
        if let Some(record) = &self.buf {
            set_result(ctx, &record.columns()[i as usize])?;
        }
        Ok(())
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        let record = self.buf.as_ref().unwrap();

        Ok(*record.primary_column().value.downcast_ref().unwrap())
    }
}

fn data_type_to_sql_type(data_type: &Datatype) -> &str {
    match data_type {
        Datatype::Uint8 => "TINYINT UNSIGNED",
        Datatype::Uint16 => "SMALLINT UNSIGNED",
        Datatype::Uint32 => "INT UNSIGNED",
        Datatype::Uint64 => "BIGINT UNSIGNED",
        Datatype::Int8 => "TINYINT",
        Datatype::Int16 => "SMALLINT",
        Datatype::Int32 => "INT",
        Datatype::Int64 => "BIGINT",
        Datatype::String => "TEXT",
        Datatype::Boolean => "BOOLEAN",
        Datatype::Bytes => "BLOB",
    }
}

fn desc_to_create_table(desc: &[ColumnDesc], table_name: &str) -> String {
    let columns: Vec<String> = desc
        .iter()
        .map(|field| {
            format!(
                "{} {} {}",
                field.name,
                data_type_to_sql_type(&Datatype::try_from(field.ty).unwrap()),
                if field.is_nullable { "nullable" } else { "" }
            )
        })
        .collect();

    format!(
        "CREATE TABLE {} (\n    {}\n);",
        table_name,
        columns.join(",\n    ")
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use rusqlite::Connection;
    use std::fs;

    #[test]
    fn test_load_module() -> rusqlite::Result<()> {
        let db = Connection::open_in_memory()?;
        crate::load_module(&db)?;

        db.execute_batch(
            "CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
                    table_name ='tonbo',
                    addr = 'http://[::1]:50051',
            );",
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
        let mut stmt = db.prepare("SELECT * FROM tonbo where id = ?1;")?;
        let mut rows = stmt.query(["2"])?;
        assert!(rows.next()?.is_none());

        db.pragma(None, "quick_check", "tonbo", |_r| -> rusqlite::Result<()> {
            Ok(())
        })?;
        Ok(())
    }
}
