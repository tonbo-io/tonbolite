use crate::load_module;
use flume::{Receiver, Sender};
use js_sys::Object;
use rusqlite::types::{FromSql, FromSqlResult, ValueRef};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::js_sys;

#[wasm_bindgen]
pub struct Connection {
    conn: Arc<Mutex<rusqlite::Connection>>,
    req_tx: Option<Sender<Request>>,
    resp_rx: Option<Receiver<Response>>,
}

enum Request {
    Insert(String),
    Delete(String),
    Update(String),
    Select(String),
    Flush(String),
}

enum Response {
    RowCount(usize),
    Rows(Vec<JsRow>),
    Empty,
}

#[wasm_bindgen]
impl Connection {
    pub fn open() -> Self {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        load_module(&conn).unwrap();
        Self {
            conn: Arc::new(Mutex::new(conn)),
            req_tx: None,
            resp_rx: None,
        }
    }

    /// Convenience method to execute a single `CREATE` SQL statement.
    pub async fn create(&mut self, sql: String) -> Result<usize, JsValue> {
        let res = { self.conn.lock().unwrap().execute(&sql, []).unwrap() };

        let (req_tx, req_rx) = flume::bounded(1);
        let (resp_tx, resp_rx) = flume::bounded(1);
        self.req_tx = Some(req_tx);
        self.resp_rx = Some(resp_rx);
        let conn = self.conn.clone();
        wasm_thread::Builder::new()
            .spawn(move || async move {
                while let Ok(req) = req_rx.recv_async().await {
                    match req {
                        Request::Insert(sql) | Request::Delete(sql) | Request::Update(sql) => {
                            let res = conn.lock().unwrap().execute(&sql, []).unwrap();
                            resp_tx.send(Response::RowCount(res)).unwrap();
                        }
                        Request::Select(sql) => {
                            let db = conn.lock().unwrap();
                            let mut result = vec![];
                            let mut stmt = db.prepare(&sql).unwrap();
                            let mut rows = stmt.query([]).unwrap();
                            while let Some(row) = rows.next().unwrap() {
                                let stmt = row.as_ref();
                                let mut cols = vec![];

                                for i in 0..stmt.column_count() {
                                    let name = stmt.column_name(i).expect("valid column index");
                                    let mut value: JsColumn = row.get(i).unwrap();
                                    value.set_name(name.to_string());

                                    cols.push(value);
                                }
                                result.push(JsRow::new(cols))
                            }
                            resp_tx.send_async(Response::Rows(result)).await.unwrap();
                        }
                        Request::Flush(table) => {
                            conn.lock()
                                .unwrap()
                                .pragma(
                                    None,
                                    "quick_check",
                                    table.as_str(),
                                    |_r| -> rusqlite::Result<()> { Ok(()) },
                                )
                                .unwrap();
                            resp_tx.send(Response::Empty).unwrap();
                        }
                    }
                }
            })
            .unwrap();
        Ok(res)
    }

    /// Convenience method to execute a single `INSERT` SQL statement.
    pub async fn insert(&self, sql: String) -> Result<usize, JsValue> {
        self.req_tx
            .as_ref()
            .unwrap()
            .send_async(Request::Insert(sql))
            .await
            .unwrap();
        let Response::RowCount(count) = self.resp_rx.as_ref().unwrap().recv_async().await.unwrap()
        else {
            unreachable!()
        };

        Ok(count)
    }

    /// Convenience method to prepare and execute a single select SQL statement.
    pub async fn select(&self, sql: String) -> Vec<JsValue> {
        self.req_tx
            .as_ref()
            .unwrap()
            .send_async(Request::Select(sql))
            .await
            .unwrap();
        let Response::Rows(rows) = self.resp_rx.as_ref().unwrap().recv_async().await.unwrap()
        else {
            unreachable!()
        };
        let mut result = vec![];
        for row in rows {
            result.push(row.into());
        }
        result
    }

    /// Convenience method to execute a single `DELETE` SQL statement.
    pub async fn delete(&self, sql: String) -> Result<usize, JsValue> {
        self.req_tx
            .as_ref()
            .unwrap()
            .send_async(Request::Update(sql))
            .await
            .unwrap();
        let Response::RowCount(count) = self.resp_rx.as_ref().unwrap().recv_async().await.unwrap()
        else {
            unreachable!()
        };

        Ok(count)
    }

    /// Convenience method to execute a single `UPDATE` SQL statement.
    pub async fn update(&self, sql: String) -> Result<usize, JsValue> {
        self.req_tx
            .as_ref()
            .unwrap()
            .send_async(Request::Delete(sql))
            .await
            .unwrap();
        let Response::RowCount(count) = self.resp_rx.as_ref().unwrap().recv_async().await.unwrap()
        else {
            unreachable!()
        };

        Ok(count)
    }

    /// Flush data to stable storage.
    pub async fn flush(&self, table: String) {
        self.req_tx
            .as_ref()
            .unwrap()
            .send_async(Request::Flush(table))
            .await
            .unwrap();
        let _ = self.resp_rx.as_ref().unwrap().recv_async().await.unwrap();
    }
}

#[derive(Debug, Clone)]
struct JsColumn {
    name: Option<String>,
    value: Value,
}

impl JsColumn {
    fn new(value: Value) -> JsColumn {
        JsColumn { name: None, value }
    }

    fn set_name(&mut self, name: String) {
        self.name = Some(name);
    }
}

#[derive(Debug)]
struct JsRow {
    cols: Vec<JsColumn>,
}

impl JsRow {
    fn new(cols: Vec<JsColumn>) -> JsRow {
        JsRow { cols }
    }
}

#[derive(Clone, Debug)]
enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

impl FromSql for JsColumn {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let value = match value {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(i) => Value::Integer(i),
            ValueRef::Real(f) => Value::Real(f),
            ValueRef::Text(s) => Value::Text(String::from_utf8_lossy(s).to_string()),
            ValueRef::Blob(_b) => {
                todo!()
            }
        };

        Ok(JsColumn::new(value))
    }
}

impl From<JsRow> for JsValue {
    fn from(row: JsRow) -> Self {
        let object = Object::new();
        for col in row.cols {
            match col.value {
                Value::Null => {
                    js_sys::Reflect::set(&object, &col.name.unwrap().into(), &JsValue::null())
                        .unwrap();
                }
                Value::Integer(n) => {
                    js_sys::Reflect::set(&object, &col.name.unwrap().into(), &JsValue::from(n))
                        .unwrap();
                }
                Value::Real(f) => {
                    js_sys::Reflect::set(&object, &col.name.unwrap().into(), &JsValue::from_f64(f))
                        .unwrap();
                }
                Value::Text(s) => {
                    js_sys::Reflect::set(&object, &col.name.unwrap().into(), &JsValue::from(s))
                        .unwrap();
                }
            }
        }
        object.into()
    }
}
