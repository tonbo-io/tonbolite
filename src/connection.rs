use crate::load_module;
use js_sys::Object;
use rusqlite::types::{FromSql, FromSqlResult, ValueRef};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::js_sys;

#[wasm_bindgen]
pub struct Connection {
    conn: Arc<Mutex<rusqlite::Connection>>,
}

impl Connection {
    async fn execute_inner(&self, sql: String) -> Result<usize, JsValue> {
        let conn = self.conn.clone();
        let count = wasm_thread::Builder::new()
            .spawn(|| async move { conn.lock().unwrap().execute(&sql, []).unwrap() })
            .unwrap()
            .join_async()
            .await
            .unwrap();

        Ok(count)
    }
}

#[wasm_bindgen]
impl Connection {
    pub fn open() -> Self {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        load_module(&conn).unwrap();
        Self {
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    /// Convenience method to prepare and execute a single SQL statement.
    ///
    /// On success, returns the number of rows that were changed or inserted or deleted
    pub async fn execute(&self, sql: String) -> Result<usize, JsValue> {
        // TODO: Handling `CREATE` sql

        self.execute_inner(sql).await
    }

    /// Convenience method to execute a single `CREATE` SQL statement.
    pub async fn create(&self, sql: String) -> Result<usize, JsValue> {
        Ok(self.conn.lock().unwrap().execute(&sql, []).unwrap())
    }

    /// Convenience method to execute a single `INSERT` SQL statement.
    pub async fn insert(&self, sql: String) -> Result<usize, JsValue> {
        self.execute_inner(sql).await
    }

    /// Convenience method to prepare and execute a single select SQL statement.
    pub async fn select(&self, sql: String) -> Vec<JsValue> {
        let conn = self.conn.clone();
        let rows = wasm_thread::Builder::new()
            .spawn(move || async move {
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

                result
            })
            .unwrap()
            .join_async()
            .await
            .unwrap();

        let mut result = vec![];
        for row in rows {
            result.push(row.into());
        }
        result
    }

    /// Convenience method to execute a single `DELETE` SQL statement.
    pub async fn delete(&self, sql: String) -> Result<usize, JsValue> {
        self.execute_inner(sql).await
    }

    /// Convenience method to execute a single `UPDATE` SQL statement.
    pub async fn update(&self, sql: String) -> Result<usize, JsValue> {
        self.execute_inner(sql).await
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
            ValueRef::Real(f) => {
                todo!()
            }
            ValueRef::Text(s) => Value::Text(String::from_utf8_lossy(s).to_string()),
            ValueRef::Blob(b) => {
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
