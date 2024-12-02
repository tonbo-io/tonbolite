use crate::load_module;
use flume::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use wasm_bindgen::prelude::*;

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

    /// Convenience method to run multiple SQL statements (that cannot take any parameters)
    pub async fn execute_batch(&self, sql: String) {
        // TODO: Handling other types of sql
        self.conn.lock().unwrap().execute_batch(&sql).unwrap();
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
    pub async fn select(&self, sql: String) {
        let conn = self.conn.clone();
        wasm_thread::Builder::new()
            .spawn(|| async move {
                let db = conn.lock().unwrap();

                let mut stmt = db.prepare(&sql).unwrap();
                let mut rows = stmt.query([]).unwrap();
                while let Some(row) = rows.next().unwrap() {
                    // TODO: row binding
                    log::info!("{:#?}", row);
                }
            })
            .unwrap()
            .join_async()
            .await
            .unwrap();
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
