use crate::load_module;
use std::sync::{Arc, Mutex};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Connection {
    conn: Arc<Mutex<rusqlite::Connection>>,
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

        let conn = self.conn.clone();
        let count = wasm_thread::Builder::new()
            .spawn(|| async move { conn.lock().unwrap().execute(&sql, []).unwrap() })
            .unwrap()
            .join_async()
            .await
            .unwrap();

        Ok(count)
    }

    /// Convenience method to prepare and execute a single select SQL statement.
    pub async fn select(&self, sql: String) {
        let conn = self.conn.clone();
        wasm_thread::Builder::new()
            .spawn(|| async move {
                log::info!("execute \"{}\"", sql.clone());
                let db = conn.lock().unwrap();

                let mut stmt = db.prepare(&sql).unwrap();
                let mut rows = stmt.query([]).unwrap();
                while let Some(row) = rows.next().unwrap() {
                    log::info!("{:#?}", row);
                }
            })
            .unwrap()
            .join_async()
            .await
            .unwrap();
    }
}
