mod executor;

#[cfg(feature = "wasm")]
mod connection;
#[cfg(feature = "wasm")]
pub use connection::*;

mod db;

mod utils;
use crate::db::{DbState, TonboTable};
use rusqlite::vtab::update_module;
use std::fs;
use std::sync::Arc;

#[no_mangle]
pub fn load_module(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    let _ = fs::create_dir_all("./db_path/tonbo");

    let aux = Some(Arc::new(DbState::new()));
    conn.create_module("tonbo", update_module::<TonboTable>(), aux)
}

#[cfg(feature = "wasm")]
#[wasm_bindgen::prelude::wasm_bindgen(start)]
pub fn startup() {
    console_log::init().expect("could not initialize logger");
    console_error_panic_hook::set_once();
}
