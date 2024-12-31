mod executor;

#[cfg(feature = "wasm")]
mod connection;
#[cfg(feature = "wasm")]
pub use connection::*;

mod db;

mod utils;
use crate::db::{DbState, TonboTable};
use rusqlite::{vtab::update_module, Connection, Result};
use std::sync::Arc;

pub fn load_module(conn: &Connection) -> Result<()> {
    let aux = Some(Arc::new(DbState::new()));
    conn.create_module("tonbo", update_module::<TonboTable>(), aux)
}

#[cfg(feature = "wasm")]
#[wasm_bindgen::prelude::wasm_bindgen(start)]
pub fn startup() {
    console_log::init().expect("could not initialize logger");
    console_error_panic_hook::set_once();
}
