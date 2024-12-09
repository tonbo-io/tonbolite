mod executor;

#[cfg(feature = "wasm")]
mod connection;
#[cfg(feature = "wasm")]
pub use connection::*;
#[cfg(feature = "wasm")]
mod wasm;

#[cfg(not(feature = "wasm"))]
mod tokio;

mod utils;
use rusqlite::vtab::update_module;
use std::fs;
use std::sync::Arc;

#[cfg(not(feature = "wasm"))]
use crate::tokio::{DbState, TonboTable};
#[cfg(feature = "wasm")]
use crate::wasm::{DbState, TonboTable};

pub fn load_module(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    let _ = fs::create_dir_all("./db_path/tonbo");

    let aux = Some(Arc::new(DbState::new()));
    conn.create_module("tonbo", update_module::<TonboTable>(), aux)
}

#[cfg(feature = "wasm")]
#[wasm_bindgen::prelude::wasm_bindgen(start)]
pub fn startup() {
    console_log::init().expect("could not initialize logger");
}
