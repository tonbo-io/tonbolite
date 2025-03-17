mod executor;

#[cfg(feature = "wasm")]
mod connection;
#[cfg(feature = "wasm")]
pub use connection::*;

mod db;

mod utils;
use crate::db::{DbState, TonboTable};
#[cfg(feature = "loadable_extension")]
use rusqlite::ffi;
use rusqlite::{vtab::update_module, Connection, Result};
#[cfg(feature = "loadable_extension")]
use std::os::raw::{c_char, c_int};
use std::sync::Arc;

#[no_mangle]
#[cfg(feature = "loadable_extension")]
pub unsafe extern "C" fn sqlite3_sqlitetonbo_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    Connection::extension_init2(db, pz_err_msg, p_api, extension_init)
}

#[cfg(feature = "loadable_extension")]
fn extension_init(db: Connection) -> Result<bool> {
    let aux = Some(Arc::new(DbState::new()));
    db.create_module("tonbo", update_module::<TonboTable>(), aux)?;

    Ok(true)
}

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
