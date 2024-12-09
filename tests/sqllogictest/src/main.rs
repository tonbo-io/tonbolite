use clap::Parser;
use rusqlite::types::ValueRef;
use rusqlite::Connection;
use sqlite_tonbo::load_module;
use sqllogictest::{DBOutput, DefaultColumnType, Runner, DB};
use std::fs::create_dir_all;
use std::path::Path;
use tempfile::TempDir;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value = "tests/test_files/**/*.test")]
    path: String,
}

fn main() {
    let args = Args::parse();

    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    std::env::set_current_dir(path.clone()).unwrap();

    for slt_file in glob::glob(&args.path).expect("failed to find slt files") {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        std::env::set_current_dir(temp_dir.path()).unwrap();

        create_dir_all("./tonbo_test").unwrap();
        // for select 4
        for i in 1..10 {
            create_dir_all(format!("./tonbo_test/t{i}",)).unwrap();
        }

        let filepath = slt_file.expect("failed to read test file");
        let mut tester = Runner::new(
            SQLBase::new(Connection::open_in_memory().unwrap()).expect("init db error"),
        );
        tester.with_hash_threshold(8);

        if let Err(err) = tester.run_file(path.join(&filepath)) {
            panic!("test error: {}", err);
        }
        println!("- '{}' Pass", filepath.display());
    }
}

struct SQLBase {
    db: Connection,
}

impl SQLBase {
    fn new(db: Connection) -> rusqlite::Result<Self> {
        load_module(&db)?;

        Ok(Self { db })
    }
}

impl DB for SQLBase {
    type Error = rusqlite::Error;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let mut stmt = self.db.prepare(sql)?;
        let column_count = stmt.column_count();
        let mut rows = stmt.query([])?;

        let mut result_rows = Vec::new();
        while let Some(row) = rows.next()? {
            let mut result_row = Vec::with_capacity(column_count);

            for i in 0..column_count {
                result_row.push(value_display(&row.get_ref_unwrap(i)));
            }
            result_rows.push(result_row);
        }

        Ok(DBOutput::Rows {
            types: vec![DefaultColumnType::Any; column_count],
            rows: result_rows,
        })
    }
}

fn value_display(value: &ValueRef<'_>) -> String {
    match value {
        ValueRef::Null => "NULL".to_string(),
        ValueRef::Integer(val) => val.to_string(),
        ValueRef::Real(val) => val.to_string(),
        ValueRef::Text(val) => String::from_utf8(val.to_vec()).unwrap(),
        ValueRef::Blob(val) => format!("{:?}", val),
    }
}
