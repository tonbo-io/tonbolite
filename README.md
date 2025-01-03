
# TonboLite

TonboLite is a WASM compatible SQLite extension that allows users to create tables which supports analytical processing directly in SQLite. Its storage engine is powered by our open-source embedded key-value database, [Tonbo](https://github.com/tonbo-io/tonbo).

## Features
- Organizing [Parquet](https://parquet.apache.org/) files using [Log-Structured Merge Tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree) for fast writing
- Supports OPFS, S3 as remote storage or mixed them together as storage back-end.
- Compatibility with WebAssembly

## Usage

### Use in SQLite CLI
You can use [`.load`](https://www.sqlite.org/cli.html#loading_extensions) command to load a SQLite extension
```bash
sqlite> .load target/release/libsqlite_tonbo

sqlite> CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
    create_sql = 'create table tonbo(id bigint primary key, name varchar, like int)',
    path = 'db_path/tonbo'
);
sqlite> insert into tonbo (id, name, like) values (0, 'tonbo', 100);
sqlite> insert into tonbo (id, name, like) values (1, 'sqlite', 200);

sqlite> select * from tonbo;
0|tonbo|100
1|sqlite|200

sqlite> update tonbo set like = 123 where id = 0;

sqlite> select * from tonbo;
0|tonbo|123
1|sqlite|200

sqlite> delete from tonbo where id = 0;

sqlite> select * from tonbo;
1|sqlite|200
```

Or you can use SQLite extension in Python:
```python
import sqlite3

conn = sqlite3.connect(":memory")
conn.enable_load_extension(True)
# Load the tonbolite extension
conn.load_extension("target/release/libsqlite_tonbo.dylib")
con.enable_load_extension(False)

conn.execute("CREATE VIRTUAL TABLE temp.tonbo USING tonbo("
                "create_sql = 'create table tonbo(id bigint primary key, name varchar, like int)', "
                "path = 'db_path/tonbo'"
             ")")
conn.execute("INSERT INTO tonbo (id, name, like) VALUES (0, 'lol', 1)")
conn.execute("INSERT INTO tonbo (id, name, like) VALUES (1, 'lol', 100)")
rows = conn.execute("SELECT * FROM tonbo;")
for row in rows:
    print(row)
```

### Use in Rust
TonboLite can be used just like a regular SQLite program.
> Please use our Rusqlite patch
> ```toml
> [patch.crates-io.rusqlite]
> git = "https://github.com/tonbo-io/rusqlite"
> branch = "feat/integrity"
> ```
```rust
#[tokio::main]
async fn main() -> rusqlite::Result<()>  {
    let db = Connection::open_in_memory()?;
    // load TonboLite
    load_module(&db)?;

    // use TonboLite like normal SQLite
    db.execute_batch(
        "CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
            create_sql = 'create table tonbo(id bigint primary key, name varchar, like int)'
            path = 'db_path/tonbo'
        );",
    )?;
    db.execute("INSERT INTO tonbo (id, name, like) VALUES (0, 'lol', 1)", [])?;

    // query from table
    let mut stmt = db.prepare("SELECT * FROM tonbo;")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        println!("{:#?}", row);
    }
}
```


### Use in Wasm

TonboLiet exposed an easy-to-use API

```js
const conn = new tonbo.Connection();

// create table with `CREATE VIRTUAL TABLE` statement
await conn.create(
    `CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
        create_sql = 'create table tonbo(id bigint primary key, name varchar, like int)',
        path = 'db_path/tonbo'
    );`
);

// insert/update/delete table
await conn.insert(
  `INSERT INTO tonbo (id, name, like) VALUES (0, 'lol', 0)`
);
await conn.update("UPDATE tonbo SET name = 'bar' WHERE id = 0");
await conn.delete("DELETE from tonbo WHERE id = 0");

// query from table
const rows = await conn.select("SELECT * from tonbo");

// fulsh in-memory data to S3
await conn.flush("tonbo");
```


## Build

### Build as Extension
```sh
cargo build --release --features loadable_extension
```
Once building successfully, you will get a file named libsqlite_tonbo.dylib(`.dll` on windows, `.so` on most other unixes) in *target/release/*
### Build on Rust

```sh
cargo build
```

### Build on Wasm

To use TonboLite in wasm, it takes a few steps to build.
1. Add wasm32-unknown-unknown target
```sh
rustup target add wasm32-unknown-unknown
```
2. Override toolchain with nightly
```sh
rustup override set nightly
```
3. Build with [wasm-pack](https://github.com/rustwasm/wasm-pack)
```sh
wasm-pack build --target web --no-default-features --features wasm
```

Once you build successfully, you will get a *pkg* folder containing compiled js and wasm files. Copy it to your project and then you can start to use it.
```js
const tonbo = await import("./pkg/sqlite_tonbo.js");
await tonbo.default();

// start to use TonboLite ...
```

#### Limitation
TonboLite should be used in a [secure context](https://developer.mozilla.org/en-US/docs/Web/Security/Secure_Contexts) and [cross-origin isolated](https://developer.mozilla.org/en-US/docs/Web/API/Window/crossOriginIsolated), since it uses [`SharedArrayBuffer`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/SharedArrayBuffer) to share memory. Please refer to [this article](https://web.dev/articles/coop-coep) for a detailed explanation.
