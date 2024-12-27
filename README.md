
# TonboLite

TonboLite is a WASM compatible SQLite extension that allows users to create tables which supports analytical processing directly in SQLite. Its storage engine is powered by our open-source embedded key-value database, [Tonbo](https://github.com/tonbo-io/tonbo).

## Features
- Organizing [Parquet](https://parquet.apache.org/) files using [Log-Structured Merge Tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree) for fast writing
- Supporting S3 as remote storage
- Compatibility with WebAssembly

## Usage

### Use in Rust
TonboLite can be used just like a regular SQLite program.

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
