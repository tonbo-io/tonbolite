
# TonboLite

A SQLite extension that use [Tonbo](https://github.com/tonbo-io/tonbo) as storage backend. Leveraging Tonbo, TonboLite has these features:

- Suitable for analysis scenarios by using parquet format
- Scalable shared storage by using S3 
- Working on Wasm

## Usage

You can use TonboLite just like a regular SQLite program, except that you need to use `CREATE VIRTUAL TABLE` to create tables.

```rust
#[tokio::main]
async fn main() -> rusqlite::Result<()>  {
    let db = Connection::open_in_memory()?;
    // load TonboLite
    load_module(&db)?;

    db.execute_batch(
        "CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
                create_sql = 'create table tonbo(id bigint primary key, name varchar, like int)',
        );",
    )?;
    db.execute(
        &format!("INSERT INTO tonbo (id, name, like) VALUES (0, 'lol', 1)"),
        [],
    )?;
    let mut stmt = db.prepare("SELECT * FROM tonbo;")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        println!("{:#?}", row);
    }
}
```


### Using in Wasm

To use TonboLite in wasm, you should build it on wasm32-unknown-unknown target first. You can use the following steps to build a wasm target.

```sh
# add wasm32-unknown-unknown target
rustup target add wasm32-unknown-unknown
# use nightly
rustup component add rust-src --toolchain nightly
# build TonboLite
wasm-pack build --target web --no-default-features --features wasm
```

Once you build successfully, you will get a *pkg* folder containing compiled js and wasm files. Copy it to your project and you can start to use it.

```js
const tonbo = await import("./pkg/sqlite_tonbo.js");
await tonbo.default();
  
const conn = new tonbo.Connection();

await conn.create(
`CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
    create_sql = 'create table tonbo(id bigint primary key, name varchar, like int)',
);`
);

await conn.insert(
  `INSERT INTO tonbo (id, name, like) VALUES (0, 'lol', 0)`
);
await conn.update("UPDATE tonbo SET name = 'bar' WHERE id = 0");
await conn.delete("DELETE from tonbo WHERE id = 0");

const rows = await conn.select("SELECT * from tonbo");
```

#### Notes on Wasm limitations
- In order to use `SharedArrayBuffer`, you should use it in a [secure context](https://developer.mozilla.org/en-US/docs/Web/Security/Secure_Contexts) and [cross-origin isolated](https://developer.mozilla.org/en-US/docs/Web/API/Window/crossOriginIsolated).
- You should use it in web worker or async function.
