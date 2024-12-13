// tonbo = import("./pkg/sqlite_tonbo.js")


async function initWorker() {
    const tonbo = await import("./pkg/sqlite_tonbo.js");
    await tonbo.default();

    let conn = tonbo.Connection.open();
    await conn.create(
            "CREATE VIRTUAL TABLE temp.tonbo USING tonbo(" +
            "    create_sql='create table tonbo(id bigint primary key, name varchar, like int)'," +
            "    path='db_path/tetst'" +
        ");"
    ).then(async ()  => {
        for (let i = 0; i < 10000; i ++) {
            if (i % 100 === 0) {
                await conn.flush("tonbo");
            }
            await conn.insert(
                `INSERT INTO tonbo (id, name, like) VALUES (${i}, 'lol', ${i})`
            );
        }
    }).finally(async () => {
        const rows = await conn.select("SELECT * FROM tonbo limit 10");
        console.log(rows)
    });

}



initWorker()

