async function initWorker() {
    const tonbo = await import("./pkg/sqlite_tonbo.js");
    await tonbo.default();

    let conn = new tonbo.Connection();

    await conn.create(`CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
      table_name ='tonbo',
      addr = 'http://localhost:50051',
    );`);

    for (let i = 0; i < 10; i ++) {
        if (i % 100 === 0) {
            await conn.flush("tonbo");
        }
        await conn.insert(
            `INSERT INTO tonbo (id, name, like) VALUES (${i}, 'lol', ${i})`
        );
    }
    const rows = await conn.select("SELECT * FROM tonbo limit 10");
    console.log(rows)

}



initWorker()
