let conn;

(async () => {
  const tonbo = await import("./pkg/sqlite_tonbo.js");
  await tonbo.default();

  conn = new tonbo.Connection();

  await conn.create(`CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
      create_sql ='create table tonbo(id bigint primary key, name varchar, like int)',
      path = 'db_path/tonbo'
    );`);

  for (let i = 0; i < 10; i++) {
    await conn.insert(
      `INSERT INTO tonbo (id, name, like) VALUES (${i}, 'lol', ${i})`
    );
  }

  await conn.delete("DELETE FROM tonbo WHERE id = 4");
  await conn.update("UPDATE tonbo SET name = 'tonbo' WHERE id = 6");

  const rows = await conn.select("SELECT * FROM tonbo limit 10");
  console.log(rows);

})();


