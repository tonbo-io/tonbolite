let conn;

(async () => {
  const tonbo = await import("./pkg/sqlite_tonbo.js");
  await tonbo.default();

  conn = new tonbo.Connection();

  await conn.create(`CREATE VIRTUAL TABLE temp.tonbo USING tonbo(
      create_sql ='create table tonbo(id bigint primary key, name varchar, like int)',
    );`);

  for (let i = 0; i < 10; i++) {
    await conn.insert(
      `INSERT INTO tonbo (id, name, like) VALUES (${i}, 'lol', ${i})`
    );
  }

  const rows = await conn.select("SELECT * FROM tonbo limit 10");
  console.log(rows);

})();

onmessage = function (event) {
  const sql = event.data.sql;
  const type = event.data.type;
  switch (type) {
    case "select":
      conn.select(sql).then((res) => {
        this.postMessage({ type: type, data: res });
      });
      break;
    case "insert":
      conn.insert(sql).then((res) => {
        this.postMessage({ type: type, data: res });
      });
      break;
    case "delete":
      conn.delete(sql).then((res) => {
        this.postMessage({ type: type, data: res });
      });
      break;
    case "update":
      conn.update(sql).then((res) => {
        this.postMessage({ type: type, data: res });
      });
      break;
    case "flush":
      const table = event.data.table;
      conn.flush(table).then(() => {
        this.postMessage({ type: type });
      });
      break;
    default:
      break;
  }
};
