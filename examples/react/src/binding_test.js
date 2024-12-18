
const worker = new Worker(new URL('./worker.js', import.meta.url));

worker.onmessage = function (event) {
    console.log(event.data);
}


worker.postMessage({type: 'select', sql: 'SELECT * FROM tonbo limit 10'});
worker.postMessage({type: 'select', sql: 'SELECT * FROM tonbo limit 5'});
worker.postMessage({type: 'delete', sql: 'DELETE from tonbo WHERE id = 0'})
worker.postMessage({type: 'select', sql: 'SELECT * FROM tonbo limit 10'});

worker.postMessage({type: 'update', sql: 'UPDATE tonbo SET name = "tonbo" WHERE id = 2'})
worker.postMessage({type: 'select', sql: 'SELECT * FROM tonbo limit 10'});
