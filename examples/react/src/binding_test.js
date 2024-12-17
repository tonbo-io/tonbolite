
const worker = new Worker(new URL('./worker.js', import.meta.url));

worker.onmessage = function (event) {
    console.log(event.data);
}


worker.postMessage({type: 'select', sql: 'SELECT * FROM tonbo limit 10'});
worker.postMessage({type: 'select', sql: 'SELECT * FROM tonbo limit 5'});
