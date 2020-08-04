const Aerospike = require('aerospike');
const { random } = require('faker');
const { Observable } = require('rxjs');
const Ops = require('rxjs/operators');

async function getAerospikeClient() {
  const config = { hosts: 'localhost:3000' };
  return Aerospike.connect(config);
}

async function clearAerospikeTable(client) {
  return client.truncate('treelab', 'test_table', 0);
}

async function generateTable(client) {
  const policy = new Aerospike.WritePolicy({
    exists: Aerospike.policy.exists.CREATE_OR_REPLACE,
  });

  for (let row = 0; row < 10000; row++) {
    const bins = {};
    for (let col = 0; col < 100; col++) {
      Object.assign(bins, { [`col_${col}`]: random.alphaNumeric(20) });
    }
    const key = new Aerospike.Key('treelab', 'test_table', `row_${row}`);

    // console.log('pushing row', row);
    await client.put(key, bins, {}, policy);
  }
}

async function fetchEntireTable(client) {
  return new Observable(subscriber => {
    const scan = client.scan('treelab', 'test_table');
    scan.priority = Aerospike.scanPriority.AUTO;
    scan.percent = 100;

    const stream = scan.foreach();
    stream.on('error', (err) => subscriber.error(err));
    stream.on('end', () => subscriber.complete());
    stream.on('data', (record) => subscriber.next(record));
  }).pipe(
    Ops.reduce((table, record) => {
      return table.concat([record]);
    }, []),
  ).toPromise();
}

(async function() {
  const client = await getAerospikeClient();

  console.log('clear table');
  await clearAerospikeTable(client);

  // table generate
  console.log('generating table');
  console.time('generate table');
  await generateTable(client);
  console.timeEnd('generate table');

  // table read
  console.log('reading entire table');
  console.time('table scan');
  const table = await fetchEntireTable(client);
  console.timeEnd('table scan');
  console.log(table[0]);
  console.log(`scanned ${table.length}`);

  // update column


  client.close();
})();
