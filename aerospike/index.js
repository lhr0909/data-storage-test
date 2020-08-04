const Aerospike = require('aerospike');
const { random } = require('faker');
const { Observable } = require('rxjs');
const Ops = require('rxjs/operators');

(async function() {
  const config = { hosts: 'localhost:3000' };
  const client = await Aerospike.connect(config);

  console.log('clear table');
  await client.truncate('test', 'test_table', 0);
  const policy = new Aerospike.WritePolicy({
    exists: Aerospike.policy.exists.CREATE_OR_REPLACE,
  });

  // table generate

  console.log('generating table');
  console.time('generate table');
  for (let row = 0; row < 10000; row++) {
    const bins = {};
    for (let col = 0; col < 100; col++) {
      Object.assign(bins, { [`col_${col}`]: random.alphaNumeric(20) });
    }
    const key = new Aerospike.Key('test', 'test_table', `row_${row}`);

    // console.log('pushing row', row);
    await client.put(key, bins, {}, policy);
  }
  console.timeEnd('generate table');

  // table read

  console.log('reading entire table');
  console.time('table scan');
  const table = new Observable(subscriber => {
    const scan = client.scan('test', 'test_table');
    scan.priority = Aerospike.scanPriority.AUTO;
    scan.percent = 100;

    const stream = scan.foreach();
    stream.on('error', (err) => subscriber.error(err));
    stream.on('end', () => {
      client.close();
      subscriber.complete();
    });
    stream.on('data', (record) => subscriber.next(record));
  }).pipe(
    Ops.reduce((table, record) => {
      return table.concat([record]);
    }, []),
  ).subscribe((table) => {
    console.timeEnd('table scan');
    console.log(`scanned ${table.length} rows`);
  });
})();
