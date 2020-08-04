const cassandra = require('cassandra-driver');
const { random } = require('faker');
const { Observable } = require('rxjs');
const Ops = require('rxjs/operators');

function getCassandraClient() {
  const client = new cassandra.Client({
    contactPoints: ['localhost'],
    localDataCenter: 'datacenter1',
    credentials: {
      username: 'cassandra',
      password: 'cassandra',
    },
    queryOptions: { consistency: cassandra.types.consistencies.one },
  });

  return client;
}

async function createTableColumns(client) {
  const columns = [];
  for (let i = 0; i < 100; i++) {
    columns.push(i);
  }

  const query = `
  CREATE TABLE treelab.test_table (
    row_id text PRIMARY KEY,
    ${columns.map(n => `col_${n} text,`).join('\n')}
  );
  `;

  // console.log(query);

  return client.execute(query);
}

async function fetchEntireTable(client) {
  return new Observable(subscriber => {
    const query = `
    SELECT * from treelab.test_table;
    `;

    const stream = client.stream(query, { prepare: true });

    stream.on('error', (err) => subscriber.error(err));
    stream.on('end', () => subscriber.complete());
    stream.on('readable', () => {
      let row;

      while (row = stream.read()) {
        subscriber.next(row);
      }
    });
  }).pipe(
    Ops.reduce((table, record) => {
      return table.concat([record]);
    }, []),
  ).toPromise();
}

async function batchInsertTable(client) {
  const columns = [];
  for (let col = 0; col < 100; col++) {
    columns.push(col);
  }

  const query = `
  INSERT INTO treelab.test_table (row_id,${columns.map(n => `col_${n}`).join(',')}) VALUES (?,${columns.map(n => '?').join(',')})
  `;

  // console.log(query);

  let queries = [];

  for (let row = 0; row < 10000; row++) {
    // await client.execute(query, [random.alphaNumeric(10)].concat(columns.map(n => random.alphaNumeric(20))), { prepare: true });
    queries.push({ query, params: [`row_${row}`].concat(columns.map(n => random.alphaNumeric(20))) });
    if ((row + 1) % 10 === 0) {
      await client.batch(queries, { prepare: true });
      queries = [];
    }
  }
}

(async function() {
  const client = getCassandraClient();
  // console.log(client);

  console.log('setting up keyspace');
  await client.execute(`CREATE KEYSPACE IF NOT EXISTS treelab WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };`);
  console.log('dropping table');
  await client.execute(`DROP TABLE treelab.test_table;`);

  console.log('generating table');
  console.time('create cassandra table');
  await createTableColumns(client);
  await batchInsertTable(client);
  console.timeEnd('create cassandra table');

  console.log('reading entire table');
  console.time('table scan');
  const rows = await fetchEntireTable(client);
  console.timeEnd('table scan');
  console.log(rows[0]);
  console.log(`scanned ${rows.length} rows`);

  await client.shutdown();
})();
