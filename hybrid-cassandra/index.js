const { random } = require('faker');
const shuffle = require('shuffle-array');
const { DgraphClient, DgraphClientStub, Mutation, Operation } = require('dgraph-js');
const grpc = require('grpc');
const cassandra = require('cassandra-driver');
const { from, Observable } = require('rxjs');
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

async function clearCassandra(client) {
  await client.execute('DROP KEYSPACE IF EXISTS treelab;');
  await client.execute(`CREATE KEYSPACE IF NOT EXISTS treelab WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };`);
  await client.execute(`
  CREATE TYPE treelab.text_cell (
    text text,
  );
  `);
}

function getDgraphClient() {
  const stub = new DgraphClientStub('localhost:19080', grpc.credentials.createInsecure(), {
    'grpc.max_receive_message_length': -1, // unlimited
    'grpc.max_send_message_length': -1, // unlimited
  });
  const client = new DgraphClient(stub);
  // client.setDebugMode(true);
  return client;
}

async function clearDgraph(client) {
  const op = new Operation();
  op.setDropAll(true);
  return client.alter(op);
}

async function generateTableStructure(dGraphClient) {
  const result = {};

  let txn = dGraphClient.newTxn();

  let nquads = `
  _:table <dgraph.type> "Table" .
  `;

  for (let col = 0; col < 100; col++) {
    nquads += `
    _:col${col} <dgraph.type> "Column" .
    _:table <has_column> _:col${col} .
    `;
  }

  for (let row = 0; row < 10000; row++) {
    nquads += `
    _:row${row} <dgraph.type> "Row" .
    _:table <has_row> _:row${row} .
    `;
  }

  let mutation = new Mutation();
  mutation.setSetNquads(nquads);
  mutation.setCommitNow(true);

  console.time('create table graph');
  let response = await txn.mutate(mutation);
  console.timeEnd('create table graph');

  const uids = response.getUidsMap();
  result.tableUid = uids.get('table');
  console.log('table uid', result.tableUid);

  result.columnUids = [];
  for (let col = 0; col < 100; col++) {
    result.columnUids.push(uids.get(`col${col}`));
  }

  result.rowUids = [];
  for (let row = 0; row < 10000; row++) {
    result.rowUids.push(uids.get(`row${row}`));
  }

  // create a view with shuffled orders
  txn = dGraphClient.newTxn();
  nquads = `
  _:view <dgraph.type> "View" .
  <${result.tableUid}> <has_view> _:view .
  `;

  const columnOrders = shuffle(Array.from(Array(100).keys()));
  const rowOrders = shuffle(Array.from(Array(10000).keys()));

  for (let col = 0; col < 100; col++) {
    nquads += `
    _:view <has_column> <${result.columnUids[col]}> (order=${columnOrders[col]}) .
    `;
  }

  for (let row = 0; row < 10000; row++) {
    nquads += `
    _:view <has_row> <${result.rowUids[row]}> (order=${rowOrders[row]}) .
    `;
  }

  mutation = new Mutation();
  mutation.setSetNquads(nquads);
  mutation.setCommitNow(true);

  console.time('generate shuffled view');
  response = await txn.mutate(mutation);
  console.timeEnd('generate shuffled view');

  result.viewUid = response.getUidsMap().get('view');
  console.log('view uid', result.viewUid);

  return result;
}

async function fillTable(cassandraClient, tableSkeleton) {
  let query = `
  CREATE TABLE treelab.table_${tableSkeleton.tableUid} (
    row_id text PRIMARY KEY,
    ${tableSkeleton.columnUids.map(columnUid => `col_${columnUid} text_cell,`).join('\n')}
  );
  `;

  await cassandraClient.execute(query);

  query = `
  INSERT INTO treelab.table_${tableSkeleton.tableUid} (row_id,${tableSkeleton.columnUids.map(columnUid => `col_${columnUid}`).join(',')}) VALUES (?,${tableSkeleton.columnUids.map(() => '?').join(',')})
  `;

  const queries = tableSkeleton.rowUids.map(rowUid => ({
    query,
    params: [rowUid].concat(tableSkeleton.columnUids.map(() => ({ text: random.alphaNumeric(20) }))),
  }));

  return from(queries).pipe(
    Ops.bufferCount(10),
    Ops.concatMap((batch) => {
      return from(cassandraClient.batch(batch, { prepare: true }));
    }),
    Ops.reduce((results, result) => {
      results.push(result);
      return results;
    }, []),
  ).toPromise();
}

async function readTable(dGraphClient, cassandraClient, tableUid) {
  const txn = dGraphClient.newTxn({ readOnly: true });
  const query = `{
    table(func: uid(${tableUid})) {
      uid
      columns: has_column {
        uid
      }
      rows: has_row {
        uid
      }
    }
  }`;
  console.time('dgraph read table skeleton');
  const response = await txn.query(query);
  console.timeEnd('dgraph read table skeleton');

  const responseJson = response.getJson();
  const { rows } = responseJson.table[0];

  console.time('cassandra batch read table data');
  const batchResult = await new Observable(subscriber => {
    const query = `SELECT * from treelab.table_${tableUid} WHERE row_id IN (${rows.map(() => '?').join(',')});`;
    const stream = cassandraClient.stream(query, rows.map(row => row.uid), { prepare: true });

    stream.on('error', (err) => subscriber.error(err));
    stream.on('end', () => subscriber.complete());
    stream.on('readable', () => {
      let row;

      while (row = stream.read()) {
        subscriber.next(row);
      }
    });
  }).pipe(
    Ops.reduce((result, row) => {
      const uid = row.row_id;
      const cells = {};

      row.keys().forEach(key => {
        if (key === 'row_id') {
          return;
        }

        cells[key.slice(4)] = row.get(key);
      });

      result.push({
        uid,
        cells,
      });

      return result;
    }, []),
  ).toPromise();
  console.timeEnd('cassandra batch read table data');

  responseJson.table[0].rows = batchResult;
  return responseJson.table[0];
}

(async function() {
  const cassandraClient = await getCassandraClient();
  const dgraphClient = getDgraphClient();

  console.time('clear db');
  await clearCassandra(cassandraClient);
  await clearDgraph(dgraphClient);
  console.timeEnd('clear db');

  const tableSkeleton = await generateTableStructure(dgraphClient);
  console.time('fill in table cells');
  await fillTable(cassandraClient, tableSkeleton);
  console.timeEnd('fill in table cells');

  // console.log(tableSkeleton);
  const table = await readTable(dgraphClient, cassandraClient, tableSkeleton.tableUid);

  // console.log(table);
  console.log(table.rows[0]);
  console.log('total cells', table.rows.reduce((cellCount, row) => {
    return cellCount + Object.keys(row.cells).length;
  }, 0));

  await cassandraClient.shutdown();
})();