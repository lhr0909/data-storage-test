const { random } = require('faker');
const { DgraphClient, DgraphClientStub, Mutation, Operation } = require('dgraph-js');
const grpc = require('grpc');

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

async function generateTable(client) {
  let txn = client.newTxn();

  let nquads = `
  _:table <dgraph.type> "Table" .
  `;

  for (let col = 0; col < 100; col++) {
    nquads += `
    _:col${col} <dgraph.type> "Column" .
    _:table <has_column> _:col${col} .
    `;
  }

  let mutation = new Mutation();
  mutation.setSetNquads(nquads);
  mutation.setCommitNow(true);
  let response = await txn.mutate(mutation);

  const uids = response.getUidsMap();
  const tableUid = uids.get('table');
  console.log('table uid', tableUid);
  const columnUids = [];
  for (let col = 0; col < 100; col++) {
    columnUids.push(uids.get(`col${col}`));
  }

  nquads = '';

  for (let row = 0; row < 10000; row++) {
    txn = client.newTxn();

    nquads += `
    _:row${row} <dgraph.type> "Row" .
    <${tableUid}> <has_row> _:row${row} .
    `;

    for (let col = 0; col < 100; col++) {
      const cellId = random.alphaNumeric(5);
      nquads += `
      _:${cellId} <dgraph.type> "Cell" .
      _:${cellId} <text> "${random.alphaNumeric(20)}" .
      _:row${row} <has_cell> _:${cellId} .
      <${columnUids[col]}> <has_cell> _:${cellId} .
      `;
    }

    if ((row + 1) % 100 === 0) {
      console.log('row', row + 1);
      mutation = new Mutation();
      mutation.setSetNquads(nquads);
      mutation.setCommitNow(true);
      await txn.mutate(mutation);
      nquads = '';
    }
  }

  return tableUid;
}

(async function() {
  const client = getDgraphClient();

  console.log('clear dgraph');
  await clearDgraph(client);

  console.log('generating table');
  console.time('dgraph create table');
  const tableUid = await generateTable(client);
  console.timeEnd('dgraph create table');

  console.log('dgraph read entire table');
  console.time('dgraph read table');
  txn = client.newTxn({ readOnly: true });
  const query = `{
    table(func: uid(${tableUid})) {
      uid
      has_column {
        uid
      }
      has_row {
        uid
      }
    }
  }`;
  response = await txn.query(query);
  console.timeEnd('dgraph read table');
  const json = response.getJson();
  console.log(json);
  console.log(`fetched ${json.table[0].has_row.length} rows`);
})();
