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

(async function() {
  const client = getDgraphClient();
  console.log(client);
  console.log('clear dgraph');
  const op = new Operation();
  op.setDropAll(true);
  await client.alter(op);

  console.log('generating table');
  console.time('dgraph create table');

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

  console.timeEnd('dgraph create table');
})();
