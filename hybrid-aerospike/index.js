const { random } = require('faker');
const shuffle = require('shuffle-array');
const { DgraphClient, DgraphClientStub, Mutation, Operation } = require('dgraph-js');
const grpc = require('grpc');
const Aerospike = require('aerospike');
const pMap = require('p-map');
const { Observable } = require('rxjs');
const Ops = require('rxjs/operators');

const PARALLELISM = 300;

async function getAerospikeClient() {
  return Aerospike.connect({
    hosts: 'localhost:3000',
    maxConnsPerNode: PARALLELISM,
  });
}

async function clearAerospikeTable(client) {
  return client.truncate('treelab', 'test_table', 0);
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

async function fillTable(aerospikeClient, tableSkeleton) {
  const policy = new Aerospike.WritePolicy({
    exists: Aerospike.policy.exists.CREATE_OR_REPLACE,
  });

  const keys = tableSkeleton.rowUids.map(rowUid => new Aerospike.Key('treelab', tableSkeleton.tableUid, rowUid));

  const mapper = async (key) => {
    const bins = {};
    for (let col = 0; col < 100; col++) {
      // if (Math.random() > 0.5) {
        Object.assign(bins, { [tableSkeleton.columnUids[col]]: { text: random.alphaNumeric(20) } });
      // }
    }

    // if (Math.random > 0.5) {
      return aerospikeClient.put(key, bins, {}, policy);
    // }
  };

  return pMap(keys, mapper, { concurrency: PARALLELISM });
}

(async function() {
  const aerospikeClient = await getAerospikeClient();
  const dgraphClient = getDgraphClient();

  console.time('clear db');
  await clearAerospikeTable(aerospikeClient);
  await clearDgraph(dgraphClient);
  console.timeEnd('clear db');

  const tableSkeleton = await generateTableStructure(dgraphClient);
  console.time('fill in table cells');
  await fillTable(aerospikeClient, tableSkeleton);
  console.timeEnd('fill in table cells');

  // console.log(tableSkeleton);
  aerospikeClient.close();
})();