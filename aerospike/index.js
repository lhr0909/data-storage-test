const Aerospike = require('aerospike');
const { random } = require('faker');

async function getAerospikeClient() {
  const config = { hosts: 'localhost:3000' };
  return await Aerospike.connect(config);
}

(async function() {
  const client = await getAerospikeClient();
  console.log('clear table');
  await client.truncate('test', 'test_table', 0);
  const policy = new Aerospike.WritePolicy({
    exists: Aerospike.policy.exists.CREATE_OR_REPLACE,
  });

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

  client.close();
})();
