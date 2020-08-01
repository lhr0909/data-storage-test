# Benchmark between Data Storages

This is a benchmark repository for various data storages to store table data.

## Hardware Spec

The benchmark report is generated on a MacBook Pro 2019 16-inch machine with 2.4GHz 8-core Core i9 and 32GB of RAM. Data storage services (Aerospike, Cassandra, Elasticsearch and dGraph) are deployed using Docker, all with single instance. Docker is allocated with half of the computer's resources.

## Tests

### Create Table Test

I try my best to model a minimal table structure on every data storage, where every cell is a text cell. The test will generate a table with 10k rows x 100 columns, which is a total of 1 million cells. Each cell will store a text string of length 20.

Here are the results:

Aerospike: 12.583s (no batching)
Cassandra: 18.245s (no batching), 6.561s (batches of 10 rows)
dGraph: 131.058s (batches of 100 rows), 305.051s (batches of 1000 rows)