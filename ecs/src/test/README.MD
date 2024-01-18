### Run one cassandra instance

```bash
  docker run --name test-cql-replicator -d -e CASSANDRA_NUM_TOKENS=8 -p 9042:9042 cassandra:3.11
```

### Generate random cassandra workload

```bash
   cassandra-stress user profile=test/integration/cql_replicator_stress_test.yaml duration=1s no-warmup ops\(insert=1\) -rate threads=2 -node $cassandra_ip
```