# Migration tool for Amazon Keyspaces
Project aims to help customers to migrate from Cassandra to Amazon Keyspaces with zero downtime. 
Customers can replicate the Cassandra the real-time workload in low seconds to Amazon Keyspaces with no code changes on client side.
    
Customers can scale migration throughput by spinning multiple CQLReplicator instances, CQLReplicator instance is 
responsible for a certain set of token cassandra ranges. If a CQLReplicator fails you can restart the migration process from where
it was interrupted by rebooting a failed CQLReplicator instance.

# Components
1. `Partition keys replicator` replicates partition keys into the ledger_v4 to track new or deleted partition keys
2. `Cassandra rows replicator` replicates Cassandra rows into the ledger_v4 and target table
3. `Statistics` reports replication statistics 
4. `External Memcached/Internal HashMap` stores partition keys, clustering columns, and write time for each Cassandra row.   
 
# Build this project
To build and use this tool executes the following mvn command and place on the classpath of your application. 
```
mvn install package
```

# Quick start
Before running the tool, please pre-warm Amazon Keyspaces' tables. 
The tables should be provisioned with proper WCUs/RCUs to sustain the high workload.

## Run the following commands: 

1. Let's download cassandra image ```docker pull cassandra```
2. Deploy memcached
3. Configure config.properties
    ```# Target keyspace and table in Amazon Keyspaces
       TARGET_KEYSPACE: ks_test_cql_replicator
       TARGET_TABLE: test_cql_replicator
       
       # Source query that runs against Cassandra cluster per partition key
       SOURCE_CQL_QUERY: SELECT json key,col0,col1,col2,col3,col4,col5,col6,col7,col8,writetime(col2),writetime(col4) FROM ks_test_cql_replicator.test_cql_replicator WHERE key=:key
       WRITETIME_COLUMNS: col2, col4
       REPLICATE_REFRESH_PERIOD_SEC: 5
       REPLICATE_WITH_TIMESTAMP: false
       REGISTER_PARTITION_DELETES: false
       REPLICATE_RETRY_MAXATTEMPTS: 512
       
       # Refresh period of statistics
       STATS_REFRESH_PERIOD_SEC: 60
       
       # Use PartiQL statements to transform JSON Cassandra rows into a new Keyspaces' schema if need it, https://partiql.org/tutorial.html
       # inputDocument represents the source cassandra row in the PartiQL statement
       # For example, transform JSON key,col0,col1,col2,col3,col4,col5,col6,col7,col8 to JSON id, col0
       TRANSFORM_INBOUND_REQUEST: false
       TRANSFORM_SQL: SELECT \"key\" as \"id\", \"col0\" as \"new_col\" FROM inputDocument
       TRANSFORM_PARTITION_KEY: id
       
       # Rate limiting CQLReplicator
       RATELIMITER_PERMITS: 1500
       RATELIMITER_TIMEOUT_MS: 1000
       
       INTERNAL_STORAGE_INIT_CAPACITY: 687500
       INTERNAL_STORAGE_LOAD_FACTOR: 0.75f
       INTERNAL_STORAGE_CONCURRENCY_LEVEL: 2
       
       EXTERNAL_MEMCACHED_STORAGE: true
       EXTERNAL_MEMCACHED_STORAGE_ENDPOINT: localhost
       EXTERNAL_MEMCACHED_STORAGE_PORT: 11211
4. Configure connection/authentication for your source Cassandra in CassandraConnector.conf
5. Configure connection/authentication for your target Amazon Keyspaces in KeyspacesConnector.conf
6. Copy config.properties, CassandraConnector.conf, and KeyspacesConnector.conf to Amazon S3, for example, 
   copy the files to ```s3://cqlreplicator/ks_test_cql_replicator/test_cql_replicator```
7. Run ```unzip CQLReplicator-1.0-SNAPSHOT.zip```
8. Set environmental variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `BUCKETNAME=cqlreplicator`, `KEYSPACENAME=ks_test_cql_replicator`, `TABLENAME=test_cql_replicator`, and `CQLREPLICATOR_CONF`
9. Run ```java -cp CQLReplicator-1.0-SNAPSHOT.jar com.amazon.aws.cqlreplicator.Init``` to create Ledger and test table in Amazon Keyspaces
10. Run one cassandra instance
   ```docker run --name test-cql-replicator -d -e CASSANDRA_NUM_TOKENS=8 -p 9042:9042 cassandra:3.11```
11. Generate cassandra workload
   `cassandra_ip=$(ifconfig en0 | grep inet | awk '{print $2}')
    cassandra-stress user profile=test/integration/cql_replicator_stress_test.yaml duration=1s no-warmup ops\(insert=1\) -rate threads=2 -node $cassandra_ip`
12. Run CQLReplicator with two tiles
   ```cd "$(dirname "$CQLREPLICATOR_CONF")/bin"```
   ```./cqlreplicator.sh --syncPartitionKeys --tile 0 --tiles 2 & ./cqlreplicator.sh --syncPartitionKeys --tile 1 --tiles 2 & ./cqlreplicator.sh --syncClusteringColumns --tile 0 --tiles 2 & ./cqlreplicator.sh --syncClusteringColumns --tile 1 --tiles 2 &```
12. Run ```select * from replicator.stats``` against Amazon Keyspaces to monitor the migration process, or you can run ```./cqlreplicator.sh --tile 0 --tiles 2 --stats ``` from your terminal

# Repair
To mitigate the inconsistency between source and target you might run Repair operation in background on a separate instance. 
There two types of the repair operation:
    - repair a tile, for example, ```/cqlreplicator.sh --repair --tile 0 --tiles 2```
    - repair all tiles sequentially, for example, ```/cqlreplicator.sh --repair-all --tile 0 --tiles 2```    

# Clean up
Let's clean up the testing environment to avoid extra charges and resource consumption
1. to stop the docker container run the following command: 
    ```docker container stop test-cql-replicator```
2. to remove the docker container run the following command: 
    ```docker container rm test-cql-replicator```
3. stop CQLReplicator by running the following command: 
    ```kill $(jps -ml | grep com.amazon.aws.cqlreplicator.Starter | awk '{print $1}')```
    The CQLReplicator process will report:
     `12:07:10.336 [Thread-2] INFO  com.amazon.aws.cqlreplicator.Stopper - Stopping process is activated
      12:07:10.337 [Thread-2] INFO  com.amazon.aws.cqlreplicator.Stopper - Replication task is stopped: true` in the log.
4. to clean up Amazon Keyspaces tables execute the following commands in the cqlsh: 
    ```drop keyspace replicator``` and ```drop keyspace ks_test_cql_replicator```

# Appendix
Port-forwarding ```ssh -i migrationKeys.pem -f -N -L 11211:cqlreplicator.tpckwk.0001.use1.cache.amazonaws.com:11211 ec2-user@remote_host```
Run memcached locally ```/usr/local/opt/memcached/bin/memcached -l localhost```
Flush keys in memcached```echo 'flush_all' | nc localhost 11211```
Dumping all keys from memcached ```MEMCHOST=localhost; printf "stats items\n" | nc $MEMCHOST 11211 | grep ":number" | awk -F":" '{print $2}' | xargs -I % printf "stats cachedump % 10000\r\n" | nc $MEMCHOST 11211```
Dumping all keys and values from memcached ```MEMCHOST=localhost; printf "stats items\n" | nc $MEMCHOST 11211 | grep ":number" | awk -F":" '{print $2}' | xargs -I % printf "stats cachedump % 0\r\n" | nc $MEMCHOST 11211 | grep ITEM | awk '{print $2}' | sed -e 's/"/\\"/g'| xargs -I % printf "get %\r\n" | nc $MEMCHOST 11211```
```for i in {1..40}; do (echo "stats cachedump $i 0"; sleep 1; echo "quit";) | telnet localhost 11211 | grep 'APREFIX*\|ANOTHERPREFIX*'; done```

# License
This tool licensed under the Apache-2 License. See the LICENSE file.