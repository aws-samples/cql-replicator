# Migration tool for Amazon Keyspaces
The project aims to help customers to migrate from the self-managed Cassandra to Amazon Keyspaces with zero downtime, for example, 
customers can replicate the Cassandra workload in low seconds to Amazon Keyspaces with no code changes on the client side.
    
Moreover, customers can scale migration workload by spinning more CQLReplicator instances, where a CQLReplicator instance is a java process that
responsible for a certain set of token ranges (partition keys/rows). If the CQLReplicator fails you can restart the migration process from where
it was interrupted by rebooting a failed CQLReplicator instance.

# Processes 
There are 3 main processes: 
   * `PartitionDiscovery` discovers new partition keys in the Cassandra cluster.
   * `RowDiscovery` discovers and replicates rows within the discovered partitions in the Cassandra cluster.
   * `Stats` reports migration statistics. 
 
# Build this project
To build and use this tool executes the following gradle command and place on the classpath of your application. 
```
gradle build
gradle task deploy
```

# Quick start

## Run the following commands locally, or on the EC2 instance:

The below steps only for testing purposes, for production please use [Amazon ECS](ecs/README.md) or Amazon EKS.   

* Let's download cassandra image ```docker pull cassandra```.
* Working with memcached: 
    1. If you use ElasticCache (memcached) and run CQLReplicator locally ``` ssh -i privatekey.pem -f -N -L 11211:cqlreplicator.tpckwk.0001.use1.cache.amazonaws.com:11211 ec2-user@remote_host``` (optional)
    2. If want to use local memcached ``` memcached -l localhost ```
    3. Flush keys in the memcached ``` echo 'flush_all' | nc localhost 11211 ```
    4. Dumping all keys from the memcached ``` MEMCHOST=localhost; printf "stats items\n" | nc $MEMCHOST 11211 | grep ":number" | awk -F":" '{print $2}'
     | xargs -I % printf "stats cachedump % 10000\r\n" | nc $MEMCHOST 11211```
    5. Dumping all values from the memcached ``` MEMCHOST=localhost; printf "stats items\n" | nc $MEMCHOST 11211 | grep ":number" | awk -F":" '{print $2}' | xargs -I % printf "stats cachedump % 0\r\n" | nc $MEMCHOST 11211 | grep ITEM | awk '{print $2}' | sed -e 's/"/\\"/g'| xargs -I % printf "get %\r\n" | nc $MEMCHOST 11211
                                                  for i in {1..40}; do (echo "stats cachedump $i 0"; sleep 1; echo "quit";) | telnet localhost 11211 | grep 'APREFIX*\|ANOTHERPREFIX*'; done ```
* Prepare the config.properties per one table
* Configure connection/authentication for your source Cassandra in CassandraConnector.conf
* Configure connection/authentication for your target Amazon Keyspaces in KeyspacesConnector.conf
* Copy config.properties, CassandraConnector.conf, and KeyspacesConnector.conf to Amazon S3, for example, 
   copy the files to ```s3://cqlreplicator/ks_test_cql_replicator/test_cql_replicator```
* Run ```unzip CQLReplicator-1.0-SNAPSHOT.zip```
* Set environmental variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `BUCKETNAME=cqlreplicator`, `KEYSPACENAME=ks_test_cql_replicator`, `TABLENAME=test_cql_replicator`, and `CQLREPLICATOR_CONF`
* Create the replicator keyspace, and an internal table to store stats in Amazon Keyspaces:
```
    CREATE TABLE replicator.stats (
        tile int,
        keyspacename text,
        tablename text,
        ops text,
        "rows" counter,
        PRIMARY KEY ((tile, keyspacename, tablename, ops))
    )
```
* For production purposes pre-warm the target table    	
                 
After the table has been created, switch the table to on-demand mode to pre-warm the table and avoid throttling failures. 
The following script will update the throughput mode. 

* Run one cassandra instance:
   ```docker run --name test-cql-replicator -d -e CASSANDRA_NUM_TOKENS=8 -p 9042:9042 cassandra:3.11```
* Generate cassandra workload:
   `cassandra_ip=$(ifconfig en0 | grep inet | awk '{print $2}')
    cassandra-stress user profile=test/integration/cql_replicator_stress_test.yaml duration=1s no-warmup ops\(insert=1\) -rate threads=2 -node $cassandra_ip`
* Run CQLReplicator with two tiles:
Set the base folder by running the following command ```cd "$(dirname "$CQLREPLICATOR_CONF")/bin"``` after that you need to run
two CQLReplicator instances for the partition discovery
   ```./cqlreplicator.sh --syncPartitionKeys --tile 0 --tiles 2 & ```
   ```./cqlreplicator.sh --syncPartitionKeys --tile 1 --tiles 2 & ```  and two CQLReplicator instances for the row discovery
   ```./cqlreplicator.sh --syncClusteringColumns --tile 0 --tiles 2 & ```
   ```./cqlreplicator.sh --syncClusteringColumns --tile 1 --tiles 2 & ```
* To validate migration process run ```select * from replicator.stats``` against Amazon Keyspaces, or you can run ```./cqlreplicator.sh --tile 0 --tiles 2 --stats ```

# Credentials
There are two options to use and store credentials for Cassandra and Amazon Keyspaces:

* Explicitly updating the username and the password in respective configuration files and uploading to the S3 bucket (Default option)
* Use AWS Systems Manager Parameter Store by declaring an env variable "AWS_SMPS_MODE". For example: `export AWS_SMPS_MODE = true` 

# Clean up
Let's clean up the testing environment to avoid extra charges and resource consumption
* Stop the docker container by running the following command: 
    ```docker container stop test-cql-replicator```
* Remove the docker container by running the following command: 
    ```docker container rm test-cql-replicator```
* Stop CQLReplicator by running the following command: 
    ```kill $(jps -ml | grep com.amazon.aws.cqlreplicator.Starter | awk '{print $1}')```
    The CQLReplicator process will report:
     `12:07:10.336 [Thread-2] INFO  com.amazon.aws.cqlreplicator.Stopper - Stopping process is activated
      12:07:10.337 [Thread-2] INFO  com.amazon.aws.cqlreplicator.Stopper - Replication task is stopped: true` in the log.
* Clean up Amazon Keyspaces tables by executing the following commands in the cqlsh: 
    ```drop keyspace replicator``` and ```drop keyspace ks_test_cql_replicator```
    
# License
This tool licensed under the Apache-2 License. See the LICENSE file.