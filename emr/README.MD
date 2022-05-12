# Migrate historical Cassandra workload to Amazon Keyspaces
You can utilize Amazon EMR/AWS Glue in order to reduce migration costs and overall latency. 

## Create and pre-warm S3 bucket, target and ledger tables
The first step is to prepare `application_cassandra.conf` and `application_keyspaces.conf` files from the Cassandra cluster 
and Amazon Keyspaces. 
Let's create Amazon Keyspaces tables. The following CQL commands create a keyspace and two Amazon Keyspaces tables in provisioned mode. 

### Create Amazon S3 bucket
The final step is to create with a unique S3 bucket, for example, `bdc11b32-cf9d-11ec-9d64-0242ac120002` to stage our intermediate data before to write to Amazon Keyspaces.
    
### Create Keyspaces keyspaces and tables
You can use Amazon Keyspaces console or cqlsh to create keyspaces and tables.

```CREATE KEYSPACE replicator WITH replication = {'class': 'SingleRegionStrategy'}```
```CREATE KEYSPACE ks_test_cql_replicator WITH replication = {'class': 'SingleRegionStrategy'} ```

```CREATE TABLE ks_test_cql_replicator.test_cql_replicator (
    key uuid,
    col0 tinyint,
    col1 text,
    col2 date,
    col3 double,
    col4 int,
    col5 bigint,
    col6 timestamp,
    col7 float,
    col8 blob,
    PRIMARY KEY (key, col0)
) WITH default_time_to_live = 0 AND CUSTOM_PROPERTIES = {
       	'capacity_mode':{
       		'throughput_mode':'PROVISIONED',
       		'write_capacity_units':10000,
       		'read_capacity_units':5000
       	}
WITH CLUSTERING ORDER BY (col0 ASC)
```

```
CREATE TABLE replicator.ledger_v4 (
    process_name text,
    tile int,
    keyspacename text,
    tablename text,
    pk text,
    cc text,
    operation_ts timestamp,
    value bigint,
    PRIMARY KEY ((process_name, tile, keyspacename, tablename, pk), cc)
) WITH default_time_to_live = 0 AND CUSTOM_PROPERTIES = {
       	'capacity_mode':{
       		'throughput_mode':'PROVISIONED',
       		'write_capacity_units':15000,
       		'read_capacity_units':5000
       	} 
WITH CLUSTERING ORDER BY (cc ASC)
```

### Change provisioned mode to on-demand for Amazon Keyspaces' tables
After the all tables created and provisioned, turn them all into on-demand mode, by executing following commands:
```ALTER TABLE replicator.ledger_v4 
WITH CUSTOM_PROPERTIES = {
	'capacity_mode':{
		'throughput_mode':'PAY_PER_REQUEST'
	}
}
```

```ALTER TABLE ks_test_cql_replicator.test_cql_replicator 
WITH CUSTOM_PROPERTIES = {
	'capacity_mode':{
		'throughput_mode':'PAY_PER_REQUEST'
	}
}
```
 
### Configure Amazon Keyspaces conf file
You can use an example from the repository [KeyspacesConnector.conf](main/conf/KeyspacesConnector.conf).

### Configure the Cassandra file
You can use an example from the repository [CassandraConnector.conf](main/conf/CassandraConnector.conf).

## Stage data into S3 bucket
Let's offload data from the Cassandra table to the S3 bucket and prepare the ledger data to support real-time migration process. 

### Offload historical data and stage it in S3 bucket
1. Start [the EMR cluster](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html)
2. Start the spark-shell console ```./spark-shell --files CassandraConnector.conf --conf spark.cassandra.connection.config.profile.path=CassandraConnector.conf --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0```
3. 

### Prepare ledger data and stage it in S3 bucket

## Load data from S3 bucket to Amazon Keyspaces

## Validate data completeness