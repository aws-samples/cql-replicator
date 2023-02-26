 # Migrate historical Cassandra workload to Amazon Keyspaces
In general terms, data migration is the transfer of the existing historical data to new storage, system,
or file format. This process is not as simple as it sounds. It involves a lot of preparation and post-migration
activities including planning, creating backups, quality testing, and validation of results.

## Migration scripts
The migration script has been designed to solve multiple migration issues:
1. replicate multiple keyspaces/tables to Amazon Keyspaces
2. reduce the migration costs by moving a bulk part of the data at once without provisioning a complex infrastructure
3. reduce the migration time by moving historical data before starting the CQLReplicator cluster
4. customer can't procure Amazon EMR/AWS Glue in the environment 

## Requirements
1. Cassandra 2.1+
2. [Apache Spark 2.4+](https://archive.apache.org/dist/spark/spark-2.4.8)
3. [Java 1.8](https://docs.aws.amazon.com/corretto/latest/corretto-8-ug/downloads-list.html)
4. Amazon EC2 (start from m6i.2xlarge or c6i.2xlarge) with EBS volume
5. Proper security group to access the Cassandra cluster via 9042/9142
6. Proper security group to access Amazon Keyspaces via 9142   

## Create and pre-warm target tables in Amazon Keyspaces
Let's create and provision the target table
```CREATE KEYSPACE ks_test_cql_replicator WITH replication = {'class': 'SingleRegionStrategy'} ```

### Calculate WCUs/RCUs for the Amazon Keyspaces table
Let's consider writing 100,000,000 rows with the row size 1 KB during one hour. The total number of write capacity units 
that you need is (100,000,000/60 min * 60s) * 1 WCU (1 KB) ~ 27,777 WCUs per second. 27,777 WCUs per second is that rate for 1 hour,
but you might add some cushion for overhead, e.g. 27,777 WCUs * 1.10 ~ 30,554 WCUs per second with 10% overhead.

```
CREATE TABLE ks_test_cql_replicator.test_cql_replicator (
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
       		'write_capacity_units':30554,
       		'read_capacity_units':5000
       	}
WITH CLUSTERING ORDER BY (col0 ASC)
``` 
After the table has been created, switch the table to on-demand mode to avoid extra costs during the migration phase. 

```
ALTER TABLE ks_test_cql_replicator.test_cql_replicator 
WITH CUSTOM_PROPERTIES = {
	'capacity_mode':{
		'throughput_mode':'PAY_PER_REQUEST'
	}
}
```
## Provision Storage
Provision an EBS/EFS volume to store intermediate parquet files, one terabyte of the replicated cassandra data will require
(1 TB/3 replicas)*1.10 = 375.4 GB of free space.

## Spark permissions to access Cassandra and Amazon Keyspaces
Before running the migration script configure necessary permissions to allow access resources.

### Cassandra permissions
Grant SELECT permissions to the migration user or the existing user
1. `GRANT SELECT ON KEYSPACE keyspace TO migration_user`
2. `GRANT SELECT ON TABLE system.size_estimates TO migration_user` 

### Amazon Keyspaces permission
1. Enable [Amazon Keyspaces Partitioner](https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-partitioners.html)
2. Check IAM Role [Spark-cassandra-connector](https://docs.aws.amazon.com/keyspaces/latest/devguide/spark-tutorial-step1.html)

### Configure Amazon Keyspaces conf file
You can use an example from the repository [KeyspacesConnector.conf](https://github.com/aws-samples/cql-replicator/blob/dqResolver/spark/conf/KeyspacesConnector.conf).

### Configure the Cassandra file
You can use an example from the repository [CassandraConnector.conf](https://github.com/aws-samples/cql-replicator/blob/dqResolver/spark/conf/CassandraConnector.conf).

### Configure config.json
config.json helps you to manage migration process of multiple keyspaces/tables, you can control `write concurrency`, 
`synchronization type`, and `nulls replacement`

1. `timestamp_column` is a regular column with a simple data type in the source table, if there is no such column use "NA",
2. `concurrent_writes` defines the number of concurrent writers to Amazon Keyspaces tables (def. is 5)
3. `sync_type` defines migration mode `batch` or `incremental`,
4. `replace_nulls` replace nulls, for example, "replace_nulls":"{\"cnt\":0, \"sum\":0}", or "\"\""" nothing to replace.

```json
{
  "ks_test_cql_replicator": [
    {
      "table_name": "test_cql_replicator",
      "timestamp_column": "col3",
      "concurrent_writes": "3",
      "sync_type": "batch",
      "replace_nulls": "{\"col1\":\"no data\", \"col4\":0}"
    }]
}
``` 
## Set essential parameters in the `bulk-migrator.sh`
1. `WORKING_DIR` path to intermediate parquet files
2. `SPARK_HOME` path to the Spark binaries
3. `JAVA_HOME` path to Java JRE/JDK
4. `SPARK_LOCAL_DIRS` by default, Spark stores map output files and RDDs in `/tmp` folder. 
    In order to avoid `no space left on the device` I would recommend allocating extra space on attached EBS volume.

## Consideration
Apache Spark can saturate CPUs of the Cassandra quickly due to unlimited read throughput. Best practice is to set `CASSANDRA_READ_THROUGHPUT`
 to lower values.

## Run the migration stages
The `bulk-migrator` allows you to replicate the Cassandra workload to Amazon Keyspaces in two migration modes.

### Full data load
Run the full data load from the Cassandra to Amazon Keyspaces run the following command 
`$./bulk-migrator.sh --batch-keyspace ks_test_cql_replicator`.

### Incremental data load
To run incremental data load set `sync_type` to `incremental` in config.json and run the following command
`$./bulk-migrator.sh --increment-keyspace ks_test_cql_replicator`. You can run the incremental data load only after
running the full data load at least once.

## Scheduling incremental migration with CRON
Let's replicate data from the Cassandra to Amazon Keyspaces every 30 minutes. Run `crontab -e` and 
add the following line to the crontab `30 * * * * /spark/bin/bulk-migrator.sh --increment-keyspace ks_test_cql_replicator`.

## Data Validation
Run `$./bulk-migrator.sh --validate-keyspace ks_test_cql_replicator` to compare the persisted parquet files
with the target table.

## Reconciliation process (optional)
There is a way to reconcile the target table with Spark scripts.

### Run the following command to start `spark-shell`
```
spark-shell --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.2 \
    --files ../conf/KeyspacesConnector.conf --conf spark.driver.memory=4g \
    --conf spark.cassandra.output.concurrent.writes=1 --conf spark.cassandra.output.batch.size.rows=1 \
    --conf spark.cassandra.output.batch.grouping.key=none \
    --conf spark.executor.memory=8g --conf spark.cassandra.connection.config.profile="KeyspacesConnector.conf" \
    --conf spark.task.maxFailures=256 
``` 
### Add imports
```
import com.datastax.spark.connector.cql.CassandraConnector
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.rand
import org.apache.spark.storage.StorageLevel
```

### Set variables
```
val keyspacename= 'ks_test_cql_replicator'
val tablename = 'test_cql_replicator'
```

### Inference the primary keys from the table to construct a joining condition between the source, and the target 
```
val connector = CassandraConnector(sc.getConf)
val pk = connector.openSession.getMetadata.getKeyspace(keyspacename).get.getTable(tablename).get.getPrimaryKey
var pkList = new ListBuffer[String]()
val it = pk.iterator
while (it.hasNext) { pkList+=(it.next.getName().torString) }
val pkFinal = pkList.toSeq
val cond = pkFinal.map(x=>col(String.format("s%.s%", "source", x)) == col(String.format("%s.%s","target", x))).reduce(_ && _)
```

### Load the previously offloaded Cassandra data and compute the gap
```
val df_source = spark.read.parquet("../keyspaces/"+keyspacename+"/"+tablename)
val df_target = spark.read.format("org.apache.spark.sql.cassandra").
    option("keyspace", keyspacename).option("table", tablename).load().persist(StorageLevel.MEMORY_AND_DISK)
val dataGap = df_soure.as("source").join(df_target.as("target"), cond, "leftanti").repartition(pkFinal.map(col): _*).orderBy(rand()).persist(StrorageLevel.MEMORY_AND_DISK)
dataGap.drop("ts").write.format("org.apache.spark.sql.cassandra").option("keyspace", keyspacename).option("table", tablename).mode("append").save()
``` 
