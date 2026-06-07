# Replicate workload to Amazon Keyspaces

## Prepare the configuration files
In order to connect to Amazon Keyspaces and the Cassandra cluster you need configure reference 
configuration files `../glue/conf/CassandraConnector.conf` and `../glue/conf/KeyspacesConnector.conf` 
in ../conf accordingly to the environment.

## Init migration process
The following command initializes the CQLReplicator environment, which involves the copying JAR artifacts, creation a Glue connector,
a S3 bucket, a Glue job, migration keyspace, and ledger table.

```shell
    cd cql-replicator/glue/bin &&
    cqlreplicator --cmd init --sg '"sg-1","sg-2"' \ 
                  --subnet "subnet-XXXXXXXXXXXX" --az us-east-1a --region us-east-1 \
                  --env ver5 \  # Optional 
                  --glue-iam-role glue-cassandra-migration \
                  --landing-zone s3://<PROVIDED_BUCKET_NAME> \
                  --target-type keyspaces
```
After running the below command, you should find in the AWS account:

1. CQLReplicator-ver5 glue job and Glue connector in AWS Glue console
2. Amazon S3 bucket that stores the artifacts
3. Keyspace `migration` and `ledger` table in Amazon Keyspaces

![](../images/init-CQLReplicator.png "Init")

Note: If you run the CQLReplicator on EC2 you should attach the following inline policy:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "iam:GetContextKeysForPrincipalPolicy",
        "iam:SimulatePrincipalPolicy",
        "iam:ListAttachedRolePolicies",
        "iam:PassRole",
        "glue:*",
        "pricing:GetProducts",
        "pricing:DescribeServices",
        "pricing:GetAttributeValues"
      ],
      "Resource": "*"
    }
  ]
}
```

- `--sg` - these security group(s) which allow access to the Cassandra cluster from AWS Glue and includes the
  self-referencing inbound rule for all traffic
- `--subnet` - this is the subnet to which the Cassandra cluster belongs. In AWS Glue, when a Glue job needs to run in
  your Virtual Private Cloud (VPC),
  AWS Glue sets up elastic network interfaces that enable your Glue jobs to connect securely to other resource within
  your VPC. Each elastic network interface
  is assigned a private IP address from the IP address range within the subnet you specified. For example, 20 workers
  need 20 IP addresses.
- `--az` - this is the AZ of the subnet
- `--region` - AWS region where the Cassandra cluster is deployed
- `--glue-iam-role` - you
  need [the IAM role permissions](https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html)
  that AWS Glue can assume when calling other services on your behalf. Services: Amazon Keyspaces and S3
- `--landing-zone` - this is an optional parameter, you can reuse any existing S3 buckets in the AWS account
  If you don't supply it, init process will try to create a new bucket to store config, JAR artifacts, and intermediate
  files
- `--target-type` - this is an optional parameter, the default value is `keyspaces`, available options: `parquet`
  , `memorydb`,and `opensearch`
- `--skip-glue-connector` - this is an optional parameter allows you to skip the glue connector creation step
- `--skip-keyspaces-ledger` - in case if you don't want to create/delete the ledger (the internal table in Amazon
  Keyspaces migration.ledger)
- `--main-script-landing` - use this flag if you want to store the main script in the `s3://your-landing-zone/script/CQLReplicator.scala`
- `--env` - use to create multiple environments, e.g. dev/test/prod

### VPC Endpoint Requirements

CQLReplicator uses Apache Iceberg with the AWS Glue Data Catalog for snapshot management. The Glue job runs inside a VPC, so the following VPC endpoints are required for Iceberg catalog operations:

| Endpoint | Type | Purpose |
|----------|------|---------|
| `com.amazonaws.{region}.glue` | Interface | Iceberg catalog operations (CREATE TABLE, getTable, etc.) |
| `com.amazonaws.{region}.s3` | Gateway | Iceberg data/metadata read/write in S3 |

The Glue interface endpoint must have:
- `PrivateDnsEnabled` = `true`
- The VPC must have `enableDnsSupport` = `true` and `enableDnsHostnames` = `true`
- The endpoint's security group must allow inbound TCP port 443 from the Glue job's security group

Verification:
```shell
# Check Glue endpoint
aws ec2 describe-vpc-endpoints \
  --filters "Name=vpc-id,Values={vpc-id}" "Name=service-name,Values=com.amazonaws.{region}.glue" \
  --query 'VpcEndpoints[*].{ID:VpcEndpointId,Subnets:SubnetIds,State:State,PrivateDns:PrivateDnsEnabled}'

# Check S3 endpoint
aws ec2 describe-vpc-endpoints \
  --filters "Name=vpc-id,Values={vpc-id}" "Name=service-name,Values=com.amazonaws.{region}.s3" \
  --query 'VpcEndpoints[*].{ID:VpcEndpointId,State:State}'
```

## Start migration process
To operate CQLReplicator on AWS Glue, you need to use `--cmd run` command, followed by a series of parameters. The
precise configuration of these is primarily determined by your unique migration requirements. For instance, these
settings may vary if you choose to replicate TTLs, updates, or offloading objects exceeding 1MB to Amazon S3.

The CQLReplicator offers three options for operation:
1. For high-volume traffic exceeding 5000 Write Capacity Units (WCU) per second:
    * Use the command: `--max-wcu-traffic 20000`
    *  This setting uses the default of 4 tiles (Glue jobs)
2. For lower-volume traffic below 5000 WCU per second:
    * Use the command:` --max-wcu-traffic 4000 --tiles 1`
    * This setting customizes the number of tiles to 1
3. For custom configurations:
    * Use the command: --tiles 12 (without specifying --max-wcu-traffic)
    * This allows you to set a custom number of tiles with your preferred worker type
    * Note: Changing the number of tiles requires to resize the migration process
4. When estimating tile requirements:
    * Each tile (concurrent Glue job) can process up to 74 million primary keys
    * This is achieved using 299 DPUs (G.025X) maximum
    * For example, 12 tiles can handle up to 1 billion primary keys

These guidelines help optimize the CQLReplicator's performance based on your specific traffic and data volume needs.

### Replicate near-real time updates, inserts, and deletes
Let's run the following command to replicate the workload from the Cassandra cluster to Amazon Keyspaces with the write traffic 20000 WCUs per second:

Note: The following command is going to capture only "insert" or "delete". In order, to capture "update" you need to add an arbitrary column from you table
that is constantly updated, for example, add `--writetime-column your-column` to the following command line.

```shell
   cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \
                 --env ver5 --target-type keyspaces
```
![](../images/run-CQLReplicator.png "Run")

The write traffic against the Amazon Keyspaces table should not exceed 20000 WCUs per second

![](../images/traffic-CQLReplicator.png "Write Traffic")

Your source keyspace and table `ks_test_cql_replicator.test_cql_replicator` in the Cassandra cluster.
Your target keyspace and table `ks_test_cql_replicator.test_cql_replicator` in Amazon Keyspaces.
Parameter `--inc-traffic` - enables incremental traffic to not overload the Cassandra Cluster and Amazon Keyspaces
with high number of requests. CQLReplicator is going to increase traffic every 4 minutes. In the following
example with 4 tiles and one cassandra row is under 2KB, the traffic is going to be gradually increased every 4
minutes. At peak traffic, it can reach up to 6,300 WCUs per second.

```shell
   cqlreplicator --cmd run --max-wcu-traffic 6000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --writetime-column col3 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \
                 --env ver5 --inc-traffic --target-type keyspaces
```
![](../images/incremental-traffic.png "Incremental traffic")

### Track changes across multiple columns
The `--writetime-column` parameter accepts multiple comma-separated column names. When multiple columns are specified,
CQLReplicator computes the maximum `WRITETIME()` across all specified columns to detect the most recent mutation for change detection.

Single column (existing behavior):
```shell
   cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --writetime-column col1 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \
                 --env ver5 --target-type keyspaces
```

Multiple columns:
```shell
   cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --writetime-column col1,col2 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \
                 --env ver5 --target-type keyspaces
```

When multiple columns are specified, CQLReplicator generates `greatest(writetime(col1), writetime(col2), ...) as ts`
in the CQL query. Cassandra's `WRITETIME()` returns null for columns that were never explicitly written. The `greatest()`
function excludes null values and returns the maximum of the non-null writetimes. If all specified columns have null
writetimes for a row, the result is null.

### Switch from UNLOGGED BATCH to Single Insert
By default, CQLReplicator uses UNLOGGED BATCH operations for improved performance and cost efficiency.
However, when dealing with large partitions, you may want to disable batching and use single inserts instead to avoid throttling issues in Amazon Keyspaces.
This can help ensure smoother migration of large partitions.

```shell
   cqlreplicator --cmd run --max-wcu-traffic 6000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --writetime-column col3 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \
                 --env ver5 --inc-traffic --target-type keyspaces --json-mapping '{"keyspaces": {"writeConfiguration": { "maxStatementsPerBatch": 1}}}’
```

### Change the workload type
If you want to replicate only the historical workload and release Glue resources once it's completed, add the following flag
`--workload-type batch` to the command line, for example:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --writetime-column col3 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \
                 --env ver5 --inc-traffic --target-type keyspaces --env ver5 --workload-type batch
```

### Replicate with TTL
the TTL feature should be [enabled](https://docs.aws.amazon.com/keyspaces/latest/devguide/TTL-how-it-works.html#ttl-howitworks_enabling)
on the target table before running the following command
```shell
   cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --writetime-column col3 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator --ttl-column col3 \ 
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator --inc-traffic --env ver5 --target-type keyspaces
```

### Replicate with original timestamp
the Client-Side Timestamps feature should be [enabled](https://docs.aws.amazon.com/keyspaces/latest/devguide/client-side-timestamps.html)
on the target table before running the following command
```shell
   cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --writetime-column col3 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator --env ver5 \ 
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator  \
                 --target-type keyspaces --json-mapping '
                    {
                     "replication": {
                       "replicateWithTimestamp": true
                     }
                    }'
```

### Offload large objects
Before running the migration process
configure [lifecycle](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html)
for the S3 bucket to delete objects after expiration. The xref column should be present in the source and the target
tables before running the cqlreplicator.

The payload will be compressed by [LZ4](https://fuchsia.googlesource.com/third_party/lz4/+/refs/tags/v1.7.5/README.md) and stored in the S3 bucket. The compressed payload includes the length of the original
decompressed data, so that you don't need to save the length at anywhere else. The compressed data must be decompressed by
LZ4DecompressorWithLength and is NOT compatible with any other decompressors in lz4-java or any other lz4 tools.
This class deliberately does not extend LZ4Compressor because they are not interchangeable.

```shell
./cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --src-keyspace ks_test_cql_replicator \
                --src-table test_cql_replicator --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator --env ver5 \
                --region us-east-1 --ttl-column col3 --target-type keyspaces --json-mapping '
                {
                 "replication": {
                   "allColumns": true,
                   "columns":[""],
                   "useCustomSerializer": false,
                   "useMaterializedView": {
                     "enabled": false,
                     "mvName": "mvName"
                  }
                },
                 "keyspaces": {
                   "compressionConfig": {
                   "enabled": false,
                   "compressAllNonPrimaryColumns": false,
                   "compressNonPrimaryColumns": [""],
                   "targetNameColumn": ""
                 },
                 "largeObjectsConfig": {
                   "enabled": true,
                   "column": "col1",
                   "bucket": "my-application-resource",
                   "prefix": "ks_test_cql_replicator/test_cql_replicator/col1",
                   "enableRefByTimeUUID": false,
                   "xref": "link"
                 }
                }}'
```
If you enable `enableRefByTimeUUID` the S3 bucket will be populated in the following format s3://<PROVIDED_BUCKET_NAME>/ks_test_cql_replicator/test_cql_replicator/col1/<timeUUID>.
The target column link will be populated by a referencing timeUUID, and col1 will be skipped.

If you disable `enableRefByTimeUUID` the S3 bucket will be populated in the following format s3://<PROVIDED_BUCKET_NAME>/ks_test_cql_replicator/test_cql_replicator/col1/key=<col0>:<key>/payload,
where col0 is the clustering column, and key is the partition key. The target column link and col1 will be skipped.

### Handling Failed Rows
When the CQLReplicator encounters persistent failures during replication, failed rows are automatically stored in
a Dead Letter Queue (DLQ) directory within your S3 bucket.
Failed rows are stored in the following S3 path format: s3://<PROVIDED_BUCKET_NAME>/ks-name/tbl-name/dlq.

Common failure scenarios:
- When source partitions exceed tens of megabytes
- When operations exceed the maximum retry limit (64 attempts). Usually indicates persistent connectivity or resource issues
- When the target table schema doesn't match the source. Fix the schema before replaying

Best practises:
- Review the failed rows in DLQ to understand the failure patterns
- Address any underlying issues (schema, connectivity, resources) before replay
- Monitor the replay process to ensure successful completion

To replay failed rows:
1. Stop the CQLReplicator with `request-stop`
2. Run it again with `--replay-log` - this option allows you to replay failed operations during the replication phase.

## Stop migration process
To stop migration process gracefully run the following command for all tiles:

```shell
   cqlreplicator --cmd request-stop --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1-ver5 \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator--target-type keyspaces --env ver5
```

To stop migration process gracefully run the following command for a single tile:

```shell
   cqlreplicator --cmd request-single-stop --tile 1 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator --target-type keyspaces --env ver5
```

To start migration process for a single tile:

```shell
   cqlreplicator --cmd run-single --tile 1 --tiles 4 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator --inc-traffic --target-type keyspaces --env ver5
```

To force an immediate termination of the CQLReplicator:
```shell
   cqlreplicator --cmd kill --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator --target-type keyspaces --env ver5
```

## Recover from failures
Customers can simply restart the migration process from the point where it was interrupted by restarting the failed
CQLReplicators jobs. To restart the failed CQLReplicators jobs, you need to rerun your `run` command with the same
parameters
if both the discovery and some of the replicators have failed. If the discovery job is still running and some of the
replicators have failed, you need to use extra flag `--skip-discovery`.

## Cleanup the ledger
If you wish to reset the CQLReplicator for a specific table, add the `--cleanup-requested` or `--cr` flag to your `run` command.
This action will clean up both the internal CQLReplicator ledger table and the S3 data, but don't forget to remove it
if you want to stop it and run it again.

## Get migration stats
To obtain the number of replicated rows during the back filling phase, run the following command:

```shell
   cqlreplicator --cmd stats --landing-zone s3://<PROVIDED_BUCKET_NAME> \ 
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
                 --target-type keyspaces --region us-east-1 --env ver5
```

To obtain the number of replicated rows after the back filling phase, run the following command:

```shell
cqlreplicator --cmd stats --landing-zone s3://<PROVIDED_BUCKET_NAME> \
              --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
              --target-type keyspaces --region us-east-1 --replication-stats-enabled --env ver5
```

```
    ___ ___  _     ____            _ _           _
  / ___/ _ \| |   |  _ \ ___ _ __ | (_) ___ __ _| |_ ___  _ __
 | |  | | | | |   | |_) / _ \ '_ \| | |/ __/ _` | __/ _ \| '__|
 | |__| |_| | |___|  _ <  __/ |_) | | | (_| (_| | || (_) | |
  \____\__\_\_____|_| \_\___| .__/|_|_|\___\__,_|\__\___/|_|
                            |_|
·······································································
:     __          _______   _____           _____                     :
:    /\ \        / / ____| |  __ \         / ____|                    :
:   /  \ \  /\  / / (___   | |__) | __ ___| (___   ___ _ ____   _____ :
:  / /\ \ \/  \/ / \___ \  |  ___/ '__/ _ \\___ \ / _ \ '__\ \ / / _ \:
: / ____ \  /\  /  ____) | | |   | | | (_) |___) |  __/ |   \ V /  __/:
:/_/    \_\/  \/  |_____/  |_|   |_|  \___/_____/ \___|_|    \_/ \___|:
·······································································
[2024-02-02T16:50:18-05:00] OS: Linux
+------------------------------------------------------------------------+
| Tile     | Inserts  | Updates  | Deletes  | Timestamp                  |
+------------------------------------------------------------------------+
| 0        | 4        | 1        | 0        | "2024-02-02T21:50:12.888"  |
+------------------------------------------------------------------------+
| 1        | 2        | 2        | 1        | "2024-02-02T21:50:25.454"  |
+------------------------------------------------------------------------+
[2024-02-02T16:50:39-05:00] Discovered rows in ks_test_cql_replicator.test_cql_replicator is 70,270
[2024-02-02T16:50:39-05:00] Replicated rows in ks_test_cql_replicator.test_cql_replicator is 70,270
```
## Retrieve logs from failed Glue jobs
To retrieve errors from Glue jobs, you can execute the following command:
```shell
cqlreplicator --cmd get-failed-jobs --landing-zone s3://<PROVIDED_BUCKET_NAME> \
              --region us-east-1 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
              --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator
```

## Configure log level
By default, CQLReplicator sets the Spark log level to `ERROR` to reduce noise in CloudWatch logs. You can change this using the `--logging-level` flag:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \
                 --env ver5 --target-type keyspaces --logging-level WARN
```

Available log levels: `ALL`, `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR` (default), `FATAL`, `OFF`.

This controls Spark's internal logging (DAGScheduler, BlockManager, TaskSetManager, etc.). CQLReplicator's own messages via GlueLogger are always visible regardless of this setting.

## AWS Glue Monitoring
You can profile and monitor CQLReplicator operations using AWS Glue job profiler. It collects and processes raw data
from AWS Glue jobs into readable, near real-time metrics stored in Amazon CloudWatch.
These statistics are retained and aggregated in CloudWatch so that you can access historical information for a better
perspective on how your application is performing.

In order tp enable the CQLReplicator enhanced monitoring use the `--enhanced-monitoring-enabled` flag with your init
command.

***You may incur additional charges when you enable job metrics and CloudWatch custom metrics are created***

## Point in time replication
if you want to replicate your data from a timestamp simply add the flag `--start-replication-from` followed by
the timestamp to your command line:

```shell
   cqlreplicator --cmd --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME>  \
                 --writetime-column col1 --region us-east-1 \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \
                 --target-type keyspaces --start-replication-from 1706408365278858 --env ver5
```

if you want to change the default point in time replication behavior and replicate data *before* timestamp then specify predicate in json mapping:
```json
{
  "replication": {
    "pointInTimeReplicationConfig": {
      "predicateOp": "lessThanOrEqual"
    }
  }
}
```

## Compress selected columns
To minimize the size of large columns in the target table, enable the `compressionConfig` in the json mapping.
All selected columns will be encoded in JSON format and compressed by the LZ4 fast compressor.

Note that the compressed payload includes the length of the original decompressed data, so that you don't need to save
the length at anywhere else. The compressed data must be decompressed by [LZ4DecompressorWithLength](https://javadoc.io/doc/org.lz4/lz4-java/latest/net/jpountz/lz4/LZ4DecompressorWithLength.html)
and is NOT compatible with any other decompressors in lz4-java or any other lz4 tools. This class deliberately does not extend
LZ4Compressor because they are not interchangeable.

```shell
./cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --writetime-column col2 \
                                      --region us-east-1 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                                      --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator_compressed_columns --env ver5 \
                                      --target-type keyspaces --json-mapping '{
                                          "replication": {
                                            "allColumns": true,
                                            "columns":[],
                                            "useCustomSerializer": false
                                          },
                                          "keyspaces": {
                                          "compressionConfig": {
                                            "enabled": true,
                                            "compressAllNonPrimaryColumns": false,
                                            "compressNonPrimaryColumns": ["col2","col3","col4","col5","col6","col7","col8"],
                                            "targetNameColumn": "compressedPayload"
                                          }}
                                        }'
```

## Replicate only selected columns
if you want to change the replicated columns set `allColumns`:`false`, and set the columns
list `columns`:`["col1","col2"]`
in the json mapping during the `run`, for example,

```shell
./cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME>--writetime-column col2 \
                                      --region us-east-1 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                                      --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator_compressed_columns --env ver5 \
                                      --target-type keyspaces --json-mapping '{
                                          "replication": {
                                            "allColumns": false,
                                            "columns":["col1", "col2"],
                                            "useCustomSerializer": false
                                          },
                                          "keyspaces": {
                                          "compressionConfig": {
                                            "enabled": false,
                                            "compressAllNonPrimaryColumns": false,
                                            "compressNonPrimaryColumns": ["col2","col3","col4","col5","col6","col7","col8"],
                                            "targetNameColumn": "compressedPayload"
                                          }}
                                        }'
```

```shell
./cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --writetime-column col2 \
                                      --region us-east-1 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                                      --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator_filtered_columns --env ver5 \
                                      --target-type keyspaces --json-mapping '{
                                          "replication": {
                                            "allColumns": false,
                                            "columns":["key","col0","col1","col2","col3","col4","col5","col6"],
                                            "useCustomSerializer": false
                                          },
                                          "keyspaces": {
                                          "compressionConfig": {
                                            "enabled": false,
                                            "compressAllNonPrimaryColumns": false,
                                            "compressNonPrimaryColumns": ["payload"],
                                            "targetNameColumn": "compressed"
                                          }}
                                        }'
```
## Filtering Records by Primary and Non-Primary Keys
There are use-cases when you need to filter out some certain keys during the migration phase, for example:

```shell
./cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --writetime-column col2 \
                                      --region us-east-1 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                                      --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator_filtered_columns --env ver5 \
                                      --target-type keyspaces --json-mapping '{
                                          "keyspaces": {
                                            "transformation": {        
                                            "enabled": true,        
                                            "filterExpression": "client_id NOT like \"ACME%\"" 
                                            }
                                          }
                                        }'
```

```shell
./cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --writetime-column col2 \
                                      --region us-east-1 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                                      --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator_filtered_columns --env ver5 \
                                      --target-type keyspaces --json-mapping '{
                                          "keyspaces": {
                                            "transformation": {        
                                            "enabled": true,
                                            "addNonPrimaryKeyColumns": ["col2"],        
                                            "filterExpression": "col2 > timestamp \"2024-11-12 00:00:59.588000+0000\"" 
                                            }
                                          }
                                        }'
```
Note: You can use only primary key columns in the `filterExpression` and it should be defined as [the Spark SQL Expression](https://archive.apache.org/dist/spark/docs/3.3.0/sql-ref-functions-builtin.html).

## Data Transformation

CQLReplicator supports data transformation during migration using various hash functions. This feature is useful for:
- **Data anonymization**: Hash sensitive data like emails, phone numbers, or personal identifiers
- **Data obfuscation**: Transform data while maintaining referential integrity
- **Compliance requirements**: Meet data privacy regulations by hashing PII data

### Supported Hash Functions

- **MurmurHash3**: Fast, non-cryptographic hash function (returns integer)
- **MD5**: 128-bit cryptographic hash (returns hex string)
- **SHA-1**: 160-bit cryptographic hash (returns hex string)
- **SHA-256**: 256-bit cryptographic hash (returns hex string)
- **XXHASH64**: Fast, 64-bit non-cryptographic hash (returns long integer)

### Transform Expression Parameters

- `columnName`: The source column to transform to hash value 
- `rule`: Hash function to apply (murmurhash3, md5, sha-1, sha-256, xxhash64)
- `alias`: Optional new column name (if not provided, replaces original column)
- `keepSource`: Boolean to keep original column when using alias (default: false)

### Example Usage

```shell
cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<BUCKET_NAME> \
              --region us-east-1 --src-keyspace source_ks --src-table users \
              --trg-keyspace target_ks --trg-table users_transformed --env prod \
              --target-type keyspaces --json-mapping '{
                  "keyspaces": {
                    "transformation": {
                      "enabled": true,
                      "transformExpressions": [
                        {
                          "columnName": "email",
                          "rule": "md5",
                          "alias": "email_hash",
                          "keepSource": false
                        },
                        {
                          "columnName": "phone",
                          "rule": "sha-256",
                          "alias": "phone_hash",
                          "keepSource": true
                        },
                        {
                          "columnName": "user_id",
                          "rule": "murmurhash3"
                        }
                      ]
                    }
                  }
                }'
```

This example:
- Hashes `email` to MD5 and replaces it with `email_hash`
- Hashes `phone` to SHA-256 as `phone_hash` while keeping original `phone` column
- Hashes `user_id` in place using MurmurHash3

## Not overwriting existing records in Amazon Keyspaces
In order to preserve existing records during the bulk migration phase set `readBeforeWrite` to `true` in the `json-mapping`, for example:
```shell
./cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --writetime-column col2 \
                                      --region us-east-1 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                                      --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator_filtered_columns --env ver5 \
                                      --target-type keyspaces --json-mapping '{
                                          "keyspaces": {
                                              readBeforeWrite: true
                                          }
                                        }'
```
## Converting UDTs to JSON
This feature allows the CQLReplicator tool to automatically convert UDTs in C* to plain JSON strings.
This enhancement provides a seamless way to transform UDTs into a more universal compatible JSON format,
facilitating easier integration with other NoSQL databases.

```shell
./cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --writetime-column col2 \
                                      --region us-east-1 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                                      --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator_filtered_columns --env ver5 \
                                      --target-type keyspaces --json-mapping '{
                                          "keyspaces": {
                                            "udtConversion": { "enabled": true,"columns": ["device", "user"] }
                                            }}'
```

## Handling Large VARINT Values (Over 38 Digits)

### Why Use This Feature
By default, AWS Glue converts VARINT columns to `decimal(38,0)` during the read operation. 
This causes values exceeding 38 digits to be implicitly converted to `null`, resulting in data loss during migration. 
This is a known limitation of Spark's DataFrame API when reading from Cassandra.

### When to Use This Feature
Enable the custom VARINT reader when:
- Your source table contains VARINT columns with values exceeding 38 digits
- You notice VARINT values being converted to `null` in the target table
- You need to preserve the full precision of arbitrary-precision integers
- You're migrating financial data, cryptographic keys, or other data requiring exact large integer values

### How It Works
The custom VARINT reader uses Cassandra's low-level RDD API instead of Spark's DataFrame API. This bypasses Spark's automatic type conversion and reads VARINT values directly as strings, preserving their full precision regardless of size.

### Usage Example
Enable the custom VARINT reader by setting `useCustomVarintReader` to `true` in the `readConfiguration`:

```shell
./cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> \
                --region us-east-1 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
                --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator --env ver5 \
                --writetime-column col2 --target-type keyspaces --json-mapping '{
                  "replication": {
                    "readConfiguration": {
                      "useCustomVarintReader": true,
                      "concurrentReads": 32,
                      "consistencyLevel": "LOCAL_ONE",
                      "fetchSizeInRows": 500
                    }
                  }
                }'
```

### Configuration Options
The `readConfiguration` section supports the following parameters:
- `useCustomVarintReader` (boolean, default: `false`) - Enable custom VARINT handling for values over 38 digits

### Performance Considerations
- The custom VARINT reader uses the RDD API which may have slightly different performance characteristics than the standard DataFrame reader
- For tables without large VARINT values, the standard reader (default) is recommended
- The custom reader is only necessary when VARINT values exceed 38 digits

## Enable the custom JSON Serializer
The CQLReplicator relies on Cassandra JSON encoding/decoding that has been introduced in Cassandra 2.2, but if you use
older Cassandra version you must enable the custom JSON Serialization on the client side:

```shell
./cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --writetime-column col2 \
                                      --region us-east-1 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                                      --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator_filtered_columns --env ver5 \
                                      --target-type keyspaces --json-mapping '{
                                          "replication": {
                                            "allColumns": false,
                                            "columns":["key","col0","col1","col2","col3","col4","col5","col6"],
                                            "useCustomSerializer": true
                                          },
                                          "keyspaces": {
                                          "compressionConfig": {
                                            "enabled": false,
                                            "compressAllNonPrimaryColumns": false,
                                            "compressNonPrimaryColumns": ["payload"],
                                            "targetNameColumn": "compressed"
                                          }}
                                        }'
```
### Limitations
The custom JSON serializer supports the following data types: boolean, int, text, ascii, bigint, blob, counter, date,
decimal, double,float,timestamp,smallint,timeuuid,uuid, and tinyint.

The following parameters must be
set `allColumns`:`false`, `columns` : ["partitionKey", "clusteringKey","regularColumn"],
and `useCustomSerializer`:`true`.

## Cost optimization

### Use Batch Mode for historical workload
In order to reduce AWS Glue costs after the historical workload moved to the target storage:

1. Stop the migration process:
    ```shell
    cqlreplicator --cmd request-stop --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                  --target-type keyspaces --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator --env ver5
    ```
2. Restart the migration process with the same number of tiles but with lower number of
   DPUs:
   ```shell
   cqlreplicator --cmd run --max-wcu-traffic 5000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator --target-type keyspaces --env ver5
   ```

### Use scheduled run
1. Stop the migration process:
    ```shell
    cqlreplicator --cmd request-stop --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                  --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator --target-type keyspaces --env ver5
    ```
2. Schedule the migration process to run it every 30 minutes:

   ```shell
   ./cqlreplicator --cmd scheduled-run --max-wcu-traffic 5000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                   --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
                   --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \
                   --env ver5 --target-type keyspaces --schedule "*/30 * * * *"
   ```
   Note: Use only EC2 or ECS to run scheduled jobs

### Monitor your Migration costs
- Monitor costs by reviewing the automated estimation displayed when CQLReplicator initializes, which provides output such as:
  `The total cost of running CQLReplicator is XX.YY USD per hour`
- Use Cost Dashboard and AWS Cost Explorer to calculate and track cost savings from optimization efforts

## Resize the replication process
To modify the number of tiles (Glue jobs) during the replication process:
- Safely stop the current migration process: Do not force-stop any running jobs to prevent data inconsistency,
- Ensure system state: Verify all Glue jobs show "COMPLETED" status and Check that no replication processes are actively running
- After confirmation of complete shutdown, proceed with your desired tile count modifications using the appropriate commands:

  a. Reduce the number of tiles:
    ```shell 
      ./cqlreplicator --cmd resize --tiles 4 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \ 
                      --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator --target-type keyspaces --env ver5
    ```
  Increase the number of tiles:
    ```shell 
      ./cqlreplicator --cmd resize --tiles 12 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                      --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator --target-type keyspaces --env ver5
    ```
  b. After the resizing job is completed you can start the CQLReplicator again with the new number of tiles `--tiles 12`

After confirming the resizing job has successfully completed, restart CQLReplicator with the updated number of tiles.
Before restarting, verify that all system components are in a stable state.

## Know issues
| Error Message/Issue                                                                                                           |   |   | CW Discovery logs | CW Replication logs | Work around                                                                                                                                                                                                     |
|-------------------------------------------------------------------------------------------------------------------------------|---|---|-------------------|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1. No space left on device                                                                                                    |   |   | Yes               | No                  | Scale out the replication process by increasing the number of DPUs per discovery Glue job, for example, add `--odw 15`                                                                                          |
| 2. No space left on device                                                                                                    |   |   | Yes               | No                  | Increase the `--max-wcu-traffic`                                                                                                                                                                                |
| 3. You have limited IPs in the subnet                                                                                         |   |   | Yes               | Yes                 | Create a new Glue connection with the new subnet and attach the CQLReplicator Glue Job                                                                                                                          |
| 4. Cassandra timeout during read query at consistency ONE <br/>(1 responses were required but only 0 replica responded) <br/> |   |   | Yes               | No                  | Reduce page size and increase the request timeout in CassandraConnector.conf                                                                                                                                    |
| 5. Cassandra timeout during read query at consistency ONE <br/>(1 responses were required but only 0 replica responded) <br/> |   |   | Yes               | No                  | Create a materialized view in the Cassandra cluster with the same primary keys and one regular column.<br/> Configure the CQLReplicator to use this materialized view as the source for the primary keys. <br/> |
| 6. Cassandra timeout during read query at consistency ONE <br/>(1 responses were required but only 0 replica responded) <br/> |   |   | Yes               | No                  | Increase `read_request_timeout_in_ms`, `range_request_timeout_in_ms`, and `request_timeout_in_ms` in the cassandra.yaml and run rolling restart of the Cassandra cluster.                                       |
| 7. java.lang.OutOfMemoryError: GC overhead limit exceeded                                                                     |   |   | Yes               | No                  | Increase the number of DPUs for the discovery job by using the parameter, e.g. `--override-discovery-workers 12`                                                                                                |
| 8. Improve update/insert latency after the historical phase                                                                   |   |   | No                | No                  | Open the Glue job -> Job Details -> Advanced properties -> Job parameters, and in the Value of the key `--conf` reduce parameters for spark.shuffle.partitions=100 and spark.default.parallelism=100 or less    |
| 9. The requested number value being inserted exceeds the maximum allowed precision of 38 decimal digits                       |   |   | No                | Yes                 | Add the following `--json-mapping '{"replication": {"readConfiguration": {"useCustomVarintReader": true}}` to enable custom VARINT handling for values over 38 digits                                           |

### Work around for `Cassandra timeout during read query at consistency ONE` with Cassandra materialized view
1. Create the materialized view `ks_test_cql_replicator_mv` based on the `ks_test_cql_replicator` in the Cassandra
   cluster with the same primary key and only one regular column
2. Prepare the json mapping with ```"useMaterializedView": {"enabled": true, "mvName": "ks_test_cql_replicator_mv"}```
3. Add the json mapping to the run command, e.g.

```shell 
./cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://<PROVIDED_BUCKET_NAME> --writetime-column col2 \ 
                                      --ttl-column reg_column --region us-east-1 --src-keyspace ks_test_cql_replicator \ 
                                      --src-table ks_test_cql_replicator --trg-keyspace ks_test_cql_replicator --env ver5 \ 
                                      --trg-table ks_test_cql_replicator --target-type keyspaces --json-mapping '{
                                       "replication": {
                                         "allColumns": true,
                                         "columns":[""],
                                         "useCustomSerializer": false,
                                         "useMaterializedView": 
                                           {
                                             "enabled": true,
                                             "mvName": "ks_test_cql_replicator_mv"
                                           }
                                       },
                                       "keyspaces": {
                                           "compressionConfig": {
                                               "enabled": false,
                                               "compressAllNonPrimaryColumns": false,
                                               "compressNonPrimaryColumns": [""],
                                               "targetNameColumn": ""
                                           }
                                       }}'
```  

## Clean up
The following command will delete the Glue job, connector, the S3 bucket, and the ledger table:

```shell
   cqlreplicator --cmd cleanup --landing-zone s3://<PROVIDED_BUCKET_NAME> --region "us-east-1"
```

To also clean up Iceberg databases and tables from the Glue Data Catalog, provide the source keyspaces used during migration:

```shell
   cqlreplicator --cmd cleanup --landing-zone s3://<PROVIDED_BUCKET_NAME> --region "us-east-1" \
                 --src-keyspace-list "ks_source_1,ks_source_2"
```

Each keyspace maps to a Glue database named `{keyspace}_db`. Without `--src-keyspace-list`, the Glue Data Catalog databases are not deleted and a warning is displayed.

## Reconcile primary keys

The reconciliation job is an on-demand CQLReplicator job that compares the
primary-key sets of a source Apache Cassandra table and a target Amazon
Keyspaces table and produces a JSON report plus Parquet snapshots of the
source set, the target set, and the set differences. It is **read-only** with
respect to both Cassandra and Amazon Keyspaces, does not touch
`migration.ledger`, the per-tile Iceberg snapshot tables, or the replication
DLQ, and is safe to run concurrently with an active replication job. This
release supports Keyspaces targets only.

The job is provisioned at `cqlreplicator init` time as a Glue job named
`PrimaryKeyReconciliation${env}`, using the same Glue version, Glue version,
IAM role, Glue connector, and `artifacts/` JAR set as the replicator's job.
The reconciliation Glue job is created with **`MaxRetries = 0`**: a failed run
is not retried automatically by Glue; the operator reruns it explicitly.

### Invocation

The job is launched through the existing CLI using the dedicated
`--state/--cmd` value:

```
--cmd reconcile-keys
```

### CLI arguments

These are the `cqlreplicator` CLI flags consumed by the `reconcile-keys`
subcommand (each flag also has a short alias shown in parentheses).

| Flag | Type | Default | Required | Meaning |
|------|------|---------|----------|---------|
| `--src-keyspace` (`--sk`) | string | — | yes | Source Cassandra keyspace. |
| `--src-table` (`--st`) | string | — | yes | Source Cassandra table. |
| `--trg-keyspace` (`--tk`) | string | — | yes | Target Amazon Keyspaces keyspace. |
| `--trg-table` (`--tt`) | string | — | yes | Target Amazon Keyspaces table. |
| `--landing-zone` (`--lz`) | string (`s3://<bucket>/`) | — | yes | Existing CQLReplicator S3 landing zone. The `<bucket>` is reused for `artifacts/` and for the reconciliation outputs. |
| `--region` (`--sr`) | string | — | yes | AWS region of the Keyspaces target and the Glue job. |
| `--cqlreplicator-enviroment` (`--env`) | string | — | yes | Environment suffix; selects the `PrimaryKeyReconciliation${env}` Glue job. |
| `--target-type` (`--ttp`) | string | — | yes | Must be `keyspaces`. Any other value is rejected; the flag must be supplied explicitly for this subcommand (no default is assumed). |
| `--retention-days` (`--rd`) | integer in `[1, 3650]` | — | no | Switches the subcommand into cleanup mode (see [Cleanup of historical reports](#cleanup-of-historical-reports)). No Glue job is submitted in this mode. |
| `--run-id` (`--rid`) | string `[A-Za-z0-9_-]`, ≤128 chars | derived from Glue `JOB_RUN_ID` | no | Forwarded as `RUN_ID`; isolates this run's S3 artifacts. |
| `--keep-snapshots` (`--ks`) | boolean | `true` | no | Forwarded as `KEEP_SNAPSHOTS`. |
| `--bidirectional` (`--bd`) | boolean | `true` | no | Forwarded as `BIDIRECTIONAL`. When `false`, only source-minus-target is computed. |
| `--sample-size` (`--smz`) | integer in `[0, 10000]` | `100` | no | Forwarded as `SAMPLE_SIZE`; bounds the sample arrays in the report. |
| `--max-target-rcus` (`--mtr`) | non-negative integer | `0` (no throttle) | no | Forwarded as `MAX_TARGET_RCUS`; caps the target read rate. |
| `--output-prefix-override` (`--opo`) | string | empty (use the standard prefix) | no | Forwarded as `OUTPUT_PREFIX_OVERRIDE`; writes under `<override>/<Run_ID>/` instead of the default prefix. |
| `--json-mapping` (`--jm`) | JSON string | `None` | no | Forwarded base64-encoded as `JSON_MAPPING`. Only `keyspaces.transformation` (filter expression and primary-key transforms) is consulted; it must match the value used at replication time. |

### S3 output layout

All artifacts for a single run land under one prefix,
`<Reconciliation_Prefix>`, which by default is:

```
<Landing_Zone><source_keyspace>/<source_table>/reconciliation/<Run_ID>/
```

When `OUTPUT_PREFIX_OVERRIDE` is non-empty, the prefix is
`<OUTPUT_PREFIX_OVERRIDE>/<Run_ID>/` instead. The layout under the prefix:

```
<Reconciliation_Prefix>
├── source/                                 # Source_Snapshot Parquet
│   └── group=<n>/part-*.parquet            # rows per file <= KEYS_PER_PARQUET_FILE (10,500,000)
├── target/                                 # Target_Snapshot Parquet
│   └── group=<n>/part-*.parquet
├── diff/
│   ├── source_minus_target/                # A_Minus_B_Snapshot
│   │   └── part-*.parquet
│   └── target_minus_source/                # B_Minus_A_Snapshot, only when BIDIRECTIONAL=true
│       └── part-*.parquet
└── report.json                             # Report_Document
```

Each snapshot Parquet schema is `Effective_Source_PK_Columns` plus a `group`
integer column, so the tile assignment is recoverable from the file content.

### `report.json` schema

`report.json` is a single JSON object with the following fields:

| Field | Type | Notes |
|-------|------|-------|
| `runId` | string | The resolved `Run_ID`. |
| `runStartedAt` | string | ISO-8601 UTC, millisecond precision, `Z` suffix. |
| `runFinishedAt` | string | Same format as `runStartedAt`. |
| `sourceKeyspace` | string | |
| `sourceTable` | string | |
| `targetKeyspace` | string | |
| `targetTable` | string | |
| `partitionKeyColumns` | array of `{name, type}` | Ordered partition-key columns. |
| `clusteringKeyColumns` | array of `{name, type}` | Ordered clustering-key columns. |
| `schemaFingerprint` | string | Fingerprint of the reconciled effective schema. |
| `totalTiles` | integer | The `TOTAL_TILES` used. |
| `sourceCount` | integer | Distinct primary keys in the source snapshot. |
| `targetCount` | integer | Distinct primary keys in the target snapshot. |
| `intersectionCount` | integer | Size of the source ∩ target intersection. |
| `sourceMinusTargetCount` | integer | Keys present in source but absent from target. |
| `targetMinusSourceCount` | integer | Keys present in target but absent from source. **Omitted when `BIDIRECTIONAL` is `false`.** |
| `sourceMinusTargetSample` | array of PK rows | Up to `SAMPLE_SIZE` rows, sorted deterministically (PK columns ascending, NULLs last). |
| `targetMinusSourceSample` | array of PK rows | Same shape and ordering. **Omitted when `BIDIRECTIONAL` is `false`.** |
| `sourceSnapshotPath` | string (`s3://...`) | Path to `source/`. |
| `targetSnapshotPath` | string (`s3://...`) | Path to `target/`. |
| `sourceMinusTargetPath` | string (`s3://...`) | Path to `diff/source_minus_target/`. |
| `targetMinusSourcePath` | string (`s3://...`) | Path to `diff/target_minus_source/`. **Omitted when `BIDIRECTIONAL` is `false`.** |
| `status` | `"success"` or `"partial"` | `"success"` when all tiles scanned and all writes succeeded; `"partial"` when the run terminated early. |
| `phase` | one of `preflight`, `source_scan`, `target_scan`, `diff`, `report` | Present only when `status` is `"partial"`. |
| `failure` | `{exceptionClass, message}` | Present only when `status` is `"partial"`; `message` ≤ 4096 chars. |
| `jsonMapping` | object | The resolved `JSON_MAPPING` object actually applied at runtime (never the raw base64 string). |

In summary, the three fields `targetMinusSourceCount`,
`targetMinusSourceSample`, and `targetMinusSourcePath` are omitted from
`report.json` whenever `BIDIRECTIONAL` is `false`.

### Example

The following invocation reconciles
`ks_test_cql_replicator.test_cql_replicator` on Cassandra against the same
keyspace/table on Amazon Keyspaces and prints the resulting Glue `JobRunId`:

```shell
   cqlreplicator --cmd reconcile-keys \
                 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \
                 --target-type keyspaces --env ver5
```

When the run finishes, inspect
`s3://<PROVIDED_BUCKET_NAME>/ks_test_cql_replicator/test_cql_replicator/reconciliation/<Run_ID>/report.json`.

### Cleanup of historical reports

Reconciliation outputs accumulate one `<Run_ID>/` directory per run. To age out
old runs for a given `(source_keyspace, source_table)` pair, run the
`reconcile-keys` subcommand with `--retention-days`:

```shell
   cqlreplicator --cmd reconcile-keys \
                 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
                 --retention-days 30
```

- `--retention-days N` accepts an integer in the inclusive range `[1, 3650]`.
- In cleanup mode no Glue job is submitted. The CLI lists the immediate
  subdirectories of
  `<Landing_Zone><source_keyspace>/<source_table>/reconciliation/` and deletes
  those whose `report.json` was last modified earlier than (CLI start UTC) − N
  days.
- A subdirectory without a `report.json` (an in-flight or aborted run) is
  skipped with a `WARN` line.
- On a clean run the CLI prints a summary line with the counts of
  subdirectories deleted, skipped, and failed, for example:

  ```
  reconciliation cleanup summary: deleted=3 skipped=1 failed=0
  ```

- If the reconciliation prefix does not exist or contains no subdirectories,
  the CLI exits zero and prints a single `INFO` line stating that
  `no reconciliation runs were found`.

## License
This tool licensed under the Apache-2 License. See the LICENSE file.