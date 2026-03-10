# Replicate workload to Amazon S3

## Prepare the configuration files
To connect to Amazon Keyspaces (used for the internal ledger) and the Cassandra cluster, configure
the reference configuration files `../glue/conf/CassandraConnector.conf` and `../glue/conf/KeyspacesConnector.conf`
in ../conf accordingly to the environment.

## Init migration process
The following command initializes the CQLReplicator environment for S3/Parquet target, which involves copying JAR artifacts,
creation of a Glue connector, a S3 bucket, a Glue job, migration keyspace, and ledger table.

```shell
    cd cql-replicator/glue/bin &&
    cqlreplicator --cmd init \
                  --subnet "subnet-00a00b00" \
                  --sg '"sg-0e0b0a000000d000b"' \
                  --az "us-east-1a" \
                  --region us-east-1 \
                  --glue-iam-role GlueRole \
                  --env parquet \
                  --target-type parquet
```

After running the above command, you should find in the AWS account:

1. CQLReplicator-parquet Glue job and Glue connector in AWS Glue console
2. Amazon S3 bucket that stores the artifacts
3. Keyspace `migration` and `ledger` table in Amazon Keyspaces (used internally for tracking replication progress)

- `--sg` - security group(s) which allow access to the Cassandra cluster from AWS Glue and includes the
  self-referencing inbound rule for all traffic
- `--subnet` - the subnet to which the Cassandra cluster belongs
- `--az` - the AZ of the subnet
- `--region` - AWS region where the Cassandra cluster is deployed
- `--glue-iam-role` - the [IAM role permissions](https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html)
  that AWS Glue can assume when calling other services on your behalf. Services: Amazon Keyspaces, S3, and AWS Glue Data Catalog
- `--landing-zone` - optional parameter, you can reuse any existing S3 buckets in the AWS account.
  If you don't supply it, init process will try to create a new bucket
- `--target-type` - must be set to `parquet`
- `--skip-glue-connector` - optional parameter to skip the Glue connector creation step
- `--skip-keyspaces-ledger` - optional parameter to skip creation/deletion of the ledger table in Amazon Keyspaces
- `--env` - use to create multiple environments, e.g. dev/test/prod

### VPC Endpoint Requirements

CQLReplicator uses Apache Iceberg with the AWS Glue Data Catalog for snapshot management. The Glue job runs inside a VPC, so the following VPC endpoints are required:

| Endpoint | Type | Purpose |
|----------|------|---------|
| `com.amazonaws.{region}.glue` | Interface | Iceberg catalog operations (CREATE TABLE, getTable, etc.) |
| `com.amazonaws.{region}.s3` | Gateway | Iceberg data/metadata and Parquet output read/write in S3 |

The Glue interface endpoint must have:
- `PrivateDnsEnabled` = `true`
- The VPC must have `enableDnsSupport` = `true` and `enableDnsHostnames` = `true`
- The endpoint's security group must allow inbound TCP port 443 from the Glue job's security group

## Architecture

CQLReplicator uses Apache Iceberg for internal change detection and writes the resulting inserts, updates, and deletes to S3 as Parquet files.

```
Cassandra ──► Discovery Process ──► Per-tile Iceberg snapshots (Glue Data Catalog + S3)
                                          │
                                          ▼
                                    Replication Process ──► Reads full rows from Cassandra
                                          │                       │
                                          ▼                       ▼
                                    Change detection        JSON buffer (FlushingSet)
                                    (Iceberg time-travel)         │
                                                                  ▼
                                                            Parquet files on S3
```

The discovery process reads primary keys from Cassandra, groups them into tiles using `xxhash64`, and writes per-tile Iceberg snapshots. The replication process detects changes between consecutive snapshots (inserts, updates, deletes), reads the full rows from Cassandra, and writes them to S3 as Parquet files.

## Start migration process

```shell
   cqlreplicator --cmd run \
                 --target-type parquet \
                 --landing-zone s3://<BUCKET_NAME> \
                 --region us-east-1 \
                 --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --writetime-column col1 \
                 --env parquet \
                 --json-mapping '{
                   "s3": {
                     "bucket": "my-parquet-bucket",
                     "prefix": "snapshots",
                     "maxFileSizeMb": 32
                   }
                 }'
```

Output Parquet files are written to:
```
s3://<S3_BUCKET>/snapshots/ks/tbl/<tile>/<operation>/<timestamp>/
```
Where `operation` is `insert`, `update`, or `delete`.

The CQLReplicator offers three options for operation:
1. For high-volume traffic:
    * Use the command: `--tiles 4` (default)
    * This setting uses 4 tiles (Glue jobs)
2. For lower-volume traffic:
    * Use the command: `--tiles 1`
    * This setting customizes the number of tiles to 1
3. For custom configurations:
    * Use the command: `--tiles 12`
    * Note: Changing the number of tiles requires resizing the migration process

### Replicate near-real time updates, inserts, and deletes

To capture updates in addition to inserts and deletes, specify a column that is constantly updated using `--writetime-column`:

```shell
   cqlreplicator --cmd run --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --writetime-column data \
                 --env parquet --target-type parquet \
                 --json-mapping '{
                   "replication": {"allColumns": true},
                   "s3": {"bucket": "my-parquet-bucket", "prefix": "snapshots", "maxFileSizeMb": 32}
                 }'
```

### Track changes across multiple columns

The `--writetime-column` parameter accepts multiple comma-separated column names. When multiple columns are specified,
CQLReplicator computes the maximum `WRITETIME()` across all specified columns to detect the most recent mutation.

Single column:
```shell
   --writetime-column col1
```

Multiple columns:
```shell
   --writetime-column col1,col2,col3
```

When multiple columns are specified, CQLReplicator generates `greatest(writetime(col1), writetime(col2), ...) as ts`
in the CQL query. Cassandra's `WRITETIME()` returns null for columns that were never explicitly written. The `greatest()`
function excludes null values and returns the maximum of the non-null writetimes.

### Change the workload type

To replicate only the historical workload and release Glue resources once completed, add `--workload-type batch`:

```shell
   cqlreplicator --cmd run --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env parquet --target-type parquet --workload-type batch \
                 --json-mapping '{
                   "replication": {"allColumns": true},
                   "s3": {"bucket": "my-parquet-bucket", "prefix": "snapshots", "maxFileSizeMb": 32}
                 }'
```

### Replicate with TTL filtering

Filter rows based on TTL values before writing to S3. Configure the `ttlAddOn` section in `--json-mapping` and specify the TTL column with `--ttl-column`:

```shell
   cqlreplicator --cmd run --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --ttl-column data \
                 --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env parquet --target-type parquet \
                 --json-mapping '{
                   "replication": {
                     "allColumns": true,
                     "ttlAddOn": {
                       "enabled": true,
                       "predicateOp": "greaterThan",
                       "predicateVal": 86400
                     }
                   },
                   "s3": {"bucket": "my-parquet-bucket", "prefix": "snapshots", "maxFileSizeMb": 32}
                 }'
```

Supported predicate operators: `greaterThan`, `lessThan`, `equal`.

### Point in time replication

To replicate data from a specific timestamp, add the `--start-replication-from` flag:

```shell
   cqlreplicator --cmd run --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env parquet --target-type parquet \
                 --start-replication-from 1706408365278858 \
                 --json-mapping '{
                   "replication": {"allColumns": true},
                   "s3": {"bucket": "my-parquet-bucket", "prefix": "snapshots", "maxFileSizeMb": 32}
                 }'
```

To replicate data *before* a timestamp, specify the predicate in json mapping:
```json
{
  "replication": {
    "pointInTimeReplicationConfig": {
      "predicateOp": "lessThanOrEqual"
    }
  }
}
```

## S3 Output Configuration

The `s3` section in `--json-mapping` controls where Parquet files are written:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bucket` | Landing zone bucket | Target S3 bucket for Parquet output |
| `prefix` | `parquet-storage` | S3 key prefix for Parquet files |
| `maxFileSizeMb` | `32` | Target Parquet file size in MB. Controls the JSON buffer flush threshold (legacy pipeline) and the coalesce target (bulk pipeline) |

If `bucket` is not specified, the landing zone bucket is used. The output path structure is:
```
s3://<bucket>/<prefix>/<trg-keyspace>/<trg-table>/<tile>/insert/<timestamp>/
s3://<bucket>/<prefix>/<trg-keyspace>/<trg-table>/<tile>/update/<timestamp>/
s3://<bucket>/<prefix>/<trg-keyspace>/<trg-table>/<tile>/delete/<timestamp>/
```

Insert and update files contain the full row data. Delete files contain only the primary key columns.

## Stop migration process

To stop migration process gracefully for all tiles:

```shell
   cqlreplicator --cmd request-stop --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks --src-table tbl --target-type parquet --env parquet
```

To stop a single tile:

```shell
   cqlreplicator --cmd request-single-stop --tile 1 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks --src-table tbl --target-type parquet --env parquet
```

To force an immediate termination:
```shell
   cqlreplicator --cmd kill --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks --src-table tbl --target-type parquet --env parquet
```

## Recover from failures

Restart the migration process from the point where it was interrupted by rerunning your `run` command with the same
parameters. If the discovery job is still running and some of the replicators have failed, use the extra flag
`--skip-discovery`.

## Cleanup the ledger

To reset the CQLReplicator for a specific table, add the `--cleanup-requested` or `--cr` flag to your `run` command.
This cleans up the internal CQLReplicator ledger table, the per-tile Iceberg tables from the Glue Data Catalog, and the S3 Iceberg data.
Don't forget to remove the flag if you want to stop and run it again.

## Get migration stats

To obtain the number of replicated rows during the back filling phase:

```shell
   cqlreplicator --cmd stats --landing-zone s3://<BUCKET_NAME> \
                 --src-keyspace ks --src-table tbl \
                 --target-type parquet --region us-east-1 --env parquet
```

To obtain the number of replicated rows after the back filling phase:

```shell
   cqlreplicator --cmd stats --landing-zone s3://<BUCKET_NAME> \
                 --src-keyspace ks --src-table tbl \
                 --target-type parquet --region us-east-1 --replication-stats-enabled --env parquet
```

## Resize the replication process

To modify the number of tiles (Glue jobs) during the replication process:
1. Safely stop the current migration process (do not force-stop running jobs)
2. Verify all Glue jobs show "COMPLETED" status
3. Run the resize command:

```shell
   cqlreplicator --cmd resize --tiles 8 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks --src-table tbl --target-type parquet --env parquet
```

After the resizing job completes, restart CQLReplicator with the new number of tiles `--tiles 8`.

## Retrieve logs from failed Glue jobs

```shell
   cqlreplicator --cmd get-failed-jobs --landing-zone s3://<BUCKET_NAME> \
                 --region us-east-1 --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl
```

## Configure log level

By default, CQLReplicator sets the Spark log level to `ERROR`. You can change this using the `--logging-level` flag:

```shell
   cqlreplicator --cmd run --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env parquet --target-type parquet --logging-level WARN \
                 --json-mapping '{
                   "replication": {"allColumns": true},
                   "s3": {"bucket": "my-parquet-bucket", "prefix": "snapshots", "maxFileSizeMb": 32}
                 }'
```

Available log levels: `ALL`, `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR` (default), `FATAL`, `OFF`.

## JSON Mapping Reference

The complete `--json-mapping` schema for the S3/Parquet target:

```json
{
  "replication": {
    "allColumns": true,
    "columns": [""],
    "useCustomSerializer": false,
    "useMaterializedView": {"enabled": false, "mvName": ""},
    "pointInTimeReplicationConfig": {"predicateOp": "greaterThan"},
    "ttlAddOn": {
      "enabled": false,
      "predicateOp": "greaterThan",
      "predicateVal": 0
    }
  },
  "s3": {
    "bucket": "my-parquet-bucket",
    "prefix": "snapshots",
    "maxFileSizeMb": 32
  }
}
```

## Known Issues

| Error Message/Issue | CW Discovery logs | CW Replication logs | Work around |
|---|---|---|---|
| No space left on device | Yes | No | Scale out by increasing the number of DPUs per discovery Glue job, e.g. `--odw 15` |
| No space left on device | Yes | No | Increase the number of tiles |
| Limited IPs in the subnet | Yes | Yes | Create a new Glue connection with a new subnet and attach to the CQLReplicator Glue Job |
| Cassandra timeout during read query at consistency ONE | Yes | No | Reduce page size and increase request timeout in CassandraConnector.conf |
| Cassandra timeout during read query at consistency ONE | Yes | No | Create a materialized view with the same primary keys and one regular column, configure CQLReplicator to use it |
| java.lang.OutOfMemoryError: GC overhead limit exceeded | Yes | No | Increase DPUs for the discovery job using `--override-discovery-workers 12` |

## Clean up

The following command will delete the Glue job, connector, the S3 bucket, and the ledger table:

```shell
   cqlreplicator --cmd cleanup --landing-zone s3://<BUCKET_NAME> --region "us-east-1"
```

To also clean up Iceberg databases and tables from the Glue Data Catalog:

```shell
   cqlreplicator --cmd cleanup --landing-zone s3://<BUCKET_NAME> --region "us-east-1" \
                 --src-keyspace-list "ks_source_1,ks_source_2"
```

## License
This tool licensed under the Apache-2 License. See the LICENSE file.
