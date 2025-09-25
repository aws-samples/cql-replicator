### Replicate workload to Amazon S3
You can use the CQLReplicator to replicate the workload from Amazon Keyspaces to Amazon S3, the following init command
deploys the CQLReplicator for Amazon S3.

```shell
cqlreplicator --cmd init --sg '"sg-1"' \
                           --subnet "subnet-XXXXXXX" 
                           --region "us-east-1" \ 
                           --az "us-east-1a" \
                           --glue-iam-role "glue-s3-replication" \
                           --target-type parquet \
                           --env parquet
```
The following run command starts 4 tiles, and replicate the source table `test_cql_replicator`
to `s3://cql-replicator-your-backup/snapshots/keyspace_prefix/table_prefix/[0-7]/[insert|update|delete]/YYYY-MM-HHTMM:SS:ss.sss`.

```shell
./cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone "s3://your-bucket-ver5" --env parquet \
                --ttl-column col2 --region us-east-1 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator --trg-keyspace keyspace_prefix  --trg-table table_prefix --writetime-column col1 --json-mapping '
                {"replication": {
                        "allColumns": true,
                        "ttlAddOn": {
                          "enabled": false,
                          "predicateOp": "lessThan",
                          "predicateVal": 900000
                        },
                        "pointInTimeReplicationConfig": {
                          "predicateOp": "greaterThan"
                        },
                        "columns":[""],
                        "useCustomSerializer": false,
                        "useMaterializedView": {
                          "enabled": false,
                          "mvName": "mvName"
                        }
                    },
                    "s3": {
                      "bucket":"cql-replicator-backup",
                      "prefix": "snapshots",
                      "maxFileSizeMb": 32
                  }}'
```
You can specify the S3 bucket name and prefix to store the parquet files. Moreover, you might want to replicate within a specific
timestamp and filter out rows based the TTL parameter in `ttlAddOn` section.


## License
This tool licensed under the Apache-2 License. See the LICENSE file.
