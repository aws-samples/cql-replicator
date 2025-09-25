### Replicate workload to Amazon OpenSearch
In order to connect to Amazon OpenSearch you need to prepare the connectivity file `/conf/CassandraConnector.conf`, 
`/conf/KeyspacesConnector.conf`, and `/conf/OpenSearchConnector.conf`.

The following command will create Glue connector to C* (optional for Amazon Keyspaces) uses `subnet-XXXXXXX`, `us-east-1a`, and `sg-1`, and
Glue connector to Amazon OpenSearch Service uses `subnet-YYYYYY`, `sg-2`, and `us-east-1a`:

```shell
cqlreplicator --cmd init --sg '"sg-1"' \
                           --subnet "subnet-XXXXXXX" 
                           --region "us-east-1" \ 
                           --az "us-east-1a" \
                           --glue-iam-role "glue-oss-replication" \
                           --target-type opensearch \
                           --target-sg "sg-2" \ 
                           --target-subnet "subnet-YYYYYY" \
                           --target-az "us-east-1a" \
                           --env opensearch 
```

After the init phase is completed you can run the following command to start the replication process:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 20000 --landing-zone s3://your-s3-bucket --region us-east-1 \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \
                 --writetime-column col2 \
                 --json-mapping '
                     {"replication": {
                      "allColumns": false,
                      "ttlAddOn": {
                        "enabled": false,
                        "predicateOp": "greaterThan",
                        "predicateVal": 900000
                      },
                      "pointInTimeReplicationConfig": {
                        "predicateOp": "greaterThan"
                      },
                      "columns":["pk", "cc", "col1", "col2"],
                      "useCustomSerializer": false,
                      "useMaterializedView": {
                        "enabled": false,
                        "mvName": "mvName"
                      }
                    },
                      "opensearch": {
                        "resource": "aos-index-name",
                        "enableDeletes": false,
                        "maxFileSizeMb": 32,
                        "transformation": {
                          "enabled": true,
                          "fieldsMapping": [
                            {"source": "pk", "target": "field1", "sourceType": "string", "targetType":  "string"},
                            {"source": "cc", "target": "field2", "sourceType": "integer", "targetType":  "integer"},
                            {"source": "col1", "target": "field3", "sourceType":  "string", "targetType":  "string"},
                            {"source": "col2", "target": "field4", "sourceType":  "string", "targetType":  "string"}
                        ]}
                      }'
```
In the below example the CQLReplicator replicates by following the instructions in the json mapping. Source columns
pk, cc, col1, and col2 will be renamed to field1, field2, field3, and field4 respectively.

In order to enable deletes set `enableDeletes` to `true` in the json mapping, but deletes are disabled
by default in Amazon OpenSearch. To enable this functionality for the CQLReplicator, you
need to update the configuration by sending the following request:
```
PUT _plugins/_query/settings
{
  "transient": {
    "plugins.sql.delete.enabled": "true"
  }
}
```

## License
This tool licensed under the Apache-2 License. See the LICENSE file.
