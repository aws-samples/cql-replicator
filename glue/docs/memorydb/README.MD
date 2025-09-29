# Replicate workload to Amazon MemoryDB
In order to connect to Amazon MemoryDB you need configure `/conf/CassandraConnector.conf`, `/conf/KeyspacesConnector.conf`, 
and `/conf/RedisConnector.conf` and after run the following command:

## Init 
The following command initializes the CQLReplicator environment, which involves the copying JAR artifacts,
creation a Glue connector, a S3 bucket, a Glue job, migration keyspace, and ledger table.

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

```shell
cqlreplicator --cmd init --sg '"sg-1"' \
                           --subnet "subnet-XXXXXXX" 
                           --region "us-east-1" \ 
                           --az "us-west-1a" \
                           --glue-iam-role "glue-memorydb-replication" \
                           --target-type memorydb \
                           --target-sg "sg-2" \ 
                           --target-subnet "subnet-YYYYYY" \
                           --target-az "us-east-1a"
                           --env memorydb
```

## Run
To operate CQLReplicator on AWS Glue, you need to use `--cmd run` command, followed by a series of parameters. The
precise configuration of these is primarily determined by your unique migration requirements.

```shell
   cqlreplicator --cmd run --max-wcu-traffic 6000 --landing-zone s3://<PROVIDED_BUCKET_NAME> \ 
                 --region us-east-1 --target-type memorydb \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator --env memorydb
```
## Stop
To stop migration process gracefully run the following command for all tiles:

```shell
   cqlreplicator --cmd request-stop --landing-zone s3://<PROVIDED_BUCKET_NAME> \ 
                 --region us-east-1 --target-type memorydb \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator --env memorydb
```

## License
This tool licensed under the Apache-2 License. See the LICENSE file.
