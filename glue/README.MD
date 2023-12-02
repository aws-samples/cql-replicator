
# Architecture
![](../architecture-glue.png "CQLReplicator on Glue")

# Init migration
```
    cqlreplicator --state init --sg '"sg-1","sg-2"' \ 
                  --subnet "subnet-XXXXXXXXXXXX" --az us-west-2a --region us-west-2 \ 
                  --glue-iam-role glue-cassandra-migration --landing-zone s3://aws-glue-assets-09123456789-us-west-2
```

# Start migration process
```
   cqlreplicator --state run --tiles 5 --landing-zone s3://migration-landing-zone --writetime-column col3  \
                 --ttl-column None --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator
```

# Stop migration process
```
   cqlreplicator --state request-stop --landing-zone s3://migration-landing-zone \ 
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator
```
