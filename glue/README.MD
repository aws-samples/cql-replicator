
# Architecture
![](../architecture-glue.png "CQLReplicator on Glue")

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
