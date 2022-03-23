### Build Docker image
`docker system prune -a`
`docker build -t cqlreplicator:latest --build-arg AWS_REGION="us-east-1" \
--build-arg AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID --build-arg AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
--build-arg AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN .`
### Run Docker containers with two partition key scanners
#### Starting tile 0
`docker run -it -p 9042:9042 --env BUCKETNAME=cqlreplicator --env KEYSPACENAME=ks_test_cql_replicator \
--env TABLENAME=test_cql_replicator --entrypoint cqlreplicator.sh cqlreplicator:latest --syncPartitionKeys \
--tile 0 --tiles 2`
#### Starting tile 1
`docker run -it -p 9042:9042 --env BUCKETNAME=cqlreplicator --env KEYSPACENAME=ks_test_cql_replicator \
--env TABLENAME=test_cql_replicator --entrypoint cqlreplicator.sh cqlreplicator:latest --syncPartitionKeys \
--tile 1 --tiles 2`
### Run Docker containers with two Cassandra row scanners
#### Starting tile 0
`docker run -it -p 9042:9042 --env BUCKETNAME=cqlreplicator --env KEYSPACENAME=ks_test_cql_replicator \
--env TABLENAME=test_cql_replicator --entrypoint cqlreplicator.sh cqlreplicator:latest --syncClusteringColumns \
--tile 0 --tiles 2`
#### Starting tile 1
`docker run -it -p 9042:9042 --env BUCKETNAME=cqlreplicator --env KEYSPACENAME=ks_test_cql_replicator \
--env TABLENAME=test_cql_replicator --entrypoint cqlreplicator.sh cqlreplicator:latest --syncClusteringColumns \
--tile 1 --tiles 2`