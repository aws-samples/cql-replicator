# Docker and CQLReplicator

The below example will demonstrate how to use Docker and CQLReplicator

## Build Docker image
`docker system prune -a`
`docker build -t cqlreplicator:latest --build-arg AWS_REGION="us-east-1" \
--build-arg AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID --build-arg AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
--build-arg AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN .`

## Run partition discovery processes in Docker
### Starting tile 0
`docker run -it -p 9042:9042 --env BUCKETNAME=cqlreplicator --env KEYSPACENAME=ks_test_cql_replicator \
--env TABLENAME=test_cql_replicator --entrypoint cqlreplicator.sh cqlreplicator:latest --syncPartitionKeys \
--tile 0 --tiles 2`
### Starting tile 1
`docker run -it -p 9042:9042 --env BUCKETNAME=cqlreplicator --env KEYSPACENAME=ks_test_cql_replicator \
--env TABLENAME=test_cql_replicator --entrypoint cqlreplicator.sh cqlreplicator:latest --syncPartitionKeys \
--tile 1 --tiles 2`

## Run row discovery processes in Docker
### Starting tile 0
`docker run -it -p 9042:9042 --env BUCKETNAME=cqlreplicator --env KEYSPACENAME=ks_test_cql_replicator \
--env TABLENAME=test_cql_replicator --entrypoint cqlreplicator.sh cqlreplicator:latest --syncClusteringColumns \
--tile 0 --tiles 2`
### Starting tile 1
`docker run -it -p 9042:9042 --env BUCKETNAME=cqlreplicator --env KEYSPACENAME=ks_test_cql_replicator \
--env TABLENAME=test_cql_replicator --entrypoint cqlreplicator.sh cqlreplicator:latest --syncClusteringColumns \
--tile 1 --tiles 2`