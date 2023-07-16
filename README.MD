## CQLReplicator

The aim of this project is to assist customers is seamlessly migrating from self-managed Cassandra to Amazon Keyspaces,
ensuring zero downtime. This means that customers can replicate their existing Cassandra workload in to Amazon Keyspaces,
without needing to make any changes to their client-side code or using proxies.

### Architecture

Customers have the flexibility to scale the migration workload by deploying multiple instances of CQLReplicator. Each instance
of CQLReplicator is a java process responsible for handling specific token range (tile). In the event of a CQLReplicator failure,
customers can simply restart the migration process from the point where it was interrupted by restarting the failed 
CQLReplicator instances.

### Prerequisites

List any prerequisites needed to run CQLReplicator, such as:

- AWS account
- Docker
- ECS Copilot CLI

### Getting Started

 Set up and deploy CQLReplicator using [AWS ECS Copilot](https://aws.github.io/copilot-cli/docs/getting-started/install/). Include the following steps:

### Configuration

- Set TILES to the number of tiles that should be equal to the number of ECS tasks
- Create a S3 bucket `cqlreplicator` and set it to BUCKETNAME. The bucket stores the artifacts: 
  `config.properties`, `KeyspacesConnector.conf`, and `CassandraConnector.conf` e.g, s3:/bucketname/keyspace_name/table_name
- Set KEYSPACENAME to the keyspace name (s3 prefix)
- Set TABLENAME to the table name (s3 prefix)

1. Clone the repository:

   ```bash
   git clone https://github.com/aws-samples/cql-replicator
   ```

2. Navigate into the project directory:

   ```bash
   cd cql-replicator
   ```

3. Build CQLReplicator:

   ```bash
   gradle build
   gradle task deploy
   ```

4. Initialize the ECS Copilot environment:

Configure the required resources, such as VPC, S3 buckets etc., using the prompts provided by the `copilot init` command.

   ```bash
   copilot app init cql-replicator
   ```

5. Configure CQLReplicator in `config.properties`:

   1. Configure the source and target tables
   2. Configure migrated columns
   3. Configure writetime regular columns to detect updates in the source table
   4. Configure the total number of tiles
   5. Create a S3 bucket to store configuration files
   6. Configure connectivity to the source Cassandra cluster `CassandraConnector.conf`
   7. Configure connectivity to the target Keyspaces table `KeyspacesConnector.conf`
   8. Copy all artifacts to s3://your-bucket-name/keyspacename/tablename

6. Deploy the CQLReplicator cluster:

Choose the backend service and name it `cql-replicator-service`

   ```bash
   copilot svc init --name cql-replicator-service
   copilot env deploy --name test
   copilot svc deploy --name cql-replicator-service --env test
   ```


### Usage

CQLReplicator will immediately start replicating the Cassandra workload when all ECS tasks are running.

### Cleanup

Delete the CQLReplicator ECS cluster:

   ```bash
   copilot svc delete --name cql-replicator-service
   copilot app delete --name cql-replicator
   ```

### License

This tool licensed under the Apache-2 License. See the LICENSE file.