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

5. Configure CQLReplicator:

   * Configure the source and target tables in `config.properties`
   * Configure migrated columns in `config.properties`
   * Configure write time of regular columns to detect updates in the source table in `config.properties`
   * Configure the total number of tiles in `config.properties`
   * Create a S3 bucket to store configuration files in `config.properties`
   * Configure connectivity to the source Cassandra cluster `CassandraConnector.conf`
   * Configure connectivity to the target Keyspaces table `KeyspacesConnector.conf`
   * Copy all artifacts to s3://your-bucket-name/keyspacename/tablename

6. Deploy the CQLReplicator cluster:

Initiate and deploy the service and deploy the test environment,

   ```bash
   copilot svc init --name cql-replicator-service
   copilot env deploy --name test
   copilot svc deploy --name cql-replicator-service --env test
   ```

### Usage

CQLReplicator will immediately start replicating the Cassandra workload when all ECS tasks are running.
Before restarting or updating the ECS tasks delete `tiles` with the locking files from `s3://bucket/keyspace_name/table_name`.

#### Define the number of tiles

Let's say you want to migrate a Cassandra cluster with 10B primary keys (1KB per primary key), the Cassandra cluster 
has 64 vnodes and 32 nodes. So that you can spin up to 2048 ecs tasks (tiles). 
Each ecs task will handle up to 5M primary keys. 

You can avoid high ECS costs by replicating the historical workload with AWS Glue and after
enable `ENABLE_REPLICATION_POINT` to handle only new workload after specific timestamp.

#### Restarting the ECS cluster

After restarting the ECS cluster, the CQLReplicator instances will start replicating
from a scratch. To void losing previously replicated workload you might use two workarounds:
* Use Amazon EFS instead of ephemeral storage in `manifest.yml` e.g, 
   ```yaml
   storage:
     volumes:
       myManagedEFSVolume:
        efs: true
        path: /var/efs
        read_only: false
   ```
  and set `LOCAL_STORAGE_PATH` to `/var/efs` in `config.properties`
* Set `ENABLE_REPLICATION_POINT` to `true` and `STARTING_REPLICATION_TIMESTAMP` to the most recent timestamp
  in `config.properties`.

#### Avoid the high CPU utilization of the Cassandra cluster

CQLReplicator might add a CPU overhead due to pooling data periodically.If the CPU utilization is over 
70% you can increase `POOLING_PERIOD` from 1 second to a higher value and scale down 
the CQLReplicator instances by reducing `REPLICATE_WITH_CORE_POOL_SIZE` and
`REPLICATE_WITH_MAX_CORE_POOL_SIZE` and decrease the number of CPUs per ECS task. 
Finally, you might deploy another Cassandra DC to shift your migration workload to it.

### Cleanup

Delete the CQLReplicator ECS cluster:

   ```bash
   copilot svc delete --name cql-replicator-service
   copilot app delete --name cql-replicator
   ```

### License

This tool licensed under the Apache-2 License. See the LICENSE file.