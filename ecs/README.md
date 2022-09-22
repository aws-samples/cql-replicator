# The CQLReplicator and Amazon ECS
The CQLReplicator is designed to run within an Amazon ECS. 
Amazon ECS is a fully managed container orchestration service makes it easy for you to deploy, manage,
 and scale the migration process. Each ECS container handles a single subset of primary keys.  

## Build the project
```
    gradle clean build
    gradle task deploy
```
## Deploy Amazon ElastiCache (memcached)
Following, this [instruction](https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/deploy-cluster.html), you can deploy the ElastiCache cluster.

## Copy the config and conf files to the S3 bucket
Let's create a S3 bucket cqlreplicator with prefix /ks_test_cql_replicator/test_cql_replicator.
Copy CassandraConnector.conf, KeyspacesConnector.conf, and config.properties to ```s3://cqlreplicator/ks_test_cql_replicator/test_cql_replicator```.

## Build and push the docker image to the ECS repository
Let's build the docker image for x86 Linux `docker build -f docker/Dockerfile -t cqlreplicator:latest --build-arg AWS_REGION="us-east-1" \
--build-arg AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID --build-arg AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
--build-arg AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN .`.

or you can build the docker image for [ARM64 Linux](https://docs.docker.com/desktop/multi-arch/) `docker buildx build --platform linux/arm64 --load -f docker/Dockerfile -t cqlreplicator:latest --build-arg AWS_REGION="us-east-1" \
 --build-arg AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID --build-arg AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY --build-arg AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN .`.

Retrieve an authentication token and authenticate your Docker client to your registry.
Use the AWS CLI: ```aws ecr get-login-password --region us-east-1 | docker login --username AWS 
--password-stdin 123456789012.dkr.ecr.us-east-1.amazonaws.com```.

After the build completes, tag your image, so you can push the image to this repository:
```docker tag cqlreplicator:latest 123456789012.dkr.ecr.us-east-1.amazonaws.com/cqlreplicator:latest```
Run the following command to push this image to your newly created AWS repository:
```docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/cqlreplicator:latest```

## Create ECS Task Execution Role
You must create an IAM policy for your tasks to use that specifies the permissions that 
you would like the containers in your tasks to have. You have several ways to create 
a new IAM permission policy. You can copy a complete AWS managed policy that already 
does some of what you're looking for and then customize it to your specific requirements. Add
an inline policies to access Amazon S3 and Keyspaces. 
[ECS task execution role](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html)  

## Create ECS Role
Amazon ECS container instances, including both Amazon EC2 and external instances, 
run the Amazon ECS container agent and require an IAM role for the service to know that the agent 
belongs to you. Before you launch container instances and register them to a cluster, 
you must create an IAM role for your container instances to use. The Amazon ECS instance role is 
automatically created for you when completing the Amazon ECS console first-run experience. 
However, you can manually create the role and attach the managed IAM policy for container instances 
to allow Amazon ECS to add permissions for future features and enhancements as they are introduced. 
Use the following procedure to check and see if your account already has the Amazon ECS container 
instance IAM role and to attach the managed IAM policy if needed.
[ECS role](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/instance_IAM_role.html)

## Define the number of tiles
Best practice is to define the number of tiles equal to the vnodes in the Cassandra cluster, for example,
16 vnodes is equal to 16 tiles, 32 vnodes is equal to 32 tiles, etc.

## Change Amazon Keyspaces tables capacity mode
Let's create and pre-warm CQLReplicator the target table in cqlsh:

```
CREATE TABLE replicator.stats (
    tile int,
    keyspacename text,
    tablename text,
    ops text,
    "rows" counter,
    PRIMARY KEY ((tile, keyspacename, tablename, ops))
)
```

Create and provision the target table test_cql_replicator:
```
CREATE TABLE ks_test_cql_replicator.test_cql_replicator (
    key uuid,
    col0 tinyint,
    col1 text,
    col2 date,
    col3 double,
    col4 int,
    col5 bigint,
    col6 timestamp,
    col7 float,
    col8 blob,
    PRIMARY KEY (key, col0)
) WITH default_time_to_live = 0 AND CUSTOM_PROPERTIES = {
  	'capacity_mode':{
  		'throughput_mode':'PROVISIONED',
  		'write_capacity_units':30000,
  		'read_capacity_units':10000
  	}
  } AND  CLUSTERING ORDER BY (col0 ASC)
```

```
ALTER TABLE ks_test_cql_replicator.test_cql_replicator 
    WITH CUSTOM_PROPERTIES = {'capacity_mode':{ 'throughput_mode':'PAY_PER_REQUEST'}}
```  

## Run the CQLReplicator on ECS cluster

To start the ECS cluster you can use keyspaces_migration.sh with parameters TILES ACCOUNT TASK_ROLE ECS_ROLE S3_BUCKET KEYSPACE_NANE TABLE_NAME SUBNETS VPC SG KEYPAIR.
The following example starts Amazon ECS cluster with 16 CQLReplicator's instances (16 tiles): 
```
keyspaces_migration.sh 16 123456789012 ecsTaskExecutionRole ecsRole cqlreplicator ks_test_cql_replicator test_cql_replicator subnets vpc-id sg keypair_name
```
## Check ECS logs
ECS logs are available in `Amazon CloudWatch/Log groups/test_cq_replicator`

## Clean up
After you completed the migration process stop the ECS cluster, drop the internal CQLReplicator tables, and 
unregister ECS tasks.

### Stop the ECS cluster
if you want to stop the cluster execute stop_ecs_cluster.sh within the cluster name and the region
```
 stop_ecs_cluster.sh test_cql_replicator region
```
### De-register tasks 
```
 deregister_ecs_task.sh CQLReplicator 16
```
## Cost consideration
Originally the project built for m6i.large VMs, but AWS introduced a new EC2 A1.
Amazon EC2 A1 instances deliver significant cost savings for scale-out and Arm-based applications such as CQLReplicator, 
and distributed data stores that are supported by the extensive Arm ecosystem. A1 instances are the first EC2 instances 
powered by AWS Graviton Processors that feature 64-bit Arm Neoverse cores and custom silicon designed by AWS. 
These instances will also appeal to developers, enthusiasts, and educators across the Arm developer community. 
Most architecture-agnostic applications that can run on Arm cores could also benefit from A1 instances.  