# The CQLReplicator and Amazon ECS
The CQLReplicator is designed to run within an Amazon ECS. 
Amazon ECS is a fully managed container orchestration service makes it easy for you to deploy, manage,
 and scale the migration process. Each ECS container handles a single subset of primary keys.  

## Build the project
```
    mvn install package
```
## Copy the archive and config files to the S3 bucket
Let's create a S3 bucket cqlreplicator with prefix /ks_test_cql_replicator/test_cql_replicator.
Copy CassandraConnector.conf, KeyspacesConnector.conf, and config.yaml to ```s3://cqlreplicator/ks_test_cql_replicator/test_cql_replicator```.

## Build and push the docker image to the ECS repository
Copy CQLReplicator-1.0-SNAPSHOT.zip to ```s3://cqlreplicator/```. 
Set environmental variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, 
`BUCKETNAME=cqlreplicator`, `KEYSPACENAME=ks_test_cql_replicator`, `TABLENAME=test_cql_replicator`, 
and `CQLREPLICATOR_CONF`.
Let's build the docker image `docker build -t cqlreplicator:latest --build-arg AWS_REGION="us-east-1" \
--build-arg AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID --build-arg AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
--build-arg AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN .`.
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
Before provision tables let's create the internal CQLReplicator tables, and the target table:
```java -cp CQLReplicator-1.0-SNAPSHOT.jar com.amazon.aws.cqlreplicator.Init```

After tables created let's provision the internal CQLReplicator tables in cqlsh:
```
ALTER TABLE replicator.ledger_v4 
    WITH CUSTOM_PROPERTIES={'capacity_mode':
    {'throughput_mode': 'PROVISIONED', 'read_capacity_units': 5000, 'write_capacity_units': 15000}};
```
after provision the target table in cqlsh:
```
ALTER TABLE ks_test_cql_replicator.test_cql_replicator 
    WITH CUSTOM_PROPERTIES={'capacity_mode':
    {'throughput_mode': 'PROVISIONED', 'read_capacity_units': 15000, 'write_capacity_units': 15000}};
```  
## Run the CQLReplicator on ECS cluster

To start the ECS cluster you can use keyspaces_migration.sh with parameters TILES ACCOUNT TASK_ROLE ECS_ROLE S3_BUCKET KEYSPACE_NANE TABLE_NAME SUBNETS VPC SG KEYPAIR.
The following example starts Amazon ECS cluster with 16 CQLReplicator's instances (16 tiles): 
```
keyspaces_migration.sh 16 123456789012 ecsTaskExecutionRole ecsRole cqlreplicator ks_test_cql_replicator test_cql_replicator subnets vpc-id sg keypair_name
```
## Check ECS logs
if you want to get all WARNs and ERRORs from ECS execute get_ecs_logs.sh with the cluster name, and an absolute path to the pem file
```
  get_ecs_logs.sh test_cql_replicator 
``` 

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