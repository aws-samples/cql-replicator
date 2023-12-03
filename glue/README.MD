# CQLReplicator with AWS Glue
The aim of this project is to assist customers is seamlessly migrating from self-managed large Cassandra clusters to Amazon Keyspaces,
ensuring zero downtime and code compilation, predictable incremental traffic, and low migration costs.

## Architecture
Customers have the flexibility to scale the migration workload up and out by deploying multiple Glue jobs of CQLReplicator. 
Each Glue job is responsible for handling a subset of primary keys. In the event of a glue failure,
customers can simply restart the migration process from the point where it was interrupted by restarting the failed jobs.
![](../architecture-glue.png "CQLReplicator on Glue")

## Prerequisites
List of prerequisites needed to run CQLReplicator with AWS Glue, such as:

- AWS account
- [AWS CloudShell](https://aws.amazon.com/cloudshell/)
- [AWS Glue](https://aws.amazon.com/glue/)
- [Amazon Keyspaces](https://aws.amazon.com/keyspaces/) (keyspace and tables)
- [Cassandra cluster](https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/deploy-a-cassandra-cluster-on-amazon-ec2-with-private-static-ips-to-avoid-rebalancing.html)
- [Reference](https://docs.datastax.com/en/developer/java-driver/4.3/manual/core/configuration/reference/) configuration 
files for Amazon Keyspaces and Cassandra Cluster

## Init migration process
The following command initializes the CQLReplicator environment: copies JAR artifacts, creates a Glue connector, 
a S3 bucket (optional), a Glue job, migration keyspace, and ledger table. 

To enable AWS Glue components to communicate, you must set up access to your Cassandra cluster in Amazon VPC. 
To enable AWS Glue to communicate between its components, specify a security group with a self-referencing inbound rule 
for all TCP ports. By creating a self-referencing rule, you can restrict the source to the same security group in the VPC, 
and it's not open to all networks. The default security group for your VPC might already have a self-referencing inbound rule 
for ALL Traffic.

`--sg` - a list of security group(s) provides access to Cassandra cluster form AWS Glue and contains the self-referencing 
inbound rule for ALL Traffic.
`--subnet` - the Cassandra cluster's subnet
`--az` - the subnet's availability zone
`--region` - AWS Region where the Cassandra cluster is deployed 
`glue-iam-role` - you need [the IAM role permissions](https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html)
that AWS Glue can assume when calling other services on your behalf. Services: Amazon Keyspaces and S3
`--landing zone` - this is an optional parameter because can reuse any existing S3 buckets. 
If you don't supply it, init process will try to create a new bucket to store config, JAR artifacts, and intermediate files

```shell
    cqlreplicator --state init --sg '"sg-1","sg-2"' \ 
                  --subnet "subnet-XXXXXXXXXXXX" --az us-west-2a --region us-west-2 \ 
                  --glue-iam-role glue-cassandra-migration --landing-zone s3://cql-replicator-1234567890-us-west-2
```
After running this command, you should find in the AWS account:
1. CQLReplicator Glue job and Glue connector in AWS Glue
2. Amazon S3 bucket that stores the artifacts
3. Keyspace `migration` and `ledger` table in Amazon Keyspaces

## Start migration process
In order, to run CQLReplicator on AWS Glue, you need to use `--state run` with a different variety of parameters.
The variety of parameters will depend on your migration use-case, e.g. replicate ttl, update, or objects over 1MB.

### Getting Started
Let's run the following command to replicate the workload from the Cassandra cluster to Amazon Keyspaces.
Your source keyspace and table `ks_test_cql_replicator.test_cql_replicator` in the Cassandra cluster.
Your target keyspace and table `ks_test_cql_replicator.test_cql_replicator` in Amazon Keyspaces.
Parameter `--inc-traffic` - enables incremental traffic to not overload the Cassandra Cluster and Amazon Keyspaces
with high number of requests. CQLReplicator is going to increase traffic every 2 minutes. In the following
example with 8 tiles, the traffic is going to be increased the traffic by 12.5% every 2 minutes,

```shell
   cqlreplicator --state run --tiles 8 --landing-zone s3://cql-replicator-1234567890-us-west-2 \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator --inc-traffic
```

### Replicate near-real time updates and inserts
```shell
   cqlreplicator --state run --tiles 8 --landing-zone s3://cql-replicator-1234567890-us-west-2 --writetime-column col3  \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \ 
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator --inc-traffic
```

### Replicate with TTL
the TTL feature should be [enabled](https://docs.aws.amazon.com/keyspaces/latest/devguide/TTL-how-it-works.html#ttl-howitworks_enabling) 
on the target table before running the following command
```shell
   cqlreplicator --state run --tiles 8 --landing-zone s3://cql-replicator-1234567890-us-west-2 --writetime-column col3  \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator --ttl-column col3 \ 
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator --inc-traffic
```

### Offload large objects
Before running the migration process configure [lifecycle](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html)
for the S3 bucket to delete objects after expiration.
```shell
./cqlreplicator --state run --tiles 8 --landing-zone s3://cql-replicator-1234567890-us-west-2 --src-keyspace ks_test_cql_replicator 
                --src-table test_cql_replicator --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \ 
                --ttl-column col3 --offload-large-objects '{"column":"col1","bucket":"my-application-resource",
                                          "prefix":"ks_test_cql_replicator/test_cql_replicator/col1","xref":"link"}'
```

## Stop migration process
To stop migration process gracefully run the following command:
```shell
   cqlreplicator --state request-stop --landing-zone s3://cql-replicator-1234567890-us-west-2 \ 
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator
```

## Recover from failures
In order, to restart CQLReplicator on AWS Glue, you need to rerun `--state run` with the parameters.

## Get migration stats
To get replicated row stats, run the following command:
```shell
   cqlreplicator --state stats --landing-zone s3://cql-replicator-1234567890-us-west-2 \ 
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator
```

## Clean up
The following command will delete the Glue job, connector, the S3 bucket, and Keyspaces'table ledger:

```shell
   cqlreplicator --state cleanup --landing-zone s3://cql-replicator-1234567890-us-west-2
```
