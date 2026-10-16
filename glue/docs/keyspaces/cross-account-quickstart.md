# Cross-account replication: step-by-step deployment

This procedure deploys CQLReplicator across two AWS accounts and replicates one
Amazon Keyspaces table. For the full background, read [cross-account-replication.md](cross-account-replication.md).

**Names used in this procedure.** Replace them with your values.

| Item | Value                                        |
|---|----------------------------------------------|
| Source account (has the data) | `XXXXXXXXXXXX`                               |
| Target account (runs CQLReplicator) | `YYYYYYYYYYYY`                               |
| Region | `us-east-1`                                  |
| Keyspace and table | `ks_test_cql_replicator.test_cql_replicator` |
| Glue job role, in the target account | `GlueKeyspacesRole`                          |
| Source-read role, in the source account | `CQLReplicatorSourceRole`                    |
| Environment name | `dev`                                        |

The tool runs in the **target** account. It reads from the source account
through one IAM role. All steps say which account to use.

> **Use the region of the source table for all target resources.** Create the
> VPC endpoints, the Glue job, and the target table in the same region as the
> source table. A VPC endpoint serves only its own region. If the job runs in a
> different region, it cannot reach the source without a NAT path, and the job
> stays in `RUNNING` with no clear error.
>
> Set the region once, and pass it to every `aws ec2` and `aws glue` command.
> If you do not, the AWS CLI uses its default region. A Gateway endpoint
> request in the wrong region fails with
> `Endpoint type (Gateway) does not match available service types ([Interface])`.
>
> ```bash
> export AWS_REGION=us-east-1 R=us-east-1
> ```

---

## Phase 1 — Prepare the target account

Do the steps of this phase in the **target** account.

### Step 1. Make sure that the account can start Glue jobs

A new AWS account cannot always start Glue job runs immediately. Each
`StartJobRun` call then fails with `ResourceNumberLimitExceededException`,
although all quotas show unused capacity.

Do this test. The test needs a role that Glue can assume. If the account has no
such role yet, do Step 2 first and use `GlueKeyspacesRole`.

### Step 2. Create the Glue job role

Create the role **before** you touch the source account. IAM validates the
principal ARN of a trust policy at creation time. A trust policy that names a
role that does not exist fails with `MalformedPolicyDocument: Invalid principal`.

```bash
aws iam create-role --role-name GlueKeyspacesRole \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"glue.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam attach-role-policy --role-name GlueKeyspacesRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
```

### Step 3. Grant data access to the Glue job role

For a test account, a wide grant is acceptable:

```bash
aws iam put-role-policy --role-name GlueKeyspacesRole --policy-name CQLReplicatorDataAccess \
  --policy-document '{"Version":"2012-10-17","Statement":[
    {"Effect":"Allow","Action":["cassandra:Select","cassandra:Modify","cassandra:Create","cassandra:Alter","cassandra:Drop"],"Resource":"*"},
    {"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],"Resource":"*"}]}'
```

For production, scope the Cassandra actions to these keyspaces: the target
keyspace, the `migration` keyspace (the ledger), and `system*`. Scope the S3
actions to the landing-zone bucket and the `aws-glue-assets-<account>-<region>`
bucket.

> **Known check limitation.** The `init` permission check simulates the
> Cassandra actions against the account-wide resource `arn:aws:cassandra:...:*`.
> A keyspace-scoped policy then shows false ❌ lines. The lines are
> informational. Replication works with the scoped policy.

### Step 4. Grant the cross-account assume permission

```bash
aws iam put-role-policy --role-name GlueKeyspacesRole --policy-name AssumeCQLReplicatorSourceRole \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:AssumeRole","Resource":"arn:aws:iam::XXXXXXXXXXXX:role/CQLReplicatorSourceRole"}]}'
```

Both trust directions are necessary. This grant alone is not sufficient; Phase 2
adds the matching trust policy in the source account.

### Step 5. Create the security group

`init` requires a security group with a self-referencing inbound rule.

```bash
VPC_ID=$(aws ec2 describe-vpcs --region "$R" --filters Name=isDefault,Values=true \
  --query 'Vpcs[0].VpcId' --output text)

SG_ID=$(aws ec2 create-security-group --region "$R" --group-name cql-replicator-sg \
  --description "CQLReplicator Glue connection" --vpc-id "$VPC_ID" \
  --query GroupId --output text)

aws ec2 authorize-security-group-ingress --region "$R" --group-id "$SG_ID" \
  --protocol -1 --source-group "$SG_ID"
```

Select a subnet and record its Availability Zone:

```bash
aws ec2 describe-subnets --region "$R" --filters Name=vpc-id,Values="$VPC_ID" \
  --query 'Subnets[].[SubnetId,AvailabilityZone]' --output table
SUBNET_ID=<your choice>
AZ=<its availability zone>
```

### Step 6. Create the VPC endpoints

A Glue job that uses a VPC connection gets no public IP address. The job must
reach S3, STS, Glue, CloudWatch, and Keyspaces through VPC endpoints (or through
a NAT gateway — then skip this step).

```bash
RT_ID=$(aws ec2 describe-route-tables --region "$R" \
  --filters Name=vpc-id,Values="$VPC_ID" Name=association.main,Values=true \
  --query 'RouteTables[0].RouteTableId' --output text)

aws ec2 create-vpc-endpoint --region "$R" --vpc-id "$VPC_ID" --vpc-endpoint-type Gateway \
  --service-name "com.amazonaws.$R.s3" --route-table-ids "$RT_ID"

for svc in cassandra sts glue monitoring logs; do
  aws ec2 create-vpc-endpoint --region "$R" --vpc-id "$VPC_ID" --vpc-endpoint-type Interface \
    --service-name "com.amazonaws.$R.$svc" \
    --subnet-ids "$SUBNET_ID" --security-group-ids "$SG_ID" --private-dns-enabled
done
```

Notes:

- The `cassandra` endpoint serves both connections. With private DNS enabled,
  `cassandra.us-east-1.amazonaws.com` resolves to it. Authentication stays
  SigV4, so the cross-account read is not affected.
- The endpoint serves only its own region. The source table must be in the
  same region (see the note at the top of this procedure).
- `monitoring` is CloudWatch metrics. `logs` is CloudWatch Logs. Glue continuous
  logging needs `logs`.
- Keyspaces through a VPC endpoint needs two extra permissions on the Glue role:

```bash
aws iam put-role-policy --role-name GlueKeyspacesRole --policy-name KeyspacesVpceLookup \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["ec2:DescribeNetworkInterfaces","ec2:DescribeVpcEndpoints"],"Resource":"*"}]}'
```

Wait until all endpoints show `available`:

```bash
aws ec2 describe-vpc-endpoints --region "$R" \
  --query 'VpcEndpoints[].[ServiceName,State]' --output table
```

### Step 7. Create the target schema

Create the keyspace and the table with the same DDL as the source table. Copy
the DDL from the source account (`DESCRIBE TABLE`, or
`aws keyspaces get-table`). Wait until the table status is `ACTIVE`.

---

## Phase 2 — Prepare the source account

Do this step in the **source** account.

### Step 8. Create the source-read role

```bash
aws iam create-role --role-name CQLReplicatorSourceRole --max-session-duration 3600 \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::YYYYYYYYYYYY:role/GlueKeyspacesRole"},"Action":"sts:AssumeRole"}]}'

aws iam put-role-policy --role-name CQLReplicatorSourceRole --policy-name KeyspacesSourceRead \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["cassandra:Select"],"Resource":["arn:aws:cassandra:us-east-1:XXXXXXXXXXXX:/keyspace/ks_test_cql_replicator/table/test_cql_replicator","arn:aws:cassandra:us-east-1:XXXXXXXXXXXX:/keyspace/system*/table/*"]}]}'
```

Rules:

- The Glue job role (Step 2) must exist first. See the note in Step 2.
- Do not set `--max-session-duration` below 3600. The provider requests a
  3600-second session.
- If you delete and re-create the Glue job role later, apply this trust policy
  again. IAM pins the principal to the role's internal unique ID.

---

## Phase 3 — Deploy and run

Do the steps of this phase with **target**-account credentials, from
`cql-replicator/glue/bin`.

### Step 9. Configure the source connection

Replace `glue/conf/CassandraConnector.conf` with the Keyspaces cross-account
profile (full template: [cross-account-replication.md](cross-account-replication.md),
section 4). The key block:

```hocon
advanced.auth-provider = {
  class        = com.amazon.cqlreplicator.CrossAccountSigV4AuthProvider
  aws-region   = us-east-1
  aws-role-arn = "arn:aws:iam::XXXXXXXXXXXXX:role/CQLReplicatorSourceRole"
}
```

Rules:

- Keep `class` fully qualified. An unqualified name does not load.
- Keep the `advanced.ssl-engine-factory` block. Keyspaces refuses a plaintext
  connection.
- Write the source region as a literal, not `${AWS_DEFAULT_REGION}`.
- Do not change `KeyspacesConnector.conf`. It serves the target and the ledger
  with the Glue job's own credentials.

### Step 10. Deploy

```bash
./cqlreplicator --state init --sg "\"$SG_ID\"" --subnet "$SUBNET_ID" --az "$AZ" \
  --region us-east-1 --env dev --glue-iam-role GlueKeyspacesRole \
  --target-type keyspaces
```

Notes:

- Without `--landing-zone`, `init` uses
  `s3://cql-replicator-<account>-<region>-<env>`.
- All permission checks must show ✅ (with a scoped Cassandra policy, see the
  note in Step 3).
- To change a conf file after `init`: edit it, upload it to
  `s3://<landing-zone>/artifacts/`, and start a new run. The local copy alone
  has no effect.

### Step 11. Test the IAM hop

```bash
./glue/sbin/keyspaces/smoke/run-cross-account-probe.sh \
  --landing-zone s3://cql-replicator-YYYYYYYYYYYY-us-east-1-dev --region us-east-1 \
  --glue-iam-role GlueKeyspacesRole \
  --role-arn arn:aws:iam::XXXXXXXXXXXX:role/CQLReplicatorSourceRole
```

The probe performs the `AssumeRole` and confirms that the identity belongs to
the source account. It opens no Keyspaces session.

### Step 12. Run the replication

```bash
./cqlreplicator --state run --max-wcu-traffic 20000 --tiles 4 \
  --landing-zone s3://cql-replicator-YYYYYYYYYYYY-us-east-1-dev --region us-east-1 \
  --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
  --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \
  --env dev --target-type keyspaces
```

Rules:

- Do not pass `--writetime-column` unless client-side timestamps are enabled on
  the **source** table. Check with `aws keyspaces get-table`. If they are
  enabled, pass one non-key scalar column.
- The source reads bill RCUs in the source account. On provisioned capacity,
  raise the read capacity for the initial scan.

### Step 13. Monitor and verify

```bash
./cqlreplicator --state stats \
  --landing-zone s3://cql-replicator-YYYYYYYYYYYY-us-east-1-dev --region us-east-1 \
  --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator --env dev
```

- Search the Glue `/aws-glue/jobs/output` log stream for the preflight warnings
  listed in [cross-account-replication.md](cross-account-replication.md). No
  warning means the cross-account provider loaded.
- Make sure that the log contains this line:
  `Cross-account source: connector sessions use com.amazon.cqlreplicator.CrossAccountConnectionFactory`.
  If it is missing, the executors cannot load the provider.
- If the job stays in `RUNNING` and the log shows `ConnectTimeoutException` for
  `cassandra.<region>.amazonaws.com:9142`, the job cannot reach the source.
  Make sure that the job and the VPC endpoints are in the source table's region.
- A few `Lost connection to remote peer` task failures are normal. Spark runs
  the failed tasks again. For details, see the operational notes in
  [cross-account-replication.md](cross-account-replication.md).
- Expect Discovered = Replicated = the source row count. The replicated counter
  is cumulative across cycles; a job restart can make it larger than the source
  row count without data damage.
- `--cmd reconcile-keys` does not support a cross-account source. Verify with
  row counts or `SELECT JSON` samples.

### Step 14. Clean up after the test

```bash
./cqlreplicator --state request-stop ...   # stop the jobs first
./cqlreplicator --state cleanup --landing-zone s3://cql-replicator-YYYYYYYYYYYY-us-east-1-dev \
  --region us-east-1 --env dev
```

Also delete the VPC interface endpoints (each costs about $0.01 per hour), the
probe jobs from Step 1, and — in the source account — `CQLReplicatorSourceRole`
if it is no longer needed.
