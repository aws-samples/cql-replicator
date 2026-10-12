# Cross-account replication between Amazon Keyspaces accounts

Replicate an Amazon Keyspaces table in one AWS account (**account A**, the
source) into an Amazon Keyspaces table in another (**account B**, the target).

This is a different topology from the ordinary migration path documented in
[README.MD](README.MD), where the source is a self-managed Cassandra cluster
reached over the VPC and the only AWS-authenticated endpoint is the target. Here
**both** endpoints are Amazon Keyspaces, and one of them is in a foreign account,
so the source connection has to authenticate as a role it can only reach through
`sts:AssumeRole`.

> **Scope.** This document covers Keyspaces → Keyspaces across accounts only. If
> you are migrating from Apache Cassandra to Amazon Keyspaces, follow
> [README.MD](README.MD) instead — none of the configuration below applies, and
> you should leave `CassandraConnector.conf` as it is.

---

## Deployment topology

**CQLReplicator is deployed in the target account, B, and reads from A.**

```
  account A (source) ◀─────  CQLReplicatorSourceRole trusts B's Glue role  cassandra:Select─────account B (target)
```

Why the job belongs in B, not A:

| | Deploy in B (reads from A) | Deploy in A (writes to B) |
|---|---|---|
| Write path (heavy: batches, retries) | Local, ambient credentials | Every write is cross-account |
| Ledger (`migration.ledger`) | Local, in B | Cross-account, or split from the target |
| S3 landing zone, Glue Data Catalog, Iceberg snapshots | Local, in B | Cross-account bucket and catalog policies |
| Cross-account surface | One read-only connection | Three write-heavy connections |
| Cost of a misconfigured role | A failed read | A partial write |

Deploying in B puts the single cross-account hop on the *read* path and keeps
everything that writes — target table, ledger, S3, Catalog — local. All
`cqlreplicator` commands (`init`, `run`, `stats`, `cleanup`) are run in account
B.

The ledger lives in account B, which is the default once the deployment is in B —
no extra flags are needed.

---

## Configuration

### 1. IAM: the source-read role in account A

Create a role in **account A** that account B's Glue job role can assume, with
read-only access to the source table.

```bash
# Run these in account A.
# B_ACCOUNT = the account CQLReplicator runs in
# GLUE_ROLE = the Glue job role name passed to cqlreplicator --glue-iam-role
B_ACCOUNT=<account-B-id>
GLUE_ROLE=<glue-job-role-name>

aws iam create-role \
  --role-name CQLReplicatorSourceRole \
  --max-session-duration 3600 \
  --assume-role-policy-document "$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "AWS": "arn:aws:iam::${B_ACCOUNT}:role/${GLUE_ROLE}" },
    "Action": "sts:AssumeRole"
  }]
}
EOF
)"

aws iam put-role-policy \
  --role-name CQLReplicatorSourceRole \
  --policy-name KeyspacesSourceRead \
  --policy-document "$(cat <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["cassandra:Select"],
    "Resource": "*"
  }]
}
EOF
)"
```

`cassandra:Select` is the only action the source connection needs — the read
path never writes to A. Narrow `Resource` to the specific keyspace and table
ARNs (`arn:aws:cassandra:<region>:<A>:/keyspace/<ks>/table/<tbl>`) plus
`/keyspace/system*/table/*` for the driver's metadata reads.

**`--max-session-duration` must be at least 3600 seconds**, because the provider
requests a 3600-second session. A shorter maximum makes `AssumeRole` fail
outright.

### 2. IAM: the Glue job role in account B

Cross-account assumption requires **both** sides. In addition to the trust
policy above, the Glue job role in B needs an identity-based grant:

```bash
# Run this in account B.
A_ACCOUNT=<account-A-id>

aws iam put-role-policy \
  --role-name <glue-job-role-name> \
  --policy-name AssumeCQLReplicatorSourceRole \
  --policy-document "$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "sts:AssumeRole",
    "Resource": "arn:aws:iam::${A_ACCOUNT}:role/CQLReplicatorSourceRole"
  }]
}
EOF
)"
```

Missing either side yields:

```
User: arn:aws:sts::<B>:assumed-role/<GlueRole>/GlueJobRunnerSession is not
authorized to perform: sts:AssumeRole on resource:
arn:aws:iam::<A>:role/CQLReplicatorSourceRole (Status Code: 403)
```

The Glue job role in B also needs its usual target-side permissions —
`cassandra:Select`, `cassandra:Insert`, `cassandra:Update`, `cassandra:Delete`,
the S3 landing-zone actions, and the Glue Data Catalog actions listed in
[README.MD](README.MD). `cqlreplicator init` reports these via
`iam:SimulatePrincipalPolicy`.

### 3. Source connection: `glue/conf/CassandraConnector.conf`

The source is now Amazon Keyspaces in account A, so this file must be rewritten
from its shipped Cassandra defaults into a Keyspaces profile — TLS, the
Keyspaces endpoint, and the cross-account auth provider:

```hocon
datastax-java-driver {
  # Region of the SOURCE table, in account A.
  endpoint = "cassandra.us-east-1.amazonaws.com:9142"

  basic.contact-points = [ ${datastax-java-driver.endpoint} ]
  basic.request.consistency = "LOCAL_QUORUM"
  basic.request.default-idempotence = true
  basic.request.page-size = 200

  basic.load-balancing-policy {
    local-datacenter = us-east-1
    slow-replica-avoidance = false
  }

  advanced.auth-provider = {
    class        = com.amazon.cqlreplicator.CrossAccountSigV4AuthProvider
    aws-region   = us-east-1
    aws-role-arn = "arn:aws:iam::<account-A-id>:role/CQLReplicatorSourceRole"
  }

  advanced.ssl-engine-factory {
    class = DefaultSslEngineFactory
    hostname-validation = false
  }

  advanced.reconnect-on-init = true
  advanced.resolve-contact-points = false
  advanced.connection.pool.local.size = 3
  advanced.prepared-statements.prepare-on-all-nodes = false
  advanced.prepared-statements.reprepare-on-up.enabled = false
}
```

Notes:

- `class` is **fully qualified**. An unqualified name silently fails to load.
- `aws-region` must be the region of the **source** table in A. If A and B are
  in different regions, this differs from `KeyspacesConnector.conf`'s region and
  from `--region`.
- `local-datacenter` must equal the source region.
- Do not use `${AWS_DEFAULT_REGION}` here when A's region differs from B's — in
  Glue that variable resolves to the region the *job* runs in.
- `advanced.ssl-engine-factory` is required. The shipped Cassandra defaults have
  no TLS; Keyspaces will not accept a plaintext connection on 9142.

### 4. Target and ledger connection: `glue/conf/KeyspacesConnector.conf`

Leave this file as shipped. The target and the ledger are in account B, where
the Glue job's own credentials already apply, so the stock
`software.aws.mcs.auth.SigV4AuthProvider` with `aws-region = ${AWS_DEFAULT_REGION}`
is correct. Do not add `aws-role-arn` here.

### 5. Deploy

Both files are uploaded to `s3://<landing-zone>/artifacts/` by `cqlreplicator
init`, and are re-read on every job run. Run all commands in account B, exactly
as in [README.MD](README.MD):

```shell
   cd cql-replicator/glue/bin &&
   cqlreplicator --cmd init --sg '"sg-1"' \
                 --subnet "subnet-XXXXXXXXXXXX" --az us-east-1a --region us-east-1 \
                 --env dev \
                 --glue-iam-role <glue-job-role-name> \
                 --landing-zone s3://<PROVIDED_BUCKET_NAME> \
                 --target-type keyspaces
```

To change a conf file after `init`, edit it and re-upload, then start a new run:

```shell
   aws s3 cp glue/conf/CassandraConnector.conf \
     s3://<PROVIDED_BUCKET_NAME>/artifacts/CassandraConnector.conf --region us-east-1
```

---

## Usage

The replication commands are unchanged — cross-account access is entirely a
property of the conf:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 20000 \
                 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \
                 --env dev --target-type keyspaces
```

`--region` is account B's region: it selects where the Glue job, the target
table, and the ledger live. The source region comes only from
`CassandraConnector.conf`.

### Capturing updates

As with any CQLReplicator run, inserts and deletes are detected from the primary
key set alone, but **updates require `--writetime-column`**:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 20000 \
                 --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
                 --writetime-column col3 \
                 --src-keyspace ks_test_cql_replicator --src-table test_cql_replicator \
                 --trg-keyspace ks_test_cql_replicator --trg-table test_cql_replicator \
                 --env dev --target-type keyspaces
```

Read [Limitations](#limitations) first — `writetime()` behaves differently on a
Keyspaces source than on Cassandra, and it constrains which columns are usable
here.

### Verifying the cross-account hop

The job logs the assumed identity only indirectly, so the cleanest confirmation
is a preflight warning check plus a successful source scan. CQLReplicator
inspects both conf files before opening any connection and warns about
misconfigurations that would otherwise fail confusingly. Search the Glue
`/aws-glue/jobs/output` log stream for these:

| Warning | Meaning |
|---|---|
| `sets advanced.auth-provider.aws-role-arn but class is software.aws.mcs.auth.SigV4AuthProvider, which IGNORES it` | The stock provider is still configured. The job is authenticating as B's Glue role, not the role in A. |
| `requests cross-account auth but the AWS SDK v2 STS module is not on the classpath` | The Glue image does not bundle the AWS SDK. Use Glue 5.0. |
| `is not fully qualified and is not a built-in driver provider` | `class` is missing the `com.amazon.cqlreplicator.` prefix and will not load. |

For an independent check of the IAM setup before running a replication job, the
harness at `glue/sbin/keyspaces/smoke/` submits a read-only probe that performs
`sts:AssumeRole` and confirms the resulting identity belongs to the expected
account. It opens no Keyspaces session and creates nothing but a throwaway Glue
job:

```shell
   ./glue/sbin/keyspaces/smoke/run-cross-account-probe.sh \
       --landing-zone s3://<PROVIDED_BUCKET_NAME> --region us-east-1 \
       --glue-iam-role <glue-job-role-name> \
       --role-arn arn:aws:iam::<account-A-id>:role/CQLReplicatorSourceRole
```

---

## Limitations

### Requires Glue 5.0

The provider needs the AWS SDK v2 STS module at runtime. Glue 5.0 supplies the
whole SDK as a single bundle
(`/usr/share/aws/aws-java-sdk-v2/aws-sdk-java-bundle-2.29.52.jar`), and
CQLReplicator deliberately ships **no** SDK jars of its own, since a second copy
alongside the bundle risks a split-version `NoSuchMethodError`.

On an image without that bundle, a conf with `aws-role-arn` fails at startup
with an explicit message rather than a bare `ClassNotFoundException`. A conf
*without* `aws-role-arn` is unaffected on any image — STS is never referenced.

### `writetime()` on a Keyspaces source

This is the most significant functional limitation, and it is a property of
Amazon Keyspaces rather than of CQLReplicator.

Amazon Keyspaces exposes `writetime()` only on tables where
[client-side timestamps](https://docs.aws.amazon.com/keyspaces/latest/devguide/client-side-timestamps.html)
are explicitly enabled. Consequences for a Keyspaces **source**:

- If client-side timestamps are **not** enabled on the source table,
  `--writetime-column` cannot be used and **updates will not be detected**.
  Inserts and deletes still replicate.
- If they **are** enabled, `writetime()` reflects the timestamp the client
  supplied. Rows written before the setting was turned on have no usable value,
  so early updates can be missed even after enabling it.
- `writetime()` cannot be read for collection columns or primary-key columns —
  on Keyspaces as on Cassandra. Pick a regularly-updated non-key scalar column.

Verify the setting on the source table before planning on update capture (via
`DESCRIBE TABLE`, the Keyspaces console, or
`aws keyspaces get-table --keyspace-name <ks> --table-name <tbl>`, whose
response includes the client-side-timestamp status).

If updates must be captured and client-side timestamps are unavailable, use
periodic full runs (batch workload) rather than incremental detection, and
reconcile with `--cmd reconcile-keys`.

Note that this is separate from `--json-mapping '{"replication":
{"replicateWithTimestamp": true}}'`, which requires client-side timestamps on
the **target** table (see [README.MD](README.MD)). Preserving original
timestamps end to end needs the feature enabled on **both** sides.

### Split skew from `system.size_estimates`

The spark-cassandra-connector sizes its token-range splits from
`system.size_estimates`. Amazon Keyspaces populates that table differently from
Cassandra, so the derived split count can be a poor fit for the real data
distribution — usually manifesting as a few very long-running Spark tasks while
most finish quickly.

If you see that, override the split size through `--json-mapping`:

```json
{"replication": {"readConfiguration": {"splitSizeInMB": 16, "concurrentReads": 32}}}
```

Lower `splitSizeInMB` produces more, smaller splits and more even parallelism at
the cost of more requests. The defaults are `splitSizeInMB: 64`,
`concurrentReads: 32`, `fetchSizeInRows: 500`, `consistencyLevel: LOCAL_ONE`.

### Source-side read cost and throttling

A Cassandra source costs nothing per read. A Keyspaces source is billed in RCUs,
and every discovery pass and full scan reads through it. Before a large run:

- Confirm the **source** table's capacity mode. On provisioned capacity, a full
  scan will throttle unless read capacity is raised for the duration; on-demand
  absorbs it but bills for it.
- Budget the source read cost, not just the target write cost. Point-in-time
  replication and repeated full loads multiply it.
- Source-side throttling surfaces as `ReadTimeoutException` in the Glue logs, and
  the failure analyzer may report `Failed to open native connection to
  Cassandra`. The mitigation is the same conservative retry policy described
  under *Handling Amazon Keyspaces throttling during reconciliation* in
  [README.MD](README.MD).

### Not supported in this topology

| | Status |
|---|---|
| Keyspaces source in account A → Keyspaces target in account B | Supported |
| Cassandra source → Keyspaces target, same account | Supported — see [README.MD](README.MD); do not apply this document |
| Cross-account **target** (job in A, writing to B) | Not supported. `KeyspacesConnector.conf` covers both the target and the ledger, so one role ARN cannot address a foreign target and a local ledger. |
| `--target-type parquet` / `dynamodb` with a cross-account Keyspaces source | Not supported. The provider ships only in `glue/sbin/keyspaces/CQLReplicator.scala`. |
| `--cmd reconcile-keys` against a cross-account source | Not yet. `PrimaryKeyReconciliation` is a separate Glue script that does not yet carry the provider, so a conf naming it by FQN will not load there. Reconciliation remains available for same-account sources. |
| Cross-**region** source and target | Works, but every read crosses regions: expect higher latency, inter-region data transfer charges, and no VPC endpoint for the source. Set `aws-region` and `local-datacenter` to A's region and size splits conservatively. |
| IAM external ID, MFA, or a role chain on the source role | Not supported. The provider requests a plain `AssumeRole` with session name `cqlreplicator` and a 3600-second duration. |

### Operational notes

- **Both trust directions are required.** A trust policy in A without the
  matching `sts:AssumeRole` grant in B fails with a 403, and vice versa.
- **`--max-session-duration` on the source role must be ≥ 3600s.**
- **Session name is fixed** at `cqlreplicator`. If you scope CloudTrail alarms or
  IAM conditions on `sts:RoleSessionName`, use that value.
- **The conf is read on every run**, so a role ARN change takes effect on the
  next job start with no `init` needed — but the file must be re-uploaded to
  `s3://<landing-zone>/artifacts/`, since editing the local copy alone has no
  effect.
- **Deleting the source role mid-run** does not fail immediately. Because
  credentials are resolved per authentication, existing connections continue
  until their next re-authentication.
