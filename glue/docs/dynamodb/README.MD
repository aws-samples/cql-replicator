# Replicate workload to Amazon DynamoDB

## Prepare the configuration files
In order to connect to Amazon Keyspaces (used for the internal ledger) and the Cassandra cluster you need to configure
reference configuration files `../glue/conf/CassandraConnector.conf` and `../glue/conf/KeyspacesConnector.conf`
in ../conf accordingly to the environment.

## Init migration process
The following command initializes the CQLReplicator environment, which involves the copying JAR artifacts, creation of a Glue connector,
a S3 bucket, a Glue job, migration keyspace, and ledger table.

```shell
    cd cql-replicator/glue/bin &&
    cqlreplicator --cmd init \
                  --subnet "subnet-00a00b00" \
                  --sg '"sg-0e0b0a000000d000b"' \
                  --az "us-east-1a" \
                  --region us-east-1 \
                  --glue-iam-role GlueRole \
                  --env ddbdemo \
                  --target-type dynamodb
```
After running the above command, you should find in the AWS account:

1. CQLReplicator-ddbdemo Glue job and Glue connector in AWS Glue console
2. Amazon S3 bucket that stores the artifacts
3. Keyspace `migration` and `ledger` table in Amazon Keyspaces (used internally for tracking replication progress)

Note: If you run the CQLReplicator on EC2 you should attach the following inline policy:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "iam:GetContextKeysForPrincipalPolicy",
        "iam:SimulatePrincipalPolicy",
        "iam:ListAttachedRolePolicies",
        "iam:PassRole",
        "glue:*",
        "dynamodb:*",
        "pricing:GetProducts",
        "pricing:DescribeServices",
        "pricing:GetAttributeValues"
      ],
      "Resource": "*"
    }
  ]
}
```

- `--sg` - security group(s) which allow access to the Cassandra cluster from AWS Glue and includes the
  self-referencing inbound rule for all traffic
- `--subnet` - the subnet to which the Cassandra cluster belongs. AWS Glue sets up elastic network interfaces
  that enable your Glue jobs to connect securely to other resources within your VPC
- `--az` - the AZ of the subnet
- `--region` - AWS region where the Cassandra cluster is deployed
- `--glue-iam-role` - the [IAM role permissions](https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html)
  that AWS Glue can assume when calling other services on your behalf. Services: Amazon DynamoDB, Amazon Keyspaces, and S3
- `--landing-zone` - optional parameter, you can reuse any existing S3 buckets in the AWS account.
  If you don't supply it, init process will try to create a new bucket
- `--target-type` - must be set to `dynamodb`
- `--skip-glue-connector` - optional parameter to skip the Glue connector creation step
- `--skip-keyspaces-ledger` - optional parameter to skip creation/deletion of the ledger table in Amazon Keyspaces
- `--env` - use to create multiple environments, e.g. dev/test/prod

### VPC Endpoint Requirements

CQLReplicator uses Apache Iceberg with the AWS Glue Data Catalog for snapshot management. The Glue job runs inside a VPC, so the following VPC endpoints are required:

| Endpoint | Type | Purpose |
|----------|------|---------|
| `com.amazonaws.{region}.glue` | Interface | Iceberg catalog operations (CREATE TABLE, getTable, etc.) |
| `com.amazonaws.{region}.s3` | Gateway | Iceberg data/metadata read/write in S3 |
| `com.amazonaws.{region}.dynamodb` | Gateway | DynamoDB write operations |

The Glue interface endpoint must have:
- `PrivateDnsEnabled` = `true`
- The VPC must have `enableDnsSupport` = `true` and `enableDnsHostnames` = `true`
- The endpoint's security group must allow inbound TCP port 443 from the Glue job's security group

## DynamoDB Primary Key Mapping

Cassandra and DynamoDB have different primary key models. Cassandra supports composite partition keys (multiple columns)
and multiple clustering columns, while DynamoDB supports exactly one partition key and an optional sort key.
CQLReplicator bridges this gap by mapping Cassandra primary key columns to DynamoDB's key structure.

### Configuration

Define the mapping using `dynamoDBPrimaryKey` in `--json-mapping`:

```json
{
  "replication": {
    "dynamoDBPrimaryKey": {
      "partitionKeyName": "pk",
      "sortKeyName": "sk",
      "separator": "#"
    }
  }
}
```

- `partitionKeyName` - the DynamoDB partition key attribute name (default: `PK`)
- `sortKeyName` - the DynamoDB sort key attribute name (leave empty `""` for tables without a sort key)
- `separator` - the delimiter used when concatenating multiple Cassandra columns into a single DynamoDB key value (default: `#`)

### Key Composition Rules

CQLReplicator handles six distinct key mapping scenarios based on the number of Cassandra partition keys and clustering columns:

#### Case 1: Single partition key, no clustering columns
Cassandra: `PRIMARY KEY (user_id)`

The Cassandra partition key maps directly to the DynamoDB partition key, preserving the original data type (string, numeric, etc.). No sort key is created.

| Cassandra | DynamoDB |
|---|---|
| `user_id = "abc123"` | `pk = "abc123"` |

```json
{"dynamoDBPrimaryKey": {"partitionKeyName": "pk", "sortKeyName": ""}}
```

#### Case 2: Single partition key, single clustering column
Cassandra: `PRIMARY KEY (user_id, timestamp)`

Both keys map directly to DynamoDB, preserving their original data types.

| Cassandra | DynamoDB |
|---|---|
| `user_id = "abc123"`, `timestamp = 1706408365` | `pk = "abc123"`, `sk = 1706408365` |

```json
{"dynamoDBPrimaryKey": {"partitionKeyName": "pk", "sortKeyName": "sk"}}
```

#### Case 3: Multiple partition keys, single clustering column
Cassandra: `PRIMARY KEY ((tenant_id, region), timestamp)`

Multiple Cassandra partition keys are concatenated into a single DynamoDB partition key string using the separator.
The clustering column maps directly to the sort key, preserving its type.

| Cassandra | DynamoDB |
|---|---|
| `tenant_id = "acme"`, `region = "us-east"`, `timestamp = 1706408365` | `pk = "acme#us-east"`, `sk = 1706408365` |

```json
{"dynamoDBPrimaryKey": {"partitionKeyName": "pk", "sortKeyName": "sk", "separator": "#"}}
```

#### Case 4: Single partition key, multiple clustering columns
Cassandra: `PRIMARY KEY (user_id, year, month)`

The partition key maps directly. Multiple clustering columns are concatenated into a single DynamoDB sort key string using the separator.

| Cassandra | DynamoDB |
|---|---|
| `user_id = "abc123"`, `year = "2024"`, `month = "01"` | `pk = "abc123"`, `sk = "2024#01"` |

```json
{"dynamoDBPrimaryKey": {"partitionKeyName": "pk", "sortKeyName": "sk", "separator": "#"}}
```

#### Case 5: Multiple partition keys, no clustering columns
Cassandra: `PRIMARY KEY ((tenant_id, region))`

Multiple partition keys are concatenated into a single DynamoDB partition key string. No sort key is created.

| Cassandra | DynamoDB |
|---|---|
| `tenant_id = "acme"`, `region = "us-east"` | `pk = "acme#us-east"` |

```json
{"dynamoDBPrimaryKey": {"partitionKeyName": "pk", "sortKeyName": "", "separator": "#"}}
```

#### Case 6: Multiple partition keys, multiple clustering columns
Cassandra: `PRIMARY KEY ((tenant_id, region), year, month)`

Both sides are concatenated independently using the separator.

| Cassandra | DynamoDB |
|---|---|
| `tenant_id = "acme"`, `region = "us-east"`, `year = "2024"`, `month = "01"` | `pk = "acme#us-east"`, `sk = "2024#01"` |

```json
{"dynamoDBPrimaryKey": {"partitionKeyName": "pk", "sortKeyName": "sk", "separator": "#"}}
```

### Type Preservation

When a single Cassandra column maps to a DynamoDB key, the original data type is preserved:
- Cassandra `text`/`varchar`/`ascii`/`uuid`/`timeuuid` → DynamoDB String (`S`)
- Cassandra `int`/`bigint`/`float`/`double`/`decimal` → DynamoDB Number (`N`)
- Cassandra `blob` → DynamoDB Binary (`B`)
- Cassandra `boolean` → DynamoDB Boolean (`BOOL`)

When multiple columns are concatenated (composite keys), the result is always a DynamoDB String (`S`), regardless of the original column types.

### Data Type Mapping for Non-Key Columns

All non-key columns from the Cassandra source are automatically mapped to DynamoDB attribute types:

| Cassandra Type | DynamoDB Type |
|---|---|
| `text`, `varchar`, `ascii`, `inet`, `uuid`, `timeuuid`, `date`, `time`, `timestamp` | String (`S`) |
| `int`, `bigint`, `float`, `double`, `decimal`, `smallint`, `tinyint`, `varint`, `counter` | Number (`N`) |
| `blob` | Binary (`B`) |
| `boolean` | Boolean (`BOOL`) |
| `list<int>`, `list<bigint>` | Number Set (`NS`) |
| `list<text>`, `list<varchar>`, `list<uuid>`, etc. | String Set (`SS`) |
| `map<text, int>` | Map with Number values (`M`) |
| `map<int, text>`, `map<text, text>` | Map with String values (`M`) |
| UDT (User Defined Type) | String (`S`) — serialized as JSON |

### Choosing the Right Separator

The separator is used when concatenating multiple Cassandra columns into a single DynamoDB key. Choose a character
that does not appear in your key values to avoid ambiguity. Common choices:
- `#` (default) — good for most use cases
- `|` — useful when key values may contain `#`
- `::` — useful for namespaced keys

## Start migration process
To operate CQLReplicator on AWS Glue, you need to use `--cmd run` command, followed by a series of parameters.

```shell
   cqlreplicator --cmd run \
                 --target-type dynamodb \
                 --landing-zone s3://<BUCKET_NAME> \
                 --region us-east-1 --max-wcu-traffic 30000 \
                 --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo \
                 --writetime-column data \
                 --json-mapping '{
                   "replication": {
                     "dynamoDBPrimaryKey": {
                       "partitionKeyName": "pk",
                       "sortKeyName": "sk",
                       "separator": "#"
                     }
                   },
                   "dynamodb": {
                     "readBeforeWrite": false,
                     "dynamoDBConnection": {
                       "endpoint": "dynamodb.us-east-1.amazonaws.com",
                       "region": "us-east-1"
                     }
                   }
                 }' \
                 --workload-type batch
```

The CQLReplicator offers three options for operation:
1. For high-volume traffic exceeding 5000 WCU per second:
    * Use the command: `--max-wcu-traffic 30000`
    * This setting uses the default of 4 tiles (Glue jobs)
2. For lower-volume traffic below 5000 WCU per second:
    * Use the command: `--max-wcu-traffic 4000 --tiles 1`
    * This setting customizes the number of tiles to 1
3. For custom configurations:
    * Use the command: `--tiles 12` (without specifying `--max-wcu-traffic`)
    * This allows you to set a custom number of tiles with your preferred worker type
    * Note: Changing the number of tiles requires resizing the migration process

### Replicate near-real time updates, inserts, and deletes

Note: The following command captures only "insert" or "delete". To capture "update" you need to add an arbitrary column
from your table that is constantly updated, for example, add `--writetime-column your-column` to the command line.

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb \
                 --writetime-column data \
                 --json-mapping '{
                   "replication": {
                     "dynamoDBPrimaryKey": {"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}
                   },
                   "dynamodb": {
                     "readBeforeWrite": false,
                     "dynamoDBConnection": {"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"}
                   }
                 }'
```

### Track changes across multiple columns
The `--writetime-column` parameter accepts multiple comma-separated column names. When multiple columns are specified,
CQLReplicator computes the maximum `WRITETIME()` across all specified columns to detect the most recent mutation.

Single column:
```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column col1 --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb \
                 --json-mapping '{"replication":{"dynamoDBPrimaryKey":{"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}},"dynamodb":{"dynamoDBConnection":{"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"}}}'
```

Multiple columns:
```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column col1,col2,col3 --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb \
                 --json-mapping '{"replication":{"dynamoDBPrimaryKey":{"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}},"dynamodb":{"dynamoDBConnection":{"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"}}}'
```

When multiple columns are specified, CQLReplicator generates `greatest(writetime(col1), writetime(col2), ...) as ts`
in the CQL query. Cassandra's `WRITETIME()` returns null for columns that were never explicitly written. The `greatest()`
function excludes null values and returns the maximum of the non-null writetimes.

### Change the workload type
If you want to replicate only the historical workload and release Glue resources once it's completed, add the
`--workload-type batch` flag:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb --workload-type batch \
                 --json-mapping '{"replication":{"dynamoDBPrimaryKey":{"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}},"dynamodb":{"dynamoDBConnection":{"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"}}}'
```

### Replicate with TTL
DynamoDB supports TTL natively. The CQLReplicator maps Cassandra TTL values to a DynamoDB TTL attribute.
Ensure the [TTL attribute is enabled](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html) on the target DynamoDB table before running:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --ttl-column data \
                 --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb \
                 --json-mapping '{"replication":{"dynamoDBPrimaryKey":{"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}},"dynamodb":{"dynamoDBConnection":{"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"}}}'
```

### Not overwriting existing records in DynamoDB
To preserve existing records during the bulk migration phase, set `readBeforeWrite` to `true`. This uses
DynamoDB's `attribute_not_exists` conditional expression to skip rows that already exist in the target:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb \
                 --json-mapping '{
                   "replication": {
                     "dynamoDBPrimaryKey": {"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}
                   },
                   "dynamodb": {
                     "readBeforeWrite": true,
                     "dynamoDBConnection": {"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"}
                   }
                 }'
```

### Switch from BatchWriteItem to single puts
By default, CQLReplicator uses DynamoDB `BatchWriteItem` for improved throughput. When dealing with large items or
needing conditional writes, you can switch to single `PutItem` operations:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb \
                 --json-mapping '{
                   "replication": {
                     "dynamoDBPrimaryKey": {"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}
                   },
                   "dynamodb": {
                     "dynamoDBConnection": {"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"},
                     "writeConfiguration": {"maxStatementsPerBatch": 1}
                   }
                 }'
```

### Offload large objects to S3
For items exceeding DynamoDB's 400KB item size limit, CQLReplicator can offload large column values to S3.
The payload is compressed by [LZ4](https://fuchsia.googlesource.com/third_party/lz4/+/refs/tags/v1.7.5/README.md) and stored in the S3 bucket.

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb \
                 --json-mapping '{
                   "replication": {
                     "dynamoDBPrimaryKey": {"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}
                   },
                   "dynamodb": {
                     "dynamoDBConnection": {"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"},
                     "largeObjectsConfig": {
                       "enabled": true,
                       "columns": ["payload"],
                       "bucket": "my-large-objects-bucket",
                       "prefix": "ks/tbl/payload",
                       "keySeparator": "#",
                       "compressionEnabled": true
                     }
                   }
                 }'
```

### Compress selected columns
To minimize the size of large columns in the target DynamoDB table, enable `compressionConfig`.
Two compression algorithms are available:

| Algorithm | Use Case | Decompression |
|-----------|----------|---------------|
| `lz4` (default) | General-purpose, fast compression with moderate ratio. Good for most workloads | [LZ4DecompressorWithLength](https://javadoc.io/doc/org.lz4/lz4-java/latest/net/jpountz/lz4/LZ4DecompressorWithLength.html) |
| `zstd-dict-based` | Dictionary-trained compression for higher compression ratios on structured/repetitive data. Requires a sample file for dictionary training | [zstd-jni](https://github.com/luben/zstd-jni) `ZstdDictDecompress` with the generated dictionary |

Use `lz4` when you need fast compression without additional setup. Use `zstd-dict-based` when your data has repetitive patterns (e.g., JSON documents with consistent schemas) and you want better compression ratios at the cost of a one-time dictionary training step.

#### compressionConfig Schema

```json
{
  "dynamodb": {
    "compressionConfig": {
      "enabled": true,
      "compression": "zstd-dict-based",
      "compressionLevel": 3,
      "compressAllNonPrimaryKeysColumns": true,
      "compressNonPrimaryColumns": ["col1", "col2"],
      "targetAttributeName": "compressed_data"
    }
  }
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `false` | Enables column-level compression |
| `compression` | string | `"lz4"` | Compression algorithm: `"lz4"` or `"zstd-dict-based"` |
| `compressionLevel` | integer | `3` | Zstd compression level (1–22). Higher values give better compression at the cost of speed. Ignored for lz4 |
| `compressAllNonPrimaryKeysColumns` | boolean | `false` | Compress all non-primary-key columns |
| `compressNonPrimaryColumns` | list[string] | `[""]` | Specific columns to compress (when `compressAllNonPrimaryKeysColumns` is false) |
| `targetAttributeName` | string | `""` | Target attribute name for the compressed binary data |

If `compression` is omitted, it defaults to `"lz4"`. If `compressionLevel` is outside the range 1–22, the Glue job terminates with an error.

#### LZ4 compression example

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb \
                 --json-mapping '{
                   "replication": {
                     "dynamoDBPrimaryKey": {"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}
                   },
                   "dynamodb": {
                     "dynamoDBConnection": {"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"},
                     "compressionConfig": {
                       "enabled": true,
                       "compressAllNonPrimaryKeysColumns": false,
                       "compressNonPrimaryColumns": ["col2","col3","col4"],
                       "targetAttributeName": "compressedPayload"
                     }
                   }
                 }'
```

#### Zstd dictionary-based compression example

Before running the discovery job with `zstd-dict-based` compression, create and upload a sample file to S3:

1. Create a JSON file named `{ks_name}_{table_name}_sample.json` containing a JSON array of strings:

```json
[
  "{\"col1\": \"value1\", \"col2\": 42}",
  "{\"col1\": \"value2\", \"col2\": 99}",
  "{\"col1\": \"value3\", \"col2\": 7}"
]
```

2. Upload to: `s3://{S3_LANDING_ZONE}/artifacts/{ks_name}_{table_name}_sample.json`

During the discovery job, CQLReplicator trains a zstd dictionary from the sample file, verifies it via a round-trip compress/decompress check, and uploads it to S3 at:
`s3://{S3_LANDING_ZONE}/artifacts/{ks_name}_{table_name}_dictionary.bin`

Each replication job downloads the dictionary at startup and uses it for all compression operations. You can also download the dictionary and use it in your own application with `ZstdDictDecompress` from [zstd-jni](https://github.com/luben/zstd-jni) for decompression.

Running with `--cleanup-requested` regenerates the dictionary. Without the flag, the existing dictionary is reused.

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb \
                 --json-mapping '{
                   "replication": {
                     "dynamoDBPrimaryKey": {"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}
                   },
                   "dynamodb": {
                     "dynamoDBConnection": {"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"},
                     "compressionConfig": {
                       "enabled": true,
                       "compression": "zstd-dict-based",
                       "compressionLevel": 5,
                       "compressAllNonPrimaryKeysColumns": false,
                       "compressNonPrimaryColumns": ["col2","col3","col4"],
                       "targetAttributeName": "compressedPayload"
                     }
                   }
                 }'
```

## Data Transformation

CQLReplicator supports data transformation during migration using various hash functions. This is useful for
data anonymization, obfuscation, or compliance requirements.

### Supported Hash Functions

- **MurmurHash3**: Fast, non-cryptographic hash function (returns integer)
- **MD5**: 128-bit cryptographic hash (returns hex string)
- **SHA-1**: 160-bit cryptographic hash (returns hex string)
- **SHA-256**: 256-bit cryptographic hash (returns hex string)
- **XXHASH64**: Fast, 64-bit non-cryptographic hash (returns long integer)

### Transform Expression Parameters

- `columnName`: The source column to transform
- `rule`: Hash function to apply (`murmurhash3`, `md5`, `sha-1`, `sha-256`, `xxhash64`)
- `alias`: Optional new column name (if not provided, replaces original column)
- `keepSource`: Boolean to keep original column when using alias (default: `false`)

### Example Usage

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks --src-table users \
                 --trg-keyspace ks --trg-table users \
                 --env ddbdemo --target-type dynamodb \
                 --json-mapping '{
                   "replication": {
                     "dynamoDBPrimaryKey": {"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}
                   },
                   "dynamodb": {
                     "dynamoDBConnection": {"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"},
                     "transformation": {
                       "enabled": true,
                       "transformExpressions": [
                         {"columnName":"email","rule":"md5","alias":"email_hash","keepSource":false},
                         {"columnName":"phone","rule":"sha-256","alias":"phone_hash","keepSource":true},
                         {"columnName":"user_id","rule":"murmurhash3"}
                       ]
                     }
                   }
                 }'
```

This example:
- Hashes `email` to MD5 and replaces it with `email_hash`
- Hashes `phone` to SHA-256 as `phone_hash` while keeping original `phone` column
- Hashes `user_id` in place using MurmurHash3

## Filtering Records
Filter out certain keys during the migration phase using `filterExpression` with Spark SQL syntax:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb \
                 --json-mapping '{
                   "replication": {
                     "dynamoDBPrimaryKey": {"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}
                   },
                   "dynamodb": {
                     "dynamoDBConnection": {"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"},
                     "transformation": {
                       "enabled": true,
                       "filterExpression": "client_id NOT like \"ACME%\""
                     }
                   }
                 }'
```

To filter by non-primary key columns, add them to `addNonPrimaryKeyColumns`:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb \
                 --json-mapping '{
                   "replication": {
                     "dynamoDBPrimaryKey": {"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}
                   },
                   "dynamodb": {
                     "dynamoDBConnection": {"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"},
                     "transformation": {
                       "enabled": true,
                       "addNonPrimaryKeyColumns": ["status"],
                       "filterExpression": "status = \"active\""
                     }
                   }
                 }'
```

Note: The `filterExpression` should be defined as a [Spark SQL Expression](https://archive.apache.org/dist/spark/docs/3.3.0/sql-ref-functions-builtin.html).

## Converting UDTs to JSON
CQLReplicator can automatically convert Cassandra User Defined Types (UDTs) to plain JSON strings,
facilitating easier integration with DynamoDB's schemaless model:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb \
                 --json-mapping '{
                   "replication": {
                     "dynamoDBPrimaryKey": {"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}
                   },
                   "dynamodb": {
                     "dynamoDBConnection": {"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"},
                     "udtConversion": {"enabled": true, "columns": ["device", "user"]}
                   }
                 }'
```

## Handling Large VARINT Values (Over 38 Digits)

By default, AWS Glue converts VARINT columns to `decimal(38,0)` during the read operation.
This causes values exceeding 38 digits to be implicitly converted to `null`, resulting in data loss.

Enable the custom VARINT reader when your source table contains VARINT columns with values exceeding 38 digits:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb \
                 --json-mapping '{
                   "replication": {
                     "readConfiguration": {"useCustomVarintReader": true},
                     "dynamoDBPrimaryKey": {"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}
                   },
                   "dynamodb": {
                     "dynamoDBConnection": {"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"}
                   }
                 }'
```

The custom VARINT reader uses Cassandra's low-level RDD API instead of Spark's DataFrame API, reading VARINT values
directly as strings to preserve their full precision regardless of size.

## Handling Failed Rows (Dead Letter Queue)
When the CQLReplicator encounters persistent failures during replication, failed items are automatically stored in
a Dead Letter Queue (DLQ) directory within your S3 bucket.

Failed items are stored as JSON in the following S3 path format:
```
s3://<BUCKET_NAME>/ks-name/tbl-name/dlq/{tile}/{operation}/log-{timestamp}.msg
```

Where `operation` is `insert`, `update`, or `delete`. Each `.msg` file contains the DynamoDB item serialized as JSON
using the AWS SDK's `ItemUtils.toItem().toJSON` format, which can be deserialized back to `Map<String, AttributeValue>`
for replay.

Common failure scenarios:
- When DynamoDB write capacity is exceeded and all retry attempts (64 with exponential backoff) are exhausted
- When items exceed DynamoDB's 400KB item size limit
- When the target table schema doesn't match expectations

To replay failed items:
1. Stop the CQLReplicator with `request-stop`
2. Run it again with `--replay-log` — this replays failed DynamoDB put/delete operations before starting the replication cycle

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb --replay-log \
                 --json-mapping '{"replication":{"dynamoDBPrimaryKey":{"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}},"dynamodb":{"dynamoDBConnection":{"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"}}}'
```

## Stop migration process
To stop migration process gracefully for all tiles:

```shell
   cqlreplicator --cmd request-stop --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks --src-table tbl --target-type dynamodb --env ddbdemo
```

To stop a single tile:

```shell
   cqlreplicator --cmd request-single-stop --tile 1 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks --src-table tbl --target-type dynamodb --env ddbdemo
```

To force an immediate termination:
```shell
   cqlreplicator --cmd kill --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks --src-table tbl --target-type dynamodb --env ddbdemo
```

## Recover from failures
Restart the migration process from the point where it was interrupted by rerunning your `run` command with the same
parameters. If the discovery job is still running and some of the replicators have failed, use the extra flag
`--skip-discovery`.

## Cleanup the ledger
To reset the CQLReplicator for a specific table, add the `--cleanup-requested` or `--cr` flag to your `run` command.
This cleans up the internal CQLReplicator ledger table, the Iceberg tables from the Glue Data Catalog, and the S3 data.
Don't forget to remove the flag if you want to stop and run it again.

## Get migration stats
To obtain the number of replicated rows during the back filling phase:

```shell
   cqlreplicator --cmd stats --landing-zone s3://<BUCKET_NAME> \
                 --src-keyspace ks --src-table tbl \
                 --target-type dynamodb --region us-east-1 --env ddbdemo
```

To obtain the number of replicated rows after the back filling phase:

```shell
   cqlreplicator --cmd stats --landing-zone s3://<BUCKET_NAME> \
                 --src-keyspace ks --src-table tbl \
                 --target-type dynamodb --region us-east-1 --replication-stats-enabled --env ddbdemo
```

## Retrieve logs from failed Glue jobs
```shell
   cqlreplicator --cmd get-failed-jobs --landing-zone s3://<BUCKET_NAME> \
                 --region us-east-1 --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl
```

## Configure log level
By default, CQLReplicator sets the Spark log level to `ERROR`. You can change this using the `--logging-level` flag:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb --logging-level WARN \
                 --json-mapping '{"replication":{"dynamoDBPrimaryKey":{"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}},"dynamodb":{"dynamoDBConnection":{"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"}}}'
```

Available log levels: `ALL`, `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR` (default), `FATAL`, `OFF`.

## Point in time replication
To replicate data from a specific timestamp, add the `--start-replication-from` flag:

```shell
   cqlreplicator --cmd run --max-wcu-traffic 30000 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --writetime-column data --src-keyspace ks --src-table tbl \
                 --trg-keyspace ks --trg-table tbl \
                 --env ddbdemo --target-type dynamodb \
                 --start-replication-from 1706408365278858 \
                 --json-mapping '{"replication":{"dynamoDBPrimaryKey":{"partitionKeyName":"pk","sortKeyName":"sk","separator":"#"}},"dynamodb":{"dynamoDBConnection":{"endpoint":"dynamodb.us-east-1.amazonaws.com","region":"us-east-1"}}}'
```

To replicate data *before* a timestamp, specify the predicate in json mapping:
```json
{
  "replication": {
    "pointInTimeReplicationConfig": {
      "predicateOp": "lessThanOrEqual"
    }
  }
}
```

## Resize the replication process
To modify the number of tiles (Glue jobs) during the replication process:
1. Safely stop the current migration process (do not force-stop running jobs)
2. Verify all Glue jobs show "COMPLETED" status
3. Run the resize command:

```shell
   cqlreplicator --cmd resize --tiles 8 --landing-zone s3://<BUCKET_NAME> --region us-east-1 \
                 --src-keyspace ks --src-table tbl --target-type dynamodb --env ddbdemo
```

After the resizing job completes, restart CQLReplicator with the new number of tiles `--tiles 8`.

## AWS Glue Monitoring
You can profile and monitor CQLReplicator operations using AWS Glue job profiler. Enable enhanced monitoring
with the `--enhanced-monitoring-enabled` flag during init.

***You may incur additional charges when you enable job metrics and CloudWatch custom metrics are created***

## JSON Mapping Reference

The complete `--json-mapping` schema for the DynamoDB target:

```json
{
  "replication": {
    "allColumns": true,
    "columns": [""],
    "useCustomSerializer": false,
    "replicateWithTimestamp": false,
    "useMaterializedView": {"enabled": false, "mvName": ""},
    "pointInTimeReplicationConfig": {"predicateOp": "greaterThan"},
    "readConfiguration": {
      "splitSizeInMB": 64,
      "concurrentReads": 32,
      "consistencyLevel": "LOCAL_ONE",
      "fetchSizeInRows": 500,
      "queryRetryCount": 180,
      "readTimeoutMS": 120000,
      "useCustomVarintReader": false
    },
    "dynamoDBPrimaryKey": {
      "partitionKeyName": "PK",
      "sortKeyName": "",
      "separator": "#"
    }
  },
  "dynamodb": {
    "dynamoDBConnection": {
      "endpoint": "dynamodb.us-east-1.amazonaws.com",
      "region": "us-east-1"
    },
    "readBeforeWrite": false,
    "writeConfiguration": {
      "maxRetryAttempts": 64,
      "maxStatementsPerBatch": 24,
      "backoff": 25
    },
    "compressionConfig": {
      "enabled": false,
      "compression": "lz4",
      "compressionLevel": 3,
      "compressNonPrimaryColumns": [""],
      "compressAllNonPrimaryKeysColumns": false,
      "targetAttributeName": ""
    },
    "largeObjectsConfig": {
      "enabled": false,
      "columns": [""],
      "bucket": "",
      "prefix": "",
      "keySeparator": "#",
      "compressionEnabled": false
    },
    "transformation": {
      "enabled": false,
      "addNonPrimaryKeyColumns": [],
      "filterExpression": "",
      "transformExpressions": []
    },
    "udtConversion": {"enabled": true, "columns": [""]}
  }
}
```

## Known Issues
| Error Message/Issue | CW Discovery logs | CW Replication logs | Work around |
|---|---|---|---|
| No space left on device | Yes | No | Scale out by increasing the number of DPUs per discovery Glue job, e.g. `--odw 15` |
| No space left on device | Yes | No | Increase `--max-wcu-traffic` |
| Limited IPs in the subnet | Yes | Yes | Create a new Glue connection with a new subnet and attach to the CQLReplicator Glue Job |
| Cassandra timeout during read query at consistency ONE | Yes | No | Reduce page size and increase request timeout in CassandraConnector.conf |
| Cassandra timeout during read query at consistency ONE | Yes | No | Create a materialized view with the same primary keys and one regular column, configure CQLReplicator to use it |
| java.lang.OutOfMemoryError: GC overhead limit exceeded | Yes | No | Increase DPUs for the discovery job using `--override-discovery-workers 12` |
| DynamoDB ProvisionedThroughputExceededException | No | Yes | Increase provisioned WCU on the target table or switch to on-demand capacity mode |
| Item size exceeds 400KB limit | No | Yes | Enable `largeObjectsConfig` to offload large columns to S3 |
| VARINT values exceeding 38 digits converted to null | No | Yes | Enable `useCustomVarintReader: true` in `readConfiguration` |

## Clean up
The following command will delete the Glue job, connector, the S3 bucket, and the ledger table:

```shell
   cqlreplicator --cmd cleanup --landing-zone s3://<BUCKET_NAME> --region "us-east-1"
```

To also clean up Iceberg databases and tables from the Glue Data Catalog:

```shell
   cqlreplicator --cmd cleanup --landing-zone s3://<BUCKET_NAME> --region "us-east-1" \
                 --src-keyspace-list "ks_source_1,ks_source_2"
```

## License
This tool licensed under the Apache-2 License. See the LICENSE file.
