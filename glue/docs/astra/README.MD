# Replicate workload to Amazon Keyspaces from AstraDB

## Prepare the configuration files
In order to connect to Amazon Keyspaces and the Cassandra cluster you need configure reference 
configuration files `../glue/conf/CassandraConnector.conf` and `../glue/conf/KeyspacesConnector.conf` 
in ../conf accordingly to the environment.

## Configure

### Get secure-connect-bundle from your AstraDB console
Open your console, click `Get Bundle`, and place the bundle in the ../conf folder. The Secure Connect Bundle contains the SSL
and TLS artifacts needed for CQLReplicator to connect.

### Get an application token
The application token facilitates secure connections between the CQLReplicator and your Astra DB instance. 
To generate the database token, select the `Read Only User` option and proceed with the token generation process.
```json
{
    "clientId": "......",
    "secret": "-......",
    "token": "AstraCS:<TOKEN>"
}
```
In the subsequent step, you will only require the `token` for the `CassandraConnector.conf` and `init` command.

### Prepare CassandraConnector.conf

```
datastax-java-driver {
    basic {
        session-keyspace = "your-astra-keyspace"
        cloud {
        # Provide the relative or absolute path to your Secure Connect Bundle ZIP file
        secure-connect-bundle = "your-secure-connect-db.zip"
        }
    }
        advanced {
            auth-provider {
            class = PlainTextAuthProvider
            username = "token"
            password = "AstraCS:<TOKEN>"
        }
    }
}
```

## Init migration process
The following command initializes the CQLReplicator environment, which involves the copying JAR artifacts, creation a Glue connector,
a S3 bucket, a Glue job, migration keyspace, and ledger table.

```shell
    cd cql-replicator/glue/bin &&
    cqlreplicator --cmd init --skip-glue-connector \ # You will require the glue connector if you intend to connect to AstraDB solely via the VPC.
                  --source-type astra \
                  --target-type keyspaces \
                  --glue-iam-role glue-cassandra-migration \
                  --landing-zone s3://<PROVIDED_BUCKET_NAME> \
                  --token AstraCS:<TOKEN> \
                  --scb your-secure-connect-bundel.zip
```

Other functionalities remain unchanged when migrating from Cassandra to Amazon Keyspaces. Please refer to this [guide](../keyspaces/README.MD) for further details.

## License
This tool licensed under the Apache-2 License. See the LICENSE file.