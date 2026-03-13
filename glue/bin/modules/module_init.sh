#!/usr/bin/env bash
# Module: module_init.sh
# Command: init
# Description: Deploy CQLReplicator Glue job and download jars
# Required params: AWS_REGION, GLUE_IAM_ROLE
# Optional params: AZ, SUBNET, SG, S3_LANDING_ZONE, SKIP_GLUE_CONNECTOR, TARGET_TYPE, MAIN_SCRIPT_LANDING, DEFAULT_SOURCE, TOKEN, SCB, SKIP_KEYSPACES_LEDGER, GLUE_MONITORING, DEFAULT_ENV, WORKER_TYPE, JOB_NAME, DESCRIPTION, GLUE_TYPE, MAVEN_REPO, ICEBERG_CATALOG, BASE_FOLDER, TRG_SUBNET, TRG_SG, TRG_AZ

# Entry point — called by the dispatcher
# Args: $1 = nameref to params associative array
module_init() {
    local -n _params=$1

    if [[ ${_params[SKIP_GLUE_CONNECTOR]} == false ]]; then
      check_input "${_params[AZ]}" "Error: availability zone is empty, must be provided"
      check_input "${_params[SUBNET]}" "Error: subnet is empty, must be provided"
      check_input "${_params[SG]}" "Error: sg is empty, must be provided"
      validate_security_groups_quoting "${_params[SG]}"
    else
      log_info "Skipping glue connector creation"
    fi
    check_input "${_params[AWS_REGION]}" "Error: region is empty, must be provided"
    log_info "TARGET TYPE: ${_params[TARGET_TYPE]}"

    AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --region "${_params[AWS_REGION]}" --output text)
    log_info "Starting initialization process for AWS account: $AWS_ACCOUNT"
    # Create S3 bucket
    if [[ -z ${_params[S3_LANDING_ZONE]} ]]; then
      log_info "S3 LANDING ZONE is empty"
      bucket=$(echo "cql-replicator-$AWS_ACCOUNT-${_params[AWS_REGION]}${_params[DEFAULT_ENV]}" | tr ' [:upper:]' ' [:lower:]')
      _params[S3_LANDING_ZONE]="s3://$bucket"
      log_info "Creating a new S3 bucket: ${_params[S3_LANDING_ZONE]}"
      if aws s3 mb "${_params[S3_LANDING_ZONE]}" > /dev/null 2>&1
      then
        echo "${_params[S3_LANDING_ZONE]}" > "working_bucket.dat"
      else
        log_error "Error: not able to create a S3 bucket: ${_params[S3_LANDING_ZONE]}"
        exit 1
      fi
    fi

    local astra_spark_creds=""
    local astra_path_scb=""
    if [[ ${_params[DEFAULT_SOURCE]} == astra ]]; then
      check_input "${_params[TOKEN]}" "Error: token is empty, must be provided"
      check_input "${_params[SCB]}" "Error: scb file name is empty, must be provided"
      astra_spark_creds=" --conf spark.cassandra.connection.config.cloud.path=${_params[SCB]} --conf spark.cassandra.auth.username=token --conf spark.cassandra.auth.password=${_params[TOKEN]}"
      astra_path_scb=",${_params[S3_LANDING_ZONE]}/artifacts/${_params[SCB]}"
    fi

  # Uploading the jars
  ARTIFACTS_BASE=("/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.5.1/spark-cassandra-connector-assembly_2.12-3.5.1.jar"
  "/software/aws/mcs/aws-sigv4-auth-cassandra-java-driver-plugin/4.0.9/aws-sigv4-auth-cassandra-java-driver-plugin-4.0.9.jar")
  ARTIFACTS_KEYSPACES=("/io/vavr/vavr/0.10.4/vavr-0.10.4.jar" "/io/github/resilience4j/resilience4j-retry/1.7.1/resilience4j-retry-1.7.1.jar" "/io/github/resilience4j/resilience4j-core/1.7.1/resilience4j-core-1.7.1.jar")
  ARTIFACTS_MEMORYDB=("/redis/clients/jedis/4.4.6/jedis-4.4.6.jar")
  ARTIFACTS_OSS=("/org/opensearch/client/opensearch-spark-30_2.12/1.0.1/opensearch-spark-30_2.12-1.0.1.jar" "/org/opensearch/driver/opensearch-sql-jdbc/1.4.0.1/opensearch-sql-jdbc-1.4.0.1.jar")
  S3_PATH_BASE=("${_params[S3_LANDING_ZONE]}/artifacts/spark-cassandra-connector-assembly_2.12-3.5.1.jar" "${_params[S3_LANDING_ZONE]}/artifacts/aws-sigv4-auth-cassandra-java-driver-plugin-4.0.9.jar")
  S3_PATH_KEYSPACES=("${_params[S3_LANDING_ZONE]}/artifacts/vavr-0.10.4.jar" "${_params[S3_LANDING_ZONE]}/artifacts/resilience4j-retry-1.7.1.jar" "${_params[S3_LANDING_ZONE]}/artifacts/resilience4j-core-1.7.1.jar")
  S3_PATH_MEMORYDB=("${_params[S3_LANDING_ZONE]}/artifacts/jedis-4.4.6.jar")
  S3_PATH_OSS=("${_params[S3_LANDING_ZONE]}/artifacts/opensearch-spark-30_2.12-1.0.1.jar" "${_params[S3_LANDING_ZONE]}/artifacts/opensearch-sql-jdbc-1.4.0.1.jar")

  uploader_jars "${ARTIFACTS_BASE[@]}"
  join_array "${S3_PATH_BASE[@]}"

  if [[ ${_params[TARGET_TYPE]} == "keyspaces" ]]; then
    uploader_jars "${ARTIFACTS_KEYSPACES[@]}"
    S3_PATH_LIBS+=","
    join_array "${S3_PATH_KEYSPACES[@]}"
    validate_iam_role_permissions "${_params[GLUE_IAM_ROLE]}" "${_params[AWS_REGION]}" "$AWS_ACCOUNT"
    security_group_id=$(echo "${_params[SG]}" | tr -d "'\"")
    check_security_group_self_referencing "$security_group_id"
  fi

  if [[ ${_params[TARGET_TYPE]} == "dynamodb" ]]; then
      uploader_jars "${ARTIFACTS_KEYSPACES[@]}"
      S3_PATH_LIBS+=","
      join_array "${S3_PATH_KEYSPACES[@]}"
      validate_iam_role_permissions "${_params[GLUE_IAM_ROLE]}" "${_params[AWS_REGION]}" "$AWS_ACCOUNT"
      security_group_id=$(echo "${_params[SG]}" | tr -d "'\"")
      check_security_group_self_referencing "$security_group_id"
  fi

  if [[ ${_params[TARGET_TYPE]} == "memorydb" ]]; then
    uploader_jars "${ARTIFACTS_MEMORYDB[@]}"
    S3_PATH_LIBS+=","
    join_array "${S3_PATH_MEMORYDB[@]}"
  fi

  if [[ ${_params[TARGET_TYPE]} == "opensearch" ]]; then
    uploader_jars "${ARTIFACTS_OSS[@]}"
    S3_PATH_LIBS+=","
    join_array "${S3_PATH_OSS[@]}"
  fi

  # Uploading the config files
  local path_to_conf
  local path_to_scala
  path_to_conf=$(ls -d "${_params[BASE_FOLDER]}" | sed 's/bin/conf/g')
  path_to_scala=$(ls -d "${_params[BASE_FOLDER]}" | sed 's/bin/sbin/g')"/${_params[TARGET_TYPE]}"

  if [[ ${_params[TARGET_TYPE]} == "memorydb" ]]; then
    uploader_helper "RedisConnector.conf" 0 1 5
  fi

  if [[ ${_params[TARGET_TYPE]} == "opensearch" ]]; then
    uploader_helper "OpenSearchConnector.conf" 0 1 5
  fi

  # KeyspacesConnector.conf is needed for ledger access (keyspaces, memorydb, opensearch targets)
  # DynamoDB target uses a DynamoDB-based ledger — no Keyspaces connection needed
  if [[ ${_params[TARGET_TYPE]} != "dynamodb" ]]; then
    uploader_helper "KeyspacesConnector.conf" 0 1 5
  fi

  # Source C*/K*
  uploader_helper "CassandraConnector.conf" 1 2 5

  local glue_bucket_artifacts
  if [[ "${_params[MAIN_SCRIPT_LANDING]}" = false ]]; then
    glue_bucket_artifacts=s3://aws-glue-assets-"$AWS_ACCOUNT"-"${_params[AWS_REGION]}"
  else
    glue_bucket_artifacts="${_params[S3_LANDING_ZONE]}"
  fi

  if aws s3 ls "$glue_bucket_artifacts"/scripts/ --region "${_params[AWS_REGION]}" > /dev/null --region "${_params[AWS_REGION]}"
  then
    aws s3 cp "$path_to_scala"/CQLReplicator.scala "$glue_bucket_artifacts"/scripts/${_params[JOB_NAME]}${_params[DEFAULT_ENV]}.scala --region "${_params[AWS_REGION]}" > /dev/null
  else
    aws s3 mb "$glue_bucket_artifacts" --region "${_params[AWS_REGION]}" > /dev/null 2>&1
    sleep 25
    if ls "$path_to_scala"/CQLReplicator.scala
    then
      progress 3 5 "Uploading CQLReplicator.scala                  "
      aws s3 cp "$path_to_scala"/CQLReplicator.scala "$glue_bucket_artifacts"/scripts/${_params[JOB_NAME]}${_params[DEFAULT_ENV]}.scala --region "${_params[AWS_REGION]}" > /dev/null
    else
      log_error "Error: $path_to_scala/CQLReplicator.scala not found"
      exit 1
    fi
  fi

  # Upload the SCB file if the source is astra
  if [[ ${_params[DEFAULT_SOURCE]} == "astra" ]]; then
    uploader_helper "${_params[SCB]}" 0 1 1
  fi

  # Create Glue Connector
  local glue_conn_name
  local enhanced_monitoring=""
  if [[ "${_params[GLUE_MONITORING]}" == true ]]; then
      enhanced_monitoring=',"--enable-continuous-cloudwatch-log":"true","--enable-continuous-log-filter":"true","--enable-metrics":"true","--enable-observability-metrics":"true"'
  fi
  local conf_string="spark.files=${_params[S3_LANDING_ZONE]}/artifacts/KeyspacesConnector.conf,${_params[S3_LANDING_ZONE]}/artifacts/CassandraConnector.conf${astra_path_scb} --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.${_params[ICEBERG_CATALOG]}=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.${_params[ICEBERG_CATALOG]}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.${_params[ICEBERG_CATALOG]}.warehouse=${_params[S3_LANDING_ZONE]} --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true --conf spark.kryoserializer.buffer.max=128m --conf spark.rdd.compress=true --conf spark.cleaner.periodicGC.interval=1min --conf spark.kryo.referenceTracking=false --conf spark.cleaner.referenceTracking.cleanCheckpoints=true --conf spark.task.maxFailures=64 --conf spark.shuffle.file.buffer=1mb --conf spark.io.compression.lz4.blockSize=512kb --conf spark.sql.shuffle.partitions=100 --conf spark.default.parallelism=100 --conf spark.locality.wait=0${astra_spark_creds}"
  
  if [[ -n "${astra_path_scb:1}" ]]; then
    local default_args=$(jq -n \
      --arg job_lang "scala" \
      --arg extra_jars "$S3_PATH_LIBS" \
      --arg conf "$conf_string" \
      --arg extra_files "${astra_path_scb:1}" \
      --arg class "GlueApp" \
      --arg datalake_formats "iceberg" \
      '{
        "--job-language": $job_lang,
        "--extra-jars": $extra_jars,
        "--conf": $conf,
        "--extra-files": $extra_files,
        "--class": $class,
        "--datalake-formats": $datalake_formats
      }')
  else
    local default_args=$(jq -n \
      --arg job_lang "scala" \
      --arg extra_jars "$S3_PATH_LIBS" \
      --arg conf "$conf_string" \
      --arg class "GlueApp" \
      --arg datalake_formats "iceberg" \
      '{
        "--job-language": $job_lang,
        "--extra-jars": $extra_jars,
        "--conf": $conf,
        "--class": $class,
        "--datalake-formats": $datalake_formats
      }')
  fi

  if [[ ${_params[SKIP_GLUE_CONNECTOR]} == false ]]; then
      progress 3 5 "Creating Glue artifacts                             "

      # Check if Glue job already exists before creating resources
      if aws glue get-job --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" --region "${_params[AWS_REGION]}" > /dev/null 2>&1; then
        log_warn "Glue job '${_params[JOB_NAME]}${_params[DEFAULT_ENV]}' already exists."
        read -rp "Do you want to recreate the job and connection? (y/n): " user_choice
        if [[ "$user_choice" == "y" || "$user_choice" == "Y" ]]; then
          local existing_conn
          existing_conn=$(aws glue get-job --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" --region "${_params[AWS_REGION]}" --query 'Job.Connections.Connections[0]' --output text 2>/dev/null)
          log_info "Deleting existing Glue job '${_params[JOB_NAME]}${_params[DEFAULT_ENV]}'..."
          aws glue delete-job --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" --region "${_params[AWS_REGION]}" > /dev/null
          if [[ -n "$existing_conn" && "$existing_conn" != "None" ]]; then
            log_info "Deleting existing Glue connection '$existing_conn'..."
            aws glue delete-connection --connection-name "$existing_conn" --region "${_params[AWS_REGION]}" > /dev/null 2>&1
          fi
        else
          log_info "Skipping Glue job and connection creation. Using existing resources."
          # Retrieve existing connection name for downstream use
          glue_conn_name=$(aws glue get-job --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" --region "${_params[AWS_REGION]}" --query 'Job.Connections.Connections[0]' --output text 2>/dev/null)
          return 0
        fi
      fi

      glue_conn_name=$(echo cql-replicator-"$(uuidgen)" | tr ' [:upper:]' ' [:lower:]')
      aws glue create-connection --connection-input '{
         "Name":"'$glue_conn_name${_params[DEFAULT_ENV]}'",
         "Description":"CQLReplicator connection to the C* cluster",
         "ConnectionType":"NETWORK",
         "ConnectionProperties":{
           "JDBC_ENFORCE_SSL": "false"
           },
         "PhysicalConnectionRequirements":{
           "SubnetId":"'${_params[SUBNET]}'",
           "SecurityGroupIdList":['${_params[SG]}'],
           "AvailabilityZone":"'${_params[AZ]}'"}
           }' --region "${_params[AWS_REGION]}" --endpoint https://glue."${_params[AWS_REGION]}".amazonaws.com --output json

       if [[ ${_params[TARGET_TYPE]} == "opensearch" || ${_params[TARGET_TYPE]} == "memorydb" ]]; then
         check_input "${_params[TRG_SUBNET]}" "Error: subnet for ${_params[TARGET_TYPE]} is empty, must be provided"
         check_input "${_params[TRG_SG]}" "Error: sg for ${_params[TARGET_TYPE]} is empty, must be provided"
         check_input "${_params[TRG_AZ]}" "Error: az for ${_params[TARGET_TYPE]} is empty, must be provided"
         glue_conn_name_oss="cql-replicator-${_params[TARGET_TYPE]}-integration"
         glue_conn_name="$glue_conn_name${_params[DEFAULT_ENV]},$glue_conn_name_oss"

         aws glue create-connection --connection-input '{
          "Name":"'$glue_conn_name_oss${_params[DEFAULT_ENV]}'",
          "Description":"CQLReplicator connection to '${_params[TARGET_TYPE]}'",
          "ConnectionType":"NETWORK",
          "ConnectionProperties":{
            "JDBC_ENFORCE_SSL": "false"
          },
          "PhysicalConnectionRequirements":{
            "SubnetId":"'${_params[TRG_SUBNET]}'",
            "SecurityGroupIdList":['${_params[TRG_SG]}'],
            "AvailabilityZone":"'${_params[TRG_AZ]}'"}
          }' --region "${_params[AWS_REGION]}" --endpoint https://glue."${_params[AWS_REGION]}".amazonaws.com --output json
       fi
       aws glue create-job \
           --name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" \
           --role "${_params[GLUE_IAM_ROLE]}" \
           --description "${_params[DESCRIPTION]} for ${_params[TARGET_TYPE]}" \
           --glue-version "5.1" \
           --number-of-workers 2 \
           --worker-type "${_params[WORKER_TYPE]}" \
           --connections "Connections=$glue_conn_name${_params[DEFAULT_ENV]}" \
           --command "Name=${_params[GLUE_TYPE]},ScriptLocation=$glue_bucket_artifacts/scripts/${_params[JOB_NAME]}${_params[DEFAULT_ENV]}.scala" \
           --execution-property '{"MaxConcurrentRuns": 64}' \
           --max-retries 1 \
           --region "${_params[AWS_REGION]}" \
           --default-arguments "$default_args" > /dev/null
   fi

  if [[ ${_params[SKIP_GLUE_CONNECTOR]} == true ]]; then
        progress 3 5 "Creating Glue artifacts                             "

        # Check if Glue job already exists before creating
        if aws glue get-job --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" --region "${_params[AWS_REGION]}" > /dev/null 2>&1; then
          log_warn "Glue job '${_params[JOB_NAME]}${_params[DEFAULT_ENV]}' already exists."
          read -rp "Do you want to recreate the job? (y/n): " user_choice
          if [[ "$user_choice" == "y" || "$user_choice" == "Y" ]]; then
            log_info "Deleting existing Glue job '${_params[JOB_NAME]}${_params[DEFAULT_ENV]}'..."
            aws glue delete-job --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" --region "${_params[AWS_REGION]}" > /dev/null
          else
            log_info "Skipping Glue job creation. Using existing job."
            return 0
          fi
        fi

        aws glue create-job \
            --name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" \
            --role "${_params[GLUE_IAM_ROLE]}" \
            --description "${_params[DESCRIPTION]} -> ${_params[TARGET_TYPE]}" \
            --glue-version "5.0" \
            --number-of-workers 2 \
            --worker-type "${_params[WORKER_TYPE]}" \
            --command "Name=${_params[GLUE_TYPE]},ScriptLocation=$glue_bucket_artifacts/scripts/${_params[JOB_NAME]}${_params[DEFAULT_ENV]}.scala" \
            --execution-property '{"MaxConcurrentRuns": 64}' \
            --max-retries 1 \
            --region "${_params[AWS_REGION]}" \
            --default-arguments "$default_args" > /dev/null
    fi

  if [[ ${_params[SKIP_KEYSPACES_LEDGER]} == true || ${_params[TARGET_TYPE]} == "dynamodb" ]]; then
    progress 4 5 "Skipping CQLReplicator's internal keyspace          "
    progress 5 5 "Skipping CQLReplicator's internal table             "
  fi

  if [[ ${_params[SKIP_KEYSPACES_LEDGER]} == false && ${_params[TARGET_TYPE]} != "dynamodb" ]]; then
    progress 4 5 "Creating CQLReplicator's internal resources         "
    # Create a keyspace - migration
    aws keyspaces create-keyspace --keyspace-name "$INT_KS_NAME" --region "${_params[AWS_REGION]}" > /dev/null
    sleep 20

    # Create a table - ledger
    aws keyspaces create-table --keyspace-name "$INT_KS_NAME" --table-name "$INT_TBL_NAME" --schema-definition '{
    "allColumns": [ { "name": "ks", "type": "text" },
    { "name": "tbl", "type": "text" },
    { "name": "tile", "type": "int" },
    { "name": "ver", "type": "text" },
    { "name": "dt_load", "type": "timestamp" },
    { "name": "dt_offload", "type": "timestamp" },
    { "name": "load_status", "type": "text" },
    { "name": "location", "type": "text" },
    { "name": "offload_status", "type": "text" } ],
    "partitionKeys": [ { "name": "ks" }, { "name": "tbl" } ],
    "clusteringKeys": [ { "name": "tile", "orderBy": "ASC" }, { "name": "ver", "orderBy": "ASC" } ] }' --region "${_params[AWS_REGION]}" > /dev/null
  progress 5 5 "Created the CQLReplicator internal resources        "
fi

log_info "Deploy is completed"
}
