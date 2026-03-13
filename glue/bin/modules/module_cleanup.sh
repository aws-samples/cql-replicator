#!/usr/bin/env bash
# Module: module_cleanup.sh
# Command: cleanup
# Description: Delete all CQLReplicator artifacts (S3 bucket, Glue connections, Glue job, Iceberg databases)
# Required params: S3_LANDING_ZONE, AWS_REGION, JOB_NAME, DEFAULT_ENV
# Optional params: SOURCE_KS_LIST, SKIP_KEYSPACES_LEDGER, TARGET_TYPE

# Entry point — called by the dispatcher
# Args: $1 = nameref to params associative array
module_cleanup() {
    local -n _params=$1

    log_info "Deleting deployed artifacts: the glue connection (optional), the S3 bucket, and the glue job"

    check_input "${_params[S3_LANDING_ZONE]}" "Error: landing zone parameter is empty, must be provided"
    check_input "${_params[AWS_REGION]}" "Error: AWS Region is empty, must be provided"
    aws s3 rm "${_params[S3_LANDING_ZONE]}" --recursive --region "${_params[AWS_REGION]}"
    aws s3 rb "${_params[S3_LANDING_ZONE]}" --region "${_params[AWS_REGION]}"
    local connection_name
    connection_name=$(aws glue get-job --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" --query 'Job.Connections.Connections[0]' --output text)
    aws glue delete-connection --connection-name "$connection_name" --region "${_params[AWS_REGION]}" > /dev/null 2>&1
    aws glue delete-connection --connection-name "cql-replicator-memorydb-integration${_params[DEFAULT_ENV]}" --region "${_params[AWS_REGION]}" > /dev/null 2>&1
    aws glue delete-connection --connection-name "cql-replicator-opensearch-integration${_params[DEFAULT_ENV]}" --region "${_params[AWS_REGION]}" > /dev/null 2>&1
    aws glue delete-job --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" --region "${_params[AWS_REGION]}"

    # Clean up Iceberg databases/tables from Glue Data Catalog
    if [[ -n "${_params[SOURCE_KS_LIST]}" ]]; then
      IFS=',' read -ra ks_array <<< "${_params[SOURCE_KS_LIST]}"
      for ks_name in "${ks_array[@]}"; do
        ks_name=$(echo "$ks_name" | tr -d ' ')
        local db_name="${ks_name}_db"
        local tables
        tables=$(aws glue get-tables --database-name "$db_name" --region "${_params[AWS_REGION]}" --query 'TableList[].Name' --output text 2>/dev/null) || true
        for tbl_name in $tables; do
          aws glue delete-table --database-name "$db_name" --name "$tbl_name" --region "${_params[AWS_REGION]}" > /dev/null 2>&1
          log_info "Deleted Glue table $db_name.$tbl_name"
        done
        aws glue delete-database --name "$db_name" --region "${_params[AWS_REGION]}" > /dev/null 2>&1
        log_info "Deleted Glue database $db_name"
      done
    else
      log_warn "Glue Data Catalog databases were not cleaned up. Use --src-keyspace-list 'ks1,ks2' to specify which Iceberg databases to delete."
    fi

    if [[ ${_params[SKIP_KEYSPACES_LEDGER]} == false && ${_params[TARGET_TYPE]} != "dynamodb" ]]; then
      aws keyspaces delete-keyspace --keyspace-name "$INT_KS_NAME" --region "${_params[AWS_REGION]}"
    fi
}
