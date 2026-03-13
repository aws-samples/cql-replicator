#!/usr/bin/env bash
# Module: module_resize.sh
# Command: resize
# Description: Resize the number of tiles for a running migration
# Required params: SOURCE_KS, SOURCE_TBL, S3_LANDING_ZONE, AWS_REGION, TILES
# Optional params: WORKER_TYPE, WRITETIME_COLUMN, TTL_COLUMN, SAFE_MODE, REPLICATION_POINT_IN_TIME, CLEANUP_REQUESTED, DEFAULT_WORKLOAD_TYPE, ICEBERG_CATALOG, LOGGING, REPLAY_LOG, DEFAULT_ENV, JOB_NAME

# Entry point — called by the dispatcher
# Args: $1 = nameref to params associative array
module_resize() {
    local -n _params=$1
    check_input "${_params[SOURCE_KS]}" "Error: source keyspace name is empty, must be provided"
    check_input "${_params[SOURCE_TBL]}" "Error: source table name is empty, must be provided"
    check_input "${_params[S3_LANDING_ZONE]}" "Error: landing zone must be provided"
    check_input "${_params[AWS_REGION]}" "Error: aws region must be provided"
    check_input "${_params[TILES]}" "Error: new number of tiles should be provided"
    log_info "SOURCE:" "${_params[SOURCE_KS]}"."${_params[SOURCE_TBL]}"
    log_info "LANDING ZONE:" "${_params[S3_LANDING_ZONE]}"
    log_info "WORKER_TYPE: ${_params[WORKER_TYPE]}"
    log_warn "The replication process for ${_params[SOURCE_KS]}.${_params[SOURCE_TBL]} is going to be resized"
    log_warn "Before proceeding the replication process must be stopped"
    log_info "Resizing the tiles. The new size is ${_params[TILES]}"
    confirm "<=== Do you want to continue? ===> "
    check_sampler_runs resize
    if [ "${_params[PROCESS_RUNNING]}" = false ]; then
        # Iceberg-based resize: the Scala recomputeTiles() reads directly from per-tile Iceberg tables
        # No need to copy/delete primaryKeys Parquet files — Iceberg handles data via the Glue Catalog
        log_info "Clearing stats before resize"
        aws s3 rm "${_params[S3_LANDING_ZONE]}/${_params[SOURCE_KS]}/${_params[SOURCE_TBL]}/stats" \
                      --recursive --region "${_params[AWS_REGION]}" >/dev/null
        rs=$(aws glue start-job-run --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" --worker-type "G.1X" --number-of-workers $(( ${_params[TILES]} + 1 )) --region "${_params[AWS_REGION]}" --arguments '{
            "--PROCESS_TYPE":"resize",
            "--TILE":"0",
            "--TOTAL_TILES":"'${_params[TILES]}'",
            "--S3_LANDING_ZONE":"'${_params[S3_LANDING_ZONE]}'",
            "--SOURCE_KS":"'${_params[SOURCE_KS]}'",
            "--SOURCE_TBL":"'${_params[SOURCE_TBL]}'",
            "--TARGET_KS":"'${_params[SOURCE_KS]}'",
            "--TARGET_TBL":"'${_params[SOURCE_TBL]}'",
            "--WRITETIME_COLUMN":"'${_params[WRITETIME_COLUMN]}'",
            "--SAFE_MODE":"'${_params[SAFE_MODE]}'",
            "--REPLICATION_POINT_IN_TIME":"'${_params[REPLICATION_POINT_IN_TIME]}'",
            "--CLEANUP_REQUESTED":"'${_params[CLEANUP_REQUESTED]}'",
            "--JSON_MAPPING":"'$JSON_MAPPING_B64'",
            "--REPLAY_LOG":"'${_params[REPLAY_LOG]}'",
            "--TTL_COLUMN":"'${_params[TTL_COLUMN]}'",
            "--WORKLOAD_TYPE":"'${_params[DEFAULT_WORKLOAD_TYPE]}'",
            "--ICEBERG_CATALOG":"'${_params[ICEBERG_CATALOG]}'",
            "--LOGGING":"'${_params[LOGGING]}'"}' --output text)
        _params[PROCESS_RUNNING]=true
        local delay=0.1
        local spinstr='|/-\'
        output="Glue job $rs is running. Resizing in progress..."
        sleep 20
        while [ "${_params[PROCESS_RUNNING]}" = true ]; do
            local temp=${spinstr#?}
            printf "\r%s [%c]  " "$output" "$spinstr"
            local spinstr=$temp${spinstr%"$temp"}
            sleep $delay
            check_sampler_runs resize
        done
        printf "\r%s    \n" "$output"
    fi
}
