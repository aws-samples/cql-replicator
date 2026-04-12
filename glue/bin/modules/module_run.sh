#!/usr/bin/env bash
# Module: module_run.sh
# Command: run, run-single, sample
# Description: Start migration discovery and replication, single-tile replication, or sampler
# Required params: SOURCE_KS, SOURCE_TBL, TARGET_KS, TARGET_TBL, S3_LANDING_ZONE, AWS_REGION, TARGET_TYPE
# Optional params: TILES, TILE, WORKER_TYPE, DISCOVERY_WORKER_TYPE, OVERRIDE_DISCOVERY_WORKERS, WRITETIME_COLUMN, TTL_COLUMN, SAFE_MODE, REPLICATION_POINT_IN_TIME, CLEANUP_REQUESTED, DEFAULT_WORKLOAD_TYPE, SKIP_DISCOVERY, WCU_TRAFFIC, DEFAULT_WORKERS, ROWS_PER_WORKER, COOLING_PERIOD, ICEBERG_CATALOG, LOGGING, REPLAY_LOG, DEFAULT_ENV, JOB_NAME, PROCESS_TYPE_DISCOVERY, PROCESS_TYPE_REPLICATION, PROCESS_TYPE_SAMPLER

# Internal helper — Build optional REPLICATION_POINT_IN_TIME argument fragment.
# Sets _RPIT_ARG variable: the JSON key-value pair when value > 0, empty otherwise.
_build_rpit_arg() {
    local -n _rp=$1
    local _val="${_rp[REPLICATION_POINT_IN_TIME]:-0}"
    _RPIT_ARG=""
    if [[ "$_val" -gt 0 ]] 2>/dev/null; then
        _RPIT_ARG="\"--REPLICATION_POINT_IN_TIME\":\"'${_val}'\"," 
    fi
}

# Internal helper — Start_Discovery logic
_start_discovery() {
    local -n _params=$1
    check_input "${_params[SOURCE_KS]}" "Error: source keyspace name is empty, must be provided"
    check_input "${_params[SOURCE_TBL]}" "Error: source table name is empty, must be provided"
    check_input "${_params[TARGET_KS]}" "Error: target keyspace name is empty, must be provided"
    check_input "${_params[TARGET_TBL]}" "Error: target table name is empty, must be provided"
    check_input "${_params[S3_LANDING_ZONE]}" "Error: landing zone must be provided"
    check_input "${_params[AWS_REGION]}" "Error: aws region must be provided"
    check_input "${_params[TARGET_TYPE]}" "Error: target type must be provided"
    check_target_table_req "${_params[TARGET_TYPE]}"
    check_num_tiles
    check_glue_job_connection "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" "${_params[AWS_REGION]}"

    log_info "Your workload is divided into ${_params[TILES]} tile(s)"
    log_info "Source: ${_params[SOURCE_KS]}.${_params[SOURCE_TBL]}"
    log_info "Target: ${_params[TARGET_KS]}.${_params[TARGET_TBL]}"
    log_info "Landing zone for artifacts and temp files: ${_params[S3_LANDING_ZONE]}"
    log_info "Column used to detect changes in the source: ${_params[WRITETIME_COLUMN]}"
    log_info "TTL column: ${_params[TTL_COLUMN]}"
    log_info "Each Glue DPU will handle up to ${_params[ROWS_PER_WORKER]} rows (will be ignored if you use --max-wcu-traffic)"
    log_info "Replication start timestamp: ${_params[REPLICATION_POINT_IN_TIME]} (0 means disabled)"
    log_info "Safe mode: ${_params[SAFE_MODE]} (reduces overhead on the source)"
    log_info "Default worker type: ${_params[WORKER_TYPE]}"

    local workers=0
    if [[ ${_params[OVERRIDE_DISCOVERY_WORKERS]} == 0 ]]; then
        max_value2=2
        max_value1=$((2 * ${_params[TILES]} + 1))
        workers=$(max_value $max_value1 $max_value2)
    else
        workers=${_params[OVERRIDE_DISCOVERY_WORKERS]}
    fi
    # TCO
    total_dpu=$(( workers + ( _params[DEFAULT_WORKERS] * _params[TILES] )))
    "$SCRIPT_DIR"/helper --state get-tco --total-dpu $total_dpu --region ${_params[AWS_REGION]}
    log_info "Checking if the discovery job is already running..."
    if [[ ${_params[CLEANUP_REQUESTED]} == "true" ]]; then
        log_info "The ledger is going to be cleaned up"
        confirm "<=== Do you want to continue? ===> "
        log_info "Deleting objects in ${_params[S3_LANDING_ZONE]}/${_params[SOURCE_KS]}/${_params[SOURCE_TBL]}..."
        aws s3 rm "${_params[S3_LANDING_ZONE]}/${_params[SOURCE_KS]}/${_params[SOURCE_TBL]}" --recursive --region "${_params[AWS_REGION]}" > /dev/null 2>&1

        # Clean up old Parquet snapshots from the S3 target path if json-mapping specifies a custom bucket/prefix
        if [[ -n "$JSON_MAPPING" && "$JSON_MAPPING" != "None" ]]; then
          PARQUET_BUCKET=$(echo "$JSON_MAPPING" | jq -r '.s3.bucket // empty')
          PARQUET_PREFIX=$(echo "$JSON_MAPPING" | jq -r '.s3.prefix // empty')
          if [[ -n "$PARQUET_BUCKET" && -n "$PARQUET_PREFIX" ]]; then
            log_info "Deleting old Parquet snapshots in s3://${PARQUET_BUCKET}/${PARQUET_PREFIX}/${_params[TARGET_KS]}/${_params[TARGET_TBL]}..."
            aws s3 rm "s3://${PARQUET_BUCKET}/${PARQUET_PREFIX}/${_params[TARGET_KS]}/${_params[TARGET_TBL]}" --recursive --region "${_params[AWS_REGION]}" > /dev/null 2>&1
          fi
        fi
    fi
    if check_discovery_runs "true"; then
        Delete_Stop_Event_D
        log_info "Starting the discovery job..."
        rs=$(aws glue start-job-run --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" --worker-type "${_params[DISCOVERY_WORKER_TYPE]}" --number-of-workers "$workers" --region "${_params[AWS_REGION]}" --arguments '{"--PROCESS_TYPE":"'${_params[PROCESS_TYPE_DISCOVERY]}'",
            "--TILE":"0",
            "--TOTAL_TILES":"'${_params[TILES]}'",
            "--S3_LANDING_ZONE":"'${_params[S3_LANDING_ZONE]}'",
            "--SOURCE_KS":"'${_params[SOURCE_KS]}'",
            "--SOURCE_TBL":"'${_params[SOURCE_TBL]}'",
            "--TARGET_KS":"'${_params[TARGET_KS]}'",
            "--TARGET_TBL":"'${_params[TARGET_TBL]}'",
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
        DISCOVERY_RUNNING_ID=$rs
        JOBS+=("$rs")
    fi
}

# Internal helper — Start_Replication logic
_start_replication() {
    local -n _params=$1
    cnt=0
    KEYS_PER_TILE=$(aws s3 cp "${_params[S3_LANDING_ZONE]}/${_params[SOURCE_KS]}/${_params[SOURCE_TBL]}/stats/discovery/$cnt/count.json" --region "${_params[AWS_REGION]}" - | head | jq '.primaryKeys')
    log_info "Sampled primary keys per tile is $KEYS_PER_TILE"
    local workers
    if [ "${_params[WCU_TRAFFIC]}" -gt 0 ]; then
        workers=${_params[DEFAULT_WORKERS]}
    else
        workers=$(( 2 + KEYS_PER_TILE/${_params[ROWS_PER_WORKER]} ))
    fi

    if [[ workers -gt 299 ]]; then
        log_info "=== The total number of Glue workers are over 299 per each tile (glue job), you have two options:                  ==="
        log_info "=== [1] Increase the number of tiles. In order to rerun with the higher number of tiles                            ==="
        log_info "=== request a stop for this migration process, and after rerun again with the flag --cleanup-requested             ==="
        log_info "======================================================================================================================"
        log_info "=== [2] Increase the number of primary keys per worker. Default value is ${_params[ROWS_PER_WORKER]} per worker                         ==="
        log_info "=== request a stop for this migration process and rerun again with the flag --override-rows-per-worker <new value> ==="
        exit 1
    fi
    while [ $cnt -lt ${_params[TILES]} ]
    do
        if check_replication_runs $cnt; then
            Delete_Stop_Event_R $cnt
            if [ "${_params[WCU_TRAFFIC]}" -gt 0 ]; then
                workers=${_params[DEFAULT_WORKERS]}
            fi
            rs=$(aws glue start-job-run --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" --worker-type "${_params[WORKER_TYPE]}" --number-of-workers "$workers" --region "${_params[AWS_REGION]}" --arguments '{"--PROCESS_TYPE":"'${_params[PROCESS_TYPE_REPLICATION]}'",
                "--TILE":"'$cnt'",
                "--TOTAL_TILES":"'${_params[TILES]}'",
                "--S3_LANDING_ZONE":"'${_params[S3_LANDING_ZONE]}'",
                "--SOURCE_KS":"'${_params[SOURCE_KS]}'",
                "--SOURCE_TBL":"'${_params[SOURCE_TBL]}'",
                "--TARGET_KS":"'${_params[TARGET_KS]}'",
                "--TARGET_TBL":"'${_params[TARGET_TBL]}'",
                "--WRITETIME_COLUMN":"'${_params[WRITETIME_COLUMN]}'",
                "--SAFE_MODE":"'${_params[SAFE_MODE]}'",
                "--REPLICATION_POINT_IN_TIME":"'${_params[REPLICATION_POINT_IN_TIME]}'",
                "--CLEANUP_REQUESTED":"false",
                "--JSON_MAPPING":"'$JSON_MAPPING_B64'",
                "--REPLAY_LOG":"'${_params[REPLAY_LOG]}'",
                "--TTL_COLUMN":"'${_params[TTL_COLUMN]}'",
                "--WORKLOAD_TYPE":"'${_params[DEFAULT_WORKLOAD_TYPE]}'",
                "--ICEBERG_CATALOG":"'${_params[ICEBERG_CATALOG]}'",
                "--LOGGING":"'${_params[LOGGING]}'"}' --output text)
            JOBS+=("$rs")
            sleep ${_params[COOLING_PERIOD]}
        fi
        ((cnt++))
        progress "$cnt" "${_params[TILES]}" "Starting Glue Jobs"
    done
}

# Internal helper — Start_Single_Replication logic
_start_single_replication() {
    local -n _params=$1
    cnt=${_params[TILE]}
    KEYS_PER_TILE=$(aws s3 cp "${_params[S3_LANDING_ZONE]}/${_params[SOURCE_KS]}/${_params[SOURCE_TBL]}/stats/discovery/$cnt/count.json" --region "${_params[AWS_REGION]}" - | head | jq '.primaryKeys')
    log_info "Sampled primary keys per tile is $KEYS_PER_TILE"
    local workers=$(( 2 + KEYS_PER_TILE/${_params[ROWS_PER_WORKER]} ))
    if [[ workers -gt 299 ]]; then
        log_info "=== The total number of Glue workers are over 299 per each tile (glue job), you have two options:                  ==="
        log_info "=== [1] Increase the number of tiles. In order to rerun with the higher number of tiles                            ==="
        log_info "=== request a stop for this migration process, and after rerun again with the flag --cleanup-requested             ==="
        log_info "======================================================================================================================"
        log_info "=== [2] Increase the number of primary keys per worker. Default value is ${_params[ROWS_PER_WORKER]} per worker                         ==="
        log_info "=== request a stop for this migration process and rerun again with the flag --override-rows-per-worker <new value> ==="
        exit 1
    fi
    if check_replication_runs "$cnt"; then
        Delete_Stop_Event_R "$cnt"
        if [ "${_params[WCU_TRAFFIC]}" -gt 0 ]; then
            workers=${_params[DEFAULT_WORKERS]}
        fi
        rs=$(aws glue start-job-run --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" --worker-type "${_params[WORKER_TYPE]}" --number-of-workers "$workers" --region "${_params[AWS_REGION]}" --arguments '{"--PROCESS_TYPE":"'${_params[PROCESS_TYPE_REPLICATION]}'",
              "--TILE":"'$cnt'",
              "--TOTAL_TILES":"'${_params[TILES]}'",
              "--S3_LANDING_ZONE":"'${_params[S3_LANDING_ZONE]}'",
              "--SOURCE_KS":"'${_params[SOURCE_KS]}'",
              "--SOURCE_TBL":"'${_params[SOURCE_TBL]}'",
              "--TARGET_KS":"'${_params[TARGET_KS]}'",
              "--TARGET_TBL":"'${_params[TARGET_TBL]}'",
              "--WRITETIME_COLUMN":"'${_params[WRITETIME_COLUMN]}'",
              "--SAFE_MODE":"'${_params[SAFE_MODE]}'",
              "--REPLICATION_POINT_IN_TIME":"'${_params[REPLICATION_POINT_IN_TIME]}'",
              "--CLEANUP_REQUESTED":"false",
              "--JSON_MAPPING":"'$JSON_MAPPING_B64'",
              "--REPLAY_LOG":"'${_params[REPLAY_LOG]}'",
              "--TTL_COLUMN":"'${_params[TTL_COLUMN]}'",
              "--WORKLOAD_TYPE":"'${_params[DEFAULT_WORKLOAD_TYPE]}'",
              "--ICEBERG_CATALOG":"'${_params[ICEBERG_CATALOG]}'",
              "--LOGGING":"'${_params[LOGGING]}'"}' --output text)
        JOBS+=("$rs")
    fi
}

# Internal helper — Start_Sampler logic
_start_sampler() {
    local -n _params=$1
    check_input "${_params[SOURCE_KS]}" "Error: source keyspace name is empty, must be provided"
    check_input "${_params[SOURCE_TBL]}" "Error: source table name is empty, must be provided"
    check_input "${_params[S3_LANDING_ZONE]}" "Error: landing zone must be provided"
    check_input "${_params[AWS_REGION]}" "Error: aws region must be provided"
    log_info "SOURCE:" "${_params[SOURCE_KS]}"."${_params[SOURCE_TBL]}"
    log_info "LANDING ZONE:" "${_params[S3_LANDING_ZONE]}"
    check_sampler_runs sampler
    if [ "${_params[PROCESS_RUNNING]}" = false ]; then
        rs=$(aws glue start-job-run --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" \
              --worker-type "G.025X" --number-of-workers 10 --region "${_params[AWS_REGION]}" --arguments '{
              "--PROCESS_TYPE":"'${_params[PROCESS_TYPE_SAMPLER]}'",
              "--TILE":"0",
              "--TOTAL_TILES":"1",
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
        output="Glue job $rs is running"
        sleep 20
        while [ "${_params[PROCESS_RUNNING]}" = true ]; do
            local temp=${spinstr#?}
            printf "\r%s [%c]  " "$output" "$spinstr"
            local spinstr=$temp${spinstr%"$temp"}
            sleep $delay
            check_sampler_runs sampler
        done
        printf "\r%s    \n" "$output"
        sampled_result=$(aws s3 cp "${_params[S3_LANDING_ZONE]}"/"${_params[SOURCE_KS]}"/"${_params[SOURCE_TBL]}"/columnStats/stats.json --region "${_params[AWS_REGION]}" -)
        echo "$sampled_result"
    fi
}

# Entry point for 'run' command
module_run() {
    local -n _params=$1

    if [[ ${_params[CLEANUP_REQUESTED]} != "true" ]]; then
        "$SCRIPT_DIR"/helper --state recovery --tiles "${_params[TILES]}" \
        --landing-zone "${_params[S3_LANDING_ZONE]}" --region "${_params[AWS_REGION]}" \
        --src-keyspace "${_params[SOURCE_KS]}" --src-table "${_params[SOURCE_TBL]}"
        RETURN_CODE=$?
        if [ $RETURN_CODE -ne 0 ]; then
            log_error "Recovery step has failed, the code is ${RETURN_CODE}"
            exit $RETURN_CODE
        fi
    fi

    if [[ ${_params[SKIP_DISCOVERY]} == "false" ]]; then
        _start_discovery "$1"
        barrier "true"
    fi
    _start_replication "$1"
    log_info "Started jobs:" "${JOBS[@]}"
}

# Entry point for 'run-single' command
module_run_single() {
    local -n _params=$1
    check_input "${_params[TILE]}" "Error: tile parameter is empty, must be provided"
    _start_single_replication "$1"
    log_info "Started jobs:" "${JOBS[@]}"
}

# Entry point for 'sample' command
module_sample() {
    local -n _params=$1
    _start_sampler "$1"
}
