#!/usr/bin/env bash
# Module: module_kill.sh
# Command: kill
# Description: Kill running discovery and replication Glue jobs for the specified source table
# Required params: JOB_NAME, DEFAULT_ENV, AWS_REGION, SOURCE_KS, SOURCE_TBL
# Optional params: none

# Internal helper — kills running Glue jobs matching a given process type
# Args: $1 = nameref to params associative array, $2 = process type (discovery|replication)
_kill_replication() {
    local -n _params=$1
    local process_type="$2"

    local rs
    rs=$(aws glue get-job-runs --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" --region "${_params[AWS_REGION]}" \
        --query 'JobRuns[?JobRunState==`RUNNING`] | [].{Id: Id, Arguments: Arguments} | [?Arguments."--PROCESS_TYPE"==`'"${process_type}"'`] | [?Arguments."--SOURCE_KS"==`'"${_params[SOURCE_KS]}"'`] | [?Arguments."--SOURCE_TBL"==`'"${_params[SOURCE_TBL]}"'`]' | jq -r '.[].Id')

    for job_id in $rs; do
      aws glue batch-stop-job-run --job-name "${_params[JOB_NAME]}${_params[DEFAULT_ENV]}" --job-run-ids "$job_id" --region "${_params[AWS_REGION]}"
    done
}

# Entry point — called by the dispatcher
# Args: $1 = nameref to params associative array
module_kill() {
    local -n _params=$1
    _kill_replication "$1" discovery
    _kill_replication "$1" replication
}
