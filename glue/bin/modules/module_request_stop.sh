#!/usr/bin/env bash
# Module: module_request_stop.sh
# Command: request-stop, request-single-stop
# Description: Request stop for discovery and replication jobs, or a single replication tile
# Required params: S3_LANDING_ZONE, SOURCE_KS, SOURCE_TBL, AWS_REGION, TILES
# Optional params: TILE

# Entry point for 'request-stop' command — stops discovery + all replication tiles
# Args: $1 = nameref to params associative array
module_request_stop() {
    local -n _params=$1
    local tile=0
    log_info "Requested a stop for the discovery job"
    if aws s3api put-object --bucket "${_params[S3_LANDING_ZONE]:5}" --key "${_params[SOURCE_KS]}/${_params[SOURCE_TBL]}/discovery/stopRequested" --region "${_params[AWS_REGION]}" >/dev/null
    then
      while [ $tile -lt ${_params[TILES]} ]
      do
        log_info "Requested a stop for the replication tile: $tile"
        aws s3api put-object --bucket "${_params[S3_LANDING_ZONE]:5}" --key "${_params[SOURCE_KS]}/${_params[SOURCE_TBL]}/replication/$tile/stopRequested" --region "${_params[AWS_REGION]}" >/dev/null
        ((tile++))
      done
    fi
}

# Entry point for 'request-single-stop' command — stops a single replication tile
# Args: $1 = nameref to params associative array
module_request_single_stop() {
    local -n _params=$1
    check_input "${_params[TILE]}" "Error: tile parameter is empty, must be provided"
    local tile=${_params[TILE]}
    log_info "Requested a stop for the replication tile: $tile"
    aws s3api put-object --bucket "${_params[S3_LANDING_ZONE]:5}" --key "${_params[SOURCE_KS]}/${_params[SOURCE_TBL]}/replication/$tile/stopRequested" --region "${_params[AWS_REGION]}" >/dev/null
}
