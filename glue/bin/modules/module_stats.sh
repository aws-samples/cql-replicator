#!/usr/bin/env bash
# Module: module_stats.sh
# Command: stats
# Description: Gather and display discovery/replication statistics from S3
# Required params: SOURCE_KS, SOURCE_TBL, S3_LANDING_ZONE, AWS_REGION, TARGET_KS, TARGET_TBL, TILES
# Optional params: REPLICATION_STATS_ENABLED

# Internal helper — gathers stats for a single tile and process type
# Args: $1 = nameref name for params, $2 = tile number, $3 = process type
_gather_stats() {
   local -n _params=$1
   local tile=$2
   local process_type=$3
   local total_per_tile=0
   if aws s3 ls "${_params[S3_LANDING_ZONE]}/${_params[SOURCE_KS]}/${_params[SOURCE_TBL]}/stats/" --region "${_params[AWS_REGION]}" > /dev/null
   then
     if [[ $process_type == "${_params[PROCESS_TYPE_DISCOVERY]}" ]]; then
       total_per_tile=$(aws s3 cp "${_params[S3_LANDING_ZONE]}/${_params[SOURCE_KS]}/${_params[SOURCE_TBL]}/stats/$process_type/$tile/count.json" --region "${_params[AWS_REGION]}" - | head | jq '.primaryKeys') && _params[DISCOVERED_TOTAL]=$(( ${_params[DISCOVERED_TOTAL]} + total_per_tile ))
     fi
     if [[ $process_type == "${_params[PROCESS_TYPE_REPLICATION]}" ]]; then
       if  aws s3 ls "${_params[S3_LANDING_ZONE]}/${_params[SOURCE_KS]}/${_params[SOURCE_TBL]}/stats/$process_type/$tile/" --region "${_params[AWS_REGION]}" > /dev/null
         then
         total_per_tile=$(aws s3 cp "${_params[S3_LANDING_ZONE]}/${_params[SOURCE_KS]}/${_params[SOURCE_TBL]}/stats/$process_type/$tile/count.json" --region "${_params[AWS_REGION]}" - | head | jq '.primaryKeys') && _params[REPLICATED_TOTAL]=$(( ${_params[REPLICATED_TOTAL]} + total_per_tile ))
      fi
      if [[ ${_params[REPLICATION_STATS_ENABLED]} == true ]]; then
        if aws s3 ls "${_params[S3_LANDING_ZONE]}/${_params[SOURCE_KS]}/${_params[SOURCE_TBL]}/stats/$process_type/$tile" --region "${_params[AWS_REGION]}" > /dev/null
        then
          # Download count.json once and extract all fields from the cached result
          local stats_json
          stats_json=$(aws s3 cp "${_params[S3_LANDING_ZONE]}/${_params[SOURCE_KS]}/${_params[SOURCE_TBL]}/stats/$process_type/$tile/count.json" --region "${_params[AWS_REGION]}" - | head)
          local inserted=0
          inserted=$(echo "$stats_json" | jq '.insertedPrimaryKeys')
          local updated=0
          updated=$(echo "$stats_json" | jq '.updatedPrimaryKeys')
          local deleted=0
          deleted=$(echo "$stats_json" | jq '.deletedPrimaryKeys')
          local timestamp=""
          timestamp=$(echo "$stats_json" | jq '.updatedTimestamp')
          local header=true
          if [[ $tile != 0 ]]; then
            header=false
          fi
          print_stat_table "$tile" "$inserted" "$updated" "$deleted" "$timestamp" "$header"
        fi
      fi
    fi
  fi
}

# Entry point — called by the dispatcher
# Args: $1 = nameref to params associative array
module_stats() {
    local -n _params=$1
    check_input "${_params[SOURCE_KS]}" "Error: source keyspace name is empty, must be provided"
    check_input "${_params[SOURCE_TBL]}" "Error: source table name is empty, must be provided"
    check_input "${_params[S3_LANDING_ZONE]}" "Error: landing zone must be provided"
    check_input "${_params[AWS_REGION]}" "Error: aws region must be provided"
    check_input "${_params[SOURCE_KS]}" "Error: source keyspace name is empty, must be provided"
    check_input "${_params[SOURCE_TBL]}" "Error: source table name is empty, must be provided"
    check_input "${_params[TARGET_TBL]}" "Error: target table name is empty, must be provided"
    check_input "${_params[TARGET_KS]}" "Error: target keyspace name is empty, must be provided"
    local tile=0
    while [ $tile -lt "${_params[TILES]}" ]
      do
        _gather_stats params $tile "${_params[PROCESS_TYPE_DISCOVERY]}"
        _gather_stats params $tile "${_params[PROCESS_TYPE_REPLICATION]}"
        ((tile++))
      done
    log_info "Discovered rows in" "${_params[SOURCE_KS]}"."${_params[SOURCE_TBL]}" is "$(format_number ${_params[DISCOVERED_TOTAL]})"
    log_info "Replicated rows in" "${_params[TARGET_KS]}"."${_params[TARGET_TBL]}" is "$(format_number ${_params[REPLICATED_TOTAL]})"
    if [[ ${_params[REPLICATION_STATS_ENABLED]} == true ]]; then
      local t=0
      while [ $t -lt "${_params[TILES]}" ]
      do
        _gather_stats params $t "detailed-replication"
        ((t++))
      done
    fi
}
