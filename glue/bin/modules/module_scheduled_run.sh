#!/usr/bin/env bash
# Module: module_scheduled_run.sh
# Command: scheduled-run
# Description: Schedule CQLReplicator as a cron job on an EC2 instance
# Required params: TIME_SCHEDULE
# Optional params: WORKLOAD_TYPE

# Entry point — called by the dispatcher
# Args: $1 = nameref to params associative array
module_scheduled_run() {
    local -n _params=$1
    if check_cloudshell; then
        log_error "You can't run cron in CloudShell, please use an EC2 instance"
        exit 1
    else
        log_info "Scheduling CQLReplicator..."
        CMD_PARAMS="${CMD_PARAMS% --cr}"
        CMD_PARAMS="${CMD_PARAMS/scheduled-run/run}"
        if [[ "${_params[WORKLOAD_TYPE]}" = "" ]]; then
            CMD_PARAMS="$CMD_PARAMS --workload-type batch"
        fi
        cqlreplicator_home="$(pwd)"
        cqlreplicator_bin="/bin/bash $cqlreplicator_home/cqlreplicator"
        cmd_final="$cqlreplicator_bin $CMD_PARAMS >> $cqlreplicator_home/cron.log 2>&1"
        # Examples with different minutes:
        # 0 * * * *     # Runs at minute 0 of every hour (1:00, 2:00, 3:00, etc.)
        # 30 * * * *    # Runs at minute 30 of every hour (1:30, 2:30, 3:30, etc.)
        # 15 * * * *    # Runs at minute 15 of every hour (1:15, 2:15, 3:15, etc.)
        # 0/15 * * * *  # Runs every 15 minutes
        log_info "The cron schedule: ${_params[TIME_SCHEDULE]}"
        if (crontab -l 2>/dev/null; echo "${_params[TIME_SCHEDULE]} $cmd_final") | crontab -; then
            log_info "Successfully scheduled cron job"
        else
            log_error "Failed to schedule cron job"
            exit 1
        fi
        exit 0
    fi
}
