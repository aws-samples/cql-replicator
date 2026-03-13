#!/usr/bin/env bash
# Module: module_get_failed_jobs.sh
# Command: get-failed-jobs
# Description: Analyze CloudWatch logs for failed Glue jobs
# Required params: AWS_REGION, JOB_NAME, DEFAULT_ENV
# Optional params: none

# Entry point — called by the dispatcher
# Args: $1 = nameref to params associative array
module_get_failed_jobs() {
    local -n _params=$1
    check_input "${_params[AWS_REGION]}" "Error: aws region must be provided"
    check_input "${_params[SOURCE_KS]}" "Error: source keyspace name is empty, must be provided"
    check_input "${_params[SOURCE_TBL]}" "Error: source table name is empty, must be provided"

    local job_name="${_params[JOB_NAME]}${_params[DEFAULT_ENV]}"
    local region="${_params[AWS_REGION]}"

    log_info "Analyzing logs for job: $job_name"

    # Get only FAILED job runs
    local failed_runs=$(aws glue get-job-runs --job-name "$job_name" --region "$region" --max-items 5 --query 'JobRuns[?JobRunState==`FAILED`] | [0].Id' --output text)

    for run_id in $failed_runs; do
        log_info "=== FAILED Job: $run_id ==="
        if [ "$run_id" != "None" ]; then
          # Check both log groups
          local log_groups=("/aws-glue/jobs/error" "/aws-glue/jobs/output")

          for log_group in "${log_groups[@]}"; do
              log_info "Checking log group $log_group:"
              local streams=$(aws logs describe-log-streams --log-group-name "$log_group" --region "$region" --query "logStreams[?contains(logStreamName, \`$run_id\`)].logStreamName" --output text 2>/dev/null)

              for stream in $streams; do
                  log_info "$log_group  $stream:"

                  # Try ERROR first
                  result=$(aws logs get-log-events --log-group-name "$log_group" --log-stream-name "$stream" --region "$region" --no-start-from-head --limit 1000 --output json | jq -r '.events[] | select(.message | test("ERROR|Caused by")) | .message')

                  if [ -n "$result" ]; then
                      echo "$result"
                      continue
                  fi

                  # Try WARN Exception Error if no "Caused by" or "ERROR"
                  result=$(aws logs get-log-events --log-group-name "$log_group" --log-stream-name "$stream" --region "$region" --no-start-from-head --limit 1000 --output json | jq -r '.events[] | select(.message | test("Exception|Error|WARN|FAILED|Failed")) | .message')

                  if [ -n "$result" ]; then
                      echo "$result"
                      continue
                  fi
              done
          done
        fi
    done
}
