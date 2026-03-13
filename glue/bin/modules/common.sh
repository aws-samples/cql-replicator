#!/usr/bin/env bash
# common.sh — Shared library for CQLReplicator modules
# Sourced by the dispatcher before any command module.
# // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# // SPDX-License-Identifier: Apache-2.0

# ── Color codes ──────────────────────────────────────────────────────────────
GREEN='\033[1;32m'
NC='\033[0m' # No Color
LIGHT_GREEN='\033[1;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'

# ── Progress bar configuration ───────────────────────────────────────────────
PS=40
PCC="|"
PCU="-"
PPS=2

# ── Logging ──────────────────────────────────────────────────────────────────
log_info() { echo -e "${LIGHT_GREEN}INFO [$(date -Iseconds)]${NC} $*"; }
log_error() { echo -e "${RED}ERROR [$(date -Iseconds)]${NC} $*"; }
log_warn() { echo -e "${YELLOW}WARN [$(date -Iseconds)]${NC} $*"; }

# ── Display helpers ──────────────────────────────────────────────────────────
format_number() {
    local n=$1
    local formatted=""
    local len=${#n}

    for (( i=$len-1; i>=0; i-- )); do
        if [ $(( ($len-$i-1) % 3 )) -eq 0 ] && [ $i -lt $(($len-1)) ]; then
            formatted=",${formatted}"
        fi
        formatted="${n:$i:1}${formatted}"
    done
    echo "$formatted"
}

print_stat_table() {
  local tile=$1
  local inserts=$2
  local updates=$3
  local deletes=$4
  local timestamp=$5
  local head=$6
  if [[ $head == true ]]; then
    echo "+------------------------------------------------------------------------+"
    printf "| %-8s | %-8s | %-8s | %-8s | %-20s       |\n" "Tile" "Inserts" "Updates" "Deletes" "Timestamp"
    echo "+------------------------------------------------------------------------+"
  fi
  printf "| %-8d | %-8d | %-8d | %-8d | %-20s  |\n" $tile $inserts $updates $deletes "$timestamp"
  echo "+------------------------------------------------------------------------+"
}

function progress {
  local current="$1"
  local total="$2"
  local title="$3"
  percent=$(bc <<< "scale=$PPS; 100 * $current / $total")
  completed=$(bc <<< "scale=0; $PS * $percent / 100")
  uncompleted=$(bc <<< "scale=0; $PS - $completed")
  completed_sub_bar=$(printf "%${completed}s" | tr " " "${PCC}")
  uncompleted_sub_bar=$(printf "%${uncompleted}s" | tr " " "${PCU}")
  echo -ne "\r$title" : [${completed_sub_bar}${uncompleted_sub_bar}] ${percent}%
  if [ "$total" -eq "$current" ]; then
      echo -e " - COMPLETED"
  fi
}

# ── Validation ───────────────────────────────────────────────────────────────
function check_input() {
  local input=$1
  local param_name=$2
  if [[ -z $input ]]; then
      log_error "Parameter $param_name empty or null"
      exit 1
  fi
  return 0
}

function check_file_exists() {
  file=$1
  if [ ! -f "$file" ]; then
    log_error "File $file doesn't exists, please place files correctly"
    exit 1
  fi
}

function validate_json() {
  local json_str=$1
  echo "$json_str" | jq empty
  if [[ $? -ne 0 ]]; then
      log_error "Error: Invalid JSON"
      log_error '{"column": "column_name", "bucket": "bucket-name", "prefix": "keyspace_name/table_name/payload", "xref": "reference-column"}'
      exit 1
  fi
  empty_values=$(echo "$json_str" | jq 'recurse | select(. == "" or . == null)')
  if [[ -n $empty_values ]]; then
      log_error "Error: JSON contains empty values"
      return 1
  fi
  local column
  local bucket
  local prefix
  local xref
  column=$(echo "$json_str" | jq -r '.column')
  bucket=$(echo "$json_str" | jq -r '.bucket')
  prefix=$(echo "$json_str" | jq -r '.prefix')
  xref=$(echo "$json_str" | jq -r '.xref')
  if [[ "$column" == null || "$bucket" == null || "$prefix" == null || "$xref" == null ]]; then
      log_error "Error: JSON doesn't contain required keys: column, bucket, xref, and prefix"
      return 1
  fi
  return 0
}

validate_security_groups_quoting() {
    local sg_value="$1"
    local stripped
    stripped=$(echo "$sg_value" | sed "s/^'//;s/'$//")
    IFS=',' read -ra sg_parts <<< "$stripped"
    for part in "${sg_parts[@]}"; do
        local cleaned
        cleaned=$(echo "$part" | tr -d "'\" " )
        if [[ -n "$cleaned" && ! "$cleaned" =~ ^sg-[a-f0-9]+$ ]]; then
            log_error "Invalid security group format: '$cleaned'"
            log_error "Expected format: --sg '\"sg-abc123\",\"sg-def456\"'"
            exit 1
        fi
    done
    if [[ "$stripped" != *'"'* ]]; then
        log_error "Security groups must be wrapped in double quotes for the Glue connection JSON."
        log_error "You provided: --sg $sg_value"
        log_error "Expected:     --sg '\"sg-abc123\",\"sg-def456\"'"
        log_error "Example:      --sg '\"$stripped\"'"
        exit 1
    fi
}

function check_num_tiles() {
  if [[ ${params[TILES]} -lt 1 ]]; then
    log_info "Total number of tiles should be => 1"
    exit 1
  fi
  return 0
}

# ── AWS state query functions ────────────────────────────────────────────────
function check_discovery_runs() {
    local rs
    local mode
    local mode=$1
    local wait_time=30
    waiting_state=$(aws glue get-job-runs --job-name "${params[JOB_NAME]}${params[DEFAULT_ENV]}" --region "${params[AWS_REGION]}" \
            --query 'JobRuns[?JobRunState==`WAITING`] | [].Arguments | [?"--PROCESS_TYPE"==`discovery`] | [?"--SOURCE_KS"==`'"${params[SOURCE_KS]}"'`] | [?"--SOURCE_TBL"==`'"${params[SOURCE_TBL]}"'`]' | jq 'length != 0')
    if [[ $waiting_state == true ]]; then
      while [[ $waiting_state == true ]]; do
        sleep $wait_time
        waiting_state=$(aws glue get-job-runs --job-name "${params[JOB_NAME]}${params[DEFAULT_ENV]}" --region "${params[AWS_REGION]}" \
                    --query 'JobRuns[?JobRunState==`WAITING`] | [].Arguments | [?"--PROCESS_TYPE"==`discovery`] | [?"--SOURCE_KS"==`'"${params[SOURCE_KS]}"'`] | [?"--SOURCE_TBL"==`'"${params[SOURCE_TBL]}"'`]' | jq 'length != 0')
      done
    fi
    rs=$(aws glue get-job-runs --job-name "${params[JOB_NAME]}${params[DEFAULT_ENV]}" --region "${params[AWS_REGION]}" \
        --query 'JobRuns[?JobRunState==`RUNNING`] | [].Arguments | [?"--PROCESS_TYPE"==`discovery`] | [?"--SOURCE_KS"==`'"${params[SOURCE_KS]}"'`] | [?"--SOURCE_TBL"==`'"${params[SOURCE_TBL]}"'`]' | jq 'length != 0')
    if [[ $rs == "$mode" ]]; then
        log_error "Error: The discovery job ${params[JOB_NAME]}${params[DEFAULT_ENV]} has failed, check AWS Glue logs"
        error_message=$(aws glue get-job-run --job-name "${params[JOB_NAME]}${params[DEFAULT_ENV]}" --run-id "$DISCOVERY_RUNNING_ID" | jq '.JobRun | {ErrorMessage}')
        log_error "$error_message"
        exit 1
    fi
    return 0
}

function check_target_table_req() {
    local current_mode
    local target_type=$1
    if [[ "$target_type" == "dynamodb" ]]; then
        if ! current_mode=$(aws dynamodb describe-table --table-name "${params[TARGET_TBL]}" \
            --query 'Table.TableStatus' --output text 2>&1); then
            if [[ $current_mode == *"ResourceNotFoundException"* ]]; then
                log_error "Error: DynamoDB Table ${params[TARGET_TBL]} not found."
                exit 1
            else
                log_error "Error: Failed to get DynamoDB table information: $current_mode"
                exit 1
            fi
        fi
   fi
   if [[ "$target_type" == "keyspaces" ]]; then
        if ! current_mode=$(aws keyspaces get-table --keyspace-name "${params[TARGET_KS]}" --table-name "${params[TARGET_TBL]}" \
            --query 'capacitySpecification' --output json 2>&1); then
            if [[ $current_mode == *"ResourceNotFoundException"* ]]; then
                log_error "Error: Keyspaces or Table ${params[TARGET_KS]}.${params[TARGET_TBL]} not found."
                exit 1
            else
                log_error "Error: Failed to get table information: $current_mode"
                exit 1
            fi
        fi
    fi
}

function check_replication_runs() {
   local tile
   local rs
   tile=$1
   rs=$(aws glue get-job-runs --job-name "${params[JOB_NAME]}${params[DEFAULT_ENV]}" --region "${params[AWS_REGION]}" \
    --query 'length(JobRuns[?JobRunState==`RUNNING`] | [].Arguments | [?"--PROCESS_TYPE"==`replication`] | [?"--SOURCE_KS"==`'"${params[SOURCE_KS]}"'`] | [?"--SOURCE_TBL"==`'"${params[SOURCE_TBL]}"'`] | [?"--TILE"==`"'"$tile"'"`])')
   if [ "${rs}" -ne 0 ]; then
     log_error "Error: Replication job is already running per tile $tile for ${params[SOURCE_KS]}.${params[SOURCE_TBL]}"
     log_error "$rs"
     return 1
   fi
   return 0
}

function check_sampler_runs() {
   local process_type="$1"
   local rs
   rs=$(aws glue get-job-runs --job-name "${params[JOB_NAME]}${params[DEFAULT_ENV]}" \
      --region "${params[AWS_REGION]}" --query 'JobRuns[?JobRunState==`RUNNING`] | [].Arguments | [?"--PROCESS_TYPE"==`'"$process_type"'`] | [?"--SOURCE_KS"==`'"${params[SOURCE_KS]}"'`] | [?"--SOURCE_TBL"==`'"${params[SOURCE_TBL]}"'`]' | jq 'length != 0')
   params[PROCESS_RUNNING]=$rs
}

# ── IAM/security functions ───────────────────────────────────────────────────
check_glue_job_connection() {
    local job_name="$1"
    local aws_region="$2"
    if [ -z "$job_name" ] || [ -z "$aws_region" ]; then
        log_error "Error: Job name and AWS region must be provided."
        return 1
    fi
    job_details=$(aws glue get-job --job-name "$job_name" --region "$aws_region" 2>/dev/null)
    if [ $? -ne 0 ]; then
        log_error "Error: Job $job_name not found in region $aws_region."
        return 1
    fi
    connections=$(echo "$job_details" | jq -r '.Job.Connections.Connections[]' 2>/dev/null)
    if [ -z "$connections" ]; then
        log_warn "Warn: The Glue job $job_name does not have any attached connections."
    fi
}

validate_iam_role_permissions() {
    local role_name="$1"
    local role_arn
    local region="$2"
    local account="$3"
    role_arn=$(aws iam get-role --role-name "$role_name" --query 'Role.Arn' --output text)
    if [ -z "$role_arn" ]; then
        log_error "Error: Unable to find role $role_name"
        return 1
    fi
    local keyspaces_actions=("cassandra:Insert" "cassandra:Update" "cassandra:Select" "cassandra:Delete")
    local dynamodb_actions=("dynamodb:PutItem" "dynamodb:UpdateItem" "dynamodb:BatchWriteItem" "dynamodb:DeleteItem")
    local s3_actions=("s3:ListBucket" "s3:PutObject" "s3:DeleteObject" "s3:GetObject")
    local glue_catalog_actions=("glue:CreateDatabase" "glue:GetDatabase" "glue:CreateTable" "glue:GetTable" "glue:UpdateTable" "glue:DeleteTable" "glue:GetTables")

    check_permissions() {
        local service="$1"
        shift
        local actions=("$@")
        local resource_arn
        if [[ $service == "cassandra" ]]; then
          resource_arn="arn:aws:$service:$region:$account:*"
        fi
        if [[ $service == "dynamodb" ]]; then
          resource_arn="arn:aws:$service:$region:$account:*"
        fi
        if [[ $service == "s3" ]]; then
          bkt_name=$(echo "${params[S3_LANDING_ZONE]}" | sed 's|s3://||')
          resource_arn="arn:aws:$service:$region:$account:$bkt_name"
        fi
        if [[ $service == "glue" ]]; then
          resource_arn="arn:aws:$service:$region:$account:catalog"
        fi
        for action in "${actions[@]}"; do
            result=$(aws iam simulate-principal-policy \
                --policy-source-arn "$role_arn" \
                --action-names "$action" \
                --resource-arns $resource_arn \
                --query 'EvaluationResults[0].EvalDecision' \
                --output text)
            if [ "$result" = "allowed" ]; then
                log_info "✅ $action is allowed"
            else
                log_info "❌ $action is not allowed"
            fi
        done
    }

    if [ "${params[TARGET_TYPE]}" = "keyspaces" ]; then
      check_permissions "cassandra" "${keyspaces_actions[@]}"
    fi
    if [ "${params[TARGET_TYPE]}" = "dynamodb" ]; then
      check_permissions "dynamodb" "${dynamodb_actions[@]}"
    fi
    check_permissions "s3" "${s3_actions[@]}"
    check_permissions "glue" "${glue_catalog_actions[@]}"
    check_glue_service_role
}

check_glue_service_role() {
    local attached_policies=$(aws iam list-attached-role-policies --role-name "$role_name" --query 'AttachedPolicies[*].PolicyName' --output text)
    if log_info "$attached_policies" | grep -q "AWSGlueServiceRole"; then
        log_info "✅ AWSGlueServiceRole is attached to the role"
    else
        log_info "❌ AWSGlueServiceRole is not attached to the role"
        local inline_policies=$(aws iam list-role-policies --role-name "$role_name" --query 'PolicyNames' --output text)
        if [ -n "$inline_policies" ]; then
            log_info "   Checking inline policies for Glue permissions..."
            for policy in $inline_policies; do
                policy_doc=$(aws iam get-role-policy --role-name "$role_name" --policy-name "$policy" --query 'PolicyDocument' --output json)
                if log_info "$policy_doc" | grep -q "glue:"; then
                    log_info "   ✅ Custom policy '$policy' contains Glue permissions"
                    return
                fi
            done
            log_info "   ❌ No custom policies found with Glue permissions"
        else
            log_info "   ❌ No inline policies found"
        fi
    fi
}

check_security_group_self_referencing() {
    local sg_id="$1"
    sg_rules=$(aws ec2 describe-security-groups \
        --group-ids "$sg_id" \
        --query 'SecurityGroups[0]' \
        --output json)
    inbound_rules=$(echo "$sg_rules" | jq -r '.IpPermissions[] | select(.UserIdGroupPairs[].GroupId == "'"$sg_id"'") | .IpProtocol')
    if [ -n "$inbound_rules" ]; then
        log_info "✅ Self-referencing inbound rule found"
    else
        log_info "❌ No self-referencing inbound rule found"
    fi
}

# ── Upload helpers ───────────────────────────────────────────────────────────
function uploader_helper() {
  local artifact_name="$1"
  local curr_pos=$2
  local next_pos=$3
  local final_pos=$4
  check_file_exists "$path_to_conf/$artifact_name"
  progress $curr_pos $final_pos "Uploading $artifact_name                   "
      if ls "$path_to_conf/$artifact_name" > /dev/null
        then
          progress $next_pos $final_pos "Uploading $artifact_name                   "
          aws s3 cp "$path_to_conf"/"$artifact_name" "${params[S3_LANDING_ZONE]}"/artifacts/"$artifact_name" --region "${params[AWS_REGION]}" > /dev/null
        else
          log_error "Error: $path_to_conf/$artifact_name not found"
          exit 1
        fi
}

function uploader_jars() {
  cnt=1
  local artifacts=("$@")
  total_artifacts=$(echo "${artifacts[@]}" | wc -w)
  for link in "${artifacts[@]}"
  do
    file=$(basename "$link")
    progress "$cnt" "$total_artifacts" "Uploading jar artifacts"
    curl -s -O "${params[MAVEN_REPO]}""$link"
    aws s3 cp "$file" "${params[S3_LANDING_ZONE]}"/artifacts/"$file" --region "${params[AWS_REGION]}" > /dev/null
    rm "$file"
    ((cnt++))
  done
}

function join_array() {
    local joined_array=""
    for item in "$@"; do
        joined_array+="${joined_array:+,}$item"
    done
    S3_PATH_LIBS+="$joined_array"
}

# ── Control flow ─────────────────────────────────────────────────────────────
function confirm() {
  local msg=$1
  read -r -p "$msg" choice
  case $choice in
    y|Y) return 0;;
    n|N) exit 1;;
    *) echo "Invalid choice. Please enter y/Y or n/N." && exit 1;;
  esac
}

function max_value() {
  local rs=0
  if [[ $1 -gt $2 ]]; then
    rs="$1"
  else
    rs="$2"
  fi
  echo $rs
}

function barrier() {
  flag_check_discovery_run="$1"
  local delay=0.1
  local spinstr='|/-\'
  output="Glue job $rs is running"
  while true
  do
    local temp=${spinstr#?}
    printf "\r%s [%c]  " "$output" "$spinstr"
    local spinstr=$temp${spinstr%"$temp"}
    sleep $delay
    cnt=0
    for (( tile=0; tile<"${params[TILES]}"; tile++ ))
    do
      if aws s3 ls "${params[S3_LANDING_ZONE]}"/"${params[SOURCE_KS]}"/"${params[SOURCE_TBL]}"/stats/discovery/"$tile"/ --region "${params[AWS_REGION]}" > /dev/null
      then
        ((cnt++))
      fi
    done
    if [[ $cnt == "${params[TILES]}" ]]; then
      break
    fi
    if [[ $flag_check_discovery_run == "true" ]]; then
      sleep 2
      check_discovery_runs "false"
    fi
  done
  printf "\r%s    \n" "$output"
}

check_cloudshell() {
    if [[ "$(whoami)" == "cloudshell-user" ]]; then
        log_info "Running in AWS CloudShell"
        return 0
    fi
    if [[ -d "/home/cloudshell-user" ]]; then
        log_info "Running in AWS CloudShell"
        return 0
    fi
    log_info "Not running in AWS CloudShell"
    return 1
}

# ── Stop-event helpers ───────────────────────────────────────────────────────
function Delete_Stop_Event_D {
  aws s3api delete-object --bucket "${params[S3_LANDING_ZONE]:5}" --key "${params[SOURCE_KS]}/${params[SOURCE_TBL]}/discovery/stopRequested" --region "${params[AWS_REGION]}"
}

function Delete_Stop_Event_R {
  aws s3api delete-object --bucket "${params[S3_LANDING_ZONE]:5}" --key "${params[SOURCE_KS]}/${params[SOURCE_TBL]}/replication/$1/stopRequested" --region "${params[AWS_REGION]}"
}

# ── Resource calculation ─────────────────────────────────────────────────────
calculate_resources() {
  local workers=0
  local drps=${params[DEFAULT_ROWS_PER_SECOND]}
  local tiles=${params[TILES]}
  local wcu=${params[WCU_TRAFFIC]}
  if [ -z "${params[WCU_TRAFFIC]}" ]; then
    log_error "Error: WCU_TRAFFIC is not set"
    return 1
  fi
  if [ "${params[WCU_TRAFFIC]}" -le 10000 ]; then
    k=-1
    params[WORKER_TYPE]="G.025X"
    f=$(( drps * tiles ))
    workers=$(( (wcu + f - 1) / f - k ))
  elif [ "${params[WCU_TRAFFIC]}" -ge 10000 ]; then
    k=1
    params[WORKER_TYPE]="G.1X"
    f=$(( drps * tiles ))
    workers=$(( (wcu + f - 1) / f - k ))
  fi
  if [ $workers -lt 2 ]; then
      workers=$((workers + 1))
  fi
  params[DEFAULT_WORKERS]=$workers
}
