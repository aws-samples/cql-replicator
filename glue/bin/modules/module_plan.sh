#!/usr/bin/env bash
# Module: module_plan.sh
# Command: plan
# Description: Multi-table migration plan — TUI wizard, plan file persistence, and batch execution
# Required params: SCHEMA_FILE (for plan creation)
# Optional params: MIGRATION_PLAN_NAME, S3_LANDING_ZONE, AWS_REGION

# ── Plan data structures ─────────────────────────────────────────────────────
# Table mappings stored as indexed parallel arrays (populated during wizard):
declare -a PLAN_SOURCE_KS=()
declare -a PLAN_SOURCE_TBL=()
declare -a PLAN_TARGET_KS=()
declare -a PLAN_TARGET_TBL=()
declare -a PLAN_TILES=()
declare -a PLAN_WORKER_TYPE=()
declare -a PLAN_WRITETIME_COL=()
declare -a PLAN_TTL_COL=()
declare -a PLAN_JSON_MAPPING=()

# ── Entry point ──────────────────────────────────────────────────────────────
# Creates a multi-table migration plan via the TUI wizard.
# Args: $1 = nameref to params associative array
module_plan() {
    local -n _mp_params=$1

    # Detect dialog/whiptail
    _tui_detect_tool

    # Parse the schema DDL file
    local -a _ks_list=()
    local -A _tbl_map=()
    if ! _tui_parse_schema "${_mp_params[SCHEMA_FILE]}" _ks_list _tbl_map; then
        $DIALOG_TOOL --title "Schema Error" \
            --msgbox "Schema file not found: ${_mp_params[SCHEMA_FILE]}" 8 60
        return 1
    fi

    # Validate that the schema contains at least one table
    local _total_tables=0
    local _ks
    for _ks in "${_ks_list[@]}"; do
        local _tables_str="${_tbl_map[$_ks]:-}"
        if [[ -n "$_tables_str" ]]; then
            local _t
            for _t in $_tables_str; do
                (( _total_tables++ ))
            done
        fi
    done

    if [[ $_total_tables -eq 0 ]]; then
        $DIALOG_TOOL --title "Schema Error" \
            --msgbox "No tables found in schema file: ${_mp_params[SCHEMA_FILE]}" 8 60
        return 1
    fi

    # Build initial plan arrays from parsed schema (one entry per table)
    PLAN_SOURCE_KS=()
    PLAN_SOURCE_TBL=()
    PLAN_TARGET_KS=()
    PLAN_TARGET_TBL=()
    PLAN_TILES=()
    PLAN_WORKER_TYPE=()
    PLAN_WRITETIME_COL=()
    PLAN_TTL_COL=()
    PLAN_JSON_MAPPING=()

    for _ks in "${_ks_list[@]}"; do
        local _tables_str="${_tbl_map[$_ks]:-}"
        local _t
        for _t in $_tables_str; do
            PLAN_SOURCE_KS+=( "$_ks" )
            PLAN_SOURCE_TBL+=( "$_t" )
            PLAN_TARGET_KS+=( "$_ks" )
            PLAN_TARGET_TBL+=( "$_t" )
            PLAN_TILES+=( "${_mp_params[TILES]:-4}" )
            PLAN_WORKER_TYPE+=( "${_mp_params[WORKER_TYPE]:-G.2X}" )
            PLAN_WRITETIME_COL+=( "None" )
            PLAN_TTL_COL+=( "None" )
            PLAN_JSON_MAPPING+=( "" )
        done
    done

    # ── Wizard step loop (back-navigation via step counter) ──
    local _step=0

    while true; do
        case $_step in
        0)
            # Step 0: Target type selection
            if ! _plan_target_type_screen "$1"; then
                # Cancel at first screen → exit wizard cleanly
                tput reset 2>/dev/null || true
                return 0
            fi
            _step=1
            ;;
        1)
            # Step 1: Global parameters
            if ! _plan_global_params_screen "$1"; then
                # Cancel → back to target type
                _step=0
                continue
            fi
            _step=2
            ;;
        2)
            # Step 2: Mapper (table selection + target mapping)
            if ! _plan_mapper_screen "$1"; then
                # Cancel → back to global params
                _step=1
                continue
            fi
            _step=3
            ;;
        3)
            # Step 3: Per-table configuration
            if ! _plan_per_table_config_screen "$1"; then
                # Cancel → back to mapper
                _step=2
                continue
            fi
            _step=4
            ;;
        4)
            # Step 4: Summary screen
            _plan_summary_screen "$1"
            local _summary_rc=$?
            case $_summary_rc in
                0)
                    # Saved successfully → exit wizard
                    tput reset 2>/dev/null || true
                    return 0
                    ;;
                2)
                    # Edit → back to mapper
                    _step=2
                    continue
                    ;;
                *)
                    # Cancel → back to per-table config
                    _step=3
                    continue
                    ;;
            esac
            ;;
        esac
    done

    # Terminal reset on exit (fallback, should not reach here)
    tput reset 2>/dev/null || true
    return 0
}

# ── Target type selection screen ─────────────────────────────────────────────
# Displays a radiolist with keyspaces/dynamodb/parquet options.
# For dynamodb: shows an info msgbox noting JSON mapping will be required.
# For parquet: collects S3 bucket, prefix, and max file size via inputboxes.
# Stores the selected target type in params[TARGET_TYPE].
# For parquet, also stores params[PARQUET_S3_BUCKET], params[PARQUET_S3_PREFIX],
# and params[PARQUET_MAX_FILE_SIZE].
# Args: $1 = nameref to params associative array
# Returns 0 on success, 1 on Cancel/Escape.
_plan_target_type_screen() {
    local -n _ptt_params=$1
    local _tmpfile
    _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)

    # Current default
    local _tt="${_ptt_params[TARGET_TYPE]:-keyspaces}"

    $DIALOG_TOOL --title "Plan — Target Type" \
        --radiolist "Select target type for the migration plan:" 14 60 3 \
        "keyspaces"  "Amazon Keyspaces"    "$( [[ "$_tt" == "keyspaces"  ]] && echo ON || echo OFF )" \
        "dynamodb"   "Amazon DynamoDB"     "$( [[ "$_tt" == "dynamodb"   ]] && echo ON || echo OFF )" \
        "parquet"    "Amazon S3 (Parquet)" "$( [[ "$_tt" == "parquet"    ]] && echo ON || echo OFF )" \
        2>"$_tmpfile"
    if [[ $? -ne 0 ]]; then
        rm -f "$_tmpfile"
        return 1
    fi

    local _selected
    _selected=$(cat "$_tmpfile")
    rm -f "$_tmpfile"

    # Default to keyspaces if nothing selected (e.g. whiptail quirk)
    [[ -z "$_selected" ]] && _selected="keyspaces"
    _ptt_params[TARGET_TYPE]="$_selected"

    # Target-type-specific follow-up screens
    case "$_selected" in
        dynamodb)
            # Info: JSON mapping will be required per table
            _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)
            $DIALOG_TOOL --title "DynamoDB — Note" \
                --msgbox "DynamoDB target selected.\n\nA JSON mapping will be required for each table during per-table configuration." 10 60 \
                2>"$_tmpfile"
            rm -f "$_tmpfile"
            ;;
        parquet)
            # Collect S3 bucket, prefix, and max file size
            _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)

            $DIALOG_TOOL --title "Parquet — S3 Bucket" \
                --inputbox "Enter S3 bucket name:" 8 60 "${_ptt_params[PARQUET_S3_BUCKET]:-}" \
                2>"$_tmpfile"
            if [[ $? -ne 0 ]]; then
                rm -f "$_tmpfile"
                return 1
            fi
            _ptt_params[PARQUET_S3_BUCKET]=$(cat "$_tmpfile")

            $DIALOG_TOOL --title "Parquet — S3 Prefix" \
                --inputbox "Enter S3 prefix (e.g. snapshots):" 8 60 "${_ptt_params[PARQUET_S3_PREFIX]:-}" \
                2>"$_tmpfile"
            if [[ $? -ne 0 ]]; then
                rm -f "$_tmpfile"
                return 1
            fi
            _ptt_params[PARQUET_S3_PREFIX]=$(cat "$_tmpfile")

            $DIALOG_TOOL --title "Parquet — Max File Size" \
                --inputbox "Max file size in MB:" 8 60 "${_ptt_params[PARQUET_MAX_FILE_SIZE]:-32}" \
                2>"$_tmpfile"
            if [[ $? -ne 0 ]]; then
                rm -f "$_tmpfile"
                return 1
            fi
            local _max_size
            _max_size=$(cat "$_tmpfile")
            [[ -z "$_max_size" ]] && _max_size="32"
            _ptt_params[PARQUET_MAX_FILE_SIZE]="$_max_size"

            rm -f "$_tmpfile"
            ;;
        # keyspaces: no additional screens needed
    esac

    return 0
}

# ── Serialize plan to JSON ───────────────────────────────────────────────────
# Converts the in-memory parallel arrays and global params to a JSON plan file.
# Prints the JSON to stdout on success, returns non-zero on failure.
# Args: $1 = nameref to params associative array
_plan_serialize() {
    local -n _ps_params=$1
    local _json=""
    local _tables_json="[]"
    local _i

    # Build the tables array by iterating the parallel arrays
    for (( _i=0; _i<${#PLAN_SOURCE_KS[@]}; _i++ )); do
        _tables_json=$(printf '%s' "$_tables_json" | jq \
            --arg src_ks  "${PLAN_SOURCE_KS[$_i]}" \
            --arg src_tbl "${PLAN_SOURCE_TBL[$_i]}" \
            --arg tgt_ks  "${PLAN_TARGET_KS[$_i]}" \
            --arg tgt_tbl "${PLAN_TARGET_TBL[$_i]}" \
            --argjson tiles "${PLAN_TILES[$_i]:-4}" \
            --arg wtype  "${PLAN_WORKER_TYPE[$_i]:-G.2X}" \
            --arg wtcol  "${PLAN_WRITETIME_COL[$_i]:-None}" \
            --arg ttlcol "${PLAN_TTL_COL[$_i]:-None}" \
            --arg jmap   "${PLAN_JSON_MAPPING[$_i]:-}" \
            '. + [{
                source_ks: $src_ks,
                source_tbl: $src_tbl,
                target_ks: $tgt_ks,
                target_tbl: $tgt_tbl,
                tiles: $tiles,
                worker_type: $wtype,
                writetime_column: $wtcol,
                ttl_column: $ttlcol,
                json_mapping: $jmap
            }]'
        ) || { echo "Error: failed to build tables JSON at index $_i" >&2; return 1; }
    done

    # Build the full plan JSON with version, global object, and tables array
    _json=$(jq -n \
        --arg version    "1.0" \
        --arg region     "${_ps_params[AWS_REGION]:-}" \
        --arg lz         "${_ps_params[S3_LANDING_ZONE]:-}" \
        --arg env        "${_ps_params[DEFAULT_ENV]:-}" \
        --arg iam_role   "${_ps_params[GLUE_IAM_ROLE]:-}" \
        --arg tgt_type   "${_ps_params[TARGET_TYPE]:-keyspaces}" \
        --arg src_type   "${_ps_params[DEFAULT_SOURCE]:-cassandra}" \
        --arg wl_type    "${_ps_params[DEFAULT_WORKLOAD_TYPE]:-continuous}" \
        --argjson tables "$_tables_json" \
        '{
            version: $version,
            global: {
                aws_region: $region,
                s3_landing_zone: $lz,
                environment_name: $env,
                glue_iam_role: $iam_role,
                target_type: $tgt_type,
                source_type: $src_type,
                workload_type: $wl_type
            },
            tables: $tables
        }'
    ) || { echo "Error: failed to build plan JSON" >&2; return 1; }

    # Validate the JSON with jq empty
    if ! printf '%s' "$_json" | jq empty 2>/dev/null; then
        echo "Error: generated plan JSON is invalid" >&2
        return 1
    fi

    printf '%s\n' "$_json"
    return 0
}

# ── Load plan from S3 ────────────────────────────────────────────────────────
# Downloads a plan JSON file from S3 and parses it into the global parallel
# arrays and params. Returns 0 on success, 1 on failure.
# Args: $1 = nameref to params associative array
_plan_load_from_s3() {
    local -n _pl_params=$1
    local _s3_path="${_pl_params[S3_LANDING_ZONE]}/migration_plans/${_pl_params[MIGRATION_PLAN_NAME]}"
    local _tmpfile
    _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)

    # Download the plan file from S3
    local _region_flag=""
    if [[ -n "${_pl_params[AWS_REGION]:-}" ]]; then
        _region_flag="--region ${_pl_params[AWS_REGION]}"
    fi

    if ! aws s3 cp "$_s3_path" "$_tmpfile" $_region_flag 2>&1; then
        echo "Error: failed to download plan file from $_s3_path" >&2
        rm -f "$_tmpfile"
        return 1
    fi

    # Validate JSON
    if ! jq empty "$_tmpfile" 2>/dev/null; then
        echo "Error: plan file contains invalid JSON" >&2
        rm -f "$_tmpfile"
        return 1
    fi

    # Parse global params from the JSON.
    # For params that may be set by CLI flags (AWS_REGION, S3_LANDING_ZONE,
    # DEFAULT_ENV, GLUE_IAM_ROLE), only use the plan value if the CLI didn't
    # set them (i.e., they're still at their default/empty value).
    # For plan-specific params (TARGET_TYPE, DEFAULT_SOURCE, DEFAULT_WORKLOAD_TYPE),
    # always use the plan value since they define the plan's behavior.
    local _val

    _val=$(jq -r '.global.aws_region // empty' "$_tmpfile")
    [[ -z "${_pl_params[AWS_REGION]:-}" && -n "$_val" ]] && _pl_params[AWS_REGION]="$_val"

    _val=$(jq -r '.global.s3_landing_zone // empty' "$_tmpfile")
    [[ -z "${_pl_params[S3_LANDING_ZONE]:-}" && -n "$_val" ]] && _pl_params[S3_LANDING_ZONE]="$_val"

    _val=$(jq -r '.global.environment_name // empty' "$_tmpfile")
    [[ -z "${_pl_params[DEFAULT_ENV]:-}" && -n "$_val" ]] && _pl_params[DEFAULT_ENV]="$_val"

    _val=$(jq -r '.global.glue_iam_role // empty' "$_tmpfile")
    [[ -z "${_pl_params[GLUE_IAM_ROLE]:-}" && -n "$_val" ]] && _pl_params[GLUE_IAM_ROLE]="$_val"

    # Plan-specific params — always use plan values
    _val=$(jq -r '.global.target_type // empty' "$_tmpfile")
    [[ -n "$_val" ]] && _pl_params[TARGET_TYPE]="$_val"

    _val=$(jq -r '.global.source_type // empty' "$_tmpfile")
    [[ -n "$_val" ]] && _pl_params[DEFAULT_SOURCE]="$_val"

    _val=$(jq -r '.global.workload_type // empty' "$_tmpfile")
    [[ -n "$_val" ]] && _pl_params[DEFAULT_WORKLOAD_TYPE]="$_val"

    # Parse table mappings into parallel arrays
    PLAN_SOURCE_KS=()
    PLAN_SOURCE_TBL=()
    PLAN_TARGET_KS=()
    PLAN_TARGET_TBL=()
    PLAN_TILES=()
    PLAN_WORKER_TYPE=()
    PLAN_WRITETIME_COL=()
    PLAN_TTL_COL=()
    PLAN_JSON_MAPPING=()

    local _table_count
    _table_count=$(jq '.tables | length' "$_tmpfile")

    local _i
    for (( _i=0; _i<_table_count; _i++ )); do
        PLAN_SOURCE_KS+=( "$(jq -r ".tables[$_i].source_ks" "$_tmpfile")" )
        PLAN_SOURCE_TBL+=( "$(jq -r ".tables[$_i].source_tbl" "$_tmpfile")" )
        PLAN_TARGET_KS+=( "$(jq -r ".tables[$_i].target_ks" "$_tmpfile")" )
        PLAN_TARGET_TBL+=( "$(jq -r ".tables[$_i].target_tbl" "$_tmpfile")" )
        PLAN_TILES+=( "$(jq -r ".tables[$_i].tiles" "$_tmpfile")" )
        PLAN_WORKER_TYPE+=( "$(jq -r ".tables[$_i].worker_type" "$_tmpfile")" )
        PLAN_WRITETIME_COL+=( "$(jq -r ".tables[$_i].writetime_column" "$_tmpfile")" )
        PLAN_TTL_COL+=( "$(jq -r ".tables[$_i].ttl_column" "$_tmpfile")" )
        PLAN_JSON_MAPPING+=( "$(jq -r ".tables[$_i].json_mapping" "$_tmpfile")" )
    done

    # Clean up
    rm -f "$_tmpfile"
    return 0
}

# ── Validate plan JSON structure ─────────────────────────────────────────────
# Validates a plan JSON string: checks target_type, non-empty tables array,
# and required fields on each table entry.
# Returns 0 on success, 1 on validation failure (with descriptive errors on stderr).
# Args: $1 = plan JSON string
_plan_validate() {
    local _json="$1"

    # Validate target_type is one of keyspaces/dynamodb/parquet
    local _target_type
    _target_type=$(printf '%s' "$_json" | jq -r '.global.target_type // empty')
    case "$_target_type" in
        keyspaces|dynamodb|parquet) ;;
        "")
            echo "Error: plan validation failed — missing target_type in .global.target_type" >&2
            return 1
            ;;
        *)
            echo "Error: plan validation failed — invalid target_type '$_target_type' (must be keyspaces, dynamodb, or parquet)" >&2
            return 1
            ;;
    esac

    # Validate tables array is non-empty
    local _table_count
    _table_count=$(printf '%s' "$_json" | jq '.tables | length')
    if [[ "$_table_count" -eq 0 ]]; then
        echo "Error: plan validation failed — tables array is empty" >&2
        return 1
    fi

    # Validate each table has required fields: source_ks, source_tbl, target_ks, target_tbl
    local _i _field _val
    for (( _i=0; _i<_table_count; _i++ )); do
        for _field in source_ks source_tbl target_ks target_tbl; do
            _val=$(printf '%s' "$_json" | jq -r ".tables[$_i].$_field // empty")
            if [[ -z "$_val" ]]; then
                echo "Error: plan validation failed — tables[$_i] is missing required field '$_field'" >&2
                return 1
            fi
        done
    done

    return 0
}

# ── Populate params for a single table mapping ───────────────────────────────
# Copies all global params into a temporary params array, then overlays
# per-table values from the plan parallel arrays. Also sets the global
# JSON_MAPPING and JSON_MAPPING_B64 variables for the dispatcher.
# Args: $1 = nameref to source (global) params array
#       $2 = nameref to destination (temp) params array
#       $3 = table index into the plan parallel arrays
_plan_populate_params() {
    local -n _src=$1      # global params
    local -n _dst=$2      # temp params copy
    local _idx=$3          # table index

    # Copy all global params
    for key in "${!_src[@]}"; do
        _dst["$key"]="${_src[$key]}"
    done

    # Overlay per-table values
    _dst[SOURCE_KS]="${PLAN_SOURCE_KS[$_idx]}"
    _dst[SOURCE_TBL]="${PLAN_SOURCE_TBL[$_idx]}"
    _dst[TARGET_KS]="${PLAN_TARGET_KS[$_idx]}"
    _dst[TARGET_TBL]="${PLAN_TARGET_TBL[$_idx]}"
    _dst[TILES]="${PLAN_TILES[$_idx]}"
    _dst[WORKER_TYPE]="${PLAN_WORKER_TYPE[$_idx]}"
    _dst[WRITETIME_COLUMN]="${PLAN_WRITETIME_COL[$_idx]}"
    _dst[TTL_COLUMN]="${PLAN_TTL_COL[$_idx]}"

    # Handle JSON mapping
    local _jm="${PLAN_JSON_MAPPING[$_idx]}"
    if [[ -n "$_jm" ]]; then
        JSON_MAPPING="$_jm"
        JSON_MAPPING_B64=$(printf '%s' "$_jm" | base64 -w 0 2>/dev/null || printf '%s' "$_jm" | base64)
    else
        JSON_MAPPING="None"
        JSON_MAPPING_B64=$(echo "None" | base64)
    fi
}

# ── Upload plan JSON to S3 ───────────────────────────────────────────────────
# Writes the plan JSON to a temp file and uploads it to S3 at
# {S3_LANDING_ZONE}/migration_plans/{plan_name}.json.
# On success, prints the S3 path and the CLI command to run the plan.
# On failure, prints the AWS CLI error to stderr and returns 1.
# Args: $1 = nameref to params associative array
#       $2 = plan JSON string
#       $3 = plan name (without .json extension)
_plan_upload_to_s3() {
    local -n _up_params=$1
    local _plan_json="$2"
    local _plan_name="$3"

    # Write JSON to a temp file
    local _tmpfile
    _tmpfile=$(mktemp /tmp/cqlreplicator-plan-upload-XXXXXX.json)
    printf '%s\n' "$_plan_json" > "$_tmpfile"

    # Build the S3 destination path
    local _s3_path="${_up_params[S3_LANDING_ZONE]}/migration_plans/${_plan_name}.json"

    # Build optional region flag
    local _region_flag=""
    if [[ -n "${_up_params[AWS_REGION]:-}" ]]; then
        _region_flag="--region ${_up_params[AWS_REGION]}"
    fi

    # Upload to S3
    local _aws_output
    _aws_output=$(aws s3 cp "$_tmpfile" "$_s3_path" $_region_flag 2>&1)
    local _rc=$?

    # Clean up temp file
    rm -f "$_tmpfile"

    if [[ $_rc -ne 0 ]]; then
        echo "Error: failed to upload plan to $_s3_path" >&2
        echo "$_aws_output" >&2
        return 1
    fi

    # Success — print the S3 path and the CLI command to run the plan
    echo "Plan uploaded to: $_s3_path"
    echo "To run the plan:"
    echo "  --cmd run --landing-zone ${_up_params[S3_LANDING_ZONE]} --migration-plan-name ${_plan_name}.json"

    return 0
}

# ── Global parameters screen ─────────────────────────────────────────────────
# Collects global parameters: AWS region, S3 landing zone, Glue IAM role,
# and environment name. Uses dialog --form when available, falls back to
# sequential whiptail --inputbox calls.
# Stores results in params[AWS_REGION], params[S3_LANDING_ZONE],
# params[GLUE_IAM_ROLE], params[DEFAULT_ENV].
# Args: $1 = nameref to params associative array
# Returns 0 on success, 1 on Cancel/Escape.
_plan_global_params_screen() {
    local -n _pgp_params=$1
    local _tmpfile
    _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)

    # Defaults
    local _def_region="${_pgp_params[AWS_REGION]:-us-east-1}"
    local _def_lz="${_pgp_params[S3_LANDING_ZONE]:-}"
    local _def_iam="${_pgp_params[GLUE_IAM_ROLE]:-}"
    local _def_env="${_pgp_params[DEFAULT_ENV]:-}"

    if [[ "$DIALOG_TOOL" == "dialog" ]]; then
        $DIALOG_TOOL --title "Plan — Global Parameters" \
            --form "Enter global parameters for the migration plan:" 14 70 4 \
            "AWS Region:"      1 1 "$_def_region" 1 20 40 0 \
            "S3 Landing Zone:" 2 1 "$_def_lz"     2 20 40 0 \
            "Glue IAM Role:"   3 1 "$_def_iam"    3 20 40 0 \
            "Environment:"     4 1 "$_def_env"    4 20 40 0 \
            2>"$_tmpfile"
        local _rc=$?
        if [[ $_rc -ne 0 ]]; then rm -f "$_tmpfile"; return 1; fi
        local _result
        _result=$(cat "$_tmpfile")
        rm -f "$_tmpfile"
        local -a _fields
        mapfile -t _fields <<< "$_result"
        _pgp_params[AWS_REGION]="${_fields[0]:-}"
        _pgp_params[S3_LANDING_ZONE]="${_fields[1]:-}"
        _pgp_params[GLUE_IAM_ROLE]="${_fields[2]:-}"
        _pgp_params[DEFAULT_ENV]="${_fields[3]:-}"
    else
        # whiptail fallback: sequential --inputbox calls
        $DIALOG_TOOL --title "Plan — Global Parameters" \
            --inputbox "AWS Region:" 8 60 "$_def_region" 2>"$_tmpfile"
        [[ $? -ne 0 ]] && { rm -f "$_tmpfile"; return 1; }
        _pgp_params[AWS_REGION]=$(cat "$_tmpfile")

        $DIALOG_TOOL --title "Plan — Global Parameters" \
            --inputbox "S3 Landing Zone:" 8 60 "$_def_lz" 2>"$_tmpfile"
        [[ $? -ne 0 ]] && { rm -f "$_tmpfile"; return 1; }
        _pgp_params[S3_LANDING_ZONE]=$(cat "$_tmpfile")

        $DIALOG_TOOL --title "Plan — Global Parameters" \
            --inputbox "Glue IAM Role:" 8 60 "$_def_iam" 2>"$_tmpfile"
        [[ $? -ne 0 ]] && { rm -f "$_tmpfile"; return 1; }
        _pgp_params[GLUE_IAM_ROLE]=$(cat "$_tmpfile")

        $DIALOG_TOOL --title "Plan — Global Parameters" \
            --inputbox "Environment Name:" 8 60 "$_def_env" 2>"$_tmpfile"
        [[ $? -ne 0 ]] && { rm -f "$_tmpfile"; return 1; }
        _pgp_params[DEFAULT_ENV]=$(cat "$_tmpfile")

        rm -f "$_tmpfile"
    fi
}

# ── Mapper screen: table selection checklist + target mapping ────────────────
# Displays a checklist of all discovered source tables (keyspace.table) with
# all tables ON by default. Filters PLAN_* arrays based on user selection.
# Then prompts for target mapping based on params[TARGET_TYPE]:
#   - keyspaces: prompt target keyspace per distinct source keyspace
#   - dynamodb: prompt target table per source table
#   - parquet: use S3 bucket/prefix from global config
# Defaults target table names to match source table names.
# Provides an edit screen for overriding individual target table names.
# Args: $1 = nameref to params associative array
# Returns 0 on success, 1 on Cancel/Escape.
_plan_mapper_screen() {
    local -n _pm_params=$1
    local _tmpfile
    _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)

    # ── Step 1: Build checklist from PLAN_SOURCE_KS and PLAN_SOURCE_TBL ──
    local -a _checklist_args=()
    local _i
    for (( _i=0; _i<${#PLAN_SOURCE_KS[@]}; _i++ )); do
        local _tag="${PLAN_SOURCE_KS[$_i]}.${PLAN_SOURCE_TBL[$_i]}"
        _checklist_args+=( "$_tag" "Table: $_tag" "ON" )
    done

    $DIALOG_TOOL --title "Plan — Select Tables" \
        --checklist "Select tables to include in the migration plan:" 20 70 10 \
        "${_checklist_args[@]}" \
        2>"$_tmpfile"
    if [[ $? -ne 0 ]]; then
        rm -f "$_tmpfile"
        return 1
    fi

    local _selected_raw
    _selected_raw=$(cat "$_tmpfile")
    rm -f "$_tmpfile"

    # Parse selected items (dialog outputs space-separated, possibly quoted)
    local -a _selected_tags=()
    eval "_selected_tags=( $_selected_raw )"

    # If nothing selected, return (empty plan)
    if [[ ${#_selected_tags[@]} -eq 0 ]]; then
        $DIALOG_TOOL --title "Plan — No Tables" \
            --msgbox "No tables selected. Plan is empty." 8 50
        return 1
    fi

    # ── Step 2: Filter PLAN_* arrays to only include selected tables ──
    local -a _new_src_ks=() _new_src_tbl=() _new_tgt_ks=() _new_tgt_tbl=()
    local -a _new_tiles=() _new_wtype=() _new_wtcol=() _new_ttlcol=() _new_jmap=()

    for (( _i=0; _i<${#PLAN_SOURCE_KS[@]}; _i++ )); do
        local _tag="${PLAN_SOURCE_KS[$_i]}.${PLAN_SOURCE_TBL[$_i]}"
        local _found=0
        local _s
        for _s in "${_selected_tags[@]}"; do
            if [[ "$_s" == "$_tag" ]]; then
                _found=1
                break
            fi
        done
        if [[ $_found -eq 1 ]]; then
            _new_src_ks+=( "${PLAN_SOURCE_KS[$_i]}" )
            _new_src_tbl+=( "${PLAN_SOURCE_TBL[$_i]}" )
            _new_tgt_ks+=( "${PLAN_TARGET_KS[$_i]}" )
            _new_tgt_tbl+=( "${PLAN_TARGET_TBL[$_i]}" )
            _new_tiles+=( "${PLAN_TILES[$_i]}" )
            _new_wtype+=( "${PLAN_WORKER_TYPE[$_i]}" )
            _new_wtcol+=( "${PLAN_WRITETIME_COL[$_i]}" )
            _new_ttlcol+=( "${PLAN_TTL_COL[$_i]}" )
            _new_jmap+=( "${PLAN_JSON_MAPPING[$_i]}" )
        fi
    done

    PLAN_SOURCE_KS=( "${_new_src_ks[@]}" )
    PLAN_SOURCE_TBL=( "${_new_src_tbl[@]}" )
    PLAN_TARGET_KS=( "${_new_tgt_ks[@]}" )
    PLAN_TARGET_TBL=( "${_new_tgt_tbl[@]}" )
    PLAN_TILES=( "${_new_tiles[@]}" )
    PLAN_WORKER_TYPE=( "${_new_wtype[@]}" )
    PLAN_WRITETIME_COL=( "${_new_wtcol[@]}" )
    PLAN_TTL_COL=( "${_new_ttlcol[@]}" )
    PLAN_JSON_MAPPING=( "${_new_jmap[@]}" )

    # ── Step 3: Target mapping based on TARGET_TYPE ──
    local _target_type="${_pm_params[TARGET_TYPE]:-keyspaces}"
    _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)

    case "$_target_type" in
        keyspaces)
            # Collect distinct source keyspaces
            local -a _distinct_ks=()
            for (( _i=0; _i<${#PLAN_SOURCE_KS[@]}; _i++ )); do
                local _ks="${PLAN_SOURCE_KS[$_i]}"
                local _dup=0
                local _d
                for _d in "${_distinct_ks[@]}"; do
                    [[ "$_d" == "$_ks" ]] && { _dup=1; break; }
                done
                [[ $_dup -eq 0 ]] && _distinct_ks+=( "$_ks" )
            done

            # For each distinct source keyspace, prompt for target keyspace
            local -A _ks_mapping=()
            local _src_ks
            for _src_ks in "${_distinct_ks[@]}"; do
                local _tgt_ks_names=""
                local _resolve_rc=0
                _tgt_ks_names=$(_tui_resolve_target_list "keyspaces" "${_pm_params[AWS_REGION]}" "") || _resolve_rc=$?

                if [[ $_resolve_rc -ne 0 || -z "$_tgt_ks_names" ]]; then
                    # Fallback to inputbox
                    $DIALOG_TOOL --title "Target Keyspace for '$_src_ks'" \
                        --inputbox "Enter target keyspace for source '$_src_ks':" 8 60 "$_src_ks" \
                        2>"$_tmpfile"
                    if [[ $? -ne 0 ]]; then
                        rm -f "$_tmpfile"
                        return 1
                    fi
                    _ks_mapping[$_src_ks]=$(cat "$_tmpfile")
                else
                    # Build menu from AWS results
                    local -a _ks_menu=()
                    local _name
                    while IFS= read -r _name; do
                        [[ -n "$_name" ]] && _ks_menu+=( "$_name" "Keyspace: $_name" )
                    done <<< "$_tgt_ks_names"

                    $DIALOG_TOOL --title "Target Keyspace for '$_src_ks'" \
                        --menu "Select target keyspace for source '$_src_ks':" 20 60 10 \
                        "${_ks_menu[@]}" \
                        2>"$_tmpfile"
                    if [[ $? -ne 0 ]]; then
                        rm -f "$_tmpfile"
                        return 1
                    fi
                    _ks_mapping[$_src_ks]=$(cat "$_tmpfile")
                fi
            done

            # Apply keyspace mapping and default target table names to source
            for (( _i=0; _i<${#PLAN_SOURCE_KS[@]}; _i++ )); do
                PLAN_TARGET_KS[$_i]="${_ks_mapping[${PLAN_SOURCE_KS[$_i]}]}"
                PLAN_TARGET_TBL[$_i]="${PLAN_SOURCE_TBL[$_i]}"
            done
            ;;

        dynamodb)
            # For each source table, prompt for target DynamoDB table
            for (( _i=0; _i<${#PLAN_SOURCE_KS[@]}; _i++ )); do
                local _src_tag="${PLAN_SOURCE_KS[$_i]}.${PLAN_SOURCE_TBL[$_i]}"
                local _tgt_tbl_names=""
                local _resolve_rc=0
                _tgt_tbl_names=$(_tui_resolve_target_list "dynamodb" "${_pm_params[AWS_REGION]}" "") || _resolve_rc=$?

                if [[ $_resolve_rc -ne 0 || -z "$_tgt_tbl_names" ]]; then
                    # Fallback to inputbox
                    $DIALOG_TOOL --title "Target DynamoDB Table for '$_src_tag'" \
                        --inputbox "Enter target DynamoDB table for '$_src_tag':" 8 60 "${PLAN_SOURCE_TBL[$_i]}" \
                        2>"$_tmpfile"
                    if [[ $? -ne 0 ]]; then
                        rm -f "$_tmpfile"
                        return 1
                    fi
                    PLAN_TARGET_TBL[$_i]=$(cat "$_tmpfile")
                else
                    # Build menu from AWS results
                    local -a _tbl_menu=()
                    local _name
                    while IFS= read -r _name; do
                        [[ -n "$_name" ]] && _tbl_menu+=( "$_name" "Table: $_name" )
                    done <<< "$_tgt_tbl_names"

                    $DIALOG_TOOL --title "Target DynamoDB Table for '$_src_tag'" \
                        --menu "Select target DynamoDB table for '$_src_tag':" 20 60 10 \
                        "${_tbl_menu[@]}" \
                        2>"$_tmpfile"
                    if [[ $? -ne 0 ]]; then
                        rm -f "$_tmpfile"
                        return 1
                    fi
                    PLAN_TARGET_TBL[$_i]=$(cat "$_tmpfile")
                fi
                PLAN_TARGET_KS[$_i]="None"
            done
            ;;

        parquet)
            # Use S3 bucket/prefix from global config for all tables
            local _bucket="${_pm_params[PARQUET_S3_BUCKET]:-}"
            local _prefix="${_pm_params[PARQUET_S3_PREFIX]:-}"
            for (( _i=0; _i<${#PLAN_SOURCE_KS[@]}; _i++ )); do
                PLAN_TARGET_KS[$_i]="$_bucket"
                PLAN_TARGET_TBL[$_i]="$_prefix"
            done
            ;;
    esac

    rm -f "$_tmpfile"

    # ── Step 4: Edit screen for overriding individual target table names ──
    while true; do
        _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)

        # Build menu of current mappings
        local -a _edit_menu=()
        for (( _i=0; _i<${#PLAN_SOURCE_KS[@]}; _i++ )); do
            local _src_tag="${PLAN_SOURCE_KS[$_i]}.${PLAN_SOURCE_TBL[$_i]}"
            local _tgt_tag="${PLAN_TARGET_KS[$_i]}.${PLAN_TARGET_TBL[$_i]}"
            _edit_menu+=( "$_i" "$_src_tag → $_tgt_tag" )
        done
        _edit_menu+=( "done" "Accept mappings and continue" )

        $DIALOG_TOOL --title "Plan — Edit Target Table Names" \
            --menu "Select a table to edit its target name, or 'done' to continue:" 20 70 12 \
            "${_edit_menu[@]}" \
            2>"$_tmpfile"
        if [[ $? -ne 0 ]]; then
            rm -f "$_tmpfile"
            return 1
        fi

        local _choice
        _choice=$(cat "$_tmpfile")
        rm -f "$_tmpfile"

        if [[ "$_choice" == "done" ]]; then
            break
        fi

        # Edit the selected table's target name
        local _idx="$_choice"
        local _src_tag="${PLAN_SOURCE_KS[$_idx]}.${PLAN_SOURCE_TBL[$_idx]}"
        _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)

        $DIALOG_TOOL --title "Edit Target Table Name" \
            --inputbox "Enter new target table name for '$_src_tag':" 8 60 "${PLAN_TARGET_TBL[$_idx]}" \
            2>"$_tmpfile"
        if [[ $? -ne 0 ]]; then
            rm -f "$_tmpfile"
            continue
        fi

        local _new_name
        _new_name=$(cat "$_tmpfile")
        rm -f "$_tmpfile"

        if [[ -n "$_new_name" ]]; then
            PLAN_TARGET_TBL[$_idx]="$_new_name"
        fi
    done

    return 0
}

# ── Per-table configuration screen ───────────────────────────────────────────
# Loops through each selected table in the PLAN_* arrays and shows a form
# to configure: TTL column, writetime column, JSON mapping, tiles, worker type.
# Pre-fills with global defaults from params. Validates JSON mapping with
# `jq empty` if provided; re-prompts on invalid JSON. For dynamodb target,
# requires non-empty JSON mapping.
# Args: $1 = nameref to params associative array
# Returns 0 on success, 1 on Cancel/Escape.
_plan_per_table_config_screen() {
    local -n _ptc_params=$1
    local _tmpfile
    local _i

    local _target_type="${_ptc_params[TARGET_TYPE]:-keyspaces}"
    local _default_tiles="${_ptc_params[TILES]:-4}"
    local _default_worker="${_ptc_params[WORKER_TYPE]:-G.2X}"

    for (( _i=0; _i<${#PLAN_SOURCE_KS[@]}; _i++ )); do
        local _src_tag="${PLAN_SOURCE_KS[$_i]}.${PLAN_SOURCE_TBL[$_i]}"

        # Current values (pre-fill with existing or global defaults)
        local _cur_ttl="${PLAN_TTL_COL[$_i]:-None}"
        local _cur_wt="${PLAN_WRITETIME_COL[$_i]:-None}"
        local _cur_jmap="${PLAN_JSON_MAPPING[$_i]:-}"
        local _cur_tiles="${PLAN_TILES[$_i]:-$_default_tiles}"
        local _cur_worker="${PLAN_WORKER_TYPE[$_i]:-$_default_worker}"

        # Auto-populate JSON mapping for DynamoDB target if not already set
        if [[ "$_target_type" == "dynamodb" && -z "$_cur_jmap" ]]; then
            local _tgt_tbl="${PLAN_TARGET_TBL[$_i]}"
            local _pk_name="pk" _sk_name="" _describe_json=""
            _describe_json=$(aws dynamodb describe-table \
                --table-name "$_tgt_tbl" \
                --region "${_ptc_params[AWS_REGION]:-us-east-1}" 2>/dev/null) || true

            if [[ -n "$_describe_json" ]]; then
                _pk_name=$(printf '%s' "$_describe_json" | \
                    jq -r '.Table.KeySchema[] | select(.KeyType=="HASH") | .AttributeName' 2>/dev/null) || true
                _sk_name=$(printf '%s' "$_describe_json" | \
                    jq -r '.Table.KeySchema[] | select(.KeyType=="RANGE") | .AttributeName' 2>/dev/null) || true
                [[ "$_pk_name" == "null" || -z "$_pk_name" ]] && _pk_name="pk"
                [[ "$_sk_name" == "null" ]] && _sk_name=""
            fi

            local _ddb_endpoint="dynamodb.${_ptc_params[AWS_REGION]:-us-east-1}.amazonaws.com"
            if [[ -n "$_sk_name" ]]; then
                _cur_jmap=$(printf '{"replication":{"dynamoDBPrimaryKey":{"partitionKeyName":"%s","sortKeyName":"%s","separator":"#"}},"dynamodb":{"dynamoDBConnection":{"endpoint":"%s","region":"%s"}}}' \
                    "$_pk_name" "$_sk_name" "$_ddb_endpoint" "${_ptc_params[AWS_REGION]:-us-east-1}")
            else
                _cur_jmap=$(printf '{"replication":{"dynamoDBPrimaryKey":{"partitionKeyName":"%s","sortKeyName":"","separator":"#"}},"dynamodb":{"dynamoDBConnection":{"endpoint":"%s","region":"%s"}}}' \
                    "$_pk_name" "$_ddb_endpoint" "${_ptc_params[AWS_REGION]:-us-east-1}")
            fi
        fi

        local _valid=0
        while [[ $_valid -eq 0 ]]; do
            _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)

            if [[ "$DIALOG_TOOL" == "dialog" ]]; then
                $DIALOG_TOOL --title "Per-Table Config — $_src_tag" \
                    --form "Configure migration parameters for $_src_tag:" 16 70 5 \
                    "TTL Column:"       1 1 "$_cur_ttl"    1 20 40 0 \
                    "Writetime Column:" 2 1 "$_cur_wt"     2 20 40 0 \
                    "JSON Mapping:"     3 1 "$_cur_jmap"   3 20 40 0 \
                    "Tiles:"            4 1 "$_cur_tiles"  4 20 40 0 \
                    "Worker Type:"      5 1 "$_cur_worker" 5 20 40 0 \
                    2>"$_tmpfile"
                local _rc=$?
                if [[ $_rc -ne 0 ]]; then
                    rm -f "$_tmpfile"
                    return 1
                fi

                local _result
                _result=$(cat "$_tmpfile")
                rm -f "$_tmpfile"

                local -a _fields
                mapfile -t _fields <<< "$_result"
                _cur_ttl="${_fields[0]:-None}"
                _cur_wt="${_fields[1]:-None}"
                _cur_jmap="${_fields[2]:-}"
                _cur_tiles="${_fields[3]:-$_default_tiles}"
                _cur_worker="${_fields[4]:-$_default_worker}"
            else
                # whiptail fallback: sequential --inputbox calls
                $DIALOG_TOOL --title "Per-Table Config — $_src_tag" \
                    --inputbox "TTL Column:" 8 60 "$_cur_ttl" 2>"$_tmpfile"
                if [[ $? -ne 0 ]]; then rm -f "$_tmpfile"; return 1; fi
                _cur_ttl=$(cat "$_tmpfile")

                $DIALOG_TOOL --title "Per-Table Config — $_src_tag" \
                    --inputbox "Writetime Column:" 8 60 "$_cur_wt" 2>"$_tmpfile"
                if [[ $? -ne 0 ]]; then rm -f "$_tmpfile"; return 1; fi
                _cur_wt=$(cat "$_tmpfile")

                $DIALOG_TOOL --title "Per-Table Config — $_src_tag" \
                    --inputbox "JSON Mapping:" 8 60 "$_cur_jmap" 2>"$_tmpfile"
                if [[ $? -ne 0 ]]; then rm -f "$_tmpfile"; return 1; fi
                _cur_jmap=$(cat "$_tmpfile")

                $DIALOG_TOOL --title "Per-Table Config — $_src_tag" \
                    --inputbox "Tiles:" 8 60 "$_cur_tiles" 2>"$_tmpfile"
                if [[ $? -ne 0 ]]; then rm -f "$_tmpfile"; return 1; fi
                _cur_tiles=$(cat "$_tmpfile")

                $DIALOG_TOOL --title "Per-Table Config — $_src_tag" \
                    --inputbox "Worker Type:" 8 60 "$_cur_worker" 2>"$_tmpfile"
                if [[ $? -ne 0 ]]; then rm -f "$_tmpfile"; return 1; fi
                _cur_worker=$(cat "$_tmpfile")

                rm -f "$_tmpfile"
            fi

            # ── Validate JSON mapping if provided ──
            if [[ -n "$_cur_jmap" ]]; then
                if ! printf '%s' "$_cur_jmap" | jq empty 2>/dev/null; then
                    _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)
                    $DIALOG_TOOL --title "Invalid JSON" \
                        --msgbox "The JSON mapping for '$_src_tag' is not valid JSON.\nPlease correct it." 8 60 \
                        2>"$_tmpfile"
                    rm -f "$_tmpfile"
                    continue
                fi
            fi

            # ── For dynamodb target: require non-empty JSON mapping ──
            if [[ "$_target_type" == "dynamodb" && -z "$_cur_jmap" ]]; then
                _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)
                $DIALOG_TOOL --title "JSON Mapping Required" \
                    --msgbox "DynamoDB target requires a JSON mapping for '$_src_tag'.\nPlease provide a valid JSON mapping." 8 60 \
                    2>"$_tmpfile"
                rm -f "$_tmpfile"
                continue
            fi

            _valid=1
        done

        # Store results back into PLAN_* arrays
        PLAN_TTL_COL[$_i]="${_cur_ttl:-None}"
        PLAN_WRITETIME_COL[$_i]="${_cur_wt:-None}"
        PLAN_JSON_MAPPING[$_i]="$_cur_jmap"
        PLAN_TILES[$_i]="${_cur_tiles:-$_default_tiles}"
        PLAN_WORKER_TYPE[$_i]="${_cur_worker:-$_default_worker}"
    done

    return 0
}

# ── Plan summary screen ──────────────────────────────────────────────────────
# Displays a summary of all table mappings with key parameters, then offers
# a menu: "Save Plan", "Edit Plan", "Cancel".
# On "Save Plan": prompts for plan name, calls _plan_serialize and
#   _plan_upload_to_s3, returns 0 on success.
# On "Edit Plan": returns 2 (special code for caller to loop back to mapper).
# On "Cancel": returns 1 without saving.
# Cleans up temp files on all exit paths.
# Args: $1 = nameref to params associative array
# Returns: 0 = saved, 1 = cancelled, 2 = edit plan
_plan_summary_screen() {
    local -n _pss_params=$1
    local _tmpfile
    _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)

    # ── Build summary text ──
    local _summary=""
    _summary+="Migration Plan Summary\n"
    _summary+="======================\n\n"
    _summary+="Tables: ${#PLAN_SOURCE_KS[@]}\n"
    _summary+="Target Type: ${_pss_params[TARGET_TYPE]:-keyspaces}\n\n"

    local _i
    for (( _i=0; _i<${#PLAN_SOURCE_KS[@]}; _i++ )); do
        local _src="${PLAN_SOURCE_KS[$_i]}.${PLAN_SOURCE_TBL[$_i]}"
        local _tgt="${PLAN_TARGET_KS[$_i]}.${PLAN_TARGET_TBL[$_i]}"
        local _jmap_status="No"
        if [[ -n "${PLAN_JSON_MAPPING[$_i]}" ]]; then
            _jmap_status="Yes"
        fi

        _summary+="Table $(( _i + 1 )): ${_src} → ${_tgt}\n"
        _summary+="  Tiles: ${PLAN_TILES[$_i]:-4}\n"
        _summary+="  Worker Type: ${PLAN_WORKER_TYPE[$_i]:-G.2X}\n"
        _summary+="  Writetime Column: ${PLAN_WRITETIME_COL[$_i]:-None}\n"
        _summary+="  TTL Column: ${PLAN_TTL_COL[$_i]:-None}\n"
        _summary+="  JSON Mapping: ${_jmap_status}\n\n"
    done

    # ── Display summary using --msgbox (scrollable) ──
    $DIALOG_TOOL --title "Plan Summary" \
        --msgbox "$(printf '%b' "$_summary")" 22 76 \
        2>"$_tmpfile"
    # Ignore exit code from msgbox (user just presses OK to continue)
    rm -f "$_tmpfile"

    # ── Show action menu: Save Plan / Edit Plan / Cancel ──
    _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)

    $DIALOG_TOOL --title "Plan — Action" \
        --menu "Choose an action:" 12 50 3 \
        "save"   "Save Plan" \
        "edit"   "Edit Plan" \
        "cancel" "Cancel" \
        2>"$_tmpfile"
    if [[ $? -ne 0 ]]; then
        rm -f "$_tmpfile"
        return 1
    fi

    local _choice
    _choice=$(cat "$_tmpfile")
    rm -f "$_tmpfile"

    case "$_choice" in
        save)
            # ── Prompt for plan name ──
            _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)

            $DIALOG_TOOL --title "Plan — Save" \
                --inputbox "Enter plan name:" 8 60 "migration_plan" \
                2>"$_tmpfile"
            if [[ $? -ne 0 ]]; then
                rm -f "$_tmpfile"
                return 1
            fi

            local _plan_name
            _plan_name=$(cat "$_tmpfile")
            rm -f "$_tmpfile"

            [[ -z "$_plan_name" ]] && _plan_name="migration_plan"

            # ── Serialize the plan ──
            local _plan_json
            _plan_json=$(_plan_serialize _pss_params)
            if [[ $? -ne 0 ]]; then
                _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)
                $DIALOG_TOOL --title "Serialization Error" \
                    --msgbox "Failed to serialize the migration plan.\n\n$_plan_json" 10 60 \
                    2>"$_tmpfile"
                rm -f "$_tmpfile"
                return 1
            fi

            # ── Upload to S3 ──
            local _upload_output
            _upload_output=$(_plan_upload_to_s3 _pss_params "$_plan_json" "$_plan_name" 2>&1)
            if [[ $? -ne 0 ]]; then
                _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)
                $DIALOG_TOOL --title "Upload Error" \
                    --msgbox "Failed to upload plan to S3.\n\n$_upload_output" 12 60 \
                    2>"$_tmpfile"
                rm -f "$_tmpfile"
                return 1
            fi

            # ── Success message ──
            _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)
            $DIALOG_TOOL --title "Plan Saved" \
                --msgbox "$_upload_output" 12 70 \
                2>"$_tmpfile"
            rm -f "$_tmpfile"

            return 0
            ;;
        edit)
            return 2
            ;;
        cancel)
            return 1
            ;;
    esac

    # Fallback (should not reach here)
    return 1
}

# ── Batch execution: iterate table mappings and call module functions ─────────
# Iterates through all table mappings in the PLAN_* arrays, populates a
# temporary params copy for each, and invokes the appropriate module function
# (module_run, module_request_stop, or module_kill) based on the command.
# Catches failures per table, logs the error, and continues to the next table.
# After all tables are processed, displays a results summary.
# Always returns 0 — the summary shows individual successes/failures.
# Args: $1 = nameref to params associative array
#       $2 = command: "run", "request-stop", or "kill"
_plan_execute_batch() {
    local -n _peb_params=$1
    local _command="$2"

    # Map command to module function
    local _module_fn=""
    case "$_command" in
        run)          _module_fn="module_run" ;;
        request-stop) _module_fn="module_request_stop" ;;
        kill)         _module_fn="module_kill" ;;
        *)
            log_error "Unknown command '$_command' — expected run, request-stop, or kill"
            return 1
            ;;
    esac

    local _total=${#PLAN_SOURCE_KS[@]}
    if [[ $_total -eq 0 ]]; then
        log_error "No table mappings found in plan"
        return 1
    fi

    # Results tracking: parallel arrays for table tag and status
    local -a _result_tags=()
    local -a _result_status=()

    log_info "Executing '$_command' for $_total table(s)..."

    local _i
    for (( _i=0; _i<_total; _i++ )); do
        local _tag="${PLAN_SOURCE_KS[$_i]}.${PLAN_SOURCE_TBL[$_i]}"
        _result_tags+=( "$_tag" )

        log_info "[$(( _i + 1 ))/$_total] $_command: $_tag"

        # Build a temporary params copy for this table
        declare -A _tmp_params=()
        _plan_populate_params _peb_params _tmp_params "$_i"

        # Also update the global params array — common.sh functions (check_target_table_req,
        # check_discovery_run, etc.) read from the global params directly, not from namerefs.
        _plan_populate_params _peb_params params "$_i"

        # Invoke the module function, capturing exit code
        local _rc=0
        "$_module_fn" _tmp_params || _rc=$?

        if [[ $_rc -eq 0 ]]; then
            _result_status+=( "SUCCESS" )
        else
            _result_status+=( "FAILED (exit code $_rc)" )
            log_error "$_command failed for $_tag (exit code $_rc)"
        fi

        unset _tmp_params
    done

    # ── Display results summary ──
    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Batch '$_command' Results Summary"
    echo "═══════════════════════════════════════════════════════════"

    local _success_count=0
    local _fail_count=0
    for (( _i=0; _i<_total; _i++ )); do
        local _status="${_result_status[$_i]}"
        if [[ "$_status" == "SUCCESS" ]]; then
            echo "  ✓ ${_result_tags[$_i]}: $_status"
            _success_count=$(( _success_count + 1 ))
        else
            echo "  ✗ ${_result_tags[$_i]}: $_status"
            _fail_count=$(( _fail_count + 1 ))
        fi
    done

    echo "───────────────────────────────────────────────────────────"
    echo "  Total: $_total | Success: $_success_count | Failed: $_fail_count"
    echo "═══════════════════════════════════════════════════════════"
    echo ""

    return 0
}

# ── TUI operations menu for plan operations ──────────────────────────────────
# Loads a plan from S3 using _plan_load_from_s3 and _plan_validate, then
# displays a menu with "Run All", "Request Stop All", "Kill All", "Cancel".
# On selection, calls _plan_execute_batch with the appropriate command.
# On Cancel, returns 0 to the caller.
# Cleans up temp files on all exit paths.
# Args: $1 = nameref to params associative array
# Returns: 0 on success or cancel, 1 on load/validation failure
_plan_operations_menu() {
    local -n _pom_params=$1

    # Detect dialog/whiptail
    _tui_detect_tool

    # ── Load plan from S3 ──
    if ! _plan_load_from_s3 "$1"; then
        local _tmpfile
        _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)
        $DIALOG_TOOL --title "Plan Load Error" \
            --msgbox "Failed to load migration plan from S3.\n\nCheck that the plan name and S3 landing zone are correct." 10 60 \
            2>"$_tmpfile"
        rm -f "$_tmpfile"
        return 1
    fi

    # ── Serialize loaded plan to JSON for validation ──
    local _plan_json
    _plan_json=$(_plan_serialize "$1")
    if [[ $? -ne 0 ]]; then
        local _tmpfile
        _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)
        $DIALOG_TOOL --title "Plan Serialization Error" \
            --msgbox "Failed to serialize the loaded plan for validation." 8 60 \
            2>"$_tmpfile"
        rm -f "$_tmpfile"
        return 1
    fi

    # ── Validate the plan ──
    local _validate_err
    _validate_err=$(_plan_validate "$_plan_json" 2>&1)
    if [[ $? -ne 0 ]]; then
        local _tmpfile
        _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)
        $DIALOG_TOOL --title "Plan Validation Error" \
            --msgbox "Plan validation failed:\n\n$_validate_err" 12 60 \
            2>"$_tmpfile"
        rm -f "$_tmpfile"
        return 1
    fi

    # ── Display operations menu ──
    local _tmpfile
    _tmpfile=$(mktemp /tmp/cqlreplicator-plan-XXXXXX)

    $DIALOG_TOOL --title "Plan Operations — ${_pom_params[MIGRATION_PLAN_NAME]}" \
        --menu "Select an operation for the migration plan:" 14 60 4 \
        "run"          "Run All" \
        "request-stop" "Request Stop All" \
        "kill"         "Kill All" \
        "cancel"       "Cancel" \
        2>"$_tmpfile"
    if [[ $? -ne 0 ]]; then
        # Escape/Cancel pressed on the dialog itself
        rm -f "$_tmpfile"
        return 0
    fi

    local _choice
    _choice=$(cat "$_tmpfile")
    rm -f "$_tmpfile"

    # ── Handle selection ──
    case "$_choice" in
        run|request-stop|kill)
            _plan_execute_batch "$1" "$_choice"
            return $?
            ;;
        cancel|"")
            return 0
            ;;
    esac

    return 0
}
