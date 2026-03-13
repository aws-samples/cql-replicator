# Module Developer Guide

How to add a new command module to CQLReplicator.

## Directory Layout

```
glue/bin/
├── cqlreplicator              # Thin dispatcher (entry point)
├── helper                     # Recovery and TCO utilities
├── banner.txt                 # ASCII banner
└── modules/
    ├── common.sh              # Shared library (logging, validation, AWS helpers)
    ├── module_init.sh         # init command
    ├── module_run.sh          # run, run-single, sample commands
    ├── module_request_stop.sh # request-stop, request-single-stop commands
    ├── module_stats.sh        # stats command
    ├── module_cleanup.sh      # cleanup command
    ├── module_kill.sh         # kill command
    ├── module_resize.sh       # resize command
    ├── module_scheduled_run.sh# scheduled-run command
    └── module_get_failed_jobs.sh # get-failed-jobs command
```

### Naming Convention

- Module files live in `glue/bin/modules/`
- File name: `module_<command>.sh` (e.g., `module_init.sh`)
- Entry-point function: `module_<command>` (e.g., `module_init`)
- If a command contains a hyphen, replace it with an underscore in the function name (e.g., `run-single` → `module_run_single`)

## Module Template

Use this template when creating a new command module:

```bash
#!/usr/bin/env bash
# Module: module_<command>.sh
# Command: <command>
# Description: <one-line description of what this command does>
# Required params: <comma-separated list of required param keys>
# Optional params: <comma-separated list of optional param keys>

# Entry point — called by the dispatcher
# Args: $1 = nameref to params associative array
module_<command>() {
    local -n _params=$1

    # Validate required parameters
    check_input "${_params[MY_PARAM]}" "Error: MY_PARAM is empty, must be provided"

    # Implementation here — access config via _params[KEY]
    log_info "Starting <command>..."
}
```

### Rules

1. Every module **must** have the comment header block (`# Module:`, `# Command:`, `# Description:`, `# Required params:`, `# Optional params:`).
2. The entry-point function receives the `params` associative array by **nameref** (`local -n _params=$1`). Access values with `${_params[KEY]}`.
3. A single module file may expose multiple entry-point functions (e.g., `module_run.sh` defines `module_run`, `module_run_single`, and `module_sample`).
4. Use shared library functions from `common.sh` — they are sourced before your module.
5. Do not re-declare or duplicate functions already in `common.sh`.

## Shared Library Reference (`common.sh`)

All functions below are available to every module automatically.

### Logging

| Function | Description |
|----------|-------------|
| `log_info <msg>` | Log an informational message with timestamp |
| `log_error <msg>` | Log an error message with timestamp |
| `log_warn <msg>` | Log a warning message with timestamp |

### Display

| Function | Description |
|----------|-------------|
| `format_number <n>` | Format a number with comma separators (e.g., 1000000 → 1,000,000) |
| `progress <current> <total> <title>` | Display a progress bar |
| `print_stat_table <args>` | Print a formatted statistics table row |

### Validation

| Function | Description |
|----------|-------------|
| `check_input <value> <error_msg>` | Validate a parameter is not empty; exits on failure |
| `check_file_exists <path> <error_msg>` | Validate a file exists; exits on failure |
| `validate_json <json> <keys>` | Validate a JSON string has required keys |
| `validate_security_groups_quoting <sg>` | Validate security group format for Glue connections |
| `check_num_tiles <n>` | Validate tiles count is >= 1 |

### AWS State Queries

| Function | Description |
|----------|-------------|
| `check_discovery_runs` | Check if discovery Glue job is running |
| `check_replication_runs <tile>` | Check if replication Glue job is running for a tile |
| `check_sampler_runs` | Check if sampler/resize Glue job is running |
| `check_target_table_req` | Verify target table exists (DynamoDB or Keyspaces) |

### IAM / Security

| Function | Description |
|----------|-------------|
| `validate_iam_role_permissions` | Validate IAM role has required permissions |
| `check_glue_service_role` | Check if AWSGlueServiceRole is attached |
| `check_security_group_self_referencing` | Check security group has self-referencing rule |
| `check_glue_job_connection` | Check Glue job has attached connections |

### Upload Helpers

| Function | Description |
|----------|-------------|
| `uploader_helper <args>` | Upload a config artifact to S3 with progress |
| `uploader_jars <args>` | Download and upload jar artifacts to S3 |
| `join_array <array>` | Join array elements into comma-separated string, appending to S3_PATH_LIBS |

### Control Flow

| Function | Description |
|----------|-------------|
| `confirm <prompt>` | Prompt user for y/n confirmation |
| `max_value <a> <b>` | Return the maximum of two integers |
| `barrier` | Wait for discovery stats to appear in S3 |
| `check_cloudshell` | Check if running in AWS CloudShell |

### Stop Events

| Function | Description |
|----------|-------------|
| `Delete_Stop_Event_D` | Delete discovery stop event from S3 |
| `Delete_Stop_Event_R <tile>` | Delete replication stop event from S3 for a tile |

### Resources

| Function | Description |
|----------|-------------|
| `calculate_resources` | Calculate worker count and type based on WCU traffic |

## Registering a New Command

After creating your module file, register the command in the dispatcher (`glue/bin/cqlreplicator`):

### 1. Add to the dispatch table

Find the `dispatch_table` associative array and add your entry:

```bash
declare -A dispatch_table=(
    [init]=module_init
    [run]=module_run
    [run-single]=module_run_single
    [sample]=module_sample
    [request-stop]=module_request_stop
    [request-single-stop]=module_request_single_stop
    [stats]=module_stats
    [cleanup]=module_cleanup
    [kill]=module_kill
    [resize]=module_resize
    [scheduled-run]=module_scheduled_run
    [get-failed-jobs]=module_get_failed_jobs
    [my-command]=module_my_command          # ← add your entry here
)
```

### 2. Verify auto-sourcing

The dispatcher sources all `modules/module_*.sh` files automatically:

```bash
for mod in "$SCRIPT_DIR"/modules/module_*.sh; do
    source "$mod"
done
```

As long as your file follows the `module_<command>.sh` naming convention and lives in `glue/bin/modules/`, it will be sourced automatically. No additional `source` statement is needed.

### 3. Update the usage message

Add your command to the `Usage_Exit` function so it appears in `--help` output.

That's it — the dispatcher handles the rest. When a user runs `./cqlreplicator --state my-command`, the dispatcher looks up `my-command` in the dispatch table and calls `module_my_command params`.
