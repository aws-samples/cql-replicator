# Cutover

## Prerequisites

Before initiating the cutover phase, verify that the following requirements are met. Skipping these checks may result in false positives during reconciliation (ghost inserts/deletes) or incomplete data in the target.

### 1. Source Cluster Health

| Requirement | Details                                                                                                                                                                                                                                                               |
| --- |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Run `nodetool repair` | A full repair must complete successfully on all keyspaces being migrated **within the **`gc_grace_seconds`** window** (default 10 days) prior to cutover. If repair has not run, anti-entropy data will surface as phantom inserts and deletes during reconciliation. |
| Verify cluster consistency | Run `nodetool status/verify` and `nodetool describecluster` — all nodes must be in `UN` (Up/Normal) state with no pending ranges.                                                                                                                                     |
| Check `gc_grace_seconds` | Ensure tombstones have not expired without repair. If repair was skipped for longer than `gc_grace_seconds`, data resurrection or silent data loss is possible.                                                                                                       |

### 2. Successful Historical Replication

| Requirement | Details |
| --- | --- |
| Full load completed | Every table must have completed at least one successful full historical load (batch workload type). Partial loads that were interrupted and never retried leave gaps that reconciliation will flag. |
| Stats validation | Run `cqlreplicator --cmd stats` for each table and confirm `Discovered rows == Replicated rows`. |
| DLQ review | Inspect the Dead Letter Queue path (`s3://<BUCKET>/<ks>/<tbl>/dlq`). All DLQ records must be replayed or accounted for before cutover. |

### 3. Infrastructure Capacity

| Requirement | Details |
| --- | --- |
| Subnet IP availability | Each Glue DPU requires one IP address. Verify that your subnet has sufficient available IPs for the maximum number of concurrent DPUs across all tiles (discovery + replicators + reconciliation). Insufficient IPs cause silent job failures. |
| Glue concurrent job limits | Check your AWS account's Glue concurrent job run limit. Request a quota increase if needed before cutover. |

### 4. Reconciliation Readiness Checklist

Before running `cqlreplicator --cmd reconcile-keys`:

- Source repair completed within `gc_grace_seconds`
- All historical loads finished successfully (`stats` confirms parity)
- DLQ is empty or replayed
- Application writes are stopped (or dual-writing to both source and target)
- Subnet has sufficient IPs for the reconciliation Glue job
- Target table capacity mode can handle the reconciliation read traffic (check `--max-target-rcus`)

### 5. Post-Cutover Verification

After the application is pointed to Amazon Keyspaces:

1. Run a **final repair** on the source Cassandra cluster to surface any remaining inconsistencies.
2. Run `reconcile-keys` one more time to catch any records that appeared after the initial reconciliation.
3. **Archive source SSTables** to S3 (`nodetool snapshot` + upload) for audit and rollback purposes before decommissioning the Cassandra cluster.

---

### Common Pitfalls

| Pitfall | Impact | Mitigation |
| --- | --- | --- |
| Repair not run for weeks | CQLReplicator reports phantom inserts/deletes; reconciliation shows false differences | Run full repair before cutover |
| Partial historical load | Missing records in target appear as reconciliation gaps | Verify `stats` parity for every table |
| Subnet IP exhaustion | Glue jobs fail silently or cannot start | Pre-check available IPs; use multiple subnets if needed |
| Oversized nodes (>500 GB/node) | Slow repairs, streaming timeouts, inconsistent reads | Right-size cluster or accept longer repair windows |

