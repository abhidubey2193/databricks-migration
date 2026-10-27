# Architecture

## Layers
```
  Databricks App (Streamlit) ──SQL warehouse──► Delta tables ◄── SMCT serverless jobs (spark_python_task, one entrypoint)
        │  approvals = INSERT events                  ▲                      │
        └─────────────────────────────────────────────┘                      ▼
                                                              Databricks SDK (jobs, workspace, files, clusters)
```
* **Pure core (`smc/*` minus db/store/pipeline)** - no I/O, takes/returns Jobs-API JSON. Everything risky is unit-tested here.
* **Edges** - `db.Workspace` (SDK), `store.Store` (Spark/Delta), `pipeline` (orchestration). Tests swap them for `FakeWorkspace` / `FakeStore` / `FakeEngine`, so the full lifecycle (discover -> approve -> migrate -> validate -> promote -> rollback, drift, guard) runs offline.

## Data model (Delta, schema `<catalog>.<schema>`)
| Object | Purpose |
|---|---|
| `workloads` | one row per job/cluster: score, verdict, wave, rank, 30-day DBUs, limitations |
| `findings` | every rule hit with location, severity, auto-fixable flag, remediation |
| `scan_history` | one row per discovery - drives the burn-down |
| `migration_events` | append-only audit log; **status = latest event** |
| `migration_status` (view) | workloads joined to latest event |
| `validation_results` | per table A/B outcome, counts, reasons, runtime comparison |
| `code_patches` | unified diff of every rewritten notebook |
| `guard_log` | every pause the guard performed |
| `cost_by_job_day`, `cost_before_after` (views) | system.billing + list_prices, per job/team, before vs after promotion |

## State machine (`state.py`)
`DISCOVERED -> APPROVED -> CLONED -> VALIDATED -> PROMOTE_APPROVED -> PROMOTED -> ROLLBACK_REQUESTED -> ROLLED_BACK`
with `MIGRATE_FAILED`, `VALIDATION_FAILED`, `REJECTED` side states. `promote` additionally re-checks that a `VALIDATED` event exists and that the job is unchanged (hash).

## Scoring policy (`scoring.py`)
Penalty per distinct rule: non-fixable blocker 35, auto-fixable blocker 8, non-fixable warning 6, auto-fixable warning 2, info 1 (repeats of the same rule cost 1/5). Verdict: any `keep_on_classic` blocker => `KEEP_CLASSIC` (wave 3) unless the only hard blockers are jar/spark-submit tasks in a mixed job => `PARTIAL` (wave 1); any non-fixable blocker => `NEEDS_MANUAL` (wave 2); fixable blockers/warnings => `AUTO_FIXABLE` (wave 1); else `READY` (wave 0). Inside a wave: highest 30-day classic DBUs first, then highest score.

## Transformer (`transform.to_serverless`)
Deep-copies the job, then per task: skip JVM task types and tasks with hard L4/L5 blockers (left on classic), strip cluster refs + libraries, compute dependencies (task libraries + cluster init-script pip installs), group tasks by identical dependency set into `environments`, set `environment_key`, drop unused `job_clusters`, enable queueing, and (clone mode) rename, tag, and **remove every trigger**. `promote_settings` re-attaches the original name/schedule/notifications/params/tags so the promoted job is the *same job* to everyone else.

## Failure philosophy
Fail closed: unreadable init script = blocker; engine error = `ERROR`; no outputs found = validation fails with instructions; restore mode requires explicit opt-in; guard supports dry-run; every stage is idempotent and writes an event.
