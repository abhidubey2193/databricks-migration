# Architecture

## The pieces

```
Databricks App (Streamlit) --SQL warehouse--> Delta tables <-- six serverless jobs (one entrypoint)
        |  approvals are INSERTs                  ^                      |
        +-----------------------------------------+                      v
                                                          Databricks SDK (jobs, workspace, files, clusters)
```

The code is split on one rule: anything that decides something takes plain Jobs-API JSON in and returns plain data out, with no I/O. That covers the scanners, rewriters, transformer, scoring, A/B diff, drift check, guard and state machine. Three modules touch Databricks, and nothing else does:

- `db.Workspace` wraps the SDK.
- `store.Store` reads and writes the Delta tables.
- `pipeline` strings the stages together.

Tests replace those three with in-memory fakes (`FakeWorkspace`, `FakeStore`, `FakeEngine`), so the whole lifecycle (discover, approve, migrate, validate, promote, rollback, drift, guard) runs without a workspace. The flip side: the SDK calls in `db.py` are only as proven as the live runs listed in `VERIFICATION.md`.

## Tables (`<catalog>.<schema>`)

| Object | What it holds |
|---|---|
| `workloads` | One row per job or cluster: score, verdict, wave, rank, 30-day DBUs, limitations hit |
| `findings` | Every rule hit with location, severity, whether it is auto-fixable, and the suggested fix |
| `scan_history` | One row per discovery run; the burn-down chart reads this |
| `migration_events` | Append-only audit log. A workload's status is its latest event |
| `migration_status` (view) | `workloads` joined to the latest event |
| `validation_results` | Per-table A/B result: counts, reasons, runtime comparison |
| `code_patches` | Unified diff of every rewritten notebook |
| `guard_log` | Every pause the guard made, with the reason |
| `cost_by_job_day`, `cost_before_after` (views) | `system.billing` joined to `list_prices`, per job and team, before vs after promotion |

## State machine (`state.py`)

```
DISCOVERED -> APPROVED -> CLONED -> VALIDATED -> PROMOTE_APPROVED -> PROMOTED -> ROLLBACK_REQUESTED -> ROLLED_BACK
```

Side states: `MIGRATE_FAILED`, `VALIDATION_FAILED`, `REJECTED`. The two approval steps are written by people in the app. `promote` also checks for itself that a `VALIDATED` event exists and that the job hasn't been edited since the clone, so skipping a step in the table doesn't get around it.

## Scoring (`scoring.py`)

Each distinct rule costs points off 100:

| Finding | Penalty |
|---|---|
| Blocker, no automatic fix | 35 |
| Blocker, automatic fix | 8 |
| Warning, no automatic fix | 6 |
| Warning, automatic fix | 2 |
| Info | 1 |

A repeat of the same rule costs 1/5 of that. The verdict follows from the findings:

- A blocker marked keep-on-classic gives `KEEP_CLASSIC` (wave 3). If the only such blockers are jar or spark-submit tasks in a job that also has other tasks, it is `PARTIAL` (wave 1) instead.
- Any other blocker without a fix gives `NEEDS_MANUAL` (wave 2).
- Only fixable blockers or warnings give `AUTO_FIXABLE` (wave 1).
- Nothing gives `READY` (wave 0).

Inside a wave, the job with the most 30-day classic DBUs goes first, then the one with the higher score.

## Transformer (`transform.to_serverless`)

It works on a deep copy of the job. For each task:

- JVM task types and tasks with hard blockers are left on classic.
- Cluster references and cluster-level libraries are removed.
- Dependencies are collected from the task's libraries plus any `pip install` lines found in the cluster's init scripts. Tasks with the same dependency set share one entry in `environments`, so a task never picks up another task's libraries.
- Each environment gets `environment_version: "2"` and, only when there are any, a `dependencies` list.
- `environment_key` is set on Python, wheel and dbt tasks. Notebook tasks only get one when they have dependencies, because a notebook with nothing to install doesn't need it.

Then unused `job_clusters` are dropped and queueing is turned on. For a clone, the job is also renamed and tagged, and **every** trigger is removed (schedule, file arrival, continuous).

Some Jobs API releases may reject `environment_key` on notebook tasks. `pipeline.create_with_fallback` handles that: if the create call fails with an error mentioning `environment_key`, it retries once without the key on notebook tasks and adds a note telling the owner to use `%pip install` in the notebook. Any other error is raised as is.

`promote_settings` puts the original name, schedule, notifications, parameters and tags back on the serverless definition, so everyone who looked at the job before sees the same job.

## A/B validation (`pipeline.validate`)

Production jobs are never run by the validator. For the classic side it makes a baseline copy (`transform.baseline_settings`): same tasks and compute, but renamed, no schedule, no notifications, one run at a time, tagged `smct_baseline`. It runs that, then deletes it in a `finally` block. Discovery ignores jobs with that tag or the clone tag, so these copies never show up as workloads.

- **Sandbox mode:** classic and serverless each write to their own shadow schema via a job parameter, so no production table changes.
- **Restore mode** (opt-in): the baseline writes to the real tables, the outputs are saved, the tables are `RESTORE`d to the recorded Delta versions, and then the serverless clone runs.

`ab_diff` compares schema and types, row counts, `EXCEPT ALL` in both directions, and numeric aggregates. If a query fails, the result is `ERROR`.

## Secrets guard

`secrets_scan.find_literal_secrets` looks through a job definition for keys named like `password`, `token` or `api_key` that hold a real value, and for strings shaped like known tokens (Databricks, AWS, GitHub, GitLab, Slack, JWT, PEM private keys). References like `{{secrets/scope/key}}` and `${...}` are fine. It returns only the paths, never the values.

It matters because the rollback backup is a copy of the whole job definition. So `migrate` refuses to clone a job with a literal secret, before any backup is written, and `promote` checks again in case someone added one later. The finding is `L9.LITERAL_SECRET`.

## Failure behaviour

When the tool can't tell, it stops. An unreadable init script is a blocker. A failed comparison is `ERROR`. Missing output tables fail validation with instructions. Restore mode needs an explicit opt-in. The guard has a dry-run. Every stage can be run again and writes an event.
