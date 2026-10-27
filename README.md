# Serverless Migration Control Tower (SMCT)

**Move a whole Databricks workspace from classic to serverless - tracked, fixed, proven equal, and reversible.**

SMCT is a Databricks Asset Bundle that runs *on serverless itself*. It answers the four questions a migration lead actually has:

1. **Where are we?** Every job and cluster, scored 0-100, in waves, with a live burn-down. (L1)
2. **What blocks us, and can it be fixed automatically?** 41 rules across L2-L13; auto-fixes for caching, checkpoints, global temp views, mounts, streaming triggers, ANSI casts, libraries, init-script pip installs. (L2-L7, L11, L13)
3. **Is the serverless version actually the same?** Classic-vs-serverless A/B on the real outputs, with promotion *impossible* until it passes. (L8)
4. **Can we undo it, and did it save money?** Exact-definition rollback, Git-ready YAML, duplicate-schedule guard, DBU/$ before vs after. (L9, L10, L12)

```
 discover ──► [Gate 1: human approves]  ──► migrate ──► validate (A/B) ──► [Gate 2: human approves] ──► promote ──► (rollback)
 scan+score    app / audit event            rewrite code,    classic run vs        app / audit event       backup + reset original    restore exact
 plan waves                                 clone w/o         serverless run,                               + bundle YAML + diff       classic JSON
                                            schedule          table-level diff
                                   guard (every 30 min): no clone may ever fire alongside its original
```

## Why this is safe by construction
| Risk | Design answer |
|---|---|
| Double execution | Clone is created **without any trigger**; guard job pauses violators; promotion keeps the *original* job id, schedule, ownership, notifications. |
| Silent wrong results | A/B diff on schema, counts, `EXCEPT ALL`, aggregate drift. `ERROR` is never `PASS`. No `VALIDATED` event => `promote` refuses. |
| Touching originals | Notebook rewrites go to **copies** (`migrated_dir`); original notebooks are never modified. Original job JSON is backed up before promotion. |
| Concurrent edits | Settings hash taken at clone time; promotion aborts on drift. |
| Unreviewed change | Two human gates, every action an append-only `migration_events` row (who, when, detail). |
| Cost surprises | Cost views compare **per run** before/after; caveat on VM cost shown in UI. |

## Quick start (10 minutes)
Prerequisites: Unity Catalog, a SQL warehouse, Databricks CLI >= 0.250 authenticated, serverless jobs enabled, a **workspace admin** (or service principal with admin) so the scan can see all jobs, and `SELECT` on `system.billing` + `system.lakeflow` for cost.

```bash
git clone <this repo> && cd serverless-migration-control-tower
python -m unittest discover -s tests -t .                 # 74 offline tests, no workspace needed
make scan-demo                                            # offline fleet report on 22 synthetic jobs

databricks bundle validate -t dev
databricks bundle deploy   -t dev -var="warehouse_id=<warehouse-id>" -var="catalog=<your_catalog>"
databricks bundle run smct_discover -t dev                # inventory + scoring
databricks bundle run smct_app -t dev                     # opens the control-tower app
```
Optional demo data (creates *paused* classic jobs, nothing runs): `python demo/seed_demo_workspace.py --folder /Workspace/Users/<you>/smct_demo`

### Operating loop
1. App -> **Approvals -> Gate 1**: tick workloads (READY / AUTO_FIXABLE / PARTIAL) -> *Approve*.
2. `databricks bundle run smct_migrate_validate -t dev` (add `-var="target_param=target_schema"` for sandbox A/B; see below).
3. App -> **Validation** tab: inspect per-table result and runtime comparison. App -> **Gate 2** -> *Approve promotion*.
4. `databricks bundle run smct_promote -t dev`. Commit `generated_bundles/<job>.yml` to Git if the job is bundle-managed.
5. Wrong? App -> *Request rollback* -> `databricks bundle run smct_rollback -t dev` restores the exact classic definition.
6. `smct_guard` (every 30 min, enable via `guard_schedule_status=UNPAUSED`) keeps schedules single-fire. `smct_cost` refreshes cost views.

### A/B validation modes
* **sandbox (recommended)** - your job exposes a parameter such as `target_schema`; set `-var="target_param=target_schema"`. SMCT runs classic and serverless into separate shadow schemas (`smct_val_<id>_classic|serverless`) and diffs every table. Zero production writes.
* **restore (opt-in)** - for jobs with no such parameter: tag the job `smct_output_tables=cat.schema.t1,cat.schema.t2` (or let SMCT detect writes), pass `-var="allow_restore=true"`. SMCT records Delta versions, runs classic, clones outputs, `RESTORE`s, runs serverless, diffs. Use in a quiet window - production tables are briefly rewound.

### Configuration (bundle variables)
`mount_map` (`{"/mnt/raw":"/Volumes/c/s/raw"}`), `ansi` (`try`|`disable`), `performance_target`, `batch_size`/`batch_upto` (staged migration of huge DAGs), `backup_dir`, `migrated_dir`, `bundle_out_dir`, `team_tag`. Approvers: `SMC_APPROVERS` in `app.yaml`.

## Offline CLI (no workspace)
```bash
PYTHONPATH=src python -m smc.cli scan      --fixtures tests/fixtures/fleet.json --detail 11
PYTHONPATH=src python -m smc.cli rewrite   --file my_notebook.py --mount-map '{"/mnt/raw":"/Volumes/c/s/raw"}'   # prints a diff
PYTHONPATH=src python -m smc.cli transform --fixtures tests/fixtures/fleet.json --job 12 --batch-size 10        # 25-task DAG -> bundle YAML
PYTHONPATH=src python -m smc.cli guard     --fixtures tests/fixtures/fleet.json
PYTHONPATH=src python -m smc.cli coverage                                                                     # every rule + fix/detect
```

## Repository map
```
databricks.yml, resources/jobs.yml   bundle: 6 serverless jobs, UC schema + grants, Databricks App
app/app.py, app.yaml                 Streamlit app: overview, fleet, approvals (2 gates), validation, cost, guard
src/smc/                             stdlib-only core:  code_scan  config_scan  initscripts  scoring  transform
                                     ab_diff  drift  guard  state  sql | SDK/Spark edges: db  store  pipeline
src/jobs/entry.py                    single job entrypoint (--stage ...)
tests/                               74 tests: scanners, rewriters, transformer, scoring policy, A/B diff, lifecycle (fake workspace)
demo/                                fixture generator, workspace seeder, demo script
docs/                                ARCHITECTURE, LIMITATIONS (L1-L13 matrix + known gaps), RUNBOOK, VERIFICATION, DEMO_SCRIPT, SUBMISSION_EMAIL
```
No client data, tokens or credentials anywhere; fixtures are synthetic.
