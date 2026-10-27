# Serverless Migration Control Tower

A tool for moving a whole Databricks workspace from classic compute to serverless, and keeping track of how far along you are.

The /compute migration agent handles one notebook or job at a time. It doesn't tell you how many jobs you have, which ones are ready, what a migrated job costs compared to before, or whether the migrated job still produces the same data. This project covers those gaps. It is deployed as an Asset Bundle and runs on serverless itself.

## Status, up front

- The logic is covered by 86 offline tests, which also pass when run inside a Databricks workspace (serverless notebook compute).
- Discovery has run against a real workspace and wrote its tables to Unity Catalog. That workspace was Databricks Free Edition, which has no classic compute, so there was nothing classic to migrate.
- Clone, A/B validation, promotion, rollback, the schedule guard, the app and the bundle deploy have **not** been run on real classic jobs yet. `docs/VERIFICATION.md` lists what was checked and what wasn't, and has a checklist for the rest.

## What it does

1. **Finds everything.** It lists every job and all-purpose cluster, scans their settings and notebook code, and gives each one a readiness score from 0 to 100, a list of blockers and a wave (0 = ready now, 1 = automatic fixes then migrate, 2 = needs manual work, 3 = keep on classic). Inside a wave, jobs with the highest classic spend come first.
2. **Fixes what it safely can.** Cache and checkpoint calls, global temp views, mount calls and `/mnt` paths, streaming triggers, `CAST` under ANSI mode, library mapping, and `pip install` lines in init scripts are rewritten automatically. Everything else is reported with a reason and a suggested fix. Original notebooks are never edited; rewritten copies go to a separate folder.
3. **Checks the result.** An A/B run compares the classic output with the serverless output table by table. A job cannot be promoted unless that check passed and a person approved it.
4. **Can be undone.** Promotion keeps the original job id, name, schedule and notifications, and saves the old definition first. Rollback puts it back exactly.

```
discover -> approve -> migrate -> validate (A/B) -> approve -> promote -> rollback if needed
 scan, score   person    clone with     classic copy vs       person    original job id
 plan waves              no schedule    serverless clone                 gets the new setup
```

A guard job runs on a timer and pauses any clone that has a schedule while its original is still active.

## How it handles the risky parts

- **Double runs.** A clone is created with no schedule or trigger at all. The guard pauses any that appear later.
- **Validation side effects.** The A/B test does not run your production job. It runs a temporary copy of it (no schedule, no notifications) on the original classic compute, then deletes the copy.
- **Wrong results.** Schema, row counts, `EXCEPT ALL` in both directions and aggregate drift are compared. An error in the comparison is reported as an error, never as a pass.
- **Edits in the meantime.** A hash of the job is taken when it is cloned. If someone changes the job before promotion, promotion stops.
- **Secrets.** If a job definition contains a password or token in plain text, the tool will not clone or promote it, because the rollback backup stores the whole definition. Move the value into a Databricks secret first. The report shows where the value is, never the value.

## Setup

You need Unity Catalog, a SQL warehouse, the Databricks CLI (0.250 or newer), serverless jobs enabled, and an admin (or service principal) so the scan can see all jobs. Cost views also need `SELECT` on `system.billing` and `system.lakeflow`.

```bash
python -m unittest discover -s tests -t .      # offline tests, no workspace needed
make scan-demo                                  # scan 22 made-up jobs locally

databricks bundle validate -t dev
databricks bundle deploy -t dev -var="warehouse_id=<warehouse-id>" -var="catalog=<catalog>"
databricks bundle run smct_discover -t dev
databricks bundle run smct_app -t dev
```

The catalog name differs between workspaces (`main` on most, `workspace` on Free Edition). The job entrypoint falls back to the session's current catalog if the one you pass doesn't exist, but the bundle's schema resource still needs the right name, so pass `catalog=`. Set `SMC_CATALOG` in `app.yaml` to match.

To try it on made-up data, `python demo/seed_demo_workspace.py --folder /Workspace/Users/<you>/smct_demo` creates paused classic jobs. It needs a workspace that has classic compute. Nothing is run.

### Day to day

1. In the app, open Approvals and approve the workloads you want to move.
2. `databricks bundle run smct_migrate_validate -t dev` clones them and runs the A/B check.
3. Check the Validation tab, then approve promotion.
4. `databricks bundle run smct_promote -t dev`. If the job is managed by a bundle, commit the generated `generated_bundles/<job>.yml` to Git, otherwise the next bundle deploy brings the classic version back.
5. If something is wrong, request a rollback in the app and run `smct_rollback`.
6. `smct_guard` keeps schedules single-fire (turn it on with `guard_schedule_status=UNPAUSED`). `smct_cost` refreshes the cost views.

### Two ways to run the A/B check

- **Sandbox (recommended).** The job takes a parameter such as `target_schema`. Set `-var="target_param=target_schema"` and both runs write to separate shadow schemas, so production tables are not touched.
- **Restore (opt-in).** For jobs without such a parameter. Tag the job `smct_output_tables=cat.schema.t1,...` and pass `-var="allow_restore=true"`. The tool records Delta versions, runs the classic copy, saves its outputs, restores the tables, runs the serverless clone and compares. Production tables are briefly rewound, so do this in a quiet window.

### Options

Bundle variables: `mount_map` (for example `{"/mnt/raw":"/Volumes/c/s/raw"}`), `ansi` (`try` or `disable`), `performance_target`, `batch_size` and `batch_upto` (migrate a large job in dependency-ordered batches), `backup_dir`, `migrated_dir`, `bundle_out_dir`, `team_tag`. Approvers are set with `SMC_APPROVERS` in `app.yaml`.

## Command line, no workspace needed

```bash
PYTHONPATH=src python -m smc.cli scan --fixtures tests/fixtures/fleet.json --detail 11
PYTHONPATH=src python -m smc.cli rewrite --file my_notebook.py --mount-map '{"/mnt/raw":"/Volumes/c/s/raw"}'
PYTHONPATH=src python -m smc.cli transform --fixtures tests/fixtures/fleet.json --job 12 --batch-size 10
PYTHONPATH=src python -m smc.cli guard --fixtures tests/fixtures/fleet.json
PYTHONPATH=src python -m smc.cli coverage
```

`coverage` prints every rule and whether it is an automatic fix or detection only.

## Layout

```
databricks.yml, resources/jobs.yml   the bundle: six serverless jobs, a schema with grants, the app
app/app.py, app.yaml                 Streamlit app (overview, fleet, approvals, validation, cost, guard)
src/smc/                             scanners, rewriters, transformer, scoring, A/B diff, drift, guard, state machine
                                     db.py, store.py, pipeline.py are the only parts that touch Databricks
src/jobs/entry.py                    one entrypoint shared by all jobs (--stage discover, migrate, ...)
tests/                               offline tests, using fake workspace, store and SQL engine
demo/                                fixture generator, workspace seeder
docs/                                architecture, limitation coverage, runbook, verification log, demo script
```

Every limitation from the brief (L1 to L13) and where it is handled is in `docs/LIMITATIONS.md`, along with the known gaps.

No customer data, tokens or credentials are in this repository. The fixtures are made up.
