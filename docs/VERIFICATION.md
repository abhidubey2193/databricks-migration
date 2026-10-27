# Live-workspace verification log

## Run 1 - 2026-10-27, Databricks Free Edition (AWS-hosted `*.cloud.databricks.com`), serverless notebook compute

Free Edition has **serverless compute only** (no classic clusters, no classic jobs), so only part of the checklist can be exercised there.

| Check | Result |
|---|---|
| Full unit suite (`unittest discover`) run from the workspace Git folder on serverless | **PASS** - 71 tests at the time (the suite is 86 now, after the catalog fix and the changes below; not re-run in the workspace since) |
| Offline fleet scan CLI on the synthetic fleet inside the workspace | **PASS** - same ranking/verdicts as the dev machine |
| `pipeline.discover` against the real Jobs/Clusters API through `db.Workspace` (SDK auth from the notebook) | **PASS** after fix - listed 6 jobs, wrote `workloads`, `findings`, `scan_history` and the status/cost views to `workspace.smct` |
| Classification of the 6 real jobs | All `ALREADY_SERVERLESS` (they were serverless already) |
| Clone / A-B / promote / rollback / guard on real classic jobs | **NOT TESTED** - needs a workspace with classic compute |
| Bundle deploy + Databricks App | **NOT TESTED** - needs the CLI |
| Cost views over `system.billing.*` | **NOT TESTED** |

Defects found by the live run (both fixed):
1. Default catalog `main` does not exist on every workspace (`workspace` on Free Edition) -> `NO_SUCH_CATALOG`. Fixed by `store.resolve_catalog`, used by the job entrypoint (tests added). When deploying the bundle on such a workspace also pass `-var="catalog=workspace"` and set `SMC_CATALOG` in `app.yaml`.
2. Importing the package from a Git folder inside a notebook needs `importlib.invalidate_caches()` after adding `src` to `sys.path` (workspace files are not visible to the import cache otherwise). Jobs are unaffected (fresh interpreter).

## Full checklist (run on a workspace that has classic jobs)


The offline suite covers the logic; this checklist covers the SDK calls in your workspace. Write down the cloud, region, date and serverless environment version.

| # | Step | Expected | Result |
|---|---|---|---|
| 1 | `databricks bundle validate -t dev` | OK | |
| 2 | `bundle deploy`, `bundle run smct_discover` | tables `workloads`, `findings` populated; clones excluded | |
| 3 | Seed demo (`demo/seed_demo_workspace.py`), re-run discover | demo jobs appear with expected verdicts | |
| 4 | Approve `smct-demo-ab-pass` (app), run `smct_migrate_validate` with `target_param=target_schema` | CLONED then VALIDATED; clone has no schedule; rewritten notebook has no `.cache()` | |
| 5 | Approve `smct-demo-ab-overflow`, same | VALIDATION_FAILED (serverless run raises / diff), promotion blocked | |
| 6 | Gate 2 + `smct_promote` on ab-pass | original job id now serverless, same name/schedule; backup JSON + bundle YAML written | |
| 7 | Request rollback + `smct_rollback` | original classic definition restored | |
| 8 | Create a duplicate-schedule clone by hand, run `smct_guard` | clone paused, `guard_log` row | |
| 9 | `smct_cost` after some runs | `cost_before_after` returns rows | |
| 10 | Clone a notebook job that has pip dependencies. Check whether the API accepts `environment_key` on the notebook task, or whether `create_with_fallback` kicks in (the clone's notes will say so) | | |
| 11 | Run validation on a job and check that the `[classic-baseline]` copy is gone afterwards and the production job shows no new run | | |
| 12 | Put a fake token in a test job's parameters, approve it, run migrate | MIGRATE_FAILED with `LITERAL_SECRET`, no clone, no backup file | |
