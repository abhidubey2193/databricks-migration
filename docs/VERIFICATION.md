# Live-workspace verification log (fill in before submitting)

The offline suite proves the logic; this checklist proves the SDK edges in *your* workspace. Record cloud, region, date, DBR/serverless environment version.

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
| 10 | Confirm `environment_key` accepted for notebook tasks on your release (else adjust `ENV_TASK_TYPES`) | | |
