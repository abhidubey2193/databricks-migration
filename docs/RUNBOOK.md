# Runbook

| Situation | Action |
|---|---|
| First deployment | README quick start. Confirm the app's service principal can `SELECT/MODIFY` the schema (the bundle grants it). |
| Scan shows few jobs | The job identity cannot see them. Run as a workspace admin / admin service principal (`run_as` in the target). |
| `cost views skipped` in logs | Grant `USE CATALOG system`, `USE SCHEMA` + `SELECT` on `system.billing.usage`, `system.billing.list_prices`, `system.lakeflow.*`, then run `smct_cost`. |
| A/B says `ERROR: no output tables detected` | Tag the job `smct_output_tables=cat.sch.t1,...` or add a `target_schema` job parameter and set `target_param`. |
| A/B `FAIL` | App -> Validation shows reasons + stat diffs. Typical causes: volatile columns (add to `exclude_columns`), float aggregation order (already rounded to 6 dp), ANSI behaviour (choose `ansi=disable` or fix the SQL), non-idempotent job. |
| Promotion aborted with DRIFT | Someone edited the original job after cloning. Re-approve (Gate 1) to re-clone and re-validate. |
| Bundle-managed job | After promotion commit `generated_bundles/<job_id>.yml` to the repo; otherwise the next `bundle deploy` restores classic. |
| Something is wrong after promotion | App -> Request rollback -> `smct_rollback`. The pre-promotion definition is restored byte-for-byte from `backup_dir`. Rewritten notebook copies stay in `migrated_dir`; originals were never changed. |
| Clone fires twice | Should be impossible; run `smct_guard` (check `guard_log`) and file a bug. |
| Migrate failed with `LITERAL_SECRET` | The job definition holds a password or token in plain text. Move it into a Databricks secret and reference it as `{{secrets/scope/key}}`, then approve again. |
| A `[classic-baseline]` job is left over | Validation deletes it when it finishes, even on failure. If the process was killed mid-run, delete it by hand (it carries the tag `smct_baseline`). |
| Cleaning up | Clones are tagged `smct_clone=true` and named `... [serverless]`; delete after promotion if desired. Shadow schemas are `smct_val_<job_id>_*`. |
