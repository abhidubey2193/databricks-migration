# Demo script (about 5 minutes)

Read this as notes, not a script to recite. If you have never run it on a real workspace, do that first (see `VERIFICATION.md`), and only show steps that actually worked for you. Say plainly what you didn't run.

Before recording:

- `databricks bundle deploy -t dev -var="warehouse_id=<id>" -var="catalog=<catalog>"`
- `python demo/seed_demo_workspace.py --folder /Workspace/Users/<you>/smct_demo`
- `databricks bundle run smct_discover -t dev`
- Put the app in one tab, the Jobs UI in another, a terminal ready. Hide workspace URLs and other people's emails.

| Time | On screen | What to say |
|---|---|---|
| 0:00 | Terminal: `make scan-demo` | 22 sample jobs, scanned from a file with no workspace. 2 are ready, 7 can be fixed automatically, 1 can be migrated in part, 8 need manual work, 2 should stay classic, and 2 are already serverless. The 25-task job is ready, so there is no 10-task cap here. |
| 0:35 | App, Overview | The same view on the real workspace: counts per verdict, the burn-down, and the wave plan sorted by classic spend. This is the fleet tracking part. |
| 1:05 | Fleet, `cache_heavy`, then its code patch | The scan finds `.cache()`, `checkpoint()` and a mount call by line. The patch shows exactly what will change. The original notebook is untouched. |
| 1:35 | Fleet, `init_native` and `egg_lib` | The init script is read: pip lines turn into dependencies, `apt-get` is a blocker. An egg library is a blocker, with the wheel route as the fix. Nothing is skipped quietly. |
| 2:05 | Approvals, first gate: approve `smct-demo-ab-pass` and `smct-demo-ab-overflow` | A person decides what moves. Each approval is a row in the audit log. |
| 2:20 | Terminal: `databricks bundle run smct_migrate_validate -t dev -var="target_param=target_schema"` (cut the waiting) | The clone is created with no schedule. Validation runs a throwaway classic copy and the serverless clone into separate schemas. Your production job is not run. |
| 3:00 | Validation tab | `ab-pass` matches on schema, row counts and `EXCEPT ALL`. `ab-overflow` fails because the serverless run raises on integer overflow, so promotion is blocked. |
| 3:30 | Second gate, then `smct_promote`. Jobs UI: same job id, name and schedule, now serverless. Show `generated_bundles/<id>.yml` | The job keeps its identity. The YAML is ready to commit. If someone edits the job between clone and promotion, promotion stops. |
| 4:00 | App, request rollback, `smct_rollback`, Jobs UI shows classic again | The old definition comes back from the backup. |
| 4:20 | Make a duplicate scheduled clone by hand, `smct_guard`, Schedule guard tab | The guard pauses the duplicate and logs why. |
| 4:40 | Cost tab | DBUs and list-price dollars before and after, per run, by team tag, from the system tables. Classic figures leave out cloud VM cost. |
| 4:55 | Terminal: `make test` | 86 offline tests, including a full lifecycle against a fake workspace. |

Notes:

- Keep it under 5:00. Pre-run the slow steps and cut the waiting.
- If a step failed on your workspace, leave it out and say so in the README instead.
