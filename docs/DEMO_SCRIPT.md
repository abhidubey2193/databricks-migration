# 5-minute demo script (record this; every line maps to a scoring criterion)

Prep once: `databricks bundle deploy -t dev -var=warehouse_id=...`, `python demo/seed_demo_workspace.py --folder /Workspace/Users/<you>/smct_demo`, `bundle run smct_discover`. Open the app in one tab, the Jobs UI in another, terminal ready.

| Time | Show | Say |
|---|---|---|
| 0:00 | Terminal: `make scan-demo` | "22 classic jobs, one command, no workspace. 7 auto-fixable, 8 need manual work, 2 must stay classic - and 25-task job is *ready*, no 10-task cap." (L1, L2) |
| 0:30 | App **Overview** | "Same inventory on the real workspace: KPIs, verdict mix, burn-down, spend-ranked wave plan." (L1, 30 pts) |
| 1:00 | **Fleet** -> `cache_heavy` -> findings + *code patch* diff | "`.cache()`, `checkpoint()` and a mount call found at line level; patch shows exactly what SMCT will change. Originals untouched." (L6, L13) |
| 1:30 | Fleet -> `init_native` and `egg_lib` | "Init script parsed: pip lines translate, `apt-get` is a blocker. Egg is a blocker with the wheel remediation. Nothing is silently ignored." (L3, L5, L11) |
| 2:00 | **Approvals** Gate 1: approve `smct-demo-ab-pass` and `smct-demo-ab-overflow` | "Human gate, audit event, state machine." |
| 2:15 | Terminal: `bundle run smct_migrate_validate -t dev -var=target_param=target_schema` (speed up the video) | "Clone is created *with no schedule*; classic and serverless both run into shadow schemas." (L10, L8) |
| 3:00 | **Validation** tab | "ab-pass: PASS, schema/rows/EXCEPT ALL clean, runtime comparison. ab-overflow: serverless raises on int overflow - FAIL, promotion is blocked." (L8, 20 pts) |
| 3:30 | Gate 2 -> `bundle run smct_promote`; Jobs UI: same job id/name/schedule, now serverless; show `generated_bundles/<id>.yml` | "Original job keeps identity; Git-ready YAML emitted; settings-hash check prevents drift." (L9) |
| 4:00 | App -> Request rollback -> `smct_rollback`; Jobs UI shows classic again | "Exact classic definition restored from backup." |
| 4:20 | Create a duplicate scheduled clone by hand; `bundle run smct_guard`; **Schedule guard** tab | "Guard pauses it and logs why." (L10) |
| 4:40 | **Cost** tab | "DBUs and list-price $ before/after per run, by team tag, from system tables." (L12) |
| 4:55 | Terminal: `make test` | "74 offline tests incl. full lifecycle on a fake workspace." |

Tips: pre-run the slow job and cut; keep the video <= 5:00; do not show workspace URLs or emails of colleagues.
