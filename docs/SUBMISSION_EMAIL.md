To: <submission email address from the challenge brief>
Subject: Serverless Challenge - Submission - <your name>

Serverless Migration Challenge - Submission

Name(s) and team: <fill>
Repo link and final commit SHA (Option A) or zip file name (Option B): <private GitHub repo URL> @ <git rev-parse HEAD>   (Admin access granted to Nivethan2893, diggibyte, veerabadrachari-diggibyte)
Demo video link: <fill>
Cloud tested on (AWS / Azure / GCP): <fill - only what you actually ran, see docs/VERIFICATION.md>

Limitations addressed (IDs, e.g. L1, L3, L8): L1, L2, L3, L4, L5, L6, L7, L8, L9, L10, L11, L12, L13
For each one, 1-2 lines on how:
- L1: Inventory of all jobs + all-purpose clusters -> 0-100 readiness score, verdict, wave plan ranked by 30-day DBUs; Delta tables + Databricks App with overview, burn-down, fleet drill-down, two-gate approval workflow, audit trail, validation, cost, guard log.
- L2: Own deterministic transformer with no task cap (tested at 25/40 tasks) plus optional dependency-ordered batches of N tasks migrated in place (mixed compute).
- L3: Reads init scripts from DBFS/Volumes/Workspace/S3, translates pip installs into the serverless environment spec, flags apt/yum/wget/classpath lines as blockers, unreadable = blocker.
- L4: Detects custom image, ML runtime, DBR<13, instance profile/storage creds in conf, spark-submit; classifies keep-on-classic vs manual; mixed jobs migrate partially.
- L5: pypi/whl/requirements mapped to per-dependency-set environments; egg/jar/maven/cran/DBFS wheels flagged and left on classic.
- L6: Static scan + automated rewrites: cache/persist removed, checkpoint -> Delta, global temp views -> temp views, mount calls removed + /mnt paths -> UC Volumes via mount_map, CACHE TABLE handled; RDD/conf detected.
- L7: Scala/R detected; Scala classified (SQL / PySpark with draft / keep on classic); R and JAR keep on classic.
- L8: Classic vs serverless A/B (sandbox schema mode or opt-in restore mode) with schema, count, EXCEPT ALL, aggregate-drift and runtime comparison; promotion impossible without a passing validation.
- L9: Git-ready bundle YAML + diff on every promotion, bundle/git jobs flagged, settings-hash drift check aborts promotion.
- L10: Clones are created without any trigger; guard job pauses duplicates (incl. manual clones), dry-run + log.
- L11: Every dropped cluster attribute (conf, env vars, log delivery, SSH, policy, init scripts) reported per workload.
- L12: Views over system.billing.usage + list_prices: per job/day/team, before vs after promotion per run.
- L13: availableNow trigger auto-inserted/replaced; CAST->try_cast auto-rewrite (or ansi=disable mode); division/element_at/to_date/::cast detected with try_* suggestions.

Architecture (max 5 lines):
Pure-Python core (scanners, rewriters, transformer, scorer, A/B diff, drift, guard) -> thin SDK/Spark edges -> 6 serverless jobs sharing one entrypoint.
State lives in Delta (workloads, findings, append-only migration_events, validation_results, views for status + cost).
A Databricks App reads the tables through a SQL warehouse and writes approval events; a state machine enforces the two human gates.
Originals are never modified: clones + rewritten notebook copies; backup JSON enables exact rollback.

Databricks components used: Asset Bundles, Jobs (serverless), Unity Catalog schema/tables/views + grants, SQL warehouse, Databricks Apps, system.billing / list_prices, Delta (time travel/RESTORE/DEEP CLONE), Databricks SDK.

How to deploy (commands or steps):
databricks bundle deploy -t dev -var="warehouse_id=<id>" -var="catalog=<catalog>"; databricks bundle run smct_discover -t dev; databricks bundle run smct_app -t dev   (full steps in README)

What is tested and how: 71 offline unit tests (scanners, rewriters incl. ast-validity, transformer, scoring policy on a 22-job synthetic fleet, A/B diff against a fake SQL engine, full lifecycle incl. drift/rollback/guard against a fake workspace) + live run on <cloud> per docs/VERIFICATION.md.

Known gaps and what you would do next: see docs/LIMITATIONS.md (division/element_at not auto-rewritten, %run not followed, A/B assumes idempotent jobs, path-based outputs need a tag). Next: AST-based SQL rewriting, follow %run, Lakeflow/DLT pipelines, multi-workspace fleet.

Confirmation: no customer data, credentials or client workspace details are included (Yes / No): Yes
