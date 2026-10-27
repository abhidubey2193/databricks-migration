To: <submission address from the challenge brief>
Subject: Serverless Challenge - Submission - <your name>

Serverless Migration Challenge - Submission

Name(s) and team: <fill in>
Repo and final commit SHA: <private GitHub repo URL> @ <output of git rev-parse HEAD>
Admin access given to: Nivethan2893, diggibyte, veerabadrachari-diggibyte
Demo video: <link>
Cloud tested on: <AWS / Azure / GCP. Write only what you actually ran. See docs/VERIFICATION.md>

Limitations covered: L1 to L13. Short version of how:

- L1: Scans all jobs and all-purpose clusters, scores each 0-100, assigns a verdict and a wave, and ranks by 30-day DBUs. Everything is in Delta tables and a Databricks App with overview, burn-down, fleet detail, two approval gates, audit trail, validation, cost and guard log.
- L2: Own transformer with no task cap (tested at 25 and 40 tasks), plus optional dependency-ordered batches of N tasks for mixed compute.
- L3: Reads init scripts from DBFS, Volumes, Workspace and S3, turns pip installs into the serverless environment, and flags apt/yum/wget/classpath lines as blockers. An unreadable script is a blocker.
- L4: Detects custom images, ML runtimes, old DBR, instance profiles and storage credentials, and spark-submit. Each is classed as keep-on-classic or manual. Mixed jobs migrate in part.
- L5: PyPI, wheel and requirements libraries map to per-dependency-set environments. Egg, jar, maven, cran and DBFS wheels are flagged and stay on classic.
- L6: Rewrites cache/persist, checkpoint, global temp views, mount calls and /mnt paths (to Volumes via a mount map). RDD use and disallowed confs are detected.
- L7: Detects Scala and R. Scala is classified as SQL rewrite, PySpark draft, or keep on classic. R and JAR stay on classic.
- L8: A/B comparison (schema, counts, EXCEPT ALL, aggregate drift, runtime) between a throwaway classic copy and the serverless clone. Sandbox mode by default, restore mode opt-in. Promotion is blocked without a passing validation.
- L9: Every promotion emits bundle YAML and a diff. Bundle-managed and Git-sourced jobs are flagged, and a settings hash stops promotion if the job changed after cloning.
- L10: Clones are created with no trigger at all. A guard job pauses duplicates, including ones made by hand, with a dry-run and a log.
- L11: Every cluster attribute that gets dropped is reported per workload. Literal secrets in a job definition block cloning and promotion.
- L12: Views over system.billing.usage and list_prices, per job, day and team, with before/after per run.
- L13: Streaming triggers become availableNow, CAST becomes try_cast (or ansi=disable mode). Division, element_at, to_date and :: casts are detected with a try_* suggestion.

Architecture:
A core of plain-Python scanners, rewriters, a transformer, scoring, A/B diff, drift check and guard, with three thin modules for the SDK, Delta and orchestration.
Six serverless jobs share one entrypoint. State is in Delta; status is the latest row in an append-only event table.
The app reads through a SQL warehouse and writes approval events. A state machine enforces the two human gates.
Originals are never changed: clones, rewritten notebook copies and a backup JSON for rollback.

Databricks pieces used: Asset Bundles, serverless Jobs, Unity Catalog (tables, views, grants), SQL warehouse, Databricks Apps, system.billing, Delta (RESTORE), the Databricks SDK.

Deploy:
databricks bundle deploy -t dev -var="warehouse_id=<id>" -var="catalog=<catalog>"
databricks bundle run smct_discover -t dev
databricks bundle run smct_app -t dev
(full steps in the README)

Testing, honestly:
86 offline tests against in-memory fakes, including a full lifecycle. The same suite passes inside a Databricks workspace on serverless. Discovery has run against a real workspace (Free Edition). <Add here what you ran on a workspace with classic compute, or say it has not been run. See docs/VERIFICATION.md.>

Known gaps: see docs/LIMITATIONS.md. Main ones: division/element_at are detected but not auto-rewritten, %run children aren't followed, the A/B check assumes the job is idempotent, and path-based outputs need a tag.
Next: AST-based SQL rewriting, following %run, Lakeflow pipelines, more than one workspace.

No customer data, credentials or client workspace details are included: Yes
