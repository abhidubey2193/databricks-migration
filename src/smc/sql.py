"""DDL, views and cost SQL. `{fq}` = `catalog`.`schema` placeholder (see Store.fq)."""

TABLE_DDL = {
    "workloads": """workload_id STRING, kind STRING, name STRING, owner STRING, tags STRING, schedule STRING,
        paused BOOLEAN, task_count INT, score INT, verdict STRING, wave INT, blockers INT, auto_fixable INT,
        limitations STRING, monthly_dbus DOUBLE, rank INT, wave_label STRING, scanned_at TIMESTAMP""",
    "findings": """workload_id STRING, rule_id STRING, limitation STRING, severity STRING, auto_fixable BOOLEAN,
        keep_on_classic BOOLEAN, location STRING, message STRING, remediation STRING, snippet STRING, scanned_at TIMESTAMP""",
    "scan_history": """scanned_at TIMESTAMP, total INT, ready INT, auto_fixable INT, needs_manual INT, keep_classic INT,
        already_serverless INT, promoted INT""",
    "migration_events": "workload_id STRING, ts TIMESTAMP, actor STRING, event STRING, detail STRING",
    "validation_results": """workload_id STRING, run_ts TIMESTAMP, classic_run_id STRING, serverless_run_id STRING,
        table_a STRING, table_b STRING, status STRING, rows_a BIGINT, rows_b BIGINT, only_in_a BIGINT, only_in_b BIGINT,
        reasons STRING, perf STRING""",
    "code_patches": "workload_id STRING, ts TIMESTAMP, path STRING, applied_rules STRING, patch STRING",
    "guard_log": "ts TIMESTAMP, job_id STRING, name STRING, reason STRING, action STRING, dry_run BOOLEAN",
}

# Latest human/system event per workload = current status
VIEW_STATUS = """
CREATE OR REPLACE VIEW {fq}.migration_status AS
SELECT w.workload_id, w.name, w.kind, w.wave, w.score, w.verdict,
       COALESCE(e.event, 'DISCOVERED') AS status, e.actor, e.ts AS status_ts, e.detail
FROM {fq}.workloads w
LEFT JOIN (SELECT *, row_number() OVER (PARTITION BY workload_id ORDER BY ts DESC) rn FROM {fq}.migration_events) e
  ON e.workload_id = w.workload_id AND e.rn = 1
"""

# --- L12: DBU + list-price cost per migrated job, before vs after promotion.
# usage_metadata.job_id joins to our workload_id 'job:<id>'. Classic = non-serverless SKU, serverless = SKU contains SERVERLESS.
# NOTE: classic DBU cost excludes cloud VM cost (billed by the cloud provider) - see README.
VIEW_COST = """
CREATE OR REPLACE VIEW {fq}.cost_by_job_day AS
SELECT concat('job:', u.usage_metadata.job_id) AS workload_id,
       u.usage_date,
       CASE WHEN u.sku_name LIKE '%SERVERLESS%' THEN 'serverless' ELSE 'classic' END AS compute,
       u.custom_tags[ '{team_tag}' ] AS team,
       sum(u.usage_quantity) AS dbus,
       sum(u.usage_quantity * p.pricing.default) AS list_cost_usd,
       count(DISTINCT u.usage_metadata.job_run_id) AS runs
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name AND u.cloud = p.cloud
 AND u.usage_end_time >= p.price_start_time AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE u.usage_metadata.job_id IS NOT NULL AND u.usage_unit = 'DBU'
GROUP BY ALL
"""

VIEW_COST_COMPARE = """
CREATE OR REPLACE VIEW {fq}.cost_before_after AS
WITH promoted AS (
  SELECT workload_id, min(ts) AS promoted_ts FROM {fq}.migration_events WHERE event = 'PROMOTED' GROUP BY workload_id),
c AS (SELECT * FROM {fq}.cost_by_job_day)
SELECT p.workload_id,
       p.promoted_ts,
       sum(CASE WHEN c.usage_date <  to_date(p.promoted_ts) AND c.usage_date >= date_sub(to_date(p.promoted_ts), {window}) THEN c.dbus END) AS dbus_before,
       sum(CASE WHEN c.usage_date <  to_date(p.promoted_ts) AND c.usage_date >= date_sub(to_date(p.promoted_ts), {window}) THEN c.list_cost_usd END) AS usd_before,
       sum(CASE WHEN c.usage_date <  to_date(p.promoted_ts) AND c.usage_date >= date_sub(to_date(p.promoted_ts), {window}) THEN c.runs END) AS runs_before,
       sum(CASE WHEN c.usage_date >= to_date(p.promoted_ts) THEN c.dbus END) AS dbus_after,
       sum(CASE WHEN c.usage_date >= to_date(p.promoted_ts) THEN c.list_cost_usd END) AS usd_after,
       sum(CASE WHEN c.usage_date >= to_date(p.promoted_ts) THEN c.runs END) AS runs_after,
       max(c.team) AS team
FROM promoted p LEFT JOIN c ON c.workload_id = p.workload_id
GROUP BY p.workload_id, p.promoted_ts
"""

# Spend ranking for wave planning: last 30 days of classic DBUs per job.
Q_MONTHLY_DBUS = """
SELECT concat('job:', usage_metadata.job_id) AS workload_id, sum(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_metadata.job_id IS NOT NULL AND usage_unit = 'DBU' AND usage_date >= date_sub(current_date(), 30)
  AND sku_name NOT LIKE '%SERVERLESS%'
GROUP BY 1
"""
