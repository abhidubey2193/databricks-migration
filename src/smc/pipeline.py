"""Stage implementations: discover -> (human approval) -> migrate -> validate -> (human approval) -> promote / rollback.
Also: guard (L10) and cost views (L12). Every stage is idempotent and writes an audit event."""
from __future__ import annotations

import json
import re
from functools import lru_cache
from typing import Any, Dict, List, Optional

from . import ab_diff, drift, guard as guardmod, state
from .code_scan import detect_output_tables, rewrite_notebook, rewrite_python, scan_notebook, scan_python
from .config import Config
from .config_scan import scan_job, scan_standalone_cluster
from .db import Workspace
from .models import WorkloadReport
from .scoring import plan_waves, summarize
from .store import Store, now
from .transform import (CLONE_SUFFIX, TAG_CLONE, TAG_MIGRATED, batch_plan, canonical_hash, promote_settings,
                        to_serverless)


def _jid(workload_id: str) -> int:
    return int(workload_id.split(":", 1)[1])


# ======================================================================= discover (L1, L3, L4, L5, L6, L7, L11, L13)
def discover(ws: Workspace, store: Store, cfg: Config) -> Dict[str, int]:
    clusters = ws.all_purpose_clusters()
    nb_cache: Dict[str, Any] = {}
    init_cache: Dict[str, Any] = {}

    def nb(path):
        if path not in nb_cache:
            nb_cache[path] = ws.fetch_notebook(path)
        return nb_cache[path]

    def init(dest):
        if dest not in init_cache:
            init_cache[dest] = ws.fetch_init_script(dest)
        return init_cache[dest]

    reports: List[WorkloadReport] = []
    for j in ws.list_jobs():
        tags = (j.get("settings", {}).get("tags") or {})
        if tags.get(TAG_CLONE) == "true":
            continue                                   # SMCT's own clones are not fleet members
        reports.append(scan_job(j, clusters, init, nb, cfg.mount_map))
    for c in clusters.values():
        reports.append(scan_standalone_cluster(c, init))
    rows = plan_waves(reports, store.monthly_dbus())
    ts = now()
    store.ensure()
    store.replace("workloads", [{**r, "scanned_at": ts} for r in rows])
    frows = []
    for rep in reports:
        for f in rep.findings:
            d = f.to_dict()
            d.update(workload_id=rep.workload_id, scanned_at=ts)
            frows.append(d)
    store.replace("findings", frows)
    ev = store.latest_events()
    summ = summarize(rows)
    store.append("scan_history", [{"scanned_at": ts, "total": summ["TOTAL"], "ready": summ.get("READY", 0),
                                   "auto_fixable": summ.get("AUTO_FIXABLE", 0) + summ.get("PARTIAL", 0), "needs_manual": summ.get("NEEDS_MANUAL", 0),
                                   "keep_classic": summ.get("KEEP_CLASSIC", 0), "already_serverless": summ.get("ALREADY_SERVERLESS", 0),
                                   "promoted": sum(1 for e in ev.values() if e["event"] == "PROMOTED")}])
    store.create_views(cfg.team_tag, cfg.cost_window_days)
    print(f"[smct] discovered {summ}")
    return summ


# ======================================================================= migrate (L2, L3, L5, L6, L10, L13)
def migrate(ws: Workspace, store: Store, cfg: Config, ids: Optional[List[str]] = None) -> None:
    ids = ids or store.ids_in_status("APPROVED")
    clusters = ws.all_purpose_clusters()
    for wid in ids:
        if not wid.startswith("job:"):
            continue
        try:
            _migrate_one(ws, store, cfg, wid, clusters)
        except Exception as e:  # noqa: BLE001
            store.add_event(wid, "MIGRATE_FAILED", detail={"error": f"{type(e).__name__}: {e}"[:500]})
            print(f"[smct] {wid} migrate failed: {e}")


def _migrate_one(ws: Workspace, store: Store, cfg: Config, wid: str, clusters: Dict[str, Any]) -> None:
    job_id = _jid(wid)
    job = ws.get_job(job_id)
    settings = job["settings"]
    path_map: Dict[str, str] = {}
    applied_all: Dict[str, List[str]] = {}
    patches = []
    if not settings.get("git_source"):
        for t in settings.get("tasks", []):
            for tk, pk in (("notebook_task", "notebook_path"), ("spark_python_task", "python_file")):
                p = (t.get(tk) or {}).get(pk)
                if not p or p in path_map:
                    continue
                got = ws.fetch_notebook(p)
                if not got:
                    continue
                text, lang = got
                if tk == "notebook_task":
                    new, applied = rewrite_notebook(p, text, lang, cfg.mount_map, cfg.ckpt, cfg.ansi)
                else:
                    new, applied = rewrite_python(text, p, cfg.mount_map, cfg.ckpt)
                if applied and new != text:
                    dest = f"{cfg.migrated_dir}/{job_id}/{p.rstrip('/').split('/')[-1]}"
                    if tk == "notebook_task":
                        ws.import_notebook(dest, new, lang)
                    else:
                        ws.write_text(dest, new)
                    path_map[p] = dest
                    applied_all[p] = applied
                    patches.append({"workload_id": wid, "ts": now(), "path": p, "applied_rules": ",".join(applied),
                                    "patch": drift.code_patch(p, text, new)})
    store.append("code_patches", patches)
    # batched (staged) migration for very large jobs
    task_keys = None
    if cfg.batch_size:
        batches = batch_plan(settings, cfg.batch_size)
        upto = cfg.batch_upto or len(batches)
        task_keys = [k for b in batches[:upto] for k in b]
    res = to_serverless(settings, clone=True, origin_job_id=job_id, task_keys=task_keys, clusters_by_id=clusters,
                        init_fetch=ws.fetch_init_script, notebook_path_map=path_map,
                        performance_target=cfg.performance_target)
    if not res.migrated_tasks:
        store.add_event(wid, "MIGRATE_FAILED", detail={"reason": "no task could be moved", "left_on_classic": res.left_on_classic})
        return
    clone_id = ws.create_job(res.settings)
    backup = f"{cfg.backup_dir}/{job_id}/{now().strftime('%Y%m%dT%H%M%S')}.json"
    ws.write_text(backup, json.dumps(settings, indent=2, sort_keys=True))
    store.add_event(wid, "CLONED", detail={
        "clone_job_id": clone_id, "migrated_tasks": res.migrated_tasks, "left_on_classic": res.left_on_classic,
        "notes": res.notes, "path_map": path_map, "applied_rules": applied_all,
        "original_hash": canonical_hash(settings), "backup_path": backup})
    print(f"[smct] {wid}: clone job {clone_id} created ({len(res.migrated_tasks)} tasks serverless, "
          f"{len(res.left_on_classic)} left on classic) - clone has NO schedule")


# ======================================================================= validate (L8)
def validate(ws: Workspace, store: Store, cfg: Config, spark, ids: Optional[List[str]] = None) -> None:
    ids = ids or store.ids_in_status("CLONED")
    sql = lambda q: [r.asDict() for r in spark.sql(q).collect()]  # noqa: E731
    for wid in ids:
        try:
            _validate_one(ws, store, cfg, spark, sql, wid)
        except Exception as e:  # noqa: BLE001
            store.add_event(wid, "VALIDATION_FAILED", detail={"error": f"{type(e).__name__}: {e}"[:500]})
            print(f"[smct] {wid} validation errored: {e}")


def _output_tables(ws: Workspace, settings: Dict[str, Any]) -> List[str]:
    tagged = (settings.get("tags") or {}).get("smct_output_tables")
    if tagged:
        return [t.strip() for t in tagged.split(",") if t.strip()]
    out: List[str] = []
    for t in settings.get("tasks", []):
        for tk, pk in (("notebook_task", "notebook_path"), ("spark_python_task", "python_file")):
            p = (t.get(tk) or {}).get(pk)
            got = ws.fetch_notebook(p) if p else None
            if got:
                out.extend(x for x in detect_output_tables(got[0]) if x not in out)
    return out


def _validate_one(ws, store, cfg, spark, sql, wid) -> None:
    cl = store.last_event(wid, "CLONED")
    if not cl:
        raise RuntimeError("no CLONED event")
    job_id, clone_id = _jid(wid), int(cl["detail"]["clone_job_id"])
    orig = ws.get_job(job_id)["settings"]
    params = {p["name"] for p in orig.get("parameters", []) or []}
    diffs: List[ab_diff.TableDiff] = []
    perf: Dict[str, Any] = {}
    ca = sa = ""
    if cfg.target_param and cfg.target_param in params:                       # ---- sandbox mode (no production writes)
        cat = cfg.validation_catalog or cfg.catalog
        sc_a, sc_b = f"{cat}.smct_val_{job_id}_classic", f"{cat}.smct_val_{job_id}_serverless"
        for sc in (sc_a, sc_b):
            spark.sql(f"DROP SCHEMA IF EXISTS {sc} CASCADE")
            spark.sql(f"CREATE SCHEMA {sc}")
        ra = ws.run_and_wait(job_id, {cfg.target_param: sc_a})
        rb = ws.run_and_wait(clone_id, {cfg.target_param: sc_b})
        ca, sa = ra["run_id"], rb["run_id"]
        perf = ab_diff.perf_summary(ra["seconds"], rb["seconds"])
        if ra["result"] != "SUCCESS" or rb["result"] != "SUCCESS":
            raise RuntimeError(f"run failed: classic={ra['result']} serverless={rb['result']}")
        tables = [r["tableName"] for r in sql(f"SHOW TABLES IN {sc_a}")]
        for t in tables:
            diffs.append(ab_diff.compare_tables(sql, f"{sc_a}.{t}", f"{sc_b}.{t}", cfg.exclude_columns))
    else:                                                                       # ---- restore mode (opt-in)
        outs = _output_tables(ws, orig)
        if not outs:
            raise RuntimeError("no output tables detected: tag the job with smct_output_tables=cat.schema.t1,... "
                               "or define the job parameter named by --target_param")
        if not cfg.allow_restore:
            raise RuntimeError("restore-mode validation re-runs the job against production tables and uses Delta RESTORE; "
                               "re-run with --allow_restore true (preferably in a quiet window) or use --target_param sandbox mode")
        shadow = f"{cfg.catalog}.smct_val_{job_id}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {shadow}")
        v0 = {t: int(sql(f"DESCRIBE HISTORY {t} LIMIT 1")[0]["version"]) for t in outs}
        ra = ws.run_and_wait(job_id)
        for t in outs:
            spark.sql(f"CREATE OR REPLACE TABLE {shadow}.{t.split('.')[-1]}_classic DEEP CLONE {t}")
            spark.sql(f"RESTORE TABLE {t} TO VERSION AS OF {v0[t]}")
        rb = ws.run_and_wait(clone_id)
        ca, sa = ra["run_id"], rb["run_id"]
        perf = ab_diff.perf_summary(ra["seconds"], rb["seconds"])
        if ra["result"] != "SUCCESS" or rb["result"] != "SUCCESS":
            raise RuntimeError(f"run failed: classic={ra['result']} serverless={rb['result']}")
        for t in outs:
            diffs.append(ab_diff.compare_tables(sql, f"{shadow}.{t.split('.')[-1]}_classic", t, cfg.exclude_columns))
    verdict = ab_diff.overall_verdict(diffs)
    ts = now()
    store.append("validation_results", [{
        "workload_id": wid, "run_ts": ts, "classic_run_id": ca, "serverless_run_id": sa, "table_a": d.table_a,
        "table_b": d.table_b, "status": d.status, "rows_a": d.rows_a, "rows_b": d.rows_b, "only_in_a": d.only_in_a,
        "only_in_b": d.only_in_b, "reasons": json.dumps(d.reasons), "perf": json.dumps(perf)} for d in diffs])
    store.add_event(wid, "VALIDATED" if verdict == "PASS" else "VALIDATION_FAILED",
                    detail={"verdict": verdict, "tables": len(diffs), "perf": perf,
                            "failed": [d.table_b for d in diffs if d.status != "PASS"]})
    print(f"[smct] {wid}: A/B verdict {verdict} ({len(diffs)} tables) perf={perf}")


# ======================================================================= promote / rollback (L8, L9)
def promote(ws: Workspace, store: Store, cfg: Config, ids: Optional[List[str]] = None) -> None:
    for wid in ids or store.ids_in_status("PROMOTE_APPROVED"):
        try:
            job_id = _jid(wid)
            cl = store.last_event(wid, "CLONED")["detail"]
            if store.last_event(wid, "VALIDATED") is None:
                raise RuntimeError("refusing to promote: no passing A/B validation on record")
            cur = ws.get_job(job_id)["settings"]
            if drift.detect_drift(cl["original_hash"], cur):
                store.add_event(wid, "VALIDATION_FAILED", detail={"reason": "DRIFT: job was edited after the clone was made; re-clone and re-validate"})
                continue
            clone = ws.get_job(int(cl["clone_job_id"]))["settings"]
            new = promote_settings(cur, clone)
            stamp = now().strftime("%Y%m%dT%H%M%S")
            backup = f"{cfg.backup_dir}/{job_id}/promote_{stamp}.json"
            ws.write_text(backup, json.dumps(cur, indent=2, sort_keys=True))
            bundle_note = None
            ypath = f"{cfg.bundle_out_dir}/{job_id}.yml"          # L9: always emit the Git-ready definition
            ws.write_text(ypath, drift.bundle_yaml(cur["name"], new))
            ws.write_text(f"{cfg.bundle_out_dir}/{job_id}.diff", drift.settings_diff(cur, new, cur["name"]))
            bundle_note = ypath
            ws.reset_job(job_id, new)
            ws.update_job(int(cl["clone_job_id"]), guardmod.pause_settings(clone))
            store.add_event(wid, "PROMOTED", detail={"backup_path": backup, "bundle_yaml": bundle_note,
                                                      "bundle_managed": (cur.get("deployment") or {}).get("kind") == "BUNDLE",
                                                      "new_hash": canonical_hash(new), "clone_job_id": cl["clone_job_id"]})
            print(f"[smct] {wid}: PROMOTED. rollback file: {backup}; bundle yaml: {bundle_note}")
        except Exception as e:  # noqa: BLE001
            print(f"[smct] {wid} promote failed: {e}")
            store.add_event(wid, "VALIDATION_FAILED", detail={"error": f"promote: {e}"[:400]})


def rollback(ws: Workspace, store: Store, cfg: Config, ids: Optional[List[str]] = None) -> None:
    for wid in ids or store.ids_in_status("ROLLBACK_REQUESTED"):
        pr = store.last_event(wid, "PROMOTED")
        if not pr:
            print(f"[smct] {wid}: nothing to roll back")
            continue
        backup = json.loads(ws.read_text(pr["detail"]["backup_path"]))
        ws.reset_job(_jid(wid), backup)
        store.add_event(wid, "ROLLED_BACK", detail={"restored_from": pr["detail"]["backup_path"]})
        print(f"[smct] {wid}: restored classic definition from {pr['detail']['backup_path']}")


# ======================================================================= guard (L10)
def run_guard(ws: Workspace, store: Store, cfg: Config) -> int:
    jobs = ws.list_jobs()
    vs = guardmod.find_violations(jobs)
    rows = []
    for v in vs:
        if not cfg.dry_run:
            ws.update_job(int(v.job_id), v.paused_settings)
        rows.append({"ts": now(), "job_id": str(v.job_id), "name": v.name, "reason": v.reason,
                     "action": "PAUSED" if not cfg.dry_run else "WOULD_PAUSE", "dry_run": cfg.dry_run})
    store.ensure()
    store.append("guard_log", rows)
    print(f"[smct] guard: {len(vs)} duplicate-schedule violation(s) {'(dry run)' if cfg.dry_run else 'paused'}")
    return len(vs)
