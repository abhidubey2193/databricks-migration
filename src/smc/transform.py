"""Deterministic job transformer: classic job settings -> serverless job settings.

Handles L2 (no 10-task cap, plus batched in-place migration), L5 (library mapping),
L3 (init-script deps -> environment spec), L10 (clones are born without any trigger),
L9 (promotion keeps the original identity/schedule/ownership).
Pure functions over Jobs-API JSON (as_dict()) - unit-testable offline.
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .config_scan import COMPUTE_TASK_KEYS, EnvSpec, scan_cluster_spec, scan_libraries
from .models import BLOCKER

ENV_CLIENT_VERSION = "2"                      # serverless environment version (`environment_version` in the spec); bump per release notes
# Per the Databricks bundle examples, environment_key is REQUIRED for Python script / wheel / dbt tasks on serverless.
# Notebook tasks need none unless they carry dependencies, and some API releases reject the key on them, so notebook
# tasks only get one when there is something to install (pipeline.create_with_fallback retries without it if refused).
ENV_ALWAYS_TYPES = ("spark_python_task", "python_wheel_task", "dbt_task")
ENV_TASK_TYPES = ENV_ALWAYS_TYPES + ("notebook_task",)
CLONE_SUFFIX = " [serverless]"
TAG_ORIGIN = "smct_origin_job_id"
TAG_CLONE = "smct_clone"
TAG_MIGRATED = "smct_migrated"
CLASSIC_ONLY = ("spark_jar_task", "spark_submit_task")
TRIGGER_KEYS = ("schedule", "trigger", "continuous")


@dataclass
class TransformResult:
    settings: Dict[str, Any]
    migrated_tasks: List[str] = field(default_factory=list)
    left_on_classic: List[Tuple[str, str]] = field(default_factory=list)   # (task_key, reason)
    notes: List[str] = field(default_factory=list)


def _cluster_for(task: Dict[str, Any], job_clusters: Dict[str, Dict], clusters_by_id: Dict[str, Dict]) -> Optional[Dict]:
    if task.get("new_cluster"):
        return task["new_cluster"]
    if task.get("job_cluster_key"):
        return job_clusters.get(task["job_cluster_key"])
    if task.get("existing_cluster_id"):
        return clusters_by_id.get(task["existing_cluster_id"])
    return None


def is_classic_compute_task(t: Dict[str, Any]) -> bool:
    return any(k in t for k in COMPUTE_TASK_KEYS) and bool(
        t.get("new_cluster") or t.get("existing_cluster_id") or t.get("job_cluster_key"))


def topo_order(tasks: List[Dict[str, Any]]) -> List[str]:
    deps = {t["task_key"]: {d["task_key"] for d in t.get("depends_on", [])} for t in tasks}
    order, ready = [], sorted(k for k, d in deps.items() if not d)
    done: Set[str] = set()
    while ready:
        k = ready.pop(0)
        order.append(k)
        done.add(k)
        for k2, d in sorted(deps.items()):
            if k2 not in done and k2 not in ready and d <= done:
                ready.append(k2)
    order.extend(k for k in deps if k not in done)   # cycles shouldn't exist; be total anyway
    return order


def batch_plan(settings: Dict[str, Any], size: int = 10) -> List[List[str]]:
    """Dependency-ordered batches of at most `size` classic tasks (for staged in-place migration)."""
    cls = {t["task_key"] for t in settings.get("tasks", []) if is_classic_compute_task(t)}
    ordered = [k for k in topo_order(settings.get("tasks", [])) if k in cls]
    return [ordered[i:i + size] for i in range(0, len(ordered), size)]


def to_serverless(settings: Dict[str, Any], *, clone: bool = True, origin_job_id: Any = None,
                  task_keys: Optional[List[str]] = None, clusters_by_id: Optional[Dict[str, Dict]] = None,
                  init_fetch: Optional[Callable[[str], Optional[str]]] = None,
                  notebook_path_map: Optional[Dict[str, str]] = None,
                  performance_target: Optional[str] = None) -> TransformResult:
    """Convert classic tasks to serverless. `task_keys` limits which tasks move (batched migration).
    `notebook_path_map` swaps notebook/python-file paths for rewritten copies."""
    s = copy.deepcopy(settings)
    clusters_by_id = clusters_by_id or {}
    job_clusters = {c["job_cluster_key"]: c.get("new_cluster", {}) for c in s.get("job_clusters", []) or []}
    res = TransformResult(settings=s)
    env_for_task: Dict[str, Tuple[str, ...]] = {}
    for t in s.get("tasks", []):
        key = t["task_key"]
        if not is_classic_compute_task(t) or (task_keys is not None and key not in task_keys):
            continue
        if any(c in t for c in CLASSIC_ONLY):
            res.left_on_classic.append((key, "JVM task type (jar/spark-submit) needs classic compute"))
            continue
        spec = _cluster_for(t, job_clusters, clusters_by_id) or {}
        cf, env = scan_cluster_spec(spec, f"task:{key}", init_fetch)
        lf, task_deps = scan_libraries(t.get("libraries", []), f"task:{key}")
        hard = [f for f in cf + lf if f.severity == BLOCKER and (f.limitation in ("L4", "L5") or f.rule_id == "L3.INIT_UNSUPPORTED_LINE")]
        if hard:
            res.left_on_classic.append((key, "; ".join(sorted({h.rule_id for h in hard}))))
            continue
        deps = tuple(dict.fromkeys(list(env.dependencies) + task_deps))
        env_for_task[key] = deps
        if env.env_vars:
            res.notes.append(f"{key}: cluster env vars dropped ({', '.join(env.env_vars)}); pass as job parameters/secrets")
        for k in ("new_cluster", "existing_cluster_id", "job_cluster_key", "libraries"):
            t.pop(k, None)
        if notebook_path_map:
            for tk, pk in (("notebook_task", "notebook_path"), ("spark_python_task", "python_file")):
                if tk in t and t[tk].get(pk) in notebook_path_map:
                    t[tk][pk] = notebook_path_map[t[tk][pk]]
        res.migrated_tasks.append(key)
    # environments: one per distinct dependency set
    distinct = list(dict.fromkeys(env_for_task.values()))
    envs = [e for e in s.get("environments", []) or []]
    for i, deps in enumerate(distinct, 1):
        ekey = "smct_env" if len(distinct) == 1 else f"smct_env_{i}"
        spec = {"environment_version": ENV_CLIENT_VERSION}
        if deps:
            spec["dependencies"] = list(deps)
        envs.append({"environment_key": ekey, "spec": spec})
        for t in s["tasks"]:
            if env_for_task.get(t["task_key"]) != deps:
                continue
            if any(k in t for k in ENV_ALWAYS_TYPES) or ("notebook_task" in t and deps):
                t["environment_key"] = ekey
    if envs:
        s["environments"] = envs
    still = {t.get("job_cluster_key") for t in s["tasks"] if t.get("job_cluster_key")}
    if "job_clusters" in s:
        s["job_clusters"] = [c for c in s["job_clusters"] if c["job_cluster_key"] in still]
        if not s["job_clusters"]:
            del s["job_clusters"]
    s.setdefault("queue", {"enabled": True})
    if performance_target:
        s["performance_target"] = performance_target
    if clone:
        s["name"] = s["name"] + CLONE_SUFFIX
        for k in TRIGGER_KEYS:                       # L10: a clone is born with no trigger at all
            s.pop(k, None)
        s.pop("deployment", None)
        s.pop("job_id", None)
        s["tags"] = {**(s.get("tags") or {}), TAG_CLONE: "true", TAG_ORIGIN: str(origin_job_id)}
    return res


def promote_settings(original: Dict[str, Any], migrated: Dict[str, Any]) -> Dict[str, Any]:
    """Build the settings to push onto the ORIGINAL job: migrated compute/tasks + original identity & triggers."""
    out = copy.deepcopy(migrated)
    out["name"] = original["name"]
    for k in TRIGGER_KEYS + ("email_notifications", "webhook_notifications", "notification_settings", "health",
                             "max_concurrent_runs", "timeout_seconds", "run_as", "parameters", "git_source",
                             "deployment", "description", "budget_policy_id", "edit_mode"):
        if k in original:
            out[k] = copy.deepcopy(original[k])
        elif k in out and k in TRIGGER_KEYS:
            out.pop(k)
    tags = {k: v for k, v in (original.get("tags") or {}).items()}
    tags[TAG_MIGRATED] = "true"
    out["tags"] = tags
    return out


def canonical_hash(settings: Dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(settings, sort_keys=True, default=str).encode()).hexdigest()[:16]


BASELINE_SUFFIX = " [classic-baseline]"
BASELINE_TAG = "smct_baseline"


def baseline_settings(original: Dict[str, Any], origin_job_id: Any) -> Dict[str, Any]:
    """An unscheduled, silent copy of the ORIGINAL job that keeps its classic compute.

    A/B validation runs this copy instead of the production job, so alerts, notifications and the original's run history
    are never touched by validation. The caller deletes it when the comparison is finished.
    """
    s = copy.deepcopy(original)
    s.pop("job_id", None)
    for k in ("schedule", "trigger", "continuous", "email_notifications", "webhook_notifications",
              "notification_settings", "deployment", "health"):
        s.pop(k, None)
    s["name"] = s.get("name", "job") + BASELINE_SUFFIX
    s["max_concurrent_runs"] = 1
    tags = dict(s.get("tags") or {})
    tags[BASELINE_TAG] = "true"
    tags[TAG_ORIGIN] = str(origin_job_id)
    s["tags"] = tags
    return s
