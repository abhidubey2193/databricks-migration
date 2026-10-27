"""L10: never allow a job and its serverless clone to both fire on a schedule/trigger."""
from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Dict, List

from .transform import TAG_CLONE, TAG_ORIGIN, TAG_MIGRATED, TRIGGER_KEYS


@dataclass
class Violation:
    job_id: Any
    name: str
    reason: str
    action: str = "PAUSE"
    paused_settings: Dict[str, Any] = None


def is_active(settings: Dict[str, Any]) -> bool:
    return any((settings.get(k) or {}).get("pause_status") != "PAUSED" for k in TRIGGER_KEYS if settings.get(k))


def pause_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Minimal settings payload that pauses every trigger (schedule/trigger/continuous)."""
    out = {}
    for k in TRIGGER_KEYS:
        if settings.get(k):
            v = copy.deepcopy(settings[k])
            v["pause_status"] = "PAUSED"
            out[k] = v
    return out


def find_violations(jobs: List[Dict[str, Any]]) -> List[Violation]:
    by_id = {str(j.get("job_id")): j for j in jobs}
    out: List[Violation] = []
    for j in jobs:
        s = j.get("settings", {})
        tags = s.get("tags") or {}
        if tags.get(TAG_CLONE) == "true" and is_active(s):
            origin = by_id.get(str(tags.get(TAG_ORIGIN)))
            if origin is None or is_active(origin.get("settings", {})) or (origin["settings"].get("tags") or {}).get(TAG_MIGRATED):
                out.append(Violation(j["job_id"], s.get("name", ""),
                                     "clone has an active trigger while the original job is also active/promoted",
                                     paused_settings=pause_settings(s)))
    # name+cron duplicates (manual clones made outside SMCT)
    seen: Dict[tuple, Dict[str, Any]] = {}
    for j in jobs:
        s = j.get("settings", {})
        sched = s.get("schedule")
        if not sched or sched.get("pause_status") == "PAUSED":
            continue
        base = s.get("name", "").replace(" [serverless]", "").strip().lower()
        k = (base, sched.get("quartz_cron_expression"))
        if k in seen and seen[k]["job_id"] != j["job_id"]:
            dup = j if " [serverless]" in s.get("name", "") or (s.get("tags") or {}).get(TAG_CLONE) else None
            if dup is not None and all(v.job_id != dup["job_id"] for v in out):
                out.append(Violation(dup["job_id"], dup["settings"]["name"], "same name+cron as another active job",
                                     paused_settings=pause_settings(dup["settings"])))
        else:
            seen[k] = j
    return out
