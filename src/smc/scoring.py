"""Readiness scoring (0-100), verdicts and wave planning (L1)."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from .models import BLOCKER, WARNING, INFO, WorkloadReport

PENALTY = {  # (severity, auto_fixable) -> points
    (BLOCKER, False): 35, (BLOCKER, True): 8,
    (WARNING, False): 6, (WARNING, True): 2,
    (INFO, False): 1, (INFO, True): 0,
}

WAVES = {
    -1: "Already serverless", 0: "Wave 0 - migrate now (no blockers)",
    1: "Wave 1 - auto-fix then migrate", 2: "Wave 2 - manual remediation needed",
    3: "Wave 3 - keep on classic (decision)",
}


def score_report(rep: WorkloadReport) -> WorkloadReport:
    # collapse repeated identical rules so one noisy notebook does not zero the score
    seen: Dict[tuple, int] = {}
    pts = 0
    for f in rep.findings:
        key = (f.rule_id, f.auto_fixable)
        seen[key] = seen.get(key, 0) + 1
        base = PENALTY[(f.severity, f.auto_fixable)]
        pts += base if seen[key] == 1 else (base // 5)
    rep.score = max(0, 100 - pts)
    blockers = [f for f in rep.findings if f.severity == BLOCKER]
    hard = [f for f in blockers if f.keep_on_classic]
    manual = [f for f in blockers if not f.auto_fixable]
    if hard:
        rep.verdict, rep.wave = "KEEP_CLASSIC", 3
    elif manual:
        rep.verdict, rep.wave = "NEEDS_MANUAL", 2
    elif blockers or any(f.auto_fixable and f.severity == WARNING for f in rep.findings):
        rep.verdict, rep.wave = "AUTO_FIXABLE", 1
    else:
        rep.verdict, rep.wave = "READY", 0
    return rep


def plan_waves(reports: Iterable[WorkloadReport], monthly_dbus: Optional[Dict[str, float]] = None) -> List[dict]:
    """Order workloads inside each wave: biggest spend first, then highest readiness (quick wins)."""
    monthly_dbus = monthly_dbus or {}
    rows = []
    for r in reports:
        row = r.to_row()
        row["monthly_dbus"] = round(monthly_dbus.get(r.workload_id, 0.0), 2)
        rows.append(row)
    rows.sort(key=lambda x: (x["wave"] if x["wave"] >= 0 else 99, -x["monthly_dbus"], -x["score"], x["name"]))
    for i, row in enumerate(rows, 1):
        row["rank"] = i
        row["wave_label"] = WAVES.get(row["wave"], "")
    return rows


def summarize(rows: List[dict]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        out[r["verdict"]] = out.get(r["verdict"], 0) + 1
    out["TOTAL"] = len(rows)
    return out
