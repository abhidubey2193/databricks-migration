from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

# severity
BLOCKER = "BLOCKER"   # serverless run would fail
WARNING = "WARNING"   # runs, but behaviour / capability differs
INFO = "INFO"         # informational (lost attributes etc.)


@dataclass
class Finding:
    rule_id: str                 # e.g. "L6.CACHE"
    limitation: str              # "L1".."L13"
    severity: str                # BLOCKER | WARNING | INFO
    message: str
    auto_fixable: bool = False   # does SMCT ship an automated fix?
    keep_on_classic: bool = False  # hard stop: recommend keeping on classic
    location: str = ""           # task key / notebook path:line / cluster attr
    remediation: str = ""
    snippet: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class WorkloadReport:
    workload_id: str             # "job:123" | "cluster:abc" | "notebook:/path"
    kind: str                    # job | cluster | notebook
    name: str
    owner: str = ""
    tags: Dict[str, str] = field(default_factory=dict)
    schedule: str = ""
    paused: bool = True
    task_count: int = 0
    findings: List[Finding] = field(default_factory=list)
    score: int = 100
    verdict: str = "READY"
    wave: int = 0

    def to_row(self) -> Dict[str, Any]:
        return {
            "workload_id": self.workload_id, "kind": self.kind, "name": self.name,
            "owner": self.owner, "tags": json.dumps(self.tags), "schedule": self.schedule,
            "paused": self.paused, "task_count": self.task_count, "score": self.score,
            "verdict": self.verdict, "wave": self.wave,
            "blockers": sum(1 for f in self.findings if f.severity == BLOCKER),
            "auto_fixable": sum(1 for f in self.findings if f.auto_fixable),
            "limitations": ",".join(sorted({f.limitation for f in self.findings})),
        }
