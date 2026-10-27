from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Config:
    catalog: str = "main"
    schema: str = "smct"
    backup_dir: str = "/Workspace/Shared/smct/backups"          # original job JSON before promotion (rollback source)
    migrated_dir: str = "/Workspace/Shared/smct/migrated"        # rewritten notebook copies (originals are never touched)
    bundle_out_dir: str = "/Workspace/Shared/smct/generated_bundles"
    ckpt_schema: str = ""                                        # where checkpoint() rewrites materialise (default <catalog>.<schema>_tmp)
    mount_map: Dict[str, str] = field(default_factory=dict)      # {"/mnt/raw": "/Volumes/main/lake/raw"}
    ansi: str = "try"                                            # try -> try_cast rewrite | disable -> set ansi.enabled=false
    performance_target: Optional[str] = None                     # PERFORMANCE_OPTIMIZED | STANDARD
    target_param: str = ""                                       # job parameter that redirects output schema (A/B "sandbox" mode)
    allow_restore: bool = False                                  # A/B "restore" mode mutates tables; must be opted in
    validation_catalog: str = ""
    exclude_columns: List[str] = field(default_factory=lambda: ["_ingested_at", "load_ts", "updated_at", "etl_ts"])
    team_tag: str = "team"
    cost_window_days: int = 30
    batch_size: int = 0                                          # >0: migrate tasks in dependency-ordered batches of N
    batch_upto: int = 0                                          # how many batches to include (1-based); 0 = all
    dry_run: bool = False
    approvers: List[str] = field(default_factory=list)

    @property
    def fq(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`"

    @property
    def ckpt(self) -> str:
        return self.ckpt_schema or f"{self.catalog}.{self.schema}_tmp"

    @staticmethod
    def from_args(ns) -> "Config":
        c = Config()
        for k, v in vars(ns).items():
            if v in (None, "") or not hasattr(c, k):
                continue
            cur = getattr(c, k)
            if isinstance(cur, dict):
                v = json.loads(v) if isinstance(v, str) else v
            elif isinstance(cur, list):
                v = [x.strip() for x in v.split(",") if x.strip()] if isinstance(v, str) else v
            elif isinstance(cur, bool):
                v = str(v).lower() in ("1", "true", "yes")
            elif isinstance(cur, int):
                v = int(v)
            setattr(c, k, v)
        return c
