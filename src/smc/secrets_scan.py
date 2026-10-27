"""Detect literal credentials inside job settings.

SMCT writes the full pre-promotion job definition to a workspace folder so a rollback is exact. That file must never
become a place where a pasted token lives, so promotion is refused until such values are moved to secrets.
Only the *location* of a suspicious value is reported - never the value itself.
"""
from __future__ import annotations

import re
from typing import Any, List

SENSITIVE_KEY = re.compile(r"(pass(word|wd)?|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|credential|auth)", re.I)
SECRET_VALUE = re.compile(
    r"(dapi[0-9a-f]{32}|AKIA[0-9A-Z]{16}|ghp_[A-Za-z0-9]{30,}|glpat-[A-Za-z0-9_\-]{20,}|"
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----|xox[baprs]-[A-Za-z0-9-]{10,}|eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.)")
REFERENCE = re.compile(r"(\{\{\s*secrets/|\{\{\s*job\.|\{\{\s*tasks\.|\$\{)", re.I)


def find_literal_secrets(obj: Any, path: str = "") -> List[str]:
    """Paths (e.g. 'tasks[0].new_cluster.spark_env_vars.DB_PASSWORD') of values that look like literal credentials."""
    out: List[str] = []
    if isinstance(obj, dict):
        name = str(obj.get("name", obj.get("key", "")))
        if "value" in obj or "default" in obj:          # {"name": "api_token", "default": "..."} job parameter shape
            for vk in ("value", "default"):
                v = obj.get(vk)
                if isinstance(v, str) and v and SENSITIVE_KEY.search(name) and not REFERENCE.search(v):
                    out.append(f"{path}.{vk} (parameter '{name}')")
        for k, v in obj.items():
            p = f"{path}.{k}" if path else str(k)
            if isinstance(v, str) and v and not REFERENCE.search(v):
                if SECRET_VALUE.search(v) or (SENSITIVE_KEY.search(str(k)) and len(v) >= 6 and k not in ("name", "key")
                                              and not v.startswith(("/", "s3://", "dbfs:", "abfss://", "http"))):
                    if p not in " ".join(out):
                        out.append(p)
            elif isinstance(v, (dict, list)):
                out.extend(find_literal_secrets(v, p))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            out.extend(find_literal_secrets(v, f"{path}[{i}]"))
    elif isinstance(obj, str) and SECRET_VALUE.search(obj) and not REFERENCE.search(obj):
        out.append(path)
    return sorted(set(out))
