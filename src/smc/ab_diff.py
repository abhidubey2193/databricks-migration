"""L8: output comparison between the classic run and the serverless run (A = classic, B = serverless)."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

SqlExec = Callable[[str], List[Dict[str, Any]]]
FLOAT_TYPES = ("double", "float")
NUMERIC_TYPES = ("tinyint", "smallint", "int", "bigint", "double", "float", "decimal")
COMPLEX_PREFIX = ("map", "array", "struct", "variant")


def q(ident: str) -> str:
    return ".".join("`" + p.strip("`") + "`" for p in ident.split("."))


def parse_describe(rows: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    cols = []
    for r in rows:
        name = (r.get("col_name") or "").strip()
        if not name or name.startswith("#"):
            break                              # partition / detailed info section starts
        cols.append((name, (r.get("data_type") or "").strip().lower()))
    return cols


def select_list(cols: List[Tuple[str, str]], float_round: int = 6) -> str:
    """Comparable projection: rounds floats (engines sum in different orders), JSON-ifies complex types."""
    out = []
    for n, t in cols:
        if t.startswith(FLOAT_TYPES):
            out.append(f"round({q(n)}, {float_round}) AS {q(n)}")
        elif t.startswith(COMPLEX_PREFIX):
            out.append(f"to_json({q(n)}) AS {q(n)}")
        else:
            out.append(q(n))
    return ", ".join(out) or "1"


@dataclass
class TableDiff:
    table_a: str
    table_b: str
    status: str = "PASS"                       # PASS | FAIL | ERROR
    schema_ok: bool = True
    rows_a: int = 0
    rows_b: int = 0
    only_in_a: int = 0
    only_in_b: int = 0
    reasons: List[str] = field(default_factory=list)
    sample: List[Dict[str, Any]] = field(default_factory=list)
    stat_diffs: List[Dict[str, Any]] = field(default_factory=list)


def compare_tables(sql: SqlExec, a: str, b: str, exclude: Optional[List[str]] = None,
                   max_diff_ratio: float = 0.0, rel_tol: float = 1e-9, sample_rows: int = 5) -> TableDiff:
    exclude = {e.lower() for e in (exclude or [])}
    d = TableDiff(a, b)
    try:
        ca, cb = parse_describe(sql(f"DESCRIBE TABLE {q(a)}")), parse_describe(sql(f"DESCRIBE TABLE {q(b)}"))
        ma, mb = dict(ca), dict(cb)
        if ma != mb:
            d.schema_ok = False
            miss = sorted(set(ma) ^ set(mb))
            typ = sorted(c for c in set(ma) & set(mb) if ma[c] != mb[c])
            d.reasons.append(f"schema differs (missing/extra: {miss}; type changes: {typ})")
        common = [(n, t) for n, t in ca if n in mb and mb[n] == t and n.lower() not in exclude]
        d.rows_a = int(sql(f"SELECT count(*) AS n FROM {q(a)}")[0]["n"])
        d.rows_b = int(sql(f"SELECT count(*) AS n FROM {q(b)}")[0]["n"])
        sel = select_list(common)
        d.only_in_a = int(sql(f"SELECT count(*) AS n FROM (SELECT {sel} FROM {q(a)} EXCEPT ALL SELECT {sel} FROM {q(b)})")[0]["n"])
        d.only_in_b = int(sql(f"SELECT count(*) AS n FROM (SELECT {sel} FROM {q(b)} EXCEPT ALL SELECT {sel} FROM {q(a)})")[0]["n"])
        if d.rows_a != d.rows_b:
            d.reasons.append(f"row count differs: classic={d.rows_a} serverless={d.rows_b}")
        ratio = max(d.only_in_a, d.only_in_b) / max(d.rows_a, 1)
        if d.only_in_a or d.only_in_b:
            d.reasons.append(f"{d.only_in_a} rows only in classic output, {d.only_in_b} only in serverless output")
            d.sample = sql(f"SELECT 'classic_only' AS side, * FROM (SELECT {sel} FROM {q(a)} EXCEPT ALL SELECT {sel} FROM {q(b)}) LIMIT {sample_rows}")
            d.stat_diffs = numeric_stat_diffs(sql, a, b, common, rel_tol)
        ok_rows = d.rows_a == d.rows_b and ratio <= max_diff_ratio
        d.status = "PASS" if (d.schema_ok and ok_rows) else "FAIL"
    except Exception as e:  # noqa: BLE001 - surface any engine error as a validation ERROR, never a silent pass
        d.status, d.reasons = "ERROR", [f"{type(e).__name__}: {str(e)[:300]}"]
    return d


def numeric_stat_diffs(sql: SqlExec, a: str, b: str, cols: List[Tuple[str, str]], rel_tol: float) -> List[Dict[str, Any]]:
    nums = [n for n, t in cols if t.startswith(NUMERIC_TYPES)]
    if not nums:
        return []
    def agg(tbl):
        parts = ", ".join(f"sum({q(c)}) AS s_{i}, min({q(c)}) AS mn_{i}, max({q(c)}) AS mx_{i}, count({q(c)}) AS nn_{i}" for i, c in enumerate(nums))
        return sql(f"SELECT {parts} FROM {q(tbl)}")[0]
    ra, rb = agg(a), agg(b)
    diffs = []
    for i, c in enumerate(nums):
        for stat, key in (("sum", f"s_{i}"), ("min", f"mn_{i}"), ("max", f"mx_{i}"), ("non_null", f"nn_{i}")):
            va, vb = ra.get(key), rb.get(key)
            if va is None and vb is None:
                continue
            try:
                same = abs(float(va) - float(vb)) <= rel_tol * max(abs(float(va)), abs(float(vb)), 1.0)
            except (TypeError, ValueError):
                same = va == vb
            if not same:
                diffs.append({"column": c, "stat": stat, "classic": str(va), "serverless": str(vb)})
    return diffs


def overall_verdict(diffs: List[TableDiff]) -> str:
    if not diffs:
        return "NO_OUTPUTS"
    if any(d.status == "ERROR" for d in diffs):
        return "ERROR"
    return "PASS" if all(d.status == "PASS" for d in diffs) else "FAIL"


def perf_summary(classic_s: Optional[float], serverless_s: Optional[float]) -> Dict[str, Any]:
    if not classic_s or not serverless_s:
        return {}
    return {"classic_seconds": round(classic_s, 1), "serverless_seconds": round(serverless_s, 1),
            "speedup": round(classic_s / serverless_s, 2)}
