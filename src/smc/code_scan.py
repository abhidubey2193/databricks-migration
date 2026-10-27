"""Static code scanner + automated rewriter for notebooks / python files.

Covers: L6 (cache/checkpoint/global temp/mount APIs/RDD), L7 (Scala/R classification),
L13 (streaming default triggers, ANSI-risky SQL) and the mount-path part of L11.
Pure stdlib: runs offline and in unit tests.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .models import Finding
from .rules import RULES, ALLOWED_SPARK_CONFS

HEADER_RE = re.compile(r"^(#|--|//)\s*Databricks notebook source\s*$")
SEP_RE = re.compile(r"^(#|--|//)\s*COMMAND\s*-+\s*$")
MAGIC_RE = re.compile(r"^(#|--|//)\s*MAGIC ?(.*)$")
LANG_RE = re.compile(r"^%(\w+)\b")


def mk(rule_id: str, location: str = "", snippet: str = "", message: str = "",
       auto_fixable: Optional[bool] = None) -> Finding:
    lim, sev, auto, keep, msg, rem = RULES[rule_id]
    return Finding(rule_id=rule_id, limitation=lim, severity=sev,
                   auto_fixable=auto if auto_fixable is None else auto_fixable,
                   keep_on_classic=keep, message=message or msg, location=location,
                   remediation=rem, snippet=snippet.strip()[:200])


# --------------------------------------------------------------------------- notebook parsing
@dataclass
class Cell:
    lang: str            # python | sql | scala | r | md | sh | pip | run
    code: str
    start_line: int      # 1-based line of the first line of the cell in the file
    magic: bool = False  # True if the cell was written with MAGIC prefixes


def comment_prefix(lang: str) -> str:
    return {"sql": "--", "scala": "//"}.get(lang, "#")


def parse_notebook(text: str, default_lang: str = "python") -> Tuple[List[Cell], str]:
    """Split Databricks source-format notebook text into cells. Returns (cells, comment_prefix)."""
    default_lang = (default_lang or "python").lower()
    lines = text.splitlines()
    prefix = comment_prefix(default_lang)
    if lines and HEADER_RE.match(lines[0]):
        prefix = HEADER_RE.match(lines[0]).group(1)
        lines_iter_start = 1
    else:
        lines_iter_start = 0
    raw_cells: List[Tuple[int, List[str]]] = []
    cur: List[str] = []
    start = lines_iter_start + 1
    for i in range(lines_iter_start, len(lines)):
        if SEP_RE.match(lines[i]):
            raw_cells.append((start, cur))
            cur, start = [], i + 2
        else:
            cur.append(lines[i])
    raw_cells.append((start, cur))
    cells: List[Cell] = []
    for st, ls in raw_cells:
        nonempty = [l for l in ls if l.strip()]
        if not nonempty:
            continue
        first = MAGIC_RE.match(nonempty[0])
        if first and LANG_RE.match(first.group(2).strip()):
            lang = LANG_RE.match(first.group(2).strip()).group(1).lower()
            body = []
            for l in ls:
                m = MAGIC_RE.match(l)
                body.append(m.group(2) if m else l)
            # drop the %lang token itself for sql/scala/r/python; keep rest of first line
            joined = "\n".join(body)
            joined = re.sub(r"^\s*%" + lang + r"[ \t]*", "", joined, count=1)
            cells.append(Cell(lang, joined, st, magic=True))
        else:
            cells.append(Cell(default_lang, "\n".join(ls), st))
    return cells, prefix


def render_notebook(cells: List[Cell], prefix: str, default_lang: str) -> str:
    out = [f"{prefix} Databricks notebook source"]
    for n, c in enumerate(cells):
        if n:
            out.append(f"{prefix} COMMAND ----------")
        if c.magic:
            body = c.code.split("\n")
            out.append(f"{prefix} MAGIC %{c.lang} {body[0]}".rstrip())
            out.extend(f"{prefix} MAGIC {b}".rstrip() for b in body[1:])
        else:
            out.append(c.code)
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------- helpers
def _strip_comments_py(line: str) -> str:
    s = line.lstrip()
    return "" if s.startswith("#") else line


def _balanced_end(text: str, open_idx: int) -> int:
    """Index just past the ')' matching text[open_idx] == '('; -1 if unbalanced. Skips simple strings."""
    depth, i, n = 0, open_idx, len(text)
    while i < n:
        ch = text[i]
        if ch in "\"'":
            q = ch * 3 if text.startswith(ch * 3, i) else ch
            j = text.find(q, i + len(q))
            if j == -1:
                return -1
            i = j + len(q)
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return -1


def _line_of(text: str, idx: int) -> int:
    return text.count("\n", 0, idx) + 1


def _string_literals(code: str) -> List[Tuple[int, str]]:
    """(offset, content) of string literals that look like SQL."""
    out = []
    for m in re.finditer(r'("""|\'\'\')(.*?)\1|"([^"\n]*)"|\'([^\'\n]*)\'', code, re.S):
        body = m.group(2) if m.group(2) is not None else (m.group(3) or m.group(4) or "")
        if re.search(r"(?i)\b(select|insert\s+into|merge\s+into|create\s+(or\s+replace\s+)?(table|view))\b", body):
            out.append((m.start(), body))
    return out


def _sql_clean(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    sql = re.sub(r"--[^\n]*", " ", sql)
    return re.sub(r"'[^']*'", "''", sql)


# --------------------------------------------------------------------------- detection
def scan_sql_text(sql: str, loc: str, base_line: int = 0) -> List[Finding]:
    """ANSI-risk + cache statements in SQL text."""
    f: List[Finding] = []
    raw = sql
    clean = _sql_clean(sql)
    def ln(idx): return base_line + _line_of(clean, idx) - 1
    for m in re.finditer(r"(?<!\w)CAST\s*\(", clean, re.I):
        f.append(mk("L13.ANSI_CAST", f"{loc}:{ln(m.start())}", clean[m.start():m.start() + 60]))
    for m in re.finditer(r"::\s*(int|integer|bigint|smallint|tinyint|double|float|decimal|date|timestamp|boolean)\b", clean, re.I):
        f.append(mk("L13.ANSI_CAST", f"{loc}:{ln(m.start())}", clean[max(0, m.start() - 20):m.end()],
                    message="`::type` cast raises on invalid input under ANSI mode.", auto_fixable=False))
    for m in re.finditer(r"[\w\)\]`]\s*/\s*[\w\(`]", clean):
        f.append(mk("L13.ANSI_DIVIDE", f"{loc}:{ln(m.start())}", clean[max(0, m.start() - 15):m.end() + 15]))
    for m in re.finditer(r"\belement_at\s*\(", clean, re.I):
        f.append(mk("L13.ANSI_ELEMENT", f"{loc}:{ln(m.start())}", clean[m.start():m.start() + 50]))
    for m in re.finditer(r"(?<![\w])(to_date|to_timestamp)\s*\(", clean, re.I):
        f.append(mk("L13.ANSI_DATE", f"{loc}:{ln(m.start())}", clean[m.start():m.start() + 60]))
    for m in re.finditer(r"(?<![\w])(sum|avg)\s*\(", clean, re.I):
        f.append(mk("L13.ANSI_SUM", f"{loc}:{ln(m.start())}", clean[m.start():m.start() + 40]))
    for m in re.finditer(r"(?im)^\s*(CACHE\s+(LAZY\s+)?TABLE|UNCACHE\s+TABLE|CLEAR\s+CACHE)\b.*$", raw):
        f.append(mk("L6.CACHE_TABLE", f"{loc}:{base_line + _line_of(raw, m.start()) - 1}", m.group(0)))
    for m in re.finditer(r"(?i)CREATE\s+(OR\s+REPLACE\s+)?GLOBAL\s+TEMP(ORARY)?\s+VIEW|\bglobal_temp\.", raw):
        f.append(mk("L6.GLOBAL_TEMP_VIEW", f"{loc}:{base_line + _line_of(raw, m.start()) - 1}", m.group(0)))
    return f


def _scan_streaming(code: str, loc: str, base_line: int) -> List[Finding]:
    out = []
    for m in re.finditer(r"\.writeStream\b", code):
        seg = code[m.end(): m.end() + 4000]
        end = re.search(r"\.(start|toTable|table)\s*\(", seg)
        seg = seg[: end.end()] if end else seg[:1500]
        t = re.search(r"\.trigger\s*\(([^)]*)\)", seg)
        line = f"{loc}:{base_line + _line_of(code, m.start()) - 1}"
        if not t:
            out.append(mk("L13.STREAM_DEFAULT_TRIGGER", line, "writeStream without .trigger()"))
        elif "availableNow" not in t.group(1):
            out.append(mk("L13.STREAM_DEFAULT_TRIGGER", line, t.group(0),
                          message="Trigger " + t.group(1).strip() + " is not supported on serverless (only availableNow)."))
    return out


def scan_python(code: str, loc: str, base_line: int = 1, mount_map: Optional[Dict[str, str]] = None) -> List[Finding]:
    f: List[Finding] = []
    lines = code.split("\n")
    for i, line in enumerate(lines):
        if not _strip_comments_py(line).strip():
            continue
        where = f"{loc}:{base_line + i}"
        if re.search(r"\.(cache|persist|unpersist)\s*\(", line):
            f.append(mk("L6.CACHE", where, line))
        if re.search(r"spark\.catalog\.(cacheTable|uncacheTable|clearCache)\s*\(", line):
            f.append(mk("L6.CACHE_TABLE", where, line))
        if re.search(r"\.(localCheckpoint|checkpoint)\s*\(", line):
            f.append(mk("L6.CHECKPOINT", where, line))
        if re.search(r"createGlobalTempView|createOrReplaceGlobalTempView|\bglobal_temp\.", line):
            f.append(mk("L6.GLOBAL_TEMP_VIEW", where, line))
        if re.search(r"dbutils\.fs\.(mount|unmount|updateMount|refreshMounts|mounts)\s*\(", line):
            f.append(mk("L6.MOUNT_API", where, line))
        elif re.search(r"""["'](dbfs:)?/(dbfs/)?mnt/""", line):
            f.append(mk("L6.MOUNT_PATH", where, line, auto_fixable=bool(mount_map)))
        if re.search(r"\.rdd\b|\bsparkContext\b|\bsc\.(parallelize|textFile|broadcast|accumulator|wholeTextFiles)", line):
            f.append(mk("L6.RDD", where, line))
        for m in re.finditer(r"spark\.conf\.set\(\s*[\"']([^\"']+)[\"']", line):
            if m.group(1) not in ALLOWED_SPARK_CONFS:
                f.append(mk("L6.SPARK_CONF", where, line, message=f"spark.conf.set('{m.group(1)}') is not allowed on serverless."))
        if re.search(r"\.cast\(\s*[\"']", line):
            f.append(mk("L13.ANSI_CAST", where, line, message=".cast() raises on invalid input under ANSI mode.",
                        auto_fixable=False))
    f.extend(_scan_streaming(code, loc, base_line))
    for off, body in _string_literals(code):
        f.extend(scan_sql_text(body, loc, base_line + _line_of(code, off) - 1))
    return f


# --------------------------------------------------------------------------- L7 classification
HARD_SCALA = [r"case\s+class", r"Dataset\[", r"RDD\[", r"\bimplicit\b", r"\btrait\b", r"\bextends\b",
              r"org\.apache\.spark\.ml", r"\budf\b|\budaf\b", r"akka|breeze|JavaConverters", r"\.map\s*\{|\.flatMap\s*\{|\.mapPartitions"]
DF_SCALA = [r"spark\.read", r"\.select\(", r"\.(filter|where)\(", r"\.groupBy\(", r"\.join\(", r"\.write", r"spark\.sql\("]


def classify_scala(code: str) -> Dict[str, object]:
    hard = [p for p in HARD_SCALA if re.search(p, code)]
    df = [p for p in DF_SCALA if re.search(p, code)]
    sql_only = bool(re.search(r"spark\.sql\(", code)) and not re.search(r"\.(select|filter|where|groupBy|join)\(|spark\.read", code)
    if hard:
        decision, cx = "KEEP_ON_CLASSIC", "HIGH"
        reason = "uses JVM-only constructs: " + ", ".join(h.replace("\\", "") for h in hard[:4])
    elif sql_only:
        decision, cx, reason = "REWRITE_SQL", "LOW", "only spark.sql(...) calls - move SQL into a SQL cell"
    elif df:
        decision, cx, reason = "REWRITE_PYSPARK", "MEDIUM" if len(code.splitlines()) > 60 else "LOW", "DataFrame-API only - mechanical PySpark port"
    else:
        decision, cx, reason = "KEEP_ON_CLASSIC", "MEDIUM", "no recognisable DataFrame/SQL usage - needs human review"
    return {"decision": decision, "complexity": cx, "reason": reason,
            "lines": len(code.splitlines()),
            "draft_pyspark": scala_to_pyspark_draft(code) if decision == "REWRITE_PYSPARK" else ""}


def scala_to_pyspark_draft(code: str) -> str:
    """Very small mechanical Scala->PySpark translator for DataFrame-only code. A DRAFT: always review."""
    out = ["# SMCT DRAFT translation - review before use", "from pyspark.sql import functions as F"]
    src = []
    for l in code.split("\n"):
        l = re.sub(r"^\s*import .*$", "", l)
        l = re.sub(r"//", "#", l)
        l = re.sub(r"\b(val|var)\s+", "", l)
        l = re.sub(r"\$\"([^\"]+)\"", r'F.col("\1")', l)
        l = l.replace("===", "==").replace("=!=", "!=").replace("&&", "&").replace("||", "|")
        l = re.sub(r"\.as\(", ".alias(", l)
        l = re.sub(r"(?<![\w.])(col|sum|avg|count|max|min|lit|when|coalesce)\(", r"F.\1(", l)
        src.append(l.rstrip())
    i = 0
    while i < len(src):
        j = i
        while j + 1 < len(src) and src[j + 1].lstrip().startswith("."):
            j += 1
        if j > i and "=" in src[i]:
            name, rhs = src[i].split("=", 1)
            out.append(f"{name.rstrip()} = ({rhs.strip()}")
            out.extend("    " + s.strip() for s in src[i + 1:j + 1])
            out[-1] += ")"
        elif j > i:
            out.append("(" + src[i].strip())
            out.extend("    " + s.strip() for s in src[i + 1:j + 1])
            out[-1] += ")"
        else:
            if src[i].strip():
                out.append(src[i])
        i = j + 1
    return "\n".join(out)


# --------------------------------------------------------------------------- notebook-level scan
def scan_notebook(path: str, text: str, language: str = "python",
                  mount_map: Optional[Dict[str, str]] = None) -> Tuple[List[Finding], Dict[str, object]]:
    """Returns (findings, extras) where extras carries L7 classification and %run dependencies."""
    findings: List[Finding] = []
    extras: Dict[str, object] = {"scala": [], "includes": []}
    language = (language or "python").lower()
    if language in ("scala", "r") and not text.strip().startswith(("# Databricks", "-- Databricks", "// Databricks")):
        cells, prefix = [Cell(language, text, 1)], comment_prefix(language)
    else:
        cells, prefix = parse_notebook(text, language)
    for c in cells:
        loc = path
        if c.lang == "python":
            findings.extend(scan_python(c.code, loc, c.start_line, mount_map))
        elif c.lang == "sql":
            findings.extend(scan_sql_text(c.code, loc, c.start_line))
        elif c.lang == "scala":
            findings.append(mk("L7.SCALA", f"{loc}:{c.start_line}", c.code[:80]))
            extras["scala"].append(classify_scala(c.code))
        elif c.lang == "r":
            findings.append(mk("L7.R", f"{loc}:{c.start_line}", c.code[:80]))
        elif c.lang == "run":
            extras["includes"].append(c.code.strip().split()[0] if c.code.strip() else "")
        elif c.lang == "sh":
            if re.search(r"\b(apt|yum|apt-get|wget|curl)\b", c.code):
                findings.append(mk("L3.INIT_UNSUPPORTED_LINE", f"{loc}:{c.start_line}", c.code[:80],
                                   message="%sh cell runs OS-level commands unsupported on serverless."))
    return findings, extras


# --------------------------------------------------------------------------- rewriting
def _replace_statement_calls(code: str, pattern: str, repl) -> Tuple[str, int]:
    """Replace whole call statements matching `pattern(`...`)` (balanced, possibly multi-line)."""
    n, pos = 0, 0
    rx = re.compile(r"(?m)^([ \t]*)(" + pattern + r")\s*\(")
    while True:
        m = rx.search(code, pos)
        if not m:
            return code, n
        end = _balanced_end(code, m.end() - 1)
        if end == -1:
            pos = m.end()
            continue
        original = code[m.start(2):end]
        new = repl(m.group(1), original)
        code = code[:m.start()] + new + code[end:]
        pos = m.start() + len(new)
        n += 1


def _strip_method(code: str, names: str) -> Tuple[str, int]:
    n, rx = 0, re.compile(r"\.(" + names + r")\s*\(")
    while True:
        m = rx.search(code)
        if not m:
            return code, n
        end = _balanced_end(code, m.end() - 1)
        if end == -1:
            return code, n
        code = code[:m.start()] + code[end:]
        n += 1


def rewrite_python(code: str, salt: str = "", mount_map: Optional[Dict[str, str]] = None,
                   ckpt_schema: str = "main.smct_tmp", cast: bool = True) -> Tuple[str, List[str]]:
    applied: List[str] = []
    # checkpoint -> materialise to Delta (before .cache stripping so names stay intact)
    def ckpt_sub(m):
        ind, var, src = m.group(1), m.group(2), m.group(3)
        tbl = f"{ckpt_schema}.ckpt_{var}_{hashlib.md5((salt + m.group(0)).encode()).hexdigest()[:6]}"
        applied.append("L6.CHECKPOINT")
        return (f'{ind}{src}.write.mode("overwrite").saveAsTable("{tbl}")  # SMCT: checkpoint -> Delta\n'
                f'{ind}{var} = spark.table("{tbl}")')
    code = re.sub(r"(?m)^([ \t]*)(\w+)\s*=\s*([\w\.]+)\.(?:local)?[cC]heckpoint\([^)\n]*\)[ \t]*$", ckpt_sub, code)
    code, n = _replace_statement_calls(code, r"[\w\.]+\.(?:local)?[cC]heckpoint",
                                       lambda ind, o: f"{ind}pass  # SMCT removed: {o[:60]}")
    if n:
        applied.append("L6.CHECKPOINT")
    code, n = _strip_method(code, "cache|persist|unpersist")
    if n:
        applied.append("L6.CACHE")
        code = re.sub(r"(?m)^([ \t]*)[\w\.]+[ \t]*$", lambda m: f"{m.group(1)}pass  # SMCT removed cache/persist", code)
        code = re.sub(r"(?m)^([ \t]*)(\w+)[ \t]*=[ \t]*\2[ \t]*$", lambda m: f"{m.group(1)}pass  # SMCT removed cache/persist", code)
    code, n = _replace_statement_calls(code, r"spark\.catalog\.(?:cacheTable|uncacheTable|clearCache)",
                                       lambda ind, o: f"{ind}pass  # SMCT removed: {o[:60]}")
    if n:
        applied.append("L6.CACHE_TABLE")
    code, n = _replace_statement_calls(code, r"dbutils\.fs\.(?:mount|unmount|updateMount|refreshMounts)",
                                       lambda ind, o: f"{ind}pass  # SMCT removed mount call (use UC Volumes): {o.splitlines()[0][:50]}")
    if n:
        applied.append("L6.MOUNT_API")
    if re.search(r"createGlobalTempView|createOrReplaceGlobalTempView|global_temp\.", code):
        code = re.sub(r"createOrReplaceGlobalTempView|createGlobalTempView", "createOrReplaceTempView", code)
        code = re.sub(r"\bglobal_temp\.", "", code)
        applied.append("L6.GLOBAL_TEMP_VIEW")
    if mount_map:
        for src in sorted(mount_map, key=len, reverse=True):
            dst = mount_map[src]
            before = code
            for pre in ("dbfs:" + src, "/dbfs" + src, src):
                code = code.replace(pre, dst)
            if code != before and "L6.MOUNT_PATH" not in applied:
                applied.append("L6.MOUNT_PATH")
    # streaming triggers
    def stream_fix(code):
        out, pos, changed = [], 0, False
        for m in re.finditer(r"\.writeStream\b", code):
            seg_start = m.end()
            seg = code[seg_start: seg_start + 4000]
            end = re.search(r"\.(start|toTable|table)\s*\(", seg)
            seg = seg[: end.end()] if end else seg[:1500]
            t = re.search(r"\.trigger\s*\(([^)]*)\)", seg)
            if not t:
                out.append(code[pos:seg_start] + ".trigger(availableNow=True)")
                pos, changed = seg_start, True
            elif "availableNow" not in t.group(1):
                a, b = seg_start + t.start(), seg_start + t.end()
                out.append(code[pos:a] + ".trigger(availableNow=True)")
                pos, changed = b, True
        out.append(code[pos:])
        return "".join(out), changed
    code, ch = stream_fix(code)
    if ch:
        applied.append("L13.STREAM_DEFAULT_TRIGGER")
    code, sql_applied = rewrite_sql(code, cast=cast)
    applied.extend(sql_applied)
    return code, sorted(set(applied))


def rewrite_sql(code: str, cast: bool = True) -> Tuple[str, List[str]]:
    applied: List[str] = []
    new = re.sub(r"(?<![\w.])CAST\s*\(", "try_cast(", code, flags=re.I) if cast else code
    if new != code:
        applied.append("L13.ANSI_CAST")
        code = new
    new = re.sub(r"(?i)CREATE\s+(OR\s+REPLACE\s+)?GLOBAL\s+TEMP(ORARY)?\s+VIEW", "CREATE OR REPLACE TEMPORARY VIEW", code)
    new = re.sub(r"(?i)\bglobal_temp\.", "", new)
    if new != code:
        applied.append("L6.GLOBAL_TEMP_VIEW")
        code = new
    new = re.sub(r"(?im)^(\s*)CACHE\s+(?:LAZY\s+)?TABLE\s+([\w\.`]+)\s+(?:OPTIONS\s*\([^)]*\)\s*)?AS\s+",
                 r"\1CREATE OR REPLACE TEMPORARY VIEW \2 AS ", code)
    new = re.sub(r"(?im)^(\s*)(CACHE\s+(?:LAZY\s+)?TABLE\b.*|UNCACHE\s+TABLE\b.*|CLEAR\s+CACHE\b.*)$", r"\1-- SMCT removed: \2", new)
    if new != code:
        applied.append("L6.CACHE_TABLE")
        code = new
    return code, applied


def rewrite_notebook(path: str, text: str, language: str = "python", mount_map: Optional[Dict[str, str]] = None,
                     ckpt_schema: str = "main.smct_tmp", ansi: str = "try") -> Tuple[str, List[str]]:
    """Apply all automated rewrites to a notebook. ansi='disable' prepends spark.sql.ansi.enabled=false instead."""
    language = (language or "python").lower()
    if language in ("scala", "r"):
        return text, []
    cells, prefix = parse_notebook(text, language)
    applied: List[str] = []
    ansi_found = False
    for c in cells:
        if c.lang == "python":
            if ansi == "disable":
                ansi_found |= any(f.limitation == "L13" and f.rule_id.startswith("L13.ANSI") for f in scan_python(c.code, path))
            c.code, a = rewrite_python(c.code, salt=path, mount_map=mount_map, ckpt_schema=ckpt_schema, cast=ansi != "disable")
            applied.extend(a)
        elif c.lang == "sql":
            if ansi == "disable":
                ansi_found |= any(f.rule_id.startswith("L13.ANSI") for f in scan_sql_text(c.code, path))
            c.code, a = rewrite_sql(c.code, cast=ansi != "disable")
            applied.extend(a)
    if ansi == "disable" and ansi_found:
        pre = 'spark.conf.set("spark.sql.ansi.enabled", "false")  # SMCT: keep classic (non-ANSI) semantics'
        first_py = next((c for c in cells if c.lang == "python" and not c.magic), None)
        if first_py:
            first_py.code = pre + "\n" + first_py.code
        else:
            cells.insert(0, Cell("python", pre, 1, magic=language != "python"))
        applied.append("L13.ANSI_DISABLED")
    return render_notebook(cells, prefix, language), sorted(set(applied))


# --------------------------------------------------------------------------- output-table detection (for A/B validation)
_TBL = r"[`\w]+(?:\.[`\w]+){0,2}"


def detect_output_tables(code: str) -> List[str]:
    """Best-effort list of tables a piece of code writes to (used to drive A/B validation)."""
    out: List[str] = []
    for rx in (r"saveAsTable\(\s*f?[\"'](" + _TBL + r")[\"']", r"insertInto\(\s*f?[\"'](" + _TBL + r")[\"']",
               r"\.toTable\(\s*f?[\"'](" + _TBL + r")[\"']",
               r"(?i)\bINSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?(" + _TBL + r")",
               r"(?i)\bMERGE\s+INTO\s+(" + _TBL + r")",
               r"(?i)\bCREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(" + _TBL + r")",
               r"(?i)\bDeltaTable\.forName\(\s*spark\s*,\s*[\"'](" + _TBL + r")[\"']"):
        for m in re.finditer(rx, code):
            t = m.group(1).replace("`", "")
            if "{" not in t and t not in out:
                out.append(t)
    return out
