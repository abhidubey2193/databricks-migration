"""Offline CLI - works on JSON fixtures or exported job JSON, no workspace needed.

  python -m smc.cli scan --fixtures tests/fixtures/fleet.json
  python -m smc.cli rewrite --file my_notebook.py [--lang python] [--mount-map '{"/mnt/raw":"/Volumes/c/s/raw"}']
  python -m smc.cli transform --fixtures tests/fixtures/fleet.json --job 12 [--batch-size 10]
  python -m smc.cli guard --fixtures tests/fixtures/fleet.json
  python -m smc.cli coverage
"""
from __future__ import annotations

import argparse
import json
import sys

from . import drift, guard
from .code_scan import rewrite_notebook, scan_notebook
from .config_scan import scan_job
from .rules import RULES
from .scoring import plan_waves, summarize
from .transform import batch_plan, to_serverless


def _load(path):
    d = json.load(open(path))
    nbs, inits = d.get("notebooks", {}), d.get("init_scripts", {})
    nb = lambda p: (nbs[p]["source"], nbs[p]["language"]) if p in nbs else None  # noqa: E731
    return d, nb, (lambda dest: inits.get(dest))


def cmd_scan(a):
    d, nb, init = _load(a.fixtures)
    mm = json.loads(a.mount_map) if a.mount_map else None
    reps = [scan_job(j, d.get("clusters", {}), init, nb, mm) for j in d["jobs"]]
    rows = plan_waves(reps)
    print(f"{'rank':>4} {'wave':>4} {'score':>5} {'verdict':<18} {'tasks':>5}  {'limitations':<22} name")
    for r in rows:
        print(f"{r['rank']:>4} {r['wave']:>4} {r['score']:>5} {r['verdict']:<18} {r['task_count']:>5}  {r['limitations']:<22} {r['name']}")
    print("\nsummary:", json.dumps(summarize(rows)))
    if a.detail:
        for r in reps:
            if str(r.workload_id).endswith(f":{a.detail}"):
                for f in r.findings:
                    print(f"  [{f.severity}] {f.rule_id} {f.location}: {f.message}")


def cmd_rewrite(a):
    text = open(a.file).read()
    mm = json.loads(a.mount_map) if a.mount_map else None
    f, extras = scan_notebook(a.file, text, a.lang, mm)
    new, applied = rewrite_notebook(a.file, text, a.lang, mm)
    print(f"# findings: {len(f)}; auto-applied rules: {applied}", file=sys.stderr)
    print(drift.code_patch(a.file, text, new) or "# no changes")


def cmd_transform(a):
    d, nb, init = _load(a.fixtures)
    j = next(x for x in d["jobs"] if str(x["job_id"]) == str(a.job))
    keys = None
    if a.batch_size:
        keys = [k for b in batch_plan(j["settings"], a.batch_size)[: a.batch_upto or None] for k in b]
    res = to_serverless(j["settings"], origin_job_id=a.job, task_keys=keys, init_fetch=init)
    print(f"migrated={len(res.migrated_tasks)} left_on_classic={res.left_on_classic} notes={res.notes}")
    print(drift.bundle_yaml(j["settings"]["name"], res.settings))


def cmd_guard(a):
    d, _, _ = _load(a.fixtures)
    for v in guard.find_violations(d["jobs"]):
        print(f"VIOLATION job {v.job_id} '{v.name}': {v.reason}")


def cmd_coverage(a):
    by = {}
    for rid, (lim, sev, auto, keep, msg, rem) in RULES.items():
        by.setdefault(lim, []).append((rid, sev, "auto-fix" if auto else "detect+guide"))
    for lim in sorted(by, key=lambda x: int(x[1:])):
        print(lim, "; ".join(f"{r}[{s},{m}]" for r, s, m in by[lim]))


def main(argv=None):
    p = argparse.ArgumentParser(prog="smc")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("scan"); s.add_argument("--fixtures", required=True); s.add_argument("--detail"); s.add_argument("--mount-map", dest="mount_map"); s.set_defaults(f=cmd_scan)
    r = sub.add_parser("rewrite"); r.add_argument("--file", required=True); r.add_argument("--lang", default="python"); r.add_argument("--mount-map", dest="mount_map"); r.set_defaults(f=cmd_rewrite)
    t = sub.add_parser("transform"); t.add_argument("--fixtures", required=True); t.add_argument("--job", required=True)
    t.add_argument("--batch-size", type=int, default=0); t.add_argument("--batch-upto", type=int, default=0); t.set_defaults(f=cmd_transform)
    g = sub.add_parser("guard"); g.add_argument("--fixtures", required=True); g.set_defaults(f=cmd_guard)
    c = sub.add_parser("coverage"); c.set_defaults(f=cmd_coverage)
    a = p.parse_args(argv)
    a.f(a)


if __name__ == "__main__":
    main()
