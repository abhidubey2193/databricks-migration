import json, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "fleet.json")


def load_fleet():
    d = json.load(open(FIX))
    nbs, inits = d["notebooks"], d["init_scripts"]
    nb = lambda p: (nbs[p]["source"], nbs[p]["language"]) if p in nbs else None  # noqa: E731
    return d, nb, (lambda dest: inits.get(dest))


class FakeStore:
    """In-memory stand-in for smc.store.Store (same interface)."""
    def __init__(self):
        self.events, self.tables = [], {}
    def ensure(self): pass
    def create_views(self, *a, **k): pass
    def monthly_dbus(self): return {}
    def replace(self, t, rows): self.tables[t] = list(rows)
    def append(self, t, rows): self.tables.setdefault(t, []).extend(rows)
    def add_event(self, wid, event, actor="smct", detail=None):
        self.events.append({"workload_id": wid, "event": event, "actor": actor, "detail": detail or {}, "n": len(self.events)})
    def latest_events(self):
        out = {}
        for e in self.events: out[e["workload_id"]] = e
        return out
    def last_event(self, wid, event):
        m = [e for e in self.events if e["workload_id"] == wid and e["event"] == event]
        return {"detail": m[-1]["detail"], "actor": m[-1]["actor"]} if m else None
    def ids_in_status(self, s): return [w for w, e in self.latest_events().items() if e["event"] == s]


class FakeWorkspace:
    """In-memory Workspace: jobs, notebooks, files."""
    def __init__(self, jobs, notebooks, init_scripts=None):
        self.jobs = {j["job_id"]: json.loads(json.dumps(j)) for j in jobs}
        self.nbs = {k: (v["source"], v["language"]) for k, v in notebooks.items()}
        self.files, self.next_id, self.runs = {}, 1000, []
        self.inits = init_scripts or {}
    def list_jobs(self): return list(self.jobs.values())
    def get_job(self, i): return json.loads(json.dumps(self.jobs[i]))
    def all_purpose_clusters(self): return {}
    def fetch_notebook(self, p): return self.nbs.get(p)
    def fetch_init_script(self, d): return self.inits.get(d)
    def import_notebook(self, path, text, lang): self.nbs[path] = (text, lang)
    def write_text(self, path, text): self.files[path] = text
    def read_text(self, path): return self.files[path]
    def create_job(self, s):
        self.next_id += 1; self.jobs[self.next_id] = {"job_id": self.next_id, "settings": json.loads(json.dumps(s))}; return self.next_id
    def delete_job(self, i): self.jobs.pop(i, None)
    def reset_job(self, i, s): self.jobs[i]["settings"] = json.loads(json.dumps(s))
    def update_job(self, i, partial):
        for k, v in partial.items(): self.jobs[i]["settings"][k] = v


import re
from collections import Counter


class FakeEngine:
    """Tiny SQL engine covering exactly the statements smc.ab_diff / pipeline.validate emit."""
    def __init__(self):
        self.tables = {}                      # name -> (cols [(name,type)], rows [tuple])
        self.log = []

    def put(self, name, cols, rows): self.tables[name] = (cols, [tuple(r) for r in rows])

    @staticmethod
    def _n(s): return s.replace("`", "")

    def run(self, q):
        self.log.append(q)
        q1 = " ".join(q.split())
        m = re.match(r"DROP SCHEMA IF EXISTS (\S+) CASCADE", q1)
        if m:
            for t in [t for t in self.tables if t.startswith(m.group(1) + ".")]: del self.tables[t]
            return []
        if q1.startswith("CREATE SCHEMA"): return []
        m = re.match(r"SHOW TABLES IN (\S+)", q1)
        if m: return [{"tableName": t.split(".")[-1]} for t in self.tables if t.rsplit(".", 1)[0] == m.group(1)]
        m = re.match(r"DESCRIBE TABLE (\S+)", q1)
        if m: return [{"col_name": c, "data_type": t} for c, t in self.tables[self._n(m.group(1))][0]]
        m = re.match(r"SELECT count\(\*\) AS n FROM (`[^ ]+)$", q1)
        if m: return [{"n": len(self.tables[self._n(m.group(1))][1])}]
        m = re.match(r"SELECT count\(\*\) AS n FROM \(SELECT (.*?) FROM (`[^ ]+) EXCEPT ALL SELECT .* FROM (`[^ )]+)\)", q1)
        if m:
            keep = set(re.findall(r"`([^`]+)`", m.group(1)))          # honour the projection the code under test built
            a, b = self._n(m.group(2)), self._n(m.group(3))
            ra = Counter(self._proj(a, keep)); rb = Counter(self._proj(b, keep))
            return [{"n": sum((ra - rb).values())}]
        if q1.startswith("SELECT 'classic_only'"): return [{"side": "classic_only"}]
        m = re.match(r"SELECT (sum\(.*) FROM (`[^ ]+)$", q1)
        if m:
            cols, rows = self.tables[self._n(m.group(2))]
            names = re.findall(r"sum\(`([^`]+)`\) AS s_(\d+)", q1)
            out = {}
            for c, i in names:
                idx = [x[0] for x in cols].index(c); vals = [r[idx] for r in rows if r[idx] is not None]
                out[f"s_{i}"] = sum(vals) if vals else None; out[f"mn_{i}"] = min(vals) if vals else None
                out[f"mx_{i}"] = max(vals) if vals else None; out[f"nn_{i}"] = len(vals)
            return [out]
        raise NotImplementedError(q1[:120])

    def _proj(self, name, keep=None):
        cols, rows = self.tables[name]
        idx = [i for i, (c, _) in enumerate(cols) if keep is None or c in keep]
        return [tuple(round(r[i], 6) if isinstance(r[i], float) else r[i] for i in idx) for r in rows]


class FakeSpark:
    def __init__(self, engine): self.e = engine
    def sql(self, q):
        rows = self.e.run(q)
        class R:
            def __init__(s, d): s.d = d
            def asDict(s): return s.d
        class DF:
            def collect(s): return [R(r) for r in rows]
        return DF()
