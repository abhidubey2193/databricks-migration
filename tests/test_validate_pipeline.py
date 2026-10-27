import json, unittest
from tests import helpers
from tests.helpers import FakeEngine, FakeSpark, FakeStore, FakeWorkspace, load_fleet
from smc import ab_diff, pipeline
from smc.config import Config

COLS = [("id", "bigint"), ("amount", "double"), ("name", "string")]

class ABDiff(unittest.TestCase):
    def setUp(self):
        self.e = FakeEngine(); self.sql = lambda q: self.e.run(q)
        self.rows = [(1, 1.0000001, "a"), (2, 2.5, "b"), (3, 3.5, None)]
    def test_pass_identical_with_float_noise(self):
        self.e.put("c.s.a", COLS, self.rows); self.e.put("c.s.b", COLS, [(1, 1.00000012, "a"), (2, 2.5, "b"), (3, 3.5, None)])
        d = ab_diff.compare_tables(self.sql, "c.s.a", "c.s.b"); self.assertEqual(d.status, "PASS", d.reasons)
    def test_fail_value_difference_reports_stats(self):
        self.e.put("c.s.a", COLS, self.rows); self.e.put("c.s.b", COLS, [(1, 1.0, "a"), (2, 9.9, "b"), (3, 3.5, None)])
        d = ab_diff.compare_tables(self.sql, "c.s.a", "c.s.b")
        self.assertEqual(d.status, "FAIL"); self.assertEqual((d.only_in_a, d.only_in_b), (1, 1)); self.assertTrue(d.stat_diffs)
    def test_fail_row_count(self):
        self.e.put("c.s.a", COLS, self.rows); self.e.put("c.s.b", COLS, self.rows[:2])
        d = ab_diff.compare_tables(self.sql, "c.s.a", "c.s.b"); self.assertEqual(d.status, "FAIL"); self.assertIn("row count", " ".join(d.reasons))
    def test_fail_schema_type_change(self):
        self.e.put("c.s.a", COLS, self.rows); self.e.put("c.s.b", [("id", "int")] + COLS[1:], self.rows)
        d = ab_diff.compare_tables(self.sql, "c.s.a", "c.s.b"); self.assertFalse(d.schema_ok); self.assertEqual(d.status, "FAIL")
    def test_excluded_volatile_column(self):
        cols = COLS + [("load_ts", "string")]
        self.e.put("c.s.a", cols, [r + ("t1",) for r in self.rows]); self.e.put("c.s.b", cols, [r + ("t2",) for r in self.rows])
        self.assertEqual(ab_diff.compare_tables(self.sql, "c.s.a", "c.s.b", exclude=["load_ts"]).status, "PASS")
        self.assertEqual(ab_diff.compare_tables(self.sql, "c.s.a", "c.s.b").status, "FAIL")
    def test_engine_error_is_error_not_pass(self):
        d = ab_diff.compare_tables(self.sql, "c.s.missing", "c.s.b"); self.assertEqual(d.status, "ERROR")
        self.assertEqual(ab_diff.overall_verdict([d]), "ERROR"); self.assertEqual(ab_diff.overall_verdict([]), "NO_OUTPUTS")

class Lifecycle(unittest.TestCase):
    def setUp(self):
        d, _, _ = load_fleet()
        self.ws = FakeWorkspace(d["jobs"], d["notebooks"], d["init_scripts"]); self.store = FakeStore(); self.cfg = Config(catalog="c", schema="smct")
    def clone_of(self, wid):
        return int(self.store.last_event(wid, "CLONED")["detail"]["clone_job_id"])

    def test_discover_excludes_clones_and_persists(self):
        s = pipeline.discover(self.ws, self.store, self.cfg)
        names = {r["name"] for r in self.store.tables["workloads"]}
        self.assertNotIn("clean_etl [serverless]", names); self.assertEqual(s["TOTAL"], 21)
        self.assertTrue(self.store.tables["findings"]); self.assertEqual(len(self.store.tables["scan_history"]), 1)

    def test_migrate_autofix_job_end_to_end(self):
        orig_job, orig_nb = self.ws.get_job(2), self.ws.nbs["/demo/cache_heavy"]
        self.store.add_event("job:2", "APPROVED", "boss")
        pipeline.migrate(self.ws, self.store, self.cfg)
        self.assertEqual(self.store.latest_events()["job:2"]["event"], "CLONED")
        cl = self.ws.jobs[self.clone_of("job:2")]["settings"]
        self.assertNotIn("schedule", cl); self.assertTrue(cl["name"].endswith("[serverless]"))
        new_path = cl["tasks"][0]["notebook_task"]["notebook_path"]
        self.assertTrue(new_path.startswith("/Workspace/Shared/smct/migrated/2/"))
        self.assertNotIn(".cache(", self.ws.nbs[new_path][0])
        self.assertEqual(self.ws.get_job(2), orig_job); self.assertEqual(self.ws.nbs["/demo/cache_heavy"], orig_nb)   # originals untouched
        self.assertTrue(self.store.tables["code_patches"]); self.assertTrue(any(k.endswith(".json") for k in self.ws.files))

    def test_migrate_large_dag_and_partial(self):
        self.store.add_event("job:12", "APPROVED"); self.store.add_event("job:18", "APPROVED")
        pipeline.migrate(self.ws, self.store, self.cfg)
        self.assertEqual(len(self.store.last_event("job:12", "CLONED")["detail"]["migrated_tasks"]), 25)
        d = self.store.last_event("job:18", "CLONED")["detail"]
        self.assertEqual(d["migrated_tasks"], ["py"]); self.assertEqual(d["left_on_classic"][0][0], "jvm")

    def test_unmigratable_job_fails_cleanly(self):
        self.store.add_event("job:15", "APPROVED")      # docker image job - clean_etl notebook but custom image
        pipeline.migrate(self.ws, self.store, self.cfg)
        self.assertEqual(self.store.latest_events()["job:15"]["event"], "MIGRATE_FAILED")

    def _clone_and_validate(self, wid="job:1"):
        self.store.add_event(wid, "APPROVED"); pipeline.migrate(self.ws, self.store, self.cfg)
        self.store.add_event(wid, "VALIDATED", detail={"verdict": "PASS"})

    def test_promote_then_rollback_restores_exact_original(self):
        before = self.ws.get_job(2)["settings"]
        self.store.add_event("job:2", "APPROVED"); pipeline.migrate(self.ws, self.store, self.cfg)
        self.store.add_event("job:2", "VALIDATED"); self.store.add_event("job:2", "PROMOTE_APPROVED", "boss")
        pipeline.promote(self.ws, self.store, self.cfg)
        self.assertEqual(self.store.latest_events()["job:2"]["event"], "PROMOTED")
        after = self.ws.get_job(2)["settings"]
        self.assertEqual(after["name"], "cache_heavy"); self.assertEqual(after["schedule"], before["schedule"])
        self.assertNotIn("job_clusters", after); self.assertTrue(after["tasks"][0]["environment_key"])
        self.assertEqual(self.ws.get_job(self.clone_of("job:2"))["settings"].get("schedule"), None)
        self.assertTrue(self.store.last_event("job:2", "PROMOTED")["detail"]["bundle_yaml"] in self.ws.files)
        self.store.add_event("job:2", "ROLLBACK_REQUESTED", "boss"); pipeline.rollback(self.ws, self.store, self.cfg)
        self.assertEqual(self.ws.get_job(2)["settings"], before); self.assertEqual(self.store.latest_events()["job:2"]["event"], "ROLLED_BACK")

    def test_promote_refused_without_validation(self):
        self.store.add_event("job:2", "APPROVED"); pipeline.migrate(self.ws, self.store, self.cfg)
        self.store.add_event("job:2", "PROMOTE_APPROVED")
        before = self.ws.get_job(2)["settings"]; pipeline.promote(self.ws, self.store, self.cfg)
        self.assertEqual(self.ws.get_job(2)["settings"], before); self.assertNotEqual(self.store.latest_events()["job:2"]["event"], "PROMOTED")

    def test_promote_detects_drift(self):
        self.store.add_event("job:2", "APPROVED"); pipeline.migrate(self.ws, self.store, self.cfg)
        self.store.add_event("job:2", "VALIDATED"); self.store.add_event("job:2", "PROMOTE_APPROVED")
        self.ws.jobs[2]["settings"]["name"] = "someone edited me"
        pipeline.promote(self.ws, self.store, self.cfg)
        e = self.store.latest_events()["job:2"]; self.assertEqual(e["event"], "VALIDATION_FAILED"); self.assertIn("DRIFT", e["detail"]["reason"])
        self.assertEqual(self.ws.get_job(2)["settings"]["name"], "someone edited me")

    def test_guard_pauses_duplicate_schedule(self):
        self.assertEqual(pipeline.run_guard(self.ws, self.store, self.cfg), 1)
        self.assertEqual(self.ws.jobs[22]["settings"]["schedule"]["pause_status"], "PAUSED")
        self.assertEqual(pipeline.run_guard(self.ws, self.store, self.cfg), 0)       # idempotent
        self.assertEqual(self.store.tables["guard_log"][0]["action"], "PAUSED")
    def test_guard_dry_run(self):
        self.cfg.dry_run = True; pipeline.run_guard(self.ws, self.store, self.cfg)
        self.assertEqual(self.ws.jobs[22]["settings"]["schedule"]["pause_status"], "UNPAUSED")

class ValidateSandbox(unittest.TestCase):
    """Sandbox-mode A/B: job parameter redirects output schema, classic vs serverless compared table by table."""
    def setUp(self):
        d, _, _ = load_fleet()
        job = d["jobs"][0]; job["settings"]["parameters"] = [{"name": "target_schema", "default": "x"}]
        self.eng = FakeEngine(); self.spark = FakeSpark(self.eng)
        self.ws = FakeWorkspace([job], d["notebooks"]); self.store = FakeStore(); self.cfg = Config(catalog="c", target_param="target_schema")
        self.serverless_rows = [(1, 2.0), (2, 3.0)]
        ws = self.ws
        def run_and_wait(job_id, params=None, timeout_h=3):
            schema = params["target_schema"]
            rows = [(1, 2.0), (2, 3.0)] if job_id == 1 else self.serverless_rows
            self.eng.put(f"{schema}.out", [("id", "bigint"), ("v", "double")], rows)
            return {"run_id": str(job_id), "result": "SUCCESS", "seconds": 100.0 if job_id == 1 else 40.0}
        ws.run_and_wait = run_and_wait
        self.store.add_event("job:1", "APPROVED"); pipeline.migrate(ws, self.store, self.cfg)
    def test_pass(self):
        pipeline.validate(self.ws, self.store, self.cfg, self.spark)
        e = self.store.latest_events()["job:1"]; self.assertEqual(e["event"], "VALIDATED"); self.assertEqual(e["detail"]["perf"]["speedup"], 2.5)
        self.assertEqual(self.store.tables["validation_results"][0]["status"], "PASS")
    def test_fail_blocks_promotion_path(self):
        self.serverless_rows = [(1, 2.0), (2, 3.5)]
        pipeline.validate(self.ws, self.store, self.cfg, self.spark)
        e = self.store.latest_events()["job:1"]; self.assertEqual(e["event"], "VALIDATION_FAILED"); self.assertEqual(e["detail"]["verdict"], "FAIL")
    def test_restore_mode_requires_opt_in(self):
        self.cfg.target_param = ""
        pipeline.validate(self.ws, self.store, self.cfg, self.spark)
        e = self.store.latest_events()["job:1"]; self.assertEqual(e["event"], "VALIDATION_FAILED")
        self.assertTrue("output tables" in e["detail"]["error"] or "allow_restore" in e["detail"]["error"])
