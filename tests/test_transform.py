import copy, json, unittest
from tests import helpers
from smc.transform import *
from smc.drift import bundle_yaml, to_yaml, detect_drift, settings_diff
from smc.guard import find_violations, pause_settings, is_active
from smc.state import can_transition, require

def mkjob(n=3, libs=None, cluster=None):
    return {"name": "j", "tags": {"team": "x"}, "job_clusters": [{"job_cluster_key": "jc", "new_cluster": cluster or {"spark_version": "15.4.x-scala2.12"}}],
            "schedule": {"quartz_cron_expression": "0 0 1 * * ?", "timezone_id": "UTC", "pause_status": "UNPAUSED"},
            "email_notifications": {"on_failure": ["a@b.c"]},
            "tasks": [{"task_key": f"t{i}", "notebook_task": {"notebook_path": "/n"}, "job_cluster_key": "jc",
                       "depends_on": [{"task_key": f"t{i-1}"}] if i else [], **({"libraries": libs} if libs else {})} for i in range(n)]}

class Transform(unittest.TestCase):
    def test_no_task_cap(self):
        r = to_serverless(mkjob(40), origin_job_id=1)
        self.assertEqual(len(r.migrated_tasks), 40)
        self.assertTrue(all("job_cluster_key" not in t and "new_cluster" not in t for t in r.settings["tasks"]))
        self.assertTrue(all("environment_key" not in t for t in r.settings["tasks"]))       # notebook tasks without dependencies need none
        self.assertNotIn("job_clusters", r.settings)
    def test_clone_has_no_triggers_and_is_tagged(self):
        s = to_serverless(mkjob(), origin_job_id=7).settings
        for k in ("schedule", "trigger", "continuous"): self.assertNotIn(k, s)
        self.assertEqual(s["tags"]["smct_origin_job_id"], "7"); self.assertEqual(s["tags"]["smct_clone"], "true"); self.assertTrue(s["name"].endswith("[serverless]"))
    def test_original_not_mutated(self):
        j = mkjob(); snap = copy.deepcopy(j); to_serverless(j, origin_job_id=1); self.assertEqual(j, snap)
    def test_library_mapping_and_dedupe(self):
        s = to_serverless(mkjob(3, libs=[{"pypi": {"package": "a==1"}}, {"whl": "/Volumes/c/s/v/x.whl"}]), origin_job_id=1).settings
        self.assertEqual(len(s["environments"]), 1)
        self.assertEqual(s["environments"][0]["spec"]["dependencies"], ["a==1", "/Volumes/c/s/v/x.whl"])
        self.assertEqual(s["environments"][0]["spec"]["environment_version"], ENV_CLIENT_VERSION)
    def test_blocker_library_keeps_task_on_classic(self):
        r = to_serverless(mkjob(2, libs=[{"egg": "x.egg"}]), origin_job_id=1)
        self.assertEqual(r.migrated_tasks, []); self.assertEqual(len(r.left_on_classic), 2); self.assertIn("job_clusters", r.settings)
    def test_jar_task_left_on_classic_in_mixed_job(self):
        j = mkjob(1); j["tasks"].append({"task_key": "jvm", "spark_jar_task": {"main_class_name": "M"}, "job_cluster_key": "jc"})
        r = to_serverless(j, origin_job_id=1)
        self.assertEqual(r.migrated_tasks, ["t0"]); self.assertEqual(r.left_on_classic[0][0], "jvm")
        self.assertEqual(r.settings["job_clusters"][0]["job_cluster_key"], "jc")             # still needed by the jar task
    def test_init_script_deps_flow_into_env(self):
        c = {"spark_version": "15.4.x-scala2.12", "init_scripts": [{"dbfs": {"destination": "dbfs:/i.sh"}}]}
        s = to_serverless(mkjob(1, cluster=c), origin_job_id=1, init_fetch=lambda d: "pip install requests==2").settings
        self.assertEqual(s["environments"][0]["spec"]["dependencies"], ["requests==2"])
    def test_distinct_dependency_sets_get_distinct_envs(self):
        j = mkjob(2); j["tasks"][0]["libraries"] = [{"pypi": {"package": "a"}}]; j["tasks"][1]["libraries"] = [{"pypi": {"package": "b"}}]
        s = to_serverless(j, origin_job_id=1).settings
        self.assertEqual(len(s["environments"]), 2); self.assertNotEqual(s["tasks"][0]["environment_key"], s["tasks"][1]["environment_key"])
    def test_batches_respect_dependencies(self):
        b = batch_plan(mkjob(25)); self.assertEqual([len(x) for x in b], [10, 10, 5]); self.assertEqual(b[0][0], "t0")
        r = to_serverless(mkjob(25), origin_job_id=1, task_keys=b[0])
        self.assertEqual(len(r.migrated_tasks), 10); self.assertEqual(sum(1 for t in r.settings["tasks"] if "job_cluster_key" in t), 15)
    def test_notebook_path_map(self):
        s = to_serverless(mkjob(1), origin_job_id=1, notebook_path_map={"/n": "/migrated/n"}).settings
        self.assertEqual(s["tasks"][0]["notebook_task"]["notebook_path"], "/migrated/n")
    def test_promote_keeps_identity_and_schedule(self):
        o = mkjob(2); o["tags"] = {"team": "x", "owner": "me"}
        clone = to_serverless(o, origin_job_id=1).settings
        p = promote_settings(o, clone)
        self.assertEqual(p["name"], "j"); self.assertEqual(p["schedule"], o["schedule"]); self.assertEqual(p["email_notifications"], o["email_notifications"])
        self.assertNotIn("smct_clone", p["tags"]); self.assertEqual(p["tags"]["owner"], "me"); self.assertEqual(p["tags"]["smct_migrated"], "true")
        self.assertTrue(all("job_cluster_key" not in t and "new_cluster" not in t for t in p["tasks"]))

    def test_promote_preserves_bundle_deployment_metadata(self):
        o = mkjob(1); o["deployment"] = {"kind": "BUNDLE", "metadata_file_path": "/x"}; o["description"] = "keep me"
        p = promote_settings(o, to_serverless(o, origin_job_id=1).settings)
        self.assertEqual(p["deployment"], o["deployment"]); self.assertEqual(p["description"], "keep me")

class Guard(unittest.TestCase):
    def test_fleet_violation(self):
        d, _, _ = helpers.load_fleet()
        # clone 22 is already serverless w/ active schedule? fixture clone carries the original's schedule
        vs = find_violations(d["jobs"]); self.assertEqual([v.job_id for v in vs], [22])
        self.assertEqual(vs[0].paused_settings["schedule"]["pause_status"], "PAUSED")
    def test_paused_clone_ok(self):
        o = {"job_id": 1, "settings": {"name": "a", "schedule": {"pause_status": "UNPAUSED", "quartz_cron_expression": "x"}}}
        c = {"job_id": 2, "settings": {"name": "a [serverless]", "tags": {"smct_clone": "true", "smct_origin_job_id": "1"}, "schedule": {"pause_status": "PAUSED", "quartz_cron_expression": "x"}}}
        self.assertEqual(find_violations([o, c]), [])
    def test_manual_clone_detected_by_name_and_cron(self):
        sched = {"pause_status": "UNPAUSED", "quartz_cron_expression": "0 0 * * * ?"}
        o = {"job_id": 1, "settings": {"name": "etl", "schedule": sched}}; c = {"job_id": 2, "settings": {"name": "etl [serverless]", "schedule": dict(sched)}}
        self.assertEqual([v.job_id for v in find_violations([o, c])], [2])

class Drift(unittest.TestCase):
    def test_yaml_is_parseable(self):
        import yaml
        s = to_serverless(mkjob(2, libs=[{"pypi": {"package": "a==1"}}]), origin_job_id=1).settings
        y = yaml.safe_load(bundle_yaml("my job-1", s))
        self.assertEqual(y["resources"]["jobs"]["my_job_1"]["tasks"][0]["task_key"], "t0")
        self.assertEqual(y["resources"]["jobs"]["my_job_1"]["environments"][0]["spec"]["dependencies"], ["a==1"])
    def test_hash_drift(self):
        j = mkjob(); h = canonical_hash(j); self.assertFalse(detect_drift(h, j)); j["name"] = "changed"; self.assertTrue(detect_drift(h, j))
    def test_diff_nonempty(self): self.assertIn("serverless", settings_diff({"a": 1}, {"a": 2}))

class State(unittest.TestCase):
    def test_gates(self):
        self.assertTrue(can_transition("DISCOVERED", "APPROVED")); self.assertFalse(can_transition("CLONED", "PROMOTED"))
        self.assertFalse(can_transition("VALIDATION_FAILED", "PROMOTE_APPROVED")); self.assertTrue(can_transition("VALIDATED", "PROMOTE_APPROVED"))
        with self.assertRaises(ValueError): require("DISCOVERED", "PROMOTED")


class EnvironmentKeyRules(unittest.TestCase):
    """Per the Databricks bundle examples: python/wheel/dbt tasks require environment_key on serverless; notebook tasks need it only for dependencies."""
    def _job(self, task, libs=None):
        t = {"task_key": "t", "new_cluster": {"spark_version": "14.3.x-scala2.12"}, **task}
        if libs: t["libraries"] = libs
        return {"name": "j", "tasks": [t]}
    def test_python_task_always_gets_environment_key(self):
        s = to_serverless(self._job({"spark_python_task": {"python_file": "/f.py"}}), origin_job_id=1).settings
        self.assertEqual(s["tasks"][0]["environment_key"], "smct_env")
        self.assertEqual(s["environments"][0]["spec"], {"environment_version": ENV_CLIENT_VERSION})
    def test_notebook_with_dependencies_gets_key(self):
        s = to_serverless(self._job({"notebook_task": {"notebook_path": "/n"}}, [{"pypi": {"package": "requests"}}]), origin_job_id=1).settings
        self.assertEqual(s["tasks"][0]["environment_key"], "smct_env"); self.assertEqual(s["environments"][0]["spec"]["dependencies"], ["requests"])
    def test_notebook_without_dependencies_gets_none(self):
        s = to_serverless(self._job({"notebook_task": {"notebook_path": "/n"}}), origin_job_id=1).settings
        self.assertNotIn("environment_key", s["tasks"][0])
    def test_fallback_retries_without_environment_key_on_notebook_tasks(self):
        from smc import pipeline
        s = to_serverless(self._job({"notebook_task": {"notebook_path": "/n"}}, [{"pypi": {"package": "requests"}}]), origin_job_id=1).settings
        calls = []
        class WS:
            def create_job(self, settings):
                calls.append(settings)
                if any("environment_key" in t for t in settings["tasks"]):
                    raise RuntimeError("Invalid request: environment_key is not supported for notebook tasks")
                return 55
        notes = []
        self.assertEqual(pipeline.create_with_fallback(WS(), s, notes), 55)
        self.assertEqual(len(calls), 2); self.assertNotIn("environments", calls[1]); self.assertIn("%pip install", notes[0])
    def test_other_errors_are_not_swallowed(self):
        from smc import pipeline
        class WS:
            def create_job(self, settings): raise RuntimeError("quota exceeded")
        with self.assertRaises(RuntimeError): pipeline.create_with_fallback(WS(), {"tasks": []}, [])
