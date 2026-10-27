import unittest
from tests import helpers
from smc.config_scan import scan_job, scan_cluster_spec, scan_libraries
from smc.initscripts import parse_init_script
from smc.scoring import plan_waves, summarize

class Init(unittest.TestCase):
    def test_pip_and_unsupported(self):
        r = parse_init_script("#!/bin/bash\nset -e\n/databricks/python/bin/pip install requests==2.31 pyyaml \\\n  numpy\napt-get install -y x\nexport A=1\nwget http://x/y.jar -O /databricks/jars/y.jar\n")
        self.assertEqual(r.pip_deps, ["requests==2.31", "pyyaml", "numpy"])
        self.assertEqual(r.env_vars, {"A": "1"}); self.assertTrue(r.jars)
        self.assertEqual(len(r.unsupported), 2); self.assertFalse(r.fully_translatable)
    def test_index_url_flagged(self):
        self.assertTrue(parse_init_script("pip install --index-url http://x pkg").unsupported)

class Libs(unittest.TestCase):
    def test_mapping(self):
        f, deps = scan_libraries([{"pypi": {"package": "a==1"}}, {"whl": "/Volumes/c/s/v/x.whl"}, {"whl": "dbfs:/libs/y.whl"},
                                  {"egg": "e.egg"}, {"jar": "j.jar"}, {"maven": {"coordinates": "g:a:1"}}], "t")
        self.assertEqual(deps, ["a==1", "/Volumes/c/s/v/x.whl"])
        self.assertEqual({x.rule_id for x in f}, {"L5.WHL_MAPPED", "L5.WHL_LOCATION", "L5.EGG", "L5.JAR", "L5.MAVEN"})

class Cluster(unittest.TestCase):
    def test_l4_l11(self):
        f, _ = scan_cluster_spec({"spark_version": "9.1.x-scala2.12", "docker_image": {"url": "x"}, "aws_attributes": {"instance_profile_arn": "arn"},
              "spark_conf": {"spark.hadoop.fs.s3a.secret.key": "k"}, "spark_env_vars": {"A": "b"}, "cluster_log_conf": {"dbfs": {}},
              "ssh_public_keys": ["k"], "policy_id": "p"}, "c")
        self.assertEqual({x.rule_id for x in f}, {"L4.CUSTOM_IMAGE", "L4.OLD_DBR", "L4.INSTANCE_PROFILE", "L4.STORAGE_CREDS_IN_CONF",
                          "L11.SPARK_CONF_DROPPED", "L11.ENV_VARS_DROPPED", "L11.LOG_DELIVERY", "L11.SSH", "L11.CLUSTER_POLICY"})
    def test_ml_and_unreadable_init(self):
        f, _ = scan_cluster_spec({"spark_version": "15.4.x-cpu-ml-scala2.12", "init_scripts": [{"s3": {"destination": "s3://b/i.sh"}}]}, "c", lambda d: None)
        self.assertEqual({x.rule_id for x in f}, {"L4.ML_RUNTIME", "L3.INIT_UNREADABLE"})

class Fleet(unittest.TestCase):
    """End-to-end verdicts on the synthetic fleet - this is the regression suite for the scoring policy."""
    @classmethod
    def setUpClass(cls):
        d, nb, init = helpers.load_fleet()
        cls.reps = {r.name: r for r in (scan_job(j, d["clusters"], init, nb, {"/mnt/raw": "/Volumes/m/l/raw"}) for j in d["jobs"])}
    def v(self, n): return self.reps[n].verdict
    def test_ready(self): self.assertEqual(self.v("clean_etl"), "READY")
    def test_autofix(self):
        for n in ("cache_heavy", "global_view", "mount_user", "streaming_default_trigger", "ansi_sql", "init_pip_only", "bundle_managed"):
            self.assertEqual(self.v(n), "AUTO_FIXABLE", n)
    def test_manual(self):
        for n in ("egg_lib", "instance_profile", "legacy_dbr_10_4", "ml_runtime", "rdd_user", "scala_hard", "scala_simple", "init_native"):
            self.assertEqual(self.v(n), "NEEDS_MANUAL", n)
    def test_keep_classic(self):
        for n in ("docker_image", "r_report"): self.assertEqual(self.v(n), "KEEP_CLASSIC", n)
    def test_partial_mixed(self): self.assertEqual(self.v("mixed_jar_and_python"), "PARTIAL")
    def test_serverless_done(self): self.assertEqual(self.v("already_serverless"), "ALREADY_SERVERLESS")
    def test_large_job_flagged_but_ready(self):
        r = self.reps["big_dag_25_tasks"]; self.assertIn("L2.LARGE_JOB", {f.rule_id for f in r.findings}); self.assertEqual(r.verdict, "READY")
    def test_every_limitation_exercised(self):
        seen = {f.limitation for r in self.reps.values() for f in r.findings}
        for L in ("L2", "L3", "L4", "L5", "L6", "L7", "L9", "L11", "L13"): self.assertIn(L, seen)
    def test_plan_ordering(self):
        rows = plan_waves(self.reps.values(), {"job:2": 900.0, "job:3": 10.0})
        w1 = [r for r in rows if r["wave"] == 1]
        self.assertLess([r["workload_id"] for r in w1].index("job:2"), [r["workload_id"] for r in w1].index("job:3"))
        self.assertEqual(rows[-1]["wave"], -1)
        self.assertEqual(summarize(rows)["TOTAL"], 22)
