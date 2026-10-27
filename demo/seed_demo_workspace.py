"""Seed a Databricks workspace with a small *classic* demo fleet so the judges (or you) can see SMCT work end to end.

  pip install databricks-sdk
  python demo/seed_demo_workspace.py --folder /Workspace/Users/<you>/smct_demo          # create
  python demo/seed_demo_workspace.py --folder /Workspace/Users/<you>/smct_demo --delete # clean up

Creates (all schedules PAUSED, nothing is run):
  * jobs generated from tests/fixtures/fleet.json that the API accepts (one per limitation family)
  * two RUNNABLE A/B demo jobs that write to a schema given by the `target_schema` job parameter:
      smct-demo-ab-pass      uses .cache() + CAST on bad data  -> SMCT rewrites it, A/B result PASS
      smct-demo-ab-overflow  int overflow (wraps on classic, raises under ANSI) -> A/B FAILS, promotion blocked
No customer data is used; the demo tables are generated with spark.range().
"""
import argparse, io, json, os, sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings
from databricks.sdk.service.workspace import ImportFormat, Language

HERE = os.path.dirname(os.path.abspath(__file__))
PREFIX = "smct-demo-"

AB_PASS = '''# Databricks notebook source
dbutils.widgets.text("target_schema", "main.smct_demo_out")
schema = dbutils.widgets.get("target_schema")
df = spark.range(0, 100000).selectExpr("id", "CAST(id % 7 AS STRING) AS k", "CAST(concat('x', id) AS INT) AS bad_cast", "id * 1.5 AS amount")
df = df.cache()
agg = df.groupBy("k").sum("amount").withColumnRenamed("sum(amount)", "total")
agg.write.mode("overwrite").saveAsTable(f"{schema}.agg")
df.write.mode("overwrite").saveAsTable(f"{schema}.detail")
'''
AB_OVERFLOW = '''# Databricks notebook source
dbutils.widgets.text("target_schema", "main.smct_demo_out")
schema = dbutils.widgets.get("target_schema")
df = spark.range(10).selectExpr("id", "CAST(2147483647 AS INT) + CAST(id AS INT) AS v")   # wraps on classic, raises under ANSI
df.write.mode("overwrite").saveAsTable(f"{schema}.overflow")
'''


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--folder", required=True)
    ap.add_argument("--delete", action="store_true")
    a = ap.parse_args()
    w = WorkspaceClient()
    if a.delete:
        for j in w.jobs.list():
            if j.settings and j.settings.name and j.settings.name.startswith(PREFIX):
                w.jobs.delete(j.job_id); print("deleted", j.settings.name)
        w.workspace.delete(a.folder, recursive=True); return
    fleet = json.load(open(os.path.join(HERE, "..", "tests", "fixtures", "fleet.json")))
    w.workspace.mkdirs(a.folder)
    lang_map = {"python": Language.PYTHON, "sql": Language.SQL, "scala": Language.SCALA, "r": Language.R}
    pmap = {}
    for p, nb in {**fleet["notebooks"], "/demo/ab_pass": {"source": AB_PASS, "language": "python"},
                  "/demo/ab_overflow": {"source": AB_OVERFLOW, "language": "python"}}.items():
        dest = a.folder + p[len("/demo"):]
        w.workspace.upload(dest, io.BytesIO(nb["source"].encode()), format=ImportFormat.SOURCE, language=lang_map[nb["language"]], overwrite=True)
        pmap[p] = dest
    for dbfs_path, text in fleet["init_scripts"].items():
        try:
            w.dbfs.upload(dbfs_path[5:], io.BytesIO(text.encode()), overwrite=True)
        except Exception as e:  # noqa: BLE001
            print("init script upload skipped:", e)
    node = w.clusters.select_node_type(local_disk=True)
    lts = w.clusters.select_spark_version(long_term_support=True)
    made = []
    skip = {"already_serverless", "clean_etl [serverless]", "instance_profile"}      # need real ARNs / would pre-create a clone
    for j in fleet["jobs"]:
        s = json.loads(json.dumps(j["settings"]))
        if s["name"] in skip:
            continue
        s["name"] = PREFIX + s["name"]
        s["schedule"]["pause_status"] = "PAUSED"
        for c in s.get("job_clusters", []):
            nc = c["new_cluster"]; nc["node_type_id"] = node
            if nc["spark_version"].startswith("15.4.x-scala"):
                nc["spark_version"] = lts
        for t in s["tasks"]:
            if "notebook_task" in t:
                t["notebook_task"]["notebook_path"] = pmap[t["notebook_task"]["notebook_path"]]
        s.pop("deployment", None)
        try:
            jid = w.jobs.create(**{k: v for k, v in JobSettings.from_dict(s).__dict__.items() if v is not None}).job_id
            made.append((jid, s["name"]))
        except Exception as e:  # noqa: BLE001
            print("skipped", s["name"], "->", str(e)[:120])
    for name, path in (("ab-pass", "/ab_pass"), ("ab-overflow", "/ab_overflow")):
        s = {"name": PREFIX + name, "parameters": [{"name": "target_schema", "default": "main.smct_demo_out"}], "tags": {"team": "demo"},
             "job_clusters": [{"job_cluster_key": "jc", "new_cluster": {"spark_version": lts, "node_type_id": node, "num_workers": 1}}],
             "tasks": [{"task_key": "t", "job_cluster_key": "jc", "notebook_task": {"notebook_path": a.folder + path, "base_parameters": {}}}]}
        jid = w.jobs.create(**{k: v for k, v in JobSettings.from_dict(s).__dict__.items() if v is not None}).job_id
        made.append((jid, s["name"]))
    for jid, n in made:
        print(f"{jid:>10}  {n}")
    print("\nNext: databricks bundle run smct_discover  ->  open the app  ->  approve smct-demo-ab-pass / smct-demo-ab-overflow")
    print("Run the migrate+validate job with the bundle variable target_param=target_schema (sandbox A/B mode).")


if __name__ == "__main__":
    main()
