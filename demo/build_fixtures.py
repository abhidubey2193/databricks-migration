"""Generates tests/fixtures/fleet.json - a synthetic 20-job classic fleet that exercises every limitation L1..L13.
No customer data. Run: python demo/build_fixtures.py"""
import json, os

HDR = "# Databricks notebook source\n"
SEP = "\n# COMMAND ----------\n"
CL = lambda v="15.4.x-scala2.12", **kw: {"spark_version": v, "node_type_id": "Standard_DS3_v2", "num_workers": 2, **kw}
JC = lambda key="jc", **kw: [{"job_cluster_key": key, "new_cluster": CL(**kw)}]
def nbt(key, path, jc="jc", deps=(), libs=None, **extra):
    t = {"task_key": key, "notebook_task": {"notebook_path": path}, "job_cluster_key": jc}
    if deps: t["depends_on"] = [{"task_key": d} for d in deps]
    if libs: t["libraries"] = libs
    t.update(extra); return t
def job(i, name, tasks, clusters=None, cron="0 0 2 * * ?", paused=False, tags=None, **extra):
    s = {"name": name, "tasks": tasks, "tags": tags or {"team": "data-eng"}, "max_concurrent_runs": 1,
         "schedule": {"quartz_cron_expression": cron, "timezone_id": "UTC", "pause_status": "PAUSED" if paused else "UNPAUSED"}}
    if clusters is not None: s["job_clusters"] = clusters
    s.update(extra)
    return {"job_id": i, "creator_user_name": "owner@example.com", "settings": s}

nb = {}
nb["/demo/clean_etl"] = (HDR + 'df = spark.read.table("raw.orders")\ndf.write.mode("overwrite").saveAsTable("silver.orders")', "python")
nb["/demo/cache_heavy"] = (HDR + 'df = spark.read.table("raw.events")\ndf = df.cache()\ndf.count()\nagg = df.groupBy("k").count().persist()\nagg.write.mode("overwrite").saveAsTable("gold.event_counts")\nagg.unpersist()\ndf2 = df.checkpoint()', "python")
nb["/demo/global_view"] = (HDR + 'df = spark.table("raw.a")\ndf.createGlobalTempView("shared_a")\nspark.sql("select * from global_temp.shared_a").write.saveAsTable("silver.a2")', "python")
nb["/demo/mount_user"] = (HDR + 'dbutils.fs.mount(\n  source="s3a://bucket/raw",\n  mount_point="/mnt/raw",\n  extra_configs={"x": "y"})\ndf = spark.read.parquet("/mnt/raw/customers")\ndf.write.mode("append").saveAsTable("silver.customers")', "python")
nb["/demo/scala_simple"] = ('// Databricks notebook source\nval df = spark.read.table("raw.sales")\n  .filter($"amount" > 0)\n  .groupBy("region")\n  .agg(sum("amount").as("total"))\ndf.write.mode("overwrite").saveAsTable("gold.sales_by_region")', "scala")
nb["/demo/scala_hard"] = ('// Databricks notebook source\ncase class Rec(id: Long, v: String)\nval ds = spark.read.table("raw.r").as[Rec]\nds.map { r => r.copy(v = r.v.trim) }.write.saveAsTable("silver.r")', "scala")
nb["/demo/r_report"] = ("# Databricks notebook source\nlibrary(SparkR)\ndf <- sql('select * from gold.sales_by_region')", "r")
nb["/demo/streaming"] = (HDR + 'q = (spark.readStream.table("raw.stream_in")\n  .writeStream.format("delta")\n  .option("checkpointLocation", "/Volumes/main/chk/s1")\n  .toTable("silver.stream_out"))', "python")
nb["/demo/ansi_sql"] = ("-- Databricks notebook source\nCREATE OR REPLACE TABLE silver.ratios AS\nSELECT CAST(a AS INT) AS ai, a / b AS ratio, to_date(d, 'yyyy-MM-dd') AS dt, sum(x) AS sx FROM raw.t GROUP BY ALL", "sql")
nb["/demo/rdd_user"] = (HDR + 'rdd = spark.sparkContext.parallelize([1,2,3])\nprint(rdd.count())', "python")
nb["/demo/big_task"] = (HDR + 'spark.table("raw.x").write.mode("overwrite").saveAsTable("silver.x_out")', "python")

init = {"dbfs:/init/pip_only.sh": "#!/bin/bash\n/databricks/python/bin/pip install requests==2.31.0 pyyaml\n",
        "dbfs:/init/native.sh": "#!/bin/bash\napt-get install -y libgdal-dev\npip install fiona\nexport GDAL_DATA=/usr/share/gdal\n"}
jobs = []
jobs.append(job(1, "clean_etl", [nbt("t", "/demo/clean_etl")], JC()))
jobs.append(job(2, "cache_heavy", [nbt("t", "/demo/cache_heavy")], JC()))
jobs.append(job(3, "global_view", [nbt("t", "/demo/global_view")], JC()))
jobs.append(job(4, "mount_user", [nbt("t", "/demo/mount_user")], JC()))
jobs.append(job(5, "scala_simple", [nbt("t", "/demo/scala_simple")], JC()))
jobs.append(job(6, "scala_hard", [nbt("t", "/demo/scala_hard")], JC()))
jobs.append(job(7, "r_report", [nbt("t", "/demo/r_report")], JC()))
jobs.append(job(8, "instance_profile", [nbt("t", "/demo/clean_etl")], JC(aws_attributes={"instance_profile_arn": "arn:aws:iam::123456789012:instance-profile/etl"})))
jobs.append(job(9, "egg_lib", [nbt("t", "/demo/clean_etl", libs=[{"egg": "dbfs:/libs/legacy.egg"}, {"pypi": {"package": "pandas==2.1.0"}}])], JC()))
jobs.append(job(10, "init_pip_only", [nbt("t", "/demo/clean_etl")], JC(init_scripts=[{"dbfs": {"destination": "dbfs:/init/pip_only.sh"}}], spark_env_vars={"STAGE": "prod"})))
jobs.append(job(11, "init_native", [nbt("t", "/demo/clean_etl")], JC(init_scripts=[{"dbfs": {"destination": "dbfs:/init/native.sh"}}])))
big = [nbt(f"step_{i:02d}", "/demo/big_task", deps=([f"step_{i-1:02d}"] if i else []), libs=[{"pypi": {"package": "requests==2.31.0"}}]) for i in range(25)]
jobs.append(job(12, "big_dag_25_tasks", big, JC()))
jobs.append(job(13, "streaming_default_trigger", [nbt("t", "/demo/streaming")], JC()))
jobs.append(job(14, "ansi_sql", [nbt("t", "/demo/ansi_sql")], JC()))
jobs.append(job(15, "docker_image", [nbt("t", "/demo/clean_etl")], JC(docker_image={"url": "myreg/custom:1.0"})))
jobs.append(job(16, "ml_runtime", [nbt("t", "/demo/clean_etl")], JC(v="15.4.x-cpu-ml-scala2.12")))
jobs.append(job(17, "legacy_dbr_10_4", [nbt("t", "/demo/clean_etl")], JC(v="10.4.x-scala2.12")))
jobs.append(job(18, "mixed_jar_and_python", [nbt("py", "/demo/clean_etl"),
        {"task_key": "jvm", "spark_jar_task": {"main_class_name": "com.acme.Main"}, "job_cluster_key": "jc", "depends_on": [{"task_key": "py"}]}], JC()))
jobs.append(job(19, "bundle_managed", [nbt("t", "/demo/clean_etl")], JC(), deployment={"kind": "BUNDLE", "metadata_file_path": "/Workspace/.bundle/x/state/metadata.json"}))
jobs.append(job(20, "already_serverless", [{"task_key": "t", "notebook_task": {"notebook_path": "/demo/clean_etl"}, "environment_key": "e"}],
                environments=[{"environment_key": "e", "spec": {"client": "2"}}]))
jobs.append(job(21, "rdd_user", [nbt("t", "/demo/rdd_user")], JC()))
# L10: a clone of job 1 that is ALSO active (duplicate schedule hazard)
jobs.append(job(22, "clean_etl [serverless]", [{"task_key": "t", "notebook_task": {"notebook_path": "/demo/clean_etl"}, "environment_key": "e"}],
                tags={"smct_clone": "true", "smct_origin_job_id": "1"}, environments=[{"environment_key": "e", "spec": {"client": "2"}}]))
out = {"jobs": jobs, "notebooks": {k: {"source": v[0], "language": v[1]} for k, v in nb.items()}, "init_scripts": init, "clusters": {}}
here = os.path.dirname(os.path.abspath(__file__))
dest = os.path.join(here, "..", "tests", "fixtures", "fleet.json")
json.dump(out, open(dest, "w"), indent=1)
print("wrote", dest, len(jobs), "jobs")
