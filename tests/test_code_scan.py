import ast, unittest
from tests import helpers  # noqa
from smc.code_scan import (parse_notebook, render_notebook, scan_python, scan_sql_text, scan_notebook,
                           rewrite_python, rewrite_notebook, rewrite_sql, classify_scala, detect_output_tables)

def rules(fs): return {f.rule_id for f in fs}

class CodeScan(unittest.TestCase):
    def test_detects_l6(self):
        code = 'df = df.cache()\nx = df.persist(StorageLevel.DISK_ONLY)\ndf.checkpoint()\ndf.createGlobalTempView("a")\n' \
               'dbutils.fs.mount(source="s3a://b", mount_point="/mnt/x")\nrdd = sc.parallelize([1])\nspark.conf.set("spark.foo.bar","1")\nspark.catalog.cacheTable("t")'
        r = rules(scan_python(code, "nb"))
        for x in ("L6.CACHE", "L6.CHECKPOINT", "L6.GLOBAL_TEMP_VIEW", "L6.MOUNT_API", "L6.RDD", "L6.SPARK_CONF", "L6.CACHE_TABLE"):
            self.assertIn(x, r)
    def test_allowed_conf_not_flagged(self):
        self.assertNotIn("L6.SPARK_CONF", rules(scan_python('spark.conf.set("spark.sql.shuffle.partitions","8")', "nb")))
    def test_comments_ignored(self):
        self.assertEqual(scan_python("# df.cache()\n", "nb"), [])
    def test_streaming_triggers(self):
        self.assertIn("L13.STREAM_DEFAULT_TRIGGER", rules(scan_python('df.writeStream.format("delta").start("/p")', "nb")))
        self.assertIn("L13.STREAM_DEFAULT_TRIGGER", rules(scan_python('df.writeStream.trigger(processingTime="5 seconds").start("/p")', "nb")))
        self.assertEqual(scan_python('df.writeStream.trigger(availableNow=True).start("/p")', "nb"), [])
    def test_ansi_sql(self):
        r = rules(scan_sql_text("SELECT CAST(a AS INT), a / b, element_at(x, 1), to_date(d), sum(q) FROM t -- c / d", "nb"))
        self.assertEqual(r, {"L13.ANSI_CAST", "L13.ANSI_DIVIDE", "L13.ANSI_ELEMENT", "L13.ANSI_DATE", "L13.ANSI_SUM"})
    def test_no_false_positive_date_format_slash(self):
        self.assertNotIn("L13.ANSI_DIVIDE", rules(scan_sql_text("SELECT date_format(d, 'yyyy/MM/dd') FROM t /* a / b */", "nb")))
    def test_python_embedded_sql(self):
        self.assertIn("L13.ANSI_CAST", rules(scan_python('spark.sql("select cast(x as int) from t")', "nb")))
    def test_mount_path_needs_map_to_be_autofixable(self):
        f = [x for x in scan_python('spark.read.load("/mnt/raw/x")', "nb") if x.rule_id == "L6.MOUNT_PATH"][0]
        self.assertFalse(f.auto_fixable)
        f = [x for x in scan_python('spark.read.load("/mnt/raw/x")', "nb", mount_map={"/mnt/raw": "/Volumes/a/b/c"}) if x.rule_id == "L6.MOUNT_PATH"][0]
        self.assertTrue(f.auto_fixable)

class Rewrite(unittest.TestCase):
    def test_cache_removed_and_valid_python(self):
        src = 'df = spark.table("t")\ndf = df.cache()\nagg = df.groupBy("k").count().persist(StorageLevel.MEMORY_ONLY)\nagg.unpersist()\nprint(agg.count())\n'
        new, ap = rewrite_python(src)
        ast.parse(new)
        self.assertNotIn(".cache(", new); self.assertNotIn(".persist(", new); self.assertNotIn(".unpersist(", new)
        self.assertIn("L6.CACHE", ap)
        self.assertNotIn("L6.CACHE", {f.rule_id for f in scan_python(new, "nb")})
    def test_checkpoint_to_delta(self):
        new, ap = rewrite_python('df2 = df.checkpoint()\n', ckpt_schema="c.s")
        ast.parse(new); self.assertIn("saveAsTable", new); self.assertIn('spark.table("c.s.ckpt_df2_', new)
        self.assertIn("L6.CHECKPOINT", ap)
    def test_global_temp_and_mount(self):
        src = 'df.createGlobalTempView("v")\ndbutils.fs.mount(\n  source="s3a://b",\n  mount_point="/mnt/raw")\nspark.read.load("/mnt/raw/x")\nspark.sql("select * from global_temp.v")\n'
        new, ap = rewrite_python(src, mount_map={"/mnt/raw": "/Volumes/c/s/raw"})
        ast.parse(new)
        self.assertIn("createOrReplaceTempView", new); self.assertNotIn("global_temp", new)
        self.assertIn("/Volumes/c/s/raw/x", new); self.assertNotIn("dbutils.fs.mount", new.replace("# SMCT removed mount call (use UC Volumes): dbutils.fs.mount(", ""))
    def test_stream_trigger_inserted_and_replaced(self):
        new, _ = rewrite_python('q = df.writeStream.format("delta").start("/p")')
        self.assertIn(".trigger(availableNow=True)", new)
        new, _ = rewrite_python('q = df.writeStream.trigger(processingTime="1 minute").format("delta").start("/p")')
        self.assertIn("availableNow=True", new); self.assertNotIn("processingTime", new)
        again, ap = rewrite_python(new); self.assertEqual(again, new)            # idempotent
    def test_sql_rewrite(self):
        new, ap = rewrite_sql("CACHE TABLE x AS SELECT CAST(a AS INT) FROM t;\nCLEAR CACHE;\nCREATE GLOBAL TEMPORARY VIEW v AS SELECT 1")
        self.assertIn("CREATE OR REPLACE TEMPORARY VIEW x AS", new); self.assertIn("try_cast(", new)
        self.assertIn("-- SMCT removed: CLEAR CACHE", new); self.assertIn("CREATE OR REPLACE TEMPORARY VIEW v", new)
    def test_try_cast_not_double_rewritten(self):
        new, _ = rewrite_sql("SELECT try_cast(a AS INT) FROM t"); self.assertEqual(new, "SELECT try_cast(a AS INT) FROM t")
    def test_ansi_disable_mode(self):
        nb = "# Databricks notebook source\ndf = spark.sql('select cast(a as int) from t')\n"
        new, ap = rewrite_notebook("/x", nb, "python", ansi="disable")
        self.assertIn('spark.sql.ansi.enabled", "false"', new); self.assertIn("cast(a as int)", new); self.assertNotIn("try_cast", new)
    def test_notebook_roundtrip_with_magic(self):
        nb = "# Databricks notebook source\nprint(1)\n# COMMAND ----------\n# MAGIC %sql\n# MAGIC SELECT CAST(a AS INT)\n# MAGIC FROM t\n"
        cells, pre = parse_notebook(nb, "python")
        self.assertEqual([c.lang for c in cells], ["python", "sql"])
        new, ap = rewrite_notebook("/x", nb, "python")
        self.assertIn("# MAGIC %sql", new); self.assertIn("try_cast(a AS INT)", new)
        parse_notebook(new, "python")
    def test_scala_untouched(self):
        t = "val x = 1"; self.assertEqual(rewrite_notebook("/x", t, "scala"), (t, []))

class Scala(unittest.TestCase):
    def test_classification(self):
        self.assertEqual(classify_scala('val d = spark.read.table("a").filter($"c" > 1).groupBy("k")')["decision"], "REWRITE_PYSPARK")
        self.assertEqual(classify_scala('case class R(i: Int)\nval ds = spark.read.table("a").as[R]')["decision"], "KEEP_ON_CLASSIC")
        self.assertEqual(classify_scala('spark.sql("select 1")')["decision"], "REWRITE_SQL")
    def test_draft_is_valid_python(self):
        d = classify_scala('val df = spark.read.table("raw.sales")\n  .filter($"amount" > 0)\n  .groupBy("region")')["draft_pyspark"]
        ast.parse(d)

class Outputs(unittest.TestCase):
    def test_detect(self):
        code = 'df.write.saveAsTable("a.b.c")\nspark.sql("INSERT INTO x.y SELECT 1")\nspark.sql("MERGE INTO m.n t USING s ON 1=1")'
        self.assertEqual(detect_output_tables(code), ["a.b.c", "x.y", "m.n"])
