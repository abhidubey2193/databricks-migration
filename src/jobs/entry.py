"""Single entrypoint for all SMCT job stages (runs on serverless itself).  Usage: entry.py --stage discover ..."""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from smc.config import Config  # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--stage", required=True,
                   choices=["init", "discover", "migrate", "validate", "promote", "rollback", "guard", "cost"])
    p.add_argument("--workload_ids", default="")
    for f in ("catalog", "schema", "backup_dir", "migrated_dir", "bundle_out_dir", "ckpt_schema", "mount_map", "ansi",
              "performance_target", "target_param", "allow_restore", "validation_catalog", "exclude_columns", "team_tag",
              "cost_window_days", "batch_size", "batch_upto", "dry_run"):
        p.add_argument("--" + f, default="")
    ns, _ = p.parse_known_args()
    cfg = Config.from_args(ns)
    ids = [x.strip() for x in ns.workload_ids.split(",") if x.strip()] or None

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    from smc.store import Store
    from smc import pipeline
    from smc.db import Workspace

    store = Store(spark, cfg.catalog, cfg.schema)
    store.ensure()
    if ns.stage == "init":
        store.create_views(cfg.team_tag, cfg.cost_window_days)
        return
    ws = Workspace()
    if ns.stage == "discover":
        pipeline.discover(ws, store, cfg)
    elif ns.stage == "migrate":
        pipeline.migrate(ws, store, cfg, ids)
    elif ns.stage == "validate":
        pipeline.validate(ws, store, cfg, spark, ids)
    elif ns.stage == "promote":
        pipeline.promote(ws, store, cfg, ids)
    elif ns.stage == "rollback":
        pipeline.rollback(ws, store, cfg, ids)
    elif ns.stage == "guard":
        pipeline.run_guard(ws, store, cfg)
    elif ns.stage == "cost":
        store.create_views(cfg.team_tag, cfg.cost_window_days)


if __name__ == "__main__":
    main()
