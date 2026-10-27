"""Delta persistence through Spark (used by the jobs) - tables/views are described in smc.sql."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from . import sql as S


def now():
    return datetime.now(timezone.utc)


def _cols(ddl: str) -> List[str]:
    return [p.strip().split()[0] for p in ddl.replace("\n", " ").split(",") if p.strip()]


class Store:
    def __init__(self, spark, catalog: str, schema: str):
        self.spark, self.catalog, self.schema = spark, catalog, schema
        self.fq = f"`{catalog}`.`{schema}`"

    def ensure(self) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.fq}")
        for t, ddl in S.TABLE_DDL.items():
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.fq}.{t} ({ddl})")

    def create_views(self, team_tag: str = "team", window: int = 30) -> None:
        self.spark.sql(S.VIEW_STATUS.format(fq=self.fq))
        try:
            self.spark.sql(S.VIEW_COST.format(fq=self.fq, team_tag=team_tag))
            self.spark.sql(S.VIEW_COST_COMPARE.format(fq=self.fq, window=window))
        except Exception as e:  # system tables not granted: keep the rest of the tool working
            print(f"[smct] cost views skipped (need SELECT on system.billing): {e}")

    def _df(self, table: str, rows: Iterable[Dict[str, Any]]):
        ddl = S.TABLE_DDL[table]
        cols = _cols(ddl)
        data = [tuple(r.get(c) for c in cols) for r in rows]
        return self.spark.createDataFrame(data, schema=ddl.replace("\n", " "))

    def replace(self, table: str, rows: List[Dict[str, Any]]) -> None:
        self._df(table, rows).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{self.fq}.{table}")

    def append(self, table: str, rows: List[Dict[str, Any]]) -> None:
        if rows:
            self._df(table, rows).write.mode("append").saveAsTable(f"{self.fq}.{table}")

    # ---- events / status
    def add_event(self, workload_id: str, event: str, actor: str = "smct", detail: Optional[Dict[str, Any]] = None) -> None:
        self.append("migration_events", [{"workload_id": workload_id, "ts": now(), "actor": actor, "event": event,
                                          "detail": json.dumps(detail or {}, default=str)}])

    def latest_events(self) -> Dict[str, Dict[str, Any]]:
        rows = self.spark.sql(f"""SELECT workload_id, event, actor, detail, ts FROM (
            SELECT *, row_number() OVER (PARTITION BY workload_id ORDER BY ts DESC) rn FROM {self.fq}.migration_events) WHERE rn = 1""").collect()
        return {r.workload_id: {"event": r.event, "actor": r.actor, "detail": json.loads(r.detail or "{}"), "ts": r.ts} for r in rows}

    def last_event(self, workload_id: str, event: str) -> Optional[Dict[str, Any]]:
        rows = self.spark.sql(f"SELECT detail, ts, actor FROM {self.fq}.migration_events WHERE workload_id = '{workload_id}' "
                              f"AND event = '{event}' ORDER BY ts DESC LIMIT 1").collect()
        return {"detail": json.loads(rows[0].detail or "{}"), "ts": rows[0].ts, "actor": rows[0].actor} if rows else None

    def ids_in_status(self, status: str) -> List[str]:
        return [w for w, e in self.latest_events().items() if e["event"] == status]

    def monthly_dbus(self) -> Dict[str, float]:
        try:
            return {r.workload_id: float(r.dbus) for r in self.spark.sql(S.Q_MONTHLY_DBUS).collect()}
        except Exception:  # noqa: BLE001
            return {}
