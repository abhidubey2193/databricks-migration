"""Serverless Migration Control Tower - Databricks App (Streamlit).
Reads the Delta tables written by the SMCT jobs through a SQL warehouse; approvals are written back as audit events."""
import json
import os
import sys

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
from smc.state import can_transition  # noqa: E402

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service.sql import StatementParameterListItem  # noqa: E402

CAT, SCH = os.environ.get("SMC_CATALOG", "main"), os.environ.get("SMC_SCHEMA", "smct")
FQ = f"`{CAT}`.`{SCH}`"
WH = os.environ["DATABRICKS_WAREHOUSE_ID"]
APPROVERS = [a.strip().lower() for a in os.environ.get("SMC_APPROVERS", "").split(",") if a.strip()]
w = WorkspaceClient()

st.set_page_config(page_title="Serverless Migration Control Tower", layout="wide", page_icon="🚀")


@st.cache_data(ttl=20, show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    r = w.statement_execution.execute_statement(warehouse_id=WH, statement=sql, wait_timeout="50s")
    if r.status.state.value != "SUCCEEDED":
        raise RuntimeError(r.status.error.message if r.status.error else str(r.status.state))
    cols = [c.name for c in r.manifest.schema.columns]
    return pd.DataFrame(r.result.data_array or [], columns=cols)


def num(df, *cols):
    for c in cols:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def user() -> str:
    try:
        return st.context.headers.get("X-Forwarded-Email") or st.context.headers.get("X-Forwarded-Preferred-Username") or ""
    except Exception:  # noqa: BLE001
        return ""


def can_approve(email: str) -> bool:
    return bool(email) and ("*" in APPROVERS or email.lower() in APPROVERS)


def add_event(wid: str, event: str, actor: str, detail: dict):
    cur = dict(zip(status["workload_id"], status["status"])).get(wid, "DISCOVERED")
    if not can_transition(cur, event):                      # state machine enforces the gates server-side too
        st.error(f"{wid}: illegal transition {cur} -> {event}")
        return
    w.statement_execution.execute_statement(
        warehouse_id=WH, wait_timeout="30s",
        statement=f"INSERT INTO {FQ}.migration_events VALUES (:wid, current_timestamp(), :actor, :event, :detail)",
        parameters=[StatementParameterListItem(name="wid", value=wid), StatementParameterListItem(name="actor", value=actor),
                    StatementParameterListItem(name="event", value=event), StatementParameterListItem(name="detail", value=json.dumps(detail))])
    q.clear()


st.title("🚀 Serverless Migration Control Tower")
me = user()
st.caption(f"signed in as **{me or 'unknown'}** · approver: **{'yes' if can_approve(me) else 'no (read-only)'}** · data: {CAT}.{SCH}")

try:
    status = num(q(f"SELECT * FROM {FQ}.migration_status"), "wave", "score")
    wl = num(q(f"SELECT * FROM {FQ}.workloads"), "wave", "score", "task_count", "blockers", "auto_fixable", "monthly_dbus", "rank")
except Exception as e:  # noqa: BLE001
    st.error(f"No data yet - run the `smct_discover` job first. ({e})")
    st.stop()

tab = st.tabs(["Overview", "Fleet", "Approvals", "Validation", "Cost", "Schedule guard"])

# ------------------------------------------------------------------ overview
with tab[0]:
    total = len(wl)
    done = int((status["status"] == "PROMOTED").sum()) + int((wl["verdict"] == "ALREADY_SERVERLESS").sum())
    c = st.columns(5)
    c[0].metric("Workloads", total)
    c[1].metric("On serverless", f"{done} ({done / max(total, 1):.0%})")
    c[2].metric("Ready / auto-fixable", int(wl["verdict"].isin(["READY", "AUTO_FIXABLE", "PARTIAL"]).sum()))
    c[3].metric("Needs manual work", int((wl["verdict"] == "NEEDS_MANUAL").sum()))
    c[4].metric("Keep on classic", int((wl["verdict"] == "KEEP_CLASSIC").sum()))
    a, b = st.columns(2)
    a.subheader("Readiness verdicts")
    a.bar_chart(wl["verdict"].value_counts())
    b.subheader("Migration pipeline")
    b.bar_chart(status["status"].value_counts())
    st.subheader("Burn-down (every discovery run)")
    hist = num(q(f"SELECT * FROM {FQ}.scan_history ORDER BY scanned_at"), "total", "ready", "auto_fixable", "needs_manual", "keep_classic", "already_serverless", "promoted")
    if len(hist):
        st.line_chart(hist.set_index("scanned_at")[["ready", "auto_fixable", "needs_manual", "keep_classic", "already_serverless", "promoted"]])
    st.subheader("Wave plan (spend-ranked inside each wave)")
    st.dataframe(wl.sort_values("rank")[["rank", "wave_label", "name", "verdict", "score", "monthly_dbus", "limitations", "owner"]], use_container_width=True, hide_index=True)

# ------------------------------------------------------------------ fleet
with tab[1]:
    f1, f2 = st.columns(2)
    verdicts = f1.multiselect("Verdict", sorted(wl["verdict"].unique()), default=[])
    text = f2.text_input("Search name / owner / limitation (e.g. L6)")
    v = wl.merge(status[["workload_id", "status"]], on="workload_id", how="left")
    if verdicts:
        v = v[v["verdict"].isin(verdicts)]
    if text:
        m = v.apply(lambda r: text.lower() in (str(r["name"]) + str(r["owner"]) + str(r["limitations"])).lower(), axis=1)
        v = v[m]
    st.dataframe(v[["workload_id", "name", "kind", "wave", "score", "verdict", "status", "task_count", "limitations", "schedule", "owner"]], use_container_width=True, hide_index=True)
    pick = st.selectbox("Inspect workload", [""] + v["workload_id"].tolist(), format_func=lambda x: x or "-")
    if pick:
        fd = q(f"SELECT severity, rule_id, location, message, remediation, auto_fixable FROM {FQ}.findings WHERE workload_id = '{pick.replace(chr(39), '')}' ORDER BY severity, rule_id")
        st.dataframe(fd, use_container_width=True, hide_index=True)
        pt = q(f"SELECT path, applied_rules, patch FROM {FQ}.code_patches WHERE workload_id = '{pick.replace(chr(39), '')}' ORDER BY ts DESC")
        for _, r in pt.iterrows():
            with st.expander(f"Code patch: {r['path']}  ({r['applied_rules']})"):
                st.code(r["patch"], language="diff")

# ------------------------------------------------------------------ approvals
with tab[2]:
    st.markdown("**Two gates:** (1) approve *creating a serverless clone*, (2) approve *promoting* a clone that passed the A/B check. Every click is an audit event.")
    if not can_approve(me):
        st.warning("You are not in SMC_APPROVERS - read-only.")
    cand = status.merge(wl[["workload_id", "limitations"]], on="workload_id", how="left")
    cols = st.columns(2)
    with cols[0]:
        st.subheader("Gate 1 - approve clone")
        g1 = cand[cand["status"].isin(["DISCOVERED", "REJECTED", "MIGRATE_FAILED", "ROLLED_BACK", "VALIDATION_FAILED"]) & cand["verdict"].isin(["READY", "AUTO_FIXABLE", "PARTIAL"])]
        sel = st.multiselect("Eligible (READY / AUTO_FIXABLE / PARTIAL)", g1["workload_id"].tolist(), format_func=lambda x: f"{x}  {g1.set_index('workload_id').loc[x, 'name']}")
        if st.button("Approve selected for migration", disabled=not (sel and can_approve(me))):
            for wid in sel:
                add_event(wid, "APPROVED", me, {"gate": 1})
            st.success(f"{len(sel)} approved. Run job `smct_migrate_validate`.")
    with cols[1]:
        st.subheader("Gate 2 - approve promotion")
        g2 = cand[cand["status"] == "VALIDATED"]
        sel2 = st.multiselect("A/B validated", g2["workload_id"].tolist(), format_func=lambda x: f"{x}  {g2.set_index('workload_id').loc[x, 'name']}")
        if st.button("Approve promotion", disabled=not (sel2 and can_approve(me))):
            for wid in sel2:
                add_event(wid, "PROMOTE_APPROVED", me, {"gate": 2})
            st.success(f"{len(sel2)} approved. Run job `smct_promote`.")
        st.subheader("Rollback")
        g3 = cand[cand["status"] == "PROMOTED"]
        sel3 = st.multiselect("Promoted", g3["workload_id"].tolist(), format_func=lambda x: f"{x}  {g3.set_index('workload_id').loc[x, 'name']}")
        if st.button("Request rollback", disabled=not (sel3 and can_approve(me))):
            for wid in sel3:
                add_event(wid, "ROLLBACK_REQUESTED", me, {})
            st.success("Run job `smct_rollback`.")
    st.subheader("Audit trail")
    st.dataframe(q(f"SELECT ts, workload_id, event, actor, detail FROM {FQ}.migration_events ORDER BY ts DESC LIMIT 200"), use_container_width=True, hide_index=True)

# ------------------------------------------------------------------ validation
with tab[3]:
    vr = num(q(f"SELECT * FROM {FQ}.validation_results ORDER BY run_ts DESC"), "rows_a", "rows_b", "only_in_a", "only_in_b")
    st.dataframe(vr[["run_ts", "workload_id", "table_a", "table_b", "status", "rows_a", "rows_b", "only_in_a", "only_in_b", "reasons", "perf"]], use_container_width=True, hide_index=True)

# ------------------------------------------------------------------ cost
with tab[4]:
    try:
        cb = num(q(f"SELECT * FROM {FQ}.cost_before_after"), "dbus_before", "dbus_after", "usd_before", "usd_after", "runs_before", "runs_after")
        cb["usd_per_run_before"] = cb["usd_before"] / cb["runs_before"].replace(0, pd.NA)
        cb["usd_per_run_after"] = cb["usd_after"] / cb["runs_after"].replace(0, pd.NA)
        cb["delta_per_run_pct"] = (cb["usd_per_run_after"] / cb["usd_per_run_before"] - 1) * 100
        st.caption("List-price DBU cost from system.billing; classic excludes cloud VM cost (billed by your cloud). Compare per-run, not totals, while the post-migration window is still short.")
        st.dataframe(cb, use_container_width=True, hide_index=True)
        k = st.columns(3)
        k[0].metric("DBUs before (window)", f"{cb['dbus_before'].sum():,.0f}")
        k[1].metric("DBUs after", f"{cb['dbus_after'].sum():,.0f}")
        k[2].metric("Median $/run change", f"{cb['delta_per_run_pct'].median():+.1f}%" if len(cb) else "n/a")
        by_team = num(q(f"SELECT coalesce(team,'(untagged)') AS team, compute, sum(dbus) AS dbus, sum(list_cost_usd) AS usd FROM {FQ}.cost_by_job_day WHERE usage_date >= date_sub(current_date(), 30) GROUP BY ALL"), "dbus", "usd")
        st.subheader("Last 30 days by team")
        st.bar_chart(by_team.pivot_table(index="team", columns="compute", values="usd", aggfunc="sum").fillna(0))
    except Exception as e:  # noqa: BLE001
        st.info(f"Cost views unavailable - grant SELECT on system.billing and system.lakeflow, then run `smct_cost`. ({e})")

# ------------------------------------------------------------------ guard
with tab[5]:
    st.caption("The guard job pauses any serverless clone that has an active trigger while its original is also active (or already promoted).")
    st.dataframe(q(f"SELECT * FROM {FQ}.guard_log ORDER BY ts DESC LIMIT 200"), use_container_width=True, hide_index=True)
