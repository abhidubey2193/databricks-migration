"""Scan Jobs-API job settings and cluster specs: L2, L3, L4, L5, L7(jar), L9, L11."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from .code_scan import mk, scan_notebook, scan_python
from .initscripts import parse_init_script
from .models import Finding, WorkloadReport
from .rules import MIN_SERVERLESS_DBR
from .scoring import score_report

InitFetch = Callable[[str], Optional[str]]
NotebookFetch = Callable[[str], Optional[Tuple[str, str]]]   # path -> (source_text, language)

COMPUTE_TASK_KEYS = ("notebook_task", "spark_python_task", "python_wheel_task", "spark_jar_task", "spark_submit_task")
MAX_AGENT_TASKS = 10


@dataclass
class EnvSpec:
    dependencies: List[str] = field(default_factory=list)
    env_vars: Dict[str, str] = field(default_factory=dict)

    def merge(self, other: "EnvSpec") -> None:
        for d in other.dependencies:
            if d not in self.dependencies:
                self.dependencies.append(d)
        self.env_vars.update(other.env_vars)


def dbr_major(spark_version: str) -> Optional[int]:
    m = re.match(r"^(\d+)\.", spark_version or "")
    return int(m.group(1)) if m else None


def scan_libraries(libs: List[Dict[str, Any]], loc: str) -> Tuple[List[Finding], List[str]]:
    f: List[Finding] = []
    deps: List[str] = []
    for lib in libs or []:
        if "pypi" in lib:
            pk = lib["pypi"]
            deps.append(pk["package"] if isinstance(pk, dict) else str(pk))
        elif "whl" in lib:
            p = lib["whl"]
            if p.startswith(("/Volumes/", "/Workspace/", "dbfs:/Volumes/")):
                deps.append(p.replace("dbfs:/Volumes", "/Volumes"))
            else:
                f.append(mk("L5.WHL_LOCATION", loc, p))
        elif "requirements" in lib:
            deps.append(f"-r {lib['requirements']}")
        elif "egg" in lib:
            f.append(mk("L5.EGG", loc, lib["egg"]))
        elif "jar" in lib:
            f.append(mk("L5.JAR", loc, lib["jar"]))
        elif "maven" in lib:
            f.append(mk("L5.MAVEN", loc, str(lib["maven"].get("coordinates", lib["maven"]))))
        elif "cran" in lib:
            f.append(mk("L5.CRAN", loc, str(lib["cran"])))
    if deps:
        f.append(mk("L5.WHL_MAPPED", loc, ", ".join(deps)[:150]))
    return f, deps


def scan_cluster_spec(spec: Dict[str, Any], loc: str, init_fetch: Optional[InitFetch] = None) -> Tuple[List[Finding], EnvSpec]:
    f: List[Finding] = []
    env = EnvSpec()
    sv = spec.get("spark_version", "") or ""
    if spec.get("docker_image") or sv.startswith("custom:"):
        f.append(mk("L4.CUSTOM_IMAGE", loc, str((spec.get("docker_image") or {}).get("url", sv))))
    if "-ml" in sv or "gpu" in sv:
        f.append(mk("L4.ML_RUNTIME", loc, sv))
    major = dbr_major(sv)
    if major is not None and major < MIN_SERVERLESS_DBR:
        f.append(mk("L4.OLD_DBR", loc, sv))
    if sv.startswith("apache-spark-"):
        f.append(mk("L4.OLD_DBR", loc, sv))
    aws = spec.get("aws_attributes") or {}
    if aws.get("instance_profile_arn"):
        f.append(mk("L4.INSTANCE_PROFILE", loc, aws["instance_profile_arn"]))
    if (spec.get("gcp_attributes") or {}).get("google_service_account"):
        f.append(mk("L4.INSTANCE_PROFILE", loc, spec["gcp_attributes"]["google_service_account"],
                    message="Cluster runs as a Google service account for storage access."))
    conf = spec.get("spark_conf") or {}
    cred_keys = [k for k in conf if re.search(r"fs\.s3a\.(access|secret|aws\.credentials)|fs\.azure\.account\.(key|oauth)|google\.cloud\.auth|fs\.gs\.", k)]
    if cred_keys:
        f.append(mk("L4.STORAGE_CREDS_IN_CONF", loc, ", ".join(cred_keys[:3])))
    if conf:
        f.append(mk("L11.SPARK_CONF_DROPPED", loc, ", ".join(list(conf)[:4])))
    if spec.get("spark_env_vars"):
        f.append(mk("L11.ENV_VARS_DROPPED", loc, ", ".join(spec["spark_env_vars"])))
        env.env_vars.update({k: v for k, v in spec["spark_env_vars"].items() if "{{secrets" not in str(v)})
    if spec.get("cluster_log_conf"):
        f.append(mk("L11.LOG_DELIVERY", loc, str(list(spec["cluster_log_conf"]))))
    if spec.get("ssh_public_keys"):
        f.append(mk("L11.SSH", loc, f"{len(spec['ssh_public_keys'])} key(s)"))
    if spec.get("policy_id"):
        f.append(mk("L11.CLUSTER_POLICY", loc, spec["policy_id"]))
    for i, s in enumerate(spec.get("init_scripts") or []):
        dest = next((v.get("destination") for k, v in s.items() if isinstance(v, dict) and "destination" in v), None)
        dest = dest or next((v.get("destination") for v in s.values() if isinstance(v, dict)), "")
        text = init_fetch(dest) if (init_fetch and dest) else None
        iloc = f"{loc}:init[{i}] {dest}"
        if text is None:
            f.append(mk("L3.INIT_UNREADABLE", iloc, dest))
            continue
        res = parse_init_script(text)
        env.dependencies.extend(d for d in res.pip_deps if d not in env.dependencies)
        env.env_vars.update(res.env_vars)
        f.append(mk("L3.INIT_SCRIPT", iloc, ", ".join(res.pip_deps)[:150] or "(no pip lines)", auto_fixable=True))
        for no, line, why in res.unsupported:
            f.append(mk("L3.INIT_UNSUPPORTED_LINE", f"{iloc}:{no}", line, message=why))
    return f, env


def _schedule(settings: Dict[str, Any]) -> Tuple[str, bool]:
    if settings.get("continuous"):
        return "continuous", settings["continuous"].get("pause_status") == "PAUSED"
    if settings.get("schedule"):
        s = settings["schedule"]
        return s.get("quartz_cron_expression", ""), s.get("pause_status") == "PAUSED"
    if settings.get("trigger"):
        return "trigger", settings["trigger"].get("pause_status") == "PAUSED"
    return "", True


def scan_job(job: Dict[str, Any], clusters_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
             init_fetch: Optional[InitFetch] = None, notebook_fetch: Optional[NotebookFetch] = None,
             mount_map: Optional[Dict[str, str]] = None) -> WorkloadReport:
    clusters_by_id = clusters_by_id or {}
    s = job.get("settings", job)
    jid = job.get("job_id", s.get("job_id", "?"))
    sched, paused = _schedule(s)
    tasks = s.get("tasks", []) or []
    rep = WorkloadReport(workload_id=f"job:{jid}", kind="job", name=s.get("name", f"job-{jid}"),
                         owner=job.get("creator_user_name", job.get("run_as_user_name", "")),
                         tags=s.get("tags", {}) or {}, schedule=sched, paused=paused, task_count=len(tasks))
    jc = {c["job_cluster_key"]: c.get("new_cluster", {}) for c in s.get("job_clusters", []) or []}
    findings: List[Finding] = []
    seen_clusters = set()
    classic_tasks = 0
    deps_total = EnvSpec()
    for t in tasks:
        key = t.get("task_key", "?")
        spec = None
        if t.get("new_cluster"):
            spec, ckey = t["new_cluster"], f"task:{key}"
        elif t.get("job_cluster_key"):
            spec, ckey = jc.get(t["job_cluster_key"], {}), f"job_cluster:{t['job_cluster_key']}"
        elif t.get("existing_cluster_id"):
            spec, ckey = clusters_by_id.get(t["existing_cluster_id"]), f"cluster:{t['existing_cluster_id']}"
        else:
            ckey = ""
        is_compute = any(k in t for k in COMPUTE_TASK_KEYS)
        if spec is not None or ckey:
            classic_tasks += 1 if is_compute else 0
        if ckey and ckey not in seen_clusters:
            seen_clusters.add(ckey)
            if spec:
                cf, env = scan_cluster_spec(spec, ckey, init_fetch)
                findings.extend(cf)
                deps_total.merge(env)
        lf, _ = scan_libraries(t.get("libraries", []), f"task:{key}")
        findings.extend(lf)
        if "spark_jar_task" in t:
            findings.append(mk("L7.JAR_TASK", f"task:{key}", str(t["spark_jar_task"].get("main_class_name", ""))))
        if "spark_submit_task" in t:
            findings.append(mk("L4.SPARK_SUBMIT", f"task:{key}"))
        if "notebook_task" in t and notebook_fetch and not s.get("git_source"):
            got = notebook_fetch(t["notebook_task"].get("notebook_path", ""))
            if got:
                nf, _ = scan_notebook(t["notebook_task"]["notebook_path"], got[0], got[1], mount_map)
                for x in nf:
                    x.location = f"task:{key} {x.location}"
                findings.extend(nf)
        if "spark_python_task" in t and notebook_fetch and not s.get("git_source"):
            got = notebook_fetch(t["spark_python_task"].get("python_file", ""))
            if got:
                for x in scan_python(got[0], t["spark_python_task"]["python_file"], 1, mount_map):
                    x.location = f"task:{key} {x.location}"
                    findings.append(x)
    if classic_tasks > MAX_AGENT_TASKS:
        findings.append(mk("L2.LARGE_JOB", "", f"{classic_tasks} migratable tasks"))
    if (s.get("deployment") or {}).get("kind") == "BUNDLE":
        findings.append(mk("L9.BUNDLE_MANAGED", "", (s.get("deployment") or {}).get("metadata_file_path", "")))
    if s.get("git_source"):
        findings.append(mk("L9.GIT_SOURCE", "", s["git_source"].get("git_url", "")))
    # dedupe
    uniq, seen = [], set()
    for x in findings:
        k = (x.rule_id, x.location, x.snippet)
        if k not in seen:
            seen.add(k)
            uniq.append(x)
    rep.findings = uniq
    rep.tags = dict(rep.tags)
    if classic_tasks == 0 and tasks:
        rep.verdict, rep.score, rep.wave = "ALREADY_SERVERLESS", 100, -1
    else:
        score_report(rep)
        # mixed job: JVM tasks stay on classic, everything else can move (compute may be mixed inside one job)
        jvm = {"L7.JAR_TASK", "L4.SPARK_SUBMIT"}
        hard = [f for f in rep.findings if f.keep_on_classic]
        if rep.verdict == "KEEP_CLASSIC" and hard and all(f.rule_id in jvm for f in hard) and classic_tasks > len(hard):
            rep.verdict, rep.wave = "PARTIAL", 1
    return rep


def scan_standalone_cluster(c: Dict[str, Any], init_fetch: Optional[InitFetch] = None) -> WorkloadReport:
    cid = c.get("cluster_id", "?")
    rep = WorkloadReport(workload_id=f"cluster:{cid}", kind="cluster", name=c.get("cluster_name", cid),
                         owner=c.get("creator_user_name", ""), tags=c.get("custom_tags", {}) or {})
    rep.findings, _ = scan_cluster_spec(c, f"cluster:{cid}", init_fetch)
    score_report(rep)
    return rep
