"""Thin Databricks SDK wrapper. All SDK imports are lazy so the core library stays importable offline."""
from __future__ import annotations

import base64
import dataclasses
import io
import json
from datetime import timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple


class Workspace:
    def __init__(self, client=None):
        if client is None:
            from databricks.sdk import WorkspaceClient
            client = WorkspaceClient()
        self.w = client

    # ---------------------------------------------------------------- inventory
    def list_jobs(self) -> List[Dict[str, Any]]:
        return [j.as_dict() for j in self.w.jobs.list(expand_tasks=True)]

    def get_job(self, job_id: int) -> Dict[str, Any]:
        return self.w.jobs.get(job_id=job_id).as_dict()

    def all_purpose_clusters(self) -> Dict[str, Dict[str, Any]]:
        out = {}
        for c in self.w.clusters.list():
            d = c.as_dict()
            if d.get("cluster_source") in ("UI", "API", None):
                out[d["cluster_id"]] = d
        return out

    def me(self) -> str:
        return self.w.current_user.me().user_name

    # ---------------------------------------------------------------- file access
    def fetch_notebook(self, path: str) -> Optional[Tuple[str, str]]:
        """(source_text, language) for a notebook or workspace python file; None if unreadable."""
        try:
            from databricks.sdk.service.workspace import ExportFormat, ObjectType
            st = self.w.workspace.get_status(path)
            if st.object_type == ObjectType.NOTEBOOK:
                raw = self.w.workspace.export(path, format=ExportFormat.SOURCE).content
                return base64.b64decode(raw).decode("utf-8", "replace"), (st.language.value if st.language else "PYTHON").lower()
            with self.w.workspace.download(path) as f:
                return f.read().decode("utf-8", "replace"), "python"
        except Exception:  # noqa: BLE001
            return None

    def fetch_init_script(self, dest: str) -> Optional[str]:
        try:
            if dest.startswith("dbfs:"):
                with self.w.dbfs.open(dest[5:], read=True) as f:
                    return f.read().decode("utf-8", "replace")
            if dest.startswith("/Volumes/"):
                return self.w.files.download(dest).contents.read().decode("utf-8", "replace")
            if dest.startswith(("/Workspace/", "/Users/", "/Shared/")):
                with self.w.workspace.download(dest if dest.startswith("/Workspace") else "/Workspace" + dest) as f:
                    return f.read().decode("utf-8", "replace")
            if dest.startswith("s3://"):
                import boto3  # optional dependency
                bucket, key = dest[5:].split("/", 1)
                return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            return None
        return None

    def import_notebook(self, path: str, text: str, language: str = "python") -> None:
        from databricks.sdk.service.workspace import ImportFormat, Language
        self.w.workspace.mkdirs(path.rsplit("/", 1)[0])
        self.w.workspace.upload(path, io.BytesIO(text.encode()), format=ImportFormat.SOURCE,
                                language=Language[language.upper()], overwrite=True)

    def write_text(self, path: str, text: str) -> None:
        from databricks.sdk.service.workspace import ImportFormat
        self.w.workspace.mkdirs(path.rsplit("/", 1)[0])
        self.w.workspace.upload(path, io.BytesIO(text.encode()), format=ImportFormat.RAW, overwrite=True)

    def read_text(self, path: str) -> str:
        with self.w.workspace.download(path) as f:
            return f.read().decode()

    # ---------------------------------------------------------------- jobs
    @staticmethod
    def _settings(d: Dict[str, Any]):
        from databricks.sdk.service.jobs import JobSettings
        return JobSettings.from_dict(d)

    def create_job(self, settings: Dict[str, Any]) -> int:
        import inspect
        js = self._settings(settings)
        allowed = set(inspect.signature(self.w.jobs.create).parameters)
        kw = {f.name: getattr(js, f.name) for f in dataclasses.fields(js)
              if getattr(js, f.name) is not None and f.name in allowed}
        return self.w.jobs.create(**kw).job_id

    def reset_job(self, job_id: int, settings: Dict[str, Any]) -> None:
        self.w.jobs.reset(job_id=job_id, new_settings=self._settings(settings))

    def update_job(self, job_id: int, partial: Dict[str, Any]) -> None:
        self.w.jobs.update(job_id=job_id, new_settings=self._settings(partial))

    def run_and_wait(self, job_id: int, params: Optional[Dict[str, str]] = None, timeout_h: int = 3) -> Dict[str, Any]:
        wait = self.w.jobs.run_now(job_id=job_id, job_parameters=params or None)
        run = wait.result(timeout=timedelta(hours=timeout_h))
        res = run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN"
        dur = ((run.end_time or 0) - (run.start_time or 0)) / 1000.0
        return {"run_id": str(run.run_id), "result": res, "seconds": dur}
