"""L3: parse cluster init scripts and translate what can be translated into a serverless environment spec."""
from __future__ import annotations

import re
import shlex
from dataclasses import dataclass, field
from typing import Dict, List, Tuple


@dataclass
class InitScriptResult:
    pip_deps: List[str] = field(default_factory=list)
    env_vars: Dict[str, str] = field(default_factory=dict)
    jars: List[str] = field(default_factory=list)
    unsupported: List[Tuple[int, str, str]] = field(default_factory=list)  # (line_no, line, reason)

    @property
    def fully_translatable(self) -> bool:
        return not self.unsupported and not self.jars


PIP_RE = re.compile(r"^(?:sudo\s+)?(?:/databricks/python\d?/bin/)?(?:python\d?\s+-m\s+)?pip\d?\s+install\s+(.*)$")


def parse_init_script(text: str) -> InitScriptResult:
    res = InitScriptResult()
    # join backslash continuations
    joined = re.sub(r"\\\n\s*", " ", text)
    for no, raw in enumerate(joined.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#") or line in ("set -e", "set -ex", "set -x") or line.startswith("set -"):
            continue
        m = PIP_RE.match(line)
        if m:
            try:
                toks = shlex.split(m.group(1))
            except ValueError:
                toks = m.group(1).split()
            skip = False
            for i, t in enumerate(toks):
                if skip:
                    skip = False
                    continue
                if t in ("-r", "--requirement") and i + 1 < len(toks):
                    res.pip_deps.append(f"-r {toks[i + 1]}")
                    skip = True
                elif t.startswith("-"):
                    if t in ("-q", "--quiet", "--no-cache-dir", "--upgrade", "-U", "--no-input", "--disable-pip-version-check"):
                        continue
                    res.unsupported.append((no, line, f"pip option {t} cannot be expressed in an environment spec"))
                    if t in ("--index-url", "-i", "--extra-index-url", "--find-links", "-f") and i + 1 < len(toks):
                        skip = True
                else:
                    res.pip_deps.append(t)
            continue
        if re.match(r"^(sudo\s+)?(apt-get|apt|yum|dnf|apk)\b", line):
            res.unsupported.append((no, line, "OS package install is impossible on serverless"))
            continue
        if re.search(r"/databricks/jars/", line):
            res.jars.append(line)
            res.unsupported.append((no, line, "JAR placed on the driver classpath - no JVM classpath control on serverless"))
            continue
        em = re.match(r"^export\s+([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
        if em:
            res.env_vars[em.group(1)] = em.group(2).strip("'\"")
            continue
        if re.match(r"^(wget|curl)\b", line):
            res.unsupported.append((no, line, "downloads to local disk at cluster start have no serverless hook"))
            continue
        if re.match(r"^(mkdir|chmod|chown|ln|mv|cp|echo|cat|tee|sed)\b", line):
            res.unsupported.append((no, line, "filesystem/OS mutation at cluster start has no serverless equivalent"))
            continue
        res.unsupported.append((no, line, "unrecognised command"))
    # de-dup deps keeping order
    seen, deps = set(), []
    for d in res.pip_deps:
        if d not in seen:
            seen.add(d)
            deps.append(d)
    res.pip_deps = deps
    return res
