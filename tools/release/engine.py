#!/usr/bin/env python3
"""Generic release-gate engine.

This module is intentionally phase-agnostic: it contains no gate data, no
evidence labels, and no phase identifiers. Concrete release suites under
``tools/release/suites/`` declare their gates, evidence model, and static
checks and assemble their own evidence envelope from these primitives.
"""
import hashlib
import json
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class Gate:
    name: str
    command: tuple[str, ...]
    cwd: Path
    category: str


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def git_commit(repo_root):
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout.strip()


def command_output(command, cwd):
    try:
        result = subprocess.run(
            list(command),
            cwd=cwd,
            text=True,
            capture_output=True,
        )
    except FileNotFoundError as err:
        return {"ok": False, "returncode": 127, "output": str(err)}
    output = (result.stdout + result.stderr).strip()
    return {"ok": result.returncode == 0, "returncode": result.returncode, "output": output}


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def runsc_context(repo_root):
    path = shutil.which("runsc")
    context = {
        "version": command_output(("runsc", "--version"), repo_root),
        "binary_path": path or "",
        "binary_digest": "",
    }
    if path:
        try:
            context["binary_digest"] = sha256_file(path)
        except OSError as err:
            context["binary_digest_error"] = str(err)
    return context


def proxy_context(proxy_root):
    if not proxy_root.exists():
        return {"path": str(proxy_root), "present": False}
    commit = command_output(("git", "rev-parse", "HEAD"), proxy_root)
    status = command_output(("git", "status", "--short"), proxy_root)
    return {
        "path": str(proxy_root),
        "present": True,
        "commit": commit["output"] if commit["ok"] else "",
        "dirty": bool(status["output"]) if status["ok"] else None,
        "status_short": status["output"].splitlines() if status["ok"] and status["output"] else [],
    }


def git_context(repo_root, commit=None):
    status = command_output(("git", "status", "--short"), repo_root)
    return {
        "commit": commit or git_commit(repo_root),
        "dirty": bool(status["output"]) if status["ok"] else None,
        "status_short": status["output"].splitlines() if status["ok"] and status["output"] else [],
    }


def slug(value):
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def gate_inventory(path):
    section = "Preamble"
    buckets = {}
    current = None
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        if raw.startswith("## "):
            if current:
                buckets.setdefault(section, []).append(" ".join(current))
                current = None
            section = raw[3:].strip()
            continue
        if raw.startswith("- "):
            if current:
                buckets.setdefault(section, []).append(" ".join(current))
            current = [raw[2:].strip()]
            continue
        if current and (raw.startswith("  ") or not raw.strip()):
            stripped = raw.strip()
            if stripped:
                current.append(stripped)
    if current:
        buckets.setdefault(section, []).append(" ".join(current))

    gates = []
    for section_name, bullets in buckets.items():
        section_slug = slug(section_name)
        for index, text in enumerate(bullets, start=1):
            gates.append(
                {
                    "id": f"{section_slug}_{index:03d}",
                    "section": section_name,
                    "text": text,
                }
            )
    counts = {}
    for item in gates:
        counts[item["section"]] = counts.get(item["section"], 0) + 1
    return {"path": str(path), "total": len(gates), "counts_by_section": counts, "gates": gates}


def tail(text, limit=12000):
    if len(text) <= limit:
        return text
    return text[-limit:]


def attach_structured_output(result, json_output_gates=(), evidence_file_gates=()):
    if result["name"] in json_output_gates and result["stdout_tail"].strip():
        try:
            result["structured_output"] = json.loads(result["stdout_tail"])
        except json.JSONDecodeError:
            pass
    if result["name"] in evidence_file_gates:
        for raw in reversed(result["stdout_tail"].splitlines()):
            path = raw.strip()
            if not path.endswith("/evidence.json"):
                continue
            evidence_path = Path(path)
            result["evidence_path"] = str(evidence_path)
            if evidence_path.is_file():
                try:
                    result["structured_output"] = json.loads(evidence_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    result["structured_output_error"] = "invalid evidence json"
            break
    return result


def run_gate(gate, env=None, attach=None):
    started = utc_now()
    start = time.monotonic()
    try:
        result = subprocess.run(
            list(gate.command),
            cwd=gate.cwd,
            env=env,
            text=True,
            capture_output=True,
        )
        payload = {
            "name": gate.name,
            "category": gate.category,
            "command": list(gate.command),
            "cwd": str(gate.cwd),
            "started_at": started,
            "duration_ms": round((time.monotonic() - start) * 1000),
            "returncode": result.returncode,
            "status": "passed" if result.returncode == 0 else "failed",
            "stdout_tail": tail(result.stdout),
            "stderr_tail": tail(result.stderr),
        }
        return attach(payload) if attach else payload
    except FileNotFoundError as err:
        return {
            "name": gate.name,
            "category": gate.category,
            "command": list(gate.command),
            "cwd": str(gate.cwd),
            "started_at": started,
            "duration_ms": round((time.monotonic() - start) * 1000),
            "returncode": 127,
            "status": "failed",
            "stdout_tail": "",
            "stderr_tail": str(err),
        }


def run_gates(gates, env=None, attach=None):
    return [run_gate(gate, env=env, attach=attach) for gate in gates]


def render_gate_list(gates):
    return [
        {
            "name": gate.name,
            "category": gate.category,
            "command": list(gate.command),
            "cwd": str(gate.cwd),
        }
        for gate in gates
    ]


def parse_supplied_evidence(specs):
    supplied = {}
    for spec in specs:
        if "=" not in spec:
            raise ValueError(f"evidence must be LABEL=PATH, got {spec!r}")
        label, raw_path = spec.split("=", 1)
        label = label.strip()
        if not label:
            raise ValueError("evidence label is required")
        path = Path(raw_path).expanduser()
        if not path.is_file():
            raise FileNotFoundError(f"evidence {label} path is not a file: {path}")
        data = path.read_bytes()
        try:
            parsed = json.loads(data.decode("utf-8"))
        except json.JSONDecodeError as err:
            raise ValueError(f"evidence {label} is not valid JSON: {err}") from err
        supplied[label] = {
            "path": str(path),
            "digest": "sha256:" + hashlib.sha256(data).hexdigest(),
            "bytes": len(data),
            "status": parsed.get("result", parsed.get("status", "")) if isinstance(parsed, dict) else "",
            "payload": parsed,
        }
    return supplied


def release_completion(results, supplied_evidence, required_labels, note_incomplete, note_complete, require_release_evidence=False):
    selected_passed = all(result["status"] == "passed" for result in results)
    missing = [label for label in required_labels if label not in supplied_evidence]
    release_complete = bool(require_release_evidence and selected_passed and not missing)
    note = note_complete if release_complete else note_incomplete
    return {
        "selected_gates_passed": selected_passed,
        "release_complete": release_complete,
        "required_supplied_evidence": list(required_labels),
        "missing_supplied_evidence": missing,
        "note": note,
    }


def write_output(path, payload):
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def check_file_lacks(path, patterns):
    text = Path(path).read_text(encoding="utf-8")
    failures = []
    for label, pattern in patterns:
        if pattern in text:
            failures.append({"label": label, "pattern": pattern})
    return {
        "path": str(path),
        "status": "passed" if not failures else "failed",
        "failures": failures,
    }


def check_file_contains(path, patterns):
    text = Path(path).read_text(encoding="utf-8")
    failures = []
    for label, pattern in patterns:
        if pattern not in text:
            failures.append({"label": label, "pattern": pattern})
    return {
        "path": str(path),
        "status": "passed" if not failures else "failed",
        "failures": failures,
    }


def run_static_checks(specs):
    """Execute a list of static-check specs.

    Each spec is a dict: {"name", "kind": "lacks"|"contains", "path", "patterns"}.
    Returns {"status", "checks": [{"name", "path", "status", "failures"}]}.
    """
    checks = []
    for spec in specs:
        if spec["kind"] == "lacks":
            result = check_file_lacks(spec["path"], spec["patterns"])
        elif spec["kind"] == "contains":
            result = check_file_contains(spec["path"], spec["patterns"])
        else:
            raise ValueError(f"unknown static check kind: {spec['kind']!r}")
        checks.append({"name": spec["name"], **result})
    status = "passed" if all(check["status"] == "passed" for check in checks) else "failed"
    return {"status": status, "checks": checks}
