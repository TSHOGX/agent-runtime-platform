#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PROXY_ROOT = Path(os.environ.get("HARNESS_PROXY_ROOT", "/root/claude-code-proxy"))
RELEASE_GATES_DOC = REPO_ROOT / "docs" / "phase8" / "release-gates.md"

REQUIRED_SUPPLIED_EVIDENCE = (
    "cutover",
    "reconciliation",
    "rootfs_image",
    "proxy_contract",
    "adversarial_lab",
)


@dataclass(frozen=True)
class Gate:
    name: str
    command: tuple[str, ...]
    cwd: Path
    category: str


def parse_args():
    parser = argparse.ArgumentParser(description="Run runtime isolation release qualification gates and emit JSON evidence.")
    parser.add_argument("--include-prior-release", action="store_true", help="Run the prior deterministic release runner.")
    parser.add_argument("--include-rootfs-inspection", action="store_true", help="Inspect the configured sandbox rootfs image.")
    parser.add_argument("--include-proxy", action="store_true", help="Run the pinned claude-code-proxy contract gate.")
    parser.add_argument("--include-bridge-lab", action="store_true", help="Run the gVisor bridge durability lab.")
    parser.add_argument("--include-live-latency", action="store_true", help="Run the live turn-start latency gate.")
    parser.add_argument(
        "--evidence",
        action="append",
        default=[],
        metavar="LABEL=PATH",
        help="Attach supplied JSON evidence. Required release labels: "
        + ", ".join(REQUIRED_SUPPLIED_EVIDENCE),
    )
    parser.add_argument(
        "--require-release-evidence",
        action="store_true",
        help="Fail unless every required supplied evidence label is attached.",
    )
    parser.add_argument("--output", default="", help="Optional path for the JSON evidence file.")
    parser.add_argument("--list", action="store_true", help="List selected gates without running them.")
    parser.add_argument("--static-only", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args()


def deterministic_gates():
    return [
        Gate(
            name="go_runtime_isolation_packages",
            command=("go", "test", "-count=1", "./..."),
            cwd=REPO_ROOT / "orchestrator",
            category="deterministic",
        ),
        Gate(
            name="go_turn_start_latency_bench",
            command=(
                "go",
                "test",
                "-tags",
                "phase7bench",
                "-count=1",
                "./internal/server",
                "-run",
                "TestPhase7TurnStartLatencyGate",
            ),
            cwd=REPO_ROOT / "orchestrator",
            category="deterministic",
        ),
        Gate(
            name="python_sandbox_and_release_tools",
            command=(
                "python3",
                "-W",
                "error",
                "-m",
                "unittest",
                "sandbox-image/tests/test_harness_bridge_client.py",
                "tools/phase8/test_release_gates.py",
                "tools/phase8/test_rootfs_inspect.py",
            ),
            cwd=REPO_ROOT,
            category="deterministic",
        ),
        Gate(
            name="runtime_isolation_static_release_scans",
            command=("python3", "tools/phase8/release-gates.py", "--static-only"),
            cwd=REPO_ROOT,
            category="deterministic",
        ),
    ]


def optional_gates(args):
    gates = []
    if args.include_prior_release:
        gates.append(
            Gate(
                name="prior_deterministic_release_runner",
                command=("tools/phase7/release-gates.py",),
                cwd=REPO_ROOT,
                category="compatibility",
            )
        )
    if args.include_rootfs_inspection:
        gates.append(
            Gate(
                name="rootfs_image_inspection",
                command=("tools/phase8/rootfs-inspect.py",),
                cwd=REPO_ROOT,
                category="evidence",
            )
        )
    if args.include_proxy:
        gates.append(
            Gate(
                name="pinned_proxy_contract",
                command=(".venv/bin/python", "-m", "pytest", "-q", "tests/test_harness_probe_contract.py"),
                cwd=PROXY_ROOT,
                category="external",
            )
        )
    if args.include_bridge_lab:
        gates.append(
            Gate(
                name="gvisor_bridge_durability_lab",
                command=("tools/phase7/bridge-durability-lab.sh",),
                cwd=REPO_ROOT,
                category="external",
            )
        )
    if args.include_live_latency:
        gates.append(
            Gate(
                name="live_turn_start_latency",
                command=("tools/phase7/live-turn-start-latency.py",),
                cwd=REPO_ROOT,
                category="external",
            )
        )
    return gates


def selected_gates(args):
    return deterministic_gates() + optional_gates(args)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def git_commit():
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
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


def runsc_context():
    path = shutil.which("runsc")
    context = {
        "version": command_output(("runsc", "--version"), REPO_ROOT),
        "binary_path": path or "",
        "binary_digest": "",
    }
    if path:
        try:
            context["binary_digest"] = sha256_file(path)
        except OSError as err:
            context["binary_digest_error"] = str(err)
    return context


def proxy_context(proxy_root=PROXY_ROOT):
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


def release_context(commit=None):
    status = command_output(("git", "status", "--short"), REPO_ROOT)
    return {
        "repo_root": str(REPO_ROOT),
        "git": {
            "commit": commit or git_commit(),
            "dirty": bool(status["output"]) if status["ok"] else None,
            "status_short": status["output"].splitlines() if status["ok"] and status["output"] else [],
        },
        "runsc": runsc_context(),
        "proxy": proxy_context(),
        "release_gates_doc": str(RELEASE_GATES_DOC),
    }


def slug(value):
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def release_gate_inventory(path=RELEASE_GATES_DOC):
    section = "Preamble"
    buckets = {}
    current = None
    for raw in path.read_text(encoding="utf-8").splitlines():
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


def attach_structured_output(result):
    if result["name"] in {"live_turn_start_latency", "rootfs_image_inspection"} and result["stdout_tail"].strip():
        try:
            result["structured_output"] = json.loads(result["stdout_tail"])
        except json.JSONDecodeError:
            pass
    if result["name"] == "gvisor_bridge_durability_lab":
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


def run_gate(gate, env=None):
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
        return attach_structured_output(
            {
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
        )
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


def run_gates(gates, env=None):
    return [run_gate(gate, env=env) for gate in gates]


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


def release_completion(results, supplied_evidence, require_release_evidence=False):
    selected_passed = all(result["status"] == "passed" for result in results)
    missing = [label for label in REQUIRED_SUPPLIED_EVIDENCE if label not in supplied_evidence]
    release_complete = bool(require_release_evidence and selected_passed and not missing)
    note = "Runtime isolation release requires target-lab adversarial evidence for every gate in docs/phase8/release-gates.md."
    if release_complete:
        note = "Selected gates passed and all required supplied evidence labels were attached."
    return {
        "selected_gates_passed": selected_passed,
        "release_complete": release_complete,
        "required_supplied_evidence": list(REQUIRED_SUPPLIED_EVIDENCE),
        "missing_supplied_evidence": missing,
        "note": note,
    }


def supplied_evidence_from_gate_results(results):
    supplied = {}
    for result in results:
        if result["name"] != "rootfs_image_inspection" or result["status"] != "passed":
            continue
        payload = result.get("structured_output")
        if not isinstance(payload, dict):
            continue
        supplied["rootfs_image"] = {
            "path": "gate:rootfs_image_inspection",
            "digest": payload.get("rootfs_digest", ""),
            "bytes": 0,
            "status": payload.get("status", ""),
            "payload": payload,
        }
    return supplied


def evidence(results, commit=None, context=None, supplied_evidence=None, require_release_evidence=False):
    commit = commit or git_commit()
    supplied_evidence = {**supplied_evidence_from_gate_results(results), **(supplied_evidence or {})}
    completion = release_completion(results, supplied_evidence, require_release_evidence)
    status = "passed" if completion["selected_gates_passed"] else "failed"
    if require_release_evidence and completion["missing_supplied_evidence"]:
        status = "failed"
    return {
        "contract": "sandbox-isolation-v1",
        "qualification": "runtime-isolation",
        "result": status,
        "commit": commit,
        "generated_at": utc_now(),
        "context": context or release_context(commit),
        "release_completion": completion,
        "release_gate_inventory": release_gate_inventory(),
        "supplied_evidence": supplied_evidence,
        "gates": results,
    }


def write_output(path, payload):
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def check_file_lacks(path, patterns):
    text = path.read_text(encoding="utf-8")
    failures = []
    for label, pattern in patterns:
        if pattern in text:
            failures.append({"label": label, "pattern": pattern})
    return {
        "path": str(path),
        "status": "passed" if not failures else "failed",
        "failures": failures,
    }


def static_checks():
    checks = [
        {
            "name": "current_docs_do_not_claim_parent_session_mounts",
            **check_file_lacks(
                REPO_ROOT / "docs" / "PLAN.md",
                (
                    ("obsolete_parent_mount_boundary", "Until Phase 8 lands, the sandbox reaches"),
                    ("obsolete_parent_mount_target", "parent `/sessions` and `/agent-homes` mounts"),
                ),
            ),
        },
        {
            "name": "current_status_uses_state_db_default",
            **check_file_lacks(
                REPO_ROOT / "docs" / "current-status.md",
                (("obsolete_db_under_sessions", "/var/lib/harness/sessions/orchestrator.db"),),
            ),
        },
        {
            "name": "bridge_client_has_no_pre_turn_model_probe_config",
            **check_file_lacks(
                REPO_ROOT / "sandbox-image" / "files" / "usr" / "local" / "bin" / "harness-bridge-client",
                (("pre_turn_model_probe_status_env", "HARNESS_PROBE_MESSAGE_STATUSES"),),
            ),
        },
        {
            "name": "frontend_session_types_hide_legacy_host_fields",
            **check_file_lacks(
                REPO_ROOT / "frontend" / "lib" / "types.ts",
                (
                    ("agent_home_path", "agent_home_path"),
                    ("restore_id", "restore_id"),
                ),
            ),
        },
        {
            "name": "phase9_skills_docs_use_exact_bind_prerequisite",
            **check_file_lacks(
                REPO_ROOT / "docs" / "phase9" / "system-skills-mount.md",
                (
                    ("workspace_symlink_to_sessions", "`/workspace` is a symlink to `/sessions/<session_id>`"),
                    ("agent_home_parent_root", "`/agent-homes/<session_id>`"),
                    ("legacy_mount_centralization", "Runtime spec generation already centralizes mounts"),
                ),
            ),
        },
        {
            "name": "phase9_managed_settings_do_not_reference_live_secret_mount",
            **check_file_lacks(
                REPO_ROOT / "docs" / "phase9" / "managed-settings.md",
                (("existing_model_provider_secret_mount", "existing model-provider `/harness-secrets` mount"),),
            ),
        },
    ]
    status = "passed" if all(check["status"] == "passed" for check in checks) else "failed"
    return {"status": status, "checks": checks}


def run_static_only():
    payload = static_checks()
    print(json.dumps(payload, indent=2))
    if payload["status"] != "passed":
        raise SystemExit(1)


def main():
    args = parse_args()
    if args.static_only:
        run_static_only()
        return

    gates = selected_gates(args)
    if args.list:
        listed = [
            {
                "name": gate.name,
                "category": gate.category,
                "command": list(gate.command),
                "cwd": str(gate.cwd),
            }
            for gate in gates
        ]
        print(json.dumps(listed, indent=2))
        return

    supplied = parse_supplied_evidence(args.evidence)
    results = run_gates(gates, env=os.environ.copy())
    payload = evidence(
        results,
        supplied_evidence=supplied,
        require_release_evidence=args.require_release_evidence,
    )
    rendered = json.dumps(payload, indent=2)
    print(rendered)
    if args.output:
        write_output(args.output, payload)
    if payload["result"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as err:
        print(f"runtime isolation release gates failed: {err}", file=sys.stderr)
        raise SystemExit(1)
