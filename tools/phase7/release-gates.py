#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PROXY_ROOT = Path("/root/claude-code-proxy")


@dataclass(frozen=True)
class Gate:
    name: str
    command: tuple[str, ...]
    cwd: Path
    category: str


def parse_args():
    parser = argparse.ArgumentParser(description="Run Phase 7 release qualification gates and emit JSON evidence.")
    parser.add_argument("--include-proxy", action="store_true", help="Run the pinned claude-code-proxy contract gate.")
    parser.add_argument("--include-bridge-lab", action="store_true", help="Run the gVisor bridge durability lab.")
    parser.add_argument("--include-secret-lab", action="store_true", help="Run the rootful secret permission lab.")
    parser.add_argument("--include-live-latency", action="store_true", help="Run the live turn-start latency gate.")
    parser.add_argument("--output", default="", help="Optional path for the JSON evidence file.")
    parser.add_argument("--list", action="store_true", help="List selected gates without running them.")
    return parser.parse_args()


def deterministic_gates():
    return [
        Gate(
            name="go_phase7_packages",
            command=(
                "go",
                "test",
                "-count=1",
                "./internal/config",
                "./internal/store",
                "./internal/runtime",
                "./internal/bridge",
                "./internal/server",
                "./internal/events",
            ),
            cwd=REPO_ROOT / "orchestrator",
            category="deterministic",
        ),
        Gate(
            name="go_phase7_turn_start_latency_bench",
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
            name="python_phase7_tools_and_sandbox",
            command=(
                "python3",
                "-W",
                "error",
                "-m",
                "unittest",
                "sandbox-image/tests/test_harness_bridge_client.py",
                "tools/phase7/test_live_turn_start_latency.py",
                "tools/phase7/test_release_gates.py",
                "tools/phase7/test_secret_permission_bootstrap.py",
                "tools/phase7/test_secret_permission_lab.py",
            ),
            cwd=REPO_ROOT,
            category="deterministic",
        ),
    ]


def optional_gates(args):
    gates = []
    if args.include_proxy:
        gates.append(
            Gate(
                name="pinned_proxy_contract",
                command=(".venv/bin/python", "-m", "pytest", "-q", "tests/test_harness_probe_contract.py"),
                cwd=Path("/root/claude-code-proxy"),
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
    if args.include_secret_lab:
        gates.append(
            Gate(
                name="secret_permission_lab",
                command=("tools/phase7/secret-permission-lab.py",),
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


def load_release_config(config_path=REPO_ROOT / "config" / "harness.yaml"):
    targets = {
        "harness.max_sessions",
        "harness.network.cidr_pool",
        "harness.network.egress.dns_policy",
        "harness.events.emit_output_batch_max_rows",
        "harness.events.emit_output_batch_max_age",
        "harness.bridge.poll_interval",
        "harness.bridge.lease_ttl",
        "harness.bridge.ack_started_grace",
        "harness.secrets.root",
        "harness.secrets.readers_gid",
    }
    values = {}
    stack = []
    with open(config_path, encoding="utf-8") as handle:
        for raw in handle:
            line = raw.split("#", 1)[0].rstrip()
            if not line.strip():
                continue
            stripped = line.strip()
            if stripped.startswith("- ") or ":" not in stripped:
                continue
            indent = len(line) - len(line.lstrip(" "))
            while stack and indent <= stack[-1][0]:
                stack.pop()
            key, value = stripped.split(":", 1)
            value = value.strip().strip("'\"")
            path = ".".join([item[1] for item in stack] + [key])
            if value == "":
                stack.append((indent, key))
                continue
            if path in targets:
                values[path] = value
    return values


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
        "phase7_config": load_release_config(),
        "runsc_version": command_output(("runsc", "--version"), REPO_ROOT),
        "proxy": proxy_context(),
    }


def tail(text, limit=12000):
    if len(text) <= limit:
        return text
    return text[-limit:]


def attach_structured_output(result):
    if result["name"] in {"secret_permission_lab", "live_turn_start_latency"} and result["stdout_tail"].strip():
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
        return attach_structured_output({
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
        })
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


def evidence(results, commit=None, context=None):
    commit = commit or git_commit()
    status = "passed" if all(result["status"] == "passed" for result in results) else "failed"
    return {
        "phase": "phase7",
        "result": status,
        "commit": commit,
        "generated_at": utc_now(),
        "context": context or release_context(commit),
        "gates": results,
    }


def write_output(path, payload):
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main():
    args = parse_args()
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
    payload = evidence(run_gates(gates, env=os.environ.copy()))
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
        print(f"phase7 release gates failed: {err}", file=sys.stderr)
        raise SystemExit(1)
