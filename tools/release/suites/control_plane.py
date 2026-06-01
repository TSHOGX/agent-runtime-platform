#!/usr/bin/env python3
"""Control-plane release suite.

Owns the durable control-plane qualification gates: orchestrator package
tests, the turn-start latency bench, the pinned proxy contract, the gVisor
bridge durability lab, and live latency.
"""
import argparse
import json
import os
import sys
from pathlib import Path

from tools.release import engine
from tools.release.engine import Gate

REPO_ROOT = Path(__file__).resolve().parents[3]
PROXY_ROOT = Path("/root/claude-code-proxy")

SUITE = "control_plane"
FAILURE_BANNER = "control-plane release gates failed"

JSON_OUTPUT_GATES = {"live_turn_start_latency"}
EVIDENCE_FILE_GATES = {"gvisor_bridge_durability_lab"}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run control-plane release qualification gates and emit JSON evidence.")
    parser.add_argument("--include-proxy", action="store_true", help="Run the pinned claude-code-proxy contract gate.")
    parser.add_argument("--include-bridge-lab", action="store_true", help="Run the gVisor bridge durability lab.")
    parser.add_argument("--include-live-latency", action="store_true", help="Run the live turn-start latency gate.")
    parser.add_argument("--output", default="", help="Optional path for the JSON evidence file.")
    parser.add_argument("--list", action="store_true", help="List selected gates without running them.")
    return parser.parse_args(argv)


def deterministic_gates():
    return [
        Gate(
            name="go_orchestrator_packages",
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
            name="go_turn_start_latency_bench",
            command=(
                "go",
                "test",
                "-tags",
                "controlplanebench",
                "-count=1",
                "./internal/server",
                "-run",
                "TestTurnStartLatencyGate",
            ),
            cwd=REPO_ROOT / "orchestrator",
            category="deterministic",
        ),
        Gate(
            name="python_control_plane_tools_and_sandbox",
            command=(
                "python3",
                "-W",
                "error",
                "-m",
                "unittest",
                "sandbox-image/tests/test_harness_bridge_client.py",
                "tools/release/gates/control_plane/test_live_turn_start_latency.py",
                "tools/release/gates/control_plane/test_release_gates.py",
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
                command=("tools/release/gates/control_plane/bridge-durability-lab.sh",),
                cwd=REPO_ROOT,
                category="external",
            )
        )
    if args.include_live_latency:
        gates.append(
            Gate(
                name="live_turn_start_latency",
                command=("tools/release/gates/control_plane/live-turn-start-latency.py",),
                cwd=REPO_ROOT,
                category="external",
            )
        )
    return gates


def selected_gates(args):
    return deterministic_gates() + optional_gates(args)


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
    return engine.proxy_context(proxy_root)


def release_context(commit=None):
    return {
        "repo_root": str(REPO_ROOT),
        "git": engine.git_context(REPO_ROOT, commit),
        "harness_config": load_release_config(),
        "runsc_version": engine.command_output(("runsc", "--version"), REPO_ROOT),
        "proxy": proxy_context(),
    }


def attach_structured_output(result):
    return engine.attach_structured_output(result, JSON_OUTPUT_GATES, EVIDENCE_FILE_GATES)


def run_gate(gate, env=None):
    return engine.run_gate(gate, env=env, attach=attach_structured_output)


def run_gates(gates, env=None):
    return engine.run_gates(gates, env=env, attach=attach_structured_output)


def write_output(path, payload):
    return engine.write_output(path, payload)


def evidence(results, commit=None, context=None):
    commit = commit or engine.git_commit(REPO_ROOT)
    status = "passed" if all(result["status"] == "passed" for result in results) else "failed"
    return {
        "qualification": "control-plane",
        "result": status,
        "commit": commit,
        "generated_at": engine.utc_now(),
        "context": context or release_context(commit),
        "gates": results,
    }


def run(argv=None):
    args = parse_args(argv)
    gates = selected_gates(args)
    if args.list:
        print(json.dumps(engine.render_gate_list(gates), indent=2))
        return 0
    payload = evidence(run_gates(gates, env=os.environ.copy()))
    print(json.dumps(payload, indent=2))
    if args.output:
        write_output(args.output, payload)
    return 0 if payload["result"] == "passed" else 1


def main(argv=None):
    try:
        return run(argv)
    except KeyboardInterrupt:
        return 130
    except Exception as err:  # noqa: BLE001 - top-level reporting
        print(f"{FAILURE_BANNER}: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
