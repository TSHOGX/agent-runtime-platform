#!/usr/bin/env python3
"""Sandbox-isolation release suite.

The current runtime-isolation contract (`sandbox-isolation-v1`). Owns the
deterministic repo gates, the supplied-evidence model, the static release
scans, and the doc-inventory cross-check. This is the live release harness.
"""
import argparse
import json
import os
import sys
from pathlib import Path

from tools.release import engine
from tools.release.engine import Gate
from tools.release.suites import static_manifests

REPO_ROOT = Path(__file__).resolve().parents[3]
PROXY_ROOT = Path(os.environ.get("HARNESS_PROXY_ROOT", "/root/claude-code-proxy"))
RELEASE_GATES_DOC = REPO_ROOT / "docs" / "phase8" / "release-gates.md"

SUITE = "sandbox_isolation"
FAILURE_BANNER = "runtime isolation release gates failed"

REQUIRED_SUPPLIED_EVIDENCE = (
    "cutover",
    "reconciliation",
    "rootfs_image",
    "proxy_contract",
    "adversarial_lab",
)

JSON_OUTPUT_GATES = {
    "live_turn_start_latency",
    "rootfs_image_inspection",
    "cutover_inventory",
    "runtime_reconciliation_evidence",
    "sandbox_adversarial_lab",
}
EVIDENCE_FILE_GATES = {"gvisor_bridge_durability_lab"}

_NOTE_INCOMPLETE = "Runtime isolation release requires target-lab adversarial evidence for every gate in runtime-isolation release gates doc."
_NOTE_COMPLETE = "Selected gates passed and all required supplied evidence labels were attached."


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run runtime isolation release qualification gates and emit JSON evidence.")
    parser.add_argument("--include-prior-release", action="store_true", help="Run the prior deterministic release runner.")
    parser.add_argument("--include-cutover-inventory", action="store_true", help="Run the cutover inventory clean-state gate.")
    parser.add_argument("--include-reconciliation", action="store_true", help="Run the runtime resource reconciliation evidence gate.")
    parser.add_argument("--include-rootfs-inspection", action="store_true", help="Inspect the configured sandbox rootfs image.")
    parser.add_argument("--include-proxy", action="store_true", help="Run the pinned claude-code-proxy contract gate.")
    parser.add_argument("--include-adversarial-lab", action="store_true", help="Validate target-lab adversarial evidence coverage.")
    parser.add_argument("--adversarial-lab-report", default=os.environ.get("HARNESS_SANDBOX_ADVERSARIAL_LAB_REPORT", ""), help="Path to the target-lab adversarial JSON report.")
    parser.add_argument("--include-bridge-lab", action="store_true", help="Run the gVisor bridge durability lab.")
    parser.add_argument("--include-live-latency", action="store_true", help="Run the live turn-start latency gate.")
    parser.add_argument(
        "--evidence",
        action="append",
        default=[],
        metavar="LABEL=PATH",
        help="Attach supplied JSON evidence. Required release labels: " + ", ".join(REQUIRED_SUPPLIED_EVIDENCE),
    )
    parser.add_argument(
        "--require-release-evidence",
        action="store_true",
        help="Fail unless every required supplied evidence label is attached.",
    )
    parser.add_argument("--output", default="", help="Optional path for the JSON evidence file.")
    parser.add_argument("--list", action="store_true", help="List selected gates without running them.")
    parser.add_argument("--static-only", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args(argv)


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
            name="python_sandbox_and_release_tools",
            command=(
                "python3",
                "-W",
                "error",
                "-m",
                "unittest",
                "sandbox-image/tests/test_harness_bridge_client.py",
                "tools/release/gates/sandbox_isolation/test_adversarial_lab.py",
                "tools/release/gates/sandbox_isolation/test_cutover_cleanup.py",
                "tools/release/gates/sandbox_isolation/test_cutover_inventory.py",
                "tools/release/gates/sandbox_isolation/test_reconciliation_evidence.py",
                "tools/release/gates/sandbox_isolation/test_release_gates.py",
                "tools/release/gates/sandbox_isolation/test_rootfs_inspect.py",
                "tools/release/test_engine.py",
                "tools/release/test_suites.py",
            ),
            cwd=REPO_ROOT,
            category="deterministic",
        ),
        Gate(
            name="runtime_isolation_static_release_scans",
            command=("python3", "tools/release/run.py", "--suite", "sandbox_isolation", "--static-only"),
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
                command=("python3", "tools/release/run.py", "--suite", "control_plane"),
                cwd=REPO_ROOT,
                category="compatibility",
            )
        )
    if args.include_cutover_inventory:
        gates.append(
            Gate(
                name="cutover_inventory",
                command=("tools/release/gates/sandbox_isolation/cutover-inventory.py", "--expect-clean", "--require-host-inventory"),
                cwd=REPO_ROOT,
                category="evidence",
            )
        )
    if args.include_reconciliation:
        gates.append(
            Gate(
                name="runtime_reconciliation_evidence",
                command=(
                    "tools/release/gates/sandbox_isolation/reconciliation-evidence.py",
                    "--expect-clean",
                    "--require-runtime-table",
                    "--require-host-inventory",
                    "--verify-host-absence",
                ),
                cwd=REPO_ROOT,
                category="evidence",
            )
        )
    if args.include_rootfs_inspection:
        gates.append(
            Gate(
                name="rootfs_image_inspection",
                command=("tools/release/gates/sandbox_isolation/rootfs-inspect.py",),
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
    if args.include_adversarial_lab:
        gates.append(
            Gate(
                name="sandbox_adversarial_lab",
                command=("tools/release/gates/sandbox_isolation/adversarial-lab.py", "--report", args.adversarial_lab_report),
                cwd=REPO_ROOT,
                category="evidence",
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


def proxy_context(proxy_root=PROXY_ROOT):
    return engine.proxy_context(proxy_root)


def release_context(commit=None):
    return {
        "repo_root": str(REPO_ROOT),
        "git": engine.git_context(REPO_ROOT, commit),
        "runsc": engine.runsc_context(REPO_ROOT),
        "proxy": proxy_context(),
        "release_gates_doc": str(RELEASE_GATES_DOC),
    }


def release_gate_inventory(path=RELEASE_GATES_DOC):
    return engine.gate_inventory(path)


def attach_structured_output(result):
    return engine.attach_structured_output(result, JSON_OUTPUT_GATES, EVIDENCE_FILE_GATES)


def run_gate(gate, env=None):
    return engine.run_gate(gate, env=env, attach=attach_structured_output)


def run_gates(gates, env=None):
    return engine.run_gates(gates, env=env, attach=attach_structured_output)


def parse_supplied_evidence(specs):
    return engine.parse_supplied_evidence(specs)


def write_output(path, payload):
    return engine.write_output(path, payload)


def release_completion(results, supplied_evidence, require_release_evidence=False):
    return engine.release_completion(
        results,
        supplied_evidence,
        REQUIRED_SUPPLIED_EVIDENCE,
        _NOTE_INCOMPLETE,
        _NOTE_COMPLETE,
        require_release_evidence=require_release_evidence,
    )


def supplied_evidence_from_gate_results(results, context=None):
    context = context or {}
    supplied = {}
    for result in results:
        if result["status"] != "passed":
            continue
        if result["name"] == "pinned_proxy_contract":
            proxy = context.get("proxy", {}) if isinstance(context, dict) else {}
            supplied["proxy_contract"] = {
                "path": "gate:pinned_proxy_contract",
                "digest": proxy.get("commit", ""),
                "bytes": 0,
                "status": "passed",
                "payload": {
                    "proxy": proxy,
                    "gate": {
                        "name": result["name"],
                        "status": result["status"],
                    },
                },
            }
            continue
        payload = result.get("structured_output")
        if not isinstance(payload, dict):
            continue
        if result["name"] == "rootfs_image_inspection":
            supplied["rootfs_image"] = {
                "path": "gate:rootfs_image_inspection",
                "digest": payload.get("rootfs_digest", ""),
                "bytes": 0,
                "status": payload.get("status", ""),
                "payload": payload,
            }
        elif result["name"] == "cutover_inventory":
            supplied["cutover"] = {
                "path": "gate:cutover_inventory",
                "digest": "",
                "bytes": 0,
                "status": payload.get("status", ""),
                "payload": payload,
            }
        elif result["name"] == "runtime_reconciliation_evidence":
            supplied["reconciliation"] = {
                "path": "gate:runtime_reconciliation_evidence",
                "digest": "",
                "bytes": 0,
                "status": payload.get("status", ""),
                "payload": payload,
            }
        elif result["name"] == "sandbox_adversarial_lab":
            supplied["adversarial_lab"] = {
                "path": "gate:sandbox_adversarial_lab",
                "digest": "",
                "bytes": 0,
                "status": payload.get("status", ""),
                "payload": payload,
            }
    return supplied


def evidence(results, commit=None, context=None, supplied_evidence=None, require_release_evidence=False):
    commit = commit or engine.git_commit(REPO_ROOT)
    context = context or release_context(commit)
    supplied_evidence = {**supplied_evidence_from_gate_results(results, context), **(supplied_evidence or {})}
    completion = release_completion(results, supplied_evidence, require_release_evidence)
    status = "passed" if completion["selected_gates_passed"] else "failed"
    if require_release_evidence and completion["missing_supplied_evidence"]:
        status = "failed"
    return {
        "contract": "sandbox-isolation-v1",
        "qualification": "runtime-isolation",
        "result": status,
        "commit": commit,
        "generated_at": engine.utc_now(),
        "context": context,
        "release_completion": completion,
        "release_gate_inventory": release_gate_inventory(),
        "supplied_evidence": supplied_evidence,
        "gates": results,
    }


def static_checks():
    return engine.run_static_checks(static_manifests.sandbox_isolation_checks())


def run_static_only():
    payload = static_checks()
    print(json.dumps(payload, indent=2))
    return 0 if payload["status"] == "passed" else 1


def run(argv=None):
    args = parse_args(argv)
    if args.static_only:
        return run_static_only()

    gates = selected_gates(args)
    if args.list:
        print(json.dumps(engine.render_gate_list(gates), indent=2))
        return 0

    supplied = parse_supplied_evidence(args.evidence)
    results = run_gates(gates, env=os.environ.copy())
    payload = evidence(
        results,
        supplied_evidence=supplied,
        require_release_evidence=args.require_release_evidence,
    )
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
