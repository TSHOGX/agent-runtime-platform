#!/usr/bin/env python3
import argparse
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT = "sandbox-isolation-v1"
RELEASE_GATES_PATH = Path(__file__).with_name("release-gates.py")
SPEC = importlib.util.spec_from_file_location("phase8_release_gates", RELEASE_GATES_PATH)
RELEASE_GATES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RELEASE_GATES)

REQUIRED_RUNSC_FIELDS = ("platform", "version", "binary_path", "binary_digest")


def parse_args():
    parser = argparse.ArgumentParser(description="Validate Phase 8 target-lab adversarial evidence coverage.")
    parser.add_argument("--report", required=True, help="Path to the target-lab adversarial JSON report.")
    parser.add_argument("--output", default="", help="Optional path for JSON validation output.")
    return parser.parse_args()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def required_gates():
    return RELEASE_GATES.release_gate_inventory()["gates"]


def load_report(path):
    report_path = Path(path)
    data = report_path.read_text(encoding="utf-8")
    return json.loads(data), report_path


def normalize_gate_records(report):
    gates = report.get("gates", {})
    records = {}
    if isinstance(gates, dict):
        for gate_id, value in gates.items():
            if isinstance(value, dict):
                records[gate_id] = value
            else:
                records[gate_id] = {"status": str(value)}
    elif isinstance(gates, list):
        for item in gates:
            if not isinstance(item, dict):
                continue
            gate_id = str(item.get("id", "")).strip()
            if gate_id:
                records[gate_id] = item
    return records


def validate_report(report):
    issues = []
    if report.get("contract") != CONTRACT:
        issues.append(issue("report", "wrong_contract", got=report.get("contract", "")))
    if str(report.get("qualification", "")).strip() != "adversarial-lab":
        issues.append(issue("report", "wrong_qualification", got=report.get("qualification", "")))
    for field in ("target_host", "generated_at", "proxy_commit"):
        if not str(report.get(field, "")).strip():
            issues.append(issue("report", "missing_field", field=field))
    runsc = report.get("runsc", {})
    if not isinstance(runsc, dict):
        issues.append(issue("report", "invalid_runsc"))
        runsc = {}
    for field in REQUIRED_RUNSC_FIELDS:
        if not str(runsc.get(field, "")).strip():
            issues.append(issue("report", "missing_runsc_field", field=field))

    required = required_gates()
    records = normalize_gate_records(report)
    required_ids = {gate["id"] for gate in required}
    for gate in required:
        record = records.get(gate["id"])
        if not record:
            issues.append(issue("gate", "missing_gate", gate_id=gate["id"], section=gate["section"]))
            continue
        if str(record.get("status", "")).strip() != "passed":
            issues.append(
                issue(
                    "gate",
                    "gate_not_passed",
                    gate_id=gate["id"],
                    section=gate["section"],
                    status=record.get("status", ""),
                )
            )
        if not str(record.get("evidence", "")).strip():
            issues.append(issue("gate", "missing_gate_evidence", gate_id=gate["id"], section=gate["section"]))

    extra_ids = sorted(set(records) - required_ids)
    for gate_id in extra_ids:
        issues.append(issue("gate", "unknown_gate", gate_id=gate_id))

    return {
        "required_total": len(required),
        "reported_total": len(records),
        "passed_total": sum(1 for gate_id in required_ids if str(records.get(gate_id, {}).get("status", "")).strip() == "passed"),
        "issues": issues,
    }


def issue(name, kind, **extra):
    item = {"name": name, "kind": kind}
    item.update(extra)
    return item


def inspect_report(path):
    report, report_path = load_report(path)
    validation = validate_report(report)
    status = "failed" if validation["issues"] else "passed"
    return {
        "contract": CONTRACT,
        "qualification": "adversarial-lab",
        "status": status,
        "generated_at": utc_now(),
        "report_path": str(report_path),
        "target_host": report.get("target_host", ""),
        "proxy_commit": report.get("proxy_commit", ""),
        "runsc": report.get("runsc", {}),
        "required_total": validation["required_total"],
        "reported_total": validation["reported_total"],
        "passed_total": validation["passed_total"],
        "issues": validation["issues"],
    }


def write_output(path, payload):
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main():
    args = parse_args()
    payload = inspect_report(args.report)
    rendered = json.dumps(payload, indent=2)
    print(rendered)
    if args.output:
        write_output(args.output, payload)
    if payload["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
