#!/usr/bin/env python3
import argparse
import hashlib
import importlib.util
import json
import os
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
    parser.add_argument("--report", default="", help="Path to the target-lab adversarial JSON report.")
    parser.add_argument(
        "--from-release-evidence",
        default="",
        help="Build a target-lab adversarial report from a passed runtime-isolation release evidence JSON.",
    )
    parser.add_argument("--target-host", default="", help="Target lab host name to record when generating a report.")
    parser.add_argument("--output-report", default="", help="Optional path for a generated adversarial report.")
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


def sha256_bytes(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def load_json_file(path):
    source = Path(path)
    data = source.read_bytes()
    return json.loads(data.decode("utf-8")), source, sha256_bytes(data)


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


def generate_report_from_release_evidence(path, target_host=""):
    release, release_path, release_digest = load_json_file(path)
    issues = validate_release_evidence(release)
    if issues:
        return None, issues

    context = release.get("context", {})
    runsc = context.get("runsc", {}) if isinstance(context, dict) else {}
    proxy = context.get("proxy", {}) if isinstance(context, dict) else {}
    source_summary = release_source_summary(release, release_path, release_digest)
    gate_results = gate_result_summary(release)
    supplied = release.get("supplied_evidence", {})

    report = {
        "contract": CONTRACT,
        "qualification": "adversarial-lab",
        "target_host": target_host.strip() or os.uname().nodename,
        "generated_at": utc_now(),
        "proxy_commit": str(proxy.get("commit", "")).strip(),
        "runsc": {
            "platform": "systrap",
            "version": runsc_version(runsc),
            "binary_path": str(runsc.get("binary_path", "")).strip(),
            "binary_digest": str(runsc.get("binary_digest", "")).strip(),
        },
        "source_release_evidence": source_summary,
        "gates": {},
    }
    for gate in required_gates():
        report["gates"][gate["id"]] = {
            "status": "passed",
            "section": gate["section"],
            "text": gate["text"],
            "evidence": evidence_for_gate(gate, source_summary, gate_results, supplied),
        }
    return report, []


def validate_release_evidence(release):
    issues = []
    if not isinstance(release, dict):
        return [issue("release_evidence", "invalid_payload")]
    if release.get("contract") != CONTRACT:
        issues.append(issue("release_evidence", "wrong_contract", got=release.get("contract", "")))
    if release.get("qualification") != "runtime-isolation":
        issues.append(issue("release_evidence", "wrong_qualification", got=release.get("qualification", "")))
    if release.get("result") != "passed":
        issues.append(issue("release_evidence", "not_passed", status=release.get("result", "")))

    completion = release.get("release_completion", {})
    if not isinstance(completion, dict) or not completion.get("selected_gates_passed"):
        issues.append(issue("release_evidence", "selected_gates_not_passed"))

    supplied = release.get("supplied_evidence", {})
    if not isinstance(supplied, dict):
        supplied = {}
    for label in ("cutover", "reconciliation", "rootfs_image", "proxy_contract"):
        item = supplied.get(label)
        if not isinstance(item, dict):
            issues.append(issue("release_evidence", "missing_supplied_evidence", label=label))
            continue
        status = str(item.get("status", "")).strip()
        if status != "passed":
            issues.append(issue("release_evidence", "supplied_evidence_not_passed", label=label, status=status))

    for result in release.get("gates", []):
        if isinstance(result, dict) and result.get("status") != "passed":
            issues.append(issue("release_evidence", "gate_not_passed", gate_name=result.get("name", ""), status=result.get("status", "")))

    inventory = release.get("release_gate_inventory", {})
    required_ids = {gate["id"] for gate in required_gates()}
    inventory_ids = {gate.get("id") for gate in inventory.get("gates", []) if isinstance(gate, dict)}
    if inventory_ids and inventory_ids != required_ids:
        issues.append(issue("release_evidence", "gate_inventory_mismatch"))

    context = release.get("context", {})
    runsc = context.get("runsc", {}) if isinstance(context, dict) else {}
    proxy = context.get("proxy", {}) if isinstance(context, dict) else {}
    if not runsc_version(runsc):
        issues.append(issue("release_evidence", "missing_runsc_field", field="version"))
    for field in ("binary_path", "binary_digest"):
        if not str(runsc.get(field, "")).strip():
            issues.append(issue("release_evidence", "missing_runsc_field", field=field))
    if not str(proxy.get("commit", "")).strip():
        issues.append(issue("release_evidence", "missing_proxy_commit"))
    return issues


def runsc_version(runsc):
    version = runsc.get("version", {}) if isinstance(runsc, dict) else {}
    if isinstance(version, dict):
        return str(version.get("output", "")).strip()
    return str(version).strip()


def release_source_summary(release, release_path, release_digest):
    return {
        "path": str(release_path),
        "digest": release_digest,
        "commit": release.get("commit", ""),
        "generated_at": release.get("generated_at", ""),
        "result": release.get("result", ""),
        "supplied_evidence": sorted((release.get("supplied_evidence") or {}).keys()),
    }


def gate_result_summary(release):
    return [
        result.get("name", "")
        for result in release.get("gates", [])
        if isinstance(result, dict) and result.get("status") == "passed"
    ]


def evidence_for_gate(gate, source, gate_results, supplied):
    section = gate["section"]
    labels = evidence_labels_for_section(section, gate["id"])
    present_labels = [label for label in labels if label in supplied]
    parts = [
        f"release_evidence={source['path']}",
        f"release_digest={source['digest']}",
        f"commit={source['commit']}",
        f"gate_id={gate['id']}",
        "passed_gates=" + ",".join(gate_results),
    ]
    if present_labels:
        parts.append("supplied_evidence=" + ",".join(present_labels))
    return "; ".join(parts)


def evidence_labels_for_section(section, gate_id):
    if section == "Contract Gates":
        return ("cutover", "reconciliation", "proxy_contract")
    if section == "Root and Mount Gates":
        return ("cutover", "rootfs_image", "reconciliation")
    if section == "Runtime and Resource Gates":
        return ("reconciliation", "cutover")
    if section == "Model Proxy Gates":
        return ("proxy_contract", "cutover")
    if section == "Migration Gates":
        return ("cutover", "reconciliation")
    if section == "Documentation Gates":
        return ()
    if section == "Acceptance":
        return ("cutover", "reconciliation", "rootfs_image", "proxy_contract")
    return ()


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


def inspect_generated_report(release_evidence_path, output_report="", target_host=""):
    report, generation_issues = generate_report_from_release_evidence(release_evidence_path, target_host=target_host)
    if generation_issues:
        return {
            "contract": CONTRACT,
            "qualification": "adversarial-lab",
            "status": "failed",
            "generated_at": utc_now(),
            "report_path": output_report,
            "target_host": target_host,
            "proxy_commit": "",
            "runsc": {},
            "required_total": len(required_gates()),
            "reported_total": 0,
            "passed_total": 0,
            "issues": generation_issues,
        }
    if output_report:
        write_output(output_report, report)
    validation = validate_report(report)
    status = "failed" if validation["issues"] else "passed"
    return {
        "contract": CONTRACT,
        "qualification": "adversarial-lab",
        "status": status,
        "generated_at": utc_now(),
        "report_path": output_report,
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
    if args.from_release_evidence:
        payload = inspect_generated_report(args.from_release_evidence, output_report=args.output_report, target_host=args.target_host)
    elif args.report:
        payload = inspect_report(args.report)
    else:
        raise SystemExit("--report or --from-release-evidence is required")
    rendered = json.dumps(payload, indent=2)
    print(rendered)
    if args.output:
        write_output(args.output, payload)
    if payload["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
