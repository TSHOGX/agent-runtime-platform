#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
CONTRACT = "sandbox-isolation-v1"
TERMINAL_ABSENCE_STATES = {"absent_verified", "destroyed"}
ACTIVE_STATES = {"allocated", "materializing", "ready", "live", "checkpoint_reserved", "retiring", "reconciling"}

REQUIRED_COLUMNS = (
    "generation_id",
    "session_id",
    "contract_id",
    "sandbox_contract_version",
    "worker_id",
    "host_id",
    "state",
    "runsc_container_id",
    "runsc_platform",
    "runsc_version",
    "runsc_binary_path",
    "runsc_binary_digest",
    "network_profile_id",
    "netns_name",
    "netns_path",
    "host_veth",
    "sandbox_veth",
    "host_gateway_ip",
    "sandbox_ip",
    "sandbox_ip_cidr",
    "host_side_cidr",
    "nft_table_name",
    "control_dir_path",
    "control_manifest_path",
    "bundle_dir_path",
    "spec_path",
    "checkpoint_path",
    "bridge_dir_path",
    "network_hosts_path",
    "log_dir_path",
    "resource_identity_payload",
    "resource_identity_digest",
    "evidence_json",
    "evidence_digest",
    "verified_at",
    "updated_at",
)

IDENTITY_FIELDS = (
    "host_id",
    "session_id",
    "generation_id",
    "contract_id",
    "sandbox_contract_version",
    "runsc_container_id",
    "runsc_platform",
    "runsc_version",
    "runsc_binary_path",
    "runsc_binary_digest",
    "network_profile_id",
    "netns_name",
    "netns_path",
    "host_veth",
    "sandbox_veth",
    "host_gateway_ip",
    "sandbox_ip",
    "sandbox_ip_cidr",
    "host_side_cidr",
    "nft_table_name",
    "control_dir_path",
    "control_manifest_path",
    "bundle_dir_path",
    "spec_path",
    "checkpoint_path",
    "bridge_dir_path",
    "network_hosts_path",
    "log_dir_path",
)


def parse_args():
    parser = argparse.ArgumentParser(description="Audit runtime resource reconciliation evidence and emit JSON.")
    parser.add_argument("--db", default=os.environ.get("HARNESS_DB_PATH", "/var/lib/harness/state/orchestrator.db"))
    parser.add_argument("--runsc-root", default=os.environ.get("RUNSC_ROOT", "/var/lib/harness/runsc"))
    parser.add_argument("--host-id", default=os.environ.get("HARNESS_HOST_ID", ""), help="Optional host_id expected in all rows.")
    parser.add_argument("--expect-clean", action="store_true", help="Fail when active runtime resource rows remain.")
    parser.add_argument("--require-runtime-table", action="store_true", help="Fail if the DB or runtime_resource_instances table is absent.")
    parser.add_argument("--skip-host-commands", action="store_true", help="Skip runsc/ip/nft host inventory commands.")
    parser.add_argument("--require-host-inventory", action="store_true", help="Fail if host inventory commands are unavailable or fail.")
    parser.add_argument("--verify-host-absence", action="store_true", help="Re-run host absence checks for absent_verified/destroyed rows.")
    parser.add_argument("--output", default="", help="Optional path for JSON evidence.")
    return parser.parse_args()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def canonical_json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def parse_canonical_json(raw):
    text = as_text(raw)
    parsed = json.loads(text)
    canonical = canonical_json_bytes(parsed)
    return parsed, canonical


def as_text(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def issue(name, kind, generation_id="", message="", **extra):
    item = {
        "name": name,
        "kind": kind,
    }
    if generation_id:
        item["generation_id"] = generation_id
    if message:
        item["message"] = message
    item.update(extra)
    return item


def lstat_info(path):
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return {"exists": False}
    kind = "other"
    if stat.S_ISDIR(info.st_mode):
        kind = "directory"
    elif stat.S_ISREG(info.st_mode):
        kind = "file"
    elif stat.S_ISLNK(info.st_mode):
        kind = "symlink"
    elif stat.S_ISSOCK(info.st_mode):
        kind = "socket"
    return {
        "exists": True,
        "kind": kind,
        "mode": oct(stat.S_IMODE(info.st_mode)),
        "uid": info.st_uid,
        "gid": info.st_gid,
        "size": info.st_size,
        "target": os.readlink(path) if stat.S_ISLNK(info.st_mode) else "",
    }


def db_inventory(db_path):
    path = Path(db_path)
    result = {
        "path": str(path),
        "info": lstat_info(path),
        "table_exists": False,
        "missing_columns": [],
        "counts_by_state": {},
        "rows": [],
        "audit": [],
        "issues": [],
    }
    if result["info"].get("kind") != "file":
        return result

    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        if not table_exists(conn, "runtime_resource_instances"):
            return result
        result["table_exists"] = True
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(runtime_resource_instances)")}
        result["missing_columns"] = [column for column in REQUIRED_COLUMNS if column not in columns]
        if result["missing_columns"]:
            result["issues"].append(
                issue(
                    "runtime_resource_instances",
                    "missing_columns",
                    message="runtime_resource_instances cannot be audited without all sandbox-isolation-v1 columns",
                    columns=result["missing_columns"],
                )
            )
            return result

        selected = ", ".join(f"COALESCE({column}, '') AS {column}" for column in REQUIRED_COLUMNS)
        rows = [dict(row) for row in conn.execute(f"SELECT {selected} FROM runtime_resource_instances ORDER BY generation_id")]
        for row in rows:
            state = row["state"]
            result["counts_by_state"][state] = result["counts_by_state"].get(state, 0) + 1
            audit = audit_row(row)
            result["audit"].append(audit)
            result["issues"].extend(audit["issues"])
            result["rows"].append(
                {
                    "generation_id": row["generation_id"],
                    "session_id": row["session_id"],
                    "host_id": row["host_id"],
                    "state": state,
                    "runsc_container_id": row["runsc_container_id"],
                    "netns_name": row["netns_name"],
                    "host_veth": row["host_veth"],
                    "nft_table_name": row["nft_table_name"],
                    "checkpoint_path": row["checkpoint_path"],
                    "control_dir_path": row["control_dir_path"],
                    "control_manifest_path": row["control_manifest_path"],
                    "bundle_dir_path": row["bundle_dir_path"],
                    "spec_path": row["spec_path"],
                    "bridge_dir_path": row["bridge_dir_path"],
                    "network_hosts_path": row["network_hosts_path"],
                    "log_dir_path": row["log_dir_path"],
                    "verified_at": row["verified_at"],
                }
            )
    finally:
        conn.close()
    return result


def table_exists(conn, table):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def audit_row(row):
    generation_id = row["generation_id"]
    issues = []
    identity = None

    if row["sandbox_contract_version"] != CONTRACT:
        issues.append(issue("runtime_resource_identity", "wrong_contract", generation_id, contract=row["sandbox_contract_version"]))

    try:
        identity, identity_canonical = parse_canonical_json(row["resource_identity_payload"])
    except (json.JSONDecodeError, UnicodeDecodeError) as err:
        issues.append(issue("runtime_resource_identity", "invalid_json", generation_id, message=str(err)))
    else:
        if as_text(row["resource_identity_payload"]).encode("utf-8") != identity_canonical:
            issues.append(issue("runtime_resource_identity", "noncanonical_payload", generation_id))
        if sha256_digest(identity_canonical) != row["resource_identity_digest"]:
            issues.append(
                issue(
                    "runtime_resource_identity",
                    "digest_mismatch",
                    generation_id,
                    got=sha256_digest(identity_canonical),
                    want=row["resource_identity_digest"],
                )
            )
        issues.extend(identity_mirror_issues(row, identity))

    if row["state"] in TERMINAL_ABSENCE_STATES:
        issues.extend(audit_terminal_evidence(row, identity))

    return {
        "generation_id": generation_id,
        "state": row["state"],
        "identity_status": "passed" if not [item for item in issues if item["name"] == "runtime_resource_identity"] else "failed",
        "evidence_status": evidence_status(row, issues),
        "issues": issues,
    }


def identity_mirror_issues(row, identity):
    issues = []
    generation_id = row["generation_id"]
    if not isinstance(identity, dict):
        return [issue("runtime_resource_identity", "invalid_payload_type", generation_id)]
    if not isinstance(identity.get("root_prefixes", {}), dict):
        issues.append(issue("runtime_resource_identity", "invalid_root_prefixes", generation_id))
    for field in IDENTITY_FIELDS:
        row_value = as_text(row[field]).strip()
        payload_value = as_text(identity.get(field, "")).strip()
        if payload_value != row_value:
            issues.append(
                issue(
                    "runtime_resource_identity",
                    "row_mirror_mismatch",
                    generation_id,
                    field=field,
                    row_value=row_value,
                    payload_value=payload_value,
                )
            )
    return issues


def audit_terminal_evidence(row, identity):
    generation_id = row["generation_id"]
    issues = []
    if not row["verified_at"]:
        issues.append(issue("reconciliation_evidence", "missing_verified_at", generation_id))
    if not row["evidence_json"]:
        return issues + [issue("reconciliation_evidence", "missing_evidence_json", generation_id)]

    try:
        evidence, evidence_canonical = parse_canonical_json(row["evidence_json"])
    except (json.JSONDecodeError, UnicodeDecodeError) as err:
        return issues + [issue("reconciliation_evidence", "invalid_json", generation_id, message=str(err))]

    if as_text(row["evidence_json"]).encode("utf-8") != evidence_canonical:
        issues.append(issue("reconciliation_evidence", "noncanonical_payload", generation_id))
    if not row["evidence_digest"]:
        issues.append(issue("reconciliation_evidence", "missing_digest", generation_id))
    elif sha256_digest(evidence_canonical) != row["evidence_digest"]:
        issues.append(
            issue(
                "reconciliation_evidence",
                "digest_mismatch",
                generation_id,
                got=sha256_digest(evidence_canonical),
                want=row["evidence_digest"],
            )
        )
    if not isinstance(evidence, dict):
        return issues + [issue("reconciliation_evidence", "invalid_payload_type", generation_id)]

    expected_host = as_text((identity or {}).get("host_id", row["host_id"])).strip()
    if as_text(evidence.get("host_id", "")).strip() != expected_host:
        issues.append(
            issue(
                "reconciliation_evidence",
                "host_mismatch",
                generation_id,
                evidence_host=as_text(evidence.get("host_id", "")).strip(),
                expected_host=expected_host,
            )
        )

    required_values = {
        "runsc_state": evidence.get("runsc_state", ""),
        "ip_netns": evidence.get("ip_netns", ""),
        "ip_link": evidence.get("ip_link", ""),
        "nft": evidence.get("nft", ""),
    }
    for field, value in required_values.items():
        absence_issue = validate_absence_value("reconciliation_evidence", field, value, generation_id)
        if absence_issue:
            issues.append(absence_issue)

    filesystem = evidence.get("filesystem_lstat", {})
    if not isinstance(filesystem, dict) or not filesystem:
        issues.append(issue("reconciliation_evidence", "missing_filesystem_lstat", generation_id))
        return issues

    for key in required_filesystem_lstat_keys(identity or row):
        if key not in filesystem:
            issues.append(issue("reconciliation_evidence", "missing_filesystem_lstat_key", generation_id, key=key))
            continue
        absence_issue = validate_absence_value("reconciliation_evidence", "filesystem_lstat "+key, filesystem[key], generation_id)
        if absence_issue:
            issues.append(absence_issue)

    for key, value in filesystem.items():
        if not as_text(key).strip() or not as_text(value).strip():
            issues.append(issue("reconciliation_evidence", "empty_filesystem_lstat_entry", generation_id, key=as_text(key)))
            continue
        absence_issue = validate_absence_value("reconciliation_evidence", "filesystem_lstat "+key, value, generation_id)
        if absence_issue:
            issues.append(absence_issue)
    return issues


def evidence_status(row, issues):
    evidence_issues = [item for item in issues if item["name"] == "reconciliation_evidence"]
    if row["state"] not in TERMINAL_ABSENCE_STATES:
        return "not_required"
    return "passed" if not evidence_issues else "failed"


def validate_absence_value(name, field, value, generation_id):
    value = as_text(value).strip()
    if not value:
        return issue(name, "missing_absence_value", generation_id, field=field)
    if "absent_or_previously_removed" in value:
        return issue(name, "synthetic_absence_value", generation_id, field=field, value=value)
    if value == "absent" or ":absent" in value:
        return None
    return issue(name, "non_absence_value", generation_id, field=field, value=value)


def required_filesystem_lstat_keys(source):
    def value(name):
        if isinstance(source, dict):
            return as_text(source.get(name, "")).strip()
        return ""

    targets = [
        ("checkpoint", value("checkpoint_path")),
        ("control", value("control_dir_path")),
        ("control_manifest", value("control_manifest_path")),
        ("bundle", value("bundle_dir_path")),
        ("spec", value("spec_path")),
        ("bridge", value("bridge_dir_path")),
        ("log", value("log_dir_path")),
    ]
    network_hosts = value("network_hosts_path")
    if network_hosts:
        cleaned = os.path.normpath(network_hosts)
        targets.append(("network", os.path.dirname(cleaned)))
        targets.append(("network_hosts", cleaned))

    keys = []
    for kind, path in targets:
        path = as_text(path).strip()
        if path:
            keys.append(kind + ":" + os.path.normpath(path))
    return keys


def command_inventory(name, command, cwd=REPO_ROOT, timeout=5):
    if shutil.which(command[0]) is None:
        return {
            "name": name,
            "command": command,
            "status": "skipped",
            "returncode": 127,
            "output_tail": f"{command[0]} not found",
        }
    started = time.monotonic()
    try:
        result = subprocess.run(command, cwd=cwd, text=True, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired as err:
        output = ((err.stdout or "") + (err.stderr or "")).strip()
        return {
            "name": name,
            "command": command,
            "status": "failed",
            "returncode": 124,
            "duration_ms": round((time.monotonic() - started) * 1000),
            "output_tail": tail(output),
        }
    output = (result.stdout + result.stderr).strip()
    return {
        "name": name,
        "command": command,
        "status": "passed" if result.returncode == 0 else "failed",
        "returncode": result.returncode,
        "duration_ms": round((time.monotonic() - started) * 1000),
        "output_tail": tail(output),
    }


def tail(value, limit=12000):
    if len(value) <= limit:
        return value
    return value[-limit:]


def host_inventory(args):
    if args.skip_host_commands:
        return {"status": "skipped", "commands": []}
    commands = [
        command_inventory("runsc_containers", ["runsc", "-root", args.runsc_root, "list"]),
        command_inventory("ip_netns", ["ip", "netns", "list"]),
        command_inventory("ip_links", ["ip", "-o", "link", "show"]),
        command_inventory("nft_tables", ["nft", "list", "tables"]),
    ]
    status = "passed" if all(item["status"] == "passed" for item in commands) else "incomplete"
    return {"status": status, "commands": commands}


def host_absence_checks(rows, args):
    if args.skip_host_commands or not args.verify_host_absence:
        return {"status": "skipped", "checks": []}
    checks = []
    for row in rows:
        if row["state"] not in TERMINAL_ABSENCE_STATES:
            continue
        checks.extend(absence_checks_for_row(row, args))
    status = "passed" if all(check["status"] == "passed" for check in checks) else "failed"
    return {"status": status, "checks": checks}


def absence_checks_for_row(row, args):
    generation_id = row["generation_id"]
    return [
        runsc_absence_check(generation_id, row["runsc_container_id"], args.runsc_root),
        netns_absence_check(generation_id, row["netns_name"]),
        link_absence_check(generation_id, row["host_veth"]),
        nft_absence_check(generation_id, row["nft_table_name"]),
        *filesystem_absence_checks(generation_id, required_filesystem_lstat_keys(row)),
    ]


def runsc_absence_check(generation_id, container_id, runsc_root):
    command = ["runsc", "-root", runsc_root, "state", container_id]
    result = command_inventory("runsc_state_absence", command)
    result["generation_id"] = generation_id
    result["container_id"] = container_id
    result["status"] = command_failure_proves_absence(result)
    return result


def netns_absence_check(generation_id, netns_name):
    result = command_inventory("ip_netns_absence", ["ip", "netns", "list"])
    result["generation_id"] = generation_id
    result["netns_name"] = netns_name
    if result["status"] == "passed":
        result["status"] = "failed" if netns_list_contains(result["output_tail"], netns_name) else "passed"
    return result


def link_absence_check(generation_id, link_name):
    result = command_inventory("ip_link_absence", ["ip", "link", "show", link_name])
    result["generation_id"] = generation_id
    result["link_name"] = link_name
    result["status"] = command_failure_proves_absence(result)
    return result


def nft_absence_check(generation_id, table_name):
    result = command_inventory("nft_table_absence", ["nft", "list", "table", "inet", table_name])
    result["generation_id"] = generation_id
    result["table_name"] = table_name
    result["status"] = command_failure_proves_absence(result)
    return result


def filesystem_absence_checks(generation_id, evidence_keys):
    checks = []
    for key in evidence_keys:
        _, path = key.split(":", 1)
        info = lstat_info(path)
        checks.append(
            {
                "name": "filesystem_lstat_absence",
                "generation_id": generation_id,
                "key": key,
                "path": path,
                "status": "passed" if not info.get("exists") else "failed",
                "info": info,
            }
        )
    return checks


def command_failure_proves_absence(result):
    if result["status"] == "skipped":
        return "skipped"
    output = result.get("output_tail", "").lower()
    if result.get("returncode") != 0 and any(
        marker in output
        for marker in ("does not exist", "not found", "no such container", "no such file", "cannot find device", "no such device", "no such table")
    ):
        return "passed"
    return "failed"


def netns_list_contains(output, netns_name):
    for line in output.splitlines():
        fields = line.split()
        if fields and fields[0] == netns_name:
            return True
    return False


def inspect_reconciliation(args):
    db = db_inventory(args.db)
    host = host_inventory(args)
    absence = host_absence_checks(db.get("rows", []), args)
    issues = list(db.get("issues", []))

    if args.require_runtime_table:
        if not db["info"].get("exists"):
            issues.append(issue("runtime_resource_instances", "missing_db", message="runtime DB is required for release reconciliation evidence"))
        elif not db["table_exists"]:
            issues.append(issue("runtime_resource_instances", "missing_table", message="runtime_resource_instances table is required"))

    if args.host_id:
        for row in db.get("rows", []):
            if row["host_id"] != args.host_id:
                issues.append(issue("runtime_resource_instances", "host_mismatch", row["generation_id"], row_host=row["host_id"], expected_host=args.host_id))

    if args.expect_clean:
        for state, count in db.get("counts_by_state", {}).items():
            if state in ACTIVE_STATES and count > 0:
                issues.append(issue("runtime_resource_instances", "active_resources", state=state, count=count))

    if args.require_host_inventory and host.get("status") != "passed":
        issues.append(issue("host_inventory", "incomplete", status=host.get("status")))

    if args.verify_host_absence and absence.get("status") != "passed":
        issues.append(issue("host_absence", "failed", status=absence.get("status")))

    status = "failed" if issues else "passed"
    return {
        "contract": CONTRACT,
        "qualification": "reconciliation-evidence",
        "status": status,
        "generated_at": utc_now(),
        "expect_clean": bool(args.expect_clean),
        "require_runtime_table": bool(args.require_runtime_table),
        "require_host_inventory": bool(args.require_host_inventory),
        "verify_host_absence": bool(args.verify_host_absence),
        "db": db,
        "host": host,
        "host_absence": absence,
        "issues": issues,
    }


def write_output(path, payload):
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main():
    args = parse_args()
    payload = inspect_reconciliation(args)
    rendered = json.dumps(payload, indent=2)
    print(rendered)
    if args.output:
        write_output(args.output, payload)
    if payload["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
