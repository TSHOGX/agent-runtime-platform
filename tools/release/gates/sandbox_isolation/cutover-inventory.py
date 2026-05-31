#!/usr/bin/env python3
import argparse
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
DEFAULT_CONFIG = REPO_ROOT / "config" / "harness.yaml"


def parse_args():
    defaults = load_config_defaults()
    parser = argparse.ArgumentParser(description="Inventory runtime-isolation cutover state and emit JSON evidence.")
    parser.add_argument("--db", default=os.environ.get("HARNESS_DB_PATH", "/var/lib/harness/state/orchestrator.db"))
    parser.add_argument("--sessions-root", default=os.environ.get("HARNESS_SESSIONS_ROOT", "/var/lib/harness/sessions"))
    parser.add_argument("--agent-homes-root", default=os.environ.get("HARNESS_AGENT_HOMES_ROOT", "/var/lib/harness/agent-homes"))
    parser.add_argument("--run-dir", default=os.environ.get("HARNESS_RUN_DIR", defaults.get("harness.run_dir", "/var/lib/harness/run")))
    parser.add_argument("--runsc-root", default=os.environ.get("RUNSC_ROOT", "/var/lib/harness/runsc"))
    parser.add_argument("--checkpoints-root", default=os.environ.get("HARNESS_CHECKPOINTS_ROOT", "/var/lib/harness/checkpoints"))
    parser.add_argument("--prepared-bundle-root", default=os.environ.get("HARNESS_BUNDLE_ROOT", str(REPO_ROOT / "bundle" / "out")))
    parser.add_argument("--legacy-secret-root", default=os.environ.get("HARNESS_LEGACY_SECRET_ROOT", "/var/lib/harness/secrets"))
    parser.add_argument("--provider-credential-root", default=os.environ.get("HARNESS_PROVIDER_CREDENTIAL_ROOT", ""))
    parser.add_argument("--proxy-internal-root", default=os.environ.get("HARNESS_PROXY_INTERNAL_ROOT", ""))
    parser.add_argument("--skip-host-commands", action="store_true", help="Skip ip/nft/runsc host inventory commands.")
    parser.add_argument("--require-host-inventory", action="store_true", help="Fail if host inventory commands are unavailable or fail.")
    parser.add_argument("--expect-clean", action="store_true", help="Fail when cutover inventory finds active or legacy resources.")
    parser.add_argument("--output", default="", help="Optional path for JSON evidence.")
    return parser.parse_args()


def load_config_defaults(path=DEFAULT_CONFIG):
    targets = {"harness.run_dir"}
    values = {}
    stack = []
    if not path.is_file():
        return values
    with open(path, encoding="utf-8") as handle:
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
            dotted = ".".join([item[1] for item in stack] + [key])
            if value == "":
                stack.append((indent, key))
                continue
            if dotted in targets:
                values[dotted] = value
    return values


def utc_now():
    return datetime.now(timezone.utc).isoformat()


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


def directory_inventory(path, sample_limit=25):
    path = Path(path)
    info = lstat_info(path)
    result = {
        "path": str(path),
        "info": info,
        "entries": 0,
        "sockets": 0,
        "sample": [],
    }
    if info.get("kind") != "directory":
        return result
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                result["entries"] += 1
                entry_info = lstat_info(entry.path)
                if entry_info.get("kind") == "socket":
                    result["sockets"] += 1
                if len(result["sample"]) < sample_limit:
                    result["sample"].append({"name": entry.name, "info": entry_info})
    except PermissionError as err:
        result["error"] = str(err)
    return result


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


def db_inventory(db_path):
    path = Path(db_path)
    result = {
        "path": str(path),
        "info": lstat_info(path),
        "tables": {},
        "queries": {},
    }
    if result["info"].get("kind") != "file":
        return result
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        for table in (
            "sessions",
            "session_workspaces",
            "session_driver_homes",
            "runtime_generations",
            "runtime_generation_resources",
            "runtime_resource_instances",
            "active_model_request_contexts",
            "network_profiles",
            "sandbox_contracts",
        ):
            exists = table_exists(conn, table)
            result["tables"][table] = {"exists": exists}
            if exists:
                result["tables"][table]["rows"] = scalar(conn, f"SELECT COUNT(*) FROM {table}")
        query_specs = {
            "sessions_total": ("sessions", "SELECT COUNT(*) FROM sessions"),
            "session_workspaces_total": ("session_workspaces", "SELECT COUNT(*) FROM session_workspaces"),
            "session_driver_homes_total": ("session_driver_homes", "SELECT COUNT(*) FROM session_driver_homes"),
            "active_model_contexts_total": ("active_model_request_contexts", "SELECT COUNT(*) FROM active_model_request_contexts"),
            "runtime_generations_active": (
                "runtime_generations",
                "SELECT COUNT(*) FROM runtime_generations WHERE status NOT IN ('failed','destroyed')",
            ),
            "runtime_generation_resources_active": (
                "runtime_generation_resources",
                "SELECT COUNT(*) FROM runtime_generation_resources WHERE resource_state != 'destroyed'",
            ),
            "runtime_resource_instances_active": (
                "runtime_resource_instances",
                "SELECT COUNT(*) FROM runtime_resource_instances WHERE state NOT IN ('absent_verified','destroyed')",
            ),
            "network_profiles_active": (
                "network_profiles",
                "SELECT COUNT(*) FROM network_profiles WHERE allocation_state != 'destroyed'",
            ),
            "checkpoint_paths": (
                "runtime_generation_resources",
                "SELECT COUNT(*) FROM runtime_generation_resources WHERE checkpoint_path IS NOT NULL AND checkpoint_path != '' AND resource_state != 'destroyed'",
            ),
            "sandbox_contract_rows": ("sandbox_contracts", "SELECT COUNT(*) FROM sandbox_contracts"),
        }
        for name, (table, query) in query_specs.items():
            if table_exists(conn, table):
                result["queries"][name] = {"status": "present", "count": scalar(conn, query)}
            else:
                result["queries"][name] = {"status": "missing_table", "count": 0}
    finally:
        conn.close()
    return result


def table_exists(conn, table):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def scalar(conn, query):
    row = conn.execute(query).fetchone()
    return int(row[0] or 0)


def root_inventories(args):
    run_dir = Path(args.run_dir)
    proxy_internal = Path(args.proxy_internal_root) if args.proxy_internal_root else run_dir / "proxy-internal"
    roots = {
        "sessions_root": Path(args.sessions_root),
        "agent_homes_root": Path(args.agent_homes_root),
        "checkpoints_root": Path(args.checkpoints_root),
        "prepared_bundle_root": Path(args.prepared_bundle_root),
        "run_control_root": run_dir / "control",
        "run_runtime_root": run_dir / "runtime",
        "run_bridge_root": run_dir / "bridge",
        "proxy_internal_root": proxy_internal,
        "legacy_secret_root": Path(args.legacy_secret_root),
    }
    if args.provider_credential_root:
        roots["provider_credential_root"] = Path(args.provider_credential_root)
    return {name: directory_inventory(path) for name, path in roots.items()}


def host_inventory(args):
    if args.skip_host_commands:
        return {
            "status": "skipped",
            "commands": [],
        }
    commands = [
        command_inventory("runsc_containers", ["runsc", "-root", args.runsc_root, "list"]),
        command_inventory("ip_netns", ["ip", "netns", "list"]),
        command_inventory("ip_links", ["ip", "-o", "link", "show"]),
        command_inventory("nft_tables", ["nft", "list", "tables"]),
    ]
    status = "passed" if all(item["status"] == "passed" for item in commands) else "incomplete"
    return {"status": status, "commands": commands}


def blockers_for_inventory(db, roots, host, args):
    blockers = []
    for name, query in db.get("queries", {}).items():
        if name == "sandbox_contract_rows":
            continue
        if query.get("count", 0) > 0:
            blockers.append({"name": name, "kind": "db_rows", "count": query["count"]})
    for name, inventory in roots.items():
        entries = inventory.get("entries", 0)
        sockets = inventory.get("sockets", 0)
        if name == "proxy_internal_root":
            if sockets > 0:
                blockers.append({"name": name, "kind": "proxy_socket", "count": sockets})
            elif entries > 0:
                blockers.append({"name": name, "kind": "proxy_internal_entries", "count": entries})
            continue
        if name == "legacy_secret_root":
            if inventory["info"].get("exists"):
                blockers.append({"name": name, "kind": "legacy_secret_root_present", "count": entries})
            continue
        if name == "provider_credential_root":
            continue
        if entries > 0:
            blockers.append({"name": name, "kind": "root_entries", "count": entries})
    if args.require_host_inventory and host.get("status") != "passed":
        blockers.append({"name": "host_inventory", "kind": "incomplete", "status": host.get("status")})
    for command in host.get("commands", []):
        count = host_runtime_resource_count(command)
        if count > 0:
            blockers.append({"name": command.get("name", "host_command"), "kind": "host_runtime_resources", "count": count})
    return blockers


def host_runtime_resource_count(command):
    if command.get("status") != "passed":
        return 0
    name = command.get("name", "")
    lines = [line.strip() for line in command.get("output_tail", "").splitlines() if line.strip()]
    if name == "runsc_containers":
        return sum(1 for line in lines if not line.lower().startswith("id "))
    if name == "ip_netns":
        return sum(1 for line in lines if host_runtime_line_matches(line))
    if name == "ip_links":
        return sum(1 for line in lines if host_runtime_line_matches(line))
    if name == "nft_tables":
        return sum(1 for line in lines if host_runtime_line_matches(line))
    return 0


def host_runtime_line_matches(line):
    lower = line.lower()
    return any(marker in lower for marker in ("harness", "phase", "hgen", "hv-", "sv-"))


def inspect_cutover(args):
    db = db_inventory(args.db)
    roots = root_inventories(args)
    host = host_inventory(args)
    blockers = blockers_for_inventory(db, roots, host, args)
    status = "passed"
    if args.expect_clean and blockers:
        status = "failed"
    return {
        "contract": "sandbox-isolation-v1",
        "qualification": "cutover-inventory",
        "status": status,
        "generated_at": utc_now(),
        "expect_clean": bool(args.expect_clean),
        "require_host_inventory": bool(args.require_host_inventory),
        "db": db,
        "roots": roots,
        "host": host,
        "blockers": blockers,
    }


def write_output(path, payload):
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main():
    args = parse_args()
    payload = inspect_cutover(args)
    rendered = json.dumps(payload, indent=2)
    print(rendered)
    if args.output:
        write_output(args.output, payload)
    if payload["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
