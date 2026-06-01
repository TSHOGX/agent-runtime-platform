#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import subprocess
import stat
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
CONTRACT = "sandbox-isolation-v1"
CONTENT_ROOTS = {
    "sessions_root",
    "agent_homes_root",
    "legacy_checkpoints_root",
    "prepared_bundle_root",
    "run_control_root",
    "run_runtime_root",
    "run_bridge_root",
    "run_network_root",
    "run_logs_root",
    "proxy_internal_root",
}
ROOTS_TO_DELETE = {"legacy_secret_root"}
PROTECTED_DELETE_PATHS = {
    "/",
    "/bin",
    "/boot",
    "/dev",
    "/etc",
    "/home",
    "/lib",
    "/lib64",
    "/proc",
    "/root",
    "/run",
    "/sbin",
    "/sys",
    "/tmp",
    "/usr",
    "/var",
    "/var/lib",
    "/var/lib/harness",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Plan or apply direct destructive sandbox-isolation-v1 cutover cleanup from cutover inventory JSON.")
    parser.add_argument("--inventory", required=True, help="Path to cutover-inventory.py JSON output.")
    parser.add_argument("--apply", action="store_true", help="Apply the destructive cleanup plan.")
    parser.add_argument("--confirm-destructive-cutover", default="", help="Must equal sandbox-isolation-v1 when --apply is used.")
    parser.add_argument("--skip-host-commands", action="store_true", help="Do not delete runsc/netns/link/nft host resources.")
    parser.add_argument("--skip-filesystem", action="store_true", help="Do not delete filesystem roots or DB files.")
    parser.add_argument("--output", default="", help="Optional path for JSON evidence.")
    return parser.parse_args()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def load_inventory(path):
    inventory_path = Path(path)
    return json.loads(inventory_path.read_text(encoding="utf-8")), inventory_path


def clean_abs(path):
    value = str(path).strip()
    if not value or not os.path.isabs(value):
        return ""
    return os.path.normpath(value)


def is_within(path, root):
    path = clean_abs(path)
    root = clean_abs(root)
    if not path or not root:
        return False
    return path == root or path.startswith(root.rstrip(os.sep) + os.sep)


def validate_inputs(inventory):
    issues = []
    if inventory.get("contract") != CONTRACT:
        issues.append(issue("inventory", "wrong_contract", got=inventory.get("contract", "")))
    if inventory.get("qualification") != "cutover-inventory":
        issues.append(issue("inventory", "wrong_qualification", got=inventory.get("qualification", "")))
    return issues


def issue(name, kind, **extra):
    item = {"name": name, "kind": kind}
    item.update(extra)
    return item


def build_plan(inventory, skip_host_commands=False, skip_filesystem=False):
    actions = []
    if not skip_filesystem:
        actions.extend(db_file_actions(inventory))
        actions.extend(root_actions(inventory))
    if not skip_host_commands:
        actions.extend(host_resource_actions(inventory))
    return dedupe_delete_actions(actions)


def db_file_actions(inventory):
    actions = []
    db = inventory.get("db", {})
    db_path = clean_abs(db.get("path", ""))
    if not db_path:
        return actions
    for path in [db_path, db_path + "-wal", db_path + "-shm"]:
        if os.path.exists(path):
            actions.append(delete_action(path, "db_file"))
    return actions


def root_actions(inventory):
    actions = []
    roots = inventory.get("roots", {})
    for name, root in roots.items():
        path = clean_abs(root.get("path", ""))
        if not path or not root.get("info", {}).get("exists"):
            continue
        if name in ROOTS_TO_DELETE:
            actions.append(delete_action(path, name))
            continue
        if name not in CONTENT_ROOTS or root.get("entries", 0) <= 0:
            continue
        try:
            entries = sorted(os.listdir(path))
        except FileNotFoundError:
            continue
        for entry in entries:
            actions.append(delete_action(os.path.join(path, entry), name))
    return actions


def delete_action(source, reason):
    return {
        "type": "delete_path",
        "source": clean_abs(source),
        "reason": reason,
    }


def dedupe_delete_actions(actions):
    deduped = []
    delete_sources = []
    command_keys = set()
    for action in actions:
        if action["type"] == "run_command":
            key = tuple(action["command"])
            if key not in command_keys:
                command_keys.add(key)
                deduped.append(action)
            continue
        source = action["source"]
        if any(is_within(source, existing) for existing in delete_sources):
            continue
        retained = []
        delete_sources = []
        for existing_action in deduped:
            if existing_action["type"] == "delete_path" and is_within(existing_action["source"], source):
                continue
            retained.append(existing_action)
            if existing_action["type"] == "delete_path":
                delete_sources.append(existing_action["source"])
        retained.append(action)
        delete_sources.append(source)
        deduped = retained
    return deduped


def host_resource_actions(inventory):
    actions = []
    runsc_root = runsc_root_from_inventory(inventory)
    for command in inventory.get("host", {}).get("commands", []):
        name = command.get("name", "")
        if command.get("status") != "passed":
            continue
        if name == "runsc_containers":
            for container_id in parse_runsc_ids(command.get("output_tail", "")):
                actions.append(command_action(["runsc", "-root", runsc_root, "delete", "-force", container_id], "runsc_container", container_id))
        elif name == "ip_netns":
            for netns in parse_netns_names(command.get("output_tail", "")):
                actions.append(command_action(["ip", "netns", "delete", netns], "ip_netns", netns))
        elif name == "ip_links":
            for link in parse_link_names(command.get("output_tail", "")):
                actions.append(command_action(["ip", "link", "delete", link], "ip_link", link))
        elif name == "nft_tables":
            for family, table in parse_nft_tables(command.get("output_tail", "")):
                actions.append(command_action(["nft", "delete", "table", family, table], "nft_table", f"{family} {table}"))
    return actions


def runsc_root_from_inventory(inventory):
    for command in inventory.get("host", {}).get("commands", []):
        if command.get("name") == "runsc_containers":
            cmd = command.get("command", [])
            for index, arg in enumerate(cmd):
                if arg == "-root" and index + 1 < len(cmd):
                    return cmd[index + 1]
    return "/var/lib/harness/runsc"


def command_action(command, reason, identity):
    return {
        "type": "run_command",
        "command": command,
        "reason": reason,
        "identity": identity,
    }


def parse_runsc_ids(output):
    ids = []
    for line in output.splitlines():
        fields = line.split()
        if not fields or fields[0].lower() == "id":
            continue
        ids.append(fields[0])
    return ids


def parse_netns_names(output):
    names = []
    for line in output.splitlines():
        fields = line.split()
        if fields and host_runtime_line_matches(line):
            names.append(fields[0])
    return names


def parse_link_names(output):
    names = []
    for line in output.splitlines():
        if not host_runtime_line_matches(line) or ":" not in line:
            continue
        parts = line.split(":", 2)
        if len(parts) >= 2:
            names.append(parts[1].strip().split("@", 1)[0])
    return names


def parse_nft_tables(output):
    tables = []
    for line in output.splitlines():
        fields = line.split()
        if len(fields) >= 3 and fields[0] == "table" and host_runtime_line_matches(line):
            tables.append((fields[1], fields[2]))
    return tables


def host_runtime_line_matches(line):
    lower = line.lower()
    return any(marker in lower for marker in ("harness", "phase", "hgen", "hv-", "sv-"))


def apply_actions(actions):
    results = []
    for action in actions:
        if action["type"] == "delete_path":
            results.append(apply_delete(action))
        elif action["type"] == "run_command":
            results.append(apply_command(action))
        else:
            results.append({**action, "status": "failed", "error": "unknown action type"})
    return results


def validate_plan(actions):
    issues = []
    for action in actions:
        if action["type"] == "delete_path":
            target_issue = unsafe_delete_target_issue(action["source"])
            if target_issue:
                issues.append(target_issue)
        elif action["type"] == "run_command" and not action.get("command"):
            issues.append(issue("action", "empty_command", action=action))
    return issues


def unsafe_delete_target_issue(path):
    path = clean_abs(path)
    if not path:
        return issue("delete_path", "not_absolute", path=path)
    if path in PROTECTED_DELETE_PATHS:
        return issue("delete_path", "protected_path", path=path)
    repo = clean_abs(REPO_ROOT)
    if path == repo:
        return issue("delete_path", "protected_repo_root", path=path)
    if clean_abs(os.path.dirname(path)) == repo:
        return issue("delete_path", "protected_repo_child", path=path)
    return None


def apply_delete(action):
    source = action["source"]
    target_issue = unsafe_delete_target_issue(source)
    if target_issue:
        return {**action, "status": "failed", "error": target_issue["kind"]}
    if not os.path.lexists(source):
        return {**action, "status": "skipped", "error": "source absent"}
    try:
        info = os.lstat(source)
        if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode):
            if os.path.ismount(source):
                return {**action, "status": "failed", "error": "refusing to delete mountpoint"}
            shutil.rmtree(source)
        else:
            os.unlink(source)
    except Exception as err:
        return {**action, "status": "failed", "error": str(err)}
    return {**action, "status": "passed"}


def apply_command(action):
    try:
        result = subprocess.run(action["command"], text=True, capture_output=True, timeout=15)
    except subprocess.TimeoutExpired as err:
        output = ((err.stdout or "") + (err.stderr or "")).strip()
        return {**action, "status": "failed", "returncode": 124, "output_tail": tail(output)}
    output = (result.stdout + result.stderr).strip()
    if result.returncode != 0 and command_output_means_absent(output):
        return {**action, "status": "skipped", "returncode": result.returncode, "output_tail": tail(output), "error": "already absent"}
    status = "passed" if result.returncode == 0 else "failed"
    return {**action, "status": status, "returncode": result.returncode, "output_tail": tail(output)}


def command_output_means_absent(output):
    lower = output.lower()
    return any(marker in lower for marker in ("does not exist", "not found", "no such", "cannot find device"))


def tail(value, limit=12000):
    if len(value) <= limit:
        return value
    return value[-limit:]


def cleanup(args):
    inventory, inventory_path = load_inventory(args.inventory)
    issues = validate_inputs(inventory)
    if args.apply and args.confirm_destructive_cutover != CONTRACT:
        issues.append(issue("confirmation", "missing_destructive_confirmation"))
    actions = [] if issues else build_plan(inventory, skip_host_commands=args.skip_host_commands, skip_filesystem=args.skip_filesystem)
    if actions:
        issues.extend(validate_plan(actions))
    results = []
    if args.apply and not issues:
        results = apply_actions(actions)
        for result in results:
            if result.get("status") == "failed":
                issues.append(issue("action", "failed", action=result))
    status = "failed" if issues else ("passed" if args.apply else "planned")
    return {
        "contract": CONTRACT,
        "qualification": "cutover-cleanup",
        "status": status,
        "generated_at": utc_now(),
        "apply": bool(args.apply),
        "inventory_path": str(inventory_path),
        "delete_mode": "direct",
        "actions": actions,
        "results": results,
        "issues": issues,
    }


def write_output(path, payload):
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main():
    args = parse_args()
    payload = cleanup(args)
    print(json.dumps(payload, indent=2))
    if args.output:
        write_output(args.output, payload)
    if payload["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
