#!/usr/bin/env python3
import argparse
import json
import os
import pwd
import grp
import shutil
import stat
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Validate the Phase 7 rootful secret permission model.")
    parser.add_argument("--config", default=os.environ.get("PHASE7_CONFIG", "config/harness.yaml"))
    parser.add_argument("--secrets-root", default=os.environ.get("PHASE7_SECRETS_ROOT", ""))
    parser.add_argument("--readers-gid", type=int, default=int(os.environ.get("PHASE7_SECRET_READERS_GID", "0")))
    parser.add_argument("--owner-user", default=os.environ.get("PHASE7_SECRET_OWNER", "orchestrator"))
    parser.add_argument("--readers-group", default=os.environ.get("PHASE7_SECRET_READERS_GROUP", "harness-secret-readers"))
    parser.add_argument("--agent-uid", type=int, default=int(os.environ.get("PHASE7_AGENT_UID", "65534")))
    parser.add_argument("--agent-gid", type=int, default=int(os.environ.get("PHASE7_AGENT_GID", "65534")))
    parser.add_argument("--other-uid", type=int, default=int(os.environ.get("PHASE7_SECRET_OTHER_UID", "65533")))
    parser.add_argument("--other-gid", type=int, default=int(os.environ.get("PHASE7_SECRET_OTHER_GID", "65533")))
    parser.add_argument("--runsc", default=os.environ.get("RUNSC", "runsc"))
    parser.add_argument("--rootfs", default=os.environ.get("PHASE7_LAB_ROOTFS", "sandbox-image/rootfs"))
    parser.add_argument("--skip-sandbox", action="store_true")
    return parser.parse_args()


def load_secret_config(config_path):
    root = ""
    readers_gid = 0
    in_secrets = False
    secrets_indent = None
    with open(config_path, encoding="utf-8") as handle:
        for raw in handle:
            line = raw.split("#", 1)[0].rstrip()
            if not line.strip():
                continue
            stripped = line.strip()
            indent = len(line) - len(line.lstrip(" "))
            if stripped == "secrets:":
                in_secrets = True
                secrets_indent = indent
                continue
            if in_secrets and indent <= secrets_indent:
                in_secrets = False
            if not in_secrets or ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            value = value.strip().strip("'\"")
            if key == "root":
                root = value
            elif key == "readers_gid":
                readers_gid = int(value)
    return root, readers_gid


def user_by_name(name):
    try:
        return pwd.getpwnam(name)
    except KeyError as err:
        raise RuntimeError(f"required user {name!r} does not exist") from err


def group_by_name(name):
    try:
        return grp.getgrnam(name)
    except KeyError as err:
        raise RuntimeError(f"required group {name!r} does not exist") from err


def usernames_for_uid(uid):
    return {entry.pw_name for entry in pwd.getpwall() if entry.pw_uid == uid}


def group_member_users(group_entry):
    members = set(group_entry.gr_mem)
    for entry in pwd.getpwall():
        if entry.pw_gid == group_entry.gr_gid:
            members.add(entry.pw_name)
    return members


def assert_mode_owner_group(path, want_mode, owner_uid, readers_gid):
    info = os.stat(path)
    mode = stat.S_IMODE(info.st_mode)
    if mode != want_mode:
        raise RuntimeError(f"{path} mode {mode:04o}, want {want_mode:04o}")
    if info.st_uid != owner_uid:
        raise RuntimeError(f"{path} uid {info.st_uid}, want {owner_uid}")
    if info.st_gid != readers_gid:
        raise RuntimeError(f"{path} gid {info.st_gid}, want {readers_gid}")


def run_checked(command, expect_success=True, cwd=None):
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True)
    if expect_success and result.returncode != 0:
        raise RuntimeError(f"{command!r} failed: stdout={result.stdout!r} stderr={result.stderr!r}")
    if not expect_success and result.returncode == 0:
        raise RuntimeError(f"{command!r} unexpectedly succeeded: stdout={result.stdout!r}")
    return result


def write_lab_secret(secrets_root, owner_uid, readers_gid):
    secret_id = "phase7_permission_lab_" + uuid.uuid4().hex
    version = "local"
    secret_dir = Path(secrets_root) / secret_id
    secret_path = secret_dir / version
    secret_dir.mkdir(mode=0o750)
    os.chown(secret_dir, owner_uid, readers_gid)
    os.chmod(secret_dir, 0o750)
    fd = os.open(secret_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o440)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write("phase7-secret-lab\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chown(secret_path, owner_uid, readers_gid)
    os.chmod(secret_path, 0o440)
    return secret_id, version, secret_dir, secret_path


def verify_host_access(secret_path, agent_uid, agent_gid, readers_gid, other_uid, other_gid):
    run_checked(
        [
            "setpriv",
            "--reuid",
            str(agent_uid),
            "--regid",
            str(agent_gid),
            "--groups",
            str(readers_gid),
            "--",
            "cat",
            str(secret_path),
        ]
    )
    run_checked(
        [
            "setpriv",
            "--reuid",
            str(other_uid),
            "--regid",
            str(other_gid),
            "--clear-groups",
            "--",
            "cat",
            str(secret_path),
        ],
        expect_success=False,
    )


def verify_sandbox_access(args, secrets_root, secret_id, version, readers_gid):
    runsc = shutil.which(args.runsc)
    if not runsc:
        raise RuntimeError(f"runsc not found: {args.runsc}")
    rootfs = Path(args.rootfs).resolve()
    if not rootfs.is_dir():
        raise RuntimeError(f"rootfs not found: {rootfs}")
    with tempfile.TemporaryDirectory(prefix="harness-phase7-secret-lab.") as workdir:
        bundle = Path(workdir) / "bundle"
        runsc_root = Path(workdir) / "runsc-root"
        bundle.mkdir()
        runsc_root.mkdir()
        config = {
            "ociVersion": "1.0.2",
            "process": {
                "terminal": False,
                "user": {
                    "uid": args.agent_uid,
                    "gid": args.agent_gid,
                    "additionalGids": [readers_gid],
                },
                "args": ["/bin/cat", f"/harness-secrets/{secret_id}/{version}"],
                "env": ["PATH=/usr/local/bin:/usr/bin:/bin"],
                "cwd": "/",
                "capabilities": {
                    "bounding": [],
                    "effective": [],
                    "inheritable": [],
                    "permitted": [],
                    "ambient": [],
                },
                "noNewPrivileges": True,
            },
            "root": {"path": str(rootfs), "readonly": False},
            "hostname": "phase7-secret-lab",
            "mounts": [
                {"destination": "/proc", "type": "proc", "source": "proc"},
                {
                    "destination": "/harness-secrets",
                    "type": "bind",
                    "source": str(Path(secrets_root).resolve()),
                    "options": ["rbind", "ro", "nosuid", "nodev", "noexec"],
                },
            ],
            "linux": {
                "namespaces": [
                    {"type": "pid"},
                    {"type": "ipc"},
                    {"type": "uts"},
                    {"type": "mount"},
                ]
            },
        }
        (bundle / "config.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
        cid = "phase7-secret-lab-" + uuid.uuid4().hex
        try:
            run_checked([runsc, "--root", str(runsc_root), "run", cid], cwd=bundle)
        finally:
            subprocess.run([runsc, "--root", str(runsc_root), "delete", "-f", cid], capture_output=True)


def main():
    args = parse_args()
    config_root, config_gid = load_secret_config(args.config)
    secrets_root = args.secrets_root or config_root
    readers_gid = args.readers_gid or config_gid
    if not secrets_root:
        raise RuntimeError("secrets root is required")
    if readers_gid <= 0:
        raise RuntimeError("secret readers gid must be > 0")

    owner = user_by_name(args.owner_user)
    readers_group = group_by_name(args.readers_group)
    if readers_group.gr_gid != readers_gid:
        raise RuntimeError(f"group {args.readers_group} gid {readers_group.gr_gid}, want {readers_gid}")

    members = group_member_users(readers_group)
    agent_names = usernames_for_uid(args.agent_uid)
    if not agent_names.intersection(members):
        raise RuntimeError(f"uid {args.agent_uid} is not a member of {args.readers_group}")
    unexpected = members - agent_names - {args.owner_user}
    if unexpected:
        raise RuntimeError(f"unexpected {args.readers_group} members: {sorted(unexpected)}")

    assert_mode_owner_group(secrets_root, 0o750, owner.pw_uid, readers_gid)
    secret_id, version, secret_dir, secret_path = write_lab_secret(secrets_root, owner.pw_uid, readers_gid)
    try:
        assert_mode_owner_group(secret_dir, 0o750, owner.pw_uid, readers_gid)
        assert_mode_owner_group(secret_path, 0o440, owner.pw_uid, readers_gid)
        verify_host_access(secret_path, args.agent_uid, args.agent_gid, readers_gid, args.other_uid, args.other_gid)
        if not args.skip_sandbox:
            verify_sandbox_access(args, secrets_root, secret_id, version, readers_gid)
        print(
            json.dumps(
                {
                    "result": "passed",
                    "secrets_root": str(secrets_root),
                    "owner_user": args.owner_user,
                    "readers_group": args.readers_group,
                    "readers_gid": readers_gid,
                    "agent_uid": args.agent_uid,
                    "sandbox_checked": not args.skip_sandbox,
                },
                indent=2,
            )
        )
    finally:
        shutil.rmtree(secret_dir, ignore_errors=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"phase7 secret permission lab failed: {err}", file=sys.stderr)
        raise SystemExit(1)
