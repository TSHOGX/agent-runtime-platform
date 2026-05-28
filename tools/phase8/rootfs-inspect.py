#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import stat
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ROOTFS = REPO_ROOT / "sandbox-image" / "rootfs"


def parse_args():
    parser = argparse.ArgumentParser(description="Inspect sandbox rootfs hygiene for sandbox-isolation-v1.")
    parser.add_argument("--rootfs", default=str(DEFAULT_ROOTFS), help="Rootfs directory to inspect.")
    parser.add_argument("--output", default="", help="Optional path for JSON evidence.")
    return parser.parse_args()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def mode_string(mode):
    return oct(stat.S_IMODE(mode))


def lstat_info(path):
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return {"exists": False}
    kind = "other"
    if stat.S_ISDIR(st.st_mode):
        kind = "directory"
    elif stat.S_ISREG(st.st_mode):
        kind = "file"
    elif stat.S_ISLNK(st.st_mode):
        kind = "symlink"
    return {
        "exists": True,
        "kind": kind,
        "mode": mode_string(st.st_mode),
        "uid": st.st_uid,
        "gid": st.st_gid,
        "size": st.st_size,
        "target": os.readlink(path) if stat.S_ISLNK(st.st_mode) else "",
    }


def check_result(name, path, passed, detail, info=None):
    return {
        "name": name,
        "path": str(path),
        "status": "passed" if passed else "failed",
        "detail": detail,
        "info": info or lstat_info(path),
    }


def is_empty_directory(path):
    return path.is_dir() and not path.is_symlink() and not any(path.iterdir())


def require_empty_real_dir(rootfs, rel):
    path = rootfs / rel
    info = lstat_info(path)
    passed = info.get("kind") == "directory" and is_empty_directory(path)
    return check_result(
        f"{rel}_is_empty_real_directory",
        path,
        passed,
        f"/{rel} must exist as an empty real directory for exact bind overlay",
        info,
    )


def require_real_file(rootfs, rel):
    path = rootfs / rel
    info = lstat_info(path)
    passed = info.get("kind") == "file"
    return check_result(
        f"{rel.replace('/', '_')}_is_real_file",
        path,
        passed,
        f"/{rel} must exist as a real file, not a symlink",
        info,
    )


def require_absent(rootfs, rel):
    path = rootfs / rel
    info = lstat_info(path)
    return check_result(
        f"{rel.replace('/', '_')}_absent",
        path,
        not info["exists"],
        f"/{rel} must be absent from the baked rootfs",
        info,
    )


def require_absent_or_empty(rootfs, rel):
    path = rootfs / rel
    info = lstat_info(path)
    passed = not info["exists"] or (info.get("kind") == "directory" and is_empty_directory(path))
    return check_result(
        f"{rel.replace('/', '_')}_absent_or_empty",
        path,
        passed,
        f"/{rel} must be absent or an empty real directory",
        info,
    )


def require_absent_or_harmless_legacy_dir(rootfs, rel):
    path = rootfs / rel
    info = lstat_info(path)
    if not info["exists"]:
        passed = True
    else:
        mode = int(info.get("mode", "0"), 8)
        passed = (
            info.get("kind") == "directory"
            and info.get("uid") == 0
            and info.get("gid") == 0
            and mode & 0o022 == 0
            and is_empty_directory(path)
        )
    return check_result(
        f"{rel}_absent_or_harmless",
        path,
        passed,
        f"/{rel} must be absent or empty, root-owned, and not group/world-writable",
        info,
    )


def check_no_gvisor_filestore(rootfs):
    paths = sorted(rootfs.glob(".gvisor.filestore.*")) if rootfs.exists() else []
    return {
        "name": "no_baked_gvisor_filestore_files",
        "path": str(rootfs),
        "status": "passed" if not paths else "failed",
        "detail": "baked rootfs must not retain gVisor filestore scratch files",
        "matches": [str(path) for path in paths],
    }


def tree_digest(rootfs):
    digest = hashlib.sha256()
    for current, dirs, files in os.walk(rootfs, topdown=True, followlinks=False):
        dirs.sort()
        files.sort()
        current_path = Path(current)
        for name in dirs + files:
            path = current_path / name
            rel = path.relative_to(rootfs).as_posix()
            st = os.lstat(path)
            if stat.S_ISDIR(st.st_mode):
                kind = "dir"
            elif stat.S_ISREG(st.st_mode):
                kind = "file"
            elif stat.S_ISLNK(st.st_mode):
                kind = "symlink"
            else:
                kind = "other"
            digest.update(f"{kind} {stat.S_IMODE(st.st_mode):o} {st.st_uid}:{st.st_gid} {st.st_size} {rel}\0".encode())
            if kind == "symlink":
                digest.update(os.readlink(path).encode())
                digest.update(b"\0")
            elif kind == "file":
                with open(path, "rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def inspect_rootfs(rootfs):
    rootfs = Path(rootfs)
    checks = []
    root_info = lstat_info(rootfs)
    checks.append(
        check_result(
            "rootfs_is_real_directory",
            rootfs,
            root_info.get("kind") == "directory",
            "rootfs path must exist as a real directory",
            root_info,
        )
    )
    if root_info.get("kind") == "directory":
        checks.extend(
            [
                require_empty_real_dir(rootfs, "workspace"),
                require_empty_real_dir(rootfs, "agent-home"),
                require_real_file(rootfs, "etc/hosts"),
                require_absent_or_harmless_legacy_dir(rootfs, "sessions"),
                require_absent_or_harmless_legacy_dir(rootfs, "agent-homes"),
                require_absent(rootfs, "harness-secrets"),
                require_absent_or_empty(rootfs, "root/.claude"),
                require_absent_or_empty(rootfs, "root/.cache"),
                require_absent(rootfs, "root/.claude.json"),
                check_no_gvisor_filestore(rootfs),
            ]
        )
    status = "passed" if all(check["status"] == "passed" for check in checks) else "failed"
    return {
        "contract": "sandbox-isolation-v1",
        "qualification": "rootfs-image-hygiene",
        "status": status,
        "generated_at": utc_now(),
        "rootfs": str(rootfs),
        "rootfs_digest": tree_digest(rootfs) if root_info.get("kind") == "directory" else "",
        "checks": checks,
    }


def write_output(path, payload):
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main():
    args = parse_args()
    payload = inspect_rootfs(args.rootfs)
    rendered = json.dumps(payload, indent=2)
    print(rendered)
    if args.output:
        write_output(args.output, payload)
    if payload["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
