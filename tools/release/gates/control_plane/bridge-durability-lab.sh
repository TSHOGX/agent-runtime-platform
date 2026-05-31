#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
RUNSC="${RUNSC:-runsc}"
ROOTFS="${HARNESS_LAB_ROOTFS:-$REPO_ROOT/sandbox-image/rootfs}"
WORKDIR="${HARNESS_BRIDGE_LAB_WORKDIR:-$(mktemp -d /tmp/harness-bridge-durability-lab.XXXXXX)}"
BUNDLE_DIR="$WORKDIR/bundle"
BRIDGE_DIR="$WORKDIR/bridge"
RUNSC_ROOT="$WORKDIR/runsc-root"
CID="bridge-durability-lab-$$"

cleanup() {
  "$RUNSC" --root "$RUNSC_ROOT" delete -f "$CID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ ! -x "$(command -v "$RUNSC")" ]]; then
  echo "runsc not found: $RUNSC" >&2
  exit 1
fi
if [[ ! -d "$ROOTFS" ]]; then
  echo "rootfs not found: $ROOTFS" >&2
  exit 1
fi

mkdir -p "$BUNDLE_DIR" "$BRIDGE_DIR/tmp" "$BRIDGE_DIR/outbox" "$BRIDGE_DIR/inbox" "$BRIDGE_DIR/heartbeat" "$RUNSC_ROOT"

python3 - "$BUNDLE_DIR/config.json" "$ROOTFS" "$BRIDGE_DIR" <<'PY'
import json
import sys

config_path, rootfs, bridge_dir = sys.argv[1:4]
sandbox_writer = r'''
import json
import os
from pathlib import Path
import time
import uuid

root = Path("/harness-control/bridge")
for name in ("tmp", "outbox", "inbox", "heartbeat"):
    (root / name).mkdir(parents=True, exist_ok=True)

envelope = {
    "message_id": "bridge-durability-" + uuid.uuid4().hex,
    "request_id": "bridge-durability",
    "type": "heartbeat",
    "session_id": "sess_bridge_lab",
    "generation_id": "gen_bridge_lab",
    "payload": {
        "writer": "sandbox",
        "pid": os.getpid(),
        "time_unix_ns": time.time_ns(),
    },
}
payload = json.dumps(envelope, separators=(",", ":")).encode("utf-8") + b"\n"
tmp_path = root / "tmp" / (str(uuid.uuid4()) + ".json")
target = root / "outbox" / "00000000000000000001.json"
with open(tmp_path, "xb") as f:
    f.write(payload)
    f.flush()
    os.fsync(f.fileno())
os.replace(tmp_path, target)
dirfd = os.open(root / "outbox", os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(dirfd)
finally:
    os.close(dirfd)
print(json.dumps({"message_id": envelope["message_id"], "target": str(target)}), flush=True)
'''

capabilities = {
    "bounding": [],
    "effective": [],
    "inheritable": [],
    "permitted": [],
    "ambient": [],
}
config = {
    "ociVersion": "1.0.2",
    "process": {
        "terminal": False,
        "user": {"uid": 0, "gid": 0},
        "args": ["/usr/bin/python3", "-c", sandbox_writer],
        "env": ["PATH=/usr/local/bin:/usr/bin:/bin", "LANG=C.UTF-8"],
        "cwd": "/",
        "capabilities": capabilities,
        "rlimits": [{"type": "RLIMIT_NOFILE", "hard": 1024, "soft": 1024}],
        "noNewPrivileges": True,
    },
    "root": {"path": rootfs, "readonly": False},
    "hostname": "bridge-durability-lab",
    "mounts": [
        {"destination": "/proc", "type": "proc", "source": "proc"},
        {
            "destination": "/dev",
            "type": "tmpfs",
            "source": "tmpfs",
            "options": ["nosuid", "strictatime", "mode=755", "size=65536k"],
        },
        {
            "destination": "/tmp",
            "type": "tmpfs",
            "source": "tmpfs",
            "options": ["nosuid", "nodev", "mode=1777", "size=65536k"],
        },
        {
            "destination": "/harness-control/bridge",
            "type": "bind",
            "source": bridge_dir,
            "options": ["rbind", "rw"],
            "annotations": {
                "dev.gvisor.spec.mount./harness-control/bridge.type": "bind",
                "dev.gvisor.spec.mount./harness-control/bridge.share": "exclusive",
            },
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
with open(config_path, "w", encoding="utf-8") as f:
    json.dump(config, f, indent=2)
    f.write("\n")
PY

python3 - "$BUNDLE_DIR/config.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as f:
    config = json.load(f)
for mount in config.get("mounts", []):
    if mount.get("destination") == "/harness-control/bridge":
        annotations = mount.get("annotations", {})
        if annotations.get("dev.gvisor.spec.mount./harness-control/bridge.share") == "exclusive":
            raise SystemExit(0)
raise SystemExit("bridge mount missing exclusive gVisor annotation")
PY

COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD)"
RUNSC_VERSION="$("$RUNSC" --version | tr '\n' ' ')"

echo "bridge durability lab"
echo "commit: $COMMIT"
echo "runsc: $RUNSC_VERSION"
echo "workdir: $WORKDIR"
echo "bridge_dir: $BRIDGE_DIR"

(
  cd "$BUNDLE_DIR"
  "$RUNSC" --root "$RUNSC_ROOT" run "$CID"
) >"$WORKDIR/runsc.stdout" 2>"$WORKDIR/runsc.stderr"

echo "sandbox writer output:"
cat "$WORKDIR/runsc.stdout"

echo "starting host reader after sandbox writer exit"
(
  cd "$REPO_ROOT/orchestrator"
  HARNESS_BRIDGE_LAB_DIR="$BRIDGE_DIR" go test -tags bridgelab -count=1 ./internal/bridge -run TestBridgeDurabilityLabReadsSandboxFsyncedMessage -v
) | tee "$WORKDIR/host-reader.log"

python3 - "$WORKDIR/evidence.json" "$COMMIT" "$RUNSC_VERSION" "$BUNDLE_DIR/config.json" "$BRIDGE_DIR" "$WORKDIR" <<'PY'
import json
import sys

path, commit, runsc_version, config_path, bridge_dir, workdir = sys.argv[1:7]
evidence = {
    "commit": commit,
    "runsc_version": runsc_version.strip(),
    "config_path": config_path,
    "bridge_dir": bridge_dir,
    "workdir": workdir,
    "sandbox_stdout": f"{workdir}/runsc.stdout",
    "sandbox_stderr": f"{workdir}/runsc.stderr",
    "host_reader_log": f"{workdir}/host-reader.log",
    "result": "passed",
}
with open(path, "w", encoding="utf-8") as f:
    json.dump(evidence, f, indent=2)
    f.write("\n")
print(path)
PY
