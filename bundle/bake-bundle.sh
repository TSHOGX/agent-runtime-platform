#!/usr/bin/env bash
set -Eeuo pipefail
trap '' PIPE

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

ROOTFS_DIR="${ROOTFS_DIR:-${REPO_ROOT}/sandbox-image/rootfs}"
BUNDLE_DIR="${BUNDLE_DIR:-${SCRIPT_DIR}/out/phase2-template-bundle}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-${SCRIPT_DIR}/checkpoints/phase2-template}"
RUNSC_ROOT="${RUNSC_ROOT:-/var/lib/harness/runsc}"
RUNSC_LOG_DIR="${RUNSC_LOG_DIR:-/var/lib/harness/logs}"
SESSIONS_ROOT="${SESSIONS_ROOT:-/var/lib/harness/sessions}"
AGENT_HOMES_ROOT="${AGENT_HOMES_ROOT:-/var/lib/harness/agent-homes}"
CONTROL_DIR="${CONTROL_DIR:-/var/lib/harness/control/phase2-template}"
CONTAINER_ID="${CONTAINER_ID:-phase2-warm-template}"
RUNSC_NETWORK="${RUNSC_NETWORK:-sandbox}"
RUNSC_PLATFORM="${RUNSC_PLATFORM:-systrap}"
RUNSC_OVERLAY2="${RUNSC_OVERLAY2:-none}"
MEMORY_LIMIT_BYTES="${MEMORY_LIMIT_BYTES:-1073741824}"
CPU_SHARES="${CPU_SHARES:-1024}"
PIDS_LIMIT="${PIDS_LIMIT:-256}"
RUNSC_CHECKPOINT_DIRECT="${RUNSC_CHECKPOINT_DIRECT:-0}"
RUNSC_CHECKPOINT_COMPRESSION="${RUNSC_CHECKPOINT_COMPRESSION:-none}"
RUNSC_CHECKPOINT_EXCLUDE_COMMITTED_ZERO_PAGES="${RUNSC_CHECKPOINT_EXCLUDE_COMMITTED_ZERO_PAGES:-0}"
SKIP_CHECKPOINT="${SKIP_CHECKPOINT:-0}"

log() {
  printf '[bake-bundle] %s\n' "$*"
}

if [ ! -x "${ROOTFS_DIR}/usr/local/bin/harness-agent-entrypoint" ]; then
  echo "rootfs is missing harness-agent-entrypoint; run sandbox-image/build-rootfs.sh first." >&2
  exit 1
fi

if ! command -v runsc >/dev/null 2>&1; then
  echo "runsc is required." >&2
  exit 1
fi

mkdir -p "${BUNDLE_DIR}" "${CHECKPOINT_DIR}" "${RUNSC_ROOT}" "${RUNSC_LOG_DIR}" "${SESSIONS_ROOT}" "${AGENT_HOMES_ROOT}" "${CONTROL_DIR}"
rm -f "${CONTROL_DIR}/session.json" "${CONTROL_DIR}/session.env"

log "writing OCI config to ${BUNDLE_DIR}/config.json"
ROOTFS_DIR="${ROOTFS_DIR}" \
SESSIONS_ROOT="${SESSIONS_ROOT}" \
AGENT_HOMES_ROOT="${AGENT_HOMES_ROOT}" \
CONTROL_DIR="${CONTROL_DIR}" \
REPO_ROOT="${REPO_ROOT}" \
MEMORY_LIMIT_BYTES="${MEMORY_LIMIT_BYTES}" \
CPU_SHARES="${CPU_SHARES}" \
PIDS_LIMIT="${PIDS_LIMIT}" \
python3 - <<'PY' > "${BUNDLE_DIR}/config.json"
import json
import os

rootfs = os.environ["ROOTFS_DIR"]
sessions_root = os.environ["SESSIONS_ROOT"]
agent_homes_root = os.environ["AGENT_HOMES_ROOT"]
control_dir = os.environ["CONTROL_DIR"]
repo_root = os.environ["REPO_ROOT"]

config = {
    "ociVersion": "1.0.2",
    "process": {
        "terminal": False,
        "user": {"uid": 0, "gid": 0},
        "args": ["/usr/local/bin/harness-agent-entrypoint"],
        "env": [
            "PATH=/usr/local/bin:/usr/bin:/bin",
            "LANG=C.UTF-8",
            "MPLCONFIGDIR=/tmp/matplotlib",
        ],
        "cwd": "/",
        "capabilities": {
            "bounding": ["CAP_AUDIT_WRITE", "CAP_CHOWN", "CAP_KILL", "CAP_NET_BIND_SERVICE", "CAP_SETGID", "CAP_SETUID"],
            "effective": ["CAP_AUDIT_WRITE", "CAP_CHOWN", "CAP_KILL", "CAP_NET_BIND_SERVICE", "CAP_SETGID", "CAP_SETUID"],
            "inheritable": [],
            "permitted": ["CAP_AUDIT_WRITE", "CAP_CHOWN", "CAP_KILL", "CAP_NET_BIND_SERVICE", "CAP_SETGID", "CAP_SETUID"],
            "ambient": [],
        },
        "rlimits": [{"type": "RLIMIT_NOFILE", "hard": 1024, "soft": 1024}],
        "noNewPrivileges": True,
    },
    "root": {"path": rootfs, "readonly": False},
    "hostname": "phase2-template",
    "mounts": [
        {"destination": "/proc", "type": "proc", "source": "proc"},
        {
            "destination": "/dev",
            "type": "tmpfs",
            "source": "tmpfs",
            "options": ["nosuid", "strictatime", "mode=755", "size=65536k"],
        },
        {
            "destination": "/dev/pts",
            "type": "devpts",
            "source": "devpts",
            "options": [
                "nosuid",
                "noexec",
                "newinstance",
                "ptmxmode=0666",
                "mode=0620",
                "gid=5",
            ],
        },
        {
            "destination": "/dev/shm",
            "type": "tmpfs",
            "source": "shm",
            "options": ["nosuid", "noexec", "nodev", "mode=1777", "size=65536k"],
        },
        {
            "destination": "/dev/mqueue",
            "type": "mqueue",
            "source": "mqueue",
            "options": ["nosuid", "noexec", "nodev"],
        },
        {
            "destination": "/sys",
            "type": "sysfs",
            "source": "sysfs",
            "options": ["nosuid", "noexec", "nodev", "ro"],
        },
        {
            "destination": "/sessions",
            "type": "bind",
            "source": sessions_root,
            "options": ["rbind", "rw"],
        },
        {
            "destination": "/agent-homes",
            "type": "bind",
            "source": agent_homes_root,
            "options": ["rbind", "rw"],
        },
        {
            "destination": "/harness-control",
            "type": "bind",
            "source": control_dir,
            "options": ["rbind", "rw"],
        },
        {
            "destination": "/schema-pack",
            "type": "bind",
            "source": os.path.join(repo_root, "schema-pack"),
            "options": ["rbind", "ro"],
        },
    ],
    "linux": {
        "resources": {
            "memory": {"limit": int(os.environ["MEMORY_LIMIT_BYTES"])},
            "cpu": {"shares": int(os.environ["CPU_SHARES"])},
            "pids": {"limit": int(os.environ["PIDS_LIMIT"])},
        },
        "namespaces": [
            {"type": "pid"},
            {"type": "ipc"},
            {"type": "uts"},
            {"type": "mount"},
        ],
    },
}

config["linux"]["namespaces"].append({"type": "network", "path": "/var/run/netns/phase1-demo"})

json.dump(config, fp=os.sys.stdout, indent=2)
print()
PY

cat > "${BUNDLE_DIR}/phase2-manifest.env" <<EOF
ROOTFS_DIR='${ROOTFS_DIR}'
BUNDLE_DIR='${BUNDLE_DIR}'
CHECKPOINT_DIR='${CHECKPOINT_DIR}'
RUNSC_ROOT='${RUNSC_ROOT}'
RUNSC_LOG_DIR='${RUNSC_LOG_DIR}'
SESSIONS_ROOT='${SESSIONS_ROOT}'
AGENT_HOMES_ROOT='${AGENT_HOMES_ROOT}'
CONTROL_DIR='${CONTROL_DIR}'
CONTAINER_ID='${CONTAINER_ID}'
RUNSC_NETWORK='${RUNSC_NETWORK}'
RUNSC_PLATFORM='${RUNSC_PLATFORM}'
RUNSC_OVERLAY2='${RUNSC_OVERLAY2}'
EOF

if [ "${SKIP_CHECKPOINT}" = "1" ]; then
  log "SKIP_CHECKPOINT=1; bundle generated without checkpoint"
  exit 0
fi

if [ "${RUNSC_NETWORK}" = "host" ]; then
  echo "runsc checkpoint is not supported with RUNSC_NETWORK=host on this host; use RUNSC_NETWORK=sandbox." >&2
  exit 1
fi

log "cleaning stale runsc container ${CONTAINER_ID}"
runsc -root "${RUNSC_ROOT}" kill "${CONTAINER_ID}" KILL >/dev/null 2>&1 || true
runsc -root "${RUNSC_ROOT}" delete "${CONTAINER_ID}" >/dev/null 2>&1 || true
rm -rf -- "${CHECKPOINT_DIR}"
mkdir -p "${CHECKPOINT_DIR}"

log "starting template container and checkpointing it while it waits for control input"
set +e
runsc \
  -root "${RUNSC_ROOT}" \
  -platform "${RUNSC_PLATFORM}" \
  -overlay2 "${RUNSC_OVERLAY2}" \
  -network "${RUNSC_NETWORK}" \
  -debug-log "${RUNSC_LOG_DIR}/" \
  run \
  -detach \
  -bundle "${BUNDLE_DIR}" \
  -pid-file "${BUNDLE_DIR}/template.pid" \
  "${CONTAINER_ID}"
run_status=$?
set -e
if [ "${run_status}" -ne 0 ]; then
  if ! runsc -root "${RUNSC_ROOT}" state "${CONTAINER_ID}" >/dev/null 2>&1; then
    echo "runsc run failed with status ${run_status}" >&2
    exit "${run_status}"
  fi
fi

sleep 0.2
CHECKPOINT_STDOUT="${BUNDLE_DIR}/checkpoint.stdout" \
CHECKPOINT_STDERR="${BUNDLE_DIR}/checkpoint.stderr" \
RUNSC_ROOT="${RUNSC_ROOT}" \
CHECKPOINT_DIR="${CHECKPOINT_DIR}" \
CONTAINER_ID="${CONTAINER_ID}" \
RUNSC_OVERLAY2="${RUNSC_OVERLAY2}" \
RUNSC_CHECKPOINT_DIRECT="${RUNSC_CHECKPOINT_DIRECT}" \
RUNSC_CHECKPOINT_COMPRESSION="${RUNSC_CHECKPOINT_COMPRESSION}" \
RUNSC_CHECKPOINT_EXCLUDE_COMMITTED_ZERO_PAGES="${RUNSC_CHECKPOINT_EXCLUDE_COMMITTED_ZERO_PAGES}" \
python3 - <<'PY'
import os
import subprocess
import sys

cmd = [
    "runsc",
    "-root",
    os.environ["RUNSC_ROOT"],
    "-overlay2",
    os.environ["RUNSC_OVERLAY2"],
    "checkpoint",
]

if os.environ["RUNSC_CHECKPOINT_DIRECT"] == "1":
    cmd.append("-direct")

cmd.extend([
    "-compression",
    os.environ["RUNSC_CHECKPOINT_COMPRESSION"],
])

if os.environ["RUNSC_CHECKPOINT_EXCLUDE_COMMITTED_ZERO_PAGES"] == "1":
    cmd.append("-exclude-committed-zero-pages")

cmd.extend([
    "-image-path",
    os.environ["CHECKPOINT_DIR"],
    os.environ["CONTAINER_ID"],
])

with open(os.environ["CHECKPOINT_STDOUT"], "wb") as stdout, open(
    os.environ["CHECKPOINT_STDERR"], "wb"
) as stderr:
    proc = subprocess.run(cmd, stdout=stdout, stderr=stderr, start_new_session=True)

checkpoint_img = os.path.join(os.environ["CHECKPOINT_DIR"], "checkpoint.img")
checkpoint_exists = os.path.exists(checkpoint_img) and os.path.getsize(checkpoint_img) > 0
if proc.returncode != 0 and not checkpoint_exists:
    sys.exit(proc.returncode)
PY
checkpoint_status=$?
if [ "${checkpoint_status}" -ne 0 ] && [ ! -s "${CHECKPOINT_DIR}/checkpoint.img" ]; then
  echo "runsc checkpoint failed with status ${checkpoint_status}" >&2
  exit "${checkpoint_status}"
fi
runsc -root "${RUNSC_ROOT}" delete "${CONTAINER_ID}" >/dev/null 2>&1 || true

log "checkpoint ready at ${CHECKPOINT_DIR}"
