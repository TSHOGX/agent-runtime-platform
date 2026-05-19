#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

ROOTFS_DIR="${ROOTFS_DIR:-${SCRIPT_DIR}/rootfs}"
UBUNTU_SUITE="${UBUNTU_SUITE:-noble}"
UBUNTU_MIRROR="${UBUNTU_MIRROR:-http://archive.ubuntu.com/ubuntu}"
FORCE="${FORCE:-0}"

APT_PACKAGES=(
  ca-certificates
  curl
  git
  nodejs
  npm
  python3
  python3-pip
  python3-venv
)

PIP_PACKAGES=(
  matplotlib
  pandas
  pymysql
)

log() {
  printf '[build-rootfs] %s\n' "$*"
}

require_root() {
  if [ "$(id -u)" -ne 0 ]; then
    echo "build-rootfs.sh must run as root because debootstrap/chroot need root privileges." >&2
    exit 1
  fi
}

copy_overlay_files() {
  log "installing harness files into ${ROOTFS_DIR}"
  rsync -a --chmod=F755,D755 "${SCRIPT_DIR}/files/" "${ROOTFS_DIR}/"
}

if [ -d "${ROOTFS_DIR}" ] && [ "${FORCE}" != "1" ]; then
  log "reusing existing rootfs at ${ROOTFS_DIR}; set FORCE=1 to rebuild"
  copy_overlay_files
  log "done"
  exit 0
fi

require_root

if ! command -v debootstrap >/dev/null 2>&1; then
  echo "debootstrap is required. Install it with: apt-get update && apt-get install -y debootstrap" >&2
  exit 1
fi

if [ -e "${ROOTFS_DIR}" ]; then
  log "removing existing rootfs because FORCE=1"
  rm -rf --one-file-system "${ROOTFS_DIR}"
fi

log "bootstrapping Ubuntu ${UBUNTU_SUITE} rootfs"
mkdir -p "${ROOTFS_DIR}"
debootstrap --variant=minbase "${UBUNTU_SUITE}" "${ROOTFS_DIR}" "${UBUNTU_MIRROR}"

log "installing apt packages"
chroot "${ROOTFS_DIR}" /bin/sh -lc "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ${APT_PACKAGES[*]} && apt-get clean && rm -rf /var/lib/apt/lists/*"

log "installing Python packages"
chroot "${ROOTFS_DIR}" /bin/sh -lc "python3 -m pip install --break-system-packages --no-cache-dir ${PIP_PACKAGES[*]}"

log "installing Claude Code CLI"
chroot "${ROOTFS_DIR}" /bin/sh -lc "npm install -g @anthropic-ai/claude-code"

copy_overlay_files

log "rootfs ready at ${ROOTFS_DIR}"
