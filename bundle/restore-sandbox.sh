#!/usr/bin/env bash
set -Eeuo pipefail
trap '' PIPE

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

cat >&2 <<'EOF'
bundle/restore-sandbox.sh is quarantined for sandbox-isolation-v1.
It restores the legacy Phase 2 smoke checkpoint and must not be used as Phase 8
release evidence. Use the orchestrator runtime restore path, which validates
stored sandbox-isolation-v1 artifacts, resource identity, and runsc pins.
EOF
exit 1

DEFAULT_MANIFEST="${SCRIPT_DIR}/out/phase2-template-bundle/phase2-manifest.env"
MANIFEST="${MANIFEST:-${DEFAULT_MANIFEST}}"

if [ -f "${MANIFEST}" ]; then
  # shellcheck disable=SC1090
  . "${MANIFEST}"
fi

BUNDLE_DIR="${BUNDLE_DIR:-${SCRIPT_DIR}/out/phase2-template-bundle}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-${SCRIPT_DIR}/checkpoints/phase2-template}"
RUNSC_ROOT="${RUNSC_ROOT:-/var/lib/harness/runsc}"
RUNSC_LOG_DIR="${RUNSC_LOG_DIR:-/var/lib/harness/logs}"
SESSIONS_ROOT="${SESSIONS_ROOT:-/var/lib/harness/sessions}"
AGENT_HOMES_ROOT="${AGENT_HOMES_ROOT:-/var/lib/harness/agent-homes}"
CONTROL_DIR="${CONTROL_DIR:-/var/lib/harness/control/phase2-template}"
RUNSC_NETWORK="${RUNSC_NETWORK:-sandbox}"
RUNSC_PLATFORM="${RUNSC_PLATFORM:-systrap}"
RUNSC_OVERLAY2="${RUNSC_OVERLAY2:-none}"
SESSION_ID="${SESSION_ID:-phase2-$(date +%Y%m%d-%H%M%S)}"
RESTORE_ID="${RESTORE_ID:-phase2-${SESSION_ID}}"
HARNESS_AGENT="${HARNESS_AGENT:-claude}"
DETACH="${DETACH:-0}"

log() {
  printf '[restore-sandbox] %s\n' "$*" >&2
}

quote_env() {
  python3 - "$1" <<'PY'
import shlex
import sys

print(shlex.quote(sys.argv[1]))
PY
}

write_env_line() {
  local key="$1"
  local value="$2"
  printf 'export %s=%s\n' "${key}" "$(quote_env "${value}")"
}

if [ ! -d "${CHECKPOINT_DIR}" ]; then
  echo "checkpoint not found at ${CHECKPOINT_DIR}; run bundle/bake-bundle.sh first." >&2
  exit 1
fi

mkdir -p "${SESSIONS_ROOT}/${SESSION_ID}" "${AGENT_HOMES_ROOT}/${SESSION_ID}" "${CONTROL_DIR}" "${RUNSC_LOG_DIR}"

CONTROL_FILE="${CONTROL_DIR}/session.env"
{
  write_env_line SESSION_ID "${SESSION_ID}"
  write_env_line SESSION_WORKSPACE "/sessions/${SESSION_ID}"
  write_env_line HARNESS_AGENT_HOME "/agent-homes/${SESSION_ID}"
  write_env_line HARNESS_AGENT "${HARNESS_AGENT}"
  write_env_line HARNESS_COMMAND "${HARNESS_COMMAND:-}"
  write_env_line DORIS_HOST "${DORIS_HOST:-172.16.0.138}"
  write_env_line DORIS_PORT "${DORIS_PORT:-9030}"
  write_env_line DORIS_USER "${DORIS_USER:-ro_user_batt}"
  write_env_line DORIS_PASSWORD "${DORIS_PASSWORD:-}"
  write_env_line DORIS_DATABASE "${DORIS_DATABASE:-vhr_data}"
  write_env_line CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC "${CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:-1}"
  write_env_line CLAUDE_MODEL "${CLAUDE_MODEL:-sonnet}"
  write_env_line CLAUDE_SESSION_UUID "${CLAUDE_SESSION_UUID:-}"
} > "${CONTROL_FILE}.tmp"
chmod 0600 "${CONTROL_FILE}.tmp"
mv "${CONTROL_FILE}.tmp" "${CONTROL_FILE}"

log "session workspace: ${SESSIONS_ROOT}/${SESSION_ID}"
log "control file: ${CONTROL_FILE}"
log "runsc version: $(runsc --version 2>&1 | tr '\n' ' ' | sed 's/[[:space:]]\+$//')"

runsc -root "${RUNSC_ROOT}" kill "${RESTORE_ID}" KILL >/dev/null 2>&1 || true
runsc -root "${RUNSC_ROOT}" delete "${RESTORE_ID}" >/dev/null 2>&1 || true

start_ns="$(date +%s%N)"

restore_cmd=(
  runsc
  -root "${RUNSC_ROOT}"
  -platform "${RUNSC_PLATFORM}"
  -overlay2 "${RUNSC_OVERLAY2}"
  -network "${RUNSC_NETWORK}"
  -debug-log "${RUNSC_LOG_DIR}/"
  restore
  -bundle "${BUNDLE_DIR}"
  -image-path "${CHECKPOINT_DIR}"
  -pid-file "${SESSIONS_ROOT}/${SESSION_ID}/runsc.pid"
)

if [ "${RUNSC_RESTORE_DIRECT:-0}" = "1" ]; then
  restore_cmd+=(-direct)
fi

if [ "${RUNSC_RESTORE_BACKGROUND:-0}" = "1" ]; then
  restore_cmd+=(-background)
fi

if [ -n "${RUNSC_FS_RESTORE_IMAGE_PATH:-}" ]; then
  restore_cmd+=(-fs-restore-image-path "${RUNSC_FS_RESTORE_IMAGE_PATH}")
fi

if [ "${RUNSC_FS_RESTORE_DIRECT:-0}" = "1" ]; then
  restore_cmd+=(-fs-restore-direct)
fi

if [ "${DETACH}" = "1" ]; then
  restore_cmd+=(-detach)
fi

restore_cmd+=("${RESTORE_ID}")

log "restoring ${RESTORE_ID}"
set +e
"${restore_cmd[@]}"
status=$?
set -e

if [ "${status}" -ne 0 ] && [ "${status}" -ne 141 ]; then
  exit "${status}"
fi

end_ns="$(date +%s%N)"
duration_ms=$(( (end_ns - start_ns) / 1000000 ))
printf '%s\n' "${duration_ms}" > "${SESSIONS_ROOT}/${SESSION_ID}/restore_ms.txt"
log "restore command completed in ${duration_ms} ms with status ${status}"
log "artifacts: ${SESSIONS_ROOT}/${SESSION_ID}"

exit 0
