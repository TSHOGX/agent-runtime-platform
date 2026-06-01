#!/usr/bin/env python3
"""Single dispatcher for release-qualification suites.

Usage:
    tools/release/run.py --suite <name> [suite args...]
    tools/release/run.py --suite all --list

Each suite under ``tools/release/suites/`` owns its own CLI; the dispatcher
strips ``--suite NAME`` and forwards the remaining arguments to that suite's
``main()``. ``--suite all`` composes every registered suite (primarily for
``--list`` and a sequential default run).

Suites:
    control_plane     - durable control-plane gates.
    sandbox_isolation - runtime-isolation contract; the live harness.
    agent_capability  - capability/UX static-check view.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.release.suites import (  # noqa: E402
    agent_capability,
    control_plane,
    sandbox_isolation,
)

SUITES = {
    "control_plane": control_plane,
    "sandbox_isolation": sandbox_isolation,
    "agent_capability": agent_capability,
}
SUITE_ORDER = ["control_plane", "sandbox_isolation", "agent_capability"]


def extract_suite(argv):
    rest = []
    suite = None
    i = 0
    while i < len(argv):
        token = argv[i]
        if token == "--suite":
            if i + 1 >= len(argv):
                raise SystemExit("--suite requires a value")
            suite = argv[i + 1]
            i += 2
            continue
        if token.startswith("--suite="):
            suite = token.split("=", 1)[1]
            i += 1
            continue
        rest.append(token)
        i += 1
    return suite, rest


def run_all(rest):
    if "--list" in rest:
        listed = []
        for name in SUITE_ORDER:
            captured = _capture_list(SUITES[name])
            listed.extend(captured)
        print(json.dumps(listed, indent=2))
        return 0
    worst = 0
    for name in SUITE_ORDER:
        print(f"# suite: {name}", file=sys.stderr)
        code = SUITES[name].main(rest)
        worst = max(worst, code)
    return worst


def _capture_list(suite):
    """Return a suite's --list payload as parsed JSON (best-effort)."""
    import contextlib
    import io

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        suite.main(["--list"])
    try:
        return json.loads(buffer.getvalue())
    except json.JSONDecodeError:
        return []


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    suite, rest = extract_suite(argv)
    if suite is None:
        raise SystemExit("usage: run.py --suite {" + ",".join(SUITE_ORDER) + ",all} [args...]")
    if suite == "all":
        return run_all(rest)
    if suite not in SUITES:
        raise SystemExit(f"unknown suite {suite!r}; choose from {', '.join(SUITE_ORDER)}, all")
    return SUITES[suite].main(rest)


if __name__ == "__main__":
    raise SystemExit(main())
