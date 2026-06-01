#!/usr/bin/env python3
"""Agent-capability release suite.

Reserved suite. The next capability plane is still in design; its only
executable checks today are the static doc-pinning scans, which also live in
the sandbox_isolation suite's full static set (the single source of truth in
``static_manifests``). This suite exposes that subset as a focused
``--static-only`` view for capability development, not a replacement for
release-blocking static scans.
"""
import argparse
import json
import sys

from tools.release import engine
from tools.release.suites import static_manifests

SUITE = "agent_capability"
FAILURE_BANNER = "agent capability release gates failed"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Agent-capability release suite (reserved static-check view).")
    parser.add_argument("--output", default="", help="Optional path for the JSON evidence file.")
    parser.add_argument("--list", action="store_true", help="List selected gates without running them.")
    parser.add_argument("--static-only", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def selected_gates(args=None):
    return []


def static_checks():
    return engine.run_static_checks(static_manifests.agent_capability_checks())


def run(argv=None):
    args = parse_args(argv)
    if args.static_only:
        payload = static_checks()
        print(json.dumps(payload, indent=2))
        return 0 if payload["status"] == "passed" else 1
    if args.list:
        print(json.dumps([], indent=2))
        return 0
    payload = {"suite": SUITE, "result": "skipped", "gates": [], "static_checks": static_checks()}
    print(json.dumps(payload, indent=2))
    if args.output:
        engine.write_output(args.output, payload)
    return 0 if payload["static_checks"]["status"] == "passed" else 1


def main(argv=None):
    try:
        return run(argv)
    except KeyboardInterrupt:
        return 130
    except Exception as err:  # noqa: BLE001 - top-level reporting
        print(f"{FAILURE_BANNER}: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
