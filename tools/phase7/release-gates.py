#!/usr/bin/env python3
"""Compatibility shim for the control-plane release suite.

The implementation moved to ``tools/release/`` (generic engine + declarative
suites + dispatcher). This entry point is retained at its documented path and
forwards to the ``control_plane`` suite, which carries the former Phase 7
gates verbatim (blocking per PLAN.md guardrail #2).

Prefer ``tools/release/run.py --suite control_plane`` for new usage.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.release.suites.control_plane import *  # noqa: F401,F403,E402
from tools.release.suites.control_plane import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
