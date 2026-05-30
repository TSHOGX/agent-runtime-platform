#!/usr/bin/env python3
"""Compatibility shim for the sandbox-isolation release suite.

The implementation moved to ``tools/release/`` (generic engine + declarative
suites + dispatcher). This entry point is retained at its documented path and
forwards to the ``sandbox_isolation`` suite, which carries the former Phase 8
runtime-isolation gates verbatim.

Prefer ``tools/release/run.py --suite sandbox_isolation`` for new usage.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.release.suites.sandbox_isolation import *  # noqa: F401,F403,E402
from tools.release.suites.sandbox_isolation import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
