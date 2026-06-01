#!/usr/bin/env python3
import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("rootfs-inspect.py")
SPEC = importlib.util.spec_from_file_location("rootfs_inspect", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class RootFSInspectTest(unittest.TestCase):
    def test_valid_rootfs_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            rootfs = valid_rootfs(tmp)

            payload = MODULE.inspect_rootfs(rootfs)

            self.assertEqual(payload["status"], "passed")
            self.assertEqual(payload["contract"], "sandbox-isolation-v1")
            self.assertTrue(payload["rootfs_digest"].startswith("sha256:"))

    def test_workspace_symlink_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            rootfs = valid_rootfs(tmp)
            os.rmdir(rootfs / "workspace")
            os.symlink("/sessions/sess_old", rootfs / "workspace")

            payload = MODULE.inspect_rootfs(rootfs)

            self.assertEqual(payload["status"], "failed")
            failed = {check["name"] for check in payload["checks"] if check["status"] == "failed"}
            self.assertIn("workspace_is_empty_real_directory", failed)

    def test_baked_claude_state_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            rootfs = valid_rootfs(tmp)
            claude = rootfs / "root" / ".claude"
            claude.mkdir()
            (claude / "settings.json").write_text("{}", encoding="utf-8")

            payload = MODULE.inspect_rootfs(rootfs)

            self.assertEqual(payload["status"], "failed")
            failed = {check["name"] for check in payload["checks"] if check["status"] == "failed"}
            self.assertIn("root_.claude_absent_or_empty", failed)

    def test_legacy_session_dir_fails_even_when_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            rootfs = valid_rootfs(tmp)
            sessions = rootfs / "sessions"
            sessions.mkdir()

            payload = MODULE.inspect_rootfs(rootfs)

            self.assertEqual(payload["status"], "failed")
            failed = {check["name"] for check in payload["checks"] if check["status"] == "failed"}
            self.assertIn("sessions_absent", failed)

    def test_write_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            payload = {"status": "passed"}
            output = Path(tmp) / "nested" / "rootfs.json"

            MODULE.write_output(output, payload)

            self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["status"], "passed")


def valid_rootfs(tmp):
    rootfs = Path(tmp) / "rootfs"
    (rootfs / "workspace").mkdir(parents=True)
    (rootfs / "agent-home").mkdir()
    (rootfs / "etc").mkdir()
    (rootfs / "etc" / "hosts").write_text("127.0.0.1 localhost\n", encoding="utf-8")
    (rootfs / "root").mkdir()
    return rootfs


if __name__ == "__main__":
    unittest.main()
