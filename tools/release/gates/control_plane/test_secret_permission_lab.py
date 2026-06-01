#!/usr/bin/env python3
import tempfile
import unittest
import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("secret-permission-lab.py")
SPEC = importlib.util.spec_from_file_location("secret_permission_lab", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class SecretPermissionLabTest(unittest.TestCase):
    def test_load_secret_config_reads_harness_yaml_section(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "harness.yaml"
            config.write_text(
                """
harness:
  run_dir: /var/lib/harness/run
  secrets:
    root: /var/lib/harness/secrets
    readers_gid: 65501
  bridge:
    poll_interval: 5ms
""",
                encoding="utf-8",
            )
            root, gid = MODULE.load_secret_config(config)
            self.assertEqual(root, "/var/lib/harness/secrets")
            self.assertEqual(gid, 65501)

    def test_load_secret_config_ignores_non_harness_secrets_sections(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "harness.yaml"
            config.write_text(
                """
ignored:
  secrets:
    root: /wrong/before
    readers_gid: 1
harness:
  run_dir: /var/lib/harness/run
  secrets:
    root: /var/lib/harness/secrets
    readers_gid: 65501
  nested:
    secrets:
      root: /wrong/nested
      readers_gid: 2
other:
  secrets:
    root: /wrong/after
    readers_gid: 3
""",
                encoding="utf-8",
            )
            root, gid = MODULE.load_secret_config(config)
            self.assertEqual(root, "/var/lib/harness/secrets")
            self.assertEqual(gid, 65501)


if __name__ == "__main__":
    unittest.main()
