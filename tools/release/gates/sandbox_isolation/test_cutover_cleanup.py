#!/usr/bin/env python3
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("cutover-cleanup.py")
SPEC = importlib.util.spec_from_file_location("cutover_cleanup", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CutoverCleanupTest(unittest.TestCase):
    def test_dry_run_plans_root_deletes_and_host_deletes(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "sessions").mkdir()
            (base / "sessions" / "old-session").mkdir()
            (base / "secrets" / "key").mkdir(parents=True)
            inventory_path = write_inventory(base, inventory_for(base))

            payload = MODULE.cleanup(args_for(inventory_path))

            self.assertEqual(payload["status"], "planned")
            delete_sources = {action["source"] for action in payload["actions"] if action["type"] == "delete_path"}
            commands = {tuple(action["command"]) for action in payload["actions"] if action["type"] == "run_command"}
            self.assertIn(str(base / "sessions" / "old-session"), delete_sources)
            self.assertIn(str(base / "secrets"), delete_sources)
            self.assertIn(("runsc", "-root", "/tmp/runsc-root", "delete", "-force", "phase3-sess_old"), commands)
            self.assertIn(("ip", "netns", "delete", "harness-gen-old"), commands)
            self.assertIn(("ip", "link", "delete", "hgenold"), commands)
            self.assertIn(("nft", "delete", "table", "inet", "harness_gen_old"), commands)

    def test_apply_requires_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "sessions").mkdir()
            inventory_path = write_inventory(base, inventory_for(base))

            payload = MODULE.cleanup(args_for(inventory_path, apply=True))

            self.assertEqual(payload["status"], "failed")
            self.assertIn("missing_destructive_confirmation", {item["kind"] for item in payload["issues"]})

    def test_apply_deletes_filesystem_without_host_commands(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "sessions").mkdir()
            (base / "sessions" / "old-session").mkdir()
            inventory = inventory_for(base)
            inventory["host"]["commands"] = []
            inventory_path = write_inventory(base, inventory)

            payload = MODULE.cleanup(
                args_for(
                    inventory_path,
                    apply=True,
                    confirm=MODULE.CONTRACT,
                    skip_host_commands=True,
                )
            )

            self.assertEqual(payload["status"], "passed")
            self.assertFalse((base / "sessions" / "old-session").exists())
            self.assertFalse((base / "secrets").exists())

    def test_apply_deletes_db_sidecars(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "state").mkdir()
            db = base / "state" / "orchestrator.db"
            db.write_text("db", encoding="utf-8")
            Path(str(db) + "-wal").write_text("wal", encoding="utf-8")
            Path(str(db) + "-shm").write_text("shm", encoding="utf-8")
            inventory = inventory_for(base)
            inventory["host"]["commands"] = []
            inventory_path = write_inventory(base, inventory)

            payload = MODULE.cleanup(
                args_for(
                    inventory_path,
                    apply=True,
                    confirm=MODULE.CONTRACT,
                    skip_host_commands=True,
                )
            )

            self.assertEqual(payload["status"], "passed")
            self.assertFalse(db.exists())
            self.assertFalse(Path(str(db) + "-wal").exists())
            self.assertFalse(Path(str(db) + "-shm").exists())

    def test_clean_inventory_is_noop(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "sessions").mkdir()
            inventory = inventory_for(base)
            inventory["roots"]["sessions_root"]["entries"] = 0
            inventory["roots"]["legacy_secret_root"]["info"] = {"exists": False}
            inventory["host"]["commands"] = []
            inventory_path = write_inventory(base, inventory)

            payload = MODULE.cleanup(args_for(inventory_path))

            self.assertEqual(payload["status"], "planned")
            self.assertEqual(payload["actions"], [])

    def test_protected_delete_target_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "sessions").mkdir()
            inventory = inventory_for(base)
            inventory["roots"]["legacy_secret_root"]["path"] = "/var/lib/harness"
            inventory_path = write_inventory(base, inventory)

            payload = MODULE.cleanup(args_for(inventory_path))

            self.assertEqual(payload["status"], "failed")
            self.assertIn("protected_path", {item["kind"] for item in payload["issues"]})

    def test_write_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "nested" / "cleanup.json"
            MODULE.write_output(output, {"status": "planned"})

            self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["status"], "planned")


def args_for(
    inventory_path,
    apply=False,
    confirm="",
    skip_host_commands=False,
    skip_filesystem=False,
):
    return type(
        "Args",
        (),
        {
            "inventory": str(inventory_path),
            "apply": apply,
            "confirm_destructive_cutover": confirm,
            "skip_host_commands": skip_host_commands,
            "skip_filesystem": skip_filesystem,
            "output": "",
        },
    )()


def write_inventory(base, inventory):
    path = base / "inventory.json"
    path.write_text(json.dumps(inventory), encoding="utf-8")
    return path


def inventory_for(base):
    sessions = base / "sessions"
    secrets = base / "secrets"
    return {
        "contract": MODULE.CONTRACT,
        "qualification": "cutover-inventory",
        "status": "failed",
        "db": {
            "path": str(base / "state" / "orchestrator.db"),
            "info": {"exists": False},
        },
        "roots": {
            "sessions_root": {
                "path": str(sessions),
                "info": {"exists": True, "kind": "directory"},
                "entries": 1,
            },
            "legacy_secret_root": {
                "path": str(secrets),
                "info": {"exists": True, "kind": "directory"},
                "entries": 1,
            },
        },
        "host": {
            "status": "passed",
            "commands": [
                {
                    "name": "runsc_containers",
                    "command": ["runsc", "-root", "/tmp/runsc-root", "list"],
                    "status": "passed",
                    "output_tail": "ID PID STATUS\nphase3-sess_old -1 stopped",
                },
                {
                    "name": "ip_netns",
                    "status": "passed",
                    "output_tail": "harness-gen-old (id: 1)\ndocker-system (id: 2)",
                },
                {
                    "name": "ip_links",
                    "status": "passed",
                    "output_tail": "48: hgenold@if47: <BROADCAST> link-netns harness-gen-old",
                },
                {
                    "name": "nft_tables",
                    "status": "passed",
                    "output_tail": "table inet harness_gen_old\ntable ip docker",
                },
            ],
        },
    }


if __name__ == "__main__":
    unittest.main()
