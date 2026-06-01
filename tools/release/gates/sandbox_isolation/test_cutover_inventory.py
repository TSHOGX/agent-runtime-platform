#!/usr/bin/env python3
import importlib.util
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("cutover-inventory.py")
SPEC = importlib.util.spec_from_file_location("cutover_inventory", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CutoverInventoryTest(unittest.TestCase):
    def test_missing_roots_and_db_pass_inventory(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = args_for(tmp, expect_clean=True)

            payload = MODULE.inspect_cutover(args)

            self.assertEqual(payload["status"], "passed")
            self.assertEqual(payload["contract"], "sandbox-isolation-v1")
            self.assertEqual(payload["db"]["info"]["exists"], False)
            self.assertEqual(payload["blockers"], [])

    def test_expect_clean_fails_on_legacy_roots(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "sessions" / "sess_old").mkdir(parents=True)
            (base / "run" / "network" / "gen_old").mkdir(parents=True)
            (base / "run" / "logs" / "gen_old").mkdir(parents=True)
            (base / "secrets").mkdir()
            args = args_for(tmp, expect_clean=True)

            payload = MODULE.inspect_cutover(args)

            self.assertEqual(payload["status"], "failed")
            blockers = {(item["name"], item["kind"]) for item in payload["blockers"]}
            self.assertIn(("sessions_root", "root_entries"), blockers)
            self.assertIn(("run_network_root", "root_entries"), blockers)
            self.assertIn(("run_logs_root", "root_entries"), blockers)
            self.assertIn(("legacy_secret_root", "legacy_secret_root_present"), blockers)

    def test_expect_clean_fails_on_active_db_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "state" / "orchestrator.db"
            db.parent.mkdir()
            create_db_with_active_rows(db)
            args = args_for(tmp, db=db, expect_clean=True)

            payload = MODULE.inspect_cutover(args)

            self.assertEqual(payload["status"], "failed")
            blockers = {item["name"]: item for item in payload["blockers"]}
            self.assertEqual(blockers["sessions_total"]["count"], 1)
            self.assertEqual(blockers["runtime_resource_instances_active"]["count"], 1)
            self.assertEqual(blockers["active_model_contexts_total"]["count"], 1)

    def test_expect_clean_fails_on_obsolete_session_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "state" / "orchestrator.db"
            db.parent.mkdir()
            create_db_with_obsolete_session_columns(db)
            args = args_for(tmp, db=db, expect_clean=True)

            payload = MODULE.inspect_cutover(args)

            self.assertEqual(payload["status"], "failed")
            query = payload["db"]["queries"]["obsolete_session_columns"]
            self.assertEqual(query["count"], 2)
            self.assertEqual(query["columns"], ["restore_id", "workspace"])
            blockers = {item["name"]: item for item in payload["blockers"]}
            self.assertEqual(blockers["obsolete_session_columns"]["count"], 2)

    def test_inventory_mode_reports_blockers_without_failing(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "sessions" / "sess_old").mkdir(parents=True)
            args = args_for(tmp, expect_clean=False)

            payload = MODULE.inspect_cutover(args)

            self.assertEqual(payload["status"], "passed")
            self.assertTrue(payload["blockers"])

    def test_proxy_socket_entries_are_blockers(self):
        with tempfile.TemporaryDirectory() as tmp:
            proxy = Path(tmp) / "run" / "proxy-internal"
            proxy.mkdir(parents=True)
            (proxy / "proxy.sock").write_text("not a real socket", encoding="utf-8")
            args = args_for(tmp, expect_clean=True)

            payload = MODULE.inspect_cutover(args)

            self.assertEqual(payload["status"], "failed")
            blockers = {(item["name"], item["kind"]) for item in payload["blockers"]}
            self.assertIn(("proxy_internal_root", "proxy_internal_entries"), blockers)

    def test_expect_clean_fails_on_host_runtime_resources(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = args_for(tmp, expect_clean=True)
            db = MODULE.db_inventory(args.db)
            roots = MODULE.root_inventories(args)
            host = {
                "status": "passed",
                "commands": [
                    {
                        "name": "runsc_containers",
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
                        "output_tail": "table inet harness_gen_old",
                    },
                ],
            }

            blockers = MODULE.blockers_for_inventory(db, roots, host, args)

            found = {(item["name"], item["kind"]): item["count"] for item in blockers}
            self.assertEqual(found[("runsc_containers", "host_runtime_resources")], 1)
            self.assertEqual(found[("ip_netns", "host_runtime_resources")], 1)
            self.assertEqual(found[("ip_links", "host_runtime_resources")], 1)
            self.assertEqual(found[("nft_tables", "host_runtime_resources")], 1)

    def test_write_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "nested" / "cutover.json"
            MODULE.write_output(output, {"status": "passed"})

            self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["status"], "passed")


def args_for(tmp, db=None, expect_clean=False):
    base = Path(tmp)
    return type(
        "Args",
        (),
        {
            "db": str(db or base / "state" / "orchestrator.db"),
            "sessions_root": str(base / "sessions"),
            "agent_homes_root": str(base / "agent-homes"),
            "run_dir": str(base / "run"),
            "runsc_root": str(base / "runsc"),
            "legacy_checkpoints_root": str(base / "checkpoints"),
            "prepared_bundle_root": str(base / "bundle" / "out"),
            "legacy_secret_root": str(base / "secrets"),
            "provider_credential_root": "",
            "proxy_internal_root": "",
            "skip_host_commands": True,
            "require_host_inventory": False,
            "expect_clean": expect_clean,
        },
    )()


def create_db_with_active_rows(path):
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
CREATE TABLE sessions (id TEXT PRIMARY KEY, status TEXT NOT NULL);
INSERT INTO sessions (id, status) VALUES ('sess_old', 'running_idle');

CREATE TABLE active_model_request_contexts (sandbox_source_ip TEXT PRIMARY KEY);
INSERT INTO active_model_request_contexts (sandbox_source_ip) VALUES ('10.0.0.2');

CREATE TABLE runtime_generations (generation_id TEXT PRIMARY KEY, status TEXT NOT NULL);
INSERT INTO runtime_generations (generation_id, status) VALUES ('gen_old', 'active');

CREATE TABLE runtime_generation_resources (
  generation_id TEXT PRIMARY KEY,
  resource_state TEXT NOT NULL,
  checkpoint_path TEXT
);
INSERT INTO runtime_generation_resources (generation_id, resource_state, checkpoint_path)
VALUES ('gen_old', 'live', '/checkpoint/gen_old');

CREATE TABLE runtime_resource_instances (
  generation_id TEXT PRIMARY KEY,
  state TEXT NOT NULL
);
INSERT INTO runtime_resource_instances (generation_id, state) VALUES ('gen_old', 'live');

CREATE TABLE network_profiles (network_profile_id TEXT PRIMARY KEY, allocation_state TEXT NOT NULL);
INSERT INTO network_profiles (network_profile_id, allocation_state) VALUES ('net_old', 'live');

CREATE TABLE session_workspaces (session_id TEXT PRIMARY KEY);
CREATE TABLE session_driver_homes (session_id TEXT, driver TEXT);
CREATE TABLE sandbox_contracts (contract_id TEXT PRIMARY KEY);
"""
        )
        conn.commit()
    finally:
        conn.close()


def create_db_with_obsolete_session_columns(path):
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
CREATE TABLE sessions (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  workspace TEXT,
  restore_id TEXT
);
"""
        )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    unittest.main()
