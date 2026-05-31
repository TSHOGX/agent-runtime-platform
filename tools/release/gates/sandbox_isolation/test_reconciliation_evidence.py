#!/usr/bin/env python3
import importlib.util
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("reconciliation-evidence.py")
SPEC = importlib.util.spec_from_file_location("reconciliation_evidence", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ReconciliationEvidenceTest(unittest.TestCase):
    def test_missing_db_passes_inventory_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            payload = MODULE.inspect_reconciliation(args_for(Path(tmp) / "missing.db"))

            self.assertEqual(payload["status"], "passed")
            self.assertFalse(payload["db"]["info"]["exists"])

    def test_require_runtime_table_fails_missing_db(self):
        with tempfile.TemporaryDirectory() as tmp:
            payload = MODULE.inspect_reconciliation(args_for(Path(tmp) / "missing.db", require_runtime_table=True))

            self.assertEqual(payload["status"], "failed")
            self.assertIn("missing_db", {item["kind"] for item in payload["issues"]})

    def test_expect_clean_fails_on_active_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "orchestrator.db"
            row = runtime_row(state="live")
            create_db(db, [row])

            payload = MODULE.inspect_reconciliation(args_for(db, expect_clean=True))

            self.assertEqual(payload["status"], "failed")
            self.assertIn("active_resources", {item["kind"] for item in payload["issues"]})

    def test_absent_verified_row_with_valid_evidence_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "orchestrator.db"
            row = runtime_row(state="absent_verified", network_hosts_path="/run/network/gen_1/hosts")
            row["evidence_json"], row["evidence_digest"] = reconciliation_evidence(row)
            create_db(db, [row])

            payload = MODULE.inspect_reconciliation(args_for(db, expect_clean=True, require_runtime_table=True))

            self.assertEqual(payload["status"], "passed")
            self.assertEqual(payload["db"]["counts_by_state"]["absent_verified"], 1)
            self.assertEqual(payload["db"]["audit"][0]["evidence_status"], "passed")

    def test_absent_verified_requires_network_hosts_lstat_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "orchestrator.db"
            row = runtime_row(state="absent_verified", network_hosts_path="/run/network/gen_1/hosts")
            evidence, digest = reconciliation_evidence(row)
            data = json.loads(evidence)
            del data["filesystem_lstat"]["network_hosts:/run/network/gen_1/hosts"]
            row["evidence_json"], row["evidence_digest"] = canonical_with_digest(data)
            create_db(db, [row])

            payload = MODULE.inspect_reconciliation(args_for(db))

            self.assertEqual(payload["status"], "failed")
            self.assertIn("missing_filesystem_lstat_key", {item["kind"] for item in payload["issues"]})

    def test_evidence_digest_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "orchestrator.db"
            row = runtime_row(state="absent_verified")
            row["evidence_json"], row["evidence_digest"] = reconciliation_evidence(row)
            row["evidence_digest"] = "sha256:" + "0" * 64
            create_db(db, [row])

            payload = MODULE.inspect_reconciliation(args_for(db))

            self.assertEqual(payload["status"], "failed")
            self.assertIn("digest_mismatch", {item["kind"] for item in payload["issues"]})

    def test_host_mismatch_fails_when_requested(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "orchestrator.db"
            row = runtime_row(state="absent_verified")
            row["evidence_json"], row["evidence_digest"] = reconciliation_evidence(row)
            create_db(db, [row])

            payload = MODULE.inspect_reconciliation(args_for(db, host_id="other-host"))

            self.assertEqual(payload["status"], "failed")
            self.assertIn("host_mismatch", {item["kind"] for item in payload["issues"]})

    def test_require_host_inventory_fails_when_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "orchestrator.db"
            create_db(db, [])

            payload = MODULE.inspect_reconciliation(args_for(db, require_host_inventory=True))

            self.assertEqual(payload["status"], "failed")
            self.assertIn("host_inventory", {item["name"] for item in payload["issues"]})

    def test_write_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "nested" / "reconciliation.json"
            MODULE.write_output(output, {"status": "passed"})

            self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["status"], "passed")


def args_for(
    db,
    expect_clean=False,
    require_runtime_table=False,
    require_host_inventory=False,
    host_id="",
):
    return type(
        "Args",
        (),
        {
            "db": str(db),
            "runsc_root": "/tmp/runsc-root",
            "host_id": host_id,
            "expect_clean": expect_clean,
            "require_runtime_table": require_runtime_table,
            "skip_host_commands": True,
            "require_host_inventory": require_host_inventory,
            "verify_host_absence": False,
        },
    )()


def create_db(path, rows):
    conn = sqlite3.connect(path)
    try:
        columns = ",\n  ".join(f"{column} TEXT" for column in MODULE.REQUIRED_COLUMNS)
        conn.execute(f"CREATE TABLE runtime_resource_instances (\n  {columns}\n)")
        for row in rows:
            values = [row.get(column, "") for column in MODULE.REQUIRED_COLUMNS]
            placeholders = ", ".join("?" for _ in MODULE.REQUIRED_COLUMNS)
            conn.execute(
                f"INSERT INTO runtime_resource_instances ({', '.join(MODULE.REQUIRED_COLUMNS)}) VALUES ({placeholders})",
                values,
            )
        conn.commit()
    finally:
        conn.close()


def runtime_row(state="allocated", network_hosts_path=""):
    row = {
        "generation_id": "gen_1",
        "session_id": "sess_1",
        "contract_id": "contract_1",
        "sandbox_contract_version": MODULE.CONTRACT,
        "worker_id": "worker_1",
        "host_id": "host_1",
        "state": state,
        "runsc_container_id": "harness-gen-gen_1",
        "runsc_platform": "systrap",
        "runsc_version": "runsc test",
        "runsc_binary_path": "/usr/local/bin/runsc",
        "runsc_binary_digest": "sha256:runsc",
        "network_profile_id": "net_1",
        "netns_name": "hns-gen_1",
        "netns_path": "/run/netns/hns-gen_1",
        "host_veth": "hv-gen_1",
        "sandbox_veth": "sv-gen_1",
        "host_gateway_ip": "10.0.0.1",
        "sandbox_ip": "10.0.0.2",
        "sandbox_ip_cidr": "10.0.0.2/30",
        "host_side_cidr": "10.0.0.0/30",
        "nft_table_name": "harness_gen_1",
        "control_dir_path": "/run/control/gen_1",
        "control_manifest_path": "/run/control/gen_1/manifest.json",
        "bundle_dir_path": "/run/runtime/gen_1",
        "spec_path": "/run/runtime/gen_1/config.json",
        "checkpoint_path": "/run/checkpoints/gen_1",
        "bridge_dir_path": "/run/bridge/gen_1",
        "network_hosts_path": network_hosts_path,
        "log_dir_path": "/run/logs/gen_1",
        "evidence_json": "",
        "evidence_digest": "",
        "verified_at": "2026-05-28T00:00:00Z" if state in MODULE.TERMINAL_ABSENCE_STATES else "",
        "updated_at": "2026-05-28T00:00:00Z",
    }
    row["resource_identity_payload"], row["resource_identity_digest"] = identity_payload(row)
    return row


def identity_payload(row):
    payload = {
        "host_id": row["host_id"],
        "session_id": row["session_id"],
        "generation_id": row["generation_id"],
        "contract_id": row["contract_id"],
        "sandbox_contract_version": row["sandbox_contract_version"],
        "runsc_container_id": row["runsc_container_id"],
        "runsc_platform": row["runsc_platform"],
        "runsc_version": row["runsc_version"],
        "runsc_binary_path": row["runsc_binary_path"],
        "runsc_binary_digest": row["runsc_binary_digest"],
        "network_profile_id": row["network_profile_id"],
        "netns_name": row["netns_name"],
        "netns_path": row["netns_path"],
        "host_veth": row["host_veth"],
        "sandbox_veth": row["sandbox_veth"],
        "host_gateway_ip": row["host_gateway_ip"],
        "sandbox_ip": row["sandbox_ip"],
        "sandbox_ip_cidr": row["sandbox_ip_cidr"],
        "host_side_cidr": row["host_side_cidr"],
        "nft_table_name": row["nft_table_name"],
        "control_dir_path": row["control_dir_path"],
        "control_manifest_path": row["control_manifest_path"],
        "bundle_dir_path": row["bundle_dir_path"],
        "spec_path": row["spec_path"],
        "checkpoint_path": row["checkpoint_path"],
        "bridge_dir_path": row["bridge_dir_path"],
        "log_dir_path": row["log_dir_path"],
        "root_prefixes": {"run_dir": "/run"},
    }
    if row["network_hosts_path"]:
        payload["network_hosts_path"] = row["network_hosts_path"]
    return canonical_with_digest(payload)


def reconciliation_evidence(row):
    filesystem = {
        "checkpoint:" + row["checkpoint_path"]: "lstat:absent",
        "control:" + row["control_dir_path"]: "lstat:absent",
        "control_manifest:" + row["control_manifest_path"]: "lstat:absent",
        "bundle:" + row["bundle_dir_path"]: "lstat:absent",
        "spec:" + row["spec_path"]: "lstat:absent",
        "bridge:" + row["bridge_dir_path"]: "lstat:absent",
        "log:" + row["log_dir_path"]: "lstat:absent",
    }
    if row["network_hosts_path"]:
        filesystem["network:" + str(Path(row["network_hosts_path"]).parent)] = "lstat:absent"
        filesystem["network_hosts:" + row["network_hosts_path"]] = "lstat:absent"
    payload = {
        "host_id": row["host_id"],
        "runsc_state": "runsc_container:absent; check=test",
        "ip_netns": "netns:absent; check=test",
        "ip_link": "host_veth:absent; check=test",
        "nft": "nft_table:absent; check=test",
        "filesystem_lstat": filesystem,
    }
    return canonical_with_digest(payload)


def canonical_with_digest(payload):
    data = MODULE.canonical_json_bytes(payload)
    return data.decode("utf-8"), MODULE.sha256_digest(data)


if __name__ == "__main__":
    unittest.main()
