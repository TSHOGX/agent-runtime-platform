#!/usr/bin/env python3
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("adversarial-lab.py")
SPEC = importlib.util.spec_from_file_location("adversarial_lab", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class AdversarialLabTest(unittest.TestCase):
    def test_complete_gate_by_gate_report_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "lab.json"
            report_path.write_text(json.dumps(valid_report()), encoding="utf-8")

            payload = MODULE.inspect_report(report_path)

            self.assertEqual(payload["status"], "passed")
            self.assertEqual(payload["required_total"], payload["reported_total"])
            self.assertEqual(payload["required_total"], payload["passed_total"])

    def test_missing_gate_fails(self):
        report = valid_report()
        first = MODULE.required_gates()[0]["id"]
        del report["gates"][first]
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "lab.json"
            report_path.write_text(json.dumps(report), encoding="utf-8")

            payload = MODULE.inspect_report(report_path)

            self.assertEqual(payload["status"], "failed")
            self.assertIn("missing_gate", {item["kind"] for item in payload["issues"]})

    def test_pending_gate_fails(self):
        report = valid_report()
        first = MODULE.required_gates()[0]["id"]
        report["gates"][first]["status"] = "pending"
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "lab.json"
            report_path.write_text(json.dumps(report), encoding="utf-8")

            payload = MODULE.inspect_report(report_path)

            self.assertEqual(payload["status"], "failed")
            self.assertIn("gate_not_passed", {item["kind"] for item in payload["issues"]})

    def test_missing_report_metadata_fails(self):
        report = valid_report()
        report["target_host"] = ""
        report["runsc"]["binary_digest"] = ""
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "lab.json"
            report_path.write_text(json.dumps(report), encoding="utf-8")

            payload = MODULE.inspect_report(report_path)

            kinds = {item["kind"] for item in payload["issues"]}
            self.assertIn("missing_field", kinds)
            self.assertIn("missing_runsc_field", kinds)

    def test_write_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "nested" / "lab.json"
            MODULE.write_output(output, {"status": "passed"})

            self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["status"], "passed")

    def test_generate_report_from_release_evidence_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            release_path = Path(tmp) / "release.json"
            report_path = Path(tmp) / "adversarial.json"
            release_path.write_text(json.dumps(valid_release_evidence()), encoding="utf-8")

            payload = MODULE.inspect_generated_report(
                release_path,
                output_report=report_path,
                target_host="sandbox-lab-host",
            )

            self.assertEqual(payload["status"], "passed")
            self.assertEqual(payload["required_total"], payload["reported_total"])
            self.assertEqual(payload["required_total"], payload["passed_total"])
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["target_host"], "sandbox-lab-host")
            self.assertTrue(report["source_release_evidence"]["digest"].startswith("sha256:"))
            self.assertEqual(report["gates"]["root_and_mount_gates_027"]["status"], "passed")
            self.assertIn("rootfs_image", report["gates"]["root_and_mount_gates_027"]["evidence"])

    def test_generate_report_rejects_incomplete_release_evidence(self):
        release = valid_release_evidence()
        del release["supplied_evidence"]["reconciliation"]
        with tempfile.TemporaryDirectory() as tmp:
            release_path = Path(tmp) / "release.json"
            release_path.write_text(json.dumps(release), encoding="utf-8")

            payload = MODULE.inspect_generated_report(release_path, target_host="sandbox-lab-host")

            self.assertEqual(payload["status"], "failed")
            self.assertIn("missing_supplied_evidence", {item["kind"] for item in payload["issues"]})


def valid_report():
    return {
        "contract": MODULE.CONTRACT,
        "qualification": "adversarial-lab",
        "target_host": "sandbox-lab-host",
        "generated_at": "2026-05-28T00:00:00Z",
        "proxy_commit": "c74d5e0485b8457de68c2e5ac2b32877fbbb3932",
        "runsc": {
            "platform": "systrap",
            "version": "runsc version release-20260511.0",
            "binary_path": "/usr/local/bin/runsc",
            "binary_digest": "sha256:" + "a" * 64,
        },
        "gates": {
            gate["id"]: {
                "status": "passed",
                "evidence": "target-lab evidence for " + gate["id"],
            }
            for gate in MODULE.required_gates()
        },
    }


def valid_release_evidence():
    return {
        "contract": MODULE.CONTRACT,
        "qualification": "runtime-isolation",
        "result": "passed",
        "commit": "3ade70d11f70e386b52653b557513b6b94b29316",
        "generated_at": "2026-05-28T00:00:00Z",
        "context": {
            "runsc": {
                "version": {"ok": True, "output": "runsc version release-20260511.0\nspec: 1.2.1"},
                "binary_path": "/usr/local/bin/runsc",
                "binary_digest": "sha256:" + "b" * 64,
            },
            "proxy": {
                "commit": "c74d5e0485b8457de68c2e5ac2b32877fbbb3932",
                "dirty": False,
            },
        },
        "release_completion": {
            "selected_gates_passed": True,
            "release_complete": False,
            "missing_supplied_evidence": ["adversarial_lab"],
        },
        "release_gate_inventory": {
            "total": len(MODULE.required_gates()),
            "gates": MODULE.required_gates(),
        },
        "supplied_evidence": {
            "cutover": {"status": "passed", "path": "gate:cutover_inventory"},
            "reconciliation": {"status": "passed", "path": "gate:runtime_reconciliation_evidence"},
            "rootfs_image": {"status": "passed", "path": "gate:rootfs_image_inspection"},
            "proxy_contract": {"status": "passed", "path": "gate:pinned_proxy_contract"},
        },
        "gates": [
            {"name": "go_runtime_isolation_packages", "status": "passed"},
            {"name": "go_turn_start_latency_bench", "status": "passed"},
            {"name": "python_sandbox_and_release_tools", "status": "passed"},
            {"name": "runtime_isolation_static_release_scans", "status": "passed"},
            {"name": "cutover_inventory", "status": "passed"},
            {"name": "runtime_reconciliation_evidence", "status": "passed"},
            {"name": "rootfs_image_inspection", "status": "passed"},
            {"name": "pinned_proxy_contract", "status": "passed"},
        ],
    }


if __name__ == "__main__":
    unittest.main()
