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


def valid_report():
    return {
        "contract": MODULE.CONTRACT,
        "qualification": "adversarial-lab",
        "target_host": "phase8-lab-host",
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


if __name__ == "__main__":
    unittest.main()
