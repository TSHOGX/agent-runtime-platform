#!/usr/bin/env python3
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("release-gates.py")
SPEC = importlib.util.spec_from_file_location("phase8_release_gates", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ReleaseGatesTest(unittest.TestCase):
    def test_default_selected_gates_are_deterministic_only(self):
        args = argparse_namespace(
            include_prior_release=False,
            include_cutover_inventory=False,
            include_reconciliation=False,
            include_rootfs_inspection=False,
            include_proxy=False,
            include_adversarial_lab=False,
            adversarial_lab_report="",
            include_bridge_lab=False,
            include_live_latency=False,
        )

        gates = MODULE.selected_gates(args)

        self.assertEqual(
            [gate.name for gate in gates],
            [
                "go_runtime_isolation_packages",
                "go_turn_start_latency_bench",
                "python_sandbox_and_release_tools",
                "runtime_isolation_static_release_scans",
            ],
        )
        self.assertEqual({gate.category for gate in gates}, {"deterministic"})
        self.assertIn("tools/phase8/test_release_gates.py", gates[2].command)
        self.assertIn("tools/phase8/test_adversarial_lab.py", gates[2].command)
        self.assertIn("tools/phase8/test_cutover_inventory.py", gates[2].command)
        self.assertIn("tools/phase8/test_reconciliation_evidence.py", gates[2].command)
        self.assertIn("tools/phase8/test_rootfs_inspect.py", gates[2].command)

    def test_optional_flags_add_external_and_compatibility_gates(self):
        args = argparse_namespace(
            include_prior_release=True,
            include_cutover_inventory=True,
            include_reconciliation=True,
            include_rootfs_inspection=True,
            include_proxy=True,
            include_adversarial_lab=True,
            adversarial_lab_report="/tmp/phase8-lab.json",
            include_bridge_lab=True,
            include_live_latency=True,
        )

        gates = MODULE.selected_gates(args)

        self.assertEqual(
            [gate.name for gate in gates[-8:]],
            [
                "prior_deterministic_release_runner",
                "cutover_inventory",
                "runtime_reconciliation_evidence",
                "rootfs_image_inspection",
                "pinned_proxy_contract",
                "phase8_adversarial_lab",
                "gvisor_bridge_durability_lab",
                "live_turn_start_latency",
            ],
        )
        self.assertEqual(gates[-8].category, "compatibility")
        self.assertEqual({gates[-7].category, gates[-6].category, gates[-5].category, gates[-3].category}, {"evidence"})
        self.assertEqual({gates[-4].category, gates[-2].category, gates[-1].category}, {"external"})
        self.assertIn("/tmp/phase8-lab.json", gates[-3].command)

    def test_run_gate_captures_success_and_failure(self):
        ok = MODULE.Gate(
            name="ok",
            command=("python3", "-c", "print('ok')"),
            cwd=MODULE.REPO_ROOT,
            category="test",
        )
        bad = MODULE.Gate(
            name="bad",
            command=("python3", "-c", "import sys; print('bad'); sys.exit(3)"),
            cwd=MODULE.REPO_ROOT,
            category="test",
        )

        ok_result = MODULE.run_gate(ok)
        bad_result = MODULE.run_gate(bad)

        self.assertEqual(ok_result["status"], "passed")
        self.assertEqual(ok_result["returncode"], 0)
        self.assertIn("ok", ok_result["stdout_tail"])
        self.assertEqual(bad_result["status"], "failed")
        self.assertEqual(bad_result["returncode"], 3)
        self.assertIn("bad", bad_result["stdout_tail"])

    def test_release_gate_inventory_parses_all_sections(self):
        inventory = MODULE.release_gate_inventory()

        self.assertGreater(inventory["total"], 80)
        self.assertIn("Contract Gates", inventory["counts_by_section"])
        self.assertIn("Root and Mount Gates", inventory["counts_by_section"])
        self.assertIn("Migration Gates", inventory["counts_by_section"])
        self.assertTrue(any("sandbox_contract_version" in gate["text"] for gate in inventory["gates"]))

    def test_evidence_is_not_release_complete_without_supplied_lab_evidence(self):
        payload = MODULE.evidence(
            [{"name": "ok", "status": "passed"}],
            commit="abc123",
            context={"git": {"commit": "abc123"}},
        )

        self.assertEqual(payload["result"], "passed")
        self.assertFalse(payload["release_completion"]["release_complete"])
        self.assertEqual(
            payload["release_completion"]["missing_supplied_evidence"],
            list(MODULE.REQUIRED_SUPPLIED_EVIDENCE),
        )
        self.assertIn("release_gate_inventory", payload)
        self.assertEqual(payload["contract"], "sandbox-isolation-v1")
        self.assertNotIn("phase", payload)

    def test_require_release_evidence_fails_when_supplied_labels_are_missing(self):
        payload = MODULE.evidence(
            [{"name": "ok", "status": "passed"}],
            commit="abc123",
            context={"git": {"commit": "abc123"}},
            require_release_evidence=True,
        )

        self.assertEqual(payload["result"], "failed")
        self.assertFalse(payload["release_completion"]["release_complete"])

    def test_rootfs_gate_output_satisfies_rootfs_supplied_evidence(self):
        rootfs_payload = {
            "status": "passed",
            "rootfs_digest": "sha256:" + "a" * 64,
            "checks": [],
        }

        payload = MODULE.evidence(
            [
                {
                    "name": "rootfs_image_inspection",
                    "status": "passed",
                    "structured_output": rootfs_payload,
                }
            ],
            commit="abc123",
            context={"git": {"commit": "abc123"}},
        )

        self.assertIn("rootfs_image", payload["supplied_evidence"])
        self.assertEqual(payload["supplied_evidence"]["rootfs_image"]["digest"], rootfs_payload["rootfs_digest"])

    def test_cutover_gate_output_satisfies_cutover_supplied_evidence(self):
        cutover_payload = {
            "status": "passed",
            "qualification": "cutover-inventory",
            "blockers": [],
        }

        payload = MODULE.evidence(
            [
                {
                    "name": "cutover_inventory",
                    "status": "passed",
                    "structured_output": cutover_payload,
                }
            ],
            commit="abc123",
            context={"git": {"commit": "abc123"}},
        )

        self.assertIn("cutover", payload["supplied_evidence"])
        self.assertEqual(payload["supplied_evidence"]["cutover"]["status"], "passed")

    def test_reconciliation_gate_output_satisfies_reconciliation_supplied_evidence(self):
        reconciliation_payload = {
            "status": "passed",
            "qualification": "reconciliation-evidence",
            "issues": [],
        }

        payload = MODULE.evidence(
            [
                {
                    "name": "runtime_reconciliation_evidence",
                    "status": "passed",
                    "structured_output": reconciliation_payload,
                }
            ],
            commit="abc123",
            context={"git": {"commit": "abc123"}},
        )

        self.assertIn("reconciliation", payload["supplied_evidence"])
        self.assertEqual(payload["supplied_evidence"]["reconciliation"]["status"], "passed")

    def test_proxy_gate_output_satisfies_proxy_supplied_evidence(self):
        payload = MODULE.evidence(
            [
                {
                    "name": "pinned_proxy_contract",
                    "status": "passed",
                }
            ],
            commit="abc123",
            context={
                "git": {"commit": "abc123"},
                "proxy": {"commit": "proxyabc", "dirty": False},
            },
        )

        self.assertIn("proxy_contract", payload["supplied_evidence"])
        self.assertEqual(payload["supplied_evidence"]["proxy_contract"]["digest"], "proxyabc")

    def test_adversarial_lab_output_satisfies_supplied_evidence(self):
        lab_payload = {
            "status": "passed",
            "qualification": "adversarial-lab",
            "required_total": 112,
            "reported_total": 112,
            "passed_total": 112,
            "issues": [],
        }

        payload = MODULE.evidence(
            [
                {
                    "name": "phase8_adversarial_lab",
                    "status": "passed",
                    "structured_output": lab_payload,
                }
            ],
            commit="abc123",
            context={"git": {"commit": "abc123"}},
        )

        self.assertIn("adversarial_lab", payload["supplied_evidence"])
        self.assertEqual(payload["supplied_evidence"]["adversarial_lab"]["status"], "passed")

    def test_supplied_evidence_records_digest_and_allows_release_completion(self):
        with tempfile.TemporaryDirectory() as tmp:
            specs = []
            for label in MODULE.REQUIRED_SUPPLIED_EVIDENCE:
                path = Path(tmp) / f"{label}.json"
                path.write_text(json.dumps({"result": "passed", "label": label}), encoding="utf-8")
                specs.append(f"{label}={path}")

            supplied = MODULE.parse_supplied_evidence(specs)
            payload = MODULE.evidence(
                [{"name": "ok", "status": "passed"}],
                commit="abc123",
                context={"git": {"commit": "abc123"}},
                supplied_evidence=supplied,
                require_release_evidence=True,
            )

            self.assertEqual(payload["result"], "passed")
            self.assertTrue(payload["release_completion"]["release_complete"])
            self.assertEqual(payload["release_completion"]["missing_supplied_evidence"], [])
            self.assertTrue(payload["supplied_evidence"]["cutover"]["digest"].startswith("sha256:"))

    def test_static_checks_are_json_serializable(self):
        payload = MODULE.static_checks()

        self.assertIn(payload["status"], {"passed", "failed"})
        json.dumps(payload)

    def test_write_output(self):
        payload = {"contract": "sandbox-isolation-v1", "result": "passed"}
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "nested" / "evidence.json"
            MODULE.write_output(output, payload)
            self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["contract"], "sandbox-isolation-v1")


def argparse_namespace(**kwargs):
    return type("Args", (), kwargs)()


if __name__ == "__main__":
    unittest.main()
