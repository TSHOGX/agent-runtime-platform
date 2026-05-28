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
            include_rootfs_inspection=False,
            include_proxy=False,
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
        self.assertIn("tools/phase8/test_rootfs_inspect.py", gates[2].command)

    def test_optional_flags_add_external_and_compatibility_gates(self):
        args = argparse_namespace(
            include_prior_release=True,
            include_rootfs_inspection=True,
            include_proxy=True,
            include_bridge_lab=True,
            include_live_latency=True,
        )

        gates = MODULE.selected_gates(args)

        self.assertEqual(
            [gate.name for gate in gates[-5:]],
            [
                "prior_deterministic_release_runner",
                "rootfs_image_inspection",
                "pinned_proxy_contract",
                "gvisor_bridge_durability_lab",
                "live_turn_start_latency",
            ],
        )
        self.assertEqual(gates[-5].category, "compatibility")
        self.assertEqual(gates[-4].category, "evidence")
        self.assertEqual({gate.category for gate in gates[-3:]}, {"external"})

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
