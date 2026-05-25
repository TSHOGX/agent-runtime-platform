#!/usr/bin/env python3
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("release-gates.py")
SPEC = importlib.util.spec_from_file_location("phase7_release_gates", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ReleaseGatesTest(unittest.TestCase):
    def test_default_selected_gates_are_deterministic_only(self):
        args = type(
            "Args",
            (),
            {
                "include_proxy": False,
                "include_bridge_lab": False,
                "include_secret_lab": False,
                "include_live_latency": False,
            },
        )()

        gates = MODULE.selected_gates(args)

        self.assertEqual([gate.name for gate in gates], [
            "go_phase7_packages",
            "go_phase7_turn_start_latency_bench",
            "python_phase7_tools_and_sandbox",
        ])
        self.assertEqual({gate.category for gate in gates}, {"deterministic"})
        self.assertIn("tools/phase7/test_release_gates.py", gates[2].command)

    def test_optional_flags_add_external_gates(self):
        args = type(
            "Args",
            (),
            {
                "include_proxy": True,
                "include_bridge_lab": True,
                "include_secret_lab": True,
                "include_live_latency": True,
            },
        )()

        gates = MODULE.selected_gates(args)

        self.assertEqual([gate.name for gate in gates[-4:]], [
            "pinned_proxy_contract",
            "gvisor_bridge_durability_lab",
            "secret_permission_lab",
            "live_turn_start_latency",
        ])
        self.assertEqual({gate.category for gate in gates[-4:]}, {"external"})

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

    def test_evidence_and_output_are_json_serializable(self):
        payload = MODULE.evidence(
            [
                {"name": "ok", "status": "passed"},
                {"name": "bad", "status": "failed"},
            ],
            commit="abc123",
        )

        self.assertEqual(payload["result"], "failed")
        self.assertEqual(payload["commit"], "abc123")
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "evidence.json"
            MODULE.write_output(output, payload)
            self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["commit"], "abc123")


if __name__ == "__main__":
    unittest.main()
