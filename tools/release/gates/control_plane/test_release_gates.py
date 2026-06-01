import json
import tempfile
import unittest
from pathlib import Path

from tools.release.suites import control_plane as MODULE


class ReleaseGatesTest(unittest.TestCase):
    def test_default_selected_gates_are_deterministic_only(self):
        args = type(
            "Args",
            (),
            {
                "include_proxy": False,
                "include_bridge_lab": False,
                "include_live_latency": False,
            },
        )()

        gates = MODULE.selected_gates(args)

        self.assertEqual([gate.name for gate in gates], [
            "go_orchestrator_packages",
            "go_turn_start_latency_bench",
            "python_control_plane_tools_and_sandbox",
        ])
        self.assertEqual({gate.category for gate in gates}, {"deterministic"})
        self.assertIn("tools/release/gates/control_plane/test_release_gates.py", gates[2].command)

    def test_optional_flags_add_external_gates(self):
        args = type(
            "Args",
            (),
            {
                "include_proxy": True,
                "include_bridge_lab": True,
                "include_live_latency": True,
            },
        )()

        gates = MODULE.selected_gates(args)

        self.assertEqual([gate.name for gate in gates[-3:]], [
            "pinned_proxy_contract",
            "gvisor_bridge_durability_lab",
            "live_turn_start_latency",
        ])
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

    def test_evidence_and_output_are_json_serializable(self):
        payload = MODULE.evidence(
            [
                {"name": "ok", "status": "passed"},
                {"name": "bad", "status": "failed"},
            ],
            commit="abc123",
            context={"git": {"commit": "abc123"}, "harness_config": {}},
        )

        self.assertEqual(payload["result"], "failed")
        self.assertEqual(payload["commit"], "abc123")
        self.assertEqual(payload["context"]["git"]["commit"], "abc123")
        self.assertIn("harness_config", payload["context"])
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "evidence.json"
            MODULE.write_output(output, payload)
            self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["commit"], "abc123")

    def test_load_release_config_extracts_release_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "harness.yaml"
            config.write_text(
                """
harness:
  max_sessions: 30
  network:
    cidr_pool: 10.200.0.0/16
    egress:
      dns_policy: hostnames_only
  events:
    emit_output_batch_max_rows: 64
    emit_output_batch_max_age: 100ms
  bridge:
    poll_interval: 5ms
    lease_ttl: 60s
    ack_started_grace: 90s
""",
                encoding="utf-8",
            )

            values = MODULE.load_release_config(config)

            self.assertEqual(values["harness.bridge.poll_interval"], "5ms")
            self.assertEqual(values["harness.events.emit_output_batch_max_rows"], "64")
            self.assertNotIn("harness.secrets.readers_gid", values)

    def test_attach_structured_output_reads_bridge_lab_evidence(self):
        with tempfile.TemporaryDirectory(prefix="harness-bridge-durability-lab.") as tmp:
            evidence = Path(tmp) / "evidence.json"
            evidence.write_text(json.dumps({"result": "passed", "workdir": tmp}), encoding="utf-8")
            result = {
                "name": "gvisor_bridge_durability_lab",
                "stdout_tail": "logs\n" + str(evidence) + "\n",
            }

            enriched = MODULE.attach_structured_output(result)

            self.assertEqual(enriched["evidence_path"], str(evidence))
            self.assertEqual(enriched["structured_output"]["result"], "passed")

    def test_attach_structured_output_reads_json_stdout(self):
        result = {
            "name": "live_turn_start_latency",
            "stdout_tail": json.dumps({"max_ms": 12.5}),
        }

        enriched = MODULE.attach_structured_output(result)

        self.assertEqual(enriched["structured_output"]["max_ms"], 12.5)


if __name__ == "__main__":
    unittest.main()
