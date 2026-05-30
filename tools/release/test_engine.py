#!/usr/bin/env python3
import json
import tempfile
import unittest
from pathlib import Path

from tools.release import engine


class EngineTest(unittest.TestCase):
    def test_run_gate_captures_success_and_failure(self):
        ok = engine.Gate(name="ok", command=("python3", "-c", "print('ok')"), cwd=Path.cwd(), category="test")
        bad = engine.Gate(name="bad", command=("python3", "-c", "import sys; print('bad'); sys.exit(3)"), cwd=Path.cwd(), category="test")

        ok_result = engine.run_gate(ok)
        bad_result = engine.run_gate(bad)

        self.assertEqual(ok_result["status"], "passed")
        self.assertEqual(ok_result["returncode"], 0)
        self.assertIn("ok", ok_result["stdout_tail"])
        self.assertEqual(bad_result["status"], "failed")
        self.assertEqual(bad_result["returncode"], 3)

    def test_run_gate_missing_command_is_failed(self):
        gate = engine.Gate(name="missing", command=("definitely-not-a-real-binary-xyz",), cwd=Path.cwd(), category="test")
        result = engine.run_gate(gate)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["returncode"], 127)

    def test_attach_structured_output_parses_json_for_named_gate(self):
        result = {"name": "live_turn_start_latency", "stdout_tail": json.dumps({"max_ms": 12.5})}
        enriched = engine.attach_structured_output(result, json_output_gates={"live_turn_start_latency"}, evidence_file_gates=set())
        self.assertEqual(enriched["structured_output"]["max_ms"], 12.5)

    def test_attach_structured_output_ignores_unlisted_gate(self):
        result = {"name": "other", "stdout_tail": json.dumps({"x": 1})}
        enriched = engine.attach_structured_output(result, json_output_gates={"live_turn_start_latency"}, evidence_file_gates=set())
        self.assertNotIn("structured_output", enriched)

    def test_attach_structured_output_reads_evidence_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            evidence = Path(tmp) / "evidence.json"
            evidence.write_text(json.dumps({"result": "passed"}), encoding="utf-8")
            result = {"name": "gvisor_bridge_durability_lab", "stdout_tail": "log line\n" + str(evidence) + "\n"}
            enriched = engine.attach_structured_output(result, json_output_gates=set(), evidence_file_gates={"gvisor_bridge_durability_lab"})
            self.assertEqual(enriched["evidence_path"], str(evidence))
            self.assertEqual(enriched["structured_output"]["result"], "passed")

    def test_gate_inventory_parses_sections_and_bullets(self):
        with tempfile.TemporaryDirectory() as tmp:
            doc = Path(tmp) / "gates.md"
            doc.write_text(
                "# Title\n\n## Alpha Gates\n- first gate\n- second gate\n  continued\n\n## Beta Gates\n- third gate\n",
                encoding="utf-8",
            )
            inv = engine.gate_inventory(doc)
            self.assertEqual(inv["total"], 3)
            self.assertEqual(inv["counts_by_section"], {"Alpha Gates": 2, "Beta Gates": 1})
            ids = [g["id"] for g in inv["gates"]]
            self.assertIn("alpha_gates_001", ids)
            self.assertIn("beta_gates_001", ids)
            self.assertTrue(any("continued" in g["text"] for g in inv["gates"]))

    def test_release_completion_requires_all_labels(self):
        incomplete = engine.release_completion(
            [{"status": "passed"}], {}, ("a", "b"), "note-incomplete", "note-complete", require_release_evidence=True
        )
        self.assertFalse(incomplete["release_complete"])
        self.assertEqual(incomplete["missing_supplied_evidence"], ["a", "b"])
        self.assertEqual(incomplete["note"], "note-incomplete")

        complete = engine.release_completion(
            [{"status": "passed"}], {"a": {}, "b": {}}, ("a", "b"), "note-incomplete", "note-complete", require_release_evidence=True
        )
        self.assertTrue(complete["release_complete"])
        self.assertEqual(complete["note"], "note-complete")

    def test_release_completion_without_requirement_is_not_complete(self):
        completion = engine.release_completion([{"status": "passed"}], {"a": {}}, ("a",), "n0", "n1")
        self.assertTrue(completion["selected_gates_passed"])
        self.assertFalse(completion["release_complete"])

    def test_parse_supplied_evidence_records_digest(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ev.json"
            path.write_text(json.dumps({"result": "passed"}), encoding="utf-8")
            supplied = engine.parse_supplied_evidence([f"label={path}"])
            self.assertIn("label", supplied)
            self.assertTrue(supplied["label"]["digest"].startswith("sha256:"))
            self.assertEqual(supplied["label"]["status"], "passed")

    def test_parse_supplied_evidence_rejects_bad_spec(self):
        with self.assertRaises(ValueError):
            engine.parse_supplied_evidence(["no-equals-sign"])

    def test_run_static_checks_lacks_and_contains(self):
        with tempfile.TemporaryDirectory() as tmp:
            good = Path(tmp) / "good.txt"
            good.write_text("hello world\n", encoding="utf-8")
            specs = [
                {"name": "lacks_ok", "kind": "lacks", "path": good, "patterns": (("absent", "goodbye"),)},
                {"name": "contains_ok", "kind": "contains", "path": good, "patterns": (("present", "hello"),)},
            ]
            payload = engine.run_static_checks(specs)
            self.assertEqual(payload["status"], "passed")

            specs.append({"name": "lacks_fail", "kind": "lacks", "path": good, "patterns": (("found", "hello"),)})
            payload = engine.run_static_checks(specs)
            self.assertEqual(payload["status"], "failed")
            json.dumps(payload)

    def test_render_gate_list(self):
        gate = engine.Gate(name="g", command=("echo", "hi"), cwd=Path("/tmp"), category="deterministic")
        listed = engine.render_gate_list([gate])
        self.assertEqual(listed, [{"name": "g", "category": "deterministic", "command": ["echo", "hi"], "cwd": "/tmp"}])

    def test_write_output_creates_nested_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "nested" / "evidence.json"
            engine.write_output(output, {"result": "passed"})
            self.assertEqual(json.loads(output.read_text(encoding="utf-8"))["result"], "passed")


if __name__ == "__main__":
    unittest.main()
