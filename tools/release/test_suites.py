#!/usr/bin/env python3
import unittest

from tools.release import run as dispatcher
from tools.release.suites import (
    agent_capability,
    control_plane,
    sandbox_isolation,
    static_manifests,
)


def _args(**kwargs):
    return type("Args", (), kwargs)()


class ControlPlaneSuiteTest(unittest.TestCase):
    def test_default_gates(self):
        args = _args(include_proxy=False, include_bridge_lab=False, include_live_latency=False)
        gates = control_plane.selected_gates(args)
        self.assertEqual(
            [g.name for g in gates],
            ["go_orchestrator_packages", "go_turn_start_latency_bench", "python_control_plane_tools_and_sandbox"],
        )
        self.assertEqual({g.category for g in gates}, {"deterministic"})

    def test_evidence_envelope_uses_qualification_key(self):
        payload = control_plane.evidence(
            [{"name": "ok", "status": "passed"}], commit="abc", context={"git": {"commit": "abc"}, "harness_config": {}}
        )
        self.assertEqual(payload["qualification"], "control-plane")
        self.assertEqual(payload["result"], "passed")
        self.assertNotIn("contract", payload)


class SandboxIsolationSuiteTest(unittest.TestCase):
    def test_default_gates(self):
        args = _args(
            include_cutover_inventory=False,
            include_reconciliation=False,
            include_rootfs_inspection=False,
            include_proxy=False,
            include_adversarial_lab=False,
            adversarial_lab_report="",
            include_bridge_lab=False,
            include_live_latency=False,
        )
        gates = sandbox_isolation.selected_gates(args)
        self.assertEqual(
            [g.name for g in gates],
            [
                "go_runtime_isolation_packages",
                "go_turn_start_latency_bench",
                "python_sandbox_and_release_tools",
                "runtime_isolation_static_release_scans",
            ],
        )

    def test_evidence_envelope_is_contract_shaped_without_rollout_id(self):
        payload = sandbox_isolation.evidence(
            [{"name": "ok", "status": "passed"}], commit="abc", context={"git": {"commit": "abc"}}
        )
        self.assertEqual(payload["contract"], "sandbox-isolation-v1")
        self.assertNotIn("rollout_id", payload)
        self.assertIn("release_gate_inventory", payload)
        self.assertEqual(
            payload["release_completion"]["missing_supplied_evidence"],
            list(sandbox_isolation.REQUIRED_SUPPLIED_EVIDENCE),
        )

    def test_inventory_total_and_frozen_id(self):
        inv = sandbox_isolation.release_gate_inventory()
        self.assertGreater(inv["total"], 80)
        self.assertTrue(any(g["id"] == "root_and_mount_gates_027" for g in inv["gates"]))

    def test_static_checks_match_full_manifest(self):
        payload = sandbox_isolation.static_checks()
        names = [c["name"] for c in payload["checks"]]
        self.assertEqual(names, [c["name"] for c in static_manifests.sandbox_isolation_checks()])
        self.assertEqual(len(names), 12)


class ReservedSuiteTest(unittest.TestCase):
    def test_agent_capability_is_next_stage_subset(self):
        names = [c["name"] for c in static_manifests.agent_capability_checks()]
        full = [c["name"] for c in static_manifests.sandbox_isolation_checks()]
        self.assertEqual(len(names), 8)
        self.assertTrue(set(names).issubset(set(full)))
        self.assertEqual(agent_capability.static_checks()["checks"][0]["name"], names[0])


class DispatcherTest(unittest.TestCase):
    def test_extract_suite_space_and_equals(self):
        self.assertEqual(dispatcher.extract_suite(["--suite", "control_plane", "--list"]), ("control_plane", ["--list"]))
        self.assertEqual(dispatcher.extract_suite(["--suite=sandbox_isolation", "--static-only"]), ("sandbox_isolation", ["--static-only"]))

    def test_extract_suite_absent(self):
        self.assertEqual(dispatcher.extract_suite(["--list"]), (None, ["--list"]))

    def test_registered_suites(self):
        self.assertEqual(set(dispatcher.SUITES), {"control_plane", "sandbox_isolation", "agent_capability"})


if __name__ == "__main__":
    unittest.main()
