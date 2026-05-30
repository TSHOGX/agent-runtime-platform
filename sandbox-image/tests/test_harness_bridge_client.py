import importlib.util
from importlib.machinery import SourceFileLoader
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "files" / "usr" / "local" / "bin" / "harness-bridge-client"
BUILD_ROOTFS = Path(__file__).resolve().parents[1] / "build-rootfs.sh"
spec = importlib.util.spec_from_loader("harness_bridge_client", SourceFileLoader("harness_bridge_client", str(SCRIPT)))
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)

MANIFEST_LIB = SCRIPT.parents[1] / "lib" / "harness_manifest.py"
manifest_spec = importlib.util.spec_from_loader("harness_manifest", SourceFileLoader("harness_manifest", str(MANIFEST_LIB)))
manifest = importlib.util.module_from_spec(manifest_spec)
manifest_spec.loader.exec_module(manifest)


class BridgeClientTest(unittest.TestCase):
    def test_build_rootfs_reuse_generates_pi_agent_manifest(self):
        with tempfile.TemporaryDirectory() as root:
            rootfs = Path(root)
            bin_dir = rootfs / "usr" / "local" / "bin"
            bin_dir.mkdir(parents=True)
            pi = bin_dir / "pi"
            pi.write_text("#!/bin/sh\n", encoding="utf-8")
            pi.chmod(0o755)

            env = os.environ.copy()
            env.update(
                {
                    "ROOTFS_DIR": str(rootfs),
                    "SANDBOX_AGENT_DRIVERS": "pi",
                    "FORCE": "0",
                }
            )
            subprocess.run(["bash", str(BUILD_ROOTFS)], check=True, env=env, capture_output=True, text=True)

            manifest = json.loads((rootfs / "etc" / "harness-image" / "agents.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["build_input"]["sandbox_agent_drivers"], ["pi"])
            self.assertEqual(len(manifest["drivers"]), 1)
            pi_entry = manifest["drivers"][0]
            self.assertEqual(pi_entry["driver_id"], "pi")
            self.assertEqual(pi_entry["package_name"], "@earendil-works/pi-coding-agent")
            self.assertEqual(pi_entry["package_version"], "0.77.0")
            self.assertEqual(pi_entry["event_schema_version"], "pi_rpc_events_v1.0")
            self.assertEqual(pi_entry["binary_path"], "/usr/local/bin/pi")
            self.assertTrue(pi_entry["installed_binary_digest"].startswith("sha256:"))
            self.assertIn("/agent-home/.pi/agent/models.json", pi_entry["installed_config_paths"])

    def test_queue_write_orders_numeric_sequence(self):
        with tempfile.TemporaryDirectory() as root:
            queue = bridge.Queue(root, bridge.OUTBOX)
            queue.write({"message_id": "msg2", "type": "heartbeat", "session_id": "s", "generation_id": "g"})
            queue.write({"message_id": "msg3", "type": "heartbeat", "session_id": "s", "generation_id": "g"})
            first = Path(root) / bridge.OUTBOX / "00000000000000000001.json"
            first.rename(Path(root) / bridge.OUTBOX / "00000000000000000010.json")
            queue.write({"message_id": "msg11", "type": "heartbeat", "session_id": "s", "generation_id": "g"})

            files = queue.read_all()
            self.assertEqual([seq for seq, _, _ in files], [2, 10, 11])
            self.assertEqual([envelope["message_id"] for _, _, envelope in files], ["msg3", "msg2", "msg11"])

    def test_hello_records_resume_boundary(self):
        with tempfile.TemporaryDirectory() as root:
            client = bridge.BridgeClient(root, "sess", "gen", "claude", poll_interval=0.001)
            response = bridge.Queue(root, bridge.INBOX)

            sent = client.send("hello", request_id="req_hello")
            response.write(
                {
                    "message_id": "host_msg",
                    "request_id": sent["request_id"],
                    "type": "hello_ack",
                    "session_id": "sess",
                    "generation_id": "gen",
                    "payload": {
                        "last_output_sequence_by_turn": {"42": 7},
                        "leased_turn_id": 42,
                        "server_time": "2026-05-25T00:00:00Z",
                    },
                }
            )

            payload = client.wait_response("req_hello", timeout=0.1)["payload"]
            client.last_output_sequence_by_turn = {
                int(turn_id): int(sequence)
                for turn_id, sequence in payload["last_output_sequence_by_turn"].items()
            }
            client.leased_turn_id = payload["leased_turn_id"]

            self.assertEqual(client.last_output_sequence_by_turn, {42: 7})
            self.assertEqual(client.leased_turn_id, 42)

    def test_hello_sends_bridge_protocol_v2_identity(self):
        with tempfile.TemporaryDirectory() as root:
            client = bridge.BridgeClient(root, "sess", "gen", "claude", poll_interval=0.001)
            inbox = bridge.Queue(root, bridge.INBOX)
            original_write = bridge.Queue.write
            sent = {}

            def write_and_respond(queue, envelope):
                if queue.name == bridge.OUTBOX and envelope.get("type") == "hello":
                    sent.update(envelope)
                    inbox.write(
                        {
                            "message_id": "host_hello",
                            "request_id": envelope["request_id"],
                            "type": "hello_ack",
                            "session_id": "sess",
                            "generation_id": "gen",
                            "payload": {"last_output_sequence_by_turn": {}},
                        }
                    )
                return original_write(queue, envelope)

            with mock.patch.object(bridge.Queue, "write", write_and_respond):
                client.hello(timeout=0.1)

            self.assertEqual(
                sent["payload"],
                {"driver_id": "claude_code", "protocol_version": 2, "turn_input_schema": "RunTurn"},
            )

    def test_claim_and_lifecycle_messages(self):
        with tempfile.TemporaryDirectory() as root:
            client = bridge.BridgeClient(root, "sess", "gen", "claude", poll_interval=0.001)
            inbox = bridge.Queue(root, bridge.INBOX)

            sent = client.send("claim_next_turn", request_id="req_claim")
            inbox.write(
                {
                    "message_id": "host_grant",
                    "request_id": sent["request_id"],
                    "type": "grant",
                    "session_id": "sess",
                    "generation_id": "gen",
                    "payload": {"turn_id": 9, "sequence": 1, "turn_input_schema": "RunTurn", "input": {"content": "run"}},
                }
            )
            grant = client.wait_response("req_claim", timeout=0.1)["payload"]
            self.assertEqual(grant["turn_id"], 9)

            client.ack_turn_started(9, "10.240.0.2")
            client.emit_output(9, 1, {"line": "ok"})
            client.ack_turn_completed(9)

            outbox = bridge.Queue(root, bridge.OUTBOX)
            messages = [envelope for _, _, envelope in outbox.read_all()]
            self.assertEqual(
                [message["type"] for message in messages[-3:]],
                ["ack_turn_started", "emit_output", "ack_turn_completed"],
            )
            self.assertEqual(messages[-2]["payload"]["output_sequence"], 1)

    def test_resume_turn_returns_grant_payload(self):
        with tempfile.TemporaryDirectory() as root:
            client = bridge.BridgeClient(root, "sess", "gen", "claude", poll_interval=0.001)

            sent_request_id = None
            original_send = client.send

            def record_send(message_type, request_id=None, turn_id=None, payload=None):
                nonlocal sent_request_id
                envelope = original_send(message_type, request_id=request_id, turn_id=turn_id, payload=payload)
                if message_type == "resume_turn":
                    sent_request_id = envelope["request_id"]
                return envelope

            client.send = record_send
            with mock.patch.object(client, "wait_response") as wait_response:
                wait_response.return_value = {
                    "type": "grant",
                    "payload": {"turn_id": 9, "sequence": 1, "turn_input_schema": "RunTurn", "input": {"content": "resume"}, "replayed": True},
                }
                grant = client.resume_turn(9, timeout=0.1)
            self.assertEqual(grant["turn_id"], 9)
            self.assertTrue(grant["replayed"])
            self.assertIsNotNone(sent_request_id)
            wait_response.assert_called_once_with(sent_request_id, timeout=0.1)
            outbox = bridge.Queue(root, bridge.OUTBOX)
            messages = [envelope for _, _, envelope in outbox.read_all()]
            self.assertEqual(messages[-1]["type"], "resume_turn")
            self.assertEqual(messages[-1]["turn_id"], 9)

    def test_heartbeat_writes_bridge_mtime_file_and_message(self):
        with tempfile.TemporaryDirectory() as root:
            client = bridge.BridgeClient(root, "sess", "gen", "claude", poll_interval=0.001)
            client.heartbeat()

            heartbeat = Path(root) / bridge.HEARTBEAT / bridge.BRIDGE_HEARTBEAT
            self.assertTrue(heartbeat.exists())
            self.assertTrue(heartbeat.read_text(encoding="ascii").strip().isdigit())
            outbox = bridge.Queue(root, bridge.OUTBOX)
            messages = [envelope for _, _, envelope in outbox.read_all()]
            self.assertEqual(messages[-1]["type"], "heartbeat")

    def test_checkpoint_ready_marker_is_written_and_cleared(self):
        with tempfile.TemporaryDirectory() as root:
            client = bridge.BridgeClient(root, "sess", "gen", "claude", poll_interval=0.001)
            client.mark_checkpoint_ready()
            ready = Path(root) / bridge.HEARTBEAT / bridge.CHECKPOINT_READY
            self.assertTrue(ready.exists())
            self.assertTrue(ready.read_text(encoding="ascii").strip().isdigit())
            client.clear_checkpoint_ready()
            self.assertFalse(ready.exists())

    def test_sandbox_source_ip_ignores_manifest_env(self):
        env_name = "HARNESS_" + "SANDBOX_SOURCE_IP"
        with mock.patch.dict(os.environ, {env_name: "10.240.0.2"}, clear=False):
            with mock.patch.object(bridge.subprocess, "check_output") as check_output:
                check_output.return_value = json.dumps([
                    {"ifname": "lo", "addr_info": [{"family": "inet", "local": "127.0.0.1"}]},
                    {"ifname": "eth0", "addr_info": [{"family": "inet", "local": "10.241.0.2"}]},
                ])
                self.assertEqual(bridge.sandbox_source_ip(), "10.241.0.2")

        check_output.assert_called_once_with(["ip", "-j", "addr", "show"], text=True)

    def test_heartbeat_loop_uses_configured_interval(self):
        with tempfile.TemporaryDirectory() as root:
            args = argparse_namespace(
                bridge_dir=root,
                session_id="sess",
                generation_id="gen",
                agent="claude",
                poll_interval=0.001,
                interval=0.25,
            )
            calls = []

            def heartbeat_once(self):
                calls.append((self.session_id, self.generation_id))
                raise KeyboardInterrupt()

            with mock.patch.object(bridge.BridgeClient, "heartbeat", heartbeat_once):
                with self.assertRaises(KeyboardInterrupt):
                    bridge.run_heartbeat_loop(args)

            self.assertEqual(calls, [("sess", "gen")])

    def test_claim_loop_claims_and_records_lifecycle(self):
        with tempfile.TemporaryDirectory() as root:
            args = argparse_namespace(
                bridge_dir=root,
                session_id="sess",
                generation_id="gen",
                agent="sh",
                poll_interval=0.001,
                timeout=0.1,
                base_url="",
                healthz_statuses="200",
                http_timeout=0.1,
                heartbeat_interval=60,
                idle_interval=0.001,
                max_turns=1,
                max_empty_polls=0,
            )
            inbox = bridge.Queue(root, bridge.INBOX)
            original_write = bridge.Queue.write

            def write_and_respond(queue, envelope):
                message_type = envelope.get("type")
                request_id = envelope.get("request_id")
                if queue.name == bridge.OUTBOX and message_type in {"hello", "probe_network", "claim_next_turn"}:
                    response_type = "no_work"
                    payload = {}
                    if message_type == "hello":
                        response_type = "hello_ack"
                        payload = {"last_output_sequence_by_turn": {}}
                    elif message_type == "claim_next_turn":
                        response_type = "grant"
                        payload = {"turn_id": 9, "sequence": 1, "turn_input_schema": "RunTurn", "input": {"content": "echo ok"}}
                    inbox.write(
                        {
                            "message_id": f"host_{message_type}",
                            "request_id": request_id,
                            "type": response_type,
                            "session_id": "sess",
                            "generation_id": "gen",
                            "payload": payload,
                        }
                    )
                return original_write(queue, envelope)

            class FakeRunner:
                def run_turn(self, content, emit):
                    self.content = content
                    emit("stdout", '{"type":"harness.shell_output","text":"ok"}')
                    return "completed", "", ""

                def close(self):
                    self.closed = True

            runner = FakeRunner()
            with mock.patch.object(bridge.Queue, "write", write_and_respond):
                with mock.patch.object(bridge, "sandbox_source_ip", return_value="10.240.0.2"):
                    bridge.run_claim_loop(args, runner=runner)

            self.assertEqual(runner.content, "echo ok")
            outbox = bridge.Queue(root, bridge.OUTBOX)
            messages = [envelope for _, _, envelope in outbox.read_all()]
            types = [message["type"] for message in messages]
            self.assertIn("ack_turn_started", types)
            self.assertIn("emit_output", types)
            self.assertEqual(types[-1], "ack_turn_completed")
            output = next(message for message in messages if message["type"] == "emit_output")
            self.assertEqual(output["turn_id"], 9)
            self.assertEqual(output["payload"]["output_sequence"], 1)
            self.assertEqual(output["payload"]["payload"]["line"], '{"type":"harness.shell_output","text":"ok"}')
            ready = Path(root) / bridge.HEARTBEAT / bridge.CHECKPOINT_READY
            self.assertTrue(ready.exists())

    def test_execute_grant_rejects_non_run_turn_schema(self):
        with tempfile.TemporaryDirectory() as root:
            client = bridge.BridgeClient(root, "sess", "gen", "sh", poll_interval=0.001)

            class FakeRunner:
                def run_turn(self, content, emit):
                    raise AssertionError("runner should not execute invalid schema")

            with self.assertRaisesRegex(RuntimeError, "unsupported turn_input_schema legacy"):
                bridge.execute_grant(
                    client,
                    FakeRunner(),
                    {"turn_id": 9, "sequence": 1, "turn_input_schema": "legacy", "input": {"content": "run"}},
                )

    def test_native_events_probe_runner_emits_schema_tagged_payload(self):
        with tempfile.TemporaryDirectory() as root:
            client = bridge.BridgeClient(root, "sess", "gen", "native_events_probe", poll_interval=0.001)
            runner = bridge.make_turn_runner("native_events_probe")
            with mock.patch.object(bridge, "sandbox_source_ip", return_value="10.240.0.2"):
                bridge.execute_grant(
                    client,
                    runner,
                    {"turn_id": 9, "sequence": 1, "turn_input_schema": "RunTurn", "input": {"content": "native ok"}},
                )

            messages = [envelope for _, _, envelope in bridge.Queue(root, bridge.OUTBOX).read_all()]
            output = next(message for message in messages if message["type"] == "emit_output")
            self.assertEqual(output["payload"]["output_sequence"], 1)
            self.assertEqual(
                output["payload"]["payload"],
                {
                    "schema": "harness_native_events_v1",
                    "event": {"type": "agent.message", "payload": {"content": "native ok"}},
                },
            )
            self.assertEqual(messages[-1]["type"], "ack_turn_completed")

    def test_execute_grant_omits_driver_state_update_for_failed_turns(self):
        with tempfile.TemporaryDirectory() as root:
            client = bridge.BridgeClient(root, "sess", "gen", "pi", poll_interval=0.001)

            class FailedRunner:
                def run_turn(self, content, emit):
                    return "failed", "agent_execution_failed", "boom", {"driver_id": "pi"}

            with mock.patch.object(bridge, "sandbox_source_ip", return_value="10.240.0.2"):
                bridge.execute_grant(
                    client,
                    FailedRunner(),
                    {"turn_id": 9, "sequence": 1, "turn_input_schema": "RunTurn", "input": {"content": "run"}},
                )

            messages = [envelope for _, _, envelope in bridge.Queue(root, bridge.OUTBOX).read_all()]
            completion = messages[-1]
            self.assertEqual(completion["type"], "ack_turn_completed")
            self.assertEqual(completion["payload"]["status"], "failed")
            self.assertNotIn("driver_state_update", completion["payload"])

    def test_pi_rpc_runner_uses_session_rpc_and_returns_driver_state_update(self):
        class FakeStdin:
            def __init__(self):
                self.writes = []

            def write(self, value):
                self.writes.append(value)

            def flush(self):
                pass

        class FakeStdout:
            def __init__(self, lines):
                self.lines = list(lines)

            def readline(self):
                if not self.lines:
                    return ""
                return self.lines.pop(0)

        class FakeProcess:
            def __init__(self, lines):
                self.stdin = FakeStdin()
                self.stdout = FakeStdout(lines)
                self.stderr = iter([])

            def poll(self):
                return None

            def terminate(self):
                pass

        with tempfile.TemporaryDirectory() as root:
            agent_dir = Path(root) / "agent"
            session_dir = agent_dir / "sessions"
            session_dir.mkdir(parents=True)
            session_file = session_dir / "session-1.jsonl"
            session_file.write_text("{}\n", encoding="utf-8")
            models = agent_dir / "models.json"
            settings = agent_dir / "settings.json"
            for path in (models, settings):
                path.write_text("{}\n", encoding="utf-8")
                path.chmod(0o444)

            lines = [
                '{"id":"harness-turn-9","type":"response","command":"prompt","success":true}\n',
                '{"type":"message_update","messageId":"msg-1","assistantMessageEvent":{"type":"text_delta","delta":"ok"}}\n',
                '{"type":"turn_end","status":"completed"}\n',
                json.dumps(
                    {
                        "id": "harness-turn-9-stats",
                        "type": "response",
                        "command": "get_session_stats",
                        "success": True,
                        "data": {"sessionFile": str(session_file), "sessionId": "pi-session-1"},
                    },
                    separators=(",", ":"),
                )
                + "\n",
            ]
            captured = {}

            def popen(command, **kwargs):
                captured["command"] = command
                captured["env"] = kwargs.get("env") or {}
                captured["process"] = FakeProcess(lines)
                return captured["process"]

            patches = [
                mock.patch.object(bridge, "PI_CODING_AGENT_DIR", str(agent_dir)),
                mock.patch.object(bridge, "PI_SESSION_DIR", str(session_dir)),
                mock.patch.object(bridge, "PI_MODELS_SANDBOX_PATH", str(models)),
                mock.patch.object(bridge, "PI_SETTINGS_SANDBOX_PATH", str(settings)),
                mock.patch.object(bridge.subprocess, "Popen", side_effect=popen),
                mock.patch.dict(
                    os.environ,
                    {
                        "PI_CODING_AGENT_DIR": str(agent_dir),
                        "PI_CODING_AGENT_SESSION_DIR": str(session_dir),
                        "PI_OFFLINE": "1",
                        "PI_SKIP_VERSION_CHECK": "1",
                        "PI_TELEMETRY": "0",
                        "PI_MODEL": "sonnet",
                        "HARNESS_AGENT_UID": str(os.geteuid()),
                        "HARNESS_AGENT_GID": str(os.getegid()),
                    },
                    clear=True,
                ),
            ]
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
                runner = bridge.PiRPCTurnRunner()
                runner.set_turn_context(
                    {
                        "turn_id": 9,
                        "driver_state": {
                            "driver_id": "pi",
                            "state_digest": "sha256:" + "a" * 64,
                            "state_version": 1,
                            "state_payload": {
                                "schema_version": 1,
                                "driver_id": "pi",
                                "state_kind": "pi_uninitialized",
                                "session_dir": str(session_dir),
                            },
                        },
                    }
                )
                emitted = []
                status, error_class, error, update = runner.run_turn("hello", lambda stream, line: emitted.append((stream, line)))

            self.assertEqual((status, error_class, error), ("completed", "", ""))
            self.assertEqual(captured["command"][:7], ["/usr/local/bin/pi", "--mode", "rpc", "--provider", "harness_anthropic_proxy", "--model", "sonnet"])
            self.assertIn("--session-dir", captured["command"])
            self.assertNotIn("--no-session", captured["command"])
            self.assertEqual(captured["env"]["PI_OFFLINE"], "1")
            self.assertEqual(captured["env"]["PI_TELEMETRY"], "0")
            writes = [json.loads(value) for value in captured["process"].stdin.writes]
            self.assertEqual(writes[0], {"id": "harness-turn-9", "type": "prompt", "message": "hello"})
            self.assertEqual(writes[1], {"id": "harness-turn-9-stats", "type": "get_session_stats"})
            self.assertEqual([json.loads(line)["type"] for _, line in emitted], ["response", "message_update", "turn_end"])
            self.assertEqual(update["driver_id"], "pi")
            self.assertEqual(update["previous_state_digest"], "sha256:" + "a" * 64)
            self.assertEqual(update["state_version"], 2)
            self.assertEqual(update["state_payload"]["selected_session_relpath"], "session-1.jsonl")
            self.assertEqual(update["state_payload"]["selected_session_id"], "pi-session-1")
            self.assertTrue(update["state_digest"].startswith("sha256:"))

    def test_pi_rpc_runner_keeps_stderr_out_of_json_rpc_stream(self):
        class FakeStdin:
            def __init__(self):
                self.writes = []

            def write(self, value):
                self.writes.append(value)

            def flush(self):
                pass

        class FakeStdout:
            def __init__(self, lines):
                self.lines = list(lines)

            def readline(self):
                if not self.lines:
                    return ""
                return self.lines.pop(0)

        class FakeProcess:
            def __init__(self, stdout_lines):
                self.stdin = FakeStdin()
                self.stdout = FakeStdout(stdout_lines)
                self.stderr = iter(["Warning: startup settings lock failed\n"])

            def poll(self):
                return None

            def terminate(self):
                pass

        with tempfile.TemporaryDirectory() as root:
            agent_dir = Path(root) / "agent"
            session_dir = agent_dir / "sessions"
            session_dir.mkdir(parents=True)
            session_file = session_dir / "session-1.jsonl"
            session_file.write_text("{}\n", encoding="utf-8")
            models = agent_dir / "models.json"
            settings = agent_dir / "settings.json"
            for path in (models, settings):
                path.write_text("{}\n", encoding="utf-8")
                path.chmod(0o444)

            lines = [
                '{"id":"harness-turn-9","type":"response","command":"prompt","success":true}\n',
                '{"type":"turn_end","status":"completed"}\n',
                json.dumps(
                    {
                        "id": "harness-turn-9-stats",
                        "type": "response",
                        "command": "get_session_stats",
                        "success": True,
                        "data": {"sessionFile": str(session_file), "sessionId": "pi-session-1"},
                    },
                    separators=(",", ":"),
                )
                + "\n",
            ]
            captured = {}

            def popen(command, **kwargs):
                captured["stderr"] = kwargs.get("stderr")
                captured["process"] = FakeProcess(lines)
                return captured["process"]

            patches = [
                mock.patch.object(bridge, "PI_CODING_AGENT_DIR", str(agent_dir)),
                mock.patch.object(bridge, "PI_SESSION_DIR", str(session_dir)),
                mock.patch.object(bridge, "PI_MODELS_SANDBOX_PATH", str(models)),
                mock.patch.object(bridge, "PI_SETTINGS_SANDBOX_PATH", str(settings)),
                mock.patch.object(bridge.subprocess, "Popen", side_effect=popen),
                mock.patch.object(bridge.threading.Thread, "start", lambda self: None),
                mock.patch.dict(
                    os.environ,
                    {
                        "PI_CODING_AGENT_DIR": str(agent_dir),
                        "PI_CODING_AGENT_SESSION_DIR": str(session_dir),
                        "PI_OFFLINE": "1",
                        "PI_SKIP_VERSION_CHECK": "1",
                        "PI_TELEMETRY": "0",
                        "PI_MODEL": "sonnet",
                        "HARNESS_AGENT_UID": str(os.geteuid()),
                        "HARNESS_AGENT_GID": str(os.getegid()),
                    },
                    clear=True,
                ),
            ]
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
                runner = bridge.PiRPCTurnRunner()
                runner.set_turn_context(
                    {
                        "turn_id": 9,
                        "driver_state": {
                            "driver_id": "pi",
                            "state_digest": "sha256:" + "a" * 64,
                            "state_version": 1,
                            "state_payload": {"schema_version": 1, "driver_id": "pi", "state_kind": "pi_uninitialized"},
                        },
                    }
                )
                status, error_class, error, update = runner.run_turn("hello", lambda stream, line: None)

            self.assertEqual((status, error_class, error), ("completed", "", ""))
            self.assertEqual(captured["stderr"], bridge.subprocess.PIPE)
            self.assertEqual(update["state_payload"]["selected_session_id"], "pi-session-1")

    def test_pi_rpc_runner_reports_recent_stderr_when_process_exits(self):
        class FakeStdin:
            def write(self, value):
                pass

            def flush(self):
                pass

        class FakeStdout:
            def readline(self):
                return ""

        class FakeProcess:
            def __init__(self):
                self.stdin = FakeStdin()
                self.stdout = FakeStdout()
                self.stderr = iter([])

            def poll(self):
                return 1

            def terminate(self):
                pass

        with tempfile.TemporaryDirectory() as root:
            agent_dir = Path(root) / "agent"
            session_dir = agent_dir / "sessions"
            session_dir.mkdir(parents=True)
            models = agent_dir / "models.json"
            settings = agent_dir / "settings.json"
            for path in (models, settings):
                path.write_text("{}\n", encoding="utf-8")
                path.chmod(0o444)

            patches = [
                mock.patch.object(bridge, "PI_CODING_AGENT_DIR", str(agent_dir)),
                mock.patch.object(bridge, "PI_SESSION_DIR", str(session_dir)),
                mock.patch.object(bridge, "PI_MODELS_SANDBOX_PATH", str(models)),
                mock.patch.object(bridge, "PI_SETTINGS_SANDBOX_PATH", str(settings)),
                mock.patch.object(bridge.subprocess, "Popen", return_value=FakeProcess()),
                mock.patch.object(bridge.threading.Thread, "start", lambda self: None),
                mock.patch.dict(
                    os.environ,
                    {
                        "PI_CODING_AGENT_DIR": str(agent_dir),
                        "PI_CODING_AGENT_SESSION_DIR": str(session_dir),
                        "PI_OFFLINE": "1",
                        "PI_SKIP_VERSION_CHECK": "1",
                        "PI_TELEMETRY": "0",
                        "PI_MODEL": "sonnet",
                        "HARNESS_AGENT_UID": str(os.geteuid()),
                        "HARNESS_AGENT_GID": str(os.getegid()),
                    },
                    clear=True,
                ),
            ]
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
                runner = bridge.PiRPCTurnRunner()
                runner.set_turn_context(
                    {
                        "turn_id": 9,
                        "driver_state": {
                            "driver_id": "pi",
                            "state_digest": "sha256:" + "a" * 64,
                            "state_version": 1,
                            "state_payload": {"schema_version": 1, "driver_id": "pi", "state_kind": "pi_uninitialized"},
                        },
                    }
                )
                runner.stderr_tail.append('Error: Unknown provider "harness_anthropic_proxy"')

                with self.assertRaisesRegex(RuntimeError, 'pi rpc process exited with status 1: Error: Unknown provider "harness_anthropic_proxy"'):
                    runner.run_turn("hello", lambda stream, line: None)

    def test_pi_rpc_runner_restores_sidecar_session_before_prompt(self):
        class FakeStdin:
            def __init__(self):
                self.writes = []

            def write(self, value):
                self.writes.append(value)

            def flush(self):
                pass

        class FakeStdout:
            def __init__(self, lines):
                self.lines = list(lines)

            def readline(self):
                if not self.lines:
                    return ""
                return self.lines.pop(0)

        class FakeProcess:
            def __init__(self, lines):
                self.stdin = FakeStdin()
                self.stdout = FakeStdout(lines)
                self.stderr = iter([])

            def poll(self):
                return None

            def terminate(self):
                pass

        with tempfile.TemporaryDirectory() as root:
            agent_dir = Path(root) / "agent"
            session_dir = agent_dir / "sessions"
            session_dir.mkdir(parents=True)
            session_file = session_dir / "session-1.jsonl"
            session_file.write_text("{}\n", encoding="utf-8")
            models = agent_dir / "models.json"
            settings = agent_dir / "settings.json"
            for path in (models, settings):
                path.write_text("{}\n", encoding="utf-8")
                path.chmod(0o444)

            lines = [
                '{"id":"harness-restore-10","type":"response","command":"switch_session","success":true}\n',
                json.dumps(
                    {
                        "id": "harness-restore-10-stats",
                        "type": "response",
                        "command": "get_session_stats",
                        "success": True,
                        "data": {"sessionFile": str(session_file), "sessionId": "pi-session-1"},
                    },
                    separators=(",", ":"),
                )
                + "\n",
                '{"id":"harness-turn-10","type":"response","command":"prompt","success":true}\n',
                '{"type":"turn_end","status":"completed"}\n',
                json.dumps(
                    {
                        "id": "harness-turn-10-stats",
                        "type": "response",
                        "command": "get_session_stats",
                        "success": True,
                        "data": {"sessionFile": str(session_file), "sessionId": "pi-session-1"},
                    },
                    separators=(",", ":"),
                )
                + "\n",
            ]
            captured = {}

            def popen(command, **kwargs):
                captured["process"] = FakeProcess(lines)
                return captured["process"]

            patches = [
                mock.patch.object(bridge, "PI_CODING_AGENT_DIR", str(agent_dir)),
                mock.patch.object(bridge, "PI_SESSION_DIR", str(session_dir)),
                mock.patch.object(bridge, "PI_MODELS_SANDBOX_PATH", str(models)),
                mock.patch.object(bridge, "PI_SETTINGS_SANDBOX_PATH", str(settings)),
                mock.patch.object(bridge.subprocess, "Popen", side_effect=popen),
                mock.patch.dict(
                    os.environ,
                    {
                        "PI_CODING_AGENT_DIR": str(agent_dir),
                        "PI_CODING_AGENT_SESSION_DIR": str(session_dir),
                        "PI_OFFLINE": "1",
                        "PI_SKIP_VERSION_CHECK": "1",
                        "PI_TELEMETRY": "0",
                        "PI_MODEL": "sonnet",
                        "HARNESS_AGENT_UID": str(os.geteuid()),
                        "HARNESS_AGENT_GID": str(os.getegid()),
                    },
                    clear=True,
                ),
            ]
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
                runner = bridge.PiRPCTurnRunner()
                runner.set_turn_context(
                    {
                        "turn_id": 10,
                        "driver_state": {
                            "driver_id": "pi",
                            "state_digest": "sha256:" + "b" * 64,
                            "state_version": 2,
                            "state_payload": {
                                "schema_version": 1,
                                "driver_id": "pi",
                                "state_kind": "pi_session",
                                "session_dir": str(session_dir),
                                "selected_session_relpath": "session-1.jsonl",
                                "selected_session_file": str(session_file),
                                "selected_session_id": "pi-session-1",
                                "last_completed_turn_id": "9",
                            },
                        },
                    }
                )
                status, error_class, error, update = runner.run_turn("restored", lambda stream, line: None)

            self.assertEqual((status, error_class, error), ("completed", "", ""))
            writes = [json.loads(value) for value in captured["process"].stdin.writes]
            self.assertEqual([write["type"] for write in writes], ["switch_session", "get_session_stats", "prompt", "get_session_stats"])
            self.assertEqual(writes[0]["sessionPath"], str(session_file))
            self.assertNotIn("sessionFile", writes[0])
            self.assertEqual(writes[2], {"id": "harness-turn-10", "type": "prompt", "message": "restored"})
            self.assertEqual(update["previous_state_digest"], "sha256:" + "b" * 64)
            self.assertEqual(update["state_version"], 3)
            self.assertEqual(update["state_payload"]["selected_session_id"], "pi-session-1")

    def test_pi_rpc_runner_reports_failed_command_detail(self):
        class FakeStdin:
            def __init__(self):
                self.writes = []

            def write(self, value):
                self.writes.append(value)

            def flush(self):
                pass

        class FakeStdout:
            def __init__(self, lines):
                self.lines = list(lines)

            def readline(self):
                if not self.lines:
                    return ""
                return self.lines.pop(0)

        class FakeProcess:
            def __init__(self, lines):
                self.stdin = FakeStdin()
                self.stdout = FakeStdout(lines)
                self.stderr = iter([])

            def poll(self):
                return None

            def terminate(self):
                pass

        with tempfile.TemporaryDirectory() as root:
            agent_dir = Path(root) / "agent"
            session_dir = agent_dir / "sessions"
            session_dir.mkdir(parents=True)
            session_file = session_dir / "session-1.jsonl"
            session_file.write_text("{}\n", encoding="utf-8")
            models = agent_dir / "models.json"
            settings = agent_dir / "settings.json"
            for path in (models, settings):
                path.write_text("{}\n", encoding="utf-8")
                path.chmod(0o444)

            lines = ['{"id":"harness-restore-10","type":"response","command":"switch_session","success":false,"error":"sessionPath is required"}\n']

            def popen(command, **kwargs):
                return FakeProcess(lines)

            patches = [
                mock.patch.object(bridge, "PI_CODING_AGENT_DIR", str(agent_dir)),
                mock.patch.object(bridge, "PI_SESSION_DIR", str(session_dir)),
                mock.patch.object(bridge, "PI_MODELS_SANDBOX_PATH", str(models)),
                mock.patch.object(bridge, "PI_SETTINGS_SANDBOX_PATH", str(settings)),
                mock.patch.object(bridge.subprocess, "Popen", side_effect=popen),
                mock.patch.dict(
                    os.environ,
                    {
                        "PI_CODING_AGENT_DIR": str(agent_dir),
                        "PI_CODING_AGENT_SESSION_DIR": str(session_dir),
                        "PI_OFFLINE": "1",
                        "PI_SKIP_VERSION_CHECK": "1",
                        "PI_TELEMETRY": "0",
                        "PI_MODEL": "sonnet",
                        "HARNESS_AGENT_UID": str(os.geteuid()),
                        "HARNESS_AGENT_GID": str(os.getegid()),
                    },
                    clear=True,
                ),
            ]
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
                runner = bridge.PiRPCTurnRunner()
                runner.set_turn_context(
                    {
                        "turn_id": 10,
                        "driver_state": {
                            "driver_id": "pi",
                            "state_digest": "sha256:" + "b" * 64,
                            "state_version": 2,
                            "state_payload": {
                                "schema_version": 1,
                                "driver_id": "pi",
                                "state_kind": "pi_session",
                                "session_dir": str(session_dir),
                                "selected_session_relpath": "session-1.jsonl",
                                "selected_session_file": str(session_file),
                                "selected_session_id": "pi-session-1",
                                "last_completed_turn_id": "9",
                            },
                        },
                    }
                )
                with self.assertRaisesRegex(RuntimeError, "pi rpc command switch_session failed: sessionPath is required"):
                    runner.run_turn("restored", lambda stream, line: None)

    def test_pi_rpc_runner_requires_restore_payload(self):
        runner = bridge.PiRPCTurnRunner()
        runner.set_turn_context({"driver_state": {"state_digest": "sha256:" + "c" * 64}})
        with self.assertRaisesRegex(RuntimeError, "state_payload is required"):
            runner._restore_session_if_needed(11)

    def test_normalize_pi_session_file_rejects_symlink_session_root(self):
        with tempfile.TemporaryDirectory() as root:
            agent_dir = Path(root) / "agent"
            agent_dir.mkdir()
            real_sessions = Path(root) / "real-sessions"
            real_sessions.mkdir()
            session_link = agent_dir / "sessions"
            session_link.symlink_to(real_sessions, target_is_directory=True)
            with mock.patch.object(bridge, "PI_SESSION_DIR", str(session_link)):
                with self.assertRaisesRegex(RuntimeError, "session dir must not use sandbox symlink"):
                    bridge.normalize_pi_session_file("session-1.jsonl")

    def test_pi_config_materialization_rejects_missing_config(self):
        with tempfile.TemporaryDirectory() as root:
            agent_dir = Path(root) / "agent"
            agent_dir.mkdir(parents=True)
            settings = agent_dir / "settings.json"
            settings.write_text("{}\n", encoding="utf-8")
            settings.chmod(0o444)
            with mock.patch.object(bridge, "PI_MODELS_SANDBOX_PATH", str(agent_dir / "models.json")):
                with mock.patch.object(bridge, "PI_SETTINGS_SANDBOX_PATH", str(settings)):
                    with self.assertRaisesRegex(RuntimeError, "pi config missing"):
                        bridge.validate_pi_config_materialization()

    def test_pi_config_materialization_rejects_symlink_config_path(self):
        with tempfile.TemporaryDirectory() as root:
            agent_dir = Path(root) / "agent"
            agent_dir.mkdir(parents=True)
            real_models = Path(root) / "models.json"
            real_models.write_text("{}\n", encoding="utf-8")
            real_models.chmod(0o444)
            models = agent_dir / "models.json"
            models.symlink_to(real_models)
            settings = agent_dir / "settings.json"
            settings.write_text("{}\n", encoding="utf-8")
            settings.chmod(0o444)
            with mock.patch.object(bridge, "PI_MODELS_SANDBOX_PATH", str(models)):
                with mock.patch.object(bridge, "PI_SETTINGS_SANDBOX_PATH", str(settings)):
                    with self.assertRaisesRegex(RuntimeError, "pi config must not use sandbox symlink"):
                        bridge.validate_pi_config_materialization()

    def test_pi_rpc_runner_rejects_writable_config(self):
        with tempfile.TemporaryDirectory() as root:
            agent_dir = Path(root) / "agent"
            session_dir = agent_dir / "sessions"
            session_dir.mkdir(parents=True)
            models = agent_dir / "models.json"
            settings = agent_dir / "settings.json"
            models.write_text("{}\n", encoding="utf-8")
            settings.write_text("{}\n", encoding="utf-8")
            models.chmod(0o644)
            settings.chmod(0o444)
            with mock.patch.object(bridge, "PI_MODELS_SANDBOX_PATH", str(models)):
                with mock.patch.object(bridge, "PI_SETTINGS_SANDBOX_PATH", str(settings)):
                    with mock.patch.dict(os.environ, {"HARNESS_AGENT_UID": str(os.geteuid()), "HARNESS_AGENT_GID": str(os.getegid())}, clear=False):
                        with self.assertRaisesRegex(RuntimeError, "writable by sandbox uid/gid"):
                            bridge.validate_pi_config_materialization()

    def test_claim_loop_handles_multiple_turns(self):
        with tempfile.TemporaryDirectory() as root:
            args = argparse_namespace(
                bridge_dir=root,
                session_id="sess",
                generation_id="gen",
                agent="sh",
                poll_interval=0.001,
                timeout=0.1,
                base_url="",
                healthz_statuses="200",
                http_timeout=0.1,
                heartbeat_interval=60,
                idle_interval=0.001,
                max_turns=2,
                max_empty_polls=0,
            )
            grants = [
                {"turn_id": 9, "sequence": 1, "turn_input_schema": "RunTurn", "input": {"content": "first"}},
                {"turn_id": 10, "sequence": 2, "turn_input_schema": "RunTurn", "input": {"content": "second"}},
            ]
            inbox = bridge.Queue(root, bridge.INBOX)
            original_write = bridge.Queue.write

            def write_and_respond(queue, envelope):
                message_type = envelope.get("type")
                request_id = envelope.get("request_id")
                if queue.name == bridge.OUTBOX and message_type in {"hello", "probe_network", "claim_next_turn"}:
                    response_type = "no_work"
                    payload = {}
                    if message_type == "hello":
                        response_type = "hello_ack"
                        payload = {"last_output_sequence_by_turn": {}}
                    elif message_type == "claim_next_turn" and grants:
                        response_type = "grant"
                        payload = grants.pop(0)
                    inbox.write(
                        {
                            "message_id": f"host_{message_type}_{request_id}",
                            "request_id": request_id,
                            "type": response_type,
                            "session_id": "sess",
                            "generation_id": "gen",
                            "payload": payload,
                        }
                    )
                return original_write(queue, envelope)

            class FakeRunner:
                def __init__(self):
                    self.contents = []

                def run_turn(self, content, emit):
                    self.contents.append(content)
                    emit("stdout", content + " output")
                    return "completed", "", ""

                def close(self):
                    pass

            runner = FakeRunner()
            with mock.patch.object(bridge.Queue, "write", write_and_respond):
                with mock.patch.object(bridge, "sandbox_source_ip", return_value="10.240.0.2"):
                    bridge.run_claim_loop(args, runner=runner)

            self.assertEqual(runner.contents, ["first", "second"])
            messages = [envelope for _, _, envelope in bridge.Queue(root, bridge.OUTBOX).read_all()]
            self.assertEqual([message["type"] for message in messages].count("claim_next_turn"), 2)
            self.assertEqual([message["turn_id"] for message in messages if message["type"] == "ack_turn_started"], [9, 10])
            self.assertEqual([message["turn_id"] for message in messages if message["type"] == "ack_turn_completed"], [9, 10])
            outputs = [message for message in messages if message["type"] == "emit_output"]
            self.assertEqual([message["turn_id"] for message in outputs], [9, 10])
            self.assertEqual([message["payload"]["output_sequence"] for message in outputs], [1, 1])

    def test_claim_loop_resumes_leased_turn_from_hello_ack(self):
        with tempfile.TemporaryDirectory() as root:
            args = argparse_namespace(
                bridge_dir=root,
                session_id="sess",
                generation_id="gen",
                agent="sh",
                poll_interval=0.001,
                timeout=0.1,
                base_url="",
                healthz_statuses="200",
                http_timeout=0.1,
                heartbeat_interval=60,
                idle_interval=0.001,
                max_turns=1,
                max_empty_polls=0,
            )
            inbox = bridge.Queue(root, bridge.INBOX)
            original_write = bridge.Queue.write

            def write_and_respond(queue, envelope):
                message_type = envelope.get("type")
                request_id = envelope.get("request_id")
                if queue.name == bridge.OUTBOX and message_type in {"hello", "probe_network", "resume_turn"}:
                    response_type = "no_work"
                    payload = {}
                    if message_type == "hello":
                        response_type = "hello_ack"
                        payload = {"last_output_sequence_by_turn": {"9": 3}, "leased_turn_id": 9}
                    elif message_type == "resume_turn":
                        response_type = "grant"
                        payload = {"turn_id": 9, "sequence": 1, "turn_input_schema": "RunTurn", "input": {"content": "resume"}}
                    inbox.write(
                        {
                            "message_id": f"host_{message_type}",
                            "request_id": request_id,
                            "type": response_type,
                            "session_id": "sess",
                            "generation_id": "gen",
                            "turn_id": envelope.get("turn_id"),
                            "payload": payload,
                        }
                    )
                return original_write(queue, envelope)

            class FakeRunner:
                def run_turn(self, content, emit):
                    emit("stdout", "resumed")
                    return "completed", "", ""

                def close(self):
                    pass

            with mock.patch.object(bridge.Queue, "write", write_and_respond):
                bridge.run_claim_loop(args, runner=FakeRunner())

            outbox = bridge.Queue(root, bridge.OUTBOX)
            messages = [envelope for _, _, envelope in outbox.read_all()]
            self.assertIn("resume_turn", [message["type"] for message in messages])
            output = next(message for message in messages if message["type"] == "emit_output")
            self.assertEqual(output["payload"]["output_sequence"], 4)

    def test_network_probe_checks_health_only(self):
        with mock.patch.object(bridge, "http_status", return_value=200) as http_status:
            bridge.run_network_probe("http://10.240.0.1:8082", {200}, 0.1)

        http_status.assert_called_once_with("http://10.240.0.1:8082/healthz", timeout=0.1)

    def test_network_probe_rejects_unaccepted_statuses(self):
        with mock.patch.object(bridge, "http_status", return_value=204):
            with self.assertRaisesRegex(RuntimeError, r"probe GET /healthz returned 204"):
                bridge.run_network_probe("http://10.240.0.1:8082", {200}, 0.1)

    def test_shell_probe_skips_proxy_http_probe(self):
        with tempfile.TemporaryDirectory() as root:
            args = argparse_namespace(
                bridge_dir=root,
                session_id="sess",
                generation_id="gen",
                agent="sh",
                poll_interval=0.001,
                timeout=0.1,
                base_url="",
                healthz_statuses="200",
                http_timeout=0.1,
            )
            inbox = bridge.Queue(root, bridge.INBOX)

            with mock.patch.object(bridge, "run_network_probe") as run_network_probe:
                sent = []
                original_write = bridge.Queue.write

                def write_and_respond(queue, envelope):
                    sent.append(dict(envelope))
                    message_type = envelope.get("type")
                    request_id = envelope.get("request_id")
                    if queue.name == bridge.OUTBOX and message_type in {"hello", "probe_network"}:
                        inbox.write(
                            {
                                "message_id": f"host_{message_type}",
                                "request_id": request_id,
                                "type": "hello_ack" if message_type == "hello" else "no_work",
                                "session_id": "sess",
                                "generation_id": "gen",
                                "payload": {"last_output_sequence_by_turn": {}} if message_type == "hello" else {},
                            }
                        )
                    return original_write(queue, envelope)

                with mock.patch.object(bridge.Queue, "write", write_and_respond):
                    bridge.run_probe(args)

            run_network_probe.assert_not_called()
            self.assertEqual([message["type"] for message in sent if message["type"] in {"hello", "probe_network"}], ["hello", "probe_network"])
            self.assertTrue((Path(root) / bridge.HEARTBEAT / bridge.BRIDGE_HEARTBEAT).exists())
            self.assertTrue((Path(root) / bridge.HEARTBEAT / bridge.CHECKPOINT_READY).exists())

    def test_claude_runner_ignores_legacy_secret_environment(self):
        class FakeStdin:
            def __init__(self):
                self.writes = []
                self.closed = False

            def write(self, value):
                self.writes.append(value)

            def close(self):
                self.closed = True

        class FakeProcess:
            def __init__(self):
                self.stdin = FakeStdin()
                self.stdout = iter(['{"type":"result","result":"ok"}\n'])

            def wait(self):
                return 0

        captured = {}

        def popen(command, **kwargs):
            captured["command"] = command
            captured["env"] = kwargs.get("env") or {}
            captured["process"] = FakeProcess()
            return captured["process"]

        with mock.patch.dict(
            os.environ,
            {
                "CLAUDE_SESSION_UUID": "11111111-2222-3333-4444-555555555555",
                "SESSION_ID": "sess",
                "HARNESS_AGENT_HOME": "/agent-home",
                "HARNESS_AGENT_UID": "65534",
                "HARNESS_AGENT_GID": "65534",
                "HARNESS_SECRET_READERS_GID": "12345",
                "ANTHROPIC_BASE_URL": "http://harness-model-proxy.internal:8082",
                "ANTHROPIC_AUTH_TOKEN": "legacy-token",
                "SECRET_MOUNT_PATH": "/harness-secrets",
                "ANTHROPIC_API_KEY_SECRET_ID": "anthropic_api_key",
                "ANTHROPIC_AUTH_TOKEN_SECRET_ID": "anthropic_auth_token",
                "SECRET_VERSION": "local",
            },
            clear=True,
        ):
            with mock.patch.object(bridge.subprocess, "Popen", side_effect=popen):
                runner = bridge.ClaudeTurnRunner()
                status, error_class, error = runner.run_turn("hello", lambda stream, line: None)

        self.assertEqual((status, error_class, error), ("completed", "", ""))
        command = captured["command"]
        self.assertIn("--clear-groups", command)
        self.assertNotIn("--groups", command)
        self.assertNotIn("SECRET_MOUNT_PATH", " ".join(command))
        self.assertNotIn("ANTHROPIC_AUTH_TOKEN", " ".join(command))
        self.assertIn("/usr/local/bin/claude", command)
        self.assertEqual(captured["env"]["ANTHROPIC_API_KEY"], bridge.HOST_ONLY_DUMMY_API_KEY)
        self.assertEqual(captured["env"]["ANTHROPIC_BASE_URL"], "http://harness-model-proxy.internal:8082")
        for name in (
            "SECRET_MOUNT_PATH",
            "ANTHROPIC_API_KEY_SECRET_ID",
            "ANTHROPIC_AUTH_TOKEN_SECRET_ID",
            "SECRET_VERSION",
            "HARNESS_SECRET_READERS_GID",
            "ANTHROPIC_AUTH_TOKEN",
        ):
            self.assertNotIn(name, captured["env"])
        self.assertIn('"text":"hello"', captured["process"].stdin.writes[0])

    def test_claude_runner_uses_dummy_key_without_secret_mount(self):
        class FakeStdin:
            def __init__(self):
                self.writes = []

            def write(self, value):
                self.writes.append(value)

            def close(self):
                pass

        class FakeProcess:
            def __init__(self):
                self.stdin = FakeStdin()
                self.stdout = iter(['{"type":"result","result":"ok"}\n'])

            def wait(self):
                return 0

        captured = {}

        def popen(command, **kwargs):
            captured["command"] = command
            captured["env"] = kwargs.get("env") or {}
            captured["process"] = FakeProcess()
            return captured["process"]

        with mock.patch.dict(
            os.environ,
            {
                "CLAUDE_SESSION_UUID": "11111111-2222-3333-4444-555555555555",
                "SESSION_ID": "sess",
                "HARNESS_AGENT_HOME": "/agent-home",
                "HARNESS_AGENT_UID": "65534",
                "HARNESS_AGENT_GID": "65534",
                "ANTHROPIC_BASE_URL": "http://harness-model-proxy.internal:8082",
            },
            clear=True,
        ):
            with mock.patch.object(bridge.subprocess, "Popen", side_effect=popen):
                status, error_class, error = bridge.ClaudeTurnRunner().run_turn(
                    "hello",
                    lambda stream, line: None,
                )

        self.assertEqual((status, error_class, error), ("completed", "", ""))
        command = captured["command"]
        self.assertIn("--clear-groups", command)
        self.assertNotIn("--groups", command)
        self.assertNotIn("SECRET_MOUNT_PATH", " ".join(command))
        self.assertNotIn("ANTHROPIC_API_KEY=", " ".join(command))
        self.assertEqual(captured["env"]["ANTHROPIC_API_KEY"], bridge.HOST_ONLY_DUMMY_API_KEY)
        self.assertEqual(captured["env"]["ANTHROPIC_BASE_URL"], "http://harness-model-proxy.internal:8082")
        self.assertNotIn("ANTHROPIC_AUTH_TOKEN", captured["env"])
        self.assertIn('"text":"hello"', captured["process"].stdin.writes[0])

    def test_claude_runner_skips_setpriv_when_already_sandbox_identity(self):
        class FakeStdin:
            def __init__(self):
                self.writes = []

            def write(self, value):
                self.writes.append(value)

            def close(self):
                pass

        class FakeProcess:
            def __init__(self):
                self.stdin = FakeStdin()
                self.stdout = iter(['{"type":"result","result":"ok"}\n'])

            def wait(self):
                return 0

        captured = {}

        def popen(command, **kwargs):
            captured["command"] = command
            captured["env"] = kwargs.get("env") or {}
            captured["process"] = FakeProcess()
            return captured["process"]

        with mock.patch.dict(
            os.environ,
            {
                "CLAUDE_SESSION_UUID": "11111111-2222-3333-4444-555555555555",
                "SESSION_ID": "sess",
                "HARNESS_AGENT_HOME": "/agent-home",
                "HARNESS_AGENT_UID": "65534",
                "HARNESS_AGENT_GID": "65534",
                "ANTHROPIC_BASE_URL": "http://harness-model-proxy.internal:8082",
            },
            clear=True,
        ):
            with mock.patch.object(bridge.os, "geteuid", return_value=65534):
                with mock.patch.object(bridge.os, "getegid", return_value=65534):
                    with mock.patch.object(bridge.subprocess, "Popen", side_effect=popen):
                        status, error_class, error = bridge.ClaudeTurnRunner().run_turn(
                            "hello",
                            lambda stream, line: None,
                        )

        self.assertEqual((status, error_class, error), ("completed", "", ""))
        self.assertEqual(captured["command"][0], "/usr/local/bin/claude")
        self.assertNotIn("setpriv", captured["command"])
        self.assertEqual(captured["env"]["ANTHROPIC_API_KEY"], bridge.HOST_ONLY_DUMMY_API_KEY)

    def test_claude_runner_resumes_after_first_turn(self):
        class FakeStdin:
            def write(self, value):
                pass

            def close(self):
                pass

        class FakeProcess:
            def __init__(self):
                self.stdin = FakeStdin()
                self.stdout = iter(['{"type":"result","result":"ok"}\n'])

            def wait(self):
                return 0

        commands = []

        def popen(command, **kwargs):
            commands.append(command)
            return FakeProcess()

        with mock.patch.dict(os.environ, claude_runner_env(), clear=True):
            with mock.patch.object(bridge.subprocess, "Popen", side_effect=popen):
                runner = bridge.ClaudeTurnRunner()
                runner.run_turn("first", lambda stream, line: None)
                runner.run_turn("second", lambda stream, line: None)

        self.assertIn("--session-id", commands[0])
        self.assertNotIn("--resume", commands[0])
        self.assertIn("--resume", commands[1])
        self.assertNotIn("--session-id", commands[1])

    def test_claude_runner_resumes_after_recreated_runner_when_marker_written(self):
        class FakeStdin:
            def write(self, value):
                pass

            def close(self):
                pass

        class FakeProcess:
            def __init__(self, lines):
                self.stdin = FakeStdin()
                self.stdout = iter(lines)

            def wait(self):
                return 0

        session_uuid = "11111111-2222-3333-4444-555555555555"
        command_outputs = iter(
            [
                [
                    f'{{"type":"system","subtype":"init","session_id":"{session_uuid}"}}\n',
                    f'{{"type":"result","result":"ok","session_id":"{session_uuid}"}}\n',
                ],
                ['{"type":"result","result":"ok"}\n'],
            ]
        )
        commands = []

        def popen(command, **kwargs):
            commands.append(command)
            return FakeProcess(next(command_outputs))

        with tempfile.TemporaryDirectory() as agent_home:
            env = claude_runner_env()
            env["HARNESS_AGENT_HOME"] = agent_home
            with mock.patch.dict(os.environ, env, clear=True):
                with mock.patch.object(bridge.subprocess, "Popen", side_effect=popen):
                    bridge.ClaudeTurnRunner().run_turn("first", lambda stream, line: None)
                    bridge.ClaudeTurnRunner().run_turn("second", lambda stream, line: None)

            marker = Path(agent_home) / bridge.CLAUDE_MARKER_DIR / bridge.CLAUDE_SESSION_MARKER
            self.assertEqual(json.loads(marker.read_text(encoding="utf-8"))["claude_session_uuid"], session_uuid)

        self.assertIn("--session-id", commands[0])
        self.assertNotIn("--resume", commands[0])
        self.assertIn("--resume", commands[1])
        self.assertNotIn("--session-id", commands[1])

    def test_claude_runner_resumes_after_recreated_runner_when_claude_state_exists(self):
        class FakeStdin:
            def write(self, value):
                pass

            def close(self):
                pass

        class FakeProcess:
            def __init__(self):
                self.stdin = FakeStdin()
                self.stdout = iter(['{"type":"result","result":"ok"}\n'])

            def wait(self):
                return 0

        session_uuid = "11111111-2222-3333-4444-555555555555"
        commands = []

        def popen(command, **kwargs):
            commands.append(command)
            return FakeProcess()

        with tempfile.TemporaryDirectory() as agent_home:
            state_dir = Path(agent_home) / ".claude" / "projects" / "-workspace"
            state_dir.mkdir(parents=True)
            (state_dir / f"{session_uuid}.jsonl").write_text(
                f'{{"type":"last-prompt","sessionId":"{session_uuid}"}}\n',
                encoding="utf-8",
            )
            env = claude_runner_env()
            env["HARNESS_AGENT_HOME"] = agent_home
            with mock.patch.dict(os.environ, env, clear=True):
                with mock.patch.object(bridge.subprocess, "Popen", side_effect=popen):
                    bridge.ClaudeTurnRunner().run_turn("second", lambda stream, line: None)

        self.assertIn("--resume", commands[0])
        self.assertNotIn("--session-id", commands[0])

    def test_claude_runner_maps_stream_error_to_failed_turn(self):
        class FakeStdin:
            def write(self, value):
                pass

            def close(self):
                pass

        class FakeProcess:
            def __init__(self):
                self.stdin = FakeStdin()
                self.stdout = iter(['{"type":"error","message":"model overloaded"}\n'])

            def wait(self):
                return 0

        emitted = []

        with mock.patch.dict(os.environ, claude_runner_env(), clear=True):
            with mock.patch.object(bridge.subprocess, "Popen", return_value=FakeProcess()):
                status, error_class, error = bridge.ClaudeTurnRunner().run_turn(
                    "hello",
                    lambda stream, line: emitted.append((stream, line)),
                )

        self.assertEqual((status, error_class, error), ("failed", "agent_execution_failed", "model overloaded"))
        self.assertEqual(emitted, [("stdout", '{"type":"error","message":"model overloaded"}')])

    def test_claude_runner_maps_nonzero_exit_to_failed_turn(self):
        class FakeStdin:
            def write(self, value):
                pass

            def close(self):
                pass

        class FakeProcess:
            def __init__(self):
                self.stdin = FakeStdin()
                self.stdout = iter(["plain stderr line\n"])

            def wait(self):
                return 7

        emitted = []

        with mock.patch.dict(os.environ, claude_runner_env(), clear=True):
            with mock.patch.object(bridge.subprocess, "Popen", return_value=FakeProcess()):
                status, error_class, error = bridge.ClaudeTurnRunner().run_turn(
                    "hello",
                    lambda stream, line: emitted.append((stream, line)),
                )

        self.assertEqual((status, error_class, error), ("failed", "agent_exit_nonzero", "claude exited with status 7"))
        self.assertEqual(emitted, [("stdout", "plain stderr line")])


class EntrypointStaticTest(unittest.TestCase):
    def test_manifest_digest_matches_host_fixture(self):
        fixture = Path(__file__).resolve().parents[2] / "docs" / "phase7" / "fixtures" / "control-manifest-payload.json"
        payload = json.loads(fixture.read_text(encoding="utf-8"))
        digest = "2dcc2b3e69e7792c65fb521284d627253787e77f60202482e2839fe1fd97a341"

        self.assertEqual(manifest.manifest_digest(payload), digest)
        with tempfile.TemporaryDirectory() as root:
            control_file = Path(root) / "session.json"
            control_file.write_text(json.dumps({"payload": payload, "digest": digest}), encoding="utf-8")
            self.assertEqual(manifest.load_control_manifest(control_file), payload)

            payload["generation_id"] = "gen_tampered"
            tampered_file = Path(root) / "tampered-session.json"
            tampered_file.write_text(json.dumps({"payload": payload, "digest": digest}), encoding="utf-8")
            with self.assertRaises(SystemExit) as raised:
                manifest.load_control_manifest(tampered_file)
            self.assertEqual(str(raised.exception), "control manifest digest mismatch")

    def test_entrypoint_has_probe_mode(self):
        entrypoint = SCRIPT.with_name("harness-agent-entrypoint")
        text = entrypoint.read_text(encoding="utf-8")
        self.assertIn('HARNESS_BRIDGE_MODE:-}" = "probe"', text)
        self.assertIn("exec /usr/local/bin/harness-bridge-client probe", text)
        self.assertIn('HARNESS_BRIDGE_MODE:-}" = "claim-loop"', text)
        self.assertIn("exec_bridge_client claim-loop", text)
        self.assertIn('HARNESS_BRIDGE_MODE:-auto}" = "auto"', text)
        self.assertIn("/usr/local/bin/harness-bridge-client probe", text)
        self.assertIn("/usr/local/bin/harness-bridge-client heartbeat-loop &", text)

    def test_entrypoint_expected_mismatch_message_matches_host_classifier(self):
        entrypoint = SCRIPT.with_name("harness-agent-entrypoint")
        text = entrypoint.read_text(encoding="utf-8")
        self.assertIn('expected ${label}=${expected} got ${actual}', text)

    def test_entrypoint_requires_configured_agent_identity(self):
        entrypoint = SCRIPT.with_name("harness-agent-entrypoint")
        text = entrypoint.read_text(encoding="utf-8")
        self.assertIn('AGENT_UID="${HARNESS_AGENT_UID:-}"', text)
        self.assertIn('AGENT_GID="${HARNESS_AGENT_GID:-}"', text)
        self.assertIn("HARNESS_AGENT_UID is required", text)
        self.assertIn("HARNESS_AGENT_GID is required", text)
        self.assertNotIn('AGENT_UID="65534"', text)
        self.assertNotIn('AGENT_GID="65534"', text)

    def test_entrypoint_supports_host_only_claude_credentials(self):
        entrypoint = SCRIPT.with_name("harness-agent-entrypoint")
        text = entrypoint.read_text(encoding="utf-8")
        self.assertIn("--clear-groups", text)
        self.assertIn('[ "$(id -u)" = "$AGENT_UID" ]', text)
        self.assertIn('[ "$(id -g)" = "$AGENT_GID" ]', text)
        self.assertIn("HARNESS_PROXY_DUMMY_API_KEY:-harness-model-proxy-dummy-key", text)
        self.assertIn("exec_bridge_client()", text)
        self.assertIn('/usr/local/bin/harness-bridge-client "$@"', text)
        self.assertIn("sandbox secret mounts are not supported", text)
        self.assertNotIn("SECRET_BACKED_CLAUDE", text)
        self.assertNotIn("secret-backed claude agent", text)
        self.assertNotIn("--groups \"$HARNESS_SECRET_READERS_GID\"", text)

    def test_entrypoint_exports_pi_startup_gates(self):
        entrypoint = SCRIPT.with_name("harness-agent-entrypoint")
        text = entrypoint.read_text(encoding="utf-8")
        self.assertIn('emit("PI_CODING_AGENT_DIR", data.get("pi_coding_agent_dir"))', text)
        self.assertIn('emit("PI_CODING_AGENT_SESSION_DIR", data.get("pi_coding_agent_session_dir"))', text)
        self.assertIn('emit("PI_OFFLINE", data.get("pi_offline"))', text)
        self.assertIn('emit("PI_SKIP_VERSION_CHECK", data.get("pi_skip_version_check"))', text)
        self.assertIn('emit("PI_TELEMETRY", "0" if data.get("pi_telemetry_disabled") else "1")', text)

    def test_claim_loop_starts_after_workspace_and_agent_home_setup(self):
        entrypoint = SCRIPT.with_name("harness-agent-entrypoint")
        text = entrypoint.read_text(encoding="utf-8")
        claim_loop = text.index('HARNESS_BRIDGE_MODE:-}" = "claim-loop"')
        cd_workspace = text.index("cd /workspace")
        self.assertLess(text.index(': "${SESSION_WORKSPACE:?SESSION_WORKSPACE is required in control file}"'), claim_loop)
        self.assertLess(text.index(': "${HARNESS_AGENT_HOME:?HARNESS_AGENT_HOME is required in control file}"'), claim_loop)
        self.assertLess(text.index('[ "$SESSION_WORKSPACE" != "/workspace" ]'), claim_loop)
        self.assertLess(text.index('[ "$AGENT_HOME" != "/agent-home" ]'), claim_loop)
        self.assertLess(cd_workspace, claim_loop)
        self.assertLess(text.index("  ensure_agent_user", cd_workspace), claim_loop)
        self.assertLess(text.index("exec_bridge_client()"), claim_loop)
        self.assertNotIn("/sessions/$SESSION_ID", text)
        self.assertNotIn("/agent-homes/$SESSION_ID", text)
        self.assertNotIn("ln -sfn", text)


def argparse_namespace(**kwargs):
    return type("Args", (), kwargs)()


def claude_runner_env():
    return {
        "CLAUDE_SESSION_UUID": "11111111-2222-3333-4444-555555555555",
        "SESSION_ID": "sess",
        "HARNESS_AGENT_HOME": "/agent-home",
        "HARNESS_AGENT_UID": "65534",
        "HARNESS_AGENT_GID": "65534",
        "ANTHROPIC_BASE_URL": "http://harness-model-proxy.internal:8082",
    }


if __name__ == "__main__":
    unittest.main()
