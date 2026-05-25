import importlib.util
from importlib.machinery import SourceFileLoader
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "files" / "usr" / "local" / "bin" / "harness-bridge-client"
spec = importlib.util.spec_from_loader("harness_bridge_client", SourceFileLoader("harness_bridge_client", str(SCRIPT)))
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)

MANIFEST_LIB = SCRIPT.parents[1] / "lib" / "harness_manifest.py"
manifest_spec = importlib.util.spec_from_loader("harness_manifest", SourceFileLoader("harness_manifest", str(MANIFEST_LIB)))
manifest = importlib.util.module_from_spec(manifest_spec)
manifest_spec.loader.exec_module(manifest)


class BridgeClientTest(unittest.TestCase):
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
                    "payload": {"turn_id": 9, "sequence": 1, "content": "run"},
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
                    "payload": {"turn_id": 9, "sequence": 1, "content": "resume", "replayed": True},
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
                message_statuses="400",
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
                        payload = {"turn_id": 9, "sequence": 1, "content": "echo ok"}
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
                message_statuses="400",
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
                        payload = {"turn_id": 9, "sequence": 1, "content": "resume"}
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

    def test_network_probe_checks_health_and_message_statuses(self):
        statuses = iter([200, 400])
        with mock.patch.object(bridge, "http_status", side_effect=lambda *args, **kwargs: next(statuses)) as http_status:
            bridge.run_network_probe(
                "http://10.240.0.1:8082",
                {200},
                {400},
                0.1,
                "test-key",
                "test-token",
            )

        self.assertEqual(http_status.call_count, 2)
        message_call = http_status.call_args_list[1]
        self.assertEqual(message_call.kwargs["method"], "POST")
        self.assertEqual(message_call.kwargs["headers"]["x-api-key"], "test-key")
        self.assertEqual(message_call.kwargs["headers"]["authorization"], "Bearer test-token")

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
                message_statuses="400",
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

    def test_configured_secret_reads_materialized_secret(self):
        with tempfile.TemporaryDirectory() as root:
            secret_path = Path(root) / "api-key" / "v1"
            secret_path.parent.mkdir(parents=True)
            secret_path.write_text("test-key\n", encoding="utf-8")
            with mock.patch.dict(
                os.environ,
                {
                    "SECRET_MOUNT_PATH": root,
                    "ANTHROPIC_API_KEY_SECRET_ID": "api-key",
                    "SECRET_VERSION": "v1",
                },
                clear=False,
            ):
                self.assertEqual(
                    bridge.configured_secret("ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY_SECRET_ID"),
                    "test-key",
                )

    def test_claude_runner_drops_privileges_before_reading_secrets(self):
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
                "HARNESS_AGENT_HOME": "/agent-homes/sess",
                "HARNESS_AGENT_UID": "65534",
                "HARNESS_AGENT_GID": "65534",
                "HARNESS_SECRET_READERS_GID": "12345",
                "ANTHROPIC_BASE_URL": "http://10.240.0.1:8082",
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
        self.assertEqual(command[:7], ["setpriv", "--reuid", "65534", "--regid", "65534", "--groups", "12345"])
        self.assertIn("SECRET_MOUNT_PATH=/harness-secrets", command)
        shell_script = command[command.index("-c") + 1]
        self.assertIn('ANTHROPIC_API_KEY="$(cat "$SECRET_MOUNT_PATH/$ANTHROPIC_API_KEY_SECRET_ID/$SECRET_VERSION")"', shell_script)
        self.assertIn("/usr/local/bin/claude", command)
        self.assertNotIn("ANTHROPIC_API_KEY", captured["env"])
        self.assertIn('"text":"hello"', captured["process"].stdin.writes[0])


class EntrypointStaticTest(unittest.TestCase):
    def test_manifest_digest_matches_host_fixture(self):
        fixture = Path(__file__).resolve().parents[2] / "docs" / "phase7" / "fixtures" / "control-manifest-payload.json"
        payload = json.loads(fixture.read_text(encoding="utf-8"))
        digest = "9458fdd58b3315147cf8321bd4ba3fa130a6c880aee2daa108342400eac440e4"

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
        self.assertIn("exec /usr/local/bin/harness-bridge-client claim-loop", text)
        self.assertIn('HARNESS_BRIDGE_MODE:-auto}" = "auto"', text)
        self.assertIn("/usr/local/bin/harness-bridge-client probe", text)
        self.assertIn("/usr/local/bin/harness-bridge-client heartbeat-loop &", text)

    def test_claim_loop_starts_after_workspace_and_agent_home_setup(self):
        entrypoint = SCRIPT.with_name("harness-agent-entrypoint")
        text = entrypoint.read_text(encoding="utf-8")
        claim_loop = text.index('HARNESS_BRIDGE_MODE:-}" = "claim-loop"')
        cd_workspace = text.index("cd /workspace")
        self.assertLess(text.index('SESSION_WORKSPACE="${SESSION_WORKSPACE:-/sessions/$SESSION_ID}"'), claim_loop)
        self.assertLess(text.index("ln -sfn \"$SESSION_WORKSPACE\" /workspace"), claim_loop)
        self.assertLess(cd_workspace, claim_loop)
        self.assertLess(text.index("  ensure_agent_user", cd_workspace), claim_loop)


def argparse_namespace(**kwargs):
    return type("Args", (), kwargs)()


if __name__ == "__main__":
    unittest.main()
