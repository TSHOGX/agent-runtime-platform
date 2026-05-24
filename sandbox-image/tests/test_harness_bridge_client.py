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


class EntrypointStaticTest(unittest.TestCase):
    def test_entrypoint_has_probe_mode(self):
        entrypoint = SCRIPT.with_name("harness-agent-entrypoint")
        text = entrypoint.read_text(encoding="utf-8")
        self.assertIn('HARNESS_BRIDGE_MODE:-}" = "probe"', text)
        self.assertIn("exec /usr/local/bin/harness-bridge-client probe", text)
        self.assertIn('HARNESS_BRIDGE_MODE:-auto}" = "auto"', text)
        self.assertIn("/usr/local/bin/harness-bridge-client probe", text)
        self.assertIn("/usr/local/bin/harness-bridge-client heartbeat-loop &", text)


def argparse_namespace(**kwargs):
    return type("Args", (), kwargs)()


if __name__ == "__main__":
    unittest.main()
