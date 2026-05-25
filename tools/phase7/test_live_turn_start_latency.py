#!/usr/bin/env python3
import json
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


SCRIPT = Path(__file__).with_name("live-turn-start-latency.py")


class LiveLatencyToolTest(unittest.TestCase):
    def test_posts_to_sessions_concurrently_and_reports_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "orchestrator.db"
            init_db(db_path)
            lock = threading.Lock()
            post_starts = []

            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *_args):
                    return

                def do_POST(self):
                    parsed = urllib.parse.urlparse(self.path)
                    parts = parsed.path.strip("/").split("/")
                    if len(parts) != 4 or parts[:2] != ["api", "sessions"] or parts[3] != "messages":
                        self.send_json(404, {"error": "not found"})
                        return
                    session_id = urllib.parse.unquote(parts[2])
                    length = int(self.headers.get("Content-Length", "0"))
                    body = json.loads(self.rfile.read(length).decode("utf-8"))
                    with lock:
                        post_starts.append(time.monotonic())
                    time.sleep(0.2)
                    turn_id = insert_ack_started_event(db_path, session_id, body["content"])
                    self.send_json(202, {"message": {"id": turn_id}})

                def send_json(self, status, payload):
                    data = json.dumps(payload).encode("utf-8")
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)

            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(lambda: (server.shutdown(), server.server_close()))

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--url",
                    f"http://127.0.0.1:{server.server_port}",
                    "--db",
                    str(db_path),
                    "--session-ids",
                    "sess_a,sess_b",
                    "--budget-ms",
                    "1000",
                    "--poll-ms",
                    "1",
                ],
                check=False,
                text=True,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            summary = json.loads(result.stdout)
            self.assertEqual(summary["concurrent_sessions"], 2)
            self.assertEqual([sample["session_id"] for sample in summary["samples"]], ["sess_a", "sess_b"])
            self.assertLess(summary["max_ms"], 1000)
            self.assertEqual(len(post_starts), 2)
            self.assertLess(max(post_starts) - min(post_starts), 0.15)


def init_db(path):
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
CREATE TABLE sessions (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  active_generation_id TEXT
);
CREATE TABLE turns (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  content TEXT NOT NULL
);
CREATE TABLE events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  turn_id INTEGER NOT NULL,
  type TEXT NOT NULL
);
INSERT INTO sessions (id, status, active_generation_id)
VALUES
  ('sess_a', 'running_idle', 'gen_a'),
  ('sess_b', 'running_idle', 'gen_b');
"""
        )
        conn.commit()
    finally:
        conn.close()


def insert_ack_started_event(path, session_id, content):
    conn = sqlite3.connect(path)
    try:
        cursor = conn.execute(
            "INSERT INTO turns (session_id, content) VALUES (?, ?)",
            (session_id, content),
        )
        turn_id = cursor.lastrowid
        conn.execute(
            "INSERT INTO events (turn_id, type) VALUES (?, 'ack_turn_started')",
            (turn_id,),
        )
        conn.commit()
        return turn_id
    finally:
        conn.close()


if __name__ == "__main__":
    unittest.main()
