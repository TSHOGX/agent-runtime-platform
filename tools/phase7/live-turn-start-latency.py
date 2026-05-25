#!/usr/bin/env python3
import argparse
import concurrent.futures
import http.cookiejar
import json
import math
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def env_default(name, fallback):
    value = os.environ.get(name)
    return fallback if value is None or value == "" else value


def parse_args():
    parser = argparse.ArgumentParser(
        description="Measure Phase 7 live POST-to-ack_turn_started latency for prewarmed bridge sessions."
    )
    parser.add_argument("--url", default=env_default("PHASE7_ORCHESTRATOR_URL", "http://127.0.0.1:8090"))
    parser.add_argument("--db", default=env_default("PHASE7_DB", "/var/lib/harness/sessions/orchestrator.db"))
    parser.add_argument(
        "--session-ids",
        default=env_default("PHASE7_LATENCY_SESSION_IDS", env_default("PHASE7_LATENCY_SESSION_ID", "")),
        help="Comma-separated prewarmed session ids. Env: PHASE7_LATENCY_SESSION_IDS.",
    )
    parser.add_argument("--budget-ms", type=float, default=float(env_default("PHASE7_LATENCY_BUDGET_MS", "50")))
    parser.add_argument("--timeout-s", type=float, default=float(env_default("PHASE7_LATENCY_TIMEOUT_S", "5")))
    parser.add_argument("--poll-ms", type=float, default=float(env_default("PHASE7_LATENCY_POLL_MS", "5")))
    parser.add_argument(
        "--content-template",
        default=env_default("PHASE7_LATENCY_CONTENT", "phase7 latency probe {nonce}"),
        help="Message content. {session_id} and {nonce} are replaced per sample.",
    )
    parser.add_argument(
        "--shared-secret",
        default=os.environ.get("PHASE7_SHARED_SECRET", ""),
        help="Optional login password when orchestrator auth is enabled.",
    )
    parser.add_argument(
        "--cookie",
        default=os.environ.get("PHASE7_AUTH_COOKIE", ""),
        help="Optional raw Cookie header when orchestrator auth is enabled.",
    )
    return parser.parse_args()


def http_json(opener, method, url, payload=None, cookie=""):
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if cookie:
        headers["Cookie"] = cookie
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with opener.open(req, timeout=10) as response:
            body = response.read()
            return response.status, json.loads(body.decode("utf-8") or "{}")
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = {"error": body}
        return err.code, parsed


def login_if_needed(opener, base_url, shared_secret, cookie):
    if cookie or not shared_secret:
        return
    status, body = http_json(opener, "POST", urllib.parse.urljoin(base_url + "/", "login"), {"password": shared_secret})
    if status != 200:
        raise RuntimeError(f"login failed: status={status} body={body}")


def cookie_header_from_jar(jar):
    return "; ".join(f"{cookie.name}={cookie.value}" for cookie in jar)


def open_db(path):
    uri = "file:" + urllib.parse.quote(path) + "?mode=ro"
    return sqlite3.connect(uri, uri=True, timeout=1)


def require_idle_session(conn, session_id):
    row = conn.execute(
        "SELECT status, COALESCE(active_generation_id, '') FROM sessions WHERE id = ?",
        (session_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"session {session_id} not found in DB")
    status, active_generation_id = row
    if status != "running_idle" or active_generation_id == "":
        raise RuntimeError(
            f"session {session_id} must be running_idle with an active generation, "
            f"got status={status!r} active_generation_id={active_generation_id!r}"
        )


def wait_for_turn(conn, session_id, content, deadline, poll_s):
    while time.monotonic() < deadline:
        row = conn.execute(
            "SELECT id FROM turns WHERE session_id = ? AND content = ? ORDER BY id DESC LIMIT 1",
            (session_id, content),
        ).fetchone()
        if row is not None:
            return int(row[0])
        time.sleep(poll_s)
    raise TimeoutError(f"timed out waiting for turn row for session {session_id}")


def wait_for_ack(conn, turn_id, deadline, poll_s):
    while time.monotonic() < deadline:
        row = conn.execute(
            "SELECT event_id FROM events WHERE turn_id = ? AND type = 'ack_turn_started' ORDER BY event_id DESC LIMIT 1",
            (turn_id,),
        ).fetchone()
        if row is not None:
            return int(row[0])
        time.sleep(poll_s)
    raise TimeoutError(f"timed out waiting for ack_turn_started event for turn {turn_id}")


def percentile(values, pct):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = max(0, math.ceil((pct / 100.0) * len(ordered)) - 1)
    return ordered[min(index, len(ordered) - 1)]


def measure_session(args, base_url, session_id, cookie_header):
    conn = open_db(args.db)
    try:
        nonce = uuid.uuid4().hex
        content = args.content_template.replace("{session_id}", session_id).replace("{nonce}", nonce)
        deadline = time.monotonic() + args.timeout_s
        opener = urllib.request.build_opener()
        start = time.monotonic()
        status, body = http_json(
            opener,
            "POST",
            f"{base_url}/api/sessions/{urllib.parse.quote(session_id)}/messages",
            {"content": content},
            cookie=cookie_header,
        )
        if status != 202:
            raise RuntimeError(f"POST message failed for {session_id}: status={status} body={body}")
        turn_id = wait_for_turn(conn, session_id, content, deadline, args.poll_ms / 1000.0)
        event_id = wait_for_ack(conn, turn_id, deadline, args.poll_ms / 1000.0)
        elapsed_ms = (time.monotonic() - start) * 1000.0
        return {
            "session_id": session_id,
            "turn_id": turn_id,
            "ack_event_id": event_id,
            "latency_ms": elapsed_ms,
        }
    finally:
        conn.close()


def main():
    args = parse_args()
    session_ids = [part.strip() for part in args.session_ids.split(",") if part.strip()]
    if not session_ids:
        raise SystemExit("provide --session-ids or PHASE7_LATENCY_SESSION_IDS")
    base_url = args.url.rstrip("/")

    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    login_if_needed(opener, base_url, args.shared_secret, args.cookie)
    cookie_header = args.cookie or cookie_header_from_jar(jar)

    conn = open_db(args.db)
    try:
        for session_id in session_ids:
            require_idle_session(conn, session_id)
    finally:
        conn.close()

    samples = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(session_ids)) as executor:
        futures = [
            executor.submit(measure_session, args, base_url, session_id, cookie_header)
            for session_id in session_ids
        ]
        for future in concurrent.futures.as_completed(futures):
            samples.append(future.result())
    samples.sort(key=lambda sample: sample["session_id"])

    latencies = [sample["latency_ms"] for sample in samples]
    summary = {
        "budget_ms": args.budget_ms,
        "concurrent_sessions": len(session_ids),
        "samples": samples,
        "p50_ms": percentile(latencies, 50),
        "p95_ms": percentile(latencies, 95),
        "p99_ms": percentile(latencies, 99),
        "max_ms": max(latencies),
    }
    print(json.dumps(summary, indent=2))
    if summary["max_ms"] > args.budget_ms:
        raise SystemExit(f"max latency {summary['max_ms']:.3f}ms exceeded budget {args.budget_ms:.3f}ms")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"phase7 latency gate failed: {err}", file=sys.stderr)
        raise SystemExit(1)
