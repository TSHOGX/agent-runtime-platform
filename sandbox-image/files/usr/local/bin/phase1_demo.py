#!/usr/bin/env python3
import csv
import os
import traceback
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pymysql


workspace = Path("/workspace")
workspace.mkdir(parents=True, exist_ok=True)
report = []


def add(line):
    print(line, flush=True)
    report.append(line)


host = os.environ.get("DORIS_HOST", "172.16.0.138")
port = int(os.environ.get("DORIS_PORT", "9030"))
user = os.environ.get("DORIS_USER", "ro_user_batt")
password = os.environ.get("DORIS_PASSWORD", "")
database = os.environ.get("DORIS_DATABASE", "vhr_data")

add("# Phase 2 Doris Demo")
add("")
add(f"- workspace: {workspace}")
add(f"- database: {database}")

try:
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        connect_timeout=10,
        read_timeout=60,
        cursorclass=pymysql.cursors.DictCursor,
    )
    add("- connection: ok")
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        tables = cur.fetchall()
    table_key = next(iter(tables[0].keys())) if tables else "table"
    with (workspace / "tables.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[table_key])
        writer.writeheader()
        writer.writerows(tables)
    add(f"- SHOW TABLES: {len(tables)} tables, wrote tables.csv")

    query = """
SELECT project_name, COUNT(*) AS sessions, AVG(during_time) AS avg_during_time
FROM dwd_gongkuang_charge_res_di
GROUP BY project_name
ORDER BY sessions DESC
LIMIT 10
"""
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        with (workspace / "charge_by_project.csv").open("w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["project_name", "sessions", "avg_during_time"]
            )
            writer.writeheader()
            writer.writerows(rows)
        add(f"- aggregate query: ok, wrote charge_by_project.csv with {len(rows)} rows")
        labels = [str(r["project_name"]) for r in rows]
        values = [float(r["sessions"] or 0) for r in rows]
        plt.figure(figsize=(10, 5))
        plt.bar(labels, values)
        plt.xticks(rotation=30, ha="right")
        plt.ylabel("sessions")
        plt.title("Top charge sessions by project")
        plt.tight_layout()
        plt.savefig(workspace / "charge_by_project.png", dpi=150)
        add("- chart: wrote charge_by_project.png")
    except Exception as exc:
        add(f"- aggregate query: blocked: {exc!r}")
        with (workspace / "charge_query_error.txt").open("w") as f:
            f.write(query.strip() + "\n\n")
            traceback.print_exc(file=f)
        names = [str(row[table_key]) for row in tables]
        counts = [1] * len(names)
        plt.figure(figsize=(8, 4))
        plt.bar(names, counts)
        plt.xticks(rotation=20, ha="right")
        plt.ylabel("visible table")
        plt.title("Doris metadata visible from sandbox")
        plt.tight_layout()
        plt.savefig(workspace / "tables.png", dpi=150)
        add("- fallback chart: wrote tables.png from metadata")
    finally:
        conn.close()
except Exception as exc:
    add(f"- connection: failed: {exc!r}")
    with (workspace / "connection_error.txt").open("w") as f:
        traceback.print_exc(file=f)

(workspace / "report.md").write_text("\n".join(report) + "\n")
