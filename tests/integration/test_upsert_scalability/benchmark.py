#!/usr/bin/env python3
"""Run a bounded benchmark against an isolated local ClickHouse server."""

import argparse
from bisect import bisect_left, bisect_right
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from xml.sax.saxutils import escape


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--log-dir", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--seconds", type=int, default=600)
    parser.add_argument("--http-port", type=int, default=19198)
    parser.add_argument("--tcp-port", type=int, default=19197)
    parser.add_argument("--restart", action="store_true", help="Restart and compare row count and version sum")
    parser.add_argument("--update-pattern", choices=("uniform", "hot"), default="uniform")
    parser.add_argument("--read-after-updates", action="store_true")
    parser.add_argument("--pause-maintenance", action="store_true", help="Requires failpoints; keep the same part count in both binaries")
    args = parser.parse_args()
    if min(args.rows, args.partitions, args.seconds) < 1:
        parser.error("Rows, partitions and seconds must be positive")
    binary = args.binary.resolve(strict=True)
    work = args.work_dir.resolve()
    logs = args.log_dir.resolve(strict=True)
    work.mkdir(parents=True, exist_ok=False)
    label = work.name
    started = time.monotonic()
    deadline = started + args.seconds
    results = {
        "binary": str(binary),
        "rows": args.rows,
        "partitions": args.partitions,
        "update_pattern": args.update_pattern,
        "maintenance_paused": args.pause_maintenance,
        "measurements": [],
        "passed": False,
    }
    server = None

    def query(sql, query_id=None, timeout=180):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Benchmark deadline reached")
        params = {"wait_end_of_query": 1, "max_partitions_per_insert_block": 8192}
        if query_id:
            params["query_id"] = query_id
        request = urllib.request.Request(
            f"http://127.0.0.1:{args.http_port}/?{urllib.parse.urlencode(params)}",
            data=sql.encode(),
        )
        try:
            with urllib.request.urlopen(request, timeout=min(timeout, remaining)) as response:
                return response.read().decode()
        except urllib.error.HTTPError as error:
            raise RuntimeError(error.read().decode()) from error

    def timed(name, sql):
        query_id = f"upsert_scale_{label}_{len(results['measurements'])}"
        begin = time.monotonic()
        value = query(sql, query_id)
        measurement = {
            "name": name,
            "query_id": query_id,
            "seconds": time.monotonic() - begin,
            "result": value,
        }
        results["measurements"].append(measurement)
        print(json.dumps(measurement), flush=True)
        return value

    config = work / "config.xml"
    config.write_text(f"""<clickhouse>
  <logger><level>warning</level><log>{escape(str(logs / (label + '.server.log')))}</log>
    <errorlog>{escape(str(logs / (label + '.server.err.log')))}</errorlog></logger>
  <path>{escape(str(work / 'data'))}/</path>
  <tmp_path>{escape(str(work / 'tmp'))}/</tmp_path>
  <user_files_path>{escape(str(work / 'user_files'))}/</user_files_path>
  <listen_host>127.0.0.1</listen_host><tcp_port>{args.tcp_port}</tcp_port><http_port>{args.http_port}</http_port>
  <max_thread_pool_size>10000</max_thread_pool_size><background_pool_size>16</background_pool_size>
  <background_schedule_pool_size>8</background_schedule_pool_size>
  <background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size>
  <background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
  <merge_tree>
    <number_of_free_entries_in_pool_to_execute_optimize_entire_partition>4</number_of_free_entries_in_pool_to_execute_optimize_entire_partition>
    <number_of_free_entries_in_pool_to_execute_mutation>4</number_of_free_entries_in_pool_to_execute_mutation>
    <number_of_free_entries_in_pool_to_lower_max_size_of_merge>4</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
  </merge_tree>
  <profiles><default><max_threads>4</max_threads><log_queries>1</log_queries></default></profiles>
  <users><default><password/><networks><ip>127.0.0.1</ip></networks><profile>default</profile>
    <quota>default</quota><access_management>1</access_management></default></users>
  <quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors>
    <result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas>
  <query_log><database>system</database><table>query_log</table><flush_interval_milliseconds>1000</flush_interval_milliseconds></query_log>
</clickhouse>
""")

    def start_server():
        nonlocal server
        with (logs / f"{label}.process.log").open("a") as process_log:
            server = subprocess.Popen(
                [str(binary), "server", f"--config-file={config}"],
                stdout=process_log,
                stderr=subprocess.STDOUT,
                env={**os.environ, "CLICKHOUSE_WATCHDOG_ENABLE": "0", "TMPDIR": str(work)},
            )
        startup_deadline = min(deadline, time.monotonic() + 60)
        while time.monotonic() < startup_deadline:
            if server.poll() is not None:
                raise RuntimeError("Isolated server exited during startup")
            try:
                query("SELECT 1", timeout=1)
                break
            except (OSError, RuntimeError):
                time.sleep(0.1)
        else:
            raise TimeoutError("Isolated server startup timed out")

    def stop_server():
        if server and server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=30)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()

    try:
        for port in (args.http_port, args.tcp_port):
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(("127.0.0.1", port))
        start_server()
        query("CREATE DATABASE scale")
        query(f"""CREATE TABLE scale.data (id UInt64, v UInt64, payload String)
            ENGINE=UpsertMergeTree(v) PARTITION BY id % {args.partitions}
            ORDER BY id UNIQUE KEY id""")
        if args.pause_maintenance:
            query("SYSTEM ENABLE FAILPOINT upsert_pause_before_merge_commit")
        for start in range(0, args.rows, 100_000):
            count = min(100_000, args.rows - start)
            timed("insert", f"INSERT INTO scale.data SELECT number+{start},1,repeat('x',100) FROM numbers({count})")
        assert timed("count", "SELECT count() FROM scale.data").strip() == str(args.rows)
        for i in range(30):
            assert timed("point", f"SELECT v FROM scale.data WHERE id={i * 7919 % args.rows}").strip() == "1"
            start = i * 7919 % args.rows
            expected = min(10_001, args.rows - start)
            assert timed("range", f"SELECT sum(v) FROM scale.data WHERE id BETWEEN {start} AND {start + 10000}").strip() == str(expected)
        update_domain = args.rows if args.update_pattern == "uniform" else min(args.rows, 1000)
        for i in range(10):
            timed("overwrite", f"INSERT INTO scale.data SELECT (number*7919+{i})%{update_domain},2,repeat('y',100) FROM numbers(10000)")
        assert timed("count_after_overwrite", "SELECT count() FROM scale.data").strip() == str(args.rows)
        if args.read_after_updates:
            changed = {(number * 7919 + batch) % update_domain for batch in range(10) for number in range(10_000)}
            ordered = sorted(changed)
            for i in range(30):
                start = i * 7919 % args.rows
                expected = 2 if start in changed else 1
                assert timed("point_after_updates", f"SELECT v FROM scale.data WHERE id={start}").strip() == str(expected)
                expected = min(10_001, args.rows - start) + bisect_right(ordered, start + 10000) - bisect_left(ordered, start)
                assert timed("range_after_updates", f"SELECT sum(v) FROM scale.data WHERE id BETWEEN {start} AND {start + 10000}").strip() == str(expected)
        timed("parts", "SELECT count(),sum(rows),sum(bytes_on_disk) FROM system.parts WHERE database='scale' AND active")
        results["profile"] = query("SELECT event,value FROM system.events WHERE startsWith(event,'Upsert') FORMAT JSONEachRow")
        query("SYSTEM FLUSH LOGS")
        results["query_metrics"] = query("""SELECT query_id,query_duration_ms,read_rows,read_bytes,memory_usage,ProfileEvents
            FROM system.query_log WHERE type='QueryFinish' AND startsWith(query_id,'upsert_scale_') FORMAT JSONEachRow""")
        if args.pause_maintenance:
            query("SYSTEM NOTIFY FAILPOINT upsert_pause_before_merge_commit")
            query("SYSTEM DISABLE FAILPOINT upsert_pause_before_merge_commit")
        if args.restart:
            expected = query("SELECT count(),sum(v) FROM scale.data")
            stop_server()
            begin = time.monotonic()
            start_server()
            actual = query("SELECT count(),sum(v) FROM scale.data")
            assert actual == expected
            results["recovery_seconds"] = time.monotonic() - begin
            results["recovery_result"] = actual
            results["recovery_profile"] = query("SELECT event,value FROM system.events WHERE startsWith(event,'UpsertRecovery') FORMAT JSONEachRow")
            print(json.dumps({"recovery_seconds": results["recovery_seconds"], "result": actual}), flush=True)
        results["passed"] = True
        print("PASS benchmark", flush=True)
    finally:
        stop_server()
        results["elapsed_seconds"] = time.monotonic() - started
        (work / "results.json").write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
