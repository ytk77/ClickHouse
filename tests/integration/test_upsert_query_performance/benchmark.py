#!/usr/bin/env python3
"""Bounded wide-table scan comparison on a fresh isolated local server."""

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--binary", type=Path, required=True)
p.add_argument("--work-dir", type=Path, required=True)
p.add_argument("--log-dir", type=Path, required=True)
p.add_argument("--rows", type=int, default=5_000_000)
p.add_argument("--target-mib", type=int, default=32, choices=(32, 128, 256))
p.add_argument("--prewarm", action="store_true")
p.add_argument(
    "--legacy",
    action="store_true",
    help="Omit the new target setting for the baseline binary",
)
p.add_argument("--replacing", action="store_true")
p.add_argument("--seconds", type=int, default=900)
p.add_argument("--threads", type=int, default=8)
p.add_argument("--overwrite-fraction", type=float, default=0.0)
p.add_argument("--converge-seconds", type=int, default=180)
a = p.parse_args()
if not 0 <= a.overwrite_fraction <= 1:
    p.error("--overwrite-fraction must be between 0 and 1")
work = a.work_dir.resolve()
work.mkdir(parents=True, exist_ok=False)
logs = a.log_dir.resolve(strict=True)
binary = a.binary.resolve(strict=True)
label = work.name
start = time.monotonic()
deadline = start + a.seconds
result = {
    "rows": a.rows,
    "target_mib": a.target_mib,
    "prewarm": a.prewarm,
    "legacy": a.legacy,
    "replacing": a.replacing,
    "overwrite_fraction": a.overwrite_fraction,
    "binary": str(binary),
    "samples": [],
    "passed": False,
}
server = None


def query(sql, query_id=None, timeout=180):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Benchmark deadline reached")
    params = {
        "wait_end_of_query": 1,
        "max_threads": a.threads,
        "max_memory_usage": 4294967296,
        "use_query_cache": 0,
    }
    if query_id:
        params["query_id"] = query_id
    req = urllib.request.Request(
        "http://127.0.0.1:19208/?" + urllib.parse.urlencode(params), data=sql.encode()
    )
    try:
        with urllib.request.urlopen(req, timeout=min(timeout, remaining)) as r:
            return r.read().decode()
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode()) from e


def parts():
    return json.loads(
        query(
            "SELECT count() AS parts,sum(rows) AS rows,max(data_uncompressed_bytes) AS max_part_bytes, "
            "sum(bytes_on_disk) AS disk_bytes FROM system.parts WHERE database='bench' AND table='data' AND active FORMAT JSONEachRow"
        )
    )


config = f"""<clickhouse>
<logger><level>warning</level><log>{logs}/test_upsert_query_{label}_server.log</log><errorlog>{logs}/test_upsert_query_{label}_server.err.log</errorlog></logger>
<path>{work}/data/</path><tmp_path>{work}/tmp/</tmp_path><user_files_path>{work}/user_files/</user_files_path>
<listen_host>127.0.0.1</listen_host><tcp_port>19207</tcp_port><http_port>19208</http_port>
<max_thread_pool_size>10000</max_thread_pool_size><background_pool_size>16</background_pool_size><background_schedule_pool_size>8</background_schedule_pool_size>
<background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size><background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
<merge_tree><number_of_free_entries_in_pool_to_execute_optimize_entire_partition>4</number_of_free_entries_in_pool_to_execute_optimize_entire_partition><number_of_free_entries_in_pool_to_execute_mutation>4</number_of_free_entries_in_pool_to_execute_mutation><number_of_free_entries_in_pool_to_lower_max_size_of_merge>4</number_of_free_entries_in_pool_to_lower_max_size_of_merge></merge_tree>
<profiles><default><max_threads>{a.threads}</max_threads><log_queries>1</log_queries></default></profiles>
<users><default><password/><networks><ip>127.0.0.1</ip></networks><profile>default</profile><quota>default</quota></default></users>
<quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas>
<query_log><database>system</database><table>query_log</table><flush_interval_milliseconds>100</flush_interval_milliseconds></query_log>
</clickhouse>"""
(work / "config.xml").write_text(config)
try:
    for port in (19207, 19208):
        with socket.socket() as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("127.0.0.1", port))
    with (logs / f"test_upsert_query_{label}_process.log").open("w") as log:
        # The finalizer terminates and reaps this process with a bounded shutdown wait.
        server = subprocess.Popen(  # pylint: disable=consider-using-with
            [str(binary), "server", f"--config-file={work}/config.xml"],
            stdout=log,
            stderr=subprocess.STDOUT,
            env={**os.environ, "CLICKHOUSE_WATCHDOG_ENABLE": "0", "TMPDIR": str(work)},
        )
    for _ in range(300):
        if server.poll() is not None:
            raise RuntimeError("Isolated server exited")
        try:
            query("SELECT 1", timeout=1)
            break
        except OSError:
            time.sleep(0.1)
    else:
        raise TimeoutError("Isolated server startup timeout")
    query("CREATE DATABASE bench")
    payload_cols = ",".join(f"p{i} String" for i in range(25))
    engine = "ReplacingMergeTree(v)" if a.replacing else "UpsertMergeTree(v)"
    key = "" if a.replacing else " UNIQUE KEY id"
    extra = (
        ""
        if a.legacy or a.replacing
        else f",upsert_maintenance_target_bytes={a.target_mib<<20}"
    )
    query(
        f"""CREATE TABLE bench.data (id UInt64,v UInt64,AFFILIATE Nullable(String),USER_ID String,{payload_cols})
        ENGINE={engine} ORDER BY id{key} SETTINGS min_bytes_for_wide_part=0,min_rows_for_wide_part=0,
        prewarm_mark_cache={int(a.prewarm)},columns_to_prewarm_mark_cache='AFFILIATE,USER_ID'{extra}"""
    )
    query("SYSTEM STOP MERGES bench.data")

    def insert_rows(rows, version):
        for begin in range(0, rows, 50000):
            n = min(50000, rows - begin)
            payload = ",".join(
                f"substring(hex(sipHash128(number,{i})),1,20)" for i in range(25)
            )
            query(
                f"INSERT INTO bench.data SELECT number,{version},if(number%17=0,NULL,toString(number%1000)),toString(cityHash64(number)),{payload} FROM numbers({begin},{n})"
            )

    t = time.monotonic()
    insert_rows(a.rows, 1)
    result["insert_seconds"] = time.monotonic() - t
    t = time.monotonic()
    insert_rows(int(a.rows * a.overwrite_fraction), 2)
    result["overwrite_seconds"] = time.monotonic() - t
    result["parts_before"] = parts()
    query("SYSTEM START MERGES bench.data")
    t = time.monotonic()
    if a.replacing:
        query("OPTIMIZE TABLE bench.data FINAL", timeout=300)
    else:
        stable = 0
        previous = None
        while time.monotonic() - t < a.converge_seconds:
            current = parts()
            if current == previous:
                stable += 1
            else:
                stable = 0
            if stable >= 5:
                break
            previous = current
            time.sleep(1)
        else:
            raise TimeoutError("Maintenance did not converge within its time budget")
    result["converge_seconds"] = time.monotonic() - t
    query("SYSTEM STOP MERGES bench.data")
    result["parts_after"] = parts()
    result["server_memory_kib"] = {
        line.split(":")[0]: int(line.split()[1])
        for line in Path(f"/proc/{server.pid}/status")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.startswith(("VmRSS:", "VmHWM:"))
    }
    # Request eviction only for this benchmark's immutable files. No global kernel cache drop.
    evicted_bytes = 0
    for file in (work / "data").rglob("*"):
        if file.is_file() and (
            file.suffix in (".bin", ".cmrk2", ".mrk2", ".mrk3", ".cmrk3")
        ):
            with file.open("rb") as f:
                os.fsync(f.fileno())
                os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
            evicted_bytes += file.stat().st_size
    result["file_local_eviction_requested_bytes"] = evicted_bytes
    final = " FINAL" if a.replacing else ""
    sql = f"SELECT AFFILIATE,count(),sum(cityHash64(USER_ID)) FROM bench.data{final} GROUP BY AFFILIATE ORDER BY AFFILIATE FORMAT TSV"
    expected = None
    for i in range(6):
        qid = f"{label}_scan_{i}"
        t = time.monotonic()
        output = query(sql, query_id=qid)
        result["samples"].append(
            {
                "query_id": qid,
                "seconds": time.monotonic() - t,
                "phase": "first_after_file_eviction" if i == 0 else "warm",
            }
        )
        if expected is None:
            expected = output
            (work / "aggregate.tsv").write_text(output)
        assert output == expected
        assert sum(int(line.split("\t")[1]) for line in output.splitlines()) == a.rows
    log_deadline = min(deadline, time.monotonic() + 15)
    while time.monotonic() < log_deadline:
        query("SYSTEM FLUSH LOGS")
        result["query_log"] = [
            json.loads(x)
            for x in query(
                "SELECT query_id,query_duration_ms,read_rows,read_bytes,memory_usage,ProfileEvents "
                f"FROM system.query_log WHERE type='QueryFinish' AND startsWith(query_id,'{label}_scan_') ORDER BY query_id FORMAT JSONEachRow"
            ).splitlines()
        ]
        if len(result["query_log"]) == len(result["samples"]):
            break
        time.sleep(0.1)
    else:
        raise TimeoutError("Query log did not capture every timed sample")
    result["events"] = [
        json.loads(x)
        for x in query(
            "SELECT event,value FROM system.events WHERE startsWith(event,'Upsert') FORMAT JSONEachRow"
        ).splitlines()
    ]
    result["passed"] = True
finally:
    if server and server.poll() is None:
        server.terminate()
        try:
            server.wait(timeout=45)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
    result["elapsed_seconds"] = time.monotonic() - start
    (work / "results.json").write_text(json.dumps(result, indent=2))
    print(
        json.dumps(
            {k: v for k, v in result.items() if k not in ("events", "query_log")},
            indent=2,
        ),
        flush=True,
    )
