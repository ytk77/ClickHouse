#!/usr/bin/env python3
"""Compare bounded batches, client concurrency and ordered `UpsertMergeTree` preparation."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def parse_case(value):
    try:
        case = tuple(int(item) for item in value.split(":"))
    except ValueError as error:
        raise argparse.ArgumentTypeError("Expected batch_rows:clients:insert_threads") from error
    if len(case) != 3 or min(case) < 1:
        raise argparse.ArgumentTypeError("Expected three positive integers")
    return case


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8123")
    parser.add_argument("--rows", type=int, default=1048576)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--case", type=parse_case, action="append")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cases = args.case or [(65536, 1, 1), (65536, 1, 4), (65536, 4, 1), (262144, 1, 1)]
    if args.rows < 1 or args.repeat < 1 or args.payload_bytes < 1:
        parser.error("Rows, repeat and payload bytes must be positive")
    for batch, clients, _ in cases:
        if args.rows % clients:
            parser.error("Rows must be divisible by every client count")
        if batch > 1048576 or batch * (25 + args.payload_bytes) > 32 * 1024 * 1024:
            parser.error("Batch exceeds the row or estimated materialized-byte limit")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    database = "upsert_bench_" + uuid.uuid4().hex

    def query(sql, query_id=None):
        params = {"allow_experimental_unique_key": 1, "async_insert": 0}
        if query_id:
            params["query_id"] = query_id
        url = args.url.rstrip("/") + "/?" + urllib.parse.urlencode(params)
        try:
            with urllib.request.urlopen(url, sql.encode(), timeout=600) as response:
                return response.read().decode().strip()
        except urllib.error.HTTPError as error:
            raise RuntimeError(error.read().decode()) from error

    results = {"server_version": query("SELECT version()"), "database": database,
               "rows": args.rows, "payload_bytes": args.payload_bytes, "results": []}
    query(f"CREATE DATABASE {database}")
    try:
        for repetition in range(args.repeat):
            # Reverse every other repetition to reduce fixed case-order bias.
            for batch, clients, threads in cases[::1 if repetition % 2 == 0 else -1]:
                table = f"{database}.b{batch}_c{clients}_t{threads}_r{repetition}"
                query(f"CREATE TABLE {table} (id UInt64,ver UInt64,value String) "
                      "ENGINE=UpsertMergeTree(ver) ORDER BY id UNIQUE KEY id")
                for version in (1, 2):
                    def insert(client):
                        count = args.rows // clients
                        query_id = f"{table}_v{version}_c{client}"
                        sql = f"""INSERT INTO {table}
                            SELECT number,toUInt64({version}),repeat('x',{args.payload_bytes})
                            FROM numbers({client * count},{count})
                            SETTINGS max_threads={max(4, threads)},max_insert_threads={threads},
                            max_block_size={batch},max_insert_block_size={batch},
                            min_insert_block_size_rows={batch},min_insert_block_size_bytes=0"""
                        query(sql, query_id)
                    start = time.perf_counter()
                    with ThreadPoolExecutor(max_workers=clients) as pool:
                        list(pool.map(insert, range(clients)))
                    seconds = time.perf_counter() - start
                    actual = query(f"SELECT count(),uniqExact(id),min(ver),max(ver),sum(id),"
                                   f"countIf(value!=repeat('x',{args.payload_bytes})) FROM {table}")
                    expected = (f"{args.rows}\t{args.rows}\t{version}\t{version}\t"
                                f"{args.rows * (args.rows - 1) // 2}\t0")
                    if actual != expected:
                        raise AssertionError({"table": table, "actual": actual, "expected": expected})
                    item = {"batch_rows": batch, "clients": clients, "insert_threads": threads,
                            "repetition": repetition, "version": version, "seconds": seconds,
                            "rows_per_second": args.rows / seconds, "verified": True}
                    results["results"].append(item)
                    args.output.write_text(json.dumps(results, indent=2) + "\n")
                    print(json.dumps(item), flush=True)
                query(f"DROP TABLE {table} SYNC")
    finally:
        query(f"DROP DATABASE {database} SYNC")
    print("PASS all cases and exact visible-row checks", flush=True)


if __name__ == "__main__":
    main()
