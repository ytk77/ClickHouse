# `UpsertMergeTree` insert benchmark

Compare batch sizes, independent clients and ordered part preparation against a local test server:

```bash
python3 utils/upsert-benchmark/run.py \
    --url http://127.0.0.1:8123 --repeat 3 \
    --output tmp/upsert-benchmark/results.json
```

The default cases are `65536:1:1`, `65536:1:4`, `65536:4:1` and `262144:1:1`.
Use repeated `--case batch_rows:clients:insert_threads` arguments to select cases.
Each client sends one multi-block `INSERT SELECT`, so the single-client parallel case
exercises preparation across blocks rather than just creating extra threads for one part.
The query's `max_threads` is at least four; `max_insert_threads` selects the preparation
concurrency. Both settings and the minimum-free-memory settings can reduce concurrency.

Each repetition creates a fresh table, ingests new keys, then overwrites every key.
The timer includes source generation, HTTP dispatch and synchronous commit, and excludes
verification and table creation/removal. Every phase verifies exact row and unique-key
counts, versions, key sum and payload values using ordinary `SELECT` queries. The script
creates a uniquely named database and removes only that database when finished, including
on failure. Results are saved after every verified phase.

Use the same script, server configuration, hardware and payload for before/after comparisons.
Record both binary checksums separately. Cases run in reverse order on alternate repetitions.
Keep background maintenance enabled and assess longer runs separately; these short tests use
sequential keys and compressible fixed-width payloads and do not represent every production
update pattern. No cache clearing or source-network ingestion is included. For wider rows, set `--payload-bytes` to match the workload and reduce the batch size
to respect the 32-MiB input-block limit.

For real workloads, measure input rows/s separately from changes in visible row count.
Test 16K–256K row batches and two to four clients before increasing concurrency further.
Preserve the routing of each unique key to its SQL partition. New tables use `disk_state_v2`;
upgrading a binary does not convert existing `parts_sync_v1` tables.
