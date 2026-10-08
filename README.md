<div align=center>

[![Website](https://img.shields.io/website?up_message=AVAILABLE&down_message=DOWN&url=https%3A%2F%2Fclickhouse.com&style=for-the-badge)](https://clickhouse.com)
[![Apache 2.0 License](https://img.shields.io/badge/license-Apache%202.0-blueviolet?style=for-the-badge)](https://www.apache.org/licenses/LICENSE-2.0)

<picture align=center>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/ClickHouse/clickhouse-docs/assets/9611008/4ef9c104-2d3f-4646-b186-507358d2fe28">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/ClickHouse/clickhouse-docs/assets/9611008/b001dc7b-5a45-4dcd-9275-e03beb7f9177">
    <img alt="The ClickHouse company logo." src="https://github.com/ClickHouse/clickhouse-docs/assets/9611008/b001dc7b-5a45-4dcd-9275-e03beb7f9177">
</picture>

<h4>ClickHouse® is an open-source column-oriented database management system that allows generating analytical data reports in real-time.</h4>

</div>

## Custom engines in this fork

This fork adds `UpsertMergeTree` and `OverwriteCache`. Install a build from
[this fork's releases](https://github.com/ytk77/ClickHouse/releases) and follow the
release's `INSTALL.md` for package installation or upgrades. The official installer
in the section below installs upstream ClickHouse; these custom engines require this fork's binary.

| Engine | Storage and query access | Typical use |
| --- | --- | --- |
| `UpsertMergeTree` | Persistent column parts with a winner index on disk; supports analytical scans and aggregations. | Large datasets of current rows with synchronous upserts. |
| `OverwriteCache` | Rows and indexes in RAM, with configurable disk persistence; requires indexed lookups. | Bounded caches of current rows queried by key or declared lookup indexes. |

### Create an `UpsertMergeTree` table

Run the following SQL in your chosen database:

```sql
CREATE TABLE player_state
(
    tenant_id UInt64,
    player_id UInt64,
    ver UInt64,
    status String
)
ENGINE = UpsertMergeTree(ver)
PARTITION BY tenant_id
ORDER BY (tenant_id, player_id)
UNIQUE KEY (tenant_id, player_id)
SETTINGS
    upsert_insert_target_bytes = 536870912,
    upsert_maintenance_target_bytes = 4294967296;

INSERT INTO player_state VALUES (1, 42, 1, 'pending');
INSERT INTO player_state VALUES (1, 42, 2, 'active');
INSERT INTO player_state VALUES (1, 42, 1, 'stale');

SELECT tenant_id, player_id, ver, status
FROM player_state
ORDER BY tenant_id, player_id;
```

The result contains one row: `(1, 42, 2, 'active')`. Normal `SELECT` reads the current
winner without `FINAL`, including before background parts finish merging.

- `UNIQUE KEY` is required. `allow_experimental_unique_key` is not required for this engine.
  Use stored, non-null integer, `String`, `FixedString`, or supported tuple key columns.
- Uniqueness is **per partition**. Keep every version of a logical key in the same partition;
  partitioning by a changing update timestamp can leave that key visible in multiple partitions.
- The optional version argument accepts a stored, non-null `UInt64`, `DateTime`, or `DateTime64`
  column. Higher versions win; equal versions use later commit order, then input row order.
  With `ENGINE = UpsertMergeTree`, winners use commit order without a version column.
- Replace a row by inserting all its columns with a new version. Partial-column updates
  are unsupported. Inserts commit synchronously; a statement split into multiple units
  is not atomic across all units.
- The example pins the current defaults: a **512 MiB** uncompressed insert unit target
  and a **4 GiB** uncompressed background rewrite target. These are not guaranteed part sizes:
  input blocks, partitioning, duplicate folding, and row/lineage/state limits can make parts smaller.
  Background maintenance reduces its effective target when server or background memory limits
  require it. Larger insert units require additional preparation memory.

New tables require local disk storage. For supported operations, retry and upgrade procedures,
and the current `BACKUP`, `RESTORE`, and `CHECK TABLE` restrictions, see the
[`UpsertMergeTree` guide](docs/development/upsert-merge-tree.mdx).

### Create an `OverwriteCache` table

```sql
CREATE TABLE player_cache
(
    tenant_id UInt64,
    player_id UInt64,
    ver UInt64,
    source_sequence UInt64,
    status String
)
ENGINE = OverwriteCache(ver)
KEYS (tenant_id, player_id)
INDEX (tenant_id)
SETTINGS
    max_memory_bytes = 1073741824,
    equal_version_tiebreak_columns = 'source_sequence',
    persist_mode = 'sync';

INSERT INTO player_cache VALUES (1, 42, 1, 1, 'pending');
INSERT INTO player_cache VALUES (1, 42, 2, 1, 'active');
INSERT INTO player_cache VALUES (1, 42, 2, 2, 'verified');
INSERT INTO player_cache VALUES (1, 42, 1, 9, 'stale');

SELECT player_id, ver, source_sequence, status
FROM player_cache
WHERE tenant_id = 1 AND player_id = 42;

SELECT player_id, status
FROM player_cache
WHERE tenant_id = 1;
```

The key lookup returns `(42, 2, 2, 'verified')`. The second query uses the declared
`INDEX (tenant_id)` to retrieve that tenant's rows.

- Exactly one version-column argument and a nonempty `KEYS (...)` clause are required.
  The version column must not be part of `KEYS`.
- Higher versions win. Equal versions compare `equal_version_tiebreak_columns` in declaration
  order; a larger tie-break value wins. A full tie preserves the stored row. Choose tie-break
  columns that determine the winner reproducibly; without them, equal versions keep the existing row.
- Every `INDEX (...)` column must belong to `KEYS`. Reads require equality or `IN` predicates
  covering the full key or declared lookup indexes. Unrestricted scans such as
  `SELECT * FROM player_cache` are rejected.
- `max_memory_bytes` in this example limits engine-accounted memory to **1 GiB**;
  query and server limits still apply, and actual resident memory can be higher.
  The engine rejects writes that exceed its limit and does not evict rows automatically.
- `persist_mode = 'sync'` waits for durability before insert success. The default `'async'`
  persists in the background and can lose an unflushed tail after an abrupt stop.
  `'none'` disables persistence and loses the cache on restart. Persistent modes reload
  the data into RAM, so size the cache to fit memory after restart too.

For key types, indexed deletes, scalar lookup functions, and persistence details, see the
[`OverwriteCache` guide](docs/en/engines/table-engines/special/overwrite-cache.md).

## How To Install (Linux, macOS, FreeBSD)

```
curl https://clickhouse.com/ | sh
```
Or skip the install: try [ClickHouse Cloud](https://console.clickhouse.cloud/signUp?utm_medium=referral&utm_source=github) for free and start querying in minutes.

## Useful Links

* [Official website](https://clickhouse.com/) has a quick high-level overview of ClickHouse on the main page.
* [ClickHouse Cloud](https://clickhouse.com/cloud) ClickHouse as a service, built by the creators and maintainers.
* [Tutorial](https://clickhouse.com/docs/getting_started/tutorial/) shows how to set up and query a small ClickHouse cluster.
* [Documentation](https://clickhouse.com/docs/) provides more in-depth information.
* [YouTube channel](https://www.youtube.com/c/ClickHouseDB) has a lot of content about ClickHouse in video format.
* [Slack](https://clickhouse.com/slack) and [Telegram](https://telegram.me/clickhouse_en) allow chatting with ClickHouse users in real-time.
* [Blog](https://clickhouse.com/blog/) contains various ClickHouse-related articles, as well as announcements and reports about events.
* [X](https://x.com/ClickHouseDB) and [LinkedIn](https://www.linkedin.com/company/clickhouseinc/) for short news.
* [Code Browser (github.dev)](https://github.dev/ClickHouse/ClickHouse) with syntax highlighting, powered by github.dev.
* [Contact us](https://clickhouse.com/company/contact) to discuss your use case with a ClickHouse expert.

## Monthly Release & Community Call
The [ClickHouse **26.9** Release Call](https://www.youtube.com/watch?v=wLrTlU5LLWI) took place on September 17, 2026 — watch the recording and the [slides](https://presentations.clickhouse.com/2026-release-26.9/).

The [ClickHouse **26.8** Release Call](https://www.youtube.com/watch?v=5A9gRYE0v2M) took place on August 27, 2026 — watch the recording and the [slides](https://presentations.clickhouse.com/2026-release-26.8/).

The [ClickHouse **26.7** Release Call](https://www.youtube.com/watch?v=mKBNLaFOVDA) took place on July 23, 2026 — watch the recording and the [slides](https://presentations.clickhouse.com/2026-release-26.7/).

The [ClickHouse **26.6** special "10 Year Anniversary" Release Call](https://www.youtube.com/watch?v=-NmqMH9y4EY) — recording and [slides](https://presentations.clickhouse.com/2026-release-26.6/).

Watch all release presentations and videos at [ClickHouse Theater](https://presentations.clickhouse.com/) and [YouTube Playlist](https://www.youtube.com/playlist?list=PL0Z2YDlm0b3jAlSy1JxyP8zluvXaN3nxU).

## Upcoming Events

Keep an eye out for upcoming meetups and events around the world.
Want to speak? Apply [here](https://forms.gle/3h4XCEENJZ3eaVGy7)
You can also peruse [ClickHouse Events](https://clickhouse.com/company/news-events) for a list of all upcoming trainings, meetups, speaking engagements, etc.

Upcoming meetups

* [Open House London](https://luma.com/ggnwajnd) - September 30th, 2026
* [Grok Bot Montreal Build Day](https://luma.com/ggnwajnd) - October 3rd, 2026
* [Open House Munich](https://luma.com/6xorx3qw) - October 6th, 2026
* [SF Tech Week AI Night](https://luma.com/clickh-zngw) - October 6th, 2026
* [AI Builders and Databases Stockholm](https://luma.com/clickh-ku2p) - October 8th, 2026
* [Mountain View Meetup](https://luma.com/clickh-5ms6) - October 8th, 2026
* [AI Builders and Databases Tel Aviv](https://luma.com/clickh-satv) - October 12th, 2026
* [AI Builders and Databases London](https://luma.com/clickh-ykgp) - October 21st, 2026
* [AI Builders and Databases Dubai](https://luma.com/clickh-vtzf)- October 23rd, 2026


Recent meetups
* [DET New York](https://luma.com/xf3wghdh) - September 17th, 2026
* [The Agentic Data Stack: Paris](https://luma.com/clickh-s2a1) - September 17th, 2026
* [The Agentic Data Stack: Zurich](https://luma.com/clickh-oo1l) - September 17th, 2026
* [Rows And Columns Summit](https://luma.com/event/evt-bQcR6tDKi8OmTXu) - September 22nd, 2026
* [Hands-on training: Building agents with ClickHouse and LibreChat Seattle](https://luma.com/clickh-0vvr) - September 23rd, 2026
* [Hands-on training: Agent Observability with Langfuse in San Francisco](https://luma.com/gp4dehl2) - September 23rd, 2026
* [PyData Seattle Meetup](https://luma.com/clickh-ttgg) - September 24th, 2026
* [build fridays sf x clickhouse + langfuse - work on your startup alongside others](https://luma.com/bf925) - September 25th, 2026
* [Chicago Meetup](https://luma.com/clickh-8tnc) - September 28th, 2026
* [Paris Meetup](https://luma.com/clickh-gsz1) - September 29th, 2026


## Recent Recordings

* **Recent Meetup Videos**: [Meetup Playlist](https://www.youtube.com/playlist?list=PL0Z2YDlm0b3iNDUzpY1S3L_iV4nARda_U) Whenever possible recordings of the ClickHouse Community Meetups are edited and presented as individual talks. 

## Interested in joining ClickHouse and making it your full-time job?

ClickHouse is a nice DBMS, and it's a good place to work.

Check out our **current openings** here: https://clickhouse.com/company/careers

Email: careers@clickhouse.com!
