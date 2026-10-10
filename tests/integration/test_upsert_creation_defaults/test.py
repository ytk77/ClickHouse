import uuid
import re

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
enabled = cluster.add_instance("enabled", stay_alive=True, with_zookeeper=True,
                               main_configs=["configs/cluster.xml"], user_configs=["configs/users.xml"])
disabled = cluster.add_instance("disabled", stay_alive=True, with_zookeeper=True,
                                main_configs=["configs/cluster.xml", "configs/disabled.xml"],
                                user_configs=["configs/users.xml"])
initiator = cluster.add_instance("initiator", stay_alive=True, with_zookeeper=True,
                                 main_configs=["configs/cluster.xml"],
                                 user_configs=["configs/users.xml", "configs/old_compatibility.xml"])
FLAGS = ("upsert_lineage_compression", "upsert_lineage_compact", "upsert_state_compression")
DDL = "(id UInt64, v UInt64) ENGINE=UpsertMergeTree(v) ORDER BY id UNIQUE KEY id"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def flags(node, name, expected, absent=False):
    definition = node.query(f"SHOW CREATE TABLE {name}")
    for flag in FLAGS:
        assert (flag not in definition) if absent else bool(re.search(rf"{flag} = " + (r"(1|true)\b" if expected else r"(0|false)\b"), definition)), definition
    return definition


def written_format(node, name, expected):
    node.query(f"SYSTEM STOP MERGES {name}")
    node.query(f"INSERT INTO {name} VALUES (1,1),(1,2),(2,2)")
    assert node.query(f"SELECT count(),uniqExact(id),sum(v) FROM {name}") == "2\t2\t4\n"
    database, table = name.split(".") if "." in name else ("default", name)
    paths = node.query(f"SELECT path FROM system.parts WHERE database='{database}' AND table='{table}' AND active").splitlines()
    assert len(paths) == 1
    root = node.query(f"SELECT data_paths[1] FROM system.tables WHERE database='{database}' AND name='{table}'").strip()
    node.exec_in_container(["python3", "-c", "from pathlib import Path; "
        "h=(Path(" + repr(paths[0]) + ")/'upsert_rows.bin').read_bytes()[:32]; "
        + ("assert h[:8]==b'UMPROW03' and h[10]==1; " if expected else "assert h[:8]==b'UMPROW01'; ")
        + "options=sorted((Path(" + repr(root) + ")/'upsert_state_v2').glob('OPTIONS-*')); assert options; "
        + "text=options[-1].read_text(); "
        + ("assert 'compression=kZSTD' in text" if expected else "assert 'compression=kLZ4Compression' in text")])


@pytest.mark.parametrize("node,expected", [(enabled, True), (disabled, False)])
def test_server_config_and_restart(node, expected):
    name = "creation_server_defaults"
    try:
        node.query(f"CREATE TABLE {name} {DDL}")
        before = flags(node, name, expected)
        written_format(node, name, expected)
        node.restart_clickhouse(kill=True)
        assert flags(node, name, expected) == before
        assert node.query(f"SELECT count(),uniqExact(id),sum(v) FROM {name}") == "2\t2\t4\n"
        node.query(f"ALTER TABLE {name} ADD COLUMN payload String")
        flags(node, name, expected)
    finally:
        node.query(f"DROP TABLE IF EXISTS {name} SYNC")


def test_default_profile_compatibility_and_legacy_attach():
    old_name, legacy = "creation_old_profile", "creation_legacy_metadata"
    try:
        initiator.query(f"CREATE TABLE {old_name} {DDL}")
        flags(initiator, old_name, False, absent=True)
        written_format(initiator, old_name, False)
        legacy_uuid = uuid.uuid4()
        enabled.query(f"CREATE TABLE {legacy} UUID '{legacy_uuid}' {DDL}", settings={"compatibility": "26.8"})
        enabled.query(f"DETACH TABLE {legacy} SYNC")
        enabled.query(f"ATTACH TABLE {legacy}")
        flags(enabled, legacy, False, absent=True)
        written_format(enabled, legacy, False)
        enabled.query(f"ALTER TABLE {legacy} ADD COLUMN payload String")
        enabled.restart_clickhouse(kill=True)
        flags(enabled, legacy, False, absent=True)
        assert enabled.query(f"SELECT count(),sum(v) FROM {legacy}") == "2\t4\n"
    finally:
        initiator.query(f"DROP TABLE IF EXISTS {old_name} SYNC")
        enabled.query(f"DROP TABLE IF EXISTS {legacy} SYNC")


@pytest.mark.parametrize("source,expected", [(enabled, True), (disabled, False)])
def test_replicated_database_pins_initiator_settings(source, expected):
    database = "creation_replicated_defaults"
    try:
        for node, replica in [(enabled, "enabled"), (disabled, "disabled")]:
            node.query(f"CREATE DATABASE {database} ENGINE=Replicated('/test/{database}', 'shard', '{replica}')")
        source.query(f"CREATE TABLE {database}.events {DDL}")
        for node in (enabled, disabled):
            node.query(f"SYSTEM SYNC DATABASE REPLICA {database}")
        assert flags(enabled, f"{database}.events", expected) == flags(disabled, f"{database}.events", expected)
        for node in (enabled, disabled):
            written_format(node, f"{database}.events", expected)
            node.restart_clickhouse(kill=True)
            flags(node, f"{database}.events", expected)
    finally:
        for node in (enabled, disabled):
            node.query(f"DROP DATABASE IF EXISTS {database} SYNC")


@pytest.mark.parametrize("format_version", [1, 3])
def test_on_cluster_explicit_engine_pins_flags(format_version):
    name = f"creation_cluster_{format_version}"
    settings = {"distributed_ddl_entry_format_version": format_version, "distributed_ddl_task_timeout": 60}
    try:
        enabled.query(f"CREATE TABLE {name} ON CLUSTER upsert_defaults {DDL}", settings=settings)
        assert flags(enabled, name, True) == flags(disabled, name, True)
        for node in (enabled, disabled):
            written_format(node, name, True)
    finally:
        enabled.query(f"DROP TABLE IF EXISTS {name} ON CLUSTER upsert_defaults SYNC", settings=settings)


def test_old_cluster_create_as_without_source_on_initiator():
    source, name = "creation_remote_source", "creation_remote_copy"
    settings = {"distributed_ddl_entry_format_version": 1, "distributed_ddl_task_timeout": 60}
    try:
        for node in (enabled, disabled):
            node.query(f"CREATE TABLE {source} (id UInt64) ENGINE=MergeTree ORDER BY id")
        assert initiator.query(f"EXISTS TABLE {source}") == "0\n"
        initiator.query(f"CREATE TABLE {name} ON CLUSTER upsert_defaults AS {source}", settings=settings)
        for node in (enabled, disabled):
            definition = node.query(f"SHOW CREATE TABLE {name}")
            assert "ENGINE = MergeTree" in definition and "upsert_" not in definition
    finally:
        for node in (enabled, disabled):
            node.query(f"DROP TABLE IF EXISTS {name} SYNC")
            node.query(f"DROP TABLE IF EXISTS {source} SYNC")



def test_old_cluster_inherited_upsert_uses_each_hosts_defaults():
    source, name = "creation_remote_upsert_source", "creation_remote_upsert_copy"
    settings = {"distributed_ddl_entry_format_version": 1, "distributed_ddl_task_timeout": 60, "compatibility": ""}
    try:
        for node in (enabled, disabled):
            node.query(f"CREATE TABLE {source} {DDL}", settings={"compatibility": "26.8"})
            flags(node, source, False, absent=True)
        assert initiator.query(f"EXISTS TABLE {source}") == "0\n"
        initiator.query(f"CREATE TABLE {name} ON CLUSTER upsert_defaults AS {source}", settings=settings)
        for node, expected in [(enabled, True), (disabled, False)]:
            flags(node, name, expected)
            written_format(node, name, expected)
    finally:
        for node in (enabled, disabled):
            node.query(f"DROP TABLE IF EXISTS {name} SYNC")
            node.query(f"DROP TABLE IF EXISTS {source} SYNC")


def test_profile_constraints_do_not_get_bypassed():
    name = "creation_constrained"
    try:
        error = enabled.query_and_get_error(f"CREATE TABLE {name} {DDL}", user="compression_limited")
        assert "SETTING_CONSTRAINT_VIOLATION" in error
        enabled.query(f"CREATE TABLE {name} {DDL} SETTINGS upsert_lineage_compression=0, "
                      "upsert_lineage_compact=0, upsert_state_compression=0", user="compression_limited")
        flags(enabled, name, False)
    finally:
        enabled.query(f"DROP TABLE IF EXISTS {name} SYNC")
