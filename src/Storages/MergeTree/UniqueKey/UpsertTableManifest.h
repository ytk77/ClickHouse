#pragma once

#include <Storages/MergeTree/UniqueKey/UpsertRowMetadata.h>

namespace DB
{
struct StorageInMemoryMetadata;

namespace Upsert
{
/// Bind an existing table to its schema and incarnation before native part discovery.
ObjectID initializeTableManifest(
    const String & table_path, const String & disk_name, const String & disk_path,
    const StorageInMemoryMetadata & metadata, const String & version_column, bool attach, bool & use_disk_state);
/// Atomically upgrade old disk-state manifests before allowing mutable `TTL` metadata.
void enableMutableTTL(
    const String & table_path, const String & disk_name, const StorageInMemoryMetadata & metadata, const String & version_column);
/// Keep immutable key bindings in the manifest while SQL metadata owns columns and indexes.
void enableMutableSchema(
    const String & table_path, const String & disk_name, const StorageInMemoryMetadata & metadata, const String & version_column);
/// Enable durable detached-input cleanup; older binaries must reject this manifest.
void enablePartitionOperations(
    const String & table_path, const String & disk_name, const StorageInMemoryMetadata & metadata, const String & version_column);
}
}
