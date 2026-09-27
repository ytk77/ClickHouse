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
}
}
