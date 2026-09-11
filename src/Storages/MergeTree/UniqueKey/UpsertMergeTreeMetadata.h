#pragma once

#include <base/types.h>

namespace DB
{

struct StorageInMemoryMetadata;
struct MergeTreeSettings;

/// Validate the initial `UpsertMergeTree` schema and install mandatory durability settings.
/// Applied on both creation and attachment: loading must not bypass the format contract.
void validateUpsertMergeTreeMetadata(
    const StorageInMemoryMetadata & metadata, const String & version_column, MergeTreeSettings & settings);

}
