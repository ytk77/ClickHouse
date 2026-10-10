#include <Storages/MergeTree/UniqueKey/UpsertMergeTreeMetadata.h>

#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool ttl_only_drop_parts;
    extern const MergeTreeSettingsUInt64 upsert_maintenance_target_bytes;
    extern const MergeTreeSettingsUInt64 upsert_insert_target_bytes;
    extern const MergeTreeSettingsUInt64 upsert_lineage_compression_level;
    extern const MergeTreeSettingsBool upsert_lineage_compression;
    extern const MergeTreeSettingsBool upsert_lineage_compact;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{
bool supportedKeyType(const IDataType & type)
{
    const WhichDataType which(type);
    if (which.isInteger() || which.isString() || which.isFixedString())
        return true;
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(&type))
    {
        const auto & elements = tuple->getElements();
        return !elements.empty() && std::ranges::all_of(elements, [](const auto & element)
        {
            return supportedKeyType(*element);
        });
    }
    return false;
}
}

void validateUpsertMergeTreeMetadata(
    const StorageInMemoryMetadata & metadata, const String & version_column, MergeTreeSettings & settings)
{
    if (settings[MergeTreeSetting::upsert_lineage_compact] && !settings[MergeTreeSetting::upsert_lineage_compression])
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting upsert_lineage_compact requires upsert_lineage_compression");
    const UInt64 lineage_level = settings[MergeTreeSetting::upsert_lineage_compression_level];
    if (lineage_level < 1 || lineage_level > 9)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting upsert_lineage_compression_level={} must be between 1 and 9", lineage_level);
    const UInt64 insert_target_bytes = settings[MergeTreeSetting::upsert_insert_target_bytes];
    if (insert_target_bytes < (32ULL << 20) || insert_target_bytes > (1ULL << 30))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Setting upsert_insert_target_bytes={} must be between 33554432 and 1073741824 bytes", insert_target_bytes);

    const UInt64 target_bytes = settings[MergeTreeSetting::upsert_maintenance_target_bytes];
    if (target_bytes < (32ULL << 20) || target_bytes > (8ULL << 30))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Setting upsert_maintenance_target_bytes={} must be between 33554432 and 8589934592 bytes", target_bytes);

    if (!metadata.hasUniqueKey())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "UpsertMergeTree requires a nonempty UNIQUE KEY");

    for (const auto & type : metadata.unique_key.data_types)
        if (!supportedKeyType(*type))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "UpsertMergeTree UNIQUE KEY type {} is unsupported; use non-null integers, String, FixedString or tuples of these", type->getName());

    if (!version_column.empty())
    {
        const auto supported_version_type = [](const IDataType & type)
        {
            const WhichDataType which(type);
            return which.isUInt64() || which.isDateTime() || which.isDateTime64();
        };
        if (!metadata.columns.hasPhysical(version_column)
            || !supported_version_type(*metadata.columns.getPhysical(version_column).type))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "UpsertMergeTree version column {} must be a stored non-null UInt64, DateTime or DateTime64", version_column);
    }

    for (const auto & column : metadata.columns)
        if (column.name.starts_with("_upsert_"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column prefix _upsert_ is reserved by UpsertMergeTree");

    for (const auto * key : {&metadata.unique_key, &metadata.partition_key, &metadata.sorting_key})
        if (key->expression)
            key->expression->getActionsDAG().assertDeterministic();

    if (metadata.hasAnyColumnTTL() || !metadata.getMoveTTLs().empty()
        || !metadata.getRecompressionTTLs().empty() || !metadata.getGroupByTTLs().empty())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree supports only DELETE TTL, optionally with WHERE");
    if (metadata.hasAnyTTL() && settings[MergeTreeSetting::ttl_only_drop_parts])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree TTL requires ttl_only_drop_parts = 0");
    if (!metadata.sampling_key.column_names.empty())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree does not support SAMPLE BY");

    for (const auto & projection : metadata.getProjections())
        if (projection.with_block_number)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree does not support commit-order projections");

    /// Synchronous execution preserves per-request deduplication and acknowledges durable commits.
    settings.set("async_insert", false);

    /// These settings are format invariants, including when configuration defaults change.
    settings.set("assign_part_uuids", true);
    settings.set("fsync_after_insert", true);
    settings.set("fsync_part_directory", true);
}

}
