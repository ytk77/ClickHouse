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
    extern const MergeTreeSettingsBool async_insert;
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
    if (settings[MergeTreeSetting::async_insert])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree does not support the async_insert table setting");

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

    if (metadata.hasAnyTTL() || metadata.hasProjections() || !metadata.sampling_key.column_names.empty())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree does not support TTL, projections or SAMPLE BY");

    /// These settings are format invariants, including when configuration defaults change.
    settings.set("assign_part_uuids", true);
    settings.set("fsync_after_insert", true);
    settings.set("fsync_part_directory", true);
}

}
