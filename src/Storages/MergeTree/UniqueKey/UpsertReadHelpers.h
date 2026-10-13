#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/UniqueKey/UpsertSnapshot.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
}

/// Use only the visibility snapshot pinned together with the selected native parts.
/// Projection readers retain their own parent-mask validation.
inline bool areUpsertReadRangesFullyHidden(const RangesInDataPart & selected, const Upsert::SnapshotPtr & snapshot)
{
    if (!snapshot)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree reader requires a pinned visibility snapshot");
    const auto & part = selected.data_part;
    if (part->isProjectionPart() || selected.ranges.empty())
        return false;
    const auto mask = snapshot->getMask(part->getDeleteBitmapCacheIdentity());
    if (!mask)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree snapshot lacks the mask for part {}", part->name);
    const auto hidden = mask->cardinality();
    if (hidden > part->rows_count || mask->rangeCardinality(0, part->rows_count) != hidden)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree visibility mask exceeds the rows of part {}", part->name);
    if (!hidden)
        return false;
    if (hidden == part->rows_count)
        return true;

    const auto & granularity = *part->index_granularity;
    for (const auto & range : selected.ranges)
    {
        if (range.begin >= range.end || range.end > granularity.getMarksCountWithoutFinal())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree reader received an invalid mark range");
        const auto begin = granularity.getMarkStartingRow(range.begin);
        const auto end = std::min<UInt64>(part->rows_count, granularity.getMarkStartingRow(range.end));
        if (begin > end)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree mark range exceeds the rows of part {}", part->name);
        if (mask->rangeCardinality(begin, end) != end - begin)
            return false;
    }
    return true;
}
}
