#pragma once

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <map>

namespace DB::Upsert
{

class MaskSnapshot
{
public:
    virtual ~MaskSnapshot() = default;
    virtual std::shared_ptr<const DeleteBitmap> getMask(const String & part) const = 0;
};

/// Published with the matching native parts as one immutable read version, independently of the writer's parts lock.
/// A composed mask represents the base plus every visible delta at the partition's recorded CSN.
struct SnapshotState
{
    std::map<String, UInt64> partition_csns;
    std::map<String, std::shared_ptr<const DeleteBitmap>> part_masks;
    std::shared_ptr<const MaskSnapshot> disk_masks;

    std::shared_ptr<const DeleteBitmap> getMask(const String & part) const
    {
        if (disk_masks)
            return disk_masks->getMask(part);
        auto it = part_masks.find(part);
        return it == part_masks.end() ? nullptr : it->second;
    }
};

using SnapshotPtr = std::shared_ptr<const SnapshotState>;

}
