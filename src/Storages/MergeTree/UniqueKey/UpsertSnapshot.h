#pragma once

#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <map>

namespace DB::Upsert
{

class MaskSnapshot
{
public:
    virtual ~MaskSnapshot() = default;
    virtual UInt64 getPartitionCSN(const String & partition) const = 0;
    virtual std::shared_ptr<const DeleteBitmap> getMask(const String & part) const = 0;
    virtual void verifyPart(const String & name, const String & identity, bool force) const = 0;
    virtual void checkVerification() const = 0;
};

/// Published with the matching native parts as one immutable read version, independently of the writer's parts lock.
/// A composed mask represents the base plus every visible delta at the partition's recorded CSN.
struct SnapshotState
{
    void checkVerification() const
    {
        if (disk_masks)
            disk_masks->checkVerification();
    }
    /// The caller must pin the native part until verification and all subsequent reads finish.
    void verifyPart(const String & name, const String & identity, bool force = false) const
    {
        if (disk_masks)
            disk_masks->verifyPart(name, identity, force);
    }

    std::map<String, UInt64> partition_csns;
    std::map<String, std::shared_ptr<const DeleteBitmap>> part_masks;
    std::shared_ptr<const MaskSnapshot> disk_masks;

    UInt64 getPartitionCSN(const String & partition) const
    {
        if (disk_masks)
            return disk_masks->getPartitionCSN(partition);
        const auto it = partition_csns.find(partition);
        return it == partition_csns.end() ? 0 : it->second;
    }

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
