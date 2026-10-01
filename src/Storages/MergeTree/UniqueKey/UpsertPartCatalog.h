#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <functional>
#include <mutex>

namespace DB::Upsert
{
/// Immutable ordered catalog. Updating a part copies only the balanced search path.
/// Native part ownership and disk authorization remain the responsibility of the caller.
class PartCatalog
{
    struct Node;
    using Root = std::shared_ptr<const Node>;
    struct Node
    {
        DataPartPtr part;
        Root left;
        Root right;
        size_t height;
        size_t count;
        size_t rows;
        Int64 min_metadata_version;
        Int64 min_data_version;
        bool has_lightweight_delete;
        String first_partition;
        String last_partition;
        Node(DataPartPtr part_, Root left_, Root right_);
    };
    Root root;
    struct Materialized
    {
        std::mutex mutex;
        RangesInDataPartsPtr ranges;
        std::once_flag versions_once;
        std::shared_ptr<const std::unordered_map<String, Int64>> min_data_versions;
    };
    std::shared_ptr<Materialized> materialized = std::make_shared<Materialized>();
    static Root node(DataPartPtr part, Root left, Root right);
    static Root balance(DataPartPtr part, Root left, Root right);
    static Root insert(const Root & current, const DataPartPtr & part);
    static Root erase(const Root & current, const MergeTreePartInfo & info);
    static void collect(const Root & current, RangesInDataParts & result, size_t & index, size_t & offset,
        const std::function<bool(const IMergeTreeDataPart &)> & prune);
    static void versions(const Root & current, std::unordered_map<String, Int64> & result);

public:
    PartCatalog withPart(const DataPartPtr & part) const;
    PartCatalog withoutPart(const MergeTreePartInfo & info) const;
    size_t size() const
    {
        return root ? root->count : 0;
    }
    Int64 minMetadataVersion() const
    {
        return root ? root->min_metadata_version : -1;
    }
    bool hasLightweightDelete() const
    {
        return root && root->has_lightweight_delete;
    }
    std::shared_ptr<const std::unordered_map<String, Int64>> minDataVersions() const;
    RangesInDataPartsPtr ranges(const std::function<bool(const IMergeTreeDataPart &)> & prune = {}) const;
};
}
