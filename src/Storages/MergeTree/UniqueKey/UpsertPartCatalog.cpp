#include <Storages/MergeTree/UniqueKey/UpsertPartCatalog.h>
#include <Storages/MergeTree/UniqueKey/UpsertMetrics.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace DB::Upsert
{
PartCatalog::Node::Node(DataPartPtr part_, Root left_, Root right_)
    : part(std::move(part_)), left(std::move(left_)), right(std::move(right_))
    , height(1 + std::max(left ? left->height : 0, right ? right->height : 0))
    , count(1 + (left ? left->count : 0) + (right ? right->count : 0))
    , rows(part->rows_count + (left ? left->rows : 0) + (right ? right->rows : 0))
    , min_metadata_version(part->getMetadataVersion()), min_data_version(part->info.getDataVersion())
    , has_lightweight_delete(part->hasLightweightDelete())
    , first_partition(left ? left->first_partition : part->info.getPartitionId())
    , last_partition(right ? right->last_partition : part->info.getPartitionId())
{
    for (const auto & child : {left, right})
    {
        if (!child)
            continue;
        min_metadata_version = std::min(min_metadata_version, child->min_metadata_version);
        min_data_version = std::min(min_data_version, child->min_data_version);
        has_lightweight_delete |= child->has_lightweight_delete;
    }
    ProfileEvents::increment(ProfileEvents::UpsertCatalogNodesAllocated);
}

PartCatalog::Root PartCatalog::node(DataPartPtr part, Root left, Root right)
{
    return std::make_shared<const Node>(std::move(part), std::move(left), std::move(right));
}

PartCatalog::Root PartCatalog::balance(DataPartPtr part, Root left, Root right)
{
    const auto height = [](const Root & value) { return value ? value->height : 0; };
    if (height(left) > height(right) + 1)
    {
        if (height(left->left) < height(left->right))
        {
            const auto pivot = left->right;
            left = node(pivot->part, node(left->part, left->left, pivot->left), pivot->right);
        }
        return node(left->part, left->left, node(std::move(part), left->right, std::move(right)));
    }
    if (height(right) > height(left) + 1)
    {
        if (height(right->right) < height(right->left))
        {
            const auto pivot = right->left;
            right = node(pivot->part, pivot->left, node(right->part, pivot->right, right->right));
        }
        return node(right->part, node(std::move(part), std::move(left), right->left), right->right);
    }
    return node(std::move(part), std::move(left), std::move(right));
}

PartCatalog::Root PartCatalog::insert(const Root & current, const DataPartPtr & part)
{
    if (!current)
        return node(part, {}, {});
    if (part->info < current->part->info)
        return balance(current->part, insert(current->left, part), current->right);
    if (current->part->info < part->info)
        return balance(current->part, current->left, insert(current->right, part));
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate UpsertMergeTree catalog part {}", part->name);
}

PartCatalog::Root PartCatalog::erase(const Root & current, const MergeTreePartInfo & info)
{
    if (!current)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing UpsertMergeTree catalog part {}", info.getPartNameForLogs());
    if (info < current->part->info)
        return balance(current->part, erase(current->left, info), current->right);
    if (current->part->info < info)
        return balance(current->part, current->left, erase(current->right, info));
    if (!current->left)
        return current->right;
    if (!current->right)
        return current->left;
    auto next = current->right;
    while (next->left)
        next = next->left;
    return balance(next->part, current->left, erase(current->right, next->part->info));
}

PartCatalog PartCatalog::withPart(const DataPartPtr & part) const
{
    PartCatalog result;
    result.root = insert(root, part);
    return result;
}

PartCatalog PartCatalog::withoutPart(const MergeTreePartInfo & info) const
{
    PartCatalog result;
    result.root = erase(root, info);
    return result;
}

void PartCatalog::versions(const Root & current, std::unordered_map<String, Int64> & result)
{
    if (!current)
        return;
    auto add = [&](const String & partition, Int64 version)
    {
        auto [it, inserted] = result.emplace(partition, version);
        if (!inserted)
            it->second = std::min(it->second, version);
    };
    if (current->first_partition == current->last_partition)
    {
        add(current->first_partition, current->min_data_version);
        return;
    }
    versions(current->left, result);
    add(current->part->info.getPartitionId(), current->part->info.getDataVersion());
    versions(current->right, result);
}

std::shared_ptr<const std::unordered_map<String, Int64>> PartCatalog::minDataVersions() const
{
    ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertCatalogVersionsMicroseconds);
    std::call_once(materialized->versions_once, [&]
    {
        auto result = std::make_shared<std::unordered_map<String, Int64>>();
        versions(root, *result);
        materialized->min_data_versions = std::move(result);
    });
    return materialized->min_data_versions;
}

void PartCatalog::collect(const Root & current, RangesInDataParts & result, size_t & index, size_t & offset,
    const std::function<bool(const IMergeTreeDataPart &)> & prune)
{
    if (!current)
        return;
    if (prune && current->first_partition == current->last_partition && prune(*current->part))
    {
        index += current->count;
        offset += current->rows;
        return;
    }
    collect(current->left, result, index, offset, prune);
    if (!prune || !prune(*current->part))
    {
        result.emplace_back(current->part, nullptr, index, offset);
    }
    ++index;
    offset += current->part->rows_count;
    collect(current->right, result, index, offset, prune);
}

RangesInDataPartsPtr PartCatalog::ranges(const std::function<bool(const IMergeTreeDataPart &)> & prune) const
{
    ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertCatalogRangesMicroseconds);
    bool use_pruning = false;
    if (prune)
    {
        const auto any_pruned = [&](const auto & self, const Root & entry) -> bool
        {
            if (!entry)
                return false;
            if (prune(*entry->part))
                return true;
            if (entry->first_partition == entry->last_partition)
                return false;
            return self(self, entry->left) || self(self, entry->right);
        };
        /// A usable predicate may still exclude no partitions. Preserve the reusable ranges in that case.
        use_pruning = any_pruned(any_pruned, root);
        if (use_pruning && root->first_partition == root->last_partition)
            return std::make_shared<const RangesInDataParts>();
    }
    std::unique_lock<std::mutex> lock;
    if (!use_pruning)
    {
        lock = std::unique_lock(materialized->mutex);
        if (materialized->ranges)
            return materialized->ranges;
    }
    auto result = std::make_shared<RangesInDataParts>();
    if (!use_pruning)
        result->reserve(size());
    size_t index = 0;
    size_t offset = 0;
    const std::function<bool(const IMergeTreeDataPart &)> no_pruning;
    collect(root, *result, index, offset, use_pruning ? prune : no_pruning);
    if (!use_pruning)
        materialized->ranges = result;
    return result;
}
}
