#pragma once

#include <base/types.h>
#include <Storages/MergeTree/UniqueKey/UpsertGlobalAllocator.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <algorithm>
#include <memory>
#include <vector>

namespace DB::Upsert
{
/// Immutable mask index. Old roots remain valid for pinned read snapshots.
template <typename Value>
class MaskIndex
{
    struct Node;
    using Root = std::shared_ptr<const Node>;
    struct Node
    {
        String key;
        Value value;
        Root left;
        Root right;
        size_t height;
        size_t count;
        Node(String key_, Value value_, Root left_, Root right_)
            : key(std::move(key_)), value(std::move(value_)), left(std::move(left_)), right(std::move(right_))
            , height(1 + std::max(left ? left->height : 0, right ? right->height : 0))
            , count(1 + (left ? left->count : 0) + (right ? right->count : 0))
        {
        }
    };
    Root root;
    static Root node(const String & key, Value value, Root left, Root right)
    {
        /// Index nodes outlive the query that publishes them; account allocation and release globally.
        MemoryTrackerBlockerInThread blocker(VariableContext::User);
        return Root(new Node(key, std::move(value), std::move(left), std::move(right)), [](const Node * entry)
        {
            MemoryTrackerBlockerInThread release_blocker(VariableContext::User);
            delete entry;
        }, GlobalAllocator<Node>{});
    }
    static Root balance(const String & key, Value value, Root left, Root right)
    {
        const auto height = [](const Root & entry) { return entry ? entry->height : 0; };
        if (height(left) > height(right) + 1)
        {
            if (height(left->left) < height(left->right))
            {
                const auto pivot = left->right;
                left = node(pivot->key, pivot->value, node(left->key, left->value, left->left, pivot->left), pivot->right);
            }
            return node(left->key, left->value, left->left, node(key, value, left->right, std::move(right)));
        }
        if (height(right) > height(left) + 1)
        {
            if (height(right->right) < height(right->left))
            {
                const auto pivot = right->left;
                right = node(pivot->key, pivot->value, pivot->left, node(right->key, right->value, pivot->right, right->right));
            }
            return node(right->key, right->value, node(key, value, std::move(left), right->left), right->right);
        }
        return node(key, value, std::move(left), std::move(right));
    }
    static Root put(const Root & entry, const String & key, Value value)
    {
        if (!entry)
            return node(key, value, {}, {});
        if (key < entry->key)
            return balance(entry->key, entry->value, put(entry->left, key, value), entry->right);
        if (entry->key < key)
            return balance(entry->key, entry->value, entry->left, put(entry->right, key, value));
        return node(key, value, entry->left, entry->right);
    }
    static Root remove(const Root & entry, const String & key)
    {
        if (!entry)
            return {};
        if (key < entry->key)
            return balance(entry->key, entry->value, remove(entry->left, key), entry->right);
        if (entry->key < key)
            return balance(entry->key, entry->value, entry->left, remove(entry->right, key));
        if (!entry->left)
            return entry->right;
        if (!entry->right)
            return entry->left;
        auto next = entry->right;
        while (next->left)
            next = next->left;
        return balance(next->key, next->value, entry->left, remove(entry->right, next->key));
    }
    static Root build(const std::vector<std::pair<String, Value>> & entries, size_t begin, size_t end)
    {
        if (begin == end)
            return {};
        const auto middle = begin + (end - begin) / 2;
        return node(entries[middle].first, entries[middle].second,
            build(entries, begin, middle), build(entries, middle + 1, end));
    }

public:
    /// The caller validates sorted unique keys before constructing the recovery root.
    void assignSorted(const std::vector<std::pair<String, Value>> & entries)
    {
        root = build(entries, 0, entries.size());
    }
    size_t size() const { return root ? root->count : 0; }
    const Value * find(const String & key) const
    {
        const auto * entry = root.get();
        while (entry)
        {
            if (key == entry->key)
                return &entry->value;
            entry = (key < entry->key ? entry->left : entry->right).get();
        }
        return nullptr;
    }
    void set(const String & key, Value value)
    {
        root = put(root, key, value);
    }
    void erase(const String & key)
    {
        root = remove(root, key);
    }
    void clear()
    {
        root.reset();
    }
};

using MaskVersions = MaskIndex<UInt64>;
}
