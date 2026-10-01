#include <Storages/MergeTree/UniqueKey/UpsertMaskVersions.h>
#include <gtest/gtest.h>
#include <map>
#include <random>
#include <optional>

using namespace DB::Upsert;

TEST(UpsertMaskVersions, PinnedRootsSurviveUpdatesAndRemoval)
{
    MaskVersions current;
    std::map<String, UInt64> expected;
    std::mt19937 generator(42);
    for (UInt64 generation = 1; generation <= 10000; ++generation)
    {
        const auto key = std::to_string(generator() % 1024);
        auto pinned = current;
        const auto previous = expected.find(key);
        const std::optional<UInt64> old_value = previous == expected.end() ? std::nullopt : std::optional<UInt64>(previous->second);
        if (generation % 3 == 0)
        {
            current.erase(key);
            expected.erase(key);
            EXPECT_EQ(current.find(key), nullptr);
        }
        else
        {
            current.set(key, generation);
            EXPECT_EQ(*current.find(key), generation);
        }
        if (old_value)
        {
            ASSERT_NE(pinned.find(key), nullptr);
            EXPECT_EQ(*pinned.find(key), *old_value);
        }
        else
            EXPECT_EQ(pinned.find(key), nullptr);
        if (generation % 3 != 0)
            expected[key] = generation;
        for (const auto & [model_key, value] : expected)
        {
            ASSERT_NE(current.find(model_key), nullptr);
            ASSERT_EQ(*current.find(model_key), value);
        }
    }
    auto pinned = current;
    current.clear();
    for (const auto & [key, value] : expected)
    {
        EXPECT_EQ(current.find(key), nullptr);
        ASSERT_NE(pinned.find(key), nullptr);
        EXPECT_EQ(*pinned.find(key), value);
    }
}
