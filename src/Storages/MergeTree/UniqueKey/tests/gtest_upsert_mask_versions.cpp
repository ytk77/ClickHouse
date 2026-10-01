#include <Storages/MergeTree/UniqueKey/UpsertMaintenanceSchedule.h>
#include <Storages/MergeTree/UniqueKey/UpsertPartCatalog.h>
#include <Storages/MergeTree/UniqueKey/UpsertMaskVersions.h>
#include <gtest/gtest.h>
#include <barrier>
#include <thread>
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


TEST(UpsertMaintenanceSchedule, IdleBackoffAndTtlCap)
{
    DB::Upsert::MaintenanceSchedule schedule;
    for (UInt64 expected : {1000, 2000, 4000, 8000, 16000, 30000, 30000})
    {
        schedule.beginCycle();
        EXPECT_EQ(schedule.finishSlice(false, false, false), 100);
        EXPECT_EQ(schedule.finishSlice(true, false, false), expected);
    }
    schedule.beginCycle();
    EXPECT_EQ(schedule.finishSlice(true, false, true), 1000);
    EXPECT_TRUE(schedule.notify());
    EXPECT_FALSE(schedule.notify());
    schedule.beginCycle();
    EXPECT_EQ(schedule.finishSlice(true, false, false), 1000);
}

TEST(UpsertMaintenanceSchedule, PublicationBeforeIdleAndWorkAcrossSlices)
{
    DB::Upsert::MaintenanceSchedule schedule;
    schedule.beginCycle();
    EXPECT_FALSE(schedule.notify());
    EXPECT_EQ(schedule.finishSlice(true, false, false), 1000);
    schedule.beginCycle();
    EXPECT_EQ(schedule.finishSlice(false, true, false), 100);
    EXPECT_EQ(schedule.finishSlice(true, false, false), 100);
    EXPECT_FALSE(schedule.notify());
    schedule.beginCycle();
    EXPECT_EQ(schedule.finishSlice(true, false, false), 1000);
    EXPECT_TRUE(schedule.notify());
}

TEST(UpsertPartCatalog, CopiesShareLazyVersions)
{
    DB::Upsert::PartCatalog catalog;
    auto copy = catalog;
    const auto versions = catalog.minDataVersions();
    EXPECT_TRUE(versions->empty());
    EXPECT_EQ(versions, copy.minDataVersions());
    EXPECT_EQ(versions, catalog.minDataVersions());
    EXPECT_TRUE(catalog.ranges()->empty());
    DB::Upsert::PartCatalog unrelated;
    EXPECT_NE(versions, unrelated.minDataVersions());
}

TEST(UpsertMaintenanceSchedule, ConcurrentIdleNotificationCannotLoseWakeup)
{
    MaintenanceSchedule schedule;
    std::barrier<> boundary(2);
    bool notified = false;
    constexpr size_t rounds = 2000;
    std::thread publisher([&]
    {
        for (size_t i = 0; i < rounds; ++i)
        {
            boundary.arrive_and_wait();
            notified = schedule.notify();
            boundary.arrive_and_wait();
        }
    });
    for (size_t i = 0; i < rounds; ++i)
    {
        for (size_t cycle = 0; cycle < 7; ++cycle)
        {
            schedule.beginCycle();
            schedule.finishSlice(true, false, false);
        }
        schedule.beginCycle();
        boundary.arrive_and_wait();
        const auto delay = schedule.finishSlice(true, false, false);
        boundary.arrive_and_wait();
        EXPECT_TRUE(delay <= 1000 || notified);
    }
    publisher.join();
}
