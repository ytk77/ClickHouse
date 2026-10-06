#include "config.h"

#if USE_ROCKSDB
#include <Storages/MergeTree/UniqueKey/UpsertLookupMetrics.h>
#include <base/scope_guard.h>
#include <gtest/gtest.h>
#include <stdexcept>

TEST(UpsertLookupMetrics, PreservesCountersAndRestoresProfilingLevel)
{
    auto * context = rocksdb::get_perf_context();
    const auto previous_level = rocksdb::GetPerfLevel();
    const auto previous_hits = context->block_cache_hit_count;
    SCOPE_EXIT({
        context->block_cache_hit_count = previous_hits;
        rocksdb::SetPerfLevel(previous_level);
    });
    rocksdb::SetPerfLevel(rocksdb::kDisable);
    const auto before = ProfileEvents::global_counters[ProfileEvents::UpsertWinnerLookupCacheHits];
    {
        DB::Upsert::WinnerLookupMetrics metrics;
        EXPECT_EQ(rocksdb::GetPerfLevel(), rocksdb::kEnableCount);
        EXPECT_EQ(context->block_cache_hit_count, previous_hits);
        context->block_cache_hit_count += 7;
    }
    EXPECT_EQ(rocksdb::GetPerfLevel(), rocksdb::kDisable);
    EXPECT_EQ(context->block_cache_hit_count, previous_hits + 7);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UpsertWinnerLookupCacheHits], before + 7);
}

TEST(UpsertLookupMetrics, PreservesStrongerLevelAndRecordsExceptionPath)
{
    auto * context = rocksdb::get_perf_context();
    const auto previous_level = rocksdb::GetPerfLevel();
    const auto previous_reads = context->block_read_count;
    SCOPE_EXIT({
        context->block_read_count = previous_reads;
        rocksdb::SetPerfLevel(previous_level);
    });
    rocksdb::SetPerfLevel(rocksdb::kEnableTime);
    const auto before = ProfileEvents::global_counters[ProfileEvents::UpsertWinnerLookupBlockReads];
    EXPECT_THROW(
        {
            DB::Upsert::WinnerLookupMetrics metrics(rocksdb::kEnableTimeExceptForMutex);
            EXPECT_EQ(rocksdb::GetPerfLevel(), rocksdb::kEnableTime);
            context->block_read_count += 3;
            throw std::runtime_error("lookup fixture");
        },
        std::runtime_error);
    EXPECT_EQ(rocksdb::GetPerfLevel(), rocksdb::kEnableTime);
    EXPECT_EQ(context->block_read_count, previous_reads + 3);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UpsertWinnerLookupBlockReads], before + 3);
}
#endif
