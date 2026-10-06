#pragma once

#include <Storages/MergeTree/UniqueKey/UpsertMetrics.h>
#include <rocksdb/perf_context.h>
#include <array>

namespace DB::Upsert
{

/// Measure only the current thread's winner lookup, preserving its counters and previous profiling level.
/// Counts are enabled by default. Internal `RocksDB` timers require an explicit level and add overhead.
/// Compare a measurement build with the uninstrumented binary even when collecting only counts.
class WinnerLookupMetrics
{
public:
    explicit WinnerLookupMetrics(rocksdb::PerfLevel requested_level = rocksdb::kEnableCount)
        : context(rocksdb::get_perf_context()), previous_level(rocksdb::GetPerfLevel())
    {
        const auto & metrics = descriptors();
        for (size_t i = 0; i < metrics.size(); ++i)
            before[i] = context->*metrics[i].field;
        if (previous_level < requested_level)
            rocksdb::SetPerfLevel(requested_level);
    }

    ~WinnerLookupMetrics()
    {
        rocksdb::SetPerfLevel(previous_level);
        const auto & metrics = descriptors();
        for (size_t i = 0; i < metrics.size(); ++i)
            ProfileEvents::increment(metrics[i].event, (context->*metrics[i].field) - before[i]);
    }

    WinnerLookupMetrics(const WinnerLookupMetrics &) = delete;
    WinnerLookupMetrics & operator=(const WinnerLookupMetrics &) = delete;

private:
    struct Metric
    {
        ProfileEvents::Event event;
        uint64_t rocksdb::PerfContextBase::* field;
    };

    static const std::array<Metric, 17> & descriptors()
    {
        static const std::array<Metric, 17> metrics{{
            {ProfileEvents::UpsertWinnerLookupCacheHits, &rocksdb::PerfContextBase::block_cache_hit_count},
            {ProfileEvents::UpsertWinnerLookupIndexCacheHits, &rocksdb::PerfContextBase::block_cache_index_hit_count},
            {ProfileEvents::UpsertWinnerLookupFilterCacheHits, &rocksdb::PerfContextBase::block_cache_filter_hit_count},
            {ProfileEvents::UpsertWinnerLookupCacheHitBytes, &rocksdb::PerfContextBase::block_cache_read_byte},
            {ProfileEvents::UpsertWinnerLookupBlockReads, &rocksdb::PerfContextBase::block_read_count},
            {ProfileEvents::UpsertWinnerLookupIndexBlockReads, &rocksdb::PerfContextBase::index_block_read_count},
            {ProfileEvents::UpsertWinnerLookupFilterBlockReads, &rocksdb::PerfContextBase::filter_block_read_count},
            {ProfileEvents::UpsertWinnerLookupReadBytes, &rocksdb::PerfContextBase::block_read_byte},
            {ProfileEvents::UpsertWinnerLookupBlockReadNanoseconds, &rocksdb::PerfContextBase::block_read_time},
            {ProfileEvents::UpsertWinnerLookupDecompressNanoseconds, &rocksdb::PerfContextBase::block_decompress_time},
            {ProfileEvents::UpsertWinnerLookupChecksumNanoseconds, &rocksdb::PerfContextBase::block_checksum_time},
            {ProfileEvents::UpsertWinnerLookupMemtableProbes, &rocksdb::PerfContextBase::get_from_memtable_count},
            {ProfileEvents::UpsertWinnerLookupMemtableNanoseconds, &rocksdb::PerfContextBase::get_from_memtable_time},
            {ProfileEvents::UpsertWinnerLookupTableNanoseconds, &rocksdb::PerfContextBase::get_from_output_files_time},
            {ProfileEvents::UpsertWinnerLookupKeyComparisons, &rocksdb::PerfContextBase::user_key_comparison_count},
            {ProfileEvents::UpsertWinnerLookupBloomPositives, &rocksdb::PerfContextBase::bloom_sst_hit_count},
            {ProfileEvents::UpsertWinnerLookupBloomNegatives, &rocksdb::PerfContextBase::bloom_sst_miss_count},
        }};
        return metrics;
    }

    rocksdb::PerfContext * context;
    rocksdb::PerfLevel previous_level;
    std::array<uint64_t, 17> before{};
};

}
