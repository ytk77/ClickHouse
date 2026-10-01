#pragma once

#include <base/types.h>

namespace DB::Upsert
{
/// Performance budgets captured when the table opens; durability policy is fixed by the protocol.
struct Resources
{
    UInt64 index_cache_bytes = 64ULL << 20;
    UInt64 write_buffer_bytes = 16ULL << 20;
    UInt64 write_buffer_count = 3;
    UInt64 wal_bytes = 64ULL << 20;
    UInt64 background_jobs = 2;
    UInt64 max_open_files = 128;
    UInt64 recovery_threads = 4;
    UInt64 mask_cache_bytes = 32ULL << 20;
    bool partitioned_filters = true;
    void validate() const;
};
}
