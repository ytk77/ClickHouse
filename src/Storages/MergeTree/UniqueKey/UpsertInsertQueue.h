#pragma once

#include <cstddef>

namespace DB::Upsert
{
inline constexpr size_t MAX_PENDING_INSERT_BYTES = 128U << 20;

inline bool insertPreparationNeedsDrain(size_t pending_parts, size_t pending_bytes, size_t next_bytes, size_t max_threads)
{
    /// An oversized preparation is admitted only to an empty queue. Check the existing
    /// budget before subtracting so another preparation cannot enter through unsigned wraparound.
    return pending_parts && (pending_parts >= max_threads
        || pending_bytes >= MAX_PENDING_INSERT_BYTES
        || next_bytes > MAX_PENDING_INSERT_BYTES - pending_bytes);
}
}
