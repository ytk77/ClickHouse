#pragma once

#include <base/types.h>
#include <algorithm>
#include <limits>

namespace DB::Upsert
{
inline UInt64 maintenanceMemoryCeiling(Int64 background_limit, Int64 server_limit)
{
    UInt64 ceiling = std::numeric_limits<UInt64>::max();
    if (background_limit > 0)
        ceiling = std::min(ceiling, static_cast<UInt64>(background_limit));
    if (server_limit > 0)
        ceiling = std::min(ceiling, static_cast<UInt64>(server_limit));
    return ceiling;
}

inline UInt64 maintenanceEffectiveTarget(UInt64 configured, UInt64 ceiling)
{
    /// `Native` buffers reserve up to eight targets; leave two more for lineage and bookkeeping.
    /// Selection also checks the actual row and lineage density against the same ceiling.
    constexpr UInt64 minimum = 32ULL << 20;
    while (configured > minimum && configured > ceiling / 10)
        configured = std::max(minimum, configured / 2);
    return configured;
}
}
