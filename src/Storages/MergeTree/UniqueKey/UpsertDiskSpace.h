#pragma once

#include <base/types.h>
#include <limits>

namespace DB::Upsert
{
inline constexpr UInt64 DISK_SPACE_FLOOR = 1ULL << 30;
inline constexpr UInt64 REMOVAL_DISK_SPACE_FLOOR = 16ULL << 20;

inline UInt64 saturatingDiskBytes(UInt64 left, UInt64 right)
{
    const auto maximum = std::numeric_limits<UInt64>::max();
    return right > maximum - left ? maximum : left + right;
}

/// Physical source bytes are a planning proxy for equally encoded output, with 25 percent slack.
inline UInt64 rewriteDiskBytes(UInt64 physical_bytes, UInt64 state_allowance)
{
    const auto output = saturatingDiskBytes(physical_bytes, physical_bytes / 4 + (physical_bytes % 4 != 0));
    return saturatingDiskBytes(output, state_allowance);
}

/// Credit belongs only to the caller's held state reservation; it never grants space to another writer.
inline bool writeDiskSpaceFits(UInt64 available, UInt64 batch_bytes, UInt64 credited_bytes = 0, UInt64 floor_bytes = DISK_SPACE_FLOOR)
{
    const auto required = saturatingDiskBytes(saturatingDiskBytes(batch_bytes, batch_bytes), floor_bytes);
    return required != std::numeric_limits<UInt64>::max()
        && saturatingDiskBytes(available, credited_bytes) >= required;
}
}
