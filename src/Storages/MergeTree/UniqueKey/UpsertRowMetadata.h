#pragma once

#include <base/types.h>
#include <array>
#include <compare>
#include <optional>
#include <string_view>

namespace DB::Upsert
{

using ObjectID = std::array<UInt64, 2>;

/// The containing table manifest supplies the incarnation; the containing log supplies the partition.
struct RowMetadata
{
    ObjectID operation_id{};
    UInt64 block_ordinal = 0;
    UInt64 write_csn = 0;
    std::optional<UInt64> version = std::nullopt;
    bool self_binding = false;

    bool operator==(const RowMetadata &) const = default;

    /// Resolve `SELF` only with the producing part's authenticated activation record.
    RowMetadata resolve(UInt64 activation_csn) const;
    std::strong_ordering compareWinner(const RowMetadata & rhs) const;
};

struct IndexValue
{
    UInt32 row_offset = 0;
    RowMetadata row;
    bool operator==(const IndexValue &) const = default;
};

/// A conservative row bound keeps worst-case 32-bit roaring masks below 256 MiB.
/// Actual encoded lengths must still be checked before appending a terminal record.
inline constexpr UInt32 MAX_PART_ROWS = 1U << 28;

/// Versioned, little-endian encoding. Legacy four-byte SST values are never accepted.
String encodeIndexValue(const IndexValue & value);
/// Replace the output, retaining its allocation. Invalid input leaves it unchanged.
void encodeIndexValue(const IndexValue & value, String & output);
IndexValue decodeIndexValue(std::string_view bytes);

}
