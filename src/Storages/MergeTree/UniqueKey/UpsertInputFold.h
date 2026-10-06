#pragma once

#include <Core/Types.h>
#include <base/StringViewHash.h>
#include <algorithm>
#include <span>
#include <unordered_map>

namespace DB::Upsert
{
/// Returned views borrow immutable `keys`; the caller must retain their storage.
/// `wins` selects the original physical row, independent of hash-table iteration order.
template <typename Wins, typename Check, typename Hash = StringViewHash64>
auto selectInputWinners(std::span<const String> keys, Wins && wins, Check && check, Hash hash = {})
{
    std::unordered_map<std::string_view, size_t, Hash> winners(0, hash);
    /// Cap the initial bucket allocation for duplicate-heavy inputs.
    winners.reserve(std::min(keys.size(), size_t{4096}));
    for (size_t row = 0; row < keys.size(); ++row)
    {
        if (row % 65536 == 0)
            check();
        auto [it, inserted] = winners.emplace(std::string_view(keys[row]), row);
        if (!inserted && wins(row, it->second))
            it->second = row;
    }
    return winners;
}
}
