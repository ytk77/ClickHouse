#include <Storages/MergeTree/UniqueKey/UpsertRowMetadata.h>
#include <Common/Exception.h>
#include <tuple>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_FORMAT_VERSION;
}

namespace Upsert
{
namespace
{
void appendLE(String & out, UInt64 value, size_t width)
{
    for (size_t i = 0; i < width; ++i)
        out.push_back(static_cast<char>(value >> (8 * i)));
}

UInt64 takeLE(std::string_view & bytes, size_t width)
{
    if (bytes.size() < width)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Truncated UpsertMergeTree row metadata");
    UInt64 value = 0;
    for (size_t i = 0; i < width; ++i)
        value |= static_cast<UInt64>(static_cast<unsigned char>(bytes[i])) << (8 * i);
    bytes.remove_prefix(width);
    return value;
}

void validate(const RowMetadata & row)
{
    if (row.operation_id == ObjectID{} || (row.self_binding ? row.write_csn != 0 : row.write_csn == 0))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree row identity or write CSN binding");
}
}

RowMetadata RowMetadata::resolve(UInt64 activation_csn) const
{
    validate(*this);
    if (!self_binding)
        return *this;
    if (!activation_csn)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "UpsertMergeTree SELF binding requires a nonzero activation CSN");
    auto result = *this;
    result.self_binding = false;
    result.write_csn = activation_csn;
    return result;
}

std::strong_ordering RowMetadata::compareWinner(const RowMetadata & rhs) const
{
    validate(*this);
    validate(rhs);
    if (self_binding || rhs.self_binding || version.has_value() != rhs.version.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot compare unresolved or incompatible UpsertMergeTree winner tuples");
    return std::tie(version, write_csn, block_ordinal) <=> std::tie(rhs.version, rhs.write_csn, rhs.block_ordinal);
}

String encodeIndexValue(const IndexValue & value)
{
    String result;
    encodeIndexValue(value, result);
    return result;
}

void encodeIndexValue(const IndexValue & value, String & result)
{
    validate(value.row);
    if (value.row_offset >= MAX_PART_ROWS)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "UpsertMergeTree row offset exceeds the part row limit");
    result.clear();
    result.reserve(50);
    result.append("UMRV", 4);
    appendLE(result, 1, 1);
    appendLE(result, (value.row.version ? 1 : 0) | (value.row.self_binding ? 2 : 0), 1);
    appendLE(result, value.row_offset, 4);
    for (auto limb : value.row.operation_id)
        appendLE(result, limb, 8);
    appendLE(result, value.row.block_ordinal, 8);
    appendLE(result, value.row.write_csn, 8);
    if (value.row.version)
        appendLE(result, *value.row.version, 8);
}

IndexValue decodeIndexValue(std::string_view bytes)
{
    if (!bytes.starts_with("UMRV"))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree row metadata magic mismatch; legacy SST values are incompatible");
    bytes.remove_prefix(4);
    if (takeLE(bytes, 1) != 1)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported UpsertMergeTree row metadata version");
    const auto flags = takeLE(bytes, 1);
    if (flags & ~3ULL)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown UpsertMergeTree row metadata flags");
    IndexValue value;
    value.row_offset = static_cast<UInt32>(takeLE(bytes, 4));
    for (auto & limb : value.row.operation_id)
        limb = takeLE(bytes, 8);
    value.row.block_ordinal = takeLE(bytes, 8);
    value.row.write_csn = takeLE(bytes, 8);
    value.row.self_binding = flags & 2;
    if (flags & 1)
        value.row.version = takeLE(bytes, 8);
    validate(value.row);
    if (!bytes.empty() || value.row_offset >= MAX_PART_ROWS)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree row metadata length or offset");
    return value;
}
}
}
