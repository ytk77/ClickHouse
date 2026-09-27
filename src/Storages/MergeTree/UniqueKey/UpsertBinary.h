#pragma once

#include <Storages/MergeTree/UniqueKey/UpsertLogRecord.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int LIMIT_EXCEEDED;
}

namespace DB::Upsert
{
struct BinaryWriter
{
    String bytes;
    void number(UInt64 value, size_t width = 8)
    {
        for (size_t i = 0; i < width; ++i)
            bytes.push_back(static_cast<char>(value >> (i * 8)));
    }
    void id(ObjectID value)
    {
        number(value[0]);
        number(value[1]);
    }
    void string(std::string_view value)
    {
        if (value.size() > MAX_RECORD_BYTES - 4 || bytes.size() > MAX_RECORD_BYTES - 4 - value.size())
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree payload exceeds its byte limit");
        number(value.size(), 4);
        bytes.append(value);
    }
};

struct BinaryReader
{
    std::string_view bytes;
    UInt64 number(size_t width = 8)
    {
        if (bytes.size() < width)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Truncated UpsertMergeTree payload");
        UInt64 result = 0;
        for (size_t i = 0; i < width; ++i)
            result |= static_cast<UInt64>(static_cast<unsigned char>(bytes[i])) << (i * 8);
        bytes.remove_prefix(width);
        return result;
    }
    ObjectID id()
    {
        const auto first = number();
        return {first, number()};
    }
    String string(size_t limit = MAX_RECORD_BYTES)
    {
        const auto length = number(4);
        if (length > limit || length > bytes.size())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree payload string length");
        String result(bytes.substr(0, length));
        bytes.remove_prefix(length);
        return result;
    }
    size_t count(size_t max_count, size_t minimum_bytes)
    {
        const auto result = number(4);
        if (result > max_count || result > bytes.size() / minimum_bytes)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree payload item count");
        return result;
    }
    void end() const
    {
        if (!bytes.empty())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Trailing bytes in UpsertMergeTree payload");
    }
};
}
