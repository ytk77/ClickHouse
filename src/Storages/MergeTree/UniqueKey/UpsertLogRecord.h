#pragma once

#include <Storages/MergeTree/UniqueKey/UpsertRowMetadata.h>
#include <optional>

namespace DB::Upsert
{

/// Values are persistent. Reserved operations remain recognizable but require a versioned payload decoder.
enum class RecordKind : UInt8
{
    Upsert = 1,
    Delete = 2,
    Noop = 3,
    Part = 4,
    Drain = 5,
    Merge = 6,
    Mutation = 7,
    Materialized = 8,
    Checkpoint = 9,
};

struct LogRecord
{
    ObjectID incarnation{};
    String partition_id;
    UInt64 csn = 0;
    ObjectID previous_hash{};
    ObjectID operation_id{};
    RecordKind kind = RecordKind::Noop;
    String payload;

    bool operator==(const LogRecord &) const = default;
};

inline constexpr UInt32 MAX_RECORD_BYTES = 64U << 20;
inline constexpr UInt32 MAX_PARTITION_ID_BYTES = 1024;

String encodeLogRecord(const LogRecord & record);
ObjectID hashLogRecord(std::string_view frame);

struct DecodedRecord
{
    LogRecord record;
    ObjectID hash{};
    size_t bytes_consumed = 0;
};

/// Return no value only for a truncated final frame in an explicitly unsealed suffix.
/// Complete frames with bad checksums, versions, kinds or identities always fail closed.
std::optional<DecodedRecord> decodeLogRecord(std::string_view bytes, bool allow_incomplete_tail);

/// Validate the chain against a selected recovery root. Never search past a bad frame.
class LogReplayCursor
{
public:
    LogReplayCursor(ObjectID incarnation_, String partition_id_, UInt64 boundary_csn_ = 0, ObjectID boundary_hash_ = {});
    void accept(const DecodedRecord & decoded);
    UInt64 nextCSN() const;
    UInt64 publishedCSN() const
    {
        return csn;
    }
    const ObjectID & tailHash() const
    {
        return hash;
    }

private:
    ObjectID incarnation;
    String partition_id;
    UInt64 csn;
    ObjectID hash;
};

}
