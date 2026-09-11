#include <Storages/MergeTree/UniqueKey/UpsertLogRecord.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <zlib.h>
#include <limits>

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
constexpr std::string_view MAGIC = "UMTLOG01";
constexpr size_t PREFIX_BYTES = 20;
constexpr size_t MIN_FRAME_BYTES = 84;

void appendLE(String & out, UInt64 value, size_t width)
{
    for (size_t i = 0; i < width; ++i)
        out.push_back(static_cast<char>(value >> (8 * i)));
}

UInt64 takeLE(std::string_view & bytes, size_t width)
{
    if (bytes.size() < width)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Truncated UpsertMergeTree log field");
    UInt64 value = 0;
    for (size_t i = 0; i < width; ++i)
        value |= static_cast<UInt64>(static_cast<unsigned char>(bytes[i])) << (8 * i);
    bytes.remove_prefix(width);
    return value;
}

UInt32 checksum(std::string_view bytes)
{
    return static_cast<UInt32>(crc32(0, reinterpret_cast<const Bytef *>(bytes.data()), static_cast<uInt>(bytes.size())));
}

bool validKind(RecordKind kind)
{
    return kind >= RecordKind::Upsert && kind <= RecordKind::Checkpoint;
}

void validate(const LogRecord & record)
{
    if (record.incarnation == ObjectID{} || record.operation_id == ObjectID{} || !record.csn
        || record.partition_id.empty() || record.partition_id.size() > MAX_PARTITION_ID_BYTES || !validKind(record.kind))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree terminal record identity, CSN, partition or kind");
}
}

String encodeLogRecord(const LogRecord & record)
{
    validate(record);
    const size_t length = MIN_FRAME_BYTES + record.partition_id.size() + record.payload.size();
    if (length > MAX_RECORD_BYTES)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "UpsertMergeTree log record exceeds {} bytes", MAX_RECORD_BYTES);

    String out;
    out.reserve(length);
    out.append(MAGIC);
    appendLE(out, 1, 2);
    appendLE(out, static_cast<UInt8>(record.kind), 1);
    appendLE(out, 0, 1);
    appendLE(out, length, 4);
    appendLE(out, checksum(out), 4);
    for (auto limb : record.incarnation)
        appendLE(out, limb, 8);
    appendLE(out, record.csn, 8);
    for (auto limb : record.previous_hash)
        appendLE(out, limb, 8);
    for (auto limb : record.operation_id)
        appendLE(out, limb, 8);
    appendLE(out, record.partition_id.size(), 4);
    out.append(record.partition_id);
    out.append(record.payload);
    appendLE(out, checksum(out), 4);
    return out;
}

ObjectID hashLogRecord(std::string_view frame)
{
    SipHash hasher;
    hasher.update(frame.data(), frame.size());
    ObjectID result;
    hasher.get128(result[0], result[1]);
    return result;
}

std::optional<DecodedRecord> decodeLogRecord(std::string_view bytes, bool allow_incomplete_tail)
{
    auto incomplete = [&]() -> std::optional<DecodedRecord>
    {
        if (allow_incomplete_tail)
            return std::nullopt;
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Incomplete frame in sealed UpsertMergeTree log");
    };
    if (bytes.size() < MAGIC.size())
    {
        if (bytes != MAGIC.substr(0, bytes.size()))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree log magic mismatch");
        return incomplete();
    }
    if (!bytes.starts_with(MAGIC))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree log magic mismatch");
    if (bytes.size() < PREFIX_BYTES)
        return incomplete();

    auto fields = bytes.substr(MAGIC.size());
    if (takeLE(fields, 2) != 1)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported UpsertMergeTree log format version");
    const auto kind = static_cast<RecordKind>(takeLE(fields, 1));
    if (!validKind(kind) || takeLE(fields, 1) != 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown UpsertMergeTree record kind or flags");
    const auto length = takeLE(fields, 4);
    if (takeLE(fields, 4) != checksum(bytes.substr(0, PREFIX_BYTES - 4)))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree record header checksum mismatch");
    if (length < MIN_FRAME_BYTES || length > MAX_RECORD_BYTES)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree record length {}", length);
    if (length > bytes.size())
        return incomplete();

    const auto frame = bytes.substr(0, length);
    auto crc_field = frame.substr(length - 4);
    if (takeLE(crc_field, 4) != checksum(frame.substr(0, length - 4)))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree record checksum mismatch");

    DecodedRecord result;
    fields = frame.substr(PREFIX_BYTES, length - PREFIX_BYTES - 4);
    auto & record = result.record;
    record.kind = kind;
    for (auto & limb : record.incarnation)
        limb = takeLE(fields, 8);
    record.csn = takeLE(fields, 8);
    for (auto & limb : record.previous_hash)
        limb = takeLE(fields, 8);
    for (auto & limb : record.operation_id)
        limb = takeLE(fields, 8);
    const auto partition_length = takeLE(fields, 4);
    if (!partition_length || partition_length > MAX_PARTITION_ID_BYTES || partition_length > fields.size())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree partition identity length");
    record.partition_id = fields.substr(0, partition_length);
    fields.remove_prefix(partition_length);
    record.payload = fields;
    validate(record);
    result.hash = hashLogRecord(frame);
    result.bytes_consumed = length;
    return result;
}

LogReplayCursor::LogReplayCursor(ObjectID incarnation_, String partition_id_, UInt64 boundary_csn_, ObjectID boundary_hash_)
    : incarnation(incarnation_), partition_id(std::move(partition_id_)), csn(boundary_csn_), hash(boundary_hash_)
{
    if (incarnation == ObjectID{} || partition_id.empty() || partition_id.size() > MAX_PARTITION_ID_BYTES
        || ((csn == 0) != (hash == ObjectID{})))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid UpsertMergeTree replay boundary");
}

void LogReplayCursor::accept(const DecodedRecord & decoded)
{
    const auto & record = decoded.record;
    if (record.incarnation != incarnation || record.partition_id != partition_id || record.csn <= csn || record.previous_hash != hash)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree log identity, CSN or hash chain mismatch");
    csn = record.csn;
    hash = decoded.hash;
}

UInt64 LogReplayCursor::nextCSN() const
{
    if (csn == std::numeric_limits<UInt64>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "UpsertMergeTree commit sequence exhausted");
    return csn + 1;
}
}
}
