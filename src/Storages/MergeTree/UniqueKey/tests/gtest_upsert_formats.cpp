#include <Storages/MergeTree/UniqueKey/UpsertLogRecord.h>
#include <Storages/MergeTree/UniqueKey/UpsertRowMetadata.h>
#include <Storages/MergeTree/UniqueKey/UpsertPartData.h>
#include <Common/Exception.h>
#include <gtest/gtest.h>
#include <limits>

using namespace DB;
using namespace DB::Upsert;

namespace
{
LogRecord makeRecord()
{
    return {.incarnation = {1, 2}, .partition_id = "p", .csn = 1, .operation_id = {3, 4},
        .kind = RecordKind::Part, .payload = String("a\0b", 3)};
}

String fromHex(std::string_view hex)
{
    String result;
    auto digit = [](char c) -> unsigned
    {
        return c <= '9' ? c - '0' : c - 'a' + 10;
    };
    for (size_t i = 0; i < hex.size(); i += 2)
        result.push_back(static_cast<char>(digit(hex[i]) * 16 + digit(hex[i + 1])));
    return result;
}
}

TEST(UpsertRowMetadata, StableLittleEndianFixture)
{
    IndexValue value{.row_offset = 0x1234,
        .row = {.operation_id = {1, 2}, .block_ordinal = 3, .write_csn = 4, .version = 5}};
    const auto fixture = fromHex(
        "554d525601013412000001000000000000000200000000000000030000000000000004000000000000000500000000000000");
    EXPECT_EQ(encodeIndexValue(value), fixture);
    EXPECT_EQ(decodeIndexValue(fixture), value);
}

TEST(UpsertRowMetadata, WinnerOrder)
{
    RowMetadata old{.operation_id = {1, 1}, .block_ordinal = 20, .write_csn = 2, .version = 100};
    auto incoming = old;
    incoming.write_csn = 3;
    incoming.version = 99;
    EXPECT_TRUE(incoming.compareWinner(old) < 0);
    incoming.version = 100;
    EXPECT_TRUE(incoming.compareWinner(old) > 0);
    incoming.write_csn = 2;
    incoming.block_ordinal = 21;
    EXPECT_TRUE(incoming.compareWinner(old) > 0);
    old.version.reset();
    incoming.version.reset();
    EXPECT_TRUE(incoming.compareWinner(old) > 0);
    incoming.write_csn = 1;
    EXPECT_TRUE(incoming.compareWinner(old) < 0);
}

TEST(UpsertRowMetadata, SelfBindingAndRewritesPreserveOriginalCSN)
{
    RowMetadata prepared{.operation_id = {4, 5}, .block_ordinal = 7, .write_csn = 0, .version = 9, .self_binding = true};
    EXPECT_THROW(prepared.compareWinner(prepared), Exception);
    EXPECT_THROW(prepared.resolve(0), Exception);
    const auto committed = prepared.resolve(11);
    EXPECT_EQ(committed.write_csn, 11);
    EXPECT_FALSE(committed.self_binding);
    EXPECT_EQ(committed.resolve(42), committed);
    EXPECT_EQ(decodeIndexValue(encodeIndexValue({12, committed})).row, committed);
    EXPECT_EQ(decodeIndexValue(encodeIndexValue({13, prepared})).row, prepared);
}

TEST(UpsertRowMetadata, DeleteReinsertHasDistinctIdentity)
{
    RowMetadata before{.operation_id = {1, 1}, .block_ordinal = 0, .write_csn = 1, .version = 10};
    auto after = before;
    after.operation_id = {1, 2};
    after.write_csn = 3;
    EXPECT_NE(before, after);
    EXPECT_TRUE(after.compareWinner(before) > 0);
}

TEST(UpsertRowMetadata, RejectLegacyTruncationAndUnknownFields)
{
    EXPECT_THROW(decodeIndexValue(String(4, '\0')), Exception);
    auto bytes = encodeIndexValue({0, {.operation_id = {1, 1}, .block_ordinal = 0, .write_csn = 1}});
    for (size_t length = 0; length < bytes.size(); ++length)
        EXPECT_THROW(decodeIndexValue(std::string_view(bytes).substr(0, length)), Exception) << length;
    EXPECT_THROW(decodeIndexValue(bytes + "x"), Exception);
    auto bad_version = bytes;
    bad_version[4] = 2;
    EXPECT_THROW(decodeIndexValue(bad_version), Exception);
    auto bad_flags = bytes;
    bad_flags[5] = 4;
    EXPECT_THROW(decodeIndexValue(bad_flags), Exception);
    auto no_identity = decodeIndexValue(bytes);
    no_identity.row.operation_id = {};
    EXPECT_THROW(encodeIndexValue(no_identity), Exception);
    auto incompatible = decodeIndexValue(bytes).row;
    incompatible.version = 0;
    EXPECT_THROW(incompatible.compareWinner(decodeIndexValue(bytes).row), Exception);
}

TEST(UpsertRowMetadata, BoundPartOffsets)
{
    IndexValue value{MAX_PART_ROWS - 1, {.operation_id = {1, 1}, .block_ordinal = 0, .write_csn = 1}};
    EXPECT_EQ(decodeIndexValue(encodeIndexValue(value)), value);
    value.row_offset = MAX_PART_ROWS;
    EXPECT_THROW(encodeIndexValue(value), Exception);
}

TEST(UpsertLogRecord, RoundTripAllKindsAndBinaryPayload)
{
    auto record = makeRecord();
    for (UInt8 kind = 1; kind <= 9; ++kind)
    {
        record.kind = static_cast<RecordKind>(kind);
        auto frame = encodeLogRecord(record);
        auto decoded = decodeLogRecord(frame + "next", false);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->record, record);
        EXPECT_EQ(decoded->bytes_consumed, frame.size());
        EXPECT_EQ(decoded->hash, hashLogRecord(frame));
        EXPECT_EQ(frame.size(), 88);
    }
}

TEST(UpsertLogRecord, StableEnvelopeFixture)
{
    const auto fixture = fromHex(
        "554d544c4f4730310100040058000000012c61c4010000000000000002000000000000000100000000000000"
        "00000000000000000000000000000000030000000000000004000000000000000100000070610062846c6a58");
    EXPECT_EQ(encodeLogRecord(makeRecord()), fixture);
    EXPECT_EQ(decodeLogRecord(fixture, false)->record, makeRecord());
}

TEST(UpsertLogRecord, EveryTruncatedSuffix)
{
    const auto frame = encodeLogRecord(makeRecord());
    for (size_t length = 0; length < frame.size(); ++length)
    {
        const auto prefix = std::string_view(frame).substr(0, length);
        EXPECT_FALSE(decodeLogRecord(prefix, true)) << length;
        EXPECT_THROW(decodeLogRecord(prefix, false), Exception) << length;
    }
}

TEST(UpsertLogRecord, RejectCorruptionEvenInUnsealedTail)
{
    const auto frame = encodeLogRecord(makeRecord());
    for (size_t byte = 0; byte < frame.size(); ++byte)
    {
        auto damaged = frame;
        damaged[byte] ^= 0x40;
        EXPECT_THROW(decodeLogRecord(damaged, false), Exception) << byte;
        EXPECT_THROW(decodeLogRecord(damaged, true), Exception) << byte;
    }
}

TEST(UpsertLogRecord, BoundLengthBeforeAllocation)
{
    auto frame = encodeLogRecord(makeRecord());
    for (size_t i = 12; i < 16; ++i)
        frame[i] = '\xff';
    EXPECT_THROW(decodeLogRecord(frame, true), Exception);
    auto record = makeRecord();
    record.partition_id.assign(MAX_PARTITION_ID_BYTES + 1, 'p');
    EXPECT_THROW(encodeLogRecord(record), Exception);
    record = makeRecord();
    record.kind = static_cast<RecordKind>(10);
    EXPECT_THROW(encodeLogRecord(record), Exception);
}

TEST(UpsertLogRecord, ChainAndRootIdentity)
{
    auto record = makeRecord();
    LogReplayCursor cursor(record.incarnation, record.partition_id);
    auto first = *decodeLogRecord(encodeLogRecord(record), false);
    cursor.accept(first);
    EXPECT_EQ(cursor.nextCSN(), 2);
    EXPECT_THROW(cursor.accept(first), Exception);
    record.csn = 2;
    EXPECT_THROW(cursor.accept(*decodeLogRecord(encodeLogRecord(record), false)), Exception);
    record.previous_hash = first.hash;
    record.operation_id = {3, 5};
    auto second = *decodeLogRecord(encodeLogRecord(record), false);
    auto other_partition = second;
    other_partition.record.partition_id = "q";
    EXPECT_THROW(cursor.accept(other_partition), Exception);
    auto other_table = second;
    other_table.record.incarnation = {8, 9};
    EXPECT_THROW(cursor.accept(other_table), Exception);
    cursor.accept(second);
    EXPECT_EQ(cursor.publishedCSN(), 2);

    LogReplayCursor checkpoint_cursor(record.incarnation, record.partition_id, first.record.csn, first.hash);
    checkpoint_cursor.accept(second);
    EXPECT_EQ(checkpoint_cursor.tailHash(), cursor.tailHash());
}

TEST(UpsertLogRecord, SequenceExhaustionAndInvalidBoundaries)
{
    LogReplayCursor exhausted({1, 2}, "p", std::numeric_limits<UInt64>::max(), {3, 4});
    EXPECT_THROW(exhausted.nextCSN(), Exception);
    EXPECT_THROW((LogReplayCursor({}, "p")), Exception);
    EXPECT_THROW((LogReplayCursor({1, 2}, "p", 1, {})), Exception);
    EXPECT_THROW((LogReplayCursor({1, 2}, "p", 0, {1, 1})), Exception);
}

TEST(UpsertPartRows, PrevalidationRejectsInvalidImmutableInput)
{
    const ObjectID id{41, 17};
    PartRows rows{
        {"b", {1, {.operation_id = id, .block_ordinal = 0, .self_binding = true}}},
        {"a", {0, {.operation_id = id, .block_ordinal = 1, .self_binding = true}}}};
    ValidatedPartRows valid(rows, id);
    EXPECT_EQ(valid.getRows(), rows);
    EXPECT_EQ(valid.getPart(), id);
    EXPECT_THROW((ValidatedPartRows(rows, ObjectID{1, 2})), Exception);
    EXPECT_THROW((ValidatedPartRows({}, id)), Exception);
    auto bad = rows;
    bad[1].key = bad[0].key;
    EXPECT_THROW((ValidatedPartRows(bad, id)), Exception);
    bad = rows;
    bad[1].value.row_offset = bad[0].value.row_offset;
    EXPECT_THROW((ValidatedPartRows(bad, id)), Exception);
    bad[1].value.row_offset = 2;
    EXPECT_THROW((ValidatedPartRows(bad, id)), Exception);
    bad = rows;
    bad[1].value.row.self_binding = false;
    EXPECT_THROW((ValidatedPartRows(bad, id)), Exception);
    bad = rows;
    bad[1].value.row.write_csn = 1;
    EXPECT_THROW((ValidatedPartRows(bad, id)), Exception);
}
