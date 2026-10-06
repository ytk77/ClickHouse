#include <Storages/MergeTree/UniqueKey/UpsertLogRecord.h>
#include <Storages/MergeTree/UniqueKey/UpsertRowMetadata.h>
#include <Storages/MergeTree/UniqueKey/UpsertPartData.h>
#include <Storages/MergeTree/UniqueKey/UpsertTableManifest.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Storages/MergeTree/UniqueKey/UpsertStateStore.h>
#include <Storages/MergeTree/UniqueKey/UpsertMaintenanceMemory.h>
#include <Storages/MergeTree/UniqueKey/UpsertOperationWait.h>
#include <Storages/MergeTree/UniqueKey/UpsertInsertQueue.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/Exception.h>
#include <Common/scope_guard_safe.h>
#include <gtest/gtest.h>
#include <limits>
#include <filesystem>
#include <string>

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
}

using namespace DB;
using namespace DB::Upsert;

TEST(UpsertInsertQueue, OversizedPreparationRunsAlone)
{
    constexpr size_t oversized = 512ULL << 20;
    EXPECT_FALSE(insertPreparationNeedsDrain(0, 0, oversized, 4));
    EXPECT_TRUE(insertPreparationNeedsDrain(1, oversized, 1, 4));
    EXPECT_TRUE(insertPreparationNeedsDrain(1, oversized, oversized, 4));
    EXPECT_TRUE(insertPreparationNeedsDrain(1, std::numeric_limits<size_t>::max(), 1, 4));
}

TEST(UpsertInsertQueue, EnforcesByteAndThreadBounds)
{
    constexpr size_t half_budget = MAX_PENDING_INSERT_BYTES / 2;
    EXPECT_FALSE(insertPreparationNeedsDrain(1, half_budget, half_budget, 4));
    EXPECT_TRUE(insertPreparationNeedsDrain(1, half_budget, half_budget + 1, 4));
    EXPECT_TRUE(insertPreparationNeedsDrain(1, MAX_PENDING_INSERT_BYTES, 1, 4));
    EXPECT_TRUE(insertPreparationNeedsDrain(4, 4, 1, 4));
    EXPECT_FALSE(insertPreparationNeedsDrain(3, 3, 1, 4));
}

TEST(UpsertInsertSettings, DefaultAndHistoricalRetryBoundary)
{
    const MergeTreeSettings current;
    EXPECT_EQ(current.get("upsert_insert_target_bytes").safeGet<UInt64>(), 512ULL << 20);

    MergeTreeSettings legacy;
    legacy.applyCompatibilitySetting("26.8");
    EXPECT_EQ(legacy.get("upsert_insert_target_bytes").safeGet<UInt64>(), 32ULL << 20);

    MergeTreeSettings new_version;
    new_version.applyCompatibilitySetting("26.8.1.1");
    EXPECT_EQ(new_version.get("upsert_insert_target_bytes").safeGet<UInt64>(), 512ULL << 20);

    /// Explicit table or server settings are applied after compatibility defaults.
    legacy.set("upsert_insert_target_bytes", UInt64{1ULL << 30});
    EXPECT_EQ(legacy.get("upsert_insert_target_bytes").safeGet<UInt64>(), 1ULL << 30);
}

TEST(UpsertMaintenanceMemoryTest, UsesConfiguredLimitsIndependentlyOfCurrentUsage)
{
    EXPECT_EQ(maintenanceMemoryCeiling(0, 0), std::numeric_limits<UInt64>::max());
    EXPECT_EQ(maintenanceMemoryCeiling(-1, 1ULL << 30), 1ULL << 30);
    EXPECT_EQ(maintenanceMemoryCeiling(2ULL << 30, 1ULL << 30), 1ULL << 30);
    EXPECT_EQ(maintenanceEffectiveTarget(4ULL << 30, std::numeric_limits<UInt64>::max()), 4ULL << 30);
    EXPECT_EQ(maintenanceEffectiveTarget(4ULL << 30, 32ULL << 30), 2ULL << 30);
    EXPECT_EQ(maintenanceEffectiveTarget(4ULL << 30, 1ULL << 30), 64ULL << 20);
    EXPECT_EQ(maintenanceEffectiveTarget(4ULL << 30, 128ULL << 20), 32ULL << 20);
    EXPECT_EQ(maintenanceEffectiveTarget(32ULL << 20, std::numeric_limits<UInt64>::max()), 32ULL << 20);
}

TEST(UpsertMaintenanceMemoryTest, ForegroundWaitCountsOnlyPendingAcquisitions)
{
    std::atomic<UInt64> waiting{0};
    const auto before = CurrentMetrics::get(CurrentMetrics::UpsertForegroundOperationWaiters);
    {
        ForegroundOperationWait first(waiting);
        EXPECT_EQ(waiting.load(), 1);
        EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::UpsertForegroundOperationWaiters), before + 1);
        {
            ForegroundOperationWait second(waiting);
            EXPECT_EQ(waiting.load(), 2);
            EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::UpsertForegroundOperationWaiters), before + 2);
            first.acquired();
            first.acquired();
            EXPECT_EQ(waiting.load(), 1);
        }
        EXPECT_EQ(waiting.load(), 0);
    }
    EXPECT_EQ(waiting.load(), 0);
    ForegroundOperationWait disabled(waiting, false);
    EXPECT_EQ(waiting.load(), 0);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::UpsertForegroundOperationWaiters), before);
}

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

TEST(UpsertPartRows, LargeMaintenanceLineageExceedsForegroundRowAndByteBudgets)
{
    PartRows rows;
    rows.reserve(MAX_UNIT_ROWS + 1);
    for (UInt32 offset = 0; offset <= MAX_UNIT_ROWS; ++offset)
        rows.push_back({std::to_string(offset), {offset, {.operation_id = {1, 2}, .block_ordinal = offset, .write_csn = 1}}});
    auto bytes = encodePartRows(rows);
    EXPECT_EQ(bytes.substr(0, 8), "UMPROW02");
    auto decoded = decodePartRows(bytes);
    ASSERT_EQ(decoded.size(), rows.size());
    EXPECT_EQ(decoded.front(), rows.front());
    EXPECT_EQ(decoded.back(), rows.back());
    bytes.back() ^= 1;
    EXPECT_THROW(decodePartRows(bytes), Exception);
    rows.clear();
    decoded.clear();
    for (UInt32 offset = 0; offset < 3; ++offset)
        rows.push_back({String(23U << 20, static_cast<char>('a' + offset)),
            {offset, {.operation_id = {1, 2}, .block_ordinal = offset, .write_csn = 1}}});
    bytes = encodePartRows(rows);
    ASSERT_GT(bytes.size(), MAX_RECORD_BYTES);
    EXPECT_EQ(bytes.substr(0, 8), "UMPROW02");
    EXPECT_EQ(decodePartRows(bytes), rows);
}

TEST(UpsertPartRows, SmallLineageAndLargeAuthorizationHaveDistinctVersions)
{
    PartRows rows{{"key", {0, {.operation_id = {1, 2}, .write_csn = 1}}}};
    EXPECT_EQ(encodePartRows(rows).substr(0, 8), "UMPROW01");
    rows[0].key.assign(MAX_UNIT_BYTES + 1, 'a');
    EXPECT_THROW(encodePartRows(rows), Exception);
    PartCommit part;
    part.part = {1, 2};
    part.temporary_name = "tmp_upsert_test";
    part.final_name = "p_1_1_0";
    part.files_hash = {3, 4};
    part.rows = MAX_UNIT_ROWS + 1;
    part.token.accepted = part.rows;
    EXPECT_THROW(decodePartCommit(encodePartCommit(part)), Exception);
    part.chunked = true;
    EXPECT_TRUE(decodePartCommit(encodePartCommit(part)).chunked);
    part.rows = MAX_PART_ROWS + 1;
    part.token.accepted = part.rows;
    EXPECT_THROW(decodePartCommit(encodePartCommit(part)), Exception);
}

class UpsertTTLManifestTest : public ::testing::Test
{
protected:
    std::filesystem::path directory;
    StorageInMemoryMetadata metadata;

    void SetUp() override
    {
        directory = std::filesystem::path("tmp") / ("upsert_ttl_manifest_" + objectName(newObjectID()));
        std::filesystem::create_directories(directory);
        ColumnsDescription columns;
        columns.add(ColumnDescription("id", std::make_shared<DataTypeUInt64>()));
        metadata.setColumns(columns);
    }
    void TearDown() override
    {
        std::filesystem::remove_all(directory);
    }
    ObjectID initialize(bool attach, bool & disk_state)
    {
        return initializeTableManifest(directory, "default", directory, metadata, "", attach, disk_state);
    }
    void makeLegacyManifest(bool disk_state, bool with_ttl)
    {
        bool protocol;
        initialize(false, protocol);
        const auto path = directory / "upsert_manifest.bin";
        auto record = decodeLogRecord(readBoundedFile(path, MAX_RECORD_BYTES), false)->record;
        const String marker = "ttl_policy=metadata_v1\n";
        record.payload.erase(record.payload.find(marker), marker.size());
        if (!disk_state)
            record.payload.replace(record.payload.find("disk_state_v2"), String("disk_state_v2").size(), "parts_sync_v1");
        if (with_ttl)
        {
            const auto ttl = metadata.table_ttl.definition_ast->formatWithSecretsOneLine();
            record.payload += std::to_string(ttl.size()) + ":" + ttl;
        }
        std::filesystem::remove(path);
        createDurableFile(path, encodeLogRecord(record));
    }
};

TEST_F(UpsertTTLManifestTest, MutablePolicyKeepsStructuralAuthentication)
{
    bool disk_state;
    const auto incarnation = initialize(false, disk_state);
    metadata.table_ttl.definition_ast = make_intrusive<ASTIdentifier>("expires");
    EXPECT_EQ(initialize(true, disk_state), incarnation);
    EXPECT_TRUE(disk_state);
    metadata.table_ttl = {};
    EXPECT_EQ(initialize(true, disk_state), incarnation);
    auto columns = metadata.getColumns();
    columns.add(ColumnDescription("extra", std::make_shared<DataTypeUInt64>()));
    metadata.setColumns(columns);
    EXPECT_THROW(initialize(true, disk_state), Exception);
}

TEST_F(UpsertTTLManifestTest, UpgradeOldManifestWithoutTTLIsIdempotent)
{
    makeLegacyManifest(true, false);
    bool disk_state;
    const auto incarnation = initialize(true, disk_state);
    enableMutableTTL(directory, "default", metadata, "");
    EXPECT_EQ(initialize(true, disk_state), incarnation);
    enableMutableTTL(directory, "default", metadata, "");
    metadata.table_ttl.definition_ast = make_intrusive<ASTIdentifier>("expires");
    EXPECT_EQ(initialize(true, disk_state), incarnation);
}

TEST_F(UpsertTTLManifestTest, UpgradeOldFixedTTLManifestPreservesIncarnation)
{
    metadata.table_ttl.definition_ast = make_intrusive<ASTIdentifier>("expires");
    makeLegacyManifest(true, true);
    bool disk_state;
    const auto incarnation = initialize(true, disk_state);
    enableMutableTTL(directory, "default", metadata, "");
    metadata.table_ttl = {};
    EXPECT_EQ(initialize(true, disk_state), incarnation);
}

TEST_F(UpsertTTLManifestTest, MismatchingSchemaAndLegacyProtocolCannotUpgrade)
{
    makeLegacyManifest(false, false);
    bool disk_state;
    initialize(true, disk_state);
    EXPECT_FALSE(disk_state);
    EXPECT_THROW(enableMutableTTL(directory, "default", metadata, ""), Exception);
    EXPECT_THROW(enableMutableTTL(directory, "other_disk", metadata, ""), Exception);
}

TEST_F(UpsertTTLManifestTest, MutableSchemaPreservesKeysAndDisk)
{
    metadata.unique_key.expression_list_ast = make_intrusive<ASTIdentifier>("id");
    metadata.unique_key.data_types = {std::make_shared<DataTypeUInt64>()};
    bool disk_state;
    const auto incarnation = initialize(false, disk_state);
    enableMutableSchema(directory, "default", metadata, "");
    auto columns = metadata.getColumns();
    columns.add(ColumnDescription("extra", std::make_shared<DataTypeUInt64>()));
    metadata.setColumns(columns);
    EXPECT_EQ(initialize(true, disk_state), incarnation);
    enableMutableSchema(directory, "default", metadata, "");
    enableMutableTTL(directory, "default", metadata, "");
    columns.remove("extra");
    metadata.setColumns(columns);
    EXPECT_EQ(initialize(true, disk_state), incarnation);
    EXPECT_THROW(enableMutableSchema(directory, "other_disk", metadata, ""), Exception);
    metadata.unique_key.expression_list_ast = make_intrusive<ASTIdentifier>("other_key");
    EXPECT_THROW(initialize(true, disk_state), Exception);
}

TEST_F(UpsertTTLManifestTest, MutableSchemaUpgradesFixedTTLManifest)
{
    metadata.table_ttl.definition_ast = make_intrusive<ASTIdentifier>("expires");
    makeLegacyManifest(true, true);
    bool disk_state;
    const auto incarnation = initialize(true, disk_state);
    enableMutableSchema(directory, "default", metadata, "");
    metadata.table_ttl = {};
    EXPECT_EQ(initialize(true, disk_state), incarnation);
}

TEST(UpsertPartInventory, AuthenticatesMetadataAndDefersOnlyPayloadContents)
{
    const auto directory = std::filesystem::path("tmp") / ("upsert_inventory_" + objectName(newObjectID()));
    std::filesystem::create_directories(directory);
    SCOPE_EXIT({ std::filesystem::remove_all(directory); });
    createDurableFile(directory / "data.bin", "payload");
    createDurableFile(directory / "count.txt", "1234567");
    String inventory;
    const auto digest = hashPartDirectory(directory, true, &inventory);
    EXPECT_NO_THROW(verifyPartMetadata(directory, digest, inventory));
    std::filesystem::remove(directory / "data.bin");
    createDurableFile(directory / "data.bin", "changed");
    EXPECT_NO_THROW(verifyPartMetadata(directory, digest, inventory));
    EXPECT_NE(hashPartDirectory(directory, false), digest);
    std::filesystem::remove(directory / "count.txt");
    createDurableFile(directory / "count.txt", "7654321");
    EXPECT_THROW(verifyPartMetadata(directory, digest, inventory), Exception);
}

TEST(UpsertPartInventory, RejectsMissingTruncatedAndAdditionalFiles)
{
    const auto directory = std::filesystem::path("tmp") / ("upsert_inventory_" + objectName(newObjectID()));
    std::filesystem::create_directories(directory);
    SCOPE_EXIT({ std::filesystem::remove_all(directory); });
    createDurableFile(directory / "data.bin", "payload");
    String inventory;
    const auto digest = hashPartDirectory(directory, true, &inventory);
    std::filesystem::remove(directory / "data.bin");
    EXPECT_THROW(verifyPartMetadata(directory, digest, inventory), Exception);
    createDurableFile(directory / "data.bin", "short");
    EXPECT_THROW(verifyPartMetadata(directory, digest, inventory), Exception);
    std::filesystem::remove(directory / "data.bin");
    createDurableFile(directory / "data.bin", "payload");
    createDurableFile(directory / "extra.bin", "payload");
    EXPECT_THROW(verifyPartMetadata(directory, digest, inventory), Exception);
    std::filesystem::remove(directory / "extra.bin");
    EXPECT_THROW(verifyPartMetadata(directory, digest, inventory + "x"), Exception);
}

TEST(UpsertPartInventory, SidecarReadMatchesCommittedBytesAndRequiresInventoryEntry)
{
    const auto directory = std::filesystem::path("tmp") / ("upsert_inventory_" + objectName(newObjectID()));
    std::filesystem::create_directories(directory);
    SCOPE_EXIT({ std::filesystem::remove_all(directory); });
    createDurableFile(directory / ROWS_FILE, "original");
    String inventory;
    hashPartDirectory(directory, true, &inventory);
    EXPECT_EQ(readInventoryFile(directory, ROWS_FILE, inventory, MAX_RECORD_BYTES), "original");
    try
    {
        readInventoryFile(directory, "missing.bin", inventory, MAX_RECORD_BYTES);
        FAIL() << "Expected missing inventory entry rejection";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::CORRUPTED_DATA);
        EXPECT_NE(error.message().find("inventory lacks sidecar"), String::npos);
    }
    EXPECT_THROW(readInventoryFile(directory, ROWS_FILE, "", MAX_RECORD_BYTES), Exception);
    std::filesystem::remove(directory / ROWS_FILE);
    createDurableFile(directory / ROWS_FILE, "modified");
    try
    {
        readInventoryFile(directory, ROWS_FILE, inventory, MAX_RECORD_BYTES);
        FAIL() << "Expected committed sidecar hash rejection";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::CORRUPTED_DATA);
        EXPECT_NE(error.message().find("authenticated sidecar mismatch"), String::npos);
    }
}
