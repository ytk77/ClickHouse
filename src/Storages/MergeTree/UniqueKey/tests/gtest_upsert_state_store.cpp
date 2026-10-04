#include <Storages/MergeTree/UniqueKey/UpsertStateStore.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <gtest/gtest.h>
#include <filesystem>

using namespace DB;
using namespace DB::Upsert;

class UpsertStateStoreTest : public ::testing::Test
{
protected:
    std::filesystem::path directory;
    const ObjectID incarnation{71, 93};
    UInt64 next_part = 1;
    void SetUp() override
    {
        directory = std::filesystem::path("tmp") / ("upsert_state_" + objectName(newObjectID()));
        std::filesystem::create_directories(directory);
    }
    void TearDown() override
    {
        std::filesystem::remove_all(directory);
    }
    StateStore::Prepared prepare(StateStore & store, const std::vector<std::pair<String, UInt64>> & values, Token token = {}, const String & partition = "p")
    {
        const ObjectID op = newObjectID();
        PartRows rows;
        for (size_t offset = 0; offset < values.size(); ++offset)
            rows.push_back({values[offset].first, {static_cast<UInt32>(offset),
                {.operation_id = op, .block_ordinal = offset, .version = values[offset].second, .self_binding = true}}});
        const auto number = next_part++;
        PartCommit part;
        part.part = op;
        part.temporary_name = "tmp_upsert_" + objectName(op);
        part.final_name = partition + "_" + std::to_string(number) + "_" + std::to_string(number) + "_0";
        part.rows = rows.size();
        part.token = std::move(token);
        const auto path = directory / part.temporary_name;
        std::filesystem::create_directory(path);
        createDurableFile(path / ROWS_FILE, encodePartRows(rows));
        part.files_hash = hashPartDirectory(path, true, &part.files_inventory);
        return store.prepare(partition, std::move(part), rows);
    }
    void commit(StateStore & store, StateStore::Prepared & prepared)
    {
        store.append(prepared);
        if (prepared.produces_part)
        {
            std::filesystem::rename(directory / prepared.record.temporary_name, directory / prepared.record.final_name);
            syncDirectory(directory);
        }
        store.publish(prepared);
    }
};

TEST_F(UpsertStateStoreTest, IncomingLosersAndIncumbentKillsSurviveReplay)
{
    String original;
    String mixed;
    {
        StateStore store(directory, incarnation, false);
        auto first = prepare(store, {{"key", 10}});
        original = objectName(first.record.part);
        commit(store, first);
        const auto old_snapshot = store.snapshot();
        auto lower_and_new = prepare(store, {{"key", 9}, {"other", 1}});
        mixed = objectName(lower_and_new.record.part);
        EXPECT_EQ(lower_and_new.record.dead, (std::vector<UInt32>{0}));
        EXPECT_TRUE(lower_and_new.record.kills.empty());
        commit(store, lower_and_new);
        auto higher = prepare(store, {{"key", 11}});
        EXPECT_EQ(higher.record.kills.size(), 1);
        commit(store, higher);
        EXPECT_FALSE(old_snapshot->part_masks.at(original)->contains(0));
        EXPECT_TRUE(store.snapshot()->part_masks.at(original)->contains(0));
        EXPECT_TRUE(store.snapshot()->part_masks.at(mixed)->contains(0));
        EXPECT_FALSE(store.snapshot()->part_masks.at(mixed)->contains(1));
    }
    StateStore reopened(directory, incarnation, true);
    EXPECT_EQ(reopened.authorizedParts().size(), 3);
    EXPECT_EQ(reopened.snapshot()->partition_csns.at("p"), 3);
    EXPECT_TRUE(reopened.snapshot()->part_masks.at(original)->contains(0));
    EXPECT_TRUE(reopened.snapshot()->part_masks.at(mixed)->contains(0));
    EXPECT_FALSE(reopened.snapshot()->part_masks.at(mixed)->contains(1));
}

TEST_F(UpsertStateStoreTest, NoopTokensRetainDecisionsAndRejectChangedPayload)
{
    Token token{"retry", 0, {2, 3}, 0};
    {
        StateStore store(directory, incarnation, false);
        auto first = prepare(store, {{"key", 10}});
        commit(store, first);
        auto lower = prepare(store, {{"key", 9}}, token);
        EXPECT_FALSE(lower.produces_part);
        commit(store, lower);
        EXPECT_TRUE(store.tokenAlreadyCommitted("p", token));
        token.payload_hash = {4, 5};
        EXPECT_THROW(store.tokenAlreadyCommitted("p", token), Exception);
        EXPECT_FALSE(store.isFenced());
    }
    StateStore reopened(directory, incarnation, true);
    EXPECT_EQ(reopened.authorizedParts().size(), 1);
    EXPECT_EQ(reopened.snapshot()->partition_csns.at("p"), 2);
    token.payload_hash = {2, 3};
    EXPECT_TRUE(reopened.tokenAlreadyCommitted("p", token));
}

TEST_F(UpsertStateStoreTest, DurableAuthorizationRollsForwardTemporaryPayload)
{
    String final_name;
    {
        StateStore store(directory, incarnation, false);
        auto prepared = prepare(store, {{"key", 5}});
        final_name = prepared.record.final_name;
        store.append(prepared);
        EXPECT_TRUE(store.snapshot()->part_masks.empty());
        EXPECT_FALSE(std::filesystem::exists(directory / final_name));
    }
    StateStore reopened(directory, incarnation, true);
    EXPECT_TRUE(std::filesystem::exists(directory / final_name));
    EXPECT_EQ(reopened.authorizedParts().size(), 1);
    EXPECT_EQ(reopened.snapshot()->part_masks.size(), 1);
}

TEST_F(UpsertStateStoreTest, CorruptionAndUnauthorizedCoveringPartFailBeforeNativeLoading)
{
    String final_name;
    {
        StateStore store(directory, incarnation, false);
        auto prepared = prepare(store, {{"key", 5}});
        final_name = prepared.record.final_name;
        commit(store, prepared);
    }
    const auto unauthorized = directory / "p_0_100_1";
    std::filesystem::create_directory(unauthorized);
    EXPECT_THROW((StateStore(directory, incarnation, true)), Exception);
    EXPECT_TRUE(std::filesystem::exists(directory / final_name / ROWS_FILE));
    std::filesystem::remove(unauthorized);
    createDurableFile(directory / final_name / "foreign_file", "not authorized");
    EXPECT_THROW((StateStore(directory, incarnation, true)), Exception);
}

TEST_F(UpsertStateStoreTest, MissingRegistryIsNotBootstrap)
{
    {
        StateStore store(directory, incarnation, false);
    }
    std::filesystem::remove(directory / "upsert_state/registry.log");
    EXPECT_THROW((StateStore(directory, incarnation, true)), Exception);
}

TEST(UpsertPartRows, ExactRoundTripAndCorruptionDetection)
{
    PartRows rows{{"a", {0, {.operation_id = {1,2}, .block_ordinal = 9, .version = 0, .self_binding = true}}},
                  {"b", {1, {.operation_id = {1,2}, .block_ordinal = 4, .version = 8, .self_binding = true}}}};
    const auto encoded = encodePartRows(rows);
    EXPECT_EQ(decodePartRows(encoded), rows);
    for (size_t offset = 0; offset < encoded.size(); ++offset)
    {
        auto corrupt = encoded;
        corrupt[offset] ^= 1;
        EXPECT_THROW(decodePartRows(corrupt), Exception) << offset;
    }
}

TEST_F(UpsertStateStoreTest, ExpandedPartitionRegistryReplaysAndStillEnforcesAdmission)
{
    {
        StateStore store(directory, incarnation, false);
        for (size_t partition = 0; partition < StateStore::MAX_PARTITIONS; ++partition)
        {
            auto prepared = prepare(store, {{"key", 1}}, {}, "p" + std::to_string(partition));
            commit(store, prepared);
        }
        EXPECT_THROW(prepare(store, {{"key", 2}}, {}, "overflow"), Exception);
        EXPECT_FALSE(store.isFenced());
    }
    StateStore reopened(directory, incarnation, true);
    EXPECT_EQ(reopened.authorizedParts().size(), StateStore::MAX_PARTITIONS);
    EXPECT_EQ(reopened.snapshot()->partition_csns.size(), StateStore::MAX_PARTITIONS);
    auto update = prepare(reopened, {{"key", 2}}, {}, "p0");
    commit(reopened, update);
    EXPECT_EQ(reopened.snapshot()->partition_csns.at("p0"), 2);
}

TEST_F(UpsertStateStoreTest, ExpandedPartHistoryPreservesMasksAcrossReplay)
{
    constexpr size_t history = 1025;
    String first;
    String last;
    {
        StateStore store(directory, incarnation, false);
        for (size_t version = 0; version < history; ++version)
        {
            auto prepared = prepare(store, {{"key", version}});
            last = objectName(prepared.record.part);
            if (!version)
                first = last;
            commit(store, prepared);
        }
    }
    StateStore reopened(directory, incarnation, true);
    EXPECT_EQ(reopened.authorizedParts().size(), history);
    EXPECT_EQ(reopened.snapshot()->partition_csns.at("p"), history);
    EXPECT_TRUE(reopened.snapshot()->part_masks.at(first)->contains(0));
    EXPECT_FALSE(reopened.snapshot()->part_masks.at(last)->contains(0));
}
