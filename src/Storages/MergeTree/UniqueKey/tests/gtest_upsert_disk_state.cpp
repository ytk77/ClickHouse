#include <Storages/MergeTree/UniqueKey/UpsertStateStore.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include "config.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>

#if USE_ROCKSDB
#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
using namespace DB;
using namespace DB::Upsert;

class UpsertDiskStateTest : public ::testing::Test
{
protected:
    std::filesystem::path directory;
    ObjectID incarnation{83, 108};
    UInt64 next_part = 1;

    void SetUp() override
    {
        directory = std::filesystem::path("tmp") / ("upsert_disk_" + objectName(newObjectID()));
        std::filesystem::create_directories(directory);
    }
    void TearDown() override
    {
        std::filesystem::remove_all(directory);
    }
    StateStore::Prepared prepare(StateStore & store, const std::vector<std::pair<String, UInt64>> & values,
        const String & partition = "p", Token token = {})
    {
        const auto id = newObjectID();
        PartRows rows;
        for (size_t offset = 0; offset < values.size(); ++offset)
            rows.push_back({values[offset].first, {static_cast<UInt32>(offset),
                {.operation_id = id, .block_ordinal = offset, .version = values[offset].second, .self_binding = true}}});
        const auto number = next_part++;
        PartCommit part;
        part.part = id;
        part.temporary_name = "tmp_upsert_" + objectName(id);
        part.final_name = partition + "_" + std::to_string(number) + "_" + std::to_string(number) + "_0";
        part.rows = rows.size();
        part.token = std::move(token);
        const auto path = directory / part.temporary_name;
        std::filesystem::create_directory(path);
        createDurableFile(path / ROWS_FILE, encodePartRows(rows));
        part.files_hash = hashPartDirectory(path, true);
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
    StateStore::Prepared merge(StateStore & store, PartRows rows, const std::vector<String> & sources)
    {
        PartCommit part;
        part.part = newObjectID();
        part.rows = rows.size();
        if (!rows.empty())
        {
            part.temporary_name = "tmp_upsert_" + objectName(part.part);
            const auto number = next_part++;
            part.final_name = "p_" + std::to_string(number) + "_" + std::to_string(number) + "_0";
            for (size_t offset = 0; offset < rows.size(); ++offset)
                rows[offset].value.row_offset = static_cast<UInt32>(offset);
            const auto path = directory / part.temporary_name;
            std::filesystem::create_directory(path);
            createDurableFile(path / ROWS_FILE, encodePartRows(rows));
            part.files_hash = hashPartDirectory(path, true);
        }
        return store.prepareMerge("p", std::move(part), rows, sources);
    }

    std::pair<std::filesystem::path, size_t> appendUnpublishedBatch()
    {
        rocksdb::Options options;
        options.avoid_flush_during_shutdown = true;
        options.track_and_verify_wals = true;
        rocksdb::DB * raw = nullptr;
        const auto path = directory / "upsert_state_v2";
        const auto status = rocksdb::DB::Open(options, path.string(), &raw);
        if (!status.ok())
            throw std::runtime_error(status.ToString());
        std::unique_ptr<rocksdb::DB> database(raw);
        std::filesystem::path wal;
        for (const auto & entry : std::filesystem::directory_iterator(path))
            if (entry.path().extension() == ".log" && entry.path() > wal)
                wal = entry.path();
        const auto boundary = std::filesystem::file_size(wal);
        rocksdb::WriteBatch batch;
        String count(8, '\0');
        count[0] = 2;
        batch.Put("part_count", count);
        batch.Put("unfinished_test_batch", String(256U << 10, 'x'));
        const auto write_status = database->Write(rocksdb::WriteOptions{}, &batch);
        if (!write_status.ok())
            throw std::runtime_error(write_status.ToString());
        database.reset();
        return {wal, boundary};
    }
};

TEST_F(UpsertDiskStateTest, IncompleteFinalBatchRetainsEarlierDurableCommit)
{
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        commit(store, first);
    }
    const auto [wal, boundary] = appendUnpublishedBatch();
    ASSERT_GT(std::filesystem::file_size(wal), boundary + 40000);
    /// Preserve the first fragment and tear the following one before the batch can be acknowledged.
    std::filesystem::resize_file(wal, boundary + 40000);
    {
        StateStore reopened(directory, incarnation, true, true);
        EXPECT_EQ(reopened.authorizedPartCount(), 1);
        auto stale = prepare(reopened, {{"key", 9}});
        EXPECT_FALSE(stale.produces_part);
        auto next = prepare(reopened, {{"key", 11}});
        commit(reopened, next);
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 2);
    EXPECT_FALSE(prepare(reopened, {{"key", 10}}).produces_part);
}

TEST_F(UpsertDiskStateTest, CompleteFinalBatchWithBadChecksumStillFailsClosed)
{
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        commit(store, first);
    }
    const auto [wal, boundary] = appendUnpublishedBatch();
    std::fstream file(wal, std::ios::in | std::ios::out | std::ios::binary);
    file.seekg(static_cast<std::streamoff>(boundary + 100));
    const auto byte = file.get();
    ASSERT_NE(byte, std::char_traits<char>::eof());
    file.seekp(static_cast<std::streamoff>(boundary + 100));
    file.put(static_cast<char>(byte ^ 0x80));
    file.close();
    EXPECT_THROW((StateStore(directory, incarnation, true, true)), Exception);
}

TEST_F(UpsertDiskStateTest, AttachStateCreatedWithoutWalTracking)
{
    const auto path = directory / "upsert_state_v2";
    rocksdb::Options options;
    options.create_if_missing = true;
    options.avoid_flush_during_shutdown = true;
    rocksdb::DB * raw = nullptr;
    ASSERT_TRUE(rocksdb::DB::Open(options, path.string(), &raw).ok());
    std::unique_ptr<rocksdb::DB> database(raw);
    rocksdb::WriteBatch batch;
    batch.Put("identity", "upsert_disk_state_v2:" + objectName(incarnation));
    batch.Put("part_count", String(8, '\0'));
    batch.Put("retry_bytes", String(8, '\0'));
    rocksdb::WriteOptions write;
    write.sync = true;
    ASSERT_TRUE(database->Write(write, &batch).ok());
    database.reset();
    {
        StateStore store(directory, incarnation, true, true);
        auto first = prepare(store, {{"key", 10}});
        commit(store, first);
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 1);
    EXPECT_FALSE(prepare(reopened, {{"key", 9}}).produces_part);
}

TEST_F(UpsertDiskStateTest, TruncatedPredecessorWalFailsClosed)
{
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        commit(store, first);
    }
    rocksdb::Options options;
    options.avoid_flush_during_shutdown = true;
    options.track_and_verify_wals = true;
    options.write_buffer_size = 64U << 10;
    options.max_write_buffer_number = 4;
    const auto path = directory / "upsert_state_v2";
    rocksdb::DB * raw = nullptr;
    ASSERT_TRUE(rocksdb::DB::Open(options, path.string(), &raw).ok());
    std::unique_ptr<rocksdb::DB> database(raw);
    ASSERT_TRUE(database->PauseBackgroundWork().ok());
    rocksdb::WriteOptions write;
    write.sync = true;
    std::filesystem::path predecessor;
    for (const auto & entry : std::filesystem::directory_iterator(path))
        if (entry.path().extension() == ".log" && entry.path() > predecessor)
            predecessor = entry.path();
    ASSERT_TRUE(database->Put(write, "wal_rotation_padding", String(128U << 10, 'x')).ok());
    ASSERT_TRUE(database->Put(write, "wal_successor", "value").ok());
    std::filesystem::path successor;
    for (const auto & entry : std::filesystem::directory_iterator(path))
        if (entry.path().extension() == ".log" && entry.path() > successor)
            successor = entry.path();
    ASSERT_NE(predecessor, successor);
    database.reset();
    const auto size = std::filesystem::file_size(predecessor);
    ASSERT_GT(size, 0);
    std::filesystem::resize_file(predecessor, size - 1);
    EXPECT_THROW((StateStore(directory, incarnation, true, true)), Exception);
}

TEST_F(UpsertDiskStateTest, DurableWinnersAndSnapshots)
{
    String original;
    String mixed;
    Token token{"retry", 1, {4, 5}, 0};
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        original = objectName(first.record.part);
        commit(store, first);
        auto snapshot = store.snapshot();
        auto next = prepare(store, {{"key", 9}, {"other", 1}});
        mixed = objectName(next.record.part);
        EXPECT_EQ(next.record.dead, (std::vector<UInt32>{0}));
        commit(store, next);
        auto newer = prepare(store, {{"key", 11}});
        ASSERT_EQ(newer.record.kills.size(), 1);
        commit(store, newer);
        EXPECT_FALSE(snapshot->getMask(original)->contains(0));
        EXPECT_TRUE(store.snapshot()->getMask(original)->contains(0));
        auto noop = prepare(store, {{"key", 9}}, "p", token);
        EXPECT_FALSE(noop.produces_part);
        commit(store, noop);
        EXPECT_TRUE(store.tokenAlreadyCommitted("p", token));
        token.payload_hash = {7, 8};
        EXPECT_THROW(store.tokenAlreadyCommitted("p", token), Exception);
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 3);
    EXPECT_TRUE(reopened.snapshot()->getMask(original)->contains(0));
    EXPECT_TRUE(reopened.snapshot()->getMask(mixed)->contains(0));
    EXPECT_FALSE(reopened.snapshot()->getMask(mixed)->contains(1));
    token.payload_hash = {4, 5};
    EXPECT_TRUE(reopened.tokenAlreadyCommitted("p", token));
    auto tie = prepare(reopened, {{"key", 11}});
    EXPECT_EQ(tie.record.kills.size(), 1);
    commit(reopened, tie);
}

TEST_F(UpsertDiskStateTest, DurableCommitBeforeRenameRecovers)
{
    String name;
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        name = first.record.final_name;
        store.append(first);
        EXPECT_EQ(store.authorizedPartCount(), 0);
        EXPECT_FALSE(std::filesystem::exists(directory / name));
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 1);
    EXPECT_TRUE(std::filesystem::exists(directory / name));
    EXPECT_TRUE(reopened.authorizedPart(name));
    auto stale = prepare(reopened, {{"key", 9}});
    EXPECT_FALSE(stale.produces_part);
}

TEST_F(UpsertDiskStateTest, MoreThan512PartitionsRetainIndependentWinners)
{
    {
        StateStore store(directory, incarnation, false, true);
        for (size_t i = 0; i < 600; ++i)
        {
            auto prepared = prepare(store, {{"key", i}}, "p" + std::to_string(i));
            commit(store, prepared);
        }
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 600);
    auto stale = prepare(reopened, {{"key", 1}}, "p599");
    EXPECT_FALSE(stale.produces_part);
    auto newer = prepare(reopened, {{"key", 2}}, "p0");
    EXPECT_TRUE(newer.produces_part);
    commit(reopened, newer);
}

TEST_F(UpsertDiskStateTest, PartitionKeysExceedFormerCountAndMemoryBounds)
{
    constexpr size_t batch_rows = 32768;
    constexpr size_t batches = 9;
    const String padding(256, 'x');
    {
        StateStore store(directory, incarnation, false, true);
        for (size_t batch = 0; batch < batches; ++batch)
        {
            std::vector<std::pair<String, UInt64>> values;
            for (size_t i = 0; i < batch_rows; ++i)
                values.emplace_back(padding + std::to_string(batch * batch_rows + i), 10);
            auto prepared = prepare(store, values);
            commit(store, prepared);
        }
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), batches);
    auto stale = prepare(reopened, {{padding + "0", 9}, {padding + std::to_string(batches * batch_rows - 1), 9}});
    EXPECT_FALSE(stale.produces_part);
    auto newer = prepare(reopened, {{padding + "0", 11}, {padding + std::to_string(batches * batch_rows - 1), 11}});
    EXPECT_EQ(newer.record.kills.size(), 2);
    commit(reopened, newer);
}

TEST_F(UpsertDiskStateTest, MissingStateAndForeignIncarnationFailClosed)
{
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        commit(store, first);
    }
    EXPECT_THROW((StateStore(directory, ObjectID{1, 2}, true, true)), Exception);
    std::filesystem::rename(directory / "upsert_state_v2", directory / "saved_state");
    EXPECT_THROW((StateStore(directory, incarnation, true, true)), Exception);
    EXPECT_FALSE(std::filesystem::exists(directory / "upsert_state_v2"));
}

TEST_F(UpsertDiskStateTest, UnauthorizedPartAndDamagedPayloadPreventAttach)
{
    String name;
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        name = first.record.final_name;
        commit(store, first);
    }
    std::filesystem::create_directory(directory / "p_0_100_1");
    EXPECT_THROW((StateStore(directory, incarnation, true, true)), Exception);
    std::filesystem::remove(directory / "p_0_100_1");
    createDurableFile(directory / name / "unexpected", "foreign");
    EXPECT_THROW((StateStore(directory, incarnation, true, true)), Exception);
}

TEST_F(UpsertDiskStateTest, MergeForwardsLateKillsPreservesLineageAndRecoversWithoutSources)
{
    String first_name;
    String second_name;
    String merged_name;
    String merged_id;
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"a", 10}, {"b", 10}});
        first_name = first.record.final_name;
        const auto first_id = objectName(first.record.part);
        commit(store, first);
        auto second = prepare(store, {{"c", 10}});
        second_name = second.record.final_name;
        commit(store, second);
        auto old_snapshot = store.snapshot();
        auto rows = store.loadPartRows(first_name);
        const auto original_b = rows[1].value.row;
        auto extra = store.loadPartRows(second_name);
        rows.insert(rows.end(), extra.begin(), extra.end());
        auto concurrent = prepare(store, {{"a", 11}});
        commit(store, concurrent);
        auto prepared = merge(store, rows, {first_name, second_name});
        EXPECT_EQ(prepared.record.dead, (std::vector<UInt32>{0}));
        merged_name = prepared.record.final_name;
        merged_id = objectName(prepared.record.part);
        commit(store, prepared);
        EXPECT_EQ(store.authorizedPartCount(), 2);
        EXPECT_FALSE(store.authorizedPart(first_name));
        EXPECT_FALSE(old_snapshot->getMask(first_id)->contains(0));
        EXPECT_TRUE(store.snapshot()->getMask(merged_id)->contains(0));
        EXPECT_EQ(store.loadPartRows(merged_name)[1].value.row, original_b);
        EXPECT_TRUE(std::filesystem::exists(directory / first_name));
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 2);
    EXPECT_FALSE(std::filesystem::exists(directory / first_name));
    EXPECT_FALSE(std::filesystem::exists(directory / second_name));
    EXPECT_TRUE(reopened.snapshot()->getMask(merged_id)->contains(0));
    auto stale = prepare(reopened, {{"a", 10}, {"b", 9}, {"c", 9}});
    EXPECT_FALSE(stale.produces_part);
    auto equal_version = prepare(reopened, {{"b", 10}});
    EXPECT_TRUE(equal_version.produces_part);
    EXPECT_EQ(equal_version.record.kills.size(), 1);
    commit(reopened, equal_version);
}

TEST_F(UpsertDiskStateTest, MergeCannotOmitOrInventLiveRows)
{
    StateStore store(directory, incarnation, false, true);
    auto first = prepare(store, {{"a", 10}, {"b", 10}});
    const auto name = first.record.final_name;
    commit(store, first);
    auto rows = store.loadPartRows(name);
    EXPECT_THROW(merge(store, {rows[0]}, {name}), Exception);
    rows[0].value.row.write_csn += 1;
    EXPECT_THROW(merge(store, rows, {name}), Exception);
    EXPECT_EQ(store.authorizedPartCount(), 1);
}

TEST_F(UpsertDiskStateTest, EmptyMergeRetiresFullyDeadSource)
{
    String retired;
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        retired = first.record.final_name;
        commit(store, first);
        auto newer = prepare(store, {{"key", 11}});
        commit(store, newer);
        auto prepared = merge(store, {}, {retired});
        EXPECT_FALSE(prepared.produces_part);
        commit(store, prepared);
        EXPECT_EQ(store.authorizedPartCount(), 1);
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_FALSE(std::filesystem::exists(directory / retired));
    EXPECT_EQ(reopened.authorizedPartCount(), 1);
}

TEST_F(UpsertDiskStateTest, DurableMergeBeforeRenameRecoversAndPreservesWinner)
{
    String source;
    String output;
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        source = first.record.final_name;
        commit(store, first);
        auto prepared = merge(store, store.loadPartRows(source), {source});
        output = prepared.record.final_name;
        store.append(prepared);
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_FALSE(std::filesystem::exists(directory / source));
    EXPECT_TRUE(std::filesystem::exists(directory / output));
    EXPECT_EQ(reopened.authorizedPartCount(), 1);
    auto stale = prepare(reopened, {{"key", 9}});
    EXPECT_FALSE(stale.produces_part);
}

TEST_F(UpsertDiskStateTest, RetryWindowSurvivesCleanupAndTokenReuse)
{
    UInt64 now = 1000;
    auto clock = [&] { return now; };
    Token token{"expiring", 0, {1, 2}, 0};
    {
        StateStore store(directory, incarnation, false, true, clock);
        auto first = prepare(store, {{"key", 10}}, "p", token);
        commit(store, first);
        now += 86399;
        store.pruneRetired();
        EXPECT_TRUE(store.tokenAlreadyCommitted("p", token));
    }
    StateStore reopened(directory, incarnation, true, true, clock);
    EXPECT_TRUE(reopened.tokenAlreadyCommitted("p", token));
    ++now;
    EXPECT_FALSE(reopened.tokenAlreadyCommitted("p", token));
    token.payload_hash = {3, 4};
    auto reused = prepare(reopened, {{"key", 11}}, "p", token);
    commit(reopened, reused);
    reopened.pruneRetired();
    EXPECT_TRUE(reopened.tokenAlreadyCommitted("p", token));
    now += 86400;
    reopened.pruneRetired();
    EXPECT_FALSE(reopened.tokenAlreadyCommitted("p", token));
    auto third = prepare(reopened, {{"key", 12}}, "p", token);
    commit(reopened, third);
    EXPECT_TRUE(reopened.tokenAlreadyCommitted("p", token));
}

TEST_F(UpsertDiskStateTest, SnapshotKeepsDatabaseAliveAfterOwnerDestruction)
{
    SnapshotPtr snapshot;
    String id;
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        id = objectName(first.record.part);
        commit(store, first);
        snapshot = store.snapshot();
    }
    EXPECT_FALSE(snapshot->getMask(id)->contains(0));
    snapshot.reset();
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 1);
}

TEST_F(UpsertDiskStateTest, CorruptRecoveryRootPreservesRetiredPayloads)
{
    String source;
    String output;
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        source = first.record.final_name;
        commit(store, first);
        auto prepared = merge(store, store.loadPartRows(source), {source});
        output = prepared.record.final_name;
        commit(store, prepared);
    }
    createDurableFile(directory / output / "foreign", "damage");
    EXPECT_THROW((StateStore(directory, incarnation, true, true)), Exception);
    EXPECT_TRUE(std::filesystem::exists(directory / source / ROWS_FILE));
}

TEST_F(UpsertDiskStateTest, EmptyRetirementNeverAuthorizesDeletingTheTableDirectory)
{
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        commit(store, first);
    }
    {
        rocksdb::DB * raw = nullptr;
        ASSERT_TRUE(rocksdb::DB::Open(rocksdb::Options{}, (directory / "upsert_state_v2").string(), &raw).ok());
        std::unique_ptr<rocksdb::DB> database(raw);
        rocksdb::WriteOptions options;
        options.sync = true;
        ASSERT_TRUE(database->Put(options, "R", encodePartCommit({})).ok());
    }
    EXPECT_THROW((StateStore(directory, incarnation, true, true)), Exception);
    EXPECT_TRUE(std::filesystem::exists(directory / "upsert_state_v2/CURRENT"));
}

TEST_F(UpsertDiskStateTest, RecoveryRemovesOnlyUncommittedPreparations)
{
    String acknowledged;
    String uncommitted;
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        acknowledged = first.record.final_name;
        commit(store, first);
        auto pending = prepare(store, {{"key", 11}});
        uncommitted = pending.record.temporary_name;
    }
    ASSERT_TRUE(std::filesystem::exists(directory / uncommitted));
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_FALSE(std::filesystem::exists(directory / uncommitted));
    EXPECT_TRUE(std::filesystem::exists(directory / acknowledged));
    auto equal_version = prepare(reopened, {{"key", 10}});
    EXPECT_TRUE(equal_version.produces_part);
}

TEST_F(UpsertDiskStateTest, BatchedWinnersAndMergeAcrossLookupBoundaries)
{
    StateStore store(directory, incarnation, false, true);
    std::vector<std::pair<String, UInt64>> values;
    for (size_t i = 0; i < 2053; ++i)
        values.emplace_back("key" + std::to_string(i), 10);
    auto first = prepare(store, values);
    const auto first_name = first.record.final_name;
    const auto first_id = objectName(first.record.part);
    commit(store, first);
    const auto pinned = store.snapshot();
    for (size_t i = 0; i < values.size(); ++i)
        values[i].second = i % 2 == 0 ? 11 : 9;
    std::reverse(values.begin(), values.end());
    values.emplace_back("new", 1);
    auto second = prepare(store, values);
    EXPECT_EQ(second.record.dead.size(), 1026);
    EXPECT_EQ(second.record.kills.size(), 1027);
    const auto second_name = second.record.final_name;
    const auto second_id = objectName(second.record.part);
    commit(store, second);
    EXPECT_EQ(pinned->getMask(first_id)->cardinality(), 0);
    EXPECT_EQ(store.snapshot()->getMask(first_id)->cardinality(), 1027);
    EXPECT_EQ(store.snapshot()->getMask(second_id)->cardinality(), 1026);
    PartRows output;
    for (const auto & name : {first_name, second_name})
    {
        const auto part_id = name == first_name ? first_id : second_id;
        const auto mask = store.snapshot()->getMask(part_id);
        for (auto row : store.loadPartRows(name))
            if (!mask->contains(row.value.row_offset))
                output.push_back(std::move(row));
    }
    ASSERT_EQ(output.size(), 2054);
    auto merged = merge(store, std::move(output), {first_name, second_name});
    EXPECT_TRUE(merged.record.dead.empty());
    commit(store, merged);
    EXPECT_EQ(store.authorizedPartCount(), 1);
    EXPECT_EQ(store.snapshot()->getMask(objectName(merged.record.part))->cardinality(), 0);
    EXPECT_EQ(pinned->getMask(first_id)->cardinality(), 0);
}

#endif
