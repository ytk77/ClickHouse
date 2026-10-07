#include <Storages/MergeTree/UniqueKey/UpsertStateStore.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include "config.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <future>
#include <barrier>
#include <Storages/MergeTree/UniqueKey/UpsertMetrics.h>
#include <Common/ProfileEvents.h>
#include <IO/WriteBufferFromString.h>

namespace DB::ErrorCodes
{
extern const int CANNOT_OPEN_FILE;
extern const int CORRUPTED_DATA;
}

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
        const String & partition = "p", Token token = {}, bool with_column_payload = false)
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
        if (with_column_payload)
            createDurableFile(path / "data.bin", "original");
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
    StateStore::Prepared merge(StateStore & store, PartRows rows, const std::vector<String> & sources, const PartRows & expired = {},
        bool retire_sources = true, const StateStore::MergeInput * input = nullptr)
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
            part.files_hash = hashPartDirectory(path, true, &part.files_inventory);
        }
        return store.prepareMerge("p", std::move(part), rows, sources, expired, retire_sources, input);
    }

    std::pair<std::filesystem::path, size_t> appendUnpublishedBatch()
    {
        rocksdb::Options options;
        options.avoid_flush_during_shutdown = true;
        options.track_and_verify_wals = true;
        std::unique_ptr<rocksdb::DB> database;
        const auto path = directory / "upsert_state_v2";
        const auto status = rocksdb::DB::Open(options, path.string(), &database);
        if (!status.ok())
            throw std::runtime_error(status.ToString());
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

    PartCommit chunkedOutput(PartRows & rows)
    {
        PartCommit part;
        part.part = newObjectID();
        part.rows = rows.size();
        part.temporary_name = "tmp_upsert_" + objectName(part.part);
        const auto number = next_part++;
        part.final_name = "p_" + std::to_string(number) + "_" + std::to_string(number) + "_0";
        for (size_t offset = 0; offset < rows.size(); ++offset)
            rows[offset].value.row_offset = static_cast<UInt32>(offset);
        const auto path = directory / part.temporary_name;
        std::filesystem::create_directory(path);
        createDurableFile(path / ROWS_FILE, encodePartRows(rows));
        part.files_hash = hashPartDirectory(path, true, &part.files_inventory);
        return part;
    }
};

TEST_F(UpsertDiskStateTest, ChunkedOutputPreservesSnapshotsAndConcurrentWinners)
{
    String source_id;
    String destination_id;
    {
        StateStore store(directory, incarnation, false, true);
        auto source = prepare(store, {{"a", 1}, {"b", 1}, {"c", 1}, {"d", 1}});
        commit(store, source);
        source_id = objectName(source.record.part);
        const auto initial = store.snapshot();
        const std::vector<PartRows> lineage{store.loadPartRows(source.record.final_name)};
        PartRows output{lineage[0][3], lineage[0][2], lineage[0][1], lineage[0][0]};
        auto authorization = chunkedOutput(output);
        destination_id = objectName(authorization.part);
        std::vector<std::vector<UInt32>> offsets{{0, 2}};
        const StateStore::MergeInput input{initial, lineage, offsets};
        const std::vector<String> sources{source.record.final_name};
        auto first = store.prepareMerge("p", authorization, {output[3], output[1]}, sources, {}, false, &input, false);
        ASSERT_TRUE(first.produces_part);
        ASSERT_TRUE(first.record.chunked);
        commit(store, first);
        const auto pinned = store.snapshot();
        EXPECT_EQ(initial->getMask(source_id)->cardinality(), 0);
        EXPECT_EQ(pinned->getMask(source_id)->toVector(), (std::vector<UInt64>{0, 2}));
        EXPECT_EQ(pinned->getMask(destination_id)->toVector(), (std::vector<UInt64>{0, 2}));

        auto concurrent = prepare(store, {{"a", 2}, {"b", 2}});
        commit(store, concurrent);
        offsets = {{1, 3}};
        auto last = store.prepareMerge("p", authorization, {output[2], output[0]}, sources, {}, false, &input, true);
        EXPECT_FALSE(last.produces_part);
        commit(store, last);
        EXPECT_EQ(store.snapshot()->getMask(destination_id)->toVector(), (std::vector<UInt64>{2, 3}));
        EXPECT_EQ(store.snapshot()->getMask(source_id)->cardinality(), 4);
        EXPECT_EQ(pinned->getMask(destination_id)->toVector(), (std::vector<UInt64>{0, 2}));
        offsets = {{}};
        auto retirement = merge(store, {}, sources, {}, true, &input);
        commit(store, retirement);
        EXPECT_EQ(store.authorizedPartCount(), 2);
        EXPECT_FALSE(store.snapshot()->getMask(source_id));
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 2);
    EXPECT_EQ(reopened.snapshot()->getMask(destination_id)->toVector(), (std::vector<UInt64>{2, 3}));
    EXPECT_FALSE(reopened.snapshot()->getMask(source_id));
    EXPECT_FALSE(prepare(reopened, {{"a", 1}, {"b", 1}, {"c", 0}, {"d", 0}}).produces_part);
}

TEST_F(UpsertDiskStateTest, ChunkedOutputRecoversBeforeRenameAndRejectsRepeatedOrOmittedOffsets)
{
    PartCommit authorization;
    PartRows output;
    String source_name;
    String source_id;
    {
        StateStore store(directory, incarnation, false, true);
        auto source = prepare(store, {{"a", 1}, {"b", 1}, {"c", 1}});
        commit(store, source);
        source_name = source.record.final_name;
        source_id = objectName(source.record.part);
        const std::vector<PartRows> lineage{store.loadPartRows(source_name)};
        output = lineage[0];
        authorization = chunkedOutput(output);
        const std::vector<std::vector<UInt32>> offsets{{1}};
        const StateStore::MergeInput input{store.snapshot(), lineage, offsets};
        auto first = store.prepareMerge("p", authorization, {output[1]}, {source_name}, {}, false, &input, false);
        store.append(first);
    }
    StateStore reopened(directory, incarnation, true, true);
    const auto destination_id = objectName(authorization.part);
    EXPECT_TRUE(std::filesystem::exists(directory / authorization.final_name));
    EXPECT_EQ(reopened.snapshot()->getMask(destination_id)->toVector(), (std::vector<UInt64>{0, 2}));
    EXPECT_EQ(reopened.snapshot()->getMask(source_id)->toVector(), (std::vector<UInt64>{1}));
    const std::vector<PartRows> lineage{reopened.loadPartRows(source_name)};
    std::vector<std::vector<UInt32>> offsets{{1}};
    const StateStore::MergeInput input{reopened.snapshot(), lineage, offsets};
    EXPECT_THROW(reopened.prepareMerge("p", authorization, {output[1]}, {source_name}, {}, false, &input, false), Exception);
    offsets = {{0}};
    EXPECT_THROW(reopened.prepareMerge("p", authorization, {output[0]}, {source_name}, {}, false, &input, true), Exception);
    auto changed = authorization;
    changed.files_hash = newObjectID();
    EXPECT_THROW(reopened.prepareMerge("p", changed, {output[0]}, {source_name}, {}, false, &input, false), Exception);
    offsets = {{0, 2}};
    auto last = reopened.prepareMerge("p", authorization, {output[0], output[2]}, {source_name}, {}, false, &input, true);
    commit(reopened, last);
    EXPECT_TRUE(reopened.snapshot()->getMask(destination_id)->empty());
    EXPECT_EQ(reopened.snapshot()->getMask(source_id)->cardinality(), 3);
    EXPECT_THROW(reopened.prepareMerge("p", authorization, {}, {source_name}, {}, false, &input, false), Exception);
}

TEST_F(UpsertDiskStateTest, InterruptedChunkedOutputCanBeConsolidatedWithoutResuming)
{
    String source_name;
    String destination_name;
    {
        StateStore store(directory, incarnation, false, true);
        auto source = prepare(store, {{"a", 1}, {"b", 1}, {"c", 1}});
        commit(store, source);
        source_name = source.record.final_name;
        const std::vector<PartRows> lineage{store.loadPartRows(source_name)};
        auto output = lineage[0];
        auto authorization = chunkedOutput(output);
        destination_name = authorization.final_name;
        const std::vector<std::vector<UInt32>> offsets{{1}};
        const StateStore::MergeInput input{store.snapshot(), lineage, offsets};
        auto first = store.prepareMerge("p", authorization, {output[1]}, {source_name}, {}, false, &input, false);
        commit(store, first);
    }
    {
        StateStore reopened(directory, incarnation, true, true);
        const std::vector<PartRows> lineage{reopened.loadPartRows(source_name), reopened.loadPartRows(destination_name)};
        const std::vector<std::vector<UInt32>> offsets(2);
        const StateStore::MergeInput input{reopened.snapshot(), lineage, offsets};
        auto joined = merge(reopened, {lineage[0][0], lineage[1][1], lineage[0][2]}, {source_name, destination_name}, {}, true, &input);
        commit(reopened, joined);
        EXPECT_EQ(reopened.authorizedPartCount(), 1);
        EXPECT_TRUE(reopened.snapshot()->getMask(objectName(joined.record.part))->empty());
        EXPECT_EQ(reopened.loadPartRows(joined.record.final_name).size(), 3);
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 1);
}

TEST_F(UpsertDiskStateTest, RecoveryRejectsVisibleOutputWithoutDurableProgress)
{
    String destination_id;
    {
        StateStore store(directory, incarnation, false, true);
        auto source = prepare(store, {{"a", 1}, {"b", 1}});
        commit(store, source);
        const std::vector<PartRows> lineage{store.loadPartRows(source.record.final_name)};
        auto output = lineage[0];
        auto authorization = chunkedOutput(output);
        destination_id = objectName(authorization.part);
        const std::vector<std::vector<UInt32>> offsets{{0}};
        const StateStore::MergeInput input{store.snapshot(), lineage, offsets};
        auto first = store.prepareMerge("p", authorization, {output[0]}, {source.record.final_name}, {}, false, &input, false);
        commit(store, first);
    }
    std::unique_ptr<rocksdb::DB> database;
    ASSERT_TRUE(rocksdb::DB::Open(rocksdb::Options{}, (directory / "upsert_state_v2").string(), &database).ok());
    String progress;
    {
        WriteBufferFromString out(progress);
        DeleteBitmap{}.serialize(out);
    }
    ASSERT_TRUE(database->Put(rocksdb::WriteOptions{}, "B" + destination_id, progress).ok());
    database.reset();
    EXPECT_THROW((StateStore(directory, incarnation, true, true)), Exception);
}

TEST_F(UpsertDiskStateTest, TruncateRemovesInterruptedOutputProgress)
{
    {
        StateStore store(directory, incarnation, false, true);
        auto source = prepare(store, {{"a", 1}, {"b", 1}});
        commit(store, source);
        const std::vector<PartRows> lineage{store.loadPartRows(source.record.final_name)};
        auto output = lineage[0];
        auto authorization = chunkedOutput(output);
        const std::vector<std::vector<UInt32>> offsets{{0}};
        const StateStore::MergeInput input{store.snapshot(), lineage, offsets};
        auto first = store.prepareMerge("p", authorization, {output[0]}, {source.record.final_name}, {}, false, &input, false);
        commit(store, first);
        auto truncated = store.prepareTruncate();
        store.append(truncated);
        store.publish(truncated);
        EXPECT_EQ(store.authorizedPartCount(), 0);
    }
    std::unique_ptr<rocksdb::DB> database;
    ASSERT_TRUE(rocksdb::DB::Open(rocksdb::Options{}, (directory / "upsert_state_v2").string(), &database).ok());
    std::unique_ptr<rocksdb::Iterator> iterator(database->NewIterator(rocksdb::ReadOptions{}));
    iterator->Seek("B");
    EXPECT_FALSE(iterator->Valid() && iterator->key().starts_with("B"));
    EXPECT_TRUE(iterator->status().ok());
}

TEST_F(UpsertDiskStateTest, AuthenticatedRetirementRequiresFullMasksAndUnchangedPayloads)
{
    StateStore store(directory, incarnation, false, true);
    auto source = prepare(store, {{"a", 1}, {"b", 1}});
    commit(store, source);
    std::vector<PartCommit> authorizations(1);
    std::vector<PartRows> lineage{store.loadPartRows(source.record.final_name, &authorizations.front())};
    std::vector<std::vector<UInt32>> offsets{{0, 1}};
    const StateStore::MergeInput input{store.snapshot(), lineage, offsets, &authorizations};
    EXPECT_THROW(merge(store, {}, {source.record.final_name}, {}, true, &input), Exception);
    auto moved = merge(store, lineage.front(), {source.record.final_name}, {}, false, &input);
    commit(store, moved);
    EXPECT_NO_THROW(store.validateRetirement("p", {source.record.final_name}, input));
    const auto original_hash = authorizations.front().files_hash;
    authorizations.front().files_hash = newObjectID();
    EXPECT_THROW(merge(store, {}, {source.record.final_name}, {}, true, &input), Exception);
    authorizations.front().files_hash = original_hash;
    auto retired = merge(store, {}, {source.record.final_name}, {}, true, &input);
    commit(store, retired);
    EXPECT_EQ(store.authorizedPartCount(), 1);
    EXPECT_EQ(store.loadPartRows(moved.record.final_name).size(), 2);
}

TEST_F(UpsertDiskStateTest, RetirementValidationRejectsRemainingSourceWinners)
{
    StateStore store(directory, incarnation, false, true);
    auto source = prepare(store, {{"a", 1}});
    commit(store, source);
    std::vector<PartCommit> authorizations(1);
    std::vector<PartRows> lineage{store.loadPartRows(source.record.final_name, &authorizations.front())};
    std::vector<std::vector<UInt32>> offsets(1);
    const StateStore::MergeInput input{store.snapshot(), lineage, offsets, &authorizations};
    EXPECT_THROW(store.validateRetirement("p", {source.record.final_name}, input), Exception);
}

TEST_F(UpsertDiskStateTest, RetirementOmitsHistoricalKillsBeyondTheStateBatchBudget)
{
    constexpr UInt64 rows = 450000;
    std::vector<std::pair<String, UInt64>> values;
    values.reserve(rows);
    for (UInt64 row = 0; row < rows; ++row)
        values.emplace_back(std::to_string(row), 1);
    std::vector<String> sources;
    String output_name;
    {
        StateStore store(directory, incarnation, false, true);
        auto seed = prepare(store, values);
        const auto seed_name = seed.record.final_name;
        commit(store, seed);
        size_t historical_kill_bytes = 0;
        for (UInt64 version = 2; version <= 17; ++version)
        {
            for (auto & value : values)
                value.second = version;
            auto next = prepare(store, values);
            ASSERT_EQ(next.record.kills.size(), rows);
            historical_kill_bytes += next.record.kills.size() * 20;
            sources.push_back(next.record.final_name);
            commit(store, next);
        }
        ASSERT_GT(historical_kill_bytes, MAX_PENDING_STATE_BYTES);
        auto output = merge(store, store.loadPartRows(sources.back()), sources);
        output_name = output.record.final_name;
        commit(store, output);
        EXPECT_EQ(store.authorizedPartCount(), 2);
        EXPECT_TRUE(store.snapshot()->getMask(objectName(output.record.part))->empty());
        auto retirement = merge(store, {}, {seed_name});
        commit(store, retirement);
    }
    std::unique_ptr<rocksdb::DB> database;
    ASSERT_TRUE(rocksdb::DB::Open(rocksdb::Options{}, (directory / "upsert_state_v2").string(), &database).ok());
    for (const auto & source : sources)
    {
        String payload;
        ASSERT_TRUE(database->Get(rocksdb::ReadOptions{}, "R" + source, &payload).ok());
        const auto record = decodePartCommit(payload);
        EXPECT_TRUE(record.kills.empty());
        EXPECT_TRUE(record.dead.empty());
        EXPECT_EQ(record.token.accepted, rows);
        EXPECT_EQ(record.final_name, source);
    }
    database.reset();
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 1);
    EXPECT_EQ(reopened.loadPartRows(output_name).size(), rows);
}

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
    std::unique_ptr<rocksdb::DB> database;
    ASSERT_TRUE(rocksdb::DB::Open(options, path.string(), &database).ok());
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
    std::unique_ptr<rocksdb::DB> database;
    ASSERT_TRUE(rocksdb::DB::Open(options, path.string(), &database).ok());
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

TEST_F(UpsertDiskStateTest, WinnerLookupScratchPreservesUnsortedRowsAndPartialBatches)
{
    StateStore store(directory, incarnation, false, true);
    std::vector<std::pair<String, UInt64>> initial;
    constexpr size_t count = 2057;
    for (size_t i = 0; i < count; ++i)
    {
        String key;
        for (size_t shift : {24, 16, 8, 0})
            key.push_back(static_cast<char>(i >> shift));
        key.append(i % 2 ? 100 : 1, '\xff');
        if (i % 129 == 0)
            key.append(128U << 10, 'k');
        initial.emplace_back(std::move(key), 1000 + i);
    }
    auto first = prepare(store, initial);
    commit(store, first);
    const auto first_id = objectName(first.record.part);
    std::vector<std::pair<String, UInt64>> incoming;
    std::vector<size_t> source_offsets;
    for (size_t i = 0; i < count; ++i)
    {
        const size_t source = (i * 19) % count;
        source_offsets.push_back(source);
        incoming.emplace_back(initial[source].first, initial[source].second + (source % 3 ? 1 : -1));
    }
    auto next = prepare(store, incoming);
    EXPECT_EQ(next.record.dead.size(), (count + 2) / 3);
    EXPECT_EQ(next.record.kills.size(), count - (count + 2) / 3);
    commit(store, next);
    const auto next_id = objectName(next.record.part);
    const auto snapshot = store.snapshot();
    for (size_t i = 0; i < count; ++i)
    {
        EXPECT_EQ(snapshot->getMask(first_id)->contains(i), i % 3 != 0);
        EXPECT_EQ(snapshot->getMask(next_id)->contains(i), source_offsets[i] % 3 == 0);
    }
    auto singleton = prepare(store, {{initial[0].first, initial[0].second + 2}});
    ASSERT_EQ(singleton.record.kills.size(), 1);
    EXPECT_EQ(objectName(singleton.record.kills[0].part), first_id);
    EXPECT_EQ(singleton.record.kills[0].offset, 0);
    commit(store, singleton);
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
        std::unique_ptr<rocksdb::DB> database;
        ASSERT_TRUE(rocksdb::DB::Open(rocksdb::Options{}, (directory / "upsert_state_v2").string(), &database).ok());
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

TEST_F(UpsertDiskStateTest, ExpirationPreservesPinnedSnapshotAndRetryToken)
{
    Token token{"expired_retry", 1, {4, 5}, 0};
    String source;
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}}, "p", token);
        source = first.record.final_name;
        const auto id = objectName(first.record.part);
        commit(store, first);
        const auto pinned = store.snapshot();
        auto expired = merge(store, {}, {source}, store.loadPartRows(source));
        EXPECT_FALSE(expired.produces_part);
        commit(store, expired);
        EXPECT_EQ(store.authorizedPartCount(), 0);
        EXPECT_FALSE(store.snapshot()->getMask(id));
        ASSERT_TRUE(pinned->getMask(id));
        EXPECT_EQ(pinned->getMask(id)->cardinality(), 0);
        EXPECT_TRUE(store.tokenAlreadyCommitted("p", token));
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 0);
    EXPECT_FALSE(std::filesystem::exists(directory / source));
    EXPECT_TRUE(reopened.tokenAlreadyCommitted("p", token));
    auto reinsert = prepare(reopened, {{"key", 1}});
    EXPECT_TRUE(reinsert.produces_part);
    EXPECT_TRUE(reinsert.record.kills.empty());
    commit(reopened, reinsert);
}

TEST_F(UpsertDiskStateTest, ExpirationDoesNotDeleteConcurrentWinner)
{
    StateStore store(directory, incarnation, false, true);
    auto first = prepare(store, {{"key", 10}, {"retained", 10}});
    const auto source = first.record.final_name;
    commit(store, first);
    const auto rows = store.loadPartRows(source);
    auto concurrent = prepare(store, {{"key", 11}});
    const auto concurrent_id = objectName(concurrent.record.part);
    commit(store, concurrent);
    auto expired = merge(store, {rows[1]}, {source}, {rows[0]});
    commit(store, expired);
    EXPECT_EQ(store.authorizedPartCount(), 2);
    EXPECT_EQ(store.snapshot()->getMask(concurrent_id)->cardinality(), 0);
    EXPECT_FALSE(prepare(store, {{"key", 10}}).produces_part);
    EXPECT_FALSE(prepare(store, {{"retained", 9}}).produces_part);
}

TEST_F(UpsertDiskStateTest, ExpirationBeforePublicationRecoversAndAllowsDeadSourceReclamation)
{
    String old_source;
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}, {"retained", 10}});
        old_source = first.record.final_name;
        commit(store, first);
        auto next = prepare(store, {{"key", 11}});
        const auto source = next.record.final_name;
        commit(store, next);
        auto expired = merge(store, {}, {source}, store.loadPartRows(source));
        store.append(expired);
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 1);
    const auto rows = reopened.loadPartRows(old_source);
    auto reclaimed = merge(reopened, {rows[1]}, {old_source});
    commit(reopened, reclaimed);
    EXPECT_EQ(reopened.authorizedPartCount(), 1);
    EXPECT_FALSE(prepare(reopened, {{"retained", 9}}).produces_part);
    EXPECT_TRUE(prepare(reopened, {{"key", 1}}).produces_part);
}

TEST_F(UpsertDiskStateTest, ExpirationRejectsInventedChangedAndDuplicatedIdentities)
{
    StateStore store(directory, incarnation, false, true);
    auto first = prepare(store, {{"key", 10}});
    const auto source = first.record.final_name;
    commit(store, first);
    auto rows = store.loadPartRows(source);
    EXPECT_THROW(merge(store, {}, {source}), Exception);
    EXPECT_THROW(merge(store, rows, {source}, rows), Exception);
    EXPECT_THROW(merge(store, {}, {source}, {rows[0], rows[0]}), Exception);
    auto changed = rows;
    changed[0].value.row.version = 11;
    EXPECT_THROW(merge(store, {}, {source}, changed), Exception);
    changed = rows;
    changed[0].value.row_offset = 1;
    EXPECT_THROW(merge(store, {}, {source}, changed), Exception);
    changed = rows;
    changed[0].key = "invented";
    EXPECT_THROW(merge(store, rows, {source}, changed), Exception);
    EXPECT_EQ(store.authorizedPartCount(), 1);
    EXPECT_FALSE(prepare(store, {{"key", 9}}).produces_part);
}

TEST_F(UpsertDiskStateTest, MaskCacheKeepsPinnedVersionsAcrossOverwriteAndTruncate)
{
    Resources resources;
    resources.mask_cache_bytes = 4096;
    StateStore store(directory, incarnation, false, true, {}, resources);
    auto first = prepare(store, {{"a", 1}, {"b", 1}});
    commit(store, first);
    const auto id = objectName(first.record.part);
    const auto old = store.snapshot();
    const auto old_mask = old->getMask(id);
    ASSERT_TRUE(old_mask);
    EXPECT_TRUE(old_mask->empty());
    auto second = prepare(store, {{"a", 2}});
    commit(store, second);
    const auto updated = store.snapshot();
    EXPECT_TRUE(updated->getMask(id)->contains(0));
    EXPECT_FALSE(updated->getMask(id)->contains(1));
    EXPECT_TRUE(old->getMask(id)->empty());
    auto unrelated = prepare(store, {{"c", 1}});
    commit(store, unrelated);
    EXPECT_EQ(updated->getMask(id), store.snapshot()->getMask(id));
    auto truncate = store.prepareTruncate();
    store.append(truncate);
    store.publish(truncate);
    EXPECT_FALSE(store.snapshot()->getMask(id));
    EXPECT_TRUE(updated->getMask(id)->contains(0));
    EXPECT_TRUE(old->getMask(id)->empty());
}

TEST_F(UpsertDiskStateTest, TruncateCompactionReclaimsUnpinnedStateBeforeReload)
{
    const auto before = ProfileEvents::global_counters[ProfileEvents::UpsertTruncateCompactions];
    {
        StateStore store(directory, incarnation, false, true);
        std::vector<std::pair<String, UInt64>> values;
        for (size_t i = 0; i < 20000; ++i)
            values.emplace_back("key" + std::to_string(i), 10);
        auto part = prepare(store, values);
        commit(store, part);
    }
    {
        std::unique_ptr<rocksdb::DB> database;
        ASSERT_TRUE(rocksdb::DB::Open(rocksdb::Options{}, (directory / "upsert_state_v2").string(), &database).ok());
        /// Establish actual old SSTs, so cleanup cannot pass by dropping only memtable entries.
        ASSERT_TRUE(database->Flush(rocksdb::FlushOptions{}).ok());
        rocksdb::TablePropertiesCollection properties;
        ASSERT_TRUE(database->GetPropertiesOfAllTables(&properties).ok());
        UInt64 entries = 0;
        for (const auto & [name, table] : properties)
            entries += table->num_entries;
        ASSERT_GE(entries, 20000);
    }
    {
        StateStore store(directory, incarnation, true, true);
        {
            auto truncated = store.prepareTruncate();
            store.append(truncated);
            store.publish(truncated);
        }
        store.finishTruncate();
        EXPECT_EQ(store.authorizedPartCount(), 0);
        EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UpsertTruncateCompactions] - before, 1);
        store.finishTruncate();
        EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UpsertTruncateCompactions] - before, 1);
    }
    {
        std::unique_ptr<rocksdb::DB> database;
        ASSERT_TRUE(rocksdb::DB::Open(rocksdb::Options{}, (directory / "upsert_state_v2").string(), &database).ok());
        rocksdb::TablePropertiesCollection properties;
        ASSERT_TRUE(database->GetPropertiesOfAllTables(&properties).ok());
        UInt64 entries = 0;
        for (const auto & [name, table] : properties)
            entries += table->num_entries;
        /// Only catalog metadata, the retirement record and range boundaries remain in the SSTs.
        EXPECT_LT(entries, 64);
        String value;
        EXPECT_TRUE(database->Get(rocksdb::ReadOptions{}, "truncate_compaction_pending", &value).IsNotFound());
    }
    StateStore reopened(directory, incarnation, true, true);
    auto replacement = prepare(reopened, {{"key0", 1}});
    ASSERT_TRUE(replacement.produces_part);
    commit(reopened, replacement);
    EXPECT_EQ(reopened.loadPartRows(replacement.record.final_name).size(), 1);
}

TEST_F(UpsertDiskStateTest, TruncateCompactionPreservesPinnedMasks)
{
    Resources resources;
    resources.mask_cache_bytes = 0;
    StateStore store(directory, incarnation, false, true, {}, resources);
    String identity;
    {
        auto first = prepare(store, {{"key", 10}});
        identity = objectName(first.record.part);
        commit(store, first);
    }
    const auto original = store.snapshot();
    {
        auto second = prepare(store, {{"key", 20}});
        commit(store, second);
    }
    const auto overwritten = store.snapshot();
    {
        auto truncated = store.prepareTruncate();
        store.append(truncated);
        store.publish(truncated);
    }
    store.finishTruncate();
    ASSERT_TRUE(original->getMask(identity));
    EXPECT_TRUE(original->getMask(identity)->empty());
    ASSERT_TRUE(overwritten->getMask(identity));
    EXPECT_TRUE(overwritten->getMask(identity)->contains(0));
    EXPECT_FALSE(store.snapshot()->getMask(identity));
    auto replacement = prepare(store, {{"key", 1}});
    EXPECT_TRUE(replacement.produces_part);
    commit(store, replacement);
    EXPECT_TRUE(original->getMask(identity)->empty());
    EXPECT_TRUE(overwritten->getMask(identity)->contains(0));
}

TEST_F(UpsertDiskStateTest, RecoveryFinishesDurableUnpublishedTruncateCompaction)
{
    {
        StateStore store(directory, incarnation, false, true);
        auto part = prepare(store, {{"key", 10}});
        commit(store, part);
        auto truncated = store.prepareTruncate();
        store.append(truncated);
        /// Simulate process loss after the durable batch, before publication and cleanup.
    }
    const auto before = ProfileEvents::global_counters[ProfileEvents::UpsertTruncateCompactions];
    {
        StateStore reopened(directory, incarnation, true, true);
        EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UpsertTruncateCompactions] - before, 1);
        EXPECT_EQ(reopened.authorizedPartCount(), 0);
        auto part = prepare(reopened, {{"key", 1}});
        ASSERT_TRUE(part.produces_part);
        commit(reopened, part);
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 1);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UpsertTruncateCompactions] - before, 1);
}

TEST_F(UpsertDiskStateTest, InvalidTruncateCompactionMarkerFailsClosed)
{
    {
        StateStore store(directory, incarnation, false, true);
    }
    std::unique_ptr<rocksdb::DB> database;
    ASSERT_TRUE(rocksdb::DB::Open(rocksdb::Options{}, (directory / "upsert_state_v2").string(), &database).ok());
    ASSERT_TRUE(database->Put(rocksdb::WriteOptions{}, "truncate_compaction_pending", "invalid").ok());
    database.reset();
    try
    {
        StateStore reopened(directory, incarnation, true, true);
        FAIL() << "Expected malformed truncate compaction marker rejection";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::CORRUPTED_DATA);
        EXPECT_NE(error.message().find("invalid truncate compaction marker"), String::npos);
    }
}

TEST_F(UpsertDiskStateTest, MaskCacheTracksPartialRelocationExpirationAndRetirement)
{
    String source_id;
    String relocated_id;
    {
        Resources resources;
        resources.mask_cache_bytes = 128;
        StateStore store(directory, incarnation, false, true, {}, resources);
        auto first = prepare(store, {{"a", 1}, {"b", 1}, {"c", 1}});
        source_id = objectName(first.record.part);
        const auto source = first.record.final_name;
        commit(store, first);
        const auto initial = store.snapshot();
        const std::vector<PartRows> lineage{store.loadPartRows(source)};
        std::vector<std::vector<UInt32>> offsets{{0, 1}};
        const StateStore::MergeInput input{initial, lineage, offsets};
        auto partial = merge(store, {lineage[0][0]}, {source}, {lineage[0][1]}, false, &input);
        relocated_id = objectName(partial.record.part);
        commit(store, partial);
        const auto pinned = store.snapshot();
        EXPECT_EQ(pinned->getMask(source_id)->cardinality(), 2);
        EXPECT_TRUE(pinned->getMask(relocated_id)->empty());
        EXPECT_TRUE(initial->getMask(source_id)->empty());

        auto concurrent = prepare(store, {{"a", 2}});
        commit(store, concurrent);
        EXPECT_TRUE(store.snapshot()->getMask(relocated_id)->contains(0));
        EXPECT_TRUE(pinned->getMask(relocated_id)->empty());
        offsets = {{2}};
        auto last = merge(store, {lineage[0][2]}, {source}, {}, false, &input);
        commit(store, last);
        EXPECT_EQ(store.snapshot()->getMask(source_id)->cardinality(), 3);
        EXPECT_EQ(pinned->getMask(source_id)->cardinality(), 2);
        offsets = {{}};
        auto retirement = merge(store, {}, {source}, {}, true, &input);
        commit(store, retirement);
        EXPECT_FALSE(store.snapshot()->getMask(source_id));
        EXPECT_EQ(pinned->getMask(source_id)->cardinality(), 2);
        EXPECT_TRUE(initial->getMask(source_id)->empty());
        EXPECT_FALSE(prepare(store, {{"a", 1}}).produces_part);
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_FALSE(reopened.snapshot()->getMask(source_id));
    EXPECT_TRUE(reopened.snapshot()->getMask(relocated_id)->contains(0));
    EXPECT_EQ(reopened.authorizedPartCount(), 3);
    EXPECT_FALSE(prepare(reopened, {{"a", 1}}).produces_part);
}

TEST_F(UpsertDiskStateTest, ParallelRecoveryRejectsCorruptPayload)
{
    String part;
    {
        StateStore store(directory, incarnation, false, true);
        for (size_t i = 0; i < 16; ++i)
        {
            auto prepared = prepare(store, {{std::to_string(i), 1}});
            commit(store, prepared);
            part = prepared.record.final_name;
        }
    }
    {
        std::ofstream output(directory / part / ROWS_FILE, std::ios::binary | std::ios::app);
        output.put('x');
    }
    for (UInt64 workers : {1, 4, 64})
    {
        Resources resources;
        resources.recovery_threads = workers;
        EXPECT_THROW((StateStore(directory, incarnation, true, true, {}, resources)), Exception);
    }
}

TEST_F(UpsertDiskStateTest, PartialRelocationRecoversAndMergesMaskedSourceCopy)
{
    String source;
    String destination;
    String source_id;
    String destination_id;
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"a", 1}, {"b", 1}});
        source = first.record.final_name;
        source_id = objectName(first.record.part);
        commit(store, first);
        const std::vector<PartRows> lineage{store.loadPartRows(source)};
        const std::vector<std::vector<UInt32>> offsets{{0}};
        const StateStore::MergeInput input{store.snapshot(), lineage, offsets};
        auto partial = merge(store, {lineage[0][0]}, {source}, {}, false, &input);
        destination = partial.record.final_name;
        destination_id = objectName(partial.record.part);
        commit(store, partial);
        EXPECT_EQ(store.authorizedPartCount(), 2);
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_TRUE(reopened.snapshot()->getMask(source_id)->contains(0));
    EXPECT_FALSE(reopened.snapshot()->getMask(source_id)->contains(1));
    EXPECT_TRUE(reopened.snapshot()->getMask(destination_id)->empty());
    const std::vector<PartRows> lineage{reopened.loadPartRows(source), reopened.loadPartRows(destination)};
    const std::vector<std::vector<UInt32>> offsets(2);
    const StateStore::MergeInput input{reopened.snapshot(), lineage, offsets};
    auto joined = merge(reopened, {lineage[0][1], lineage[1][0]}, {source, destination}, {}, true, &input);
    EXPECT_TRUE(joined.record.dead.empty());
    commit(reopened, joined);
    EXPECT_EQ(reopened.authorizedPartCount(), 1);
    EXPECT_TRUE(reopened.snapshot()->getMask(objectName(joined.record.part))->empty());
    EXPECT_EQ(reopened.loadPartRows(joined.record.final_name).size(), 2);
}

TEST_F(UpsertDiskStateTest, ParallelRecoveryRejectsBothPayloadNamesAndCanRetry)
{
    String temporary;
    String final;
    {
        StateStore store(directory, incarnation, false, true);
        auto prepared = prepare(store, {{"key", 1}});
        temporary = prepared.record.temporary_name;
        final = prepared.record.final_name;
        store.append(prepared);
    }
    std::filesystem::copy(directory / temporary, directory / final, std::filesystem::copy_options::recursive);
    for (UInt64 workers : {1, 64})
    {
        Resources resources;
        resources.recovery_threads = workers;
        EXPECT_THROW((StateStore(directory, incarnation, true, true, {}, resources)), Exception);
    }
    std::filesystem::remove_all(directory / final);
    Resources resources;
    resources.recovery_threads = 64;
    StateStore recovered(directory, incarnation, true, true, {}, resources);
    EXPECT_TRUE(std::filesystem::exists(directory / final));
    EXPECT_FALSE(std::filesystem::exists(directory / temporary));
    EXPECT_EQ(recovered.authorizedPartCount(), 1);
    EXPECT_FALSE(prepare(recovered, {{"key", 0}}).produces_part);
}
TEST_F(UpsertDiskStateTest, ClosePreservesPinnedReadersAndReleasesDatabaseLock)
{
    StateStore store(directory, incarnation, false, true);
    {
        auto prepared = prepare(store, {{"key", 1}});
        commit(store, prepared);
    }
    auto pinned = store.snapshot();
    EXPECT_THROW(store.close(), Exception);
    EXPECT_EQ(pinned->getMask(objectName(store.authorizedPart("p_1_1_0").value()))->cardinality(), 0);
    pinned.reset();
    EXPECT_NO_THROW(store.close());
    /// Opening the same path proves that the original database handle and its lock are gone.
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 1);
}


TEST_F(UpsertDiskStateTest, RecoveryDefersSameSizePayloadCorruptionAndFencesOnRead)
{
    String name;
    String identity;
    {
        StateStore store(directory, incarnation, false, true);
        auto part = prepare(store, {{"key", 10}});
        name = part.record.final_name;
        identity = objectName(part.record.part);
        commit(store, part);
    }
    std::fstream file(directory / name / ROWS_FILE, std::ios::in | std::ios::out | std::ios::binary);
    file.put('X');
    file.close();
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(reopened.authorizedPartCount(), 1);
    EXPECT_THROW(reopened.snapshot()->verifyPart(name, identity), Exception);
    EXPECT_THROW(reopened.snapshot()->checkVerification(), Exception);
    EXPECT_THROW(reopened.checkAdmission(), Exception);
    EXPECT_THROW(reopened.pruneRetired(), Exception);
}

TEST_F(UpsertDiskStateTest, ConcurrentReadersSharePayloadVerification)
{
    String name;
    String identity;
    {
        StateStore store(directory, incarnation, false, true);
        auto part = prepare(store, {{"key", 10}});
        name = part.record.final_name;
        identity = objectName(part.record.part);
        commit(store, part);
    }
    StateStore reopened(directory, incarnation, true, true);
    const auto snapshot = reopened.snapshot();
    const auto before = ProfileEvents::global_counters[ProfileEvents::UpsertPayloadVerificationParts];
    std::barrier<> start(3);
    auto reader = [&]
    {
        start.arrive_and_wait();
        snapshot->verifyPart(name, identity);
    };
    auto first = std::async(std::launch::async, reader);
    auto second = std::async(std::launch::async, reader);
    start.arrive_and_wait();
    EXPECT_NO_THROW(first.get());
    EXPECT_NO_THROW(second.get());
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UpsertPayloadVerificationParts] - before, 1);
    snapshot->verifyPart(name, identity);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UpsertPayloadVerificationParts] - before, 1);
    std::fstream file(directory / name / ROWS_FILE, std::ios::in | std::ios::out | std::ios::binary);
    file.put('X');
    file.close();
    EXPECT_THROW(snapshot->verifyPart(name, identity, true), Exception);
}

TEST_F(UpsertDiskStateTest, InventoryCorruptionCannotAuthorizeMetadata)
{
    String identity;
    {
        StateStore store(directory, incarnation, false, true);
        auto part = prepare(store, {{"key", 10}});
        identity = objectName(part.record.part);
        commit(store, part);
    }
    std::unique_ptr<rocksdb::DB> database;
    ASSERT_TRUE(rocksdb::DB::Open(rocksdb::Options{}, (directory / "upsert_state_v2").string(), &database).ok());
    ASSERT_TRUE(database->Put(rocksdb::WriteOptions{}, "V" + identity, "forged").ok());
    database.reset();
    try
    {
        StateStore reopened(directory, incarnation, true, true);
        FAIL() << "Expected committed inventory hash rejection";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::CORRUPTED_DATA);
        EXPECT_NE(error.message().find("authenticated inventory or metadata mismatch"), String::npos);
    }
}

TEST_F(UpsertDiskStateTest, LegacyInventoryIsEstablishedOnlyAfterFullVerification)
{
    String name;
    String identity;
    {
        StateStore store(directory, incarnation, false, true);
        auto part = prepare(store, {{"key", 10}});
        name = part.record.final_name;
        identity = objectName(part.record.part);
        commit(store, part);
    }
    std::unique_ptr<rocksdb::DB> database;
    ASSERT_TRUE(rocksdb::DB::Open(rocksdb::Options{}, (directory / "upsert_state_v2").string(), &database).ok());
    ASSERT_TRUE(database->Delete(rocksdb::WriteOptions{}, "V" + identity).ok());
    database.reset();
    const auto full_before = ProfileEvents::global_counters[ProfileEvents::UpsertRecoveryParts];
    {
        StateStore reopened(directory, incarnation, true, true);
        EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UpsertRecoveryParts] - full_before, 1);
        EXPECT_EQ(reopened.loadPartRows(name).size(), 1);
        EXPECT_NO_THROW(reopened.snapshot()->verifyPart(name, identity));
    }
    const auto metadata_before = ProfileEvents::global_counters[ProfileEvents::UpsertRecoveryMetadataParts];
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UpsertRecoveryParts] - full_before, 1);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UpsertRecoveryMetadataParts] - metadata_before, 1);
}

TEST_F(UpsertDiskStateTest, RecoveryRetainsSourcesUntilReplacementVerification)
{
    String source;
    String output;
    String identity;
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        source = first.record.final_name;
        commit(store, first);
        auto part = merge(store, store.loadPartRows(source), {source});
        output = part.record.final_name;
        identity = objectName(part.record.part);
        commit(store, part);
    }
    /// Native cleanup may have renamed its source before the process stopped.
    std::filesystem::rename(directory / source, directory / ("delete_tmp_" + source));
    const auto retained = directory / "upsert_recovery_retired" / source;
    {
        StateStore reopened(directory, incarnation, true, true);
        EXPECT_FALSE(std::filesystem::exists(directory / source));
        EXPECT_TRUE(std::filesystem::exists(retained / ROWS_FILE));
        reopened.pruneRetired();
        EXPECT_TRUE(std::filesystem::exists(retained / ROWS_FILE));
    }
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_TRUE(std::filesystem::exists(retained / ROWS_FILE));
    reopened.snapshot()->verifyPart(output, identity);
    reopened.pruneRetired();
    EXPECT_FALSE(std::filesystem::exists(retained));
}

TEST_F(UpsertDiskStateTest, CorruptReplacementCannotReclaimRetainedSource)
{
    String source;
    String output;
    String identity;
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        source = first.record.final_name;
        commit(store, first);
        auto part = merge(store, store.loadPartRows(source), {source});
        output = part.record.final_name;
        identity = objectName(part.record.part);
        commit(store, part);
    }
    std::fstream file(directory / output / ROWS_FILE, std::ios::in | std::ios::out | std::ios::binary);
    file.put('X');
    file.close();
    StateStore reopened(directory, incarnation, true, true);
    EXPECT_THROW(reopened.snapshot()->verifyPart(output, identity), Exception);
    EXPECT_THROW(reopened.pruneRetired(), Exception);
    EXPECT_TRUE(std::filesystem::exists(directory / "upsert_recovery_retired" / source / ROWS_FILE));
}

TEST_F(UpsertDiskStateTest, AuthorizedTemporaryPayloadStillRequiresFullVerification)
{
    String temporary;
    {
        StateStore store(directory, incarnation, false, true);
        auto part = prepare(store, {{"key", 10}});
        temporary = part.record.temporary_name;
        store.append(part);
    }
    std::fstream file(directory / temporary / ROWS_FILE, std::ios::in | std::ios::out | std::ios::binary);
    file.put('X');
    file.close();
    EXPECT_THROW((StateStore(directory, incarnation, true, true)), Exception);
    EXPECT_TRUE(std::filesystem::exists(directory / temporary));
}

TEST_F(UpsertDiskStateTest, OldSnapshotVerifiesItsPayloadAfterRetirement)
{
    String source;
    String identity;
    {
        StateStore store(directory, incarnation, false, true);
        auto first = prepare(store, {{"key", 10}});
        source = first.record.final_name;
        identity = objectName(first.record.part);
        commit(store, first);
    }
    StateStore reopened(directory, incarnation, true, true);
    const auto old = reopened.snapshot();
    auto truncated = reopened.prepareTruncate();
    reopened.append(truncated);
    reopened.publish(truncated);
    EXPECT_NO_THROW(old->verifyPart(source, identity));
}

TEST_F(UpsertDiskStateTest, NewPartLineageMustMatchItsCommitDespiteValidCRC)
{
    StateStore store(directory, incarnation, false, true);
    auto prepared = prepare(store, {{"key", 10}});
    const auto name = prepared.record.final_name;
    const auto path = directory / name / ROWS_FILE;
    commit(store, prepared);
    auto rows = decodePartRows(readBoundedFile(path, MAX_RECORD_BYTES));
    rows.front().key = "bad";
    const auto bytes = encodePartRows(rows);
    ASSERT_EQ(bytes.size(), std::filesystem::file_size(path));
    ASSERT_NO_THROW(decodePartRows(bytes));
    std::filesystem::remove(path);
    createDurableFile(path, bytes);
    try
    {
        store.loadPartRows(name);
        FAIL() << "Expected committed lineage hash rejection";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::CORRUPTED_DATA);
        EXPECT_NE(error.message().find("authenticated sidecar mismatch"), String::npos);
    }
    EXPECT_THROW(store.snapshot()->checkVerification(), Exception);
    EXPECT_THROW(store.checkAdmission(), Exception);
}

TEST_F(UpsertDiskStateTest, PreviouslyVerifiedLineageIsAuthenticatedAgainAtUse)
{
    String name;
    String identity;
    {
        StateStore store(directory, incarnation, false, true);
        auto prepared = prepare(store, {{"key", 10}});
        name = prepared.record.final_name;
        identity = objectName(prepared.record.part);
        commit(store, prepared);
    }
    StateStore reopened(directory, incarnation, true, true);
    reopened.snapshot()->verifyPart(name, identity);
    const auto path = directory / name / ROWS_FILE;
    auto rows = decodePartRows(readBoundedFile(path, MAX_RECORD_BYTES));
    rows.front().value.row.version = 20;
    const auto bytes = encodePartRows(rows);
    ASSERT_EQ(bytes.size(), std::filesystem::file_size(path));
    ASSERT_NO_THROW(decodePartRows(bytes));
    std::filesystem::remove(path);
    createDurableFile(path, bytes);
    try
    {
        reopened.loadPartRows(name);
        FAIL() << "Expected committed lineage hash rejection";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::CORRUPTED_DATA);
        EXPECT_NE(error.message().find("authenticated sidecar mismatch"), String::npos);
    }
    EXPECT_THROW(reopened.snapshot()->checkVerification(), Exception);
}

TEST_F(UpsertDiskStateTest, LineageReadDoesNotScanUnrelatedPayloadOrCompleteRecovery)
{
    String name;
    String identity;
    {
        StateStore store(directory, incarnation, false, true);
        auto prepared = prepare(store, {{"key", 10}}, "p", {}, true);
        name = prepared.record.final_name;
        identity = objectName(prepared.record.part);
        commit(store, prepared);
    }
    StateStore reopened(directory, incarnation, true, true);
    const auto path = directory / name / "data.bin";
    std::filesystem::remove(path);
    createDurableFile(path, "modified");
    const auto before = ProfileEvents::global_counters[ProfileEvents::UpsertPayloadVerificationParts];
    EXPECT_EQ(reopened.loadPartRows(name).size(), 1);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UpsertPayloadVerificationParts], before);
    EXPECT_NO_THROW(reopened.snapshot()->checkVerification());
    EXPECT_THROW(reopened.snapshot()->verifyPart(name, identity, true), Exception);
}

TEST_F(UpsertDiskStateTest, LineageOpenErrorPropagatesWithoutFencing)
{
    StateStore store(directory, incarnation, false, true);
    auto prepared = prepare(store, {{"key", 10}});
    const auto name = prepared.record.final_name;
    commit(store, prepared);
    const auto path = directory / name / ROWS_FILE;
    const auto saved = directory / "saved_rows.bin";
    std::filesystem::rename(path, saved);
    try
    {
        store.loadPartRows(name);
        FAIL() << "Expected sidecar open exception";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::CANNOT_OPEN_FILE);
    }
    EXPECT_NO_THROW(store.snapshot()->checkVerification());
    std::filesystem::rename(saved, path);
    EXPECT_EQ(store.loadPartRows(name).size(), 1);
}

#endif
