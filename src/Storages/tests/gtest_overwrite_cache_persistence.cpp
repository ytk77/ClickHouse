#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskLocal.h>
#include <Storages/OverwriteCachePersistence.h>
#include <Common/tests/gtest_global_context.h>

#include <atomic>
#include <filesystem>
#include <unistd.h>

namespace DB
{
namespace
{

class CheckpointCountingDisk : public DiskLocal
{
public:
    explicit CheckpointCountingDisk(const String & path_) : DiskLocal("overwrite_cache_test", path_)
    {
    }

    void replaceFile(const String & from, const String & to) override
    {
        DiskLocal::replaceFile(from, to);
        if (to == "table/manifest")
            ++checkpoints;
    }

    std::atomic<size_t> checkpoints{0};
};

class OverwriteCachePersistenceTest : public testing::Test
{
protected:
    void SetUp() override
    {
        getContext();
        path = (std::filesystem::current_path() / "tmp"
            / ("overwrite-cache-persistence-" + std::to_string(getpid()) + "-"
                + testing::UnitTest::GetInstance()->current_test_info()->name())).string() + "/";
        ASSERT_FALSE(std::filesystem::exists(path));
        std::filesystem::create_directories(path);
        ASSERT_TRUE(std::filesystem::is_directory(path));
        disk = std::make_shared<CheckpointCountingDisk>(path);
        header.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "key"});
        persistence = makePersistence();
        persistence->load([](auto &&) {});
        persistence->start();
    }

    void TearDown() override
    {
        persistence.reset();
        disk.reset();
        std::filesystem::remove_all(path);
    }

    std::unique_ptr<OverwriteCachePersistence> makePersistence()
    {
        return std::make_unique<OverwriteCachePersistence>(
            OverwriteCachePersistMode::Sync, disk, "table/", header, "test", "OverwriteCachePersistenceTest");
    }

    void addSegment(UInt64 id, std::vector<UInt64> removed = {})
    {
        auto column = ColumnUInt64::create();
        column->insertValue(id);
        OverwriteCachePersistence::Commit commit;
        commit.generation = ++generation;
        commit.added.push_back({id, {std::move(column)}, 1});
        commit.removed = std::move(removed);
        persistence->waitDurable(persistence->enqueue(std::move(commit)));
    }

    void appendDeletions(size_t count, size_t key_bytes = 8)
    {
        UInt64 sequence = 0;
        for (size_t i = 0; i < count; ++i)
        {
            OverwriteCachePersistence::Commit commit;
            commit.generation = ++generation;
            commit.deleted_keys.emplace_back(key_bytes, 'x');
            sequence = persistence->enqueue(std::move(commit));
        }
        persistence->waitDurable(sequence);
    }

    String path;
    Block header;
    std::shared_ptr<CheckpointCountingDisk> disk;
    std::unique_ptr<OverwriteCachePersistence> persistence;
    UInt64 generation = 0;
};

TEST_F(OverwriteCachePersistenceTest, RetainedTombstonesDoNotRetriggerCheckpoint)
{
    addSegment(1);
    appendDeletions(1, 1024 * 1024);
    appendDeletions(4095);
    ASSERT_EQ(disk->checkpoints, 1);

    /// More than the record threshold of tiny commits still cannot pay for rewriting the retained keys.
    appendDeletions(4097);
    EXPECT_EQ(disk->checkpoints, 1);

    persistence.reset();
    persistence = makePersistence();
    size_t deletions = 0;
    size_t segments = 0;
    persistence->load([&](auto && record)
    {
        deletions += record.deleted_keys.size();
        if (record.segment_id)
        {
            EXPECT_EQ(record.segment_id, 1);
            ++segments;
        }
    });
    EXPECT_EQ(deletions, 8193);
    EXPECT_EQ(segments, 1);
    persistence->start();
    appendDeletions(1);
    EXPECT_EQ(disk->checkpoints, 1);

    /// Once the old segment is retired, forced backup checkpointing can safely remove its tombstones.
    addSegment(2, {1});
    {
        OverwriteCachePersistence::BackupPin pin(*persistence);
        const auto files = persistence->collectFilesForBackup();
        EXPECT_EQ(files.size(), 2);
    }
    EXPECT_EQ(disk->checkpoints, 2);
    persistence.reset();
    persistence = makePersistence();
    size_t records = 0;
    persistence->load([&](auto && record)
    {
        EXPECT_TRUE(record.deleted_keys.empty());
        EXPECT_EQ(record.segment_id, 2);
        ++records;
    });
    EXPECT_EQ(records, 1);
}

TEST_F(OverwriteCachePersistenceTest, CheckpointResumesAfterEnoughNewWork)
{
    addSegment(1);
    appendDeletions(4096);
    ASSERT_EQ(disk->checkpoints, 1);
    appendDeletions(4098);
    EXPECT_EQ(disk->checkpoints, 2);
    persistence->truncate();
    addSegment(1);
    appendDeletions(4096);
    EXPECT_EQ(disk->checkpoints, 3);
}

TEST_F(OverwriteCachePersistenceTest, StoppedWriterRejectsCommit)
{
    persistence->shutdown();
    OverwriteCachePersistence::Commit commit;
    commit.deleted_keys.emplace_back("key");
    EXPECT_THROW(persistence->enqueue(std::move(commit)), Exception);
}

TEST(OverwriteCachePersistence, QueueAccountsForDeletionPayloads)
{
    OverwriteCachePersistence::Commit commit;
    const auto empty_bytes = commit.allocatedBytes();
    commit.deleted_keys.emplace_back(1024 * 1024, 'x');
    EXPECT_GE(commit.allocatedBytes(), empty_bytes + 1024 * 1024 + sizeof(String));
    commit.removed.reserve(10000);
    EXPECT_GE(commit.allocatedBytes(), empty_bytes + 1024 * 1024 + 10000 * sizeof(UInt64));
}

}
}
