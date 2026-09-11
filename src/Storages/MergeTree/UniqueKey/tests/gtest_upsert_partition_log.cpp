#include <Storages/MergeTree/UniqueKey/UpsertPartitionLog.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Core/UUID.h>
#include <IO/WriteHelpers.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>

using namespace DB;
using namespace DB::Upsert;

class UpsertPartitionLogTest : public ::testing::Test
{
protected:
    std::filesystem::path directory;
    String path;

    void SetUp() override
    {
        directory = std::filesystem::path("tmp") / ("upsert_log_" + toString(UUIDHelpers::generateV4()));
        std::filesystem::create_directories(directory);
        path = (directory / "log").string();
    }
    void TearDown() override
    {
        std::filesystem::remove_all(directory);
    }
};

TEST_F(UpsertPartitionLogTest, DurableOrderAndRestart)
{
    {
        PartitionLog log(path, {1, 2}, "p", true);
        EXPECT_THROW(log.append(RecordKind::Noop, {1, 3}, "too early"), Exception);
        EXPECT_TRUE(log.recover().empty());
        EXPECT_EQ(log.append(RecordKind::Part, {1, 4}, "part and kills"), 1);
        EXPECT_EQ(log.append(RecordKind::Delete, {1, 5}, "delete"), 2);
    }
    PartitionLog reopened(path, {1, 2}, "p", false);
    const auto records = reopened.recover();
    ASSERT_EQ(records.size(), 2);
    EXPECT_EQ(records[0].payload, "part and kills");
    EXPECT_EQ(records[1].kind, RecordKind::Delete);
    EXPECT_EQ(reopened.append(RecordKind::Noop, {1, 6}, "rejected version"), 3);
}

TEST_F(UpsertPartitionLogTest, RepairOnlyIncompleteFinalFrame)
{
    {
        PartitionLog log(path, {1, 2}, "p", true);
        log.recover();
        log.append(RecordKind::Part, {3, 4}, "durable");
    }
    const auto original = readBoundedFile(path, 1000);
    {
        std::ofstream out(path, std::ios::binary | std::ios::app);
        out.write("UMTLOG01", 8);
    }
    PartitionLog reopened(path, {1, 2}, "p", false);
    ASSERT_EQ(reopened.recover().size(), 1);
    EXPECT_EQ(readBoundedFile(path, 1000), original);
    EXPECT_EQ(reopened.append(RecordKind::Noop, {3, 5}, "new"), 2);
}

TEST_F(UpsertPartitionLogTest, CorruptionFencesAndNeverSkipsAhead)
{
    {
        PartitionLog log(path, {1, 2}, "p", true);
        log.recover();
        log.append(RecordKind::Part, {3, 4}, "one");
        log.append(RecordKind::Part, {3, 5}, "two");
    }
    {
        std::fstream out(path, std::ios::binary | std::ios::in | std::ios::out);
        out.seekp(20);
        out.put('\x7f');
    }
    const auto before = readBoundedFile(path, 1000);
    PartitionLog reopened(path, {1, 2}, "p", false);
    EXPECT_THROW(reopened.recover(), Exception);
    EXPECT_TRUE(reopened.isFenced());
    EXPECT_THROW(reopened.append(RecordKind::Noop, {3, 6}, "must not append"), Exception);
    EXPECT_EQ(readBoundedFile(path, 1000), before);
}

TEST_F(UpsertPartitionLogTest, MissingLogsAndForeignIncarnationsFail)
{
    EXPECT_THROW((PartitionLog(path, {1, 2}, "p", false)), Exception);
    {
        PartitionLog log(path, {1, 2}, "p", true);
        log.recover();
        log.append(RecordKind::Noop, {1, 3}, "old incarnation");
    }
    PartitionLog wrong_table(path, {9, 9}, "p", false);
    EXPECT_THROW(wrong_table.recover(), Exception);
    EXPECT_TRUE(wrong_table.isFenced());
}

TEST_F(UpsertPartitionLogTest, ExclusiveOwnerAndAdmissionLimit)
{
    PartitionLog log(path, {1, 2}, "p", true, 100);
    log.recover();
    EXPECT_THROW((PartitionLog(path, {1, 2}, "p", false)), Exception);
    EXPECT_EQ(log.append(RecordKind::Noop, {1, 3}, "first"), 1);
    const auto before = readBoundedFile(path, 100);
    EXPECT_THROW(log.append(RecordKind::Noop, {1, 4}, "second"), Exception);
    EXPECT_FALSE(log.isFenced());
    EXPECT_EQ(readBoundedFile(path, 100), before);
}

#if USE_LIBFIU
TEST_F(UpsertPartitionLogTest, AmbiguousAppendAndSyncFailuresFenceAndReplay)
{
    for (const String point : {"upsert_log_throw_after_append", "upsert_log_throw_after_sync"})
    {
        const auto fault_path = (directory / point).string();
        {
            PartitionLog log(fault_path, {1, 2}, "p", true);
            log.recover();
            ASSERT_EQ(log.append(RecordKind::Noop, {1, 3}, "acknowledged"), 1);
            FailPointInjection::enableFailPoint(point);
            EXPECT_THROW(log.append(RecordKind::Part, {1, 4}, "indeterminate"), Exception);
            FailPointInjection::disableFailPoint(point);
            EXPECT_TRUE(log.isFenced());
            EXPECT_THROW(log.append(RecordKind::Noop, {1, 5}, "must not follow"), Exception);
        }
        PartitionLog reopened(fault_path, {1, 2}, "p", false);
        const auto records = reopened.recover();
        ASSERT_EQ(records.size(), 2);
        EXPECT_EQ(records[0].payload, "acknowledged");
        EXPECT_EQ(records[1].payload, "indeterminate");
        EXPECT_EQ(reopened.append(RecordKind::Noop, {1, 6}, "after recovery"), 3);
    }
}
#endif
