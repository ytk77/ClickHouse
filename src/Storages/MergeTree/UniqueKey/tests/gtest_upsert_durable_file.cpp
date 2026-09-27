#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Common/Exception.h>
#include <Core/UUID.h>
#include <IO/WriteHelpers.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>

using namespace DB;
using namespace DB::Upsert;

class UpsertDurableFileTest : public ::testing::Test
{
protected:
    std::filesystem::path directory;

    void SetUp() override
    {
        directory = std::filesystem::path("tmp") / ("upsert_durable_" + toString(UUIDHelpers::generateV4()));
        std::filesystem::create_directories(directory);
    }

    void TearDown() override
    {
        std::filesystem::remove_all(directory);
    }
};

TEST_F(UpsertDurableFileTest, CreateReadAndRefuseOverwrite)
{
    const auto path = (directory / "manifest").string();
    const String data("abc\0def", 7);
    createDurableFile(path, data);
    EXPECT_EQ(readBoundedFile(path, data.size()), data);
    EXPECT_THROW(createDurableFile(path, "replacement"), Exception);
    EXPECT_EQ(readBoundedFile(path, data.size()), data);
    EXPECT_THROW(readBoundedFile(path, data.size() - 1), Exception);
}

TEST_F(UpsertDurableFileTest, MissingRequiredFileAndDirectoryAreErrors)
{
    EXPECT_THROW(readBoundedFile((directory / "missing").string(), 100), Exception);
    EXPECT_THROW(syncDirectory((directory / "missing").string()), Exception);
    EXPECT_THROW(createDurableFile((directory / "missing" / "file").string(), "payload"), Exception);
    EXPECT_THROW(readBoundedFile(directory.string(), 10000), Exception);
}

TEST_F(UpsertDurableFileTest, RejectSymbolicLinks)
{
    const auto target = directory / "target";
    const auto link = directory / "link";
    createDurableFile(target.string(), "original");
    std::filesystem::create_symlink("target", link);
    EXPECT_THROW(readBoundedFile(link.string(), 100), Exception);
    EXPECT_THROW(createDurableFile(link.string(), "changed"), Exception);
    EXPECT_EQ(readBoundedFile(target.string(), 100), "original");
}

TEST_F(UpsertDurableFileTest, EmptyFileAndExplicitDirectoryBarrier)
{
    const auto path = (directory / "empty").string();
    createDurableFile(path, {});
    EXPECT_EQ(readBoundedFile(path, 0), "");
    EXPECT_NO_THROW(syncDirectory(directory.string()));
    EXPECT_THROW(syncDirectory(path), Exception);
}

TEST_F(UpsertDurableFileTest, GroupedFileBarriersPropagateErrors)
{
    const auto first = (directory / "first").string();
    const auto second = (directory / "second").string();
    createDurableFile(first, "one");
    createDurableFile(second, "two");
    EXPECT_NO_THROW(syncFiles({first, second}));
    EXPECT_THROW(syncFiles({first, (directory / "missing").string(), second}), Exception);
    const auto link = directory / "link";
    std::filesystem::create_symlink("second", link);
    EXPECT_THROW(syncFiles({first, link.string()}), Exception);
    EXPECT_EQ(readBoundedFile(first, 3), "one");
    EXPECT_EQ(readBoundedFile(second, 3), "two");
}
