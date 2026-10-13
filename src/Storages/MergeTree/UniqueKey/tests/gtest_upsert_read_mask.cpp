#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/UniqueKey/UpsertSnapshot.h>
#include <Columns/ColumnsNumber.h>
#include <Common/logger_useful.h>
#include <gtest/gtest.h>

using namespace DB;

namespace DB
{
struct UpsertReadMaskTestPeer
{
    static MergeTreeRangeReader::ReadResult makeRows()
    {
        MergeTreeRangeReader::ReadResult result(getLogger("UpsertReadMaskTest"));
        auto values = ColumnUInt64::create();
        for (UInt64 value : {10, 11, 12, 13, 80, 81, 82})
            values->insertValue(value);
        result.columns.push_back(std::move(values));
        result.addGranule(4, {10, 1});
        result.addGranule(3, {80, 8});
        result.addRows(7);
        result.num_rows = 7;
        return result;
    }

    static void applyPredicate(MergeTreeRangeReader::ReadResult & result, ColumnPtr predicate)
    {
        result.optimize(FilterWithCachedCount(std::move(predicate)), true, true);
    }
};
}

namespace
{
MergeTreeRangeReader::ReadResult makeRows()
{
    return UpsertReadMaskTestPeer::makeRows();
}

std::vector<UInt64> values(const MergeTreeRangeReader::ReadResult & result)
{
    if (!result.num_rows)
        return {};
    const auto & column = assert_cast<const ColumnUInt64 &>(*result.columns.at(0));
    return {column.getData().begin(), column.getData().end()};
}
}

TEST(UpsertReadMask, NoncontiguousGranulesAndPartialGranule)
{
    for (bool incomplete : {false, true})
    {
        auto result = makeRows();
        DeleteBitmap dead;
        dead.addMany({9, 11, 13, 70, 81, 83});
        MergeTreeRangeReader::applyUpsertMask(result, dead, incomplete);
        EXPECT_EQ(values(result), (std::vector<UInt64>{10, 12, 80, 82}));
        EXPECT_EQ(result.num_rows, 4);
        EXPECT_EQ(result.numReadRows(), 7);
    }
}

TEST(UpsertReadMask, FullyDeadAndEmptyMasks)
{
    auto result = makeRows();
    DeleteBitmap empty;
    MergeTreeRangeReader::applyUpsertMask(result, empty, true);
    EXPECT_EQ(result.num_rows, 7);
    DeleteBitmap all;
    all.addMany({10, 11, 12, 13, 80, 81, 82});
    MergeTreeRangeReader::applyUpsertMask(result, all, true);
    EXPECT_EQ(result.num_rows, 0);
    EXPECT_TRUE(values(result).empty());
}

TEST(UpsertReadMask, LaterPredicateCannotRestoreDeadRows)
{
    auto result = makeRows();
    DeleteBitmap dead;
    dead.addMany({11, 81});
    MergeTreeRangeReader::applyUpsertMask(result, dead, true);
    auto predicate = ColumnUInt8::create();
    for (auto row : values(result))
        predicate->insertValue(row >= 12 && row < 82);
    UpsertReadMaskTestPeer::applyPredicate(result, std::move(predicate));
    EXPECT_EQ(values(result), (std::vector<UInt64>{12, 13, 80}));
}

TEST(UpsertReadMask, OldSnapshotKeepsItsMaskAfterPublication)
{
    auto state = std::make_shared<Upsert::SnapshotState>();
    state->partition_csns.emplace("p", 1);
    state->part_masks.emplace("part_uuid", std::make_shared<const DeleteBitmap>());
    const Upsert::SnapshotPtr old = state;

    auto next = std::make_shared<Upsert::SnapshotState>(*state);
    auto kills = std::make_shared<DeleteBitmap>();
    kills->addMany({11, 81});
    next->part_masks.at("part_uuid") = std::move(kills);
    next->partition_csns.at("p") = 2;
    state = std::move(next);

    auto old_rows = makeRows();
    auto new_rows = makeRows();
    MergeTreeRangeReader::applyUpsertMask(old_rows, *old->part_masks.at("part_uuid"), true);
    MergeTreeRangeReader::applyUpsertMask(new_rows, *state->part_masks.at("part_uuid"), true);
    EXPECT_EQ(old_rows.num_rows, 7);
    EXPECT_EQ(new_rows.num_rows, 5);
    EXPECT_EQ(old->partition_csns.at("p"), 1);
    EXPECT_EQ(state->partition_csns.at("p"), 2);
}

TEST(UpsertReadMask, AllVisibilityCombinationsPreservePredicates)
{
    const std::vector<UInt64> input{10, 11, 12, 13, 80, 81, 82};
    for (bool incomplete : {false, true})
    {
        for (size_t bits = 0; bits < (1U << input.size()); ++bits)
        {
            auto result = makeRows();
            DeleteBitmap dead;
            /// Keep a non-empty mask even when none of the rows read are hidden.
            dead.add(500);
            std::vector<UInt64> expected;
            for (size_t row = 0; row < input.size(); ++row)
            {
                if (bits & (1U << row))
                    dead.add(input[row]);
                else
                    expected.push_back(input[row]);
            }
            MergeTreeRangeReader::applyUpsertMask(result, dead, incomplete);
            ASSERT_EQ(values(result), expected) << bits << ":" << incomplete;
            if (!result.num_rows)
                continue;
            auto predicate = ColumnUInt8::create();
            for (const auto value : values(result))
                predicate->insertValue(value % 2 == 0);
            UpsertReadMaskTestPeer::applyPredicate(result, std::move(predicate));
            std::erase_if(expected, [](UInt64 value) { return value % 2 != 0; });
            EXPECT_EQ(values(result), expected) << bits << ":" << incomplete;
        }
    }
}
