#include <Storages/MergeTree/UniqueKey/UpsertPartData.h>
#include <Storages/MergeTree/UniqueKey/UpsertInputFold.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>
#include <gtest/gtest.h>
#include <array>
#include <map>
#include <random>
#include <stdexcept>

using namespace DB;
using namespace DB::Upsert;

TEST(UpsertInputSplit, ExactLimitKeepsOriginalColumns)
{
    auto column = ColumnUInt64::create();
    for (UInt64 value = 0; value < 8; ++value)
        column->insertValue(value);
    Block input{{std::move(column), std::make_shared<DataTypeUInt64>(), "id"}};
    auto result = cutInputBlock(input, 0, 8, 64);
    EXPECT_EQ(result.getByPosition(0).column.get(), input.getByPosition(0).column.get());
    EXPECT_EQ(result.rows(), 8);
}

TEST(UpsertInputSplit, RowAndByteBoundsPreserveEveryValue)
{
    auto column = ColumnUInt64::create();
    for (UInt64 value = 0; value < 19; ++value)
        column->insertValue(value);
    Block input{{std::move(column), std::make_shared<DataTypeUInt64>(), "id"}};
    for (const size_t max_rows : {3, 20})
    {
        size_t offset = 0;
        while (offset < input.rows())
        {
            auto result = cutInputBlock(input, offset, max_rows, 64);
            ASSERT_GT(result.rows(), 0);
            EXPECT_LE(result.rows(), max_rows);
            EXPECT_LE(result.bytes(), 64);
            for (size_t i = 0; i < result.rows(); ++i)
                EXPECT_EQ(result.getByPosition(0).column->getUInt(i), offset + i);
            offset += result.rows();
        }
        EXPECT_EQ(offset, input.rows());
    }
}

TEST(UpsertInputSplit, UnevenStringsUseActualRowSizes)
{
    auto column = ColumnString::create();
    for (const size_t length : {1, 1, 200, 1, 50})
        column->insertData(String(length, 'x').data(), length);
    Block input{{std::move(column), std::make_shared<DataTypeString>(), "value"}};
    size_t offset = 0;
    while (offset < input.rows())
    {
        auto result = cutInputBlock(input, offset, 100, 220);
        ASSERT_GT(result.rows(), 0);
        EXPECT_LE(result.bytes(), 220);
        for (size_t i = 0; i < result.rows(); ++i)
            EXPECT_EQ(result.getByPosition(0).column->getDataAt(i), input.getByPosition(0).column->getDataAt(offset + i));
        offset += result.rows();
    }
}

TEST(UpsertInputSplit, RejectIndivisibleOversizedRow)
{
    auto column = ColumnString::create();
    column->insertData("ok", 2);
    column->insertData(String(256, 'x').data(), 256);
    Block input{{std::move(column), std::make_shared<DataTypeString>(), "value"}};
    EXPECT_EQ(cutInputBlock(input, 0, 100, 64).rows(), 1);
    EXPECT_THROW(cutInputBlock(input, 1, 100, 64), Exception);
}

TEST(UpsertInputSplit, CompactLowCardinalityDictionary)
{
    auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto column = type->createColumn();
    for (UInt64 value = 0; value < 20; ++value)
        column->insert(String(100, static_cast<char>('a' + value)));
    Block input{{std::move(column), type, "value"}};
    ASSERT_GT(input.bytes(), 220);
    size_t offset = 0;
    while (offset < input.rows())
    {
        auto result = cutInputBlock(input, offset, 100, 220);
        ASSERT_GT(result.rows(), 0);
        EXPECT_LE(result.bytes(), 220);
        for (size_t i = 0; i < result.rows(); ++i)
            EXPECT_EQ((*result.getByPosition(0).column)[i], (*input.getByPosition(0).column)[offset + i]);
        offset += result.rows();
    }
}

TEST(UpsertInputSplit, ConstantAndNestedColumns)
{
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    auto column = type->createColumn();
    for (UInt64 value = 0; value < 12; ++value)
        column->insert(Array{Field(value), Field(value + 1)});
    auto constant = ColumnString::create();
    constant->insertData("constant", 8);
    Block input{
        {std::move(column), type, "values"},
        {ColumnConst::create(std::move(constant), 12), std::make_shared<DataTypeString>(), "constant"}};
    size_t offset = 0;
    while (offset < input.rows())
    {
        auto result = cutInputBlock(input, offset, 100, 100);
        ASSERT_GT(result.rows(), 0);
        EXPECT_LE(result.bytes(), 100);
        for (size_t i = 0; i < result.rows(); ++i)
        {
            EXPECT_EQ((*result.getByPosition(0).column)[i], (*input.getByPosition(0).column)[offset + i]);
            EXPECT_EQ(result.getByPosition(1).column->getDataAt(i), "constant");
        }
        offset += result.rows();
    }
}

namespace
{
struct ConstantInputHash
{
    size_t operator()(std::string_view) const
    {
        return 0;
    }
};

template <typename Hash = StringViewHash64>
void compareInputFoldWithTree(const std::vector<String> & keys, bool versioned, Hash hash = {})
{
    std::mt19937_64 random(0x615EED);
    std::vector<UInt64> versions(keys.size()), ordinals(keys.size());
    for (size_t row = 0; row < keys.size(); ++row)
    {
        versions[row] = random() % 7;
        ordinals[row] = random();
    }
    auto wins = [&](size_t row, size_t old)
    {
        return !versioned || versions[row] > versions[old]
            || (versions[row] == versions[old] && ordinals[row] > ordinals[old]);
    };
    std::map<String, size_t> tree;
    for (size_t row = 0; row < keys.size(); ++row)
    {
        auto [it, inserted] = tree.emplace(keys[row], row);
        if (!inserted && wins(row, it->second))
            it->second = row;
    }
    auto check = []
    {
    };
    const auto winners = selectInputWinners(std::span<const String>(keys), wins, check, hash);
    ASSERT_EQ(winners.size(), tree.size());
    std::vector<size_t> kept;
    size_t lineage_bytes = PART_ROWS_ENVELOPE_BYTES;
    for (const auto & [key, row] : winners)
    {
        ASSERT_EQ(tree.at(String(key)), row);
        kept.push_back(row);
        lineage_bytes += key.size() + 8 + (versioned ? 50 : 42);
    }
    std::sort(kept.begin(), kept.end());
    std::vector<size_t> expected;
    size_t expected_bytes = PART_ROWS_ENVELOPE_BYTES;
    for (const auto & [key, row] : tree)
    {
        expected.push_back(row);
        expected_bytes += key.size() + 8 + (versioned ? 50 : 42);
    }
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(kept, expected);
    EXPECT_EQ(lineage_bytes, expected_bytes);
}
}

TEST(UpsertInputFold, DifferentialTreeOracle)
{
    std::mt19937_64 random(0x615EED);
    for (size_t cardinality : {size_t{1}, size_t{73}, size_t{15000}})
    {
        std::vector<String> keys;
        for (size_t row = 0; row < 15000; ++row)
        {
            const auto value = cardinality == 15000 ? row : random() % cardinality;
            keys.push_back(String(value % 80, 'k') + String("\0", 1) + std::to_string(value));
        }
        for (bool versioned : {false, true})
            compareInputFoldWithTree(keys, versioned);
    }
    for (bool versioned : {false, true})
    {
        compareInputFoldWithTree({}, versioned);
        compareInputFoldWithTree({"", String("a\0b", 3), String("a\0c", 3), "", String(32768, 'x')}, versioned);
    }
}

TEST(UpsertInputFold, FullEqualitySurvivesForcedHashCollisions)
{
    std::vector<String> keys;
    for (size_t row = 0; row < 600; ++row)
        keys.push_back(std::to_string(row % 257) + String("\0", 1) + String((row % 257) % 9, 'k'));
    for (bool versioned : {false, true})
        compareInputFoldWithTree(keys, versioned, ConstantInputHash{});
}

TEST(UpsertInputFold, PeriodicCancellationIsPropagated)
{
    std::vector<String> keys(2 * 65536 + 1, "key");
    size_t checks = 0;
    auto wins = [](size_t, size_t)
    {
        return true;
    };
    auto check = [&]
    {
        ++checks;
    };
    const auto winners = selectInputWinners(std::span<const String>(keys), wins, check);
    EXPECT_EQ(checks, 3);
    EXPECT_EQ(winners.at("key"), keys.size() - 1);
    checks = 0;
    EXPECT_THROW(selectInputWinners(std::span<const String>(keys), wins, [&]
    {
        if (++checks == 2)
            throw std::runtime_error("cancel fold");
    }), std::runtime_error);
    EXPECT_EQ(checks, 2);
    EXPECT_EQ(keys.size(), 2 * 65536 + 1);
}

TEST(UpsertInputFold, PhysicalOrderAndEqualVersionOrdinals)
{
    StorageInMemoryMetadata metadata;
    metadata.unique_key.column_names = {"id"};
    for (size_t mode = 0; mode < 3; ++mode)
    {
        auto ids = ColumnUInt64::create();
        auto ordinals = ColumnUInt64::create();
        auto physical = ColumnUInt64::create();
        auto versions = ColumnUInt64::create();
        auto ticks = ColumnDecimal<DateTime64>::create(0, 3);
        const std::array<UInt64, 5> input_ids{1, 1, 2, 2, 1};
        const std::array<UInt64, 5> input_ordinals{9, 1, 7, 2, 9};
        const std::array<UInt64, 5> input_versions{5, 5, 2, 3, 5};
        const std::array<Int64, 5> input_ticks{-1, -1, -2, -1, -1};
        for (size_t row = 0; row < input_ids.size(); ++row)
        {
            ids->insertValue(input_ids[row]);
            ordinals->insertValue(input_ordinals[row]);
            physical->insertValue(row);
            versions->insertValue(input_versions[row]);
            ticks->getData().push_back(DateTime64(input_ticks[row]));
        }
        auto uint_type = std::make_shared<DataTypeUInt64>();
        Block block{{std::move(ids), uint_type, "id"}, {std::move(ordinals), uint_type, ORDINAL_COLUMN},
            {std::move(physical), uint_type, "physical"}};
        if (mode == 2)
            block.insert({std::move(ticks), std::make_shared<DataTypeDateTime64>(3), "v"});
        else
            block.insert({std::move(versions), uint_type, "v"});
        const auto lineage_bytes = foldBlock(block, metadata, mode ? "v" : "", 256);
        ASSERT_EQ(block.rows(), 2);
        EXPECT_EQ(lineage_bytes, PART_ROWS_ENVELOPE_BYTES + 2 * (8 + 8 + (mode ? 50 : 42)));
        const auto & kept = *block.getByName("physical").column;
        EXPECT_EQ(kept.getUInt(0), mode ? 0 : 3);
        EXPECT_EQ(kept.getUInt(1), mode ? 3 : 4);
    }
}
