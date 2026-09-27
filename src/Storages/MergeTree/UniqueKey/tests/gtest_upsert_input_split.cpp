#include <Storages/MergeTree/UniqueKey/UpsertPartData.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>
#include <gtest/gtest.h>

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
