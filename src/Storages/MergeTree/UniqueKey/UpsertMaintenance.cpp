#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/UniqueKey/UpsertStateStore.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/UUID.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <filesystem>

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int LIMIT_EXCEEDED;
}
namespace DB::FailPoints
{
extern const char upsert_pause_before_merge_commit[];
}

namespace DB
{
void StorageMergeTree::runUpsertMaintenance()
{
    /// Each task handles a bounded unit; native merges remain disabled for this protocol.
    constexpr size_t max_source_bytes = 32U << 20;
    constexpr size_t max_sources = 16;
    bool did_work = false;
    try
    {
        auto table_lock = lockForShare(RWLockImpl::NO_QUERY, Poco::Timespan(5, 0));
        {
            auto lock = lockParts();
            if (upsert_fenced)
                return;
        }
        clearOldPartsFromFilesystem(false, false);
        {
            auto lock = lockParts();
            upsert_store->pruneRetired();
        }

        DataPartsVector sources;
        Upsert::SnapshotPtr snapshot;
        String partition;
        std::vector<Upsert::PartRows> lineage;
        const auto metadata = getInMemoryMetadataPtr(getContext(), true);
        {
            auto lock = lockParts();
            if (upsert_fenced)
                return;
            size_t bytes = 0;
            size_t rows = 0;
            bool needs_rewrite = false;
            auto worthy = [&] { return sources.size() >= 2 || needs_rewrite; };
            for (const auto & candidate : getDataPartsStateRange(DataPartState::Active))
            {
                const auto & id = candidate->info.getPartitionId();
                if (id != partition)
                {
                    if (worthy())
                        break;
                    sources.clear();
                    bytes = 0;
                    rows = 0;
                    needs_rewrite = false;
                    partition = id;
                }
                const auto size = candidate->getBytesUncompressedOnDisk();
                if (size > max_source_bytes || candidate->rows_count > Upsert::MAX_UNIT_ROWS)
                    continue;
                if (bytes + size > max_source_bytes || rows + candidate->rows_count > Upsert::MAX_UNIT_ROWS)
                {
                    if (worthy())
                        break;
                    continue;
                }
                const auto mask = upsert_store->snapshot()->getMask(candidate->getDeleteBitmapCacheIdentity());
                if (!mask)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance source lacks a mask");
                const bool rewrite = mask->cardinality() > 0 && mask->cardinality() * 4 >= candidate->rows_count;
                /// A healthy target-sized part must not block selection of later small parts.
                if (!rewrite && (size > max_source_bytes / 2 || candidate->rows_count > Upsert::MAX_UNIT_ROWS / 2))
                    continue;
                needs_rewrite |= rewrite;
                sources.push_back(candidate);
                bytes += size;
                rows += candidate->rows_count;
                if (sources.size() == max_sources)
                    break;
            }
            if (!worthy())
                sources.clear();
            snapshot = upsert_store->snapshot();
            for (const auto & source : sources)
                lineage.push_back(upsert_store->loadPartRows(source->name));
        }

        if (!sources.empty())
        {
            auto context = Context::createCopy(getContext());
            size_t largest_encoded_key = 0;
            for (const auto & part_rows : lineage)
                for (const auto & row : part_rows)
                    largest_encoded_key = std::max(largest_encoded_key, row.key.size());
            /// Maintenance preserves already admitted keys, independently of a foreground query's size setting.
            context->setSetting("unique_key_max_encoded_size", static_cast<UInt64>(largest_encoded_key));
            auto storage_snapshot = std::make_shared<StorageSnapshot>(*this, metadata);
            const auto columns = metadata->getColumns().getAllPhysical();
            Block output = metadata->getSampleBlock().cloneEmpty();
            auto output_columns = output.mutateColumns();
            auto ordinals = ColumnUInt64::create();
            auto row_lineage = ColumnString::create();
            std::vector<String> source_names;
            for (size_t source_index = 0; source_index < sources.size(); ++source_index)
            {
                const auto & source = sources[source_index];
                source_names.push_back(source->name);
                const auto mask = snapshot->getMask(source->getDeleteBitmapCacheIdentity());
                QueryPipeline pipeline(createMergeTreeSequentialSource(
                    MergeTreeSequentialSourceType::Merge, *this, storage_snapshot, RangesInDataPart(source),
                    std::make_shared<AlterConversions>(), nullptr, columns.getNames(), std::nullopt, nullptr,
                    false, false, false));
                PullingPipelineExecutor executor(pipeline);
                Block block;
                size_t offset = 0;
                while (executor.pull(block))
                {
                    if (offset + block.rows() > lineage[source_index].size())
                        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance read exceeds source lineage");
                    IColumn::Filter keep(block.rows(), 0);
                    size_t retained = 0;
                    for (size_t row = 0; row < block.rows(); ++row)
                    {
                        if (mask->contains(static_cast<UInt32>(offset + row)))
                            continue;
                        keep[row] = 1;
                        ++retained;
                        const auto & original = lineage[source_index][offset + row].value;
                        ordinals->insertValue(original.row.block_ordinal);
                        const auto encoded = Upsert::encodeIndexValue(original);
                        row_lineage->insertData(encoded.data(), encoded.size());
                    }
                    for (size_t column = 0; column < output.columns(); ++column)
                    {
                        auto filtered = block.getByName(output.getByPosition(column).name).column->filter(keep, retained);
                        filtered = filtered->convertToFullIfWrapped();
                        output_columns[column]->insertRangeFrom(*filtered, 0, retained);
                    }
                    offset += block.rows();
                }
                if (offset != source->rows_count)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance read omitted source rows");
            }
            output.setColumns(std::move(output_columns));
            if (output.bytes() > max_source_bytes || output.rows() > Upsert::MAX_UNIT_ROWS)
                throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree maintenance output exceeds its operation budget");

            MergeTreeTemporaryPartPtr temporary;
            Upsert::PartCommit authorization;
            Upsert::PartRows output_rows;
            authorization.part = Upsert::newObjectID();
            if (output.rows())
            {
                output.insert({std::move(ordinals), std::make_shared<DataTypeUInt64>(), Upsert::ORDINAL_COLUMN});
                output.insert({std::move(row_lineage), std::make_shared<DataTypeString>(), Upsert::LINEAGE_COLUMN});
                BlockWithPartition block;
                block.block = std::make_shared<Block>(std::move(output));
                block.partition = sources.front()->partition;
                temporary = writer.writeTempPart(block, metadata, context, false);
                temporary->finalize();
                auto & part = temporary->part;
                part->getDataPartStorage().commitTransaction();
                const auto path = part->getDataPartStorage().getFullPath();
                output_rows = Upsert::decodePartRows(Upsert::readBoundedFile(std::filesystem::path(path) / Upsert::ROWS_FILE, Upsert::MAX_RECORD_BYTES));
                authorization.part = {UUIDHelpers::getHighBytes(part->uuid), UUIDHelpers::getLowBytes(part->uuid)};
                authorization.temporary_name = part->getDataPartStorage().getPartDirectory();
                authorization.files_hash = Upsert::hashPartDirectory(path, true);
                authorization.rows = part->rows_count;
                Upsert::syncDirectory(std::filesystem::path(path) / "..");
            }
            FailPointInjection::pauseFailPoint(FailPoints::upsert_pause_before_merge_commit);
            auto lock = lockParts();
            if (upsert_fenced)
                return;
            if (temporary)
            {
                auto holder = fillNewPartName(temporary->part, lock);
                authorization.final_name = temporary->part->name;
            }
            auto prepared = upsert_store->prepareMerge(partition, std::move(authorization), output_rows, source_names);
            try
            {
                if (prepared.produces_part)
                    commitUpsertPart(temporary->part, prepared.snapshot, [&] { upsert_store->append(prepared); }, lock);
                else
                    upsert_store->append(prepared);
                removePartsFromWorkingSet(nullptr, sources, true, lock);
                upsert_store->publish(prepared);
                upsert_state = upsert_store->snapshot();
                did_work = true;
            }
            catch (...)
            {
                upsert_fenced = true;
                throw;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "UpsertMergeTree maintenance failed");
    }
    upsert_maintenance_task->scheduleAfter(did_work ? 100 : 1000);
}
}
