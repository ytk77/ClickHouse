#include <Storages/MergeTree/UniqueKey/UpsertSink.h>
#include <Storages/MergeTree/UniqueKey/UpsertStateStore.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <filesystem>

namespace DB::Setting
{
extern const SettingsUInt64 max_partitions_per_insert_block;
extern const SettingsUInt64 unique_key_max_encoded_size;
extern const SettingsString insert_deduplication_token;
extern const SettingsUInt64 input_format_max_block_wait_ms;
}
namespace DB::ErrorCodes
{
extern const int SUPPORT_IS_DISABLED;
extern const int FAULT_INJECTED;
extern const int LOGICAL_ERROR;
extern const int LIMIT_EXCEEDED;
}
namespace DB::FailPoints
{
extern const char upsert_throw_after_rename[];
extern const char upsert_throw_before_publish[];
}

namespace DB::Upsert
{
Sink::Sink(StorageMergeTree & storage_, StorageMetadataPtr metadata_, ContextPtr context_)
    : SinkToStorage(std::make_shared<const Block>(metadata_->getSampleBlock())), storage(storage_), metadata(std::move(metadata_)), context(std::move(context_))
{
    if (context->getSettingsRef()[Setting::input_format_max_block_wait_ms])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree does not support streaming inserts");
}

String Sink::getName() const
{
    return "UpsertMergeTreeSink";
}

void Sink::setHasDependentMaterializedViews(bool has_views)
{
    if (has_views)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree incremental materialized views require retractions");
}

void Sink::consume(Chunk & chunk)
{
    if (!chunk.getNumRows())
        return;
    auto block = getHeader().cloneWithColumns(chunk.getColumns());
    if (block.rows() > MAX_UNIT_ROWS || block.bytes() > MAX_UNIT_BYTES)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree input block exceeds {} rows or {} bytes", MAX_UNIT_ROWS, MAX_UNIT_BYTES);
    const String client_token = context->getSettingsRef()[Setting::insert_deduplication_token];
    if (client_token.size() > 1024)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree deduplication token exceeds 1024 bytes");
    auto ordinals = ColumnUInt64::create();
    for (size_t row = 0; row < block.rows(); ++row)
        ordinals->insertValue(row);
    const auto & settings = context->getSettingsRef();
    IColumn::Selector selector;
    auto partitions = MergeTreeDataWriter::splitBlockIntoParts(
        std::move(block), settings[Setting::max_partitions_per_insert_block], metadata, context, &selector);
    VectorWithMemoryTracking<MutableColumnPtr> partition_ordinals;
    if (selector.empty())
        partition_ordinals.push_back(std::move(ordinals));
    else
        partition_ordinals = static_cast<const IColumn &>(*ordinals).scatter(partitions.size(), selector);
    for (size_t i = 0; i < partitions.size(); ++i)
        partitions[i].block->insert({std::move(partition_ordinals[i]), std::make_shared<DataTypeUInt64>(), ORDINAL_COLUMN});
    /// Validate and fold every partition before committing the first one from this input block.
    std::vector<Token> tokens;
    tokens.reserve(partitions.size());
    for (auto & partition : partitions)
    {
        tokens.push_back({client_token, unit, hashInputBlock(*partition.block), 0});
        foldBlock(*partition.block, *metadata, storage.merging_params.upsert_version_column, settings[Setting::unique_key_max_encoded_size]);
    }
    for (size_t i = 0; i < partitions.size(); ++i)
        storage.insertUpsertBlock(partitions[i], metadata, context, std::move(tokens[i]));
    ++unit;
}
}

namespace DB
{
void StorageMergeTree::insertUpsertBlock(BlockWithPartition & block, const StorageMetadataPtr & metadata, ContextPtr query_context, Upsert::Token token)
{
    const String partition_id = block.partition.getID(metadata->getPartitionKey().sample_block);
    {
        auto lock = lockParts();
        if (upsert_fenced)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree is fenced pending restart and recovery");
        if (upsert_store->tokenAlreadyCommitted(partition_id, token))
            return;
        upsert_store->checkAdmission();
    }
    auto temporary = writer.writeTempPart(block, metadata, query_context, false);
    temporary->finalize();
    auto & part = temporary->part;
    if (!part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree writer returned an empty part");
    part->getDataPartStorage().commitTransaction();
    const auto path = part->getDataPartStorage().getFullPath();
    const auto rows = Upsert::decodePartRows(Upsert::readBoundedFile(std::filesystem::path(path) / Upsert::ROWS_FILE, Upsert::MAX_RECORD_BYTES));
    Upsert::PartCommit authorization;
    authorization.part = {UUIDHelpers::getHighBytes(part->uuid), UUIDHelpers::getLowBytes(part->uuid)};
    authorization.temporary_name = part->getDataPartStorage().getPartDirectory();
    authorization.files_hash = Upsert::hashPartDirectory(path, true);
    authorization.rows = part->rows_count;
    authorization.token = std::move(token);
    Upsert::syncDirectory(std::filesystem::path(path) / "..");

    auto lock = lockParts();
    if (upsert_fenced)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree is fenced pending restart and recovery");
    auto block_holder = fillNewPartName(part, lock);
    authorization.final_name = part->name;
    Upsert::StateStore::Prepared prepared;
    try
    {
        prepared = upsert_store->prepare(partition_id, std::move(authorization), rows);
    }
    catch (...)
    {
        if (upsert_store->isFenced())
            upsert_fenced = true;
        throw;
    }
    if (prepared.replayed_token)
        return;
    try
    {
        if (prepared.produces_part)
            commitUpsertPart(part, prepared.snapshot, [&]()
            {
                upsert_store->append(prepared);
            }, lock);
        else
            upsert_store->append(prepared);
        upsert_store->publish(prepared);
        upsert_state = upsert_store->snapshot();
    }
    catch (...)
    {
        upsert_fenced = true;
        throw;
    }
}

void MergeTreeData::commitUpsertPart(MutableDataPartPtr & part, Upsert::SnapshotPtr snapshot,
    const std::function<void()> & persist, DataPartsLock & lock)
{
    Transaction transaction(*this, nullptr);
    try
    {
        /// Allocate the index node, transaction bookkeeping and column counters before authorization.
        if (!renameTempPartAndAdd(part, transaction, lock, true))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree part could not be prepared");
        part->assertHasVersionMetadata(nullptr);
        addPartContributionToColumnAndSecondaryIndexSizes(part);
        persist();
        transaction.renameParts();
        Upsert::syncDirectory(std::filesystem::path(part->getDataPartStorage().getFullPath()) / "..");
        fiu_do_on(FailPoints::upsert_throw_after_rename,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree failure after durable rename");
        });
        fiu_do_on(FailPoints::upsert_throw_before_publish,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree failure before publication");
        });
        /// This changes the state of an existing index node and swaps preallocated visibility state.
        modifyPartState(part, DataPartState::Active, lock);
        increaseDataVolume(static_cast<ssize_t>(part->getBytesOnDisk()), static_cast<ssize_t>(part->rows_count), 1);
        upsert_state.swap(snapshot);
        transaction.clear();
    }
    catch (...)
    {
        /// Neither rollback nor temporary-part destruction may remove ambiguously committed payloads.
        transaction.clear();
        part->is_temp = false;
        upsert_fenced = true;
        throw;
    }
}
}
