#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/UniqueKey/UpsertStateStore.h>
#include <Storages/MergeTree/UniqueKey/UpsertTableManifest.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Storages/MergeTree/UniqueKey/UpsertBinary.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeCommittingBlock.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/FailPoint.h>
#include <filesystem>
#include <numeric>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CORRUPTED_DATA;
extern const int LIMIT_EXCEEDED;
extern const int LOGICAL_ERROR;
extern const int SUPPORT_IS_DISABLED;
}
namespace FailPoints
{
extern const char upsert_partition_pause_after_prepare[];
extern const char upsert_partition_pause_after_detach_clone[];
extern const char upsert_partition_pause_after_attach_rename[];
extern const char upsert_partition_pause_after_move_import[];
}
namespace Setting
{
extern const SettingsSeconds lock_acquire_timeout;
extern const SettingsBool allow_replace_partition_from_empty_source;
extern const SettingsUInt64 unique_key_max_encoded_size;
}
namespace
{
constexpr auto visibility_file = "upsert_visibility.bin";

String getPartNameFromAST(const ASTPtr & partition)
{
    const auto * literal = partition->as<ASTLiteral>();
    if (!literal)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a string literal for part name");
    return literal->value.safeGet<String>();
}

struct ImportParts
{
    std::vector<MergeTreeTemporaryPartPtr> temporary;
    std::vector<Upsert::PartCommit> records;
    std::vector<Upsert::ValidatedPartRows> rows;
};

void writeVisibility(const DataPartPtr & part, const Upsert::SnapshotPtr & snapshot, const String & path)
{
    const auto mask = snapshot->getMask(part->getDeleteBitmapCacheIdentity());
    if (!mask)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree export lacks a visibility mask");
    WriteBufferFromOwnString buffer;
    mask->serialize(buffer);
    Upsert::BinaryWriter payload;
    payload.id({UUIDHelpers::getHighBytes(part->uuid), UUIDHelpers::getLowBytes(part->uuid)});
    payload.number(part->rows_count);
    payload.string(buffer.str());
    const auto id = Upsert::newObjectID();
    Upsert::LogRecord record{.incarnation = id, .partition_id = part->info.getPartitionId(), .csn = 1,
        .operation_id = id, .kind = Upsert::RecordKind::Noop, .payload = std::move(payload.bytes)};
    Upsert::createDurableFile(std::filesystem::path(path) / visibility_file, Upsert::encodeLogRecord(record));
    Upsert::hashPartDirectory(path, true);
    Upsert::syncDirectoryAncestors(path, part->getDataPartStorage().getDiskPath());
}

std::shared_ptr<const DeleteBitmap> readVisibility(const DataPartPtr & part)
{
    const auto path = std::filesystem::path(part->getDataPartStorage().getFullPath());
    if (!std::filesystem::exists(path / visibility_file))
    {
        if (std::filesystem::exists(path / Upsert::ROWS_FILE))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Detached UpsertMergeTree part lacks its exported visibility");
        /// An ordinary native part has no external winner state; all its physical rows are import candidates.
        return std::make_shared<DeleteBitmap>();
    }
    const auto bytes = Upsert::readBoundedFile(path / visibility_file, Upsert::MAX_RECORD_BYTES);
    const auto decoded = Upsert::decodeLogRecord(bytes, false);
    if (!decoded || decoded->bytes_consumed != bytes.size() || decoded->record.kind != Upsert::RecordKind::Noop
        || decoded->record.partition_id != part->info.getPartitionId())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree detached visibility record");
    Upsert::BinaryReader payload{decoded->record.payload};
    if (payload.id() != Upsert::ObjectID{UUIDHelpers::getHighBytes(part->uuid), UUIDHelpers::getLowBytes(part->uuid)}
        || payload.number() != part->rows_count)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree detached visibility row count mismatch");
    const auto encoded = payload.string();
    payload.end();
    ReadBufferFromString input(encoded);
    auto mask = DeleteBitmap::deserialize(input);
    const auto offsets = mask->toVector();
    if (!offsets.empty() && offsets.back() >= part->rows_count)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree detached visibility offset exceeds its part");
    return mask;
}
}

Pipe StorageMergeTree::alterPartition(
    const StorageMetadataPtr & metadata_snapshot, const PartitionCommands & commands, ContextPtr query_context) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    if (!merging_params.upsert)
        return MergeTreeData::alterPartition(metadata_snapshot, commands, query_context);
    assertNotReadonly();
    if (!upsert_store->usesDiskState() || query_context->getCurrentTransaction())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree partition operations require disk_state_v2 outside explicit transactions");

    const auto & settings = query_context->getSettingsRef();
    auto table_lock = lockForShare(query_context->getCurrentQueryId(), settings[Setting::lock_acquire_timeout]);
    const auto table_path = std::filesystem::path(getDisks().front()->getPath()) / getRelativeDataPath();

    auto select_parts = [&](StorageMergeTree & table, const PartitionCommand & command, const UpsertReadSnapshotPtr & snapshot)
    {
        DataPartsVector parts;
        const auto ranges = snapshot->catalog.ranges();
        for (const auto & range : *ranges)
            parts.push_back(range.data_part);
        if (command.part)
        {
            const auto name = getPartNameFromAST(command.partition);
            std::erase_if(parts, [&](const auto & part)
            {
                return part->name != name;
            });
        }
        else if (command.partition && !command.partition->as<ASTPartition>()->all)
        {
            const auto partition = table.getPartitionIDFromQuery(command.partition, query_context);
            std::erase_if(parts, [&](const auto & part)
            {
                return part->info.getPartitionId() != partition;
            });
        }
        return parts;
    };
    auto reset_partitions = [&](StorageMergeTree & table, const PartitionCommand & command, const DataPartsVector & parts)
    {
        std::set<String> partitions;
        if (!command.part)
        {
            if (command.partition && !command.partition->as<ASTPartition>()->all)
                partitions.insert(table.getPartitionIDFromQuery(command.partition, query_context));
            for (const auto & part : parts)
                partitions.insert(part->info.getPartitionId());
        }
        return std::vector<String>(partitions.begin(), partitions.end());
    };

    auto prepare_import = [&](StorageMergeTree & target, StorageMergeTree & source, const DataPartsVector & parts,
                              const StorageMetadataPtr & source_metadata, const StorageMetadataPtr & target_metadata,
                              const Upsert::SnapshotPtr & visibility)
    {
        ImportParts imported;
        auto storage_snapshot = std::make_shared<StorageSnapshot>(source, source_metadata);
        const auto names = target_metadata->getColumns().getAllPhysical().getNames();
        size_t estimated_state = 0;
        for (const auto & part : parts)
        {
            auto import_context = Context::createCopy(query_context);
            size_t key_limit = settings[Setting::unique_key_max_encoded_size];
            const auto lineage_path = std::filesystem::path(part->getDataPartStorage().getFullPath()) / Upsert::ROWS_FILE;
            if (std::filesystem::exists(lineage_path))
                for (const auto & row : Upsert::decodePartRows(Upsert::readBoundedFile(lineage_path, Upsert::MAX_RECORD_BYTES)))
                    key_limit = std::max(key_limit, row.key.size());
            import_context->setSetting("unique_key_max_encoded_size", static_cast<UInt64>(key_limit));
            const auto mask = visibility ? visibility->getMask(part->getDeleteBitmapCacheIdentity()) : readVisibility(part);
            if (!mask)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree partition import lacks a visibility mask");
            QueryPipeline pipeline(createMergeTreeSequentialSource(MergeTreeSequentialSourceType::Merge, source,
                storage_snapshot, RangesInDataPart(part), std::make_shared<AlterConversions>(), nullptr,
                names, std::nullopt, nullptr, false, false, false));
            PullingPipelineExecutor executor(pipeline);
            Block block;
            size_t physical_offset = 0;
            while (executor.pull(block))
            {
                IColumn::Filter keep(block.rows(), 1);
                for (size_t row = 0; row < block.rows(); ++row)
                    keep[row] = !mask->contains(static_cast<UInt32>(physical_offset + row));
                physical_offset += block.rows();
                for (auto & column : block)
                    column.column = column.column->convertToFullIfWrapped()->filter(keep, -1);
                size_t offset = 0;
                size_t max_rows = Upsert::MAX_UNIT_ROWS;
                while (offset < block.rows())
                {
                    auto slice = Upsert::cutInputBlock(block, offset, max_rows);
                    const auto input_rows = slice.rows();
                    auto ordinals = ColumnUInt64::create(input_rows);
                    std::iota(ordinals->getData().begin(), ordinals->getData().end(), UInt64{0});
                    slice.insert({std::move(ordinals), std::make_shared<DataTypeUInt64>(), Upsert::ORDINAL_COLUMN});
                    const auto lineage_bytes = Upsert::foldBlock(slice, *target_metadata, target.merging_params.upsert_version_column,
                        key_limit);
                    const auto state_bytes = Upsert::estimateInsertStateBytes(lineage_bytes, slice.rows(), part->info.getPartitionId().size(), 0);
                    if (lineage_bytes > Upsert::MAX_RECORD_BYTES || state_bytes > Upsert::MAX_PENDING_STATE_BYTES / 2)
                    {
                        if (input_rows == 1)
                            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree partition import row exceeds its state budget");
                        max_rows = input_rows / 2;
                        continue;
                    }
                    if (state_bytes > Upsert::MAX_PENDING_STATE_BYTES - estimated_state)
                        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree atomic partition import exceeds its state budget");
                    estimated_state += state_bytes;
                    BlockWithPartition output;
                    output.block = std::make_shared<Block>(std::move(slice));
                    output.partition = part->partition;
                    auto temporary = target.writer.writeTempPart(output, target_metadata, import_context, false, true);
                    temporary->finalize();
                    temporary->part->getDataPartStorage().commitTransaction();
                    const auto path = temporary->part->getDataPartStorage().getFullPath();
                    auto rows = Upsert::decodePartRows(Upsert::readBoundedFile(std::filesystem::path(path) / Upsert::ROWS_FILE, Upsert::MAX_RECORD_BYTES));
                    Upsert::PartCommit authorization;
                    authorization.part = {UUIDHelpers::getHighBytes(temporary->part->uuid), UUIDHelpers::getLowBytes(temporary->part->uuid)};
                    authorization.temporary_name = temporary->part->getDataPartStorage().getPartDirectory();
                    authorization.files_hash = Upsert::hashPartDirectory(path, true);
                    authorization.rows = temporary->part->rows_count;
                    Upsert::syncDirectory(std::filesystem::path(path).parent_path());
                    imported.rows.emplace_back(std::move(rows), authorization.part);
                    imported.records.push_back(std::move(authorization));
                    imported.temporary.push_back(std::move(temporary));
                    offset += input_rows;
                    max_rows = Upsert::MAX_UNIT_ROWS;
                    CurrentMemoryTracker::check();
                }
            }
            if (physical_offset != part->rows_count)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree partition import read omitted source rows");
        }
        return imported;
    };

    auto apply = [&](StorageMergeTree & target, ImportParts & imported, const DataPartsVector & removed,
                     const std::vector<String> & partitions, const std::vector<String> & detached,
                     const UpsertReadSnapshotPtr & removal_snapshot)
    {
        UpsertReadSnapshotPtr retired_snapshot;
        NameSet accepted;
        {
            /// Never hold another table's schema lock while waiting for this one: an insert can visit both through a view.
            auto schema_lock = target.tryLockTimed(target.upsert_schema_lock, RWLockImpl::Write,
                query_context->getCurrentQueryId(), settings[Setting::lock_acquire_timeout]);
            std::lock_guard ttl_lock(target.upsert_ttl_mutex);
            std::vector<String> reset;
            const auto current_snapshot = target.getUpsertReadSnapshotForQuery();
            for (const auto & partition : partitions)
                if (!removal_snapshot || current_snapshot->visibility->getPartitionCSN(partition)
                    == removal_snapshot->visibility->getPartitionCSN(partition))
                    reset.push_back(partition);
            std::vector<std::unique_ptr<PlainCommittingBlockHolder>> block_holders;
            auto lock = target.lockParts();
            if (target.upsert_fenced)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree is fenced pending restart and recovery");
            for (size_t i = 0; i < imported.temporary.size(); ++i)
            {
                block_holders.push_back(target.fillNewPartName(imported.temporary[i]->part, lock));
                imported.records[i].final_name = imported.temporary[i]->part->name;
            }
            std::vector<String> removed_names;
            for (const auto & part : removed)
                removed_names.push_back(part->name);
            auto prepared = target.upsert_store->preparePartition(std::move(imported.records), imported.rows, removed_names, reset, detached, partitions);

            for (const auto & record : prepared.added_parts)
                accepted.insert(record.final_name);
            DataPartsVector added;
            for (const auto & temporary : imported.temporary)
                if (accepted.contains(temporary->part->name))
                    added.push_back(temporary->part);
            auto read_snapshot = target.prepareUpsertReadSnapshot(prepared.snapshot, nullptr, removed, lock, added);
            bool persist_started = false;
            try
            {
                MutableDataPartsVector accepted_parts;
                for (const auto & temporary : imported.temporary)
                    if (accepted.contains(temporary->part->name))
                        accepted_parts.push_back(temporary->part);
                target.commitUpsertParts(accepted_parts, [&]()
                {
                    persist_started = true;
                    target.upsert_store->append(prepared);
                }, lock);
                target.removePartsFromWorkingSet(nullptr, removed, true, lock);
                target.upsert_store->publish(prepared);
                retired_snapshot = target.publishUpsertReadSnapshot(std::move(read_snapshot));
                target.notifyUpsertMaintenance();
                target.upsert_maintenance_cursor.reset();
                for (const auto & retired : removed)
                    target.upsert_saturated_parts.erase(retired->name);
            }
            catch (...)
            {
                if (!persist_started && !target.upsert_fenced)
                    throw;
                for (auto & temporary : imported.temporary)
                    if (accepted.contains(temporary->part->name))
                        temporary->part->is_temp = false;
                target.upsert_fenced = true;
                throw;
            }
            if (target.upsert_maintenance_task)
                target.upsert_maintenance_task->schedule();
        }
        for (const auto & temporary : imported.temporary)
            if (accepted.contains(temporary->part->name))
                target.prewarmUpsertPartCaches(*temporary);
    };

    PartitionCommandsResultInfo results;
    for (const auto & command : commands)
    {
        const bool transfer = command.type == PartitionCommand::REPLACE_PARTITION
            || (command.type == PartitionCommand::MOVE_PARTITION && command.move_destination_type == PartitionCommand::MoveDestinationType::TABLE);
        const bool move = command.type == PartitionCommand::MOVE_PARTITION;
        StoragePtr other_storage;
        StorageMergeTree * other = nullptr;
        TableLockHolder other_lifetime;
        std::unique_lock operation_lock(upsert_operation_mutex, std::defer_lock);
        std::unique_lock<std::mutex> other_operation;
        if (transfer)
        {
            if (!move && command.replace && command.partition->as<ASTPartition>()->all)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "REPLACE PARTITION ALL is not supported");
            const auto id = query_context->resolveStorageID(move ? StorageID{command.to_database, command.to_table}
                : StorageID{command.from_database, command.from_table});
            other_storage = DatabaseCatalog::instance().getTable(id, query_context);
            other = dynamic_cast<StorageMergeTree *>(other_storage.get());
            if (!other || !other->merging_params.upsert || other == this || !other->upsert_store->usesDiskState())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "UpsertMergeTree partition transfers require a different disk_state_v2 UpsertMergeTree table");
            other_lifetime = other->lockForShare(query_context->getCurrentQueryId(), settings[Setting::lock_acquire_timeout]);
            other_operation = std::unique_lock(other->upsert_operation_mutex, std::defer_lock);
            std::lock(operation_lock, other_operation);
            if (other->upsert_fenced)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree transfer table is fenced pending recovery");
        }
        else
            operation_lock.lock();
        if (upsert_fenced)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree is fenced pending restart and recovery");
        const auto metadata = getInMemoryMetadataPtr(query_context, true);
        {
            std::lock_guard ttl_lock(upsert_ttl_mutex);
            Upsert::enablePartitionOperations(table_path, getDisks().front()->getName(), *metadata, merging_params.upsert_version_column);
        }

        if (command.type == PartitionCommand::DROP_DETACHED_PARTITION)
        {
            dropDetached(command.partition, command.part, query_context);
            continue;
        }
        if (command.type == PartitionCommand::UNFREEZE_PARTITION || command.type == PartitionCommand::UNFREEZE_ALL_PARTITIONS)
        {
            auto current = command.type == PartitionCommand::UNFREEZE_PARTITION
                ? unfreezePartition(command.partition, command.with_name, query_context, table_lock)
                : unfreezeAll(command.with_name, query_context, table_lock);
            results.insert(results.end(), current.begin(), current.end());
            continue;
        }
        const auto snapshot = getUpsertReadSnapshotForQuery();
        auto parts = select_parts(*this, command, snapshot);
        const auto visibility = snapshot->visibility;
        if (command.type == PartitionCommand::FREEZE_PARTITION || command.type == PartitionCommand::FREEZE_ALL_PARTITIONS)
        {
            auto current = command.type == PartitionCommand::FREEZE_PARTITION
                ? freezePartition(command.partition, command.with_name, query_context, table_lock, &parts)
                : freezeAll(command.with_name, query_context, table_lock, &parts);
            for (const auto & result : current)
            {
                const auto part = std::ranges::find_if(parts, [&](const auto & value)
                {
                    return value->name == result.part_name;
                });
                if (part == parts.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree frozen part was not in its pinned snapshot");
                writeVisibility(*part, visibility, result.part_backup_path);
            }
            results.insert(results.end(), current.begin(), current.end());
            continue;
        }
        if (command.type == PartitionCommand::DROP_PARTITION)
        {
            if (command.part)
                checkPartCanBeDropped(getPartNameFromAST(command.partition), query_context);
            else
                checkPartitionCanBeDropped(command.partition, query_context);
            if (command.detach)
            {
                for (const auto & part : parts)
                {
                    auto clone = part->makeCloneInDetached("", metadata, {});
                    writeVisibility(part, visibility, clone->getFullPath());
                }
            }
            if (command.detach)
                FailPointInjection::pauseFailPoint(FailPoints::upsert_partition_pause_after_detach_clone);
            FailPointInjection::pauseFailPoint(FailPoints::upsert_partition_pause_after_prepare);
            ImportParts empty;
            apply(*this, empty, parts, reset_partitions(*this, command, parts), {}, snapshot);
            continue;
        }
        if (command.type == PartitionCommand::ATTACH_PARTITION)
        {
            PartsTemporaryRename renamed(*this, DETACHED_DIR_NAME);
            auto loaded = tryLoadPartsToAttach(command, query_context, renamed);
            /// Persist the consumed input names before the durable batch can authorize their removal.
            /// Otherwise recovery could retain an old detached name after a successful import.
            if (!renamed.old_and_new_names.empty())
                Upsert::syncDirectoryAncestors((table_path / DETACHED_DIR_NAME).string(), getDisks().front()->getPath());
            FailPointInjection::pauseFailPoint(FailPoints::upsert_partition_pause_after_attach_rename);
            DataPartsVector inputs(loaded.begin(), loaded.end());
            const auto active_ranges = snapshot->catalog.ranges();
            for (const auto & input : inputs)
                for (const auto & active : *active_ranges)
                    if (input->uuid != UUIDHelpers::Nil && input->uuid == active.data_part->uuid)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Detached UpsertMergeTree part still has an active source identity");
            auto imported = prepare_import(*this, *this, inputs, metadata, metadata, {});
            std::vector<String> detached;
            for (const auto & rename : renamed.old_and_new_names)
                detached.push_back(rename.new_dir);
            try
            {
                FailPointInjection::pauseFailPoint(FailPoints::upsert_partition_pause_after_prepare);
                apply(*this, imported, {}, {}, detached, {});
            }
            catch (...)
            {
                if (upsert_fenced)
                    for (auto & rename : renamed.old_and_new_names)
                        rename.old_dir.clear();
                throw;
            }
            for (auto & rename : renamed.old_and_new_names)
            {
                results.push_back({.command_type = "ATTACH_PART", .partition_id = MergeTreePartInfo::fromPartName(
                    rename.part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING).getPartitionId(),
                    .part_name = rename.part_name, .old_part_name = rename.old_dir});
                rename.old_dir.clear();
            }
            loaded.clear();
            inputs.clear();
            /// Import is committed; cleanup failures must not turn success into an ambiguous error.
            upsert_maintenance_task->schedule();
            continue;
        }
        if (transfer)
        {
            const auto other_metadata = other->getInMemoryMetadataPtr(query_context, true);
            if (metadata->getColumns().getAllPhysical() != other_metadata->getColumns().getAllPhysical()
                || (metadata->getPartitionKey().expression_list_ast ? metadata->getPartitionKey().expression_list_ast->formatWithSecretsOneLine() : String{})
                    != (other_metadata->getPartitionKey().expression_list_ast ? other_metadata->getPartitionKey().expression_list_ast->formatWithSecretsOneLine() : String{})
                || metadata->getSortingKey().expression_list_ast->formatWithSecretsOneLine() != other_metadata->getSortingKey().expression_list_ast->formatWithSecretsOneLine()
                || metadata->getUniqueKey().expression_list_ast->formatWithSecretsOneLine() != other_metadata->getUniqueKey().expression_list_ast->formatWithSecretsOneLine()
                || merging_params.upsert_version_column != other->merging_params.upsert_version_column)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "UpsertMergeTree partition transfer requires matching columns, keys and version column");
            auto & source = move ? *this : *other;
            auto & target = move ? *other : *this;
            target.assertNotReadonly();
            const auto target_metadata = move ? other_metadata : metadata;
            const auto source_metadata = move ? metadata : other_metadata;
            const auto target_path = std::filesystem::path(target.getDisks().front()->getPath()) / target.getRelativeDataPath();
            {
                std::lock_guard target_ttl(target.upsert_ttl_mutex);
                Upsert::enablePartitionOperations(target_path, target.getDisks().front()->getName(), *target_metadata, target.merging_params.upsert_version_column);
            }
            const auto source_snapshot = source.getUpsertReadSnapshotForQuery();
            const auto source_parts = select_parts(source, command, source_snapshot);
            if (move)
                source.checkPartitionCanBeDropped(command.partition, query_context);
            auto imported = prepare_import(target, source, source_parts, source_metadata, target_metadata, source_snapshot->visibility);
            if (!move && command.replace && imported.records.empty()
                && !settings[Setting::allow_replace_partition_from_empty_source])
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "UpsertMergeTree REPLACE PARTITION source has no visible rows; use DROP PARTITION or "
                    "allow_replace_partition_from_empty_source = 1 to explicitly clear the destination");
            const auto removed = !move && command.replace ? parts : DataPartsVector{};
            if (!move && command.replace)
                checkPartitionCanBeDropped(command.partition, query_context);
            FailPointInjection::pauseFailPoint(FailPoints::upsert_partition_pause_after_prepare);
            apply(target, imported, removed, !move && command.replace ? reset_partitions(target, command, removed) : std::vector<String>{}, {}, move ? UpsertReadSnapshotPtr{} : snapshot);
            if (move)
            {
                FailPointInjection::pauseFailPoint(FailPoints::upsert_partition_pause_after_move_import);
                ImportParts empty;
                apply(source, empty, source_parts, reset_partitions(source, command, source_parts), {}, source_snapshot);
            }
            continue;
        }
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "UpsertMergeTree supports DROP/DETACH/ATTACH PARTITION or PART, ATTACH/REPLACE FROM, MOVE TO TABLE and FREEZE/UNFREEZE");
    }
    clearOldPartsFromFilesystem(false, false);
    return convertCommandsResultToSource(results);
}
}
