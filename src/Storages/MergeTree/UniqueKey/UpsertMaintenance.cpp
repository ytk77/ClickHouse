#include <Storages/MergeTree/UniqueKey/UpsertMetrics.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/UniqueKey/UpsertStateStore.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Storages/MergeTree/UniqueKey/UpsertTableManifest.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <IO/PackedFilesReader.h>
#include <Common/SipHash.h>
#include <Storages/AlterCommands.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/IDatabase.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/TTL/TTLDeleteAlgorithm.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/QueryMetadataCache.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Parsers/ASTAlterQuery.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadStatus.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/MemoryTracker.h>
#include <base/scope_guard.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/UUID.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/assert_cast.h>
#include <filesystem>
#include <numeric>
#include <limits>
#include <new>

namespace DB::ErrorCodes
{
extern const int ABORTED;
extern const int CORRUPTED_DATA;
extern const int BAD_ARGUMENTS;
extern const int LIMIT_EXCEEDED;
extern const int SUPPORT_IS_DISABLED;
extern const int FAULT_INJECTED;
extern const int MEMORY_LIMIT_EXCEEDED;
extern const int CANNOT_ALLOCATE_MEMORY;
}
namespace DB::FailPoints
{
extern const char upsert_pause_before_merge_commit[];
extern const char upsert_pause_background_verification[];
extern const char upsert_maintenance_corrupt_candidate[];
extern const char upsert_ttl_pause_after_manifest_upgrade[];
extern const char upsert_ttl_pause_after_metadata_commit[];
extern const char upsert_ttl_throw_after_metadata_commit[];
extern const char upsert_settings_throw_before_metadata[];
extern const char upsert_settings_throw_after_metadata[];
}

namespace CurrentMetrics
{
extern const Metric UpsertMaintenanceRewrites;
extern const Metric UpsertMaintenanceReservedBytes;
extern const Metric UpsertMaintenanceMemoryTracking;
}

namespace DB::ServerSetting
{
extern const ServerSettingsNonZeroUInt64 max_concurrent_upsert_maintenance_rewrites;
extern const ServerSettingsNonZeroUInt64 upsert_maintenance_memory_budget;
}

namespace DB::MergeTreeSetting
{
extern const MergeTreeSettingsBool share_nested_offsets;
extern const MergeTreeSettingsUInt64 upsert_maintenance_target_bytes;
}

namespace DB::Setting
{
extern const SettingsSeconds lock_acquire_timeout;
}

namespace DB
{
namespace
{
/// Reservations are process-wide, matching the server's background memory tracker.
/// Account both reservations and measured memory conservatively: active rewrites can be counted twice.
class MaintenanceAdmission
{
    static inline std::mutex mutex;
    static inline UInt64 active = 0;
    static inline UInt64 reserved = 0;
    UInt64 reservation = 0;

    static bool fits(UInt64 limit, UInt64 used, UInt64 extra)
    {
        return used <= limit && extra <= limit - used;
    }

public:
    MaintenanceAdmission() = default;
    MaintenanceAdmission(const MaintenanceAdmission &) = delete;
    MaintenanceAdmission & operator=(const MaintenanceAdmission &) = delete;

    static bool canAttempt(const ServerSettings & settings)
    {
        std::lock_guard lock(mutex);
        const auto background_limit = background_memory_tracker.getSoftLimit();
        const auto server_limit = total_memory_tracker.getHardLimit();
        return active < settings[ServerSetting::max_concurrent_upsert_maintenance_rewrites]
            && reserved < settings[ServerSetting::upsert_maintenance_memory_budget]
            && (background_limit <= 0 || fits(background_limit, std::max<Int64>(0, background_memory_tracker.get()), reserved))
            && (server_limit <= 0 || fits(server_limit, std::max<Int64>(0, total_memory_tracker.get()), reserved));
    }

    bool tryAcquire(UInt64 bytes, const ServerSettings & settings)
    {
        std::lock_guard lock(mutex);
        const UInt64 memory_limit = settings[ServerSetting::upsert_maintenance_memory_budget];
        /// This is a soft reservation budget: allow one oversized unit in isolation, subject to
        /// the actual background and server headroom checks below, so a large source can make progress.
        const bool oversized = bytes > memory_limit && active == 0;
        if (bytes > static_cast<UInt64>(std::numeric_limits<Int64>::max())
            || active >= settings[ServerSetting::max_concurrent_upsert_maintenance_rewrites]
            || (!oversized && !fits(memory_limit, reserved, bytes)))
            return false;
        const UInt64 total_reserved = reserved + bytes;
        const auto background_limit = background_memory_tracker.getSoftLimit();
        const auto server_limit = total_memory_tracker.getHardLimit();
        if ((background_limit > 0 && !fits(background_limit, std::max<Int64>(0, background_memory_tracker.get()), total_reserved))
            || (server_limit > 0 && !fits(server_limit, std::max<Int64>(0, total_memory_tracker.get()), total_reserved)))
            return false;
        if (oversized)
            ProfileEvents::increment(ProfileEvents::UpsertMaintenanceOversizedAdmissions);
        reservation = bytes;
        reserved += bytes;
        ++active;
        CurrentMetrics::add(CurrentMetrics::UpsertMaintenanceRewrites);
        CurrentMetrics::add(CurrentMetrics::UpsertMaintenanceReservedBytes, bytes);
        return true;
    }

    ~MaintenanceAdmission()
    {
        if (!reservation)
            return;
        std::lock_guard lock(mutex);
        reserved -= reservation;
        --active;
        CurrentMetrics::sub(CurrentMetrics::UpsertMaintenanceReservedBytes, reservation);
        CurrentMetrics::sub(CurrentMetrics::UpsertMaintenanceRewrites);
    }
};

UInt64 estimateRewriteMemory(const MergeTreeData::DataPartsVector & sources, UInt64 target)
{
    UInt64 largest_source = 0;
    UInt64 selected_bytes = 0;
    UInt64 state_bytes = 0;
    UInt64 lineage = 0;
    UInt64 rows = 0;
    for (const auto & source : sources)
    {
        const UInt64 source_bytes = source->getBytesUncompressedOnDisk();
        largest_source = std::max(largest_source, source_bytes);
        if (source_bytes > static_cast<UInt64>(std::numeric_limits<Int64>::max()) - selected_bytes)
            return std::numeric_limits<UInt64>::max();
        selected_bytes += source_bytes;
        const auto lineage_size = source->checksums.files.at(Upsert::ROWS_FILE).file_size;
        lineage += lineage_size;
        rows += source->rows_count;
        state_bytes += Upsert::estimateInsertStateBytes(lineage_size, source->rows_count, source->info.getPartitionId().size(), 1024);
    }
    /// Source lineage remains loaded while column buffers and a state batch are built.
    /// Saturate an unrepresentable estimate so admission rejects it without attempting allocation.
    const UInt64 fixed = 2 * lineage + rows * sizeof(Upsert::PartRow)
        + std::min<UInt64>(state_bytes, Upsert::MAX_PENDING_STATE_BYTES / 2);
    const UInt64 output_bytes = std::min(target, selected_bytes);
    const UInt64 max_estimate = std::numeric_limits<Int64>::max();
    if (fixed > max_estimate || output_bytes > (max_estimate - fixed) / 3)
        return std::numeric_limits<UInt64>::max();
    const UInt64 subtotal = fixed + 3 * output_bytes;
    if (largest_source > max_estimate - subtotal)
        return std::numeric_limits<UInt64>::max();
    return subtotal + largest_source;
}

bool isMaintenanceMemoryFailure()
{
    try
    {
        throw;
    }
    catch (const Exception & e)
    {
        return e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED || e.code() == ErrorCodes::CANNOT_ALLOCATE_MEMORY;
    }
    catch (const std::bad_alloc &)
    {
        return true;
    }
    catch (...)
    {
        return false;
    }
}

std::vector<TTLAlgorithmPtr> buildDeleteTTLAlgorithms(const StorageInMemoryMetadata & metadata, const ContextPtr & context, time_t now)
{
    std::vector<TTLAlgorithmPtr> algorithms;
    auto add = [&](const TTLDescription & description)
    {
        auto expression = description.buildExpression(context);
        auto where_expression = description.buildWhereExpression(context);
        for (const auto & sets : {expression.sets, where_expression.sets})
            if (sets)
                for (auto & subquery : sets->getSubqueries())
                    subquery->buildSetInplace(context);
        algorithms.emplace_back(std::make_unique<TTLDeleteAlgorithm>(
            TTLExpressions{expression.expression, where_expression.expression}, description, MergeTreeDataPartTTLInfo{}, now, true));
    };
    if (metadata.hasRowsTTL())
        add(metadata.getRowsTTL());
    for (const auto & description : metadata.getRowsWhereTTLs())
        add(description);
    return algorithms;
}

IColumn::Filter filterTTL(const Block & block, std::vector<TTLAlgorithmPtr> & algorithms)
{
    if (algorithms.empty())
        return {};

    /// Carry physical offsets through the standard `TTL` algorithms to preserve original winner identities.
    constexpr auto offset_name = "_upsert_ttl_offset";
    Block filtered = block;
    auto offsets = ColumnUInt64::create(block.rows());
    std::iota(offsets->getData().begin(), offsets->getData().end(), UInt64{0});
    filtered.insert({std::move(offsets), std::make_shared<DataTypeUInt64>(), offset_name});
    for (auto & algorithm : algorithms)
        algorithm->execute(filtered);
    IColumn::Filter keep(block.rows(), 0);
    const auto & survivors = assert_cast<const ColumnUInt64 &>(*filtered.getByName(offset_name).column).getData();
    for (const auto offset : survivors)
        keep[offset] = 1;
    return keep;
}
}

void StorageMergeTree::alterUpsertMetadata(const AlterCommands & commands, ContextPtr local_context, AlterLockHolder & table_lock_holder)
{
    checkAlterIsPossible(commands, local_context);
    NameSet dropped_columns;
    NameSet dropped_indices;
    NameSet dropped_projections;
    for (auto it = commands.begin(); it != commands.end(); ++it)
    {
        const bool reuses_name = (it->type == AlterCommand::ADD_COLUMN && dropped_columns.contains(it->column_name))
            || (it->type == AlterCommand::ADD_INDEX && dropped_indices.contains(it->index_name))
            || (it->type == AlterCommand::ADD_PROJECTION && dropped_projections.contains(it->projection_name));
        if (reuses_name)
        {
            /// Commit removal and retire its payload before publishing a new definition of the same name.
            const auto offset = it - commands.begin();
            auto before = commands;
            before.erase(before.begin() + offset, before.end());
            auto after = commands;
            after.erase(after.begin(), after.begin() + offset);
            alterUpsertMetadata(before, local_context, table_lock_holder);
            if (auto metadata_cache = local_context->getQueryMetadataCache())
            {
                auto [cache, cache_lock] = metadata_cache->getStorageMetadataCache();
                cache->clear();
            }
            alterUpsertMetadata(after, local_context, table_lock_holder);
            return;
        }
        if (it->type == AlterCommand::DROP_COLUMN)
            dropped_columns.insert(it->column_name);
        if (it->type == AlterCommand::DROP_INDEX)
            dropped_indices.insert(it->index_name);
        if (it->type == AlterCommand::DROP_PROJECTION)
            dropped_projections.insert(it->projection_name);
    }
    if (!upsert_store->usesDiskState())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree alters require the disk_state_v2 protocol");

    const auto table_id = getStorageID();
    const auto database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    const auto metadata_disk = database->getDisk();
    if ((database->getEngineName() != "Atomic" && database->getEngineName() != "Ordinary")
        || metadata_disk->getDataSourceDescription().type != DataSourceType::Local)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree alters require an Atomic or Ordinary database with local metadata");

    const bool changes_schema = std::ranges::any_of(commands, [](const auto & command)
    {
        return command.type != AlterCommand::MODIFY_TTL && command.type != AlterCommand::REMOVE_TTL;
    });
    RWLockImpl::LockHolder schema_lock;
    std::unique_lock operation_lock(upsert_operation_mutex, std::defer_lock);
    if (changes_schema)
        operation_lock.lock();
    const auto old_metadata = getInMemoryMetadataPtr(local_context, true);
    auto new_metadata = *old_metadata;
    commands.apply(new_metadata, local_context, (*getSettings())[MergeTreeSetting::share_nested_offsets]);
    checkMetadataDoesNotExceedMaxQuerySize(table_id, new_metadata, local_context);
    auto create_ast = database->getCreateTableQuery(table_id.table_name, local_context);
    applyMetadataChangesToCreateQuery(create_ast, new_metadata, local_context);
    const auto disk = getDisks().front();
    const auto table_path = (std::filesystem::path(disk->getPath()) / getRelativeDataPath()).string();
    const auto metadata_path = std::filesystem::path(metadata_disk->getPath()) / database->getObjectMetadataPath(table_id.table_name);

    if (commands.isSettingsAlter())
    {
        /// Settings affect future work only. Persist metadata before changing the live settings.
        std::lock_guard ttl_guard(upsert_ttl_mutex);
        if (upsert_fenced)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree is fenced pending restart and recovery");
        const auto relative_metadata_path = database->getObjectMetadataPath(table_id.table_name);
        const auto previous_metadata_text = readMetadataFile(metadata_disk, relative_metadata_path);
        bool metadata_replaced = false;
        try
        {
            fiu_do_on(FailPoints::upsert_settings_throw_before_metadata,
            {
                throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree failure before settings metadata");
            });
            database->alterTable(local_context, table_id, new_metadata, /*validate_new_create_query=*/false);
            metadata_replaced = true;
            Upsert::syncFiles({metadata_path.string()});
            Upsert::syncDirectory(metadata_path.parent_path().string());
            fiu_do_on(FailPoints::upsert_settings_throw_after_metadata,
            {
                throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree failure after settings metadata");
            });
            changeSettings(new_metadata.settings_changes, table_lock_holder);
        }
        catch (...)
        {
            const auto error = std::current_exception();
            bool unchanged = false;
            try
            {
                /// `alterTable` can throw after its rename. Only verified unchanged metadata is safe.
                unchanged = !metadata_replaced && readMetadataFile(metadata_disk, relative_metadata_path) == previous_metadata_text;
            }
            catch (...)
            {
                tryLogCurrentException(log, "Could not verify UpsertMergeTree metadata after settings failure");
            }
            if (!unchanged)
                upsert_fenced = true;
            std::rethrow_exception(error);
        }
        upsert_maintenance_cursor.reset();
        upsert_maintenance_pending.reset();
        notifyUpsertMaintenance();
        if (upsert_maintenance_task)
            upsert_maintenance_task->schedule();
        return;
    }

    if (changes_schema)
    {
        upsert_saturated_parts.clear();
        /// Current-schema inserts cannot introduce obsolete files. Clean them before excluding writers.
        reconcileUpsertParts(old_metadata, local_context);
        schema_lock = tryLockTimed(upsert_schema_lock, RWLockImpl::Write,
            local_context->getCurrentQueryId(), local_context->getSettingsRef()[Setting::lock_acquire_timeout]);
    }
    std::unique_lock ttl_lock(upsert_ttl_mutex);
    if (upsert_fenced)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree is fenced pending restart and recovery");
    if (changes_schema)
        Upsert::enableMutableSchema(table_path, disk->getName(), *old_metadata, merging_params.upsert_version_column);
    else
        Upsert::enableMutableTTL(table_path, disk->getName(), *old_metadata, merging_params.upsert_version_column);
    FailPointInjection::pauseFailPoint(FailPoints::upsert_ttl_pause_after_manifest_upgrade);
    try
    {
        /// Never publish or act on new expiration rules before the SQL metadata file and its rename are durable.
        database->alterTable(local_context, table_id, new_metadata, /*validate_new_create_query=*/false);
        Upsert::syncFiles({metadata_path.string()});
        Upsert::syncDirectory(metadata_path.parent_path().string());
        FailPointInjection::pauseFailPoint(FailPoints::upsert_ttl_pause_after_metadata_commit);
        fiu_do_on(FailPoints::upsert_ttl_throw_after_metadata_commit,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree failure after TTL metadata commit");
        });
        setInMemoryMetadata(new_metadata);
    }
    catch (...)
    {
        /// The database may already have replaced its SQL file. Recover that single authority on restart.
        upsert_fenced = true;
        throw;
    }
    ttl_lock.unlock();
    schema_lock.reset();
    if (changes_schema)
    {
        /// New inserts use the committed schema and can run while old payloads are retired.
        const auto committed_metadata = getInMemoryMetadataPtr(local_context, true);
        reconcileUpsertParts(committed_metadata, local_context);
    }
    notifyUpsertMaintenance();
    if (upsert_maintenance_task)
        upsert_maintenance_task->schedule();
}

void StorageMergeTree::notifyUpsertMaintenance()
{
    if (upsert_maintenance_schedule.notify() && upsert_maintenance_task)
        upsert_maintenance_task->schedule();
}

void StorageMergeTree::runUpsertVerification()
{
    if (upsert_fenced || shutdown_called || flush_called)
        return;
    bool verifying = false;
    try
    {
        FailPointInjection::pauseFailPoint(FailPoints::upsert_pause_background_verification);
        if (upsert_fenced || shutdown_called || flush_called)
            return;
        auto table_lock = lockForShare(RWLockImpl::NO_QUERY, Poco::Timespan(5, 0));
        std::unique_lock operation_lock(upsert_operation_mutex, std::try_to_lock);
        if (!operation_lock.owns_lock())
        {
            upsert_verification_task->scheduleAfter(1000);
            return;
        }
        DataPartPtr part;
        Upsert::SnapshotPtr snapshot;
        {
            auto lock = lockParts();
            if (upsert_fenced)
                return;
            const auto active = getDataPartsStateRange(DataPartState::Active);
            const auto next = upsert_verification_cursor
                ? data_parts_by_state_and_info.upper_bound(DataPartStateAndInfo{DataPartState::Active, *upsert_verification_cursor})
                : active.begin();
            if (next == active.end())
            {
                upsert_store->pruneRetired();
                upsert_verification_cursor.reset();
                upsert_verification_task->scheduleAfter(86400000);
                return;
            }
            part = *next;
            snapshot = upsert_store->snapshot();
        }
        /// Pin one part and release the writer lock throughout the full scan.
        verifying = true;
        snapshot->verifyPart(part->name, part->getDeleteBitmapCacheIdentity(), true);
        upsert_verification_cursor = part->info;
        upsert_verification_task->scheduleAfter(100);
    }
    catch (...)
    {
        if (verifying || getCurrentExceptionCode() == ErrorCodes::CORRUPTED_DATA)
        {
            upsert_fenced = true;
            tryLogCurrentException(log, "UpsertMergeTree background payload verification failed; table fenced pending recovery");
            return;
        }
        tryLogCurrentException(log, "Could not schedule UpsertMergeTree background payload verification");
        upsert_verification_task->scheduleAfter(1000);
    }
}

void StorageMergeTree::runUpsertMaintenance()
{
    if (upsert_fenced || shutdown_called || flush_called)
        return;
    if (const auto delay = upsert_maintenance_schedule.remainingRetryDelay())
    {
        upsert_maintenance_task->scheduleAfter(delay);
        return;
    }
    /// Each task handles a bounded unit; native merges remain disabled for this protocol.
    constexpr size_t max_sources = 16;
    constexpr size_t max_lineage_bytes = Upsert::MAX_RECORD_BYTES - Upsert::PART_ROWS_ENVELOPE_BYTES;
    bool did_work = false;
    bool continue_scan = false;
    bool has_ttl = false;
    bool failed = false;
    const char * phase = "acquiring locks";
    DataPartPtr diagnostic_part;
    const auto postpone_admission = [&]
    {
        ProfileEvents::increment(ProfileEvents::UpsertMaintenanceAdmissionDenied);
        const auto delay = upsert_maintenance_schedule.admissionFailure(sipHash64(getStorageID().getFullTableName()) % 251);
        const auto denials = upsert_maintenance_schedule.consecutiveAdmissionDenials();
        if ((denials & (denials - 1)) == 0)
            LOG_WARNING(log, "UpsertMergeTree maintenance admission denied {} consecutive times; retry in {} ms", denials, delay);
        upsert_maintenance_task->scheduleAfter(delay);
    };
    try
    {
        if (!MaintenanceAdmission::canAttempt(getContext()->getServerSettings()))
        {
            /// Work has not been selected yet; do not report an idle table as a denied rewrite.
            ProfileEvents::increment(ProfileEvents::UpsertMaintenanceAdmissionPrecheckDeferrals);
            upsert_maintenance_task->scheduleAfter(upsert_maintenance_schedule.precheckDeferral(
                sipHash64(getStorageID().getFullTableName()) % 251));
            return;
        }
        auto table_lock = lockForShare(RWLockImpl::NO_QUERY, Poco::Timespan(5, 0));
        {
            auto lock = lockParts();
            if (upsert_fenced)
                return;
        }
        std::unique_lock operation_lock(upsert_operation_mutex, std::try_to_lock);
        if (!operation_lock.owns_lock())
        {
            upsert_maintenance_task->scheduleAfter(1000);
            return;
        }
        const size_t max_source_bytes = (*getSettings())[MergeTreeSetting::upsert_maintenance_target_bytes];
        phase = "cleaning retired payloads";
        clearOldPartsFromFilesystem(false, false);
        {
            auto lock = lockParts();
            phase = "pruning durable retirement records";
            upsert_store->pruneRetired();
        }

        if (merger_mutator.merges_blocker.isCancelled())
            return;

        DataPartsVector sources;
        String partition;
        const auto metadata = getInMemoryMetadataPtr(getContext(), true);
        has_ttl = metadata->hasAnyTTL();
        const auto now = time(nullptr);
        const bool apply_ttl = metadata->hasAnyTTL() && !merger_mutator.ttl_merges_blocker.isCancelled();
        const auto policy_checksum = Upsert::ttlPolicyChecksum(*metadata);
        const auto previous_cursor = upsert_maintenance_cursor;
        const auto previous_pending = upsert_maintenance_pending;
        {
            auto lock = lockParts();
            if (upsert_fenced)
                return;
            size_t bytes = 0;
            size_t rows = 0;
            size_t lineage_bytes = 0;
            size_t state_bytes = 0;
            bool needs_rewrite = false;
            auto worthy = [&] { return sources.size() >= 2 || needs_rewrite; };
            /// The native ordered index is maintained incrementally on publication. Visit a bounded slice
            /// per tick; the cursor also advances over healthy parts instead of repeatedly rescanning them.
            constexpr size_t max_candidates = 256;
            const auto active = getDataPartsStateRange(DataPartState::Active);
            auto next = upsert_maintenance_cursor
                ? data_parts_by_state_and_info.upper_bound(DataPartStateAndInfo{DataPartState::Active, *upsert_maintenance_cursor})
                : active.begin();
            if (!upsert_maintenance_cursor || next == active.end())
            {
                upsert_maintenance_schedule.beginCycle();
                upsert_maintenance_cursor.reset();
                next = active.begin();
                upsert_maintenance_pending.reset();
            }
            DataPartsVector candidates;
            candidates.reserve(max_candidates + 1);
            if (upsert_maintenance_pending && upsert_maintenance_pending->getState() == DataPartState::Active)
                candidates.push_back(upsert_maintenance_pending);
            upsert_maintenance_pending.reset();
            for (size_t visited = 0; next != active.end() && visited < max_candidates; ++next, ++visited)
                candidates.push_back(*next);
            const auto advance_cursor = [&](const DataPartPtr & candidate)
            {
                /// A carried candidate is behind the cursor and must never move it backwards.
                if (!upsert_maintenance_cursor || *upsert_maintenance_cursor < candidate->info)
                    upsert_maintenance_cursor = candidate->info;
            };
            const auto snapshot = upsert_store->snapshot();
            phase = "validating maintenance candidates";
            for (const auto & candidate : candidates)
            {
                diagnostic_part = candidate;
                ProfileEvents::increment(ProfileEvents::UpsertMaintenanceCandidates);
                const auto & id = candidate->info.getPartitionId();
                if (id != partition)
                {
                    if (worthy())
                        break;
                    sources.clear();
                    bytes = 0;
                    rows = 0;
                    lineage_bytes = 0;
                    state_bytes = 0;
                    needs_rewrite = false;
                    partition = id;
                }
                const auto size = candidate->getBytesUncompressedOnDisk();
                fiu_do_on(FailPoints::upsert_maintenance_corrupt_candidate,
                {
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "Injected UpsertMergeTree corrupt maintenance candidate");
                });
                const auto checksum = candidate->checksums.files.find(Upsert::ROWS_FILE);
                if (checksum == candidate->checksums.files.end() || checksum->second.is_compressed
                    || checksum->second.file_size <= Upsert::PART_ROWS_ENVELOPE_BYTES
                    || checksum->second.file_size > Upsert::MAX_RECORD_BYTES)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance source has invalid lineage size");
                /// Resolving `SELF` and sorting preserve encoded row widths. Filtering can only reduce this bound.
                const auto candidate_lineage_bytes = checksum->second.file_size - Upsert::PART_ROWS_ENVELOPE_BYTES;
                const auto mask = snapshot->getMask(candidate->getDeleteBitmapCacheIdentity());
                if (!mask)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance source lacks a mask");
                if (mask->cardinality() > candidate->rows_count)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance mask exceeds its source rows");
                const size_t version_bytes = merging_params.upsert_version_column.empty() ? 0 : 8;
                if (candidate_lineage_bytes < candidate->rows_count * (50 + version_bytes))
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance lineage is too short for its source rows");
                /// Bound winner relocation plus the original authorization retained in each retirement record.
                /// Dead rows need no winner `Put`; retain their key bytes and worst-case original kills in the bound.
                const size_t winner_overhead = 73 + id.size() + version_bytes;
                const auto candidate_state_bytes = Upsert::estimateInsertStateBytes(
                    checksum->second.file_size, candidate->rows_count, id.size(), 1024)
                    - mask->cardinality() * winner_overhead;
                const auto part_policy = candidate->checksums.files.find(Upsert::TTL_FILE);
                const bool stale_ttl = part_policy == candidate->checksums.files.end()
                    || part_policy->second.file_size != policy_checksum.file_size || part_policy->second.file_hash != policy_checksum.file_hash;
                const bool ttl_due = apply_ttl && (stale_ttl || (candidate->ttl_infos.part_min_ttl && candidate->ttl_infos.part_min_ttl <= now));
                const bool rewrite = ttl_due || (mask->cardinality() > 0 && mask->cardinality() * 4 >= candidate->rows_count);
                if (!rewrite)
                {
                    const auto saturated = upsert_saturated_parts.find(candidate->name);
                    if (saturated != upsert_saturated_parts.end() && max_source_bytes <= saturated->second)
                    {
                        ProfileEvents::increment(ProfileEvents::UpsertMaintenanceSaturatedSkips);
                        advance_cursor(candidate);
                        continue;
                    }
                }
                /// Target-sized healthy parts are complete; they must not block later small parts in any budget dimension.
                if (!rewrite && (size > max_source_bytes / 2 || candidate->rows_count > Upsert::MAX_UNIT_ROWS / 2
                    || candidate_lineage_bytes > max_lineage_bytes / 2 || candidate_state_bytes > Upsert::MAX_PENDING_STATE_BYTES / 2))
                {
                    advance_cursor(candidate);
                    continue;
                }
                /// Do not repeatedly rewrite a large healthy prefix for a trickle of new rows.
                /// Small tails accumulate into their own tier before joining a larger part.
                /// An urgent source can start its own run; it never needs a size-matched partner.
                if (!sources.empty()
                    && (size < bytes / 4 + (bytes % 4 != 0) || bytes < size / 4 + (size % 4 != 0)))
                {
                    ProfileEvents::increment(ProfileEvents::UpsertMaintenanceSizeRatioSkips);
                    if (worthy())
                        break;
                    sources.clear();
                    bytes = 0;
                    rows = 0;
                    lineage_bytes = 0;
                    state_bytes = 0;
                }
                if (size > max_source_bytes - bytes || candidate->rows_count > Upsert::MAX_UNIT_ROWS - rows
                    || candidate_lineage_bytes > max_lineage_bytes - lineage_bytes
                    || candidate_state_bytes > Upsert::MAX_PENDING_STATE_BYTES - state_bytes)
                {
                    if (worthy())
                        break;
                    if (!rewrite)
                    {
                        advance_cursor(candidate);
                        continue;
                    }
                    /// A healthy prefix must not starve a due or masked source. Rewrite it alone;
                    /// output and state commits are already split into bounded units.
                    sources.clear();
                    sources.push_back(candidate);
                    needs_rewrite = true;
                    advance_cursor(candidate);
                    break;
                }
                advance_cursor(candidate);
                needs_rewrite |= rewrite;
                sources.push_back(candidate);
                bytes += size;
                rows += candidate->rows_count;
                lineage_bytes += candidate_lineage_bytes;
                state_bytes += candidate_state_bytes;
                if (sources.size() == max_sources)
                    break;
            }
            continue_scan = next != active.end()
                || (!candidates.empty() && (!upsert_maintenance_cursor || *upsert_maintenance_cursor < candidates.back()->info));
            if (!worthy())
            {
                /// Preserve a lone small candidate across scan boundaries, without pinning an unbounded queue.
                if (continue_scan && !sources.empty())
                    upsert_maintenance_pending = sources.front();
                sources.clear();
            }
        }

        if (!sources.empty())
        {
            MaintenanceAdmission admission;
            if (!admission.tryAcquire(estimateRewriteMemory(sources, max_source_bytes), getContext()->getServerSettings()))
            {
                /// Retry the same bounded slice, including a carried candidate, after resources become available.
                upsert_maintenance_cursor = previous_cursor;
                upsert_maintenance_pending = previous_pending;
                postpone_admission();
                return;
            }
            upsert_maintenance_schedule.admissionSucceeded();
            phase = "rewriting source parts";
            diagnostic_part = sources.front();
            /// Give factory usage tracking and `DEFAULT`/`TTL` expressions a query context owned by this merge.
            auto rewrite_context = Context::createCopy(getContext()->getBackgroundContext());
            rewrite_context->makeQueryContextForMerge(*getSettings());
            auto thread_group = ThreadGroup::createForMergeMutate(rewrite_context);
            thread_group->memory_tracker.setMetric(CurrentMetrics::UpsertMaintenanceMemoryTracking);
            SCOPE_EXIT({
                ProfileEvents::increment(ProfileEvents::UpsertMaintenancePeakMemoryBytes, thread_group->memory_tracker.getPeak());
                background_memory_tracker.adjustOnBackgroundTaskEnd(&thread_group->memory_tracker);
                CurrentMetrics::sub(CurrentMetrics::UpsertMaintenanceMemoryTracking, thread_group->memory_tracker.get());
            });
            ThreadGroupSwitcher switcher(thread_group, ThreadName::MERGE_MUTATE, /*allow_existing_group=*/true);
            ProfileEvents::increment(ProfileEvents::UpsertMaintenanceRewritesStarted);
            did_work = rewriteUpsertParts(sources, metadata, rewrite_context, apply_ttl, {}, max_source_bytes, true);
            if (did_work)
                upsert_maintenance_schedule.rewriteSucceeded();
        }
        else
            upsert_maintenance_schedule.admissionSucceeded();
    }
    catch (...)
    {
        if (getCurrentExceptionCode() == ErrorCodes::ABORTED && (shutdown_called || flush_called))
            return;
        ProfileEvents::increment(ProfileEvents::UpsertMaintenanceFailures);
        if (isMaintenanceMemoryFailure())
        {
            ProfileEvents::increment(ProfileEvents::UpsertMaintenanceMemoryBackoffs);
            const auto delay = upsert_maintenance_schedule.memoryFailure();
            tryLogCurrentException(log, fmt::format("UpsertMergeTree maintenance memory limit while {} (part {}); retry in {} ms",
                phase, diagnostic_part ? diagnostic_part->name : "none", delay));
            upsert_maintenance_task->scheduleAfter(delay);
            return;
        }
        if (getCurrentExceptionCode() == ErrorCodes::CORRUPTED_DATA)
        {
            {
                auto lock = lockParts();
                upsert_fenced = true;
            }
            ProfileEvents::increment(ProfileEvents::UpsertMaintenanceCorruptionFailures);
            tryLogCurrentException(log, fmt::format(
                "UpsertMergeTree maintenance detected corruption while {} (part {}); table fenced pending restart and recovery",
                phase, diagnostic_part ? diagnostic_part->name : "none"));
            return;
        }
        failed = true;
        tryLogCurrentException(log, fmt::format("UpsertMergeTree maintenance failed while {} (part {})",
            phase, diagnostic_part ? diagnostic_part->name : "none"));
    }
    upsert_maintenance_task->scheduleAfter(failed ? 1000
        : upsert_maintenance_schedule.finishSlice(!continue_scan, did_work, has_ttl));
}

bool StorageMergeTree::rewriteUpsertParts(const DataPartsVector & sources, const StorageMetadataPtr & metadata,
    ContextPtr query_context, bool apply_ttl, const MutationCommands & commands, size_t max_output_bytes, bool remember_budget_cuts)
{
    const auto now = time(nullptr);
    const auto policy_checksum = Upsert::ttlPolicyChecksum(*metadata);
    const auto partition = sources.front()->info.getPartitionId();
    Upsert::SnapshotPtr snapshot = getUpsertReadSnapshotForQuery()->visibility;
    for (const auto & source : sources)
        snapshot->verifyPart(source->name, source->getDeleteBitmapCacheIdentity());
    std::vector<Upsert::PartRows> lineage;
    {
        auto lock = lockParts();
        if (upsert_fenced)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree is fenced pending restart and recovery");
        snapshot = upsert_store->snapshot();
        for (const auto & source : sources)
            lineage.push_back(upsert_store->loadPartRows(source->name));
    }
    auto context = Context::createCopy(query_context);
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
    auto ttl_algorithms = apply_ttl ? buildDeleteTTLAlgorithms(*metadata, context, now) : std::vector<TTLAlgorithmPtr>{};
    Upsert::PartRows expired;
    std::vector<String> source_names;
    for (const auto & source : sources)
        source_names.push_back(source->name);
    bool deleted_any = false;
    bool did_work = false;
    size_t lineage_bytes = Upsert::PART_ROWS_ENVELOPE_BYTES;
    size_t unit_rows = 0;
    std::vector<std::vector<UInt32>> source_offsets(sources.size());
    const Upsert::StateStore::MergeInput merge_input{snapshot, lineage, source_offsets};
    auto flush = [&](bool retire_sources)
    {
        if (shutdown_called || flush_called)
            throw Exception(ErrorCodes::ABORTED, "UpsertMergeTree rewrite cancelled during shutdown");
        output.setColumns(std::move(output_columns));

        MergeTreeTemporaryPartPtr temporary;
        Upsert::PartCommit authorization;
        Upsert::PartRows output_rows;
        authorization.part = Upsert::newObjectID();
        if (output.rows())
        {
            if (remember_budget_cuts)
                ProfileEvents::increment(ProfileEvents::UpsertMaintenanceWrittenBytes, output.bytes());
            output.insert({std::move(ordinals), std::make_shared<DataTypeUInt64>(), Upsert::ORDINAL_COLUMN});
            output.insert({std::move(row_lineage), std::make_shared<DataTypeString>(), Upsert::LINEAGE_COLUMN});
            BlockWithPartition block;
            block.block = std::make_shared<Block>(std::move(output));
            block.partition = sources.front()->partition;
            temporary = writer.writeTempPart(block, metadata, context, false, /*force_projections=*/true);
            temporary->finalize();
            auto & part = temporary->part;
            part->getDataPartStorage().commitTransaction();
            const auto path = part->getDataPartStorage().getFullPath();
            output_rows = Upsert::decodePartRows(Upsert::readBoundedFile(std::filesystem::path(path) / Upsert::ROWS_FILE, Upsert::MAX_RECORD_BYTES));
            authorization.part = {UUIDHelpers::getHighBytes(part->uuid), UUIDHelpers::getLowBytes(part->uuid)};
            authorization.temporary_name = part->getDataPartStorage().getPartDirectory();
            authorization.files_hash = Upsert::hashPartDirectory(path, true, &authorization.files_inventory);
            authorization.rows = part->rows_count;
            Upsert::syncDirectory(std::filesystem::path(path) / "..");
        }
        UpsertReadSnapshotPtr retired_snapshot;
        bool produces_part = false;
        {
            FailPointInjection::pauseFailPoint(FailPoints::upsert_pause_before_merge_commit);
            std::lock_guard ttl_lock(upsert_ttl_mutex);
            const auto current_metadata = getInMemoryMetadataPtr(getContext(), true);
            const auto current_policy_checksum = Upsert::ttlPolicyChecksum(*current_metadata);
            if (apply_ttl && (current_policy_checksum.file_size != policy_checksum.file_size
                || current_policy_checksum.file_hash != policy_checksum.file_hash))
            {
                /// Rebuild under the current policy; an old `TTL` result must not commit after an alter.
                return false;
            }
            auto lock = lockParts();
            if (upsert_fenced)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree is fenced pending restart and recovery");
            if (temporary)
            {
                auto holder = fillNewPartName(temporary->part, lock);
                authorization.final_name = temporary->part->name;
            }
            decltype(upsert_saturated_parts)::node_type saturated_hint;
            if (remember_budget_cuts && !retire_sources && temporary)
            {
                /// Allocate optional scheduling metadata before the durable commit; publish it afterward.
                upsert_saturated_parts.reserve(upsert_saturated_parts.size() + 1);
                decltype(upsert_saturated_parts) staging;
                staging.emplace(temporary->part->name, max_output_bytes);
                saturated_hint = staging.extract(temporary->part->name);
            }
            auto prepared = upsert_store->prepareMerge(
                partition, std::move(authorization), output_rows, source_names, expired, retire_sources, &merge_input);
            const auto & retired_sources = retire_sources ? sources : DataPartsVector{};
            auto read_snapshot = prepareUpsertReadSnapshot(
                prepared.snapshot, prepared.produces_part ? temporary->part : nullptr, retired_sources, lock);
            bool persist_started = false;
            auto persist = [&]
            {
                persist_started = true;
                upsert_store->append(prepared);
            };
            try
            {
                if (prepared.produces_part)
                    commitUpsertPart(temporary->part, persist, lock);
                else
                    persist();
                removePartsFromWorkingSet(nullptr, retired_sources, true, lock);
                upsert_store->publish(prepared);
                retired_snapshot = publishUpsertReadSnapshot(std::move(read_snapshot));
                if (prepared.produces_part && !saturated_hint.empty())
                    upsert_saturated_parts.insert(std::move(saturated_hint));
                for (const auto & retired : retired_sources)
                    upsert_saturated_parts.erase(retired->name);
                chassert(upsert_saturated_parts.size() <= getActivePartsCount());
                notifyUpsertMaintenance();
                did_work = true;
                output = metadata->getSampleBlock().cloneEmpty();
                output_columns = output.mutateColumns();
                ordinals = ColumnUInt64::create();
                row_lineage = ColumnString::create();
                expired.clear();
                lineage_bytes = Upsert::PART_ROWS_ENVELOPE_BYTES;
                unit_rows = 0;
                for (auto & offsets : source_offsets)
                    offsets.clear();
                produces_part = prepared.produces_part;
            }
            catch (...)
            {
                if (persist_started)
                    upsert_fenced = true;
                throw;
            }
        }
        if (produces_part)
            prewarmUpsertPartCaches(*temporary);
        return true;
    };
    for (size_t source_index = 0; source_index < sources.size(); ++source_index)
    {
        const auto & source = sources[source_index];
        const auto mask = snapshot->getMask(source->getDeleteBitmapCacheIdentity());
        DeleteBitmap deleted;
        if (!commands.empty())
        {
            auto available = columns.getNames();
            available.push_back("_part_offset");
            MutationsInterpreter::Settings settings(true);
            settings.return_all_columns = true;
            settings.return_mutated_rows = true;
            settings.apply_deleted_mask = false;
            MutationsInterpreter interpreter(*this, source, std::make_shared<AlterConversions>(), metadata,
                commands, std::move(available), context, settings);
            auto deletion_pipeline = QueryPipelineBuilder::getPipeline(interpreter.execute());
            PullingPipelineExecutor deletion_executor(deletion_pipeline);
            Block selected;
            while (deletion_executor.pull(selected))
            {
                const auto & offsets = *selected.getByName("_part_offset").column;
                for (size_t row = 0; row < selected.rows(); ++row)
                {
                    const auto offset = offsets.getUInt(row);
                    if (offset >= source->rows_count)
                        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree delete offset exceeds source rows");
                    if (!mask->contains(static_cast<UInt32>(offset)))
                        deleted.add(static_cast<UInt32>(offset));
                }
            }
        }
        /// Foreground deletes currently pass one source. Avoid materializing or rewriting a no-match part.
        if (!commands.empty() && sources.size() == 1 && deleted.empty())
            return false;
        QueryPipeline pipeline(createMergeTreeSequentialSource(
            MergeTreeSequentialSourceType::Merge, *this, storage_snapshot, RangesInDataPart(source),
            std::make_shared<AlterConversions>(), nullptr, columns.getNames(), std::nullopt, nullptr,
            false, false, false));
        PullingPipelineExecutor executor(pipeline);
        Block block;
        size_t offset = 0;
        while (executor.pull(block))
        {
            if (remember_budget_cuts)
                ProfileEvents::increment(ProfileEvents::UpsertMaintenanceReadBytes, block.bytes());
            if (shutdown_called || flush_called)
                throw Exception(ErrorCodes::ABORTED, "UpsertMergeTree rewrite cancelled during shutdown");
            if (offset + block.rows() > lineage[source_index].size())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance read exceeds source lineage");
            const auto ttl_keep = filterTTL(block, ttl_algorithms);
            IColumn::Filter keep(block.rows(), 0);
            std::vector<UInt32> visible_offsets;
            std::vector<String> encoded_lineage;
            std::vector<size_t> lineage_prefix{0};
            std::vector<size_t> kept_prefix{0};
            visible_offsets.reserve(block.rows());
            encoded_lineage.reserve(block.rows());
            lineage_prefix.reserve(block.rows() + 1);
            kept_prefix.reserve(block.rows() + 1);
            for (size_t row = 0; row < block.rows(); ++row)
            {
                const auto source_offset = static_cast<UInt32>(offset + row);
                if (mask->contains(source_offset))
                    continue;
                const auto & original = lineage[source_index][source_offset];
                visible_offsets.push_back(source_offset);
                encoded_lineage.push_back(Upsert::encodeIndexValue(original.value));
                lineage_prefix.push_back(lineage_prefix.back() + original.key.size() + encoded_lineage.back().size() + 8);
                keep[row] = !deleted.contains(source_offset) && (ttl_keep.empty() || ttl_keep[row]);
                kept_prefix.push_back(kept_prefix.back() + keep[row]);
            }
            if (visible_offsets.empty())
            {
                offset += block.rows();
                continue;
            }

            /// Filter each column once; retained rows are contiguous in the resulting columns.
            Columns input_columns;
            for (const auto & column : output)
            {
                auto input = block.getByName(column.name).column->convertToFullIfWrapped();
                if (kept_prefix.back() != block.rows())
                    input = input->filter(keep, kept_prefix.back());
                input_columns.push_back(std::move(input));
            }
            size_t begin = 0;
            size_t max_batch_rows = visible_offsets.size();
            while (begin < visible_offsets.size())
            {
                size_t lower = 0;
                size_t upper = std::min({max_batch_rows, visible_offsets.size() - begin, Upsert::MAX_UNIT_ROWS - unit_rows});
                while (lower < upper)
                {
                    const auto count = lower + (upper - lower + 1) / 2;
                    const auto candidate_lineage_bytes = lineage_prefix[begin + count] - lineage_prefix[begin];
                    /// Reserve half the state budget for source masks and retirement authorizations.
                    if (candidate_lineage_bytes <= Upsert::MAX_RECORD_BYTES - lineage_bytes
                        && Upsert::estimateInsertStateBytes(
                            lineage_bytes + candidate_lineage_bytes, unit_rows + count, partition.size(), 1024)
                            <= Upsert::MAX_PENDING_STATE_BYTES / 2)
                        lower = count;
                    else
                        upper = count - 1;
                }
                size_t end = begin + lower;
                if (end == begin)
                {
                    if (!unit_rows)
                        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree rewrite row exceeds its state budget");
                    if (!flush(false))
                        return did_work;
                    continue;
                }

                const auto previous_rows = output_columns.front()->size();
                bool retry_after_flush = false;
                while (true)
                {
                    const auto rows = kept_prefix[end] - kept_prefix[begin];
                    size_t candidate_bytes = 0;
                    for (size_t column = 0; column < input_columns.size(); ++column)
                    {
                        if (rows)
                            output_columns[column]->insertRangeFrom(*input_columns[column], kept_prefix[begin], rows);
                        candidate_bytes += output_columns[column]->byteSize();
                    }
                    /// Measure the actual representation, including shared values in `LowCardinality` dictionaries.
                    /// A single wide value is indivisible and remains subject to the query memory limit.
                    if (candidate_bytes <= max_output_bytes || previous_rows + rows <= 1)
                        break;

                    /// Compact the retained prefix: `popBack` alone can leave unused dictionary values behind.
                    for (auto & column : output_columns)
                    {
                        auto prefix = column->cloneEmpty();
                        if (previous_rows)
                            prefix->insertRangeFrom(*column, 0, previous_rows);
                        column = std::move(prefix);
                    }
                    if (unit_rows)
                    {
                        if (!flush(false))
                            return did_work;
                        retry_after_flush = true;
                        break;
                    }
                    end = begin + (end - begin) / 2;
                    /// Reuse the smaller batch for this block instead of repeatedly copying its entire remaining suffix.
                    max_batch_rows = end - begin;
                }
                if (retry_after_flush)
                    continue;

                for (size_t row = begin; row < end; ++row)
                {
                    const auto source_offset = visible_offsets[row];
                    const auto & original = lineage[source_index][source_offset];
                    source_offsets[source_index].push_back(source_offset);
                    if (!keep[source_offset - offset])
                    {
                        expired.push_back(original);
                        deleted_any = true;
                        continue;
                    }
                    ordinals->insertValue(original.value.row.block_ordinal);
                    const auto & encoded = encoded_lineage[row];
                    row_lineage->insertData(encoded.data(), encoded.size());
                }
                lineage_bytes += lineage_prefix[end] - lineage_prefix[begin];
                unit_rows += end - begin;
                begin = end;
            }
            offset += block.rows();
        }
        if (offset != source->rows_count)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance read omitted source rows");
    }
    if (!commands.empty() && !deleted_any && !did_work)
        return false;
    return flush(true) || did_work;
}

bool StorageMergeTree::upsertPartNeedsSchemaRewrite(
    const DataPartPtr & part, const StorageMetadataPtr & metadata, ContextPtr query_context) const
{
    if (std::ranges::any_of(part->getColumns(), [&](const auto & column)
    {
        return !metadata->getColumns().hasPhysical(column.name) && !column.name.starts_with("_upsert_")
            && !metadata->virtuals.has(column.name);
    }))
        return true;

    for (const auto & [name, projection] : part->getProjectionParts())
        if (!metadata->getProjections().has(name))
            return true;

    NameSet index_files;
    for (const auto & description : metadata->getSecondaryIndices())
    {
        const auto index = MergeTreeIndexFactory::instance().get(metadata, description, *getSettings());
        const auto name = index->getFileName();
        for (const auto & stream : index->getAllSubstreamsInPart(part->checksums, name, &part->getDataPartStorage()))
        {
            const auto prefix = name + stream.suffix;
            index_files.insert(prefix + stream.extension);
            index_files.insert(sipHash128String(prefix) + stream.extension);
        }
    }
    auto obsolete_index = [&](const String & name)
    {
        const auto dot = name.rfind('.');
        if (dot == String::npos || !std::string_view(name).substr(dot).starts_with(".idx"))
            return false;
        /// Index payloads use either the reserved prefix or a hashed stream name.
        /// Column marks with a similar prefix are ordinary column files.
        const bool hashed = dot == 32 && std::ranges::all_of(std::string_view(name).substr(0, dot), [](char c)
        {
            return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
        });
        return (name.starts_with(SKIP_INDEX_FILE_PREFIX) || hashed) && !index_files.contains(name);
    };
    for (const auto & [name, checksum] : part->checksums.files)
    {
        if (name.ends_with(".proj") && !metadata->getProjections().has(name.substr(0, name.size() - 5)))
            return true;
        if (obsolete_index(name))
            return true;
        if (name == SKIP_INDICES_PACKED_FILENAME)
        {
            auto input = part->getDataPartStorage().readFile(name, query_context->getReadSettings(), std::nullopt);
            const PackedFilesReader archive(PackedFilesReader::readIndex(*input));
            for (const auto & member : archive.getFileNames())
                if (obsolete_index(member))
                    return true;
        }
    }
    return false;
}

void StorageMergeTree::reconcileUpsertParts(const StorageMetadataPtr & metadata, ContextPtr query_context)
{
    for (const auto & part : getDataPartsVectorForInternalUsage())
        if (upsertPartNeedsSchemaRewrite(part, metadata, query_context))
            rewriteUpsertParts({part}, metadata, query_context, false);
}

void StorageMergeTree::truncateUpsert(ContextPtr query_context)
{
    assertNotReadonly();
    if (!upsert_store->usesDiskState())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree TRUNCATE requires the disk_state_v2 protocol");
    if (query_context->getCurrentTransaction())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree does not support explicit transactions");
    std::lock_guard operation_lock(upsert_operation_mutex);
    {
        UpsertReadSnapshotPtr retired_snapshot;
        /// Wait for whole insert pipelines, including prepared parts and token admission checks.
        auto schema_lock = tryLockTimed(upsert_schema_lock, RWLockImpl::Write,
            query_context->getCurrentQueryId(), query_context->getSettingsRef()[Setting::lock_acquire_timeout]);
        std::lock_guard ttl_lock(upsert_ttl_mutex);
        auto lock = lockParts();
        if (upsert_fenced)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree is fenced pending restart and recovery");
        const auto parts = getDataPartsVectorForInternalUsage({DataPartState::Active}, lock);
        auto prepared = upsert_store->prepareTruncate();
        NameSet authorized(prepared.retired_parts.begin(), prepared.retired_parts.end());
        if (parts.size() != authorized.size() || std::ranges::any_of(parts, [&](const auto & part)
        {
            return !authorized.contains(part->name);
        }))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree truncate catalog differs from active parts");
        /// Build an empty version directly instead of subtracting every retired part from the old snapshot.
        auto read_snapshot = std::make_shared<UpsertReadSnapshot>();
        read_snapshot->visibility = prepared.snapshot;
        read_snapshot->parts_info = getPartsSnapshotInfo({});
        try
        {
            upsert_store->append(prepared);
            removePartsFromWorkingSet(nullptr, parts, true, lock);
            upsert_store->publish(prepared);
            retired_snapshot = publishUpsertReadSnapshot(std::move(read_snapshot));
            notifyUpsertMaintenance();
            upsert_maintenance_cursor.reset();
            upsert_maintenance_pending.reset();
            upsert_saturated_parts.clear();
        }
        catch (...)
        {
            upsert_fenced = true;
            throw;
        }
    }
    /// Existing readers keep their parts and masks pinned until they finish.
    clearOldPartsFromFilesystem(false, false);
    if (upsert_maintenance_task)
        upsert_maintenance_task->schedule();
}

void StorageMergeTree::mutateUpsert(const MutationCommands & commands, ContextPtr query_context)
{
    checkMutationIsPossible(commands, query_context->getSettingsRef());
    if (!upsert_store->usesDiskState())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree mutations require the disk_state_v2 protocol");
    if (query_context->getCurrentTransaction())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree does not support explicit transactions");
    std::lock_guard operation_lock(upsert_operation_mutex);
    const auto metadata = getInMemoryMetadataPtr(query_context, true);
    for (const auto & command : commands)
    {
        const auto ast = command.ast();
        const auto partition = ast->partition ? std::optional<String>(getPartitionIDFromQuery(ast->partition->ptr(), query_context)) : std::nullopt;
        if (command.type == MutationCommand::MATERIALIZE_INDEX && !metadata->getSecondaryIndices().has(command.index_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown index {}", command.index_name);
        if (command.type == MutationCommand::MATERIALIZE_PROJECTION && !metadata->getProjections().has(command.projection_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown projection {}", command.projection_name);
        MutationCommands deletions;
        if (command.type == MutationCommand::DELETE)
            deletions.push_back(command);
        for (const auto & part : getDataPartsVectorForInternalUsage())
        {
            if (partition && part->info.getPartitionId() != *partition)
                continue;
            rewriteUpsertParts({part}, metadata, query_context, false, deletions);
        }
    }
}
}
