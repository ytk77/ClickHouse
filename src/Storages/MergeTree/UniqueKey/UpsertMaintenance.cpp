#include <Storages/MergeTree/UniqueKey/UpsertDiskSpace.h>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/UniqueKey/UpsertMetrics.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/UniqueKey/UpsertStateStore.h>
#include <Storages/MergeTree/UniqueKey/UpsertMaintenanceMemory.h>
#include <Storages/MergeTree/UniqueKey/UpsertOperationWait.h>
#include <Storages/MergeTree/UniqueKey/UpsertBinary.h>
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
#include <unordered_map>
#include <new>

namespace DB::ErrorCodes
{
extern const int NOT_ENOUGH_SPACE;
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
extern const char upsert_pause_before_rewrite_read[];
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
extern const MergeTreeSettingsUInt64 upsert_rewrite_state_reserve_bytes;
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
        const auto lineage_size = source->upsert_lineage_decoded_size.load(std::memory_order_relaxed);
        chassert(lineage_size != 0);
        lineage += lineage_size;
        rows += source->rows_count;
        state_bytes += Upsert::estimateInsertStateBytes(lineage_size, source->rows_count, source->info.getPartitionId().size(), 1024);
    }
    /// Source lineage remains loaded while column buffers and a state batch are built.
    /// Saturate an unrepresentable estimate so admission rejects it without attempting allocation.
    const UInt64 fixed = 4 * lineage + rows * (2 * sizeof(Upsert::PartRow) + 64)
        + std::min<UInt64>(state_bytes, Upsert::MAX_PENDING_STATE_BYTES / 2);
    const UInt64 output_bytes = std::min(target, selected_bytes);
    const UInt64 max_estimate = std::numeric_limits<Int64>::max() / 2;
    if (fixed > max_estimate || output_bytes > (max_estimate - fixed) / 3)
        return std::numeric_limits<UInt64>::max();
    const UInt64 subtotal = fixed + 3 * output_bytes;
    if (largest_source > max_estimate - subtotal)
        return std::numeric_limits<UInt64>::max();
    /// Native sorting and `SSTIndexWriter` can retain further copies of keys, values and columns.
    return 2 * (subtotal + largest_source);
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
    Upsert::ForegroundOperationWait foreground_wait(upsert_foreground_waiters, changes_schema);
    std::unique_lock operation_lock(upsert_operation_mutex, std::defer_lock);
    if (changes_schema)
        operation_lock.lock();
    foreground_wait.acquired();
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
        if (upsert_foreground_waiters.load(std::memory_order_relaxed))
        {
            ProfileEvents::increment(ProfileEvents::UpsertMaintenanceYields);
            upsert_verification_task->scheduleAfter(1000);
            return;
        }
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
    constexpr size_t max_lineage_bytes = Upsert::MAX_PART_LINEAGE_BYTES - Upsert::PART_ROWS_ENVELOPE_BYTES;
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
    const auto previous_cursor = upsert_maintenance_cursor;
    const auto previous_pending = upsert_maintenance_pending;
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
        if (upsert_foreground_waiters.load(std::memory_order_relaxed))
        {
            ProfileEvents::increment(ProfileEvents::UpsertMaintenanceYields);
            upsert_maintenance_task->scheduleAfter(1000);
            return;
        }
        std::unique_lock operation_lock(upsert_operation_mutex, std::try_to_lock);
        if (!operation_lock.owns_lock())
        {
            upsert_maintenance_task->scheduleAfter(1000);
            return;
        }
        const UInt64 configured_target = (*getSettings())[MergeTreeSetting::upsert_maintenance_target_bytes];
        const UInt64 memory_ceiling = Upsert::maintenanceMemoryCeiling(
            background_memory_tracker.getSoftLimit(), total_memory_tracker.getHardLimit());
        const size_t max_source_bytes = Upsert::maintenanceEffectiveTarget(configured_target, memory_ceiling);
        if (upsert_maintenance_effective_target != max_source_bytes)
        {
            LOG_INFO(log, "UpsertMergeTree maintenance effective target {} bytes (configured {} bytes, memory ceiling {} bytes)",
                max_source_bytes, configured_target, memory_ceiling);
            upsert_maintenance_effective_target = max_source_bytes;
        }
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
        DataPartsVector candidates;
        bool more_candidates = false;
        {
            auto lock = lockParts();
            if (upsert_fenced)
                return;
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
            candidates.reserve(max_candidates + 1);
            if (upsert_maintenance_pending && upsert_maintenance_pending->getState() == DataPartState::Active)
                candidates.push_back(upsert_maintenance_pending);
            upsert_maintenance_pending.reset();
            for (size_t visited = 0; next != active.end() && visited < max_candidates; ++next, ++visited)
                candidates.push_back(*next);
            more_candidates = next != active.end();
        }
        /// Pinned sources and the operation mutex keep lineage immutable. Read planner headers
        /// outside the parts lock; cached sizes never substitute for authenticated source reads.
        phase = "reading lineage planner headers";
        for (const auto & candidate : candidates)
        {
            const auto checksum = candidate->checksums.files.find(Upsert::ROWS_FILE);
            if (checksum == candidate->checksums.files.end() || checksum->second.is_compressed)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance source has invalid lineage checksum");
            if (!candidate->upsert_lineage_decoded_size.load(std::memory_order_relaxed))
                candidate->upsert_lineage_decoded_size.store(Upsert::readPartRowsDecodedSize(
                    (std::filesystem::path(candidate->getDataPartStorage().getFullPath()) / Upsert::ROWS_FILE).string(),
                    checksum->second.file_size, candidate->rows_count), std::memory_order_relaxed);
        }
        const auto total_disk = getDisks().front()->getTotalSpace();
        if (!total_disk)
            throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Cannot determine UpsertMergeTree disk capacity");
        {
            auto lock = lockParts();
            if (upsert_fenced)
                return;
            size_t bytes = 0;
            size_t rows = 0;
            size_t lineage_bytes = 0;
            UInt64 physical_selected = 0;
            bool needs_rewrite = false;
            auto worthy = [&] { return sources.size() >= 2 || needs_rewrite; };
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
                    physical_selected = 0;
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
                    || checksum->second.file_size > Upsert::partRowsStoredSizeLimit(Upsert::MAX_PART_LINEAGE_BYTES))
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance source has invalid lineage size");
                /// Resolving `SELF` and sorting preserve encoded row widths. Filtering can only reduce this bound.
                const auto candidate_lineage_bytes = candidate->upsert_lineage_decoded_size.load(std::memory_order_relaxed)
                    - Upsert::PART_ROWS_ENVELOPE_BYTES;
                const auto mask = snapshot->getMask(candidate->getDeleteBitmapCacheIdentity());
                if (!mask)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance source lacks a mask");
                if (mask->cardinality() > candidate->rows_count)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance mask exceeds its source rows");
                const size_t version_bytes = merging_params.upsert_version_column.empty() ? 0 : 8;
                if (candidate_lineage_bytes < candidate->rows_count * (50 + version_bytes))
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance lineage is too short for its source rows");
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
                if (!rewrite && (size > max_source_bytes / 2 || candidate->rows_count > Upsert::MAX_PART_ROWS / 2
                    || candidate_lineage_bytes > max_lineage_bytes / 2))
                {
                    advance_cursor(candidate);
                    continue;
                }
                const UInt64 alive = candidate->rows_count - mask->cardinality();
                const UInt64 physical = candidate->getBytesOnDisk();
                const UInt64 physical_candidate = (physical / candidate->rows_count) * alive + (physical % candidate->rows_count) * alive / candidate->rows_count;
                const UInt64 disk_estimate = Upsert::rewriteDiskBytes(
                    physical_candidate,
                    (*getSettings())[MergeTreeSetting::upsert_rewrite_state_reserve_bytes]);
                if (Upsert::saturatingDiskBytes(disk_estimate, Upsert::DISK_SPACE_FLOOR) > *total_disk)
                {
                    if (worthy())
                        break;
                    ProfileEvents::increment(ProfileEvents::UpsertMaintenanceDiskUnadmittableParts);
                    const auto denials = ++upsert_maintenance_unadmittable;
                    if ((denials & (denials - 1)) == 0)
                        LOG_WARNING(log, "UpsertMergeTree source {} disk estimate {} exceeds capacity {}; skipping it", candidate->name, disk_estimate, *total_disk);
                    advance_cursor(candidate);
                    continue;
                }
                const auto single_estimate = estimateRewriteMemory({candidate}, max_source_bytes);
                if (single_estimate > memory_ceiling)
                {
                    if (worthy())
                        break;
                    ProfileEvents::increment(ProfileEvents::UpsertMaintenanceUnadmittableParts);
                    const auto denials = ++upsert_maintenance_unadmittable;
                    if ((denials & (denials - 1)) == 0)
                        LOG_WARNING(log, "UpsertMergeTree source {} cannot fit configured memory ceiling {} (estimate {}); skipping it so other parts can progress",
                            candidate->name, memory_ceiling, single_estimate);
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
                    physical_selected = 0;
                }
                if (size > max_source_bytes - bytes || candidate->rows_count > Upsert::MAX_PART_ROWS - rows
                    || candidate_lineage_bytes > max_lineage_bytes - lineage_bytes)
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
                auto prospective = sources;
                prospective.push_back(candidate);
                if (estimateRewriteMemory(prospective, max_source_bytes) > memory_ceiling
                    || Upsert::saturatingDiskBytes(Upsert::rewriteDiskBytes(
                        Upsert::saturatingDiskBytes(physical_selected, physical_candidate),
                        (*getSettings())[MergeTreeSetting::upsert_rewrite_state_reserve_bytes]), Upsert::DISK_SPACE_FLOOR) > *total_disk)
                {
                    if (worthy())
                        break;
                    /// An unusual lineage density can exceed the static target's allowance.
                    /// A single viable candidate starts a new run; no oversized prefix blocks later work.
                    sources.clear();
                    bytes = 0;
                    rows = 0;
                    lineage_bytes = 0;
                    physical_selected = 0;
                    needs_rewrite = false;
                }
                advance_cursor(candidate);
                needs_rewrite |= rewrite;
                sources.push_back(candidate);
                bytes += size;
                rows += candidate->rows_count;
                lineage_bytes += candidate_lineage_bytes;
                physical_selected = Upsert::saturatingDiskBytes(physical_selected, physical_candidate);
                if (sources.size() == max_sources)
                    break;
            }
            continue_scan = more_candidates
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
                /// `MemoryTracker` destruction settles background usage and its metric together.
            });
            ThreadGroupSwitcher switcher(thread_group, ThreadName::MERGE_MUTATE, /*allow_existing_group=*/true);
            ProfileEvents::increment(ProfileEvents::UpsertMaintenanceRewritesStarted);
            bool yielded = false;
            did_work = rewriteUpsertParts(sources, metadata, rewrite_context, apply_ttl, {}, max_source_bytes, true, &yielded);
            if (yielded)
            {
                if (!did_work)
                {
                    upsert_maintenance_cursor = previous_cursor;
                    upsert_maintenance_pending = previous_pending;
                }
                upsert_maintenance_task->scheduleAfter(1000);
                return;
            }
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
        if (getCurrentExceptionCode() == ErrorCodes::NOT_ENOUGH_SPACE)
        {
            upsert_maintenance_cursor = previous_cursor;
            upsert_maintenance_pending = previous_pending;
            ProfileEvents::increment(ProfileEvents::UpsertMaintenanceDiskBackoffs);
            postpone_admission();
            return;
        }
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
    ContextPtr query_context, bool apply_ttl, const MutationCommands & commands, size_t max_output_bytes,
    bool remember_budget_cuts, bool * yielded)
{
    bool did_work = false;
    const auto yield_to_foreground = [&]
    {
        if (!remember_budget_cuts || !upsert_foreground_waiters.load(std::memory_order_relaxed))
            return false;
        ProfileEvents::increment(ProfileEvents::UpsertMaintenanceYields);
        if (yielded)
            *yielded = true;
        const auto yields = ++upsert_maintenance_yields;
        if ((yields & (yields - 1)) == 0)
            LOG_INFO(log, "UpsertMergeTree maintenance yielded {} times to waiting foreground operations", yields);
        return true;
    };
    if (yield_to_foreground())
        return false;
    const auto now = time(nullptr);
    const auto policy_checksum = Upsert::ttlPolicyChecksum(*metadata);
    const auto partition = sources.front()->info.getPartitionId();
    Upsert::SnapshotPtr snapshot = getUpsertReadSnapshotForQuery()->visibility;
    const auto disk = getDisks().front();
    const UInt64 state_allowance = (*getSettings())[MergeTreeSetting::upsert_rewrite_state_reserve_bytes];
    UInt64 physical_output = 0;
    UInt64 source_compressed_bytes = 0;
    for (const auto & source : sources)
    {
        const auto mask = snapshot->getMask(source->getDeleteBitmapCacheIdentity());
        if (!mask || mask->cardinality() > source->rows_count || !source->rows_count)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree source mask for disk admission");
        const UInt64 alive = source->rows_count - mask->cardinality();
        const UInt64 bytes = source->getBytesOnDisk();
        const UInt64 estimate = (bytes / source->rows_count) * alive + (bytes % source->rows_count) * alive / source->rows_count;
        physical_output = Upsert::saturatingDiskBytes(physical_output, estimate);
        source_compressed_bytes = Upsert::saturatingDiskBytes(source_compressed_bytes, bytes);
    }
    const auto required = Upsert::rewriteDiskBytes(physical_output, state_allowance);
    /// Hold through every relocation and retirement batch; native output writes also reserve their block independently.
    const auto available_before_reservation = disk->getUnreservedSpace();
    if (!available_before_reservation
        || *available_before_reservation < Upsert::saturatingDiskBytes(required, Upsert::DISK_SPACE_FLOOR))
        throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "UpsertMergeTree rewrite postponed: needs {} reserved bytes plus the 1 GiB floor", required);
    auto rewrite_reservation = disk->reserve(required);
    if (!rewrite_reservation)
        throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "UpsertMergeTree rewrite postponed: cannot reserve {} physical output/state bytes", required);
    for (const auto & source : sources)
        snapshot->verifyPart(source->name, source->getDeleteBitmapCacheIdentity());
    std::vector<Upsert::PartRows> lineage;
    std::vector<Upsert::PartCommit> source_authorizations(sources.size());
    for (size_t i = 0; i < sources.size(); ++i)
    {
        if (yield_to_foreground())
            return false;
        lineage.push_back(upsert_store->loadPartRows(sources[i]->name, &source_authorizations[i]));
    }
    {
        auto lock = lockParts();
        if (upsert_fenced)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree is fenced pending restart and recovery");
        snapshot = upsert_store->snapshot();
    }
    if (remember_budget_cuts)
    {
        FailPointInjection::pauseFailPoint(FailPoints::upsert_pause_before_rewrite_read);
        if (yield_to_foreground())
            return false;
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
    const size_t physical_rows_limit = remember_budget_cuts ? Upsert::MAX_PART_ROWS : Upsert::MAX_UNIT_ROWS;
    const size_t physical_lineage_limit = remember_budget_cuts ? Upsert::MAX_PART_LINEAGE_BYTES : Upsert::MAX_RECORD_BYTES;
    size_t lineage_bytes = Upsert::PART_ROWS_ENVELOPE_BYTES;
    size_t unit_rows = 0;
    std::vector<std::vector<UInt32>> source_offsets(sources.size());
    auto flush = [&](bool retire_sources)
    {
        if (shutdown_called || flush_called)
            throw Exception(ErrorCodes::ABORTED, "UpsertMergeTree rewrite cancelled during shutdown");
        if (yield_to_foreground())
            return false;
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
            /// Match native rewrite codec selection from the whole source set for every output chunk.
            temporary = writer.writeTempPart(block, metadata, context, false, /*force_projections=*/true, source_compressed_bytes);
            temporary->finalize();
            auto & part = temporary->part;
            part->getDataPartStorage().commitTransaction();
            const auto path = part->getDataPartStorage().getFullPath();
            output_rows = Upsert::decodePartRows(Upsert::readBoundedFile(std::filesystem::path(path) / Upsert::ROWS_FILE, Upsert::partRowsStoredSizeLimit(Upsert::MAX_PART_LINEAGE_BYTES)), Upsert::MAX_PART_LINEAGE_BYTES);
            authorization.part = {UUIDHelpers::getHighBytes(part->uuid), UUIDHelpers::getLowBytes(part->uuid)};
            authorization.temporary_name = part->getDataPartStorage().getPartDirectory();
            authorization.files_hash = Upsert::hashPartDirectory(path, true, &authorization.files_inventory);
            authorization.rows = part->rows_count;
            Upsert::syncDirectory(std::filesystem::path(path) / "..");
        }
        std::unordered_map<std::string_view, const Upsert::PartRow *> output_by_key;
        for (const auto & row : output_rows)
        {
            if (row.value.row_offset % 65536 == 0)
                CurrentMemoryTracker::check();
            if (!output_by_key.emplace(row.key, &row).second)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree rewrite produced duplicate output keys");
        }
        std::unordered_map<std::string_view, const Upsert::PartRow *> expired_by_key;
        for (const auto & row : expired)
            if (!expired_by_key.emplace(row.key, &row).second || output_by_key.contains(row.key))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree rewrite produced duplicate expiration keys");

        Upsert::PartRows batch_rows;
        Upsert::PartRows batch_expired;
        std::vector<std::vector<UInt32>> batch_offsets(sources.size());
        const Upsert::StateStore::MergeInput batch_input{snapshot, lineage, batch_offsets, &source_authorizations};
        size_t batch_lineage_bytes = Upsert::PART_ROWS_ENVELOPE_BYTES;
        bool published_payload = false;
        auto commit_batch = [&](bool complete_output, bool retire)
        {
            if (shutdown_called || flush_called)
                throw Exception(ErrorCodes::ABORTED, "UpsertMergeTree relocation cancelled during shutdown");
            UpsertReadSnapshotPtr retired_snapshot;
            {
                FailPointInjection::pauseFailPoint(FailPoints::upsert_pause_before_merge_commit);
                std::lock_guard ttl_lock(upsert_ttl_mutex);
                const auto current_metadata = getInMemoryMetadataPtr(getContext(), true);
                const auto current_policy = Upsert::ttlPolicyChecksum(*current_metadata);
                if (apply_ttl && (current_policy.file_size != policy_checksum.file_size || current_policy.file_hash != policy_checksum.file_hash))
                    return false;
                auto lock = lockParts();
                if (upsert_fenced)
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree is fenced pending restart and recovery");
                auto part = authorization;
                std::optional<bool> output_complete;
                if (retire)
                {
                    part = {};
                    part.part = Upsert::newObjectID();
                }
                else if (temporary)
                {
                    if (!published_payload)
                    {
                        auto holder = fillNewPartName(temporary->part, lock);
                        authorization.final_name = temporary->part->name;
                        part.final_name = authorization.final_name;
                    }
                    output_complete = complete_output;
                }
                else
                    part.part = Upsert::newObjectID();
                decltype(upsert_saturated_parts)::node_type saturated_hint;
                if (remember_budget_cuts && !retire_sources && temporary && complete_output)
                {
                    /// Allocate scheduling metadata before persistence, then publish with the new part.
                    upsert_saturated_parts.reserve(upsert_saturated_parts.size() + 1);
                    decltype(upsert_saturated_parts) staging;
                    staging.emplace(temporary->part->name, max_output_bytes);
                    saturated_hint = staging.extract(temporary->part->name);
                }
                auto prepared = upsert_store->prepareMerge(
                    partition, std::move(part), batch_rows, source_names, batch_expired, retire, &batch_input, output_complete);
                const auto & retired_sources = retire ? sources : DataPartsVector{};
                auto read_snapshot = prepareUpsertReadSnapshot(
                    prepared.snapshot, prepared.produces_part ? temporary->part : nullptr, retired_sources, lock);
                const auto available = disk->getUnreservedSpace();
                if (!available)
                    throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Cannot determine UpsertMergeTree rewrite-write disk space");
                upsert_store->checkWriteSpace(prepared, *available, state_allowance);
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
                    published_payload |= prepared.produces_part;
                    if (!saturated_hint.empty())
                        upsert_saturated_parts.insert(std::move(saturated_hint));
                    for (const auto & source : retired_sources)
                        upsert_saturated_parts.erase(source->name);
                    chassert(upsert_saturated_parts.size() <= getActivePartsCount());
                    notifyUpsertMaintenance();
                    did_work = true;
                }
                catch (...)
                {
                    if (persist_started)
                        upsert_fenced = true;
                    throw;
                }
            }
            batch_rows.clear();
            batch_expired.clear();
            batch_lineage_bytes = Upsert::PART_ROWS_ENVELOPE_BYTES;
            for (auto & offsets : batch_offsets)
                offsets.clear();
            return true;
        };
        for (size_t source_index = 0; source_index < source_offsets.size(); ++source_index)
        {
            for (const auto source_offset : source_offsets[source_index])
            {
                const auto & original = lineage[source_index][source_offset];
                const auto encoded_size = original.key.size() + Upsert::encodeIndexValue(original.value).size() + 8;
                const auto batch_size = batch_rows.size() + batch_expired.size();
                const auto estimated = Upsert::estimateInsertStateBytes(batch_lineage_bytes + encoded_size, batch_size + 1, partition.size(), 1024);
                if (batch_size && (batch_size == Upsert::MAX_UNIT_ROWS || estimated > Upsert::MAX_PENDING_STATE_BYTES / 4))
                    if (!commit_batch(false, false))
                        return false;
                batch_offsets[source_index].push_back(source_offset);
                const auto moved = output_by_key.find(original.key);
                if (moved != output_by_key.end() && moved->second->value.row == original.value.row)
                    batch_rows.push_back(*moved->second);
                else
                {
                    const auto expiration = expired_by_key.find(original.key);
                    if (expiration == expired_by_key.end() || expiration->second->value != original.value)
                        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree rewrite changed or omitted selected source lineage");
                    batch_expired.push_back(original);
                }
                batch_lineage_bytes += encoded_size;
            }
        }
        if (!commit_batch(true, false))
            return false;
        if (retire_sources)
        {
            const auto retirement_snapshot = getUpsertReadSnapshotForQuery()->visibility;
            for (const auto & source : source_authorizations)
            {
                const auto mask = retirement_snapshot->getMask(Upsert::objectName(source.part));
                if (!mask || mask->cardinality() != source.rows)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree final relocation left a visible source row");
            }
            /// Source masks cannot shrink while the operation mutex excludes rewrites into these parts.
            /// The full winner assertion scan holds no parts lock; inserts continue during validation.
            upsert_store->validateRetirement(partition, source_names, batch_input);
        }
        if (retire_sources && !commit_batch(false, true))
            return false;
        if (published_payload)
            prewarmUpsertPartCaches(*temporary);
        output = metadata->getSampleBlock().cloneEmpty();
        output_columns = output.mutateColumns();
        ordinals = ColumnUInt64::create();
        row_lineage = ColumnString::create();
        expired.clear();
        lineage_bytes = Upsert::PART_ROWS_ENVELOPE_BYTES;
        unit_rows = 0;
        for (auto & offsets : source_offsets)
            offsets.clear();
        return true;
    };
    for (size_t source_index = 0; source_index < sources.size(); ++source_index)
    {
        if (yield_to_foreground())
            return did_work;
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
            if (yield_to_foreground())
                return did_work;
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
                size_t upper = std::min({max_batch_rows, visible_offsets.size() - begin, physical_rows_limit - unit_rows});
                while (lower < upper)
                {
                    const auto count = lower + (upper - lower + 1) / 2;
                    const auto candidate_lineage_bytes = lineage_prefix[begin + count] - lineage_prefix[begin];
                    if (candidate_lineage_bytes <= physical_lineage_limit - lineage_bytes)
                        lower = count;
                    else
                        upper = count - 1;
                }
                size_t end = begin + lower;
                if (end == begin)
                {
                    if (!unit_rows)
                        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree rewrite row exceeds its physical lineage budget");
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
    Upsert::ForegroundOperationWait foreground_wait(upsert_foreground_waiters);
    std::lock_guard operation_lock(upsert_operation_mutex);
    foreground_wait.acquired();
    /// Keep new inserts excluded until the old state's compaction debt has been drained.
    auto schema_lock = tryLockTimed(upsert_schema_lock, RWLockImpl::Write,
        query_context->getCurrentQueryId(), query_context->getSettingsRef()[Setting::lock_acquire_timeout]);
    {
        UpsertReadSnapshotPtr retired_snapshot;
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
        const auto available = getDisks().front()->getUnreservedSpace();
        if (!available)
            throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Cannot determine UpsertMergeTree truncate-write disk space");
        upsert_store->checkWriteSpace(prepared, *available, 0, Upsert::REMOVAL_DISK_SPACE_FLOOR);
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
    try
    {
        /// Release local pre-truncate snapshots and the parts lock before the blocking compaction.
        upsert_store->finishTruncate();
    }
    catch (...)
    {
        /// The logical truncation is durable; recovery must finish cleanup before accepting writes.
        auto lock = lockParts();
        upsert_fenced = true;
        throw;
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
    Upsert::ForegroundOperationWait foreground_wait(upsert_foreground_waiters);
    std::lock_guard operation_lock(upsert_operation_mutex);
    foreground_wait.acquired();
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
