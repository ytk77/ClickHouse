#include <Storages/MergeTree/UniqueKey/UpsertSink.h>
#include <Storages/MergeTree/UniqueKey/UpsertInsertQueue.h>
#include <Storages/MergeTree/UniqueKey/UpsertMetrics.h>
#include <Storages/MergeTree/UniqueKey/UpsertStateStore.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InsertDeduplication.h>
#include <Interpreters/ProcessList.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/FailPoint.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/setThreadName.h>
#include <deque>
#include <filesystem>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
extern const Metric LocalThreadScheduled;
}

namespace DB::Setting
{
extern const SettingsSeconds lock_acquire_timeout;
extern const SettingsBool wait_for_part_commit_in_dependent_materialized_views;
extern const SettingsUInt64 max_partitions_per_insert_block;
extern const SettingsUInt64 unique_key_max_encoded_size;
extern const SettingsString insert_deduplication_token;
extern const SettingsUInt64 input_format_max_block_wait_ms;
extern const SettingsBool insert_deduplicate;
extern const SettingsMaxThreads max_insert_threads;
extern const SettingsMaxThreads max_threads;
extern const SettingsUInt64 max_threads_min_free_memory_per_thread;
extern const SettingsUInt64 max_insert_threads_min_free_memory_per_thread;
}
namespace DB::ErrorCodes
{
extern const int SUPPORT_IS_DISABLED;
extern const int FAULT_INJECTED;
extern const int LOGICAL_ERROR;
extern const int LIMIT_EXCEEDED;
}
namespace DB::MergeTreeSetting
{
extern const MergeTreeSettingsUInt64 upsert_insert_target_bytes;
}
namespace DB::FailPoints
{
extern const char upsert_throw_during_cache_prewarm[];
extern const char upsert_throw_after_rename[];
extern const char upsert_throw_before_publish[];
extern const char upsert_throw_during_part_precommit[];
extern const char upsert_throw_before_part_persist[];
extern const char upsert_pause_first_preparation[];
extern const char upsert_pause_ready_preparation[];
extern const char upsert_throw_during_preparation[];
}

namespace DB::Upsert
{
struct PreparedInsert
{
    MergeTreeTemporaryPartPtr temporary;
    String partition;
    PartCommit authorization;
    ValidatedPartRows rows;
};

struct Sink::Impl
{
    /// Both ready parts and running preparations count against these bounds.
    /// One oversized preparation may run alone; query memory limits still apply.
    static constexpr size_t max_preparing_parts = 8;
    struct Pending
    {
        size_t bytes;
        std::future<std::unique_ptr<PreparedInsert>> future;
    };

    explicit Impl(size_t max_threads_) : max_threads(max_threads_)
    {
        if (max_threads > 1)
            pool = std::make_unique<ThreadPool>(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive,
                CurrentMetrics::LocalThreadScheduled, max_threads, max_threads, max_threads);
    }

    ~Impl()
    {
        discard();
    }

    void discard()
    {
        /// Workers only prepare temporary payloads. Join before releasing the storage's lifetime guards.
        for (auto & task : pending)
            if (task.future.valid())
                task.future.wait();
        pending.clear();
        pending_bytes = 0;
    }

    const size_t max_threads;
    std::unique_ptr<ThreadPool> pool;
    std::deque<Pending> pending;
    size_t pending_bytes = 0;
};

Sink::Sink(StorageMergeTree & storage_, StorageMetadataPtr metadata_, ContextPtr context_)
    : SinkToStorage(std::make_shared<const Block>(metadata_->getSampleBlock())), storage(storage_), metadata(std::move(metadata_)),
      context(std::move(context_)), schema_lock(storage.tryLockTimed(storage.upsert_schema_lock,
          RWLockImpl::Read, context->getCurrentQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout])),
      input_target_bytes((*storage.getSettings())[MergeTreeSetting::upsert_insert_target_bytes])
{
    const auto current_metadata = storage.getInMemoryMetadataPtr(context, true);
    if (metadata->getColumns().toString(false) != current_metadata->getColumns().toString(false)
        || metadata->getSecondaryIndices().allToString() != current_metadata->getSecondaryIndices().allToString()
        || metadata->getProjections().toString() != current_metadata->getProjections().toString())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree schema changed before insert; retry the query");
    if (context->getSettingsRef()[Setting::input_format_max_block_wait_ms])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree does not support streaming inserts");
}

Sink::~Sink() = default;

void Sink::setHasDependentMaterializedViews(bool has_views)
{
    has_dependent_views = has_views;
    synchronously_commit_for_views = has_views
        && context->getSettingsRef()[Setting::wait_for_part_commit_in_dependent_materialized_views];
}

void Sink::onStart()
{
    const auto & settings = context->getSettingsRef();
    const auto query_threads = getMaxThreadsForAvailableMemory(
        std::max<size_t>(1, settings[Setting::max_threads]), settings[Setting::max_threads_min_free_memory_per_thread]);
    const auto threads = getMaxThreadsForAvailableMemory(
        std::min({std::max<size_t>(1, settings[Setting::max_insert_threads]), query_threads, Impl::max_preparing_parts}),
        settings[Setting::max_insert_threads_min_free_memory_per_thread]);
    impl = std::make_unique<Impl>(threads);
}

void Sink::finishFront()
{
    auto prepared = impl->pending.front().future.get();
    impl->pending_bytes -= impl->pending.front().bytes;
    impl->pending.pop_front();
    if (isCancelled())
        return;
    if (auto process = context->getProcessListElement())
        process->checkTimeLimit();
    if (prepared)
        storage.commitUpsertBlock(*prepared);
}

void Sink::onFinish()
{
    while (!impl->pending.empty() && !isCancelled())
        finishFront();
    impl->discard();
}

void Sink::onException(std::exception_ptr)
{
    if (impl)
        impl->discard();
}

String Sink::getName() const
{
    return "UpsertMergeTreeSink";
}

void Sink::consume(Chunk & chunk)
{
    if (!chunk.getNumRows())
        return;
    String client_token = context->getSettingsRef()[Setting::insert_deduplication_token];
    if (client_token.size() > 1024)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree deduplication token exceeds 1024 bytes");
    const auto info = chunk.getChunkInfos().get<DeduplicationInfo>();
    const bool view_input = info && info->getVisitedViews().size() > 1;
    if (view_input)
    {
        /// View contexts disable ordinary insert deduplication; their chunk carries the actual
        /// dependency policy and a stable source/view/block identity across filtered retries.
        if (info->isDisabled())
            client_token.clear();
        else if (!client_token.empty())
        {
            SipHash hash;
            for (const auto & token : info->getDeduplicationHashes("", true))
                hash.update(token.hash);
            client_token = "upsert_view_v1:" + getSipHash128AsHexString(hash.get128());
            /// Earlier source chunks can disappear on retry. Number units inside this identified
            /// chunk, rather than counting the chunks that happened to reach this sink.
            unit = 0;
        }
    }
    else if (!context->getSettingsRef()[Setting::insert_deduplicate])
        client_token.clear();
    auto input = getHeader().cloneWithColumns(chunk.getColumns());
    for (auto & column : input)
        column.column = recursiveRemoveSparse(column.column->convertToFullIfWrapped());
    IColumn::Filter forwarded;
    if (has_dependent_views && !client_token.empty())
        forwarded.resize_fill(input.rows(), 0);
    size_t offset = 0;
    size_t max_rows = MAX_UNIT_ROWS;
    while (offset < input.rows() && !isCancelled())
    {
        if (auto process = context->getProcessListElement())
            process->checkTimeLimit();
        CurrentMemoryTracker::check();
        auto block = cutInputBlock(input, offset, max_rows, input_target_bytes);
        const size_t rows = block.rows();
        if (!consumeBlock(std::move(block), offset, client_token, forwarded.empty() ? nullptr : &forwarded))
        {
            /// A wide encoded key can exhaust the independent lineage budget even when the columns fit.
            if (rows == 1)
                throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                    "UpsertMergeTree cannot split input row {}: its row metadata exceeds {} bytes", offset, MAX_RECORD_BYTES);
            max_rows = rows / 2;
            continue;
        }
        offset += rows;
        max_rows = MAX_UNIT_ROWS;
    }
    if (synchronously_commit_for_views)
        while (!impl->pending.empty() && !isCancelled())
            finishFront();
    if (!forwarded.empty())
    {
        const size_t kept = std::count(forwarded.begin(), forwarded.end(), UInt8{1});
        if (!kept)
            chunk.clear();
        else if (kept != input.rows())
        {
            if (auto source_info = chunk.getChunkInfos().extract<DeduplicationInfo>())
                chunk.getChunkInfos().add(source_info->filterSourceRows(forwarded));
            Columns columns;
            for (const auto & column : input)
                columns.push_back(column.column->filter(forwarded, kept));
            chunk.setColumns(std::move(columns), kept);
        }
    }
}

bool Sink::consumeBlock(Block block, size_t first_row, const String & client_token, IColumn::Filter * forwarded)
{
    auto ordinals = ColumnUInt64::create();
    for (size_t row = 0; row < block.rows(); ++row)
        ordinals->insertValue(first_row + row);
    const auto & settings = context->getSettingsRef();
    const size_t input_rows = block.rows();
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
    /// Validate and fold every partition before committing the first one from this bounded input unit.
    std::vector<Token> tokens;
    tokens.reserve(partitions.size());
    bool split_state = false;
    for (auto & partition : partitions)
    {
        /// Only token-based retries compare the evaluated input hash. Payload authentication is independent.
        tokens.push_back({client_token, unit, client_token.empty() ? ObjectID{} : hashInputBlock(*partition.block), 0});
        const auto lineage_bytes = foldBlock(*partition.block, *metadata, storage.merging_params.upsert_version_column,
            settings[Setting::unique_key_max_encoded_size]);
        if (lineage_bytes > MAX_RECORD_BYTES)
            return false;
        const auto partition_id = partition.partition.getID(metadata->getPartitionKey().sample_block);
        split_state |= estimateInsertStateBytes(lineage_bytes, partition.block->rows(), partition_id.size(), client_token.size()) > MAX_PENDING_STATE_BYTES;
    }
    if (split_state)
    {
        bool preserve_committed_boundary = false;
        if (!client_token.empty())
        {
            /// One legacy partition may have committed before a failure in another partition of this unit.
            /// Its boundary also fixes the token numbering of subsequent units, even if it is small itself.
            for (size_t i = 0; i < partitions.size(); ++i)
            {
                const auto partition_id = partitions[i].partition.getID(metadata->getPartitionKey().sample_block);
                if (storage.isUpsertTokenCommitted(partition_id, tokens[i]))
                {
                    preserve_committed_boundary = true;
                    break;
                }
            }
        }
        if (!preserve_committed_boundary)
            return false;
    }
    for (size_t i = 0; i < partitions.size(); ++i)
    {
        if (isCancelled())
            return true;
        if (auto process = context->getProcessListElement())
            process->checkTimeLimit();
        if (!impl->pool || forwarded)
        {
            auto prepared = storage.prepareUpsertBlock(partitions[i], metadata, context, std::move(tokens[i]));
            if (prepared && !isCancelled())
            {
                if (auto process = context->getProcessListElement())
                    process->checkTimeLimit();
                const bool committed = storage.commitUpsertBlock(*prepared);
                if (forwarded && committed)
                    for (size_t row = 0; row < input_rows; ++row)
                        if (selector.empty() || selector[row] == i)
                            (*forwarded)[first_row + row] = 1;
            }
            continue;
        }
        CurrentMemoryTracker::check();
        const size_t bytes = partitions[i].block->bytes();
        while (insertPreparationNeedsDrain(impl->pending.size(), impl->pending_bytes, bytes, impl->max_threads))
            finishFront();
        if (isCancelled())
            return true;
        /// Allocate the queue entry before scheduling so every worker has a tracked future, including on exceptions.
        impl->pending.push_back({bytes, {}});
        impl->pending_bytes += bytes;
        impl->pending.back().future = scheduleFromThreadPoolUnsafe<std::unique_ptr<PreparedInsert>>(
            [target = &storage, snapshot = metadata, query_context = context,
                partition = std::move(partitions[i]), token = std::move(tokens[i])]() mutable
            {
                return target->prepareUpsertBlock(partition, snapshot, query_context, std::move(token));
            }, *impl->pool, ThreadName::DEFAULT_THREAD_POOL);
    }
    ++unit;
    return true;
}
}

namespace DB
{
bool StorageMergeTree::isUpsertTokenCommitted(const String & partition, const Upsert::Token & token) const
{
    auto lock = readLockParts();
    if (upsert_fenced)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree is fenced pending restart and recovery");
    /// A smaller unit may already use this number under the current splitting policy.
    /// Only an exact match permits replaying an older, larger boundary unchanged.
    return upsert_store->tokenAlreadyCommitted(partition, token, false);
}

std::unique_ptr<Upsert::PreparedInsert> StorageMergeTree::prepareUpsertBlock(
    BlockWithPartition & block, const StorageMetadataPtr & metadata, ContextPtr query_context, Upsert::Token token)
{
    const String partition_id = block.partition.getID(metadata->getPartitionKey().sample_block);
    {
        auto lock = lockParts();
        if (upsert_fenced)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree is fenced pending restart and recovery");
        if (upsert_store->tokenAlreadyCommitted(partition_id, token))
            return nullptr;
        upsert_store->checkAdmission();
    }
    if (token.unit == 0)
        FailPointInjection::pauseFailPoint(FailPoints::upsert_pause_first_preparation);
    fiu_do_on(FailPoints::upsert_throw_during_preparation,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree part preparation failure");
    });
    auto temporary = writer.writeTempPart(block, metadata, query_context, false);
    temporary->finalize();
    auto & part = temporary->part;
    if (!part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree writer returned an empty part");
    part->getDataPartStorage().commitTransaction();
    const auto path = part->getDataPartStorage().getFullPath();
    auto rows = Upsert::decodePartRows(Upsert::readBoundedFile(std::filesystem::path(path) / Upsert::ROWS_FILE, Upsert::MAX_RECORD_BYTES));
    Upsert::PartCommit authorization;
    authorization.part = {UUIDHelpers::getHighBytes(part->uuid), UUIDHelpers::getLowBytes(part->uuid)};
    authorization.temporary_name = part->getDataPartStorage().getPartDirectory();
    authorization.files_hash = Upsert::hashPartDirectory(path, true, &authorization.files_inventory);
    authorization.rows = part->rows_count;
    authorization.token = std::move(token);
    Upsert::syncDirectory(std::filesystem::path(path) / "..");
    Upsert::ValidatedPartRows validated(std::move(rows), authorization.part);
    CurrentMemoryTracker::check();
    if (authorization.token.unit != 0)
        FailPointInjection::pauseFailPoint(FailPoints::upsert_pause_ready_preparation);
    return std::make_unique<Upsert::PreparedInsert>(
        std::move(temporary), partition_id, std::move(authorization), std::move(validated));
}

void StorageMergeTree::prewarmUpsertPartCaches(MergeTreeTemporaryPart & temporary) noexcept
{
    try
    {
        const auto caches = getCachesToPrewarm(temporary.part->getBytesUncompressedOnDisk());
        if (!caches.mark_cache && !caches.index_mark_cache && !caches.primary_index_cache)
            return;
        fiu_do_on(FailPoints::upsert_throw_during_cache_prewarm,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree cache prewarm failure");
        });
        temporary.prewarmCaches();
        ProfileEvents::increment(ProfileEvents::UpsertPartCachePrewarms);
    }
    catch (...)
    {
        /// Cache filling is optional; a published durable commit must keep its successful outcome.
        ProfileEvents::increment(ProfileEvents::UpsertPartCachePrewarmFailures);
        tryLogCurrentException(log, "Could not prewarm committed UpsertMergeTree part caches");
    }
}

bool StorageMergeTree::commitUpsertBlock(Upsert::PreparedInsert & input)
{
    auto & part = input.temporary->part;
    auto & authorization = input.authorization;
    UpsertReadSnapshotPtr retired_snapshot;
    bool produces_part = false;
    {
        Stopwatch lock_wait;
        auto lock = lockParts();
        ProfileEvents::increment(ProfileEvents::UpsertCommitLockWaitMicroseconds, lock_wait.elapsedMicroseconds());
        ProfileEventTimeIncrement<Microseconds> hold_timer(ProfileEvents::UpsertCommitLockHoldMicroseconds);
        if (upsert_fenced)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree is fenced pending restart and recovery");
        auto block_holder = fillNewPartName(part, lock);
        authorization.final_name = part->name;
        Upsert::StateStore::Prepared prepared;
        try
        {
            prepared = upsert_store->prepare(input.partition, std::move(authorization), input.rows);
        }
        catch (...)
        {
            if (upsert_store->isFenced())
                upsert_fenced = true;
            throw;
        }
        if (prepared.replayed_token)
            return false;
        produces_part = prepared.produces_part;
        /// Allocate the complete next read version before any ambiguous durable operation.
        auto read_snapshot = prepareUpsertReadSnapshot(prepared.snapshot, prepared.produces_part ? part : nullptr, {}, lock);
        bool persist_started = false;
        auto persist = [&]
        {
            persist_started = true;
            upsert_store->append(prepared);
        };
        try
        {
            if (prepared.produces_part)
                commitUpsertPart(part, persist, lock);
            else
                persist();
            upsert_store->publish(prepared);
            retired_snapshot = publishUpsertReadSnapshot(std::move(read_snapshot));
            notifyUpsertMaintenance();
        }
        catch (...)
        {
            if (persist_started)
                upsert_fenced = true;
            throw;
        }
    }
    if (produces_part)
        prewarmUpsertPartCaches(*input.temporary);
    return true;
}

void MergeTreeData::commitUpsertPart(MutableDataPartPtr & part, const std::function<void()> & persist, DataPartsLock & lock)
{
    commitUpsertParts({part}, persist, lock);
}

void MergeTreeData::commitUpsertParts(const MutableDataPartsVector & parts, const std::function<void()> & persist, DataPartsLock & lock)
{
    Transaction transaction(*this, nullptr);
    bool persist_started = false;
    size_t precommitted = 0;
    try
    {
        /// Allocate index nodes, transaction bookkeeping and column counters before authorization.
        for (auto part : parts)
        {
            if (precommitted)
                fiu_do_on(FailPoints::upsert_throw_during_part_precommit,
                {
                    throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree precommit failure");
                });
            if (!renameTempPartAndAdd(part, transaction, lock, true))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree part could not be prepared");
            part->assertHasVersionMetadata(nullptr);
            addPartContributionToColumnAndSecondaryIndexSizes(part);
            ++precommitted;
        }
        fiu_do_on(FailPoints::upsert_throw_before_part_persist,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree failure before persistence");
        });
        persist_started = true;
        persist();
        transaction.renameParts();
        Upsert::syncDirectory(std::filesystem::path(getDisks().front()->getPath()) / getRelativeDataPath());
        fiu_do_on(FailPoints::upsert_throw_after_rename,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree failure after durable rename");
        });
        fiu_do_on(FailPoints::upsert_throw_before_publish,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree failure before publication");
        });
        /// The caller publishes the preallocated read version after all native part changes complete.
        for (const auto & part : parts)
        {
            modifyPartState(part, DataPartState::Active, lock);
            increaseDataVolume(static_cast<ssize_t>(part->getBytesOnDisk()), static_cast<ssize_t>(part->rows_count), 1);
        }
        transaction.clear();
    }
    catch (...)
    {
        if (!persist_started)
        {
            try
            {
                transaction.rollback(&lock);
                /// Registration can fail before transaction bookkeeping takes ownership.
                /// Incomplete rollback must still require recovery, even without a durable write.
                for (const auto & part : parts)
                    if (part->getState() == DataPartState::PreActive)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incomplete UpsertMergeTree precommit rollback for {}", part->name);
                /// Discard possibly partial accounting; lazy calculation uses only active parts.
                std::unique_lock sizes_lock(columns_and_secondary_indices_sizes_mutex);
                are_columns_and_secondary_indices_sizes_calculated = false;
                primary_index_size = {};
            }
            catch (...)
            {
                transaction.clear();
                upsert_fenced = true;
                throw;
            }
            throw;
        }
        /// Neither rollback nor temporary-part destruction may remove ambiguously committed payloads.
        transaction.clear();
        for (const auto & part : parts)
            part->is_temp = false;
        upsert_fenced = true;
        throw;
    }
}
}
