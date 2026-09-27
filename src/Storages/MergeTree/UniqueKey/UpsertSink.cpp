#include <Storages/MergeTree/UniqueKey/UpsertSink.h>
#include <Storages/MergeTree/UniqueKey/UpsertStateStore.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Common/Exception.h>
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
extern const SettingsUInt64 max_partitions_per_insert_block;
extern const SettingsUInt64 unique_key_max_encoded_size;
extern const SettingsString insert_deduplication_token;
extern const SettingsUInt64 input_format_max_block_wait_ms;
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
namespace DB::FailPoints
{
extern const char upsert_throw_after_rename[];
extern const char upsert_throw_before_publish[];
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
    static constexpr size_t max_pending_bytes = 128U << 20;
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
    : SinkToStorage(std::make_shared<const Block>(metadata_->getSampleBlock())), storage(storage_), metadata(std::move(metadata_)), context(std::move(context_))
{
    if (context->getSettingsRef()[Setting::input_format_max_block_wait_ms])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree does not support streaming inserts");
}

Sink::~Sink() = default;

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
    {
        if (isCancelled())
            return;
        if (auto process = context->getProcessListElement())
            process->checkTimeLimit();
        if (!impl->pool)
        {
            auto prepared = storage.prepareUpsertBlock(partitions[i], metadata, context, std::move(tokens[i]));
            if (prepared && !isCancelled())
            {
                if (auto process = context->getProcessListElement())
                    process->checkTimeLimit();
                storage.commitUpsertBlock(*prepared);
            }
            continue;
        }
        CurrentMemoryTracker::check();
        const size_t bytes = partitions[i].block->bytes();
        while (!impl->pending.empty() && (impl->pending.size() >= impl->max_threads
            || bytes > Impl::max_pending_bytes - impl->pending_bytes))
            finishFront();
        if (isCancelled())
            return;
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
}
}

namespace DB
{
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
    authorization.files_hash = Upsert::hashPartDirectory(path, true);
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

void StorageMergeTree::commitUpsertBlock(Upsert::PreparedInsert & input)
{
    auto & part = input.temporary->part;
    auto & authorization = input.authorization;
    auto lock = lockParts();
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
        return;
    /// Allocate the complete next read version before any ambiguous durable operation.
    auto read_snapshot = prepareUpsertReadSnapshot(prepared.snapshot, prepared.produces_part ? part : nullptr, {}, lock);
    try
    {
        if (prepared.produces_part)
            commitUpsertPart(part, [&]()
            {
                upsert_store->append(prepared);
            }, lock);
        else
            upsert_store->append(prepared);
        upsert_store->publish(prepared);
        publishUpsertReadSnapshot(std::move(read_snapshot));
    }
    catch (...)
    {
        upsert_fenced = true;
        throw;
    }
}

void MergeTreeData::commitUpsertPart(MutableDataPartPtr & part, const std::function<void()> & persist, DataPartsLock & lock)
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
        /// The caller publishes the preallocated read version after all native part changes complete.
        modifyPartState(part, DataPartState::Active, lock);
        increaseDataVolume(static_cast<ssize_t>(part->getBytesOnDisk()), static_cast<ssize_t>(part->rows_count), 1);
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
