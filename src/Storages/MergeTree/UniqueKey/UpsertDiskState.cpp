#include <Storages/MergeTree/UniqueKey/UpsertDiskState.h>
#include <Storages/MergeTree/UniqueKey/UpsertMetrics.h>
#include <Storages/MergeTree/UniqueKey/UpsertMaskVersions.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmapCache.h>
#include <Storages/MergeTree/UniqueKey/UpsertBinary.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/threadPoolCallbackRunner.h>
#include <deque>
#include "config.h"
#include <filesystem>
#include <limits>
#include <utility>
#include <chrono>

#if USE_ROCKSDB
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>
#endif

namespace CurrentMetrics
{
extern const Metric UpsertMaskCacheBytes;
extern const Metric UpsertMaskCacheEntries;
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
extern const Metric LocalThreadScheduled;
}

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ROCKSDB_ERROR;
extern const int LOGICAL_ERROR;
extern const int SUPPORT_IS_DISABLED;
extern const int FAULT_INJECTED;
}

namespace DB::FailPoints
{
extern const char upsert_log_throw_after_sync[];
extern const char upsert_log_throw_after_append[];
extern const char upsert_pause_before_state_write[];
extern const char upsert_pause_after_state_sync[];
}

namespace DB::Upsert
{
void Resources::validate() const
{
    const auto validate_range = [](const char * name, UInt64 value, UInt64 minimum, UInt64 maximum)
    {
        if (value < minimum || value > maximum)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid {} = {} for UpsertMergeTree; expected {}..{}",
                name, value, minimum, maximum);
    };
    validate_range("upsert_index_cache_bytes", index_cache_bytes, 1ULL << 20, std::numeric_limits<size_t>::max());
    validate_range("upsert_write_buffer_bytes", write_buffer_bytes, 1ULL << 20, std::numeric_limits<size_t>::max());
    validate_range("upsert_mask_cache_bytes", mask_cache_bytes, 0, std::numeric_limits<size_t>::max());
    validate_range("upsert_write_buffer_count", write_buffer_count, 2, 64);
    validate_range("upsert_wal_bytes", wal_bytes, write_buffer_bytes, std::numeric_limits<UInt64>::max());
    validate_range("upsert_background_jobs", background_jobs, 1, 128);
    validate_range("upsert_max_open_files", max_open_files, 20, std::numeric_limits<int>::max());
    validate_range("upsert_recovery_threads", recovery_threads, 1, 64);
}

#if USE_ROCKSDB
namespace
{
constexpr UInt64 retry_window_seconds = 86400;
constexpr UInt64 retry_state_budget = 1ULL << 30;
constexpr UInt64 maintenance_disk_reserve = 1ULL << 30;

void checkBatchBudget(const rocksdb::WriteBatch & batch)
{
    if (batch.GetDataSize() > MAX_PENDING_STATE_BYTES)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree pending state batch exceeds its byte budget");
}

void check(const rocksdb::Status & status)
{
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "UpsertMergeTree state database: {}", status.ToString());
}

String number(UInt64 value)
{
    BinaryWriter out;
    out.number(value);
    return std::move(out.bytes);
}

String prefix(char space, const String & partition)
{
    BinaryWriter out;
    out.number(static_cast<UInt8>(space), 1);
    out.string(partition);
    return std::move(out.bytes);
}

String tokenKey(const String & partition, const Token & token)
{
    BinaryWriter out;
    out.bytes = prefix('T', partition);
    out.string(token.client_token);
    out.number(token.unit);
    return std::move(out.bytes);
}

String expiryKey(UInt64 expiry, const String & token_key)
{
    String key("E");
    for (int shift = 56; shift >= 0; shift -= 8)
        key.push_back(static_cast<char>(expiry >> shift));
    return key + token_key;
}

UInt64 tokenExpiry(const String & value)
{
    BinaryReader in{value};
    in.id();
    in.number();
    auto result = in.number();
    in.end();
    return result;
}

UInt64 tokenCost(const String & key)
{
    return key.size() * 2 + 128;
}

String encodeMask(const DeleteBitmap & mask)
{
    WriteBufferFromOwnString out;
    mask.serialize(out);
    ProfileEvents::increment(ProfileEvents::UpsertMaskWriteBytes, out.count());
    return out.str();
}

struct Database
{
    std::unique_ptr<rocksdb::DB> db;
    std::unique_ptr<DeleteBitmapCache> mask_cache;
    MaskVersions mask_versions;
    UInt64 mask_generation = 0;

    std::optional<String> get(const String & key, const rocksdb::Snapshot * snapshot = nullptr) const
    {
        rocksdb::ReadOptions options;
        options.snapshot = snapshot;
        String value;
        auto status = db->Get(options, key, &value);
        if (status.IsNotFound())
            return std::nullopt;
        check(status);
        return value;
    }

    /// Bound temporary lookup memory independently of the input unit size.
    template <typename Callback>
    void forEachWinner(const String & partition, const PartRows & rows, Callback && callback, const std::vector<UInt32> * offsets = nullptr) const
    {
        constexpr size_t lookup_batch_size = 1024;
        const auto key_prefix = prefix('W', partition);
        const size_t size = offsets ? offsets->size() : rows.size();
        auto row_at = [&](size_t position) -> const PartRow &
        {
            const auto offset = offsets ? (*offsets)[position] : position;
            if (offset >= rows.size())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree selected source offset exceeds its lineage");
            return rows[offset];
        };
        for (size_t begin = 0; begin < size; begin += lookup_batch_size)
        {
            const auto count = std::min(lookup_batch_size, size - begin);
            std::vector<String> keys;
            keys.reserve(count);
            for (size_t i = 0; i < count; ++i)
                keys.push_back(key_prefix + row_at(begin + i).key);
            std::vector<rocksdb::Slice> slices;
            slices.reserve(count);
            for (const auto & key : keys)
                slices.emplace_back(key);
            std::vector<rocksdb::PinnableSlice> values(count);
            std::vector<rocksdb::Status> statuses(count);
            {
                ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertWinnerLookupMicroseconds);
                ProfileEvents::increment(ProfileEvents::UpsertWinnerLookupKeys, count);
                db->MultiGet(rocksdb::ReadOptions{}, db->DefaultColumnFamily(), count, slices.data(), values.data(), statuses.data());
            }
            for (size_t i = 0; i < count; ++i)
            {
                std::optional<std::string_view> value;
                if (!statuses[i].IsNotFound())
                {
                    check(statuses[i]);
                    value = std::string_view(values[i].data(), values[i].size());
                }
                callback(row_at(begin + i), keys[i], value);
            }
            CurrentMemoryTracker::check();
        }
    }

    UInt64 integer(const String & key) const
    {
        auto value = get(key);
        if (!value)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree state is missing a required counter");
        BinaryReader in{*value};
        auto result = in.number();
        in.end();
        return result;
    }
};

class DiskMaskSnapshot final : public MaskSnapshot
{
public:
    explicit DiskMaskSnapshot(std::shared_ptr<Database> database_) : database(std::move(database_))
    {
    }
    ~DiskMaskSnapshot() override
    {
        if (cut)
            database->db->ReleaseSnapshot(cut);
    }
    void bind()
    {
        if (cut)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree disk snapshot was bound twice");
        versions = database->mask_versions;
        cut = database->db->GetSnapshot();
        if (!cut)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree could not pin its disk snapshot");
    }
    UInt64 getPartitionCSN(const String & partition) const override
    {
        if (!cut)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree disk snapshot was read before commit");
        const auto value = database->get(prefix('C', partition), cut);
        if (!value)
            return 0;
        BinaryReader in{*value};
        const auto csn = in.number();
        in.end();
        return csn;
    }
    std::shared_ptr<const DeleteBitmap> getMask(const String & part) const override
    {
        if (!cut)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree disk snapshot was read before commit");
        const auto * version = versions.find(part);
        if (!version)
            return nullptr;
        auto load = [&]() -> std::shared_ptr<DeleteBitmap>
        {
            ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertMaskReadMicroseconds);
            auto value = database->get("M" + part, cut);
            if (!value)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree versioned mask is missing");
            ProfileEvents::increment(ProfileEvents::UpsertMaskReadBytes, value->size());
            ReadBufferFromString in(*value);
            return DeleteBitmap::deserialize(in);
        };
        if (!database->mask_cache)
            return load();
        auto [mask, loaded] = database->mask_cache->getOrSet(DeleteBitmapCache::makeKey(part, *version), load);
        ProfileEvents::increment(loaded ? ProfileEvents::UpsertMaskCacheMisses : ProfileEvents::UpsertMaskCacheHits);
        return mask;
    }

private:
    std::shared_ptr<Database> database;
    const rocksdb::Snapshot * cut = nullptr;
    MaskVersions versions;
};
}

namespace
{
struct MaskChange
{
    enum class Kind
    {
        Put,
        Delete,
        Clear
    };
    Kind kind;
    String part;
#ifndef NDEBUG
    bool operator==(const MaskChange &) const = default;
#endif
};

#ifndef NDEBUG
class MaskVersionUpdates final : public rocksdb::WriteBatch::Handler
{
public:
    std::vector<MaskChange> changes;
    void Put(const rocksdb::Slice & key, const rocksdb::Slice &) override
    {
        if (key.starts_with("M"))
            changes.push_back({MaskChange::Kind::Put, key.ToString().substr(1)});
    }
    void Delete(const rocksdb::Slice & key) override
    {
        if (key.starts_with("M"))
            changes.push_back({MaskChange::Kind::Delete, key.ToString().substr(1)});
    }
    rocksdb::Status DeleteRangeCF(uint32_t column_family, const rocksdb::Slice & begin, const rocksdb::Slice & end) override
    {
        if (column_family != 0)
            return rocksdb::Status::InvalidArgument("Unexpected UpsertMergeTree column family");
        if (begin.ToString() == "M" && end.ToString() == "N")
            changes.push_back({MaskChange::Kind::Clear, {}});
        else if (begin.ToString() < "N" && end.ToString() > "M")
            return rocksdb::Status::InvalidArgument("Unsupported UpsertMergeTree mask range deletion");
        return rocksdb::Status::OK();
    }
};
#endif
}

struct DiskPrepared
{
    rocksdb::WriteBatch batch;
    std::vector<MaskChange> mask_changes;
    std::shared_ptr<DiskMaskSnapshot> masks;
    UInt64 previous_csn = 0;
    bool durable = false;
    bool catalog_change = false;
    UInt64 previous_part_count = 0;

    void putMask(const String & part, const DeleteBitmap & mask)
    {
        check(batch.Put("M" + part, encodeMask(mask)));
        mask_changes.push_back({MaskChange::Kind::Put, part});
    }
    void deleteMask(const String & part)
    {
        check(batch.Delete("M" + part));
        mask_changes.push_back({MaskChange::Kind::Delete, part});
    }
    void clearMasks()
    {
        check(batch.DeleteRange("M", "N"));
        mask_changes.push_back({MaskChange::Kind::Clear, {}});
    }
};

struct DiskState::Impl
{
    std::shared_ptr<Database> database = std::make_shared<Database>();
    SnapshotPtr current;
    String table_path;
    String retirement_cursor = "R";
    UInt64 part_count = 0;
    StateClock clock;
    Resources resources;

    std::shared_ptr<SnapshotState> newSnapshot(std::shared_ptr<DiskMaskSnapshot> masks)
    {
        auto result = std::make_shared<SnapshotState>();
        result->disk_masks = std::move(masks);
        return result;
    }

    void recover()
    {
        ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertRecoveryMicroseconds);
        namespace fs = std::filesystem;
        part_count = database->integer("part_count");
        rocksdb::ReadOptions options;
        options.fill_cache = false;
        std::unique_ptr<rocksdb::Iterator> iterator(database->db->NewIterator(options));
        UInt64 found = 0;
        /// Bound outstanding payloads, decoded masks and file descriptors independently of the catalog size.
        ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive,
            CurrentMetrics::LocalThreadScheduled, resources.recovery_threads, resources.recovery_threads, resources.recovery_threads);
        std::deque<std::future<void>> pending;
        auto finish_front = [&]
        {
            pending.front().get();
            pending.pop_front();
        };
        auto verify = [this](const PartCommit & record)
        {
            namespace fs = std::filesystem;
            const fs::path final = fs::path(table_path) / record.final_name;
            const fs::path temporary = fs::path(table_path) / record.temporary_name;
            const bool final_exists = fs::exists(final);
            if (final_exists == fs::exists(temporary))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree requires exactly one authorized payload for {}", record.final_name);
            if (hashPartDirectory(final_exists ? final : temporary, false) != record.files_hash)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree authenticated files mismatch for {}", record.final_name);
            const auto encoded_mask = database->get("M" + objectName(record.part));
            if (!encoded_mask)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree authorized part is missing its visibility mask");
            ReadBufferFromString mask_input(*encoded_mask);
            const auto mask = DeleteBitmap::deserialize(mask_input);
            if (mask->rangeCardinality(0, record.rows) != mask->cardinality())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree recovered mask exceeds its part");
            if (!final_exists)
            {
                fs::rename(temporary, final);
                syncDirectory(table_path);
            }
            ProfileEvents::increment(ProfileEvents::UpsertRecoveryParts);
        };
        for (iterator->Seek("A"); iterator->Valid() && iterator->key().starts_with("A"); iterator->Next())
        {
            auto record = decodePartCommit(iterator->value().ToStringView());
            if (!record.rows || iterator->key().ToString() != "A" + record.final_name)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree catalog part identity mismatch");
            database->mask_versions.set(objectName(record.part), 0);
            if (pending.size() == resources.recovery_threads)
                finish_front();
            pending.emplace_back();
            pending.back() = scheduleFromThreadPoolUnsafe<void>(
                [verify, payload = std::move(record)] { verify(payload); }, pool, ThreadName::DEFAULT_THREAD_POOL);
            ++found;
        }
        while (!pending.empty())
            finish_front();
        check(iterator->status());
        if (found != part_count)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree durable part count differs from its catalog");
        bool had_retired_parts = false;
        /// Validate the active recovery root before reclaiming anything it superseded.
        for (iterator->Seek("R"); iterator->Valid() && iterator->key().starts_with("R"); iterator->Next())
        {
            const auto record = decodePartCommit(iterator->value().ToStringView());
            if (!record.rows || iterator->key().ToString() != "R" + record.final_name || database->get("A" + record.final_name))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree retirement authorization");
            /// No pre-restart reader can retain this retired payload. Partial deletion is retryable.
            const auto path = fs::path(table_path) / record.final_name;
            if (fs::is_symlink(path))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Symbolic link replaces a retired UpsertMergeTree part");
            fs::remove_all(path);
            had_retired_parts = true;
        }
        check(iterator->status());
        syncDirectory(table_path);
        for (const auto & entry : fs::directory_iterator(table_path))
        {
            const auto name = entry.path().filename().string();
            if (MergeTreePartInfo::tryParsePartName(name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
                && !database->get("A" + name))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree found unauthorized final part {}", name);
        }
        /// Authorized temporary payloads were rolled forward above. Remaining engine-owned
        /// preparations have no durable decision and no reader can reference them during recovery.
        for (const auto & entry : fs::directory_iterator(table_path))
        {
            if (!entry.path().filename().string().starts_with("tmp_upsert_"))
                continue;
            if (entry.is_symlink())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Symbolic link replaces an UpsertMergeTree preparation");
            fs::remove_all(entry.path());
        }
        syncDirectory(table_path);
        if (had_retired_parts)
        {
            /// Part names may be reused after restarting an empty table. Forget retirement only after durable deletion.
            rocksdb::WriteBatch batch;
            check(batch.DeleteRange("R", "S"));
            rocksdb::WriteOptions write;
            write.sync = true;
            check(database->db->Write(write, &batch));
        }
        /// Consumed detached inputs are reclaimed only after their import is durable.
        bool consumed_detached = false;
        const auto detached_path = fs::path(table_path) / "detached";
        for (iterator->Seek("D"); iterator->Valid() && iterator->key().starts_with("D"); iterator->Next())
        {
            const auto name = iterator->key().ToString().substr(1);
            if (!name.starts_with("attaching_")
                || !MergeTreePartInfo::tryParsePartName(name.substr(10), MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
                || name.find('/') != String::npos || name.find('\\') != String::npos)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree consumed detached part");
            const auto path = detached_path / name;
            if (fs::is_symlink(path))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Symbolic link replaces an UpsertMergeTree detached part");
            fs::remove_all(path);
            consumed_detached = true;
        }
        check(iterator->status());
        if (consumed_detached)
        {
            syncDirectory(detached_path);
            rocksdb::WriteBatch batch;
            check(batch.DeleteRange("D", "E"));
            rocksdb::WriteOptions write;
            write.sync = true;
            check(database->db->Write(write, &batch));
        }
        /// A preparation without a durable import remains available for another attachment attempt.
        if (fs::is_directory(detached_path))
        {
            for (const auto & entry : fs::directory_iterator(detached_path))
            {
                const auto name = entry.path().filename().string();
                if (!name.starts_with("attaching_")
                    || !MergeTreePartInfo::tryParsePartName(name.substr(10), MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING))
                    continue;
                auto restored = detached_path / name.substr(10);
                if (entry.is_symlink())
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "Symbolic link replaces an UpsertMergeTree detached preparation {}", name);
                if (fs::exists(restored))
                {
                    do
                    {
                        restored = detached_path / ("upsert_recovered_" + objectName(newObjectID()) + "_" + name.substr(10));
                    }
                    while (fs::exists(restored));
                    LOG_WARNING(getLogger("UpsertMergeTree"),
                        "Preserving uncommitted detached preparation {} as {} because its original name already exists",
                        name, restored.filename().string());
                }
                fs::rename(entry.path(), restored);
            }
            syncDirectory(detached_path);
        }
        auto masks = std::make_shared<DiskMaskSnapshot>(database);
        masks->bind();
        current = newSnapshot(std::move(masks));
    }
};

DiskState::DiskState(const String & table_path, ObjectID incarnation, bool attach, StateClock clock, const Resources & resources) : impl(std::make_unique<Impl>())
{
    namespace fs = std::filesystem;
    resources.validate();
    impl->table_path = table_path;
    impl->resources = resources;
    if (resources.mask_cache_bytes)
        impl->database->mask_cache = std::make_unique<DeleteBitmapCache>("LRU", CurrentMetrics::UpsertMaskCacheBytes,
            CurrentMetrics::UpsertMaskCacheEntries, resources.mask_cache_bytes, 0.5);
    impl->clock = clock ? std::move(clock) : StateClock([]
    {
        return static_cast<UInt64>(std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    });
    const auto path = fs::path(table_path) / "upsert_state_v2";
    if ((!attach && fs::exists(path)) || (attach && (!fs::is_directory(path) || fs::is_symlink(path))))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree state directory does not match CREATE/ATTACH mode");
    rocksdb::Options options;
    options.create_if_missing = !attach;
    options.error_if_exists = !attach;
    options.paranoid_checks = true;
    options.wal_recovery_mode = rocksdb::WALRecoveryMode::kTolerateCorruptedTailRecords;
    options.track_and_verify_wals = true;
    options.use_fsync = true;
    options.max_open_files = static_cast<int>(resources.max_open_files);
    options.write_buffer_size = resources.write_buffer_bytes;
    /// Most initial-insert probes miss. Filter complete keys without changing winner equality or key routing.
    options.memtable_prefix_bloom_size_ratio = 0.1;
    options.memtable_whole_key_filtering = true;
    options.max_write_buffer_number = static_cast<int>(resources.write_buffer_count);
    options.max_total_wal_size = resources.wal_bytes;
    options.max_manifest_file_size = 16U << 20;
    options.keep_log_file_num = 2;
    options.max_background_jobs = static_cast<int>(resources.background_jobs);
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = rocksdb::NewLRUCache(resources.index_cache_bytes);
    table_options.cache_index_and_filter_blocks = true;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
    table_options.whole_key_filtering = true;
    if (resources.partitioned_filters)
    {
        table_options.index_type = rocksdb::BlockBasedTableOptions::kTwoLevelIndexSearch;
        table_options.partition_filters = true;
    }
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    rocksdb::DB * database = nullptr;
    check(rocksdb::DB::Open(options, path.string(), &database));
    impl->database->db.reset(database);
    const auto identity = "upsert_disk_state_v2:" + objectName(incarnation);
    if (!attach)
    {
        rocksdb::WriteBatch batch;
        batch.Put("identity", identity);
        batch.Put("part_count", number(0));
        batch.Put("retry_bytes", number(0));
        rocksdb::WriteOptions write;
        write.sync = true;
        check(database->Write(write, &batch));
        syncDirectory(path);
        syncDirectory(table_path);
    }
    if (impl->database->get("identity") != identity)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree disk state incarnation or protocol mismatch");
    impl->recover();
}

DiskState::~DiskState() = default;

PartRows DiskState::loadPartRows(const String & name) const
{
    auto value = impl->database->get("A" + name);
    if (!value)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance source is not authorized");
    const auto record = decodePartCommit(*value);
    auto rows = decodePartRows(readBoundedFile(std::filesystem::path(impl->table_path) / name / ROWS_FILE, MAX_RECORD_BYTES));
    if (rows.size() != record.rows)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance source row count mismatch");
    const auto csn = impl->database->integer("O" + objectName(record.part));
    for (auto & row : rows)
    {
        if (row.value.row.self_binding && row.value.row.operation_id != record.part)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree maintenance source has invalid SELF lineage");
        row.value.row = row.value.row.resolve(csn);
    }
    return rows;
}

void DiskState::pruneRetired()
{
    rocksdb::ReadOptions options;
    options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iterator(impl->database->db->NewIterator(options));
    rocksdb::WriteBatch batch;
    size_t examined = 0;
    iterator->Seek(impl->retirement_cursor);
    if (impl->retirement_cursor != "R" && iterator->Valid() && iterator->key().ToString() == impl->retirement_cursor)
        iterator->Next();
    for (; iterator->Valid() && iterator->key().starts_with("R") && examined < 128; iterator->Next(), ++examined)
    {
        auto record = decodePartCommit(iterator->value().ToStringView());
        if (!record.rows || iterator->key().ToString() != "R" + record.final_name)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree retirement authorization");
        impl->retirement_cursor = iterator->key().ToString();
        if (!std::filesystem::exists(std::filesystem::path(impl->table_path) / record.final_name))
            batch.Delete(iterator->key());
    }
    check(iterator->status());
    if (!iterator->Valid() || !iterator->key().starts_with("R"))
        impl->retirement_cursor = "R";
    bool removed_detached = false;
    const auto detached_path = std::filesystem::path(impl->table_path) / "detached";
    for (iterator->Seek("D"), examined = 0; iterator->Valid() && iterator->key().starts_with("D") && examined < 128; iterator->Next(), ++examined)
    {
        const auto name = iterator->key().ToString().substr(1);
        if (!name.starts_with("attaching_")
            || !MergeTreePartInfo::tryParsePartName(name.substr(10), MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
            || name.find('/') != String::npos || name.find('\\') != String::npos)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree consumed detached part");
        const auto path = detached_path / name;
        if (std::filesystem::is_symlink(path))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Symbolic link replaces an UpsertMergeTree detached part");
        std::filesystem::remove_all(path);
        removed_detached = true;
        batch.Delete(iterator->key());
    }
    check(iterator->status());
    if (removed_detached)
        syncDirectory(detached_path);
    const auto now = impl->clock();
    auto retry_bytes = impl->database->integer("retry_bytes");
    for (iterator->Seek("E"), examined = 0; iterator->Valid() && iterator->key().starts_with("E") && examined < 128; iterator->Next(), ++examined)
    {
        const auto key = iterator->key().ToString();
        if (key.size() < 10 || key[9] != 'T')
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree retry expiration entry");
        UInt64 expiry = 0;
        for (size_t i = 1; i < 9; ++i)
            expiry = (expiry << 8) | static_cast<UInt8>(key[i]);
        if (expiry > now)
            break;
        const auto token_key = key.substr(9);
        const auto value = impl->database->get(token_key);
        if (value && tokenExpiry(*value) == expiry)
        {
            const auto cost = tokenCost(token_key);
            if (retry_bytes < cost)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree retry byte counter underflow");
            retry_bytes -= cost;
            batch.Delete(token_key);
        }
        batch.Delete(key);
    }
    check(iterator->status());
    if (batch.Count())
    {
        batch.Put("retry_bytes", number(retry_bytes));
        /// Deletion must survive a power loss before forgetting its durable authorization.
        syncDirectory(impl->table_path);
        rocksdb::WriteOptions write;
        write.sync = true;
        check(impl->database->db->Write(write, &batch));
    }
}

SnapshotPtr DiskState::snapshot() const
{
    return impl->current;
}

UInt64 DiskState::authorizedPartCount() const
{
    return impl->part_count;
}

void DiskState::checkAdmission() const
{
    if (std::filesystem::space(impl->table_path).available < maintenance_disk_reserve)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree is preserving free disk space for maintenance");
}

std::optional<ObjectID> DiskState::authorizedPart(const String & name) const
{
    auto value = impl->database->get("A" + name);
    if (!value)
        return std::nullopt;
    return decodePartCommit(*value).part;
}

bool DiskState::tokenAlreadyCommitted(const String & partition, const Token & token, bool throw_on_mismatch) const
{
    if (token.client_token.empty())
        return false;
    const auto value = impl->database->get(tokenKey(partition, token));
    if (!value)
        return false;
    BinaryReader in{*value};
    const auto hash = in.id();
    in.number(); // Persist the original accepted-row count, including zero.
    const auto expires = in.number();
    in.end();
    if (expires <= impl->clock())
        return false;
    if (hash != token.payload_hash)
    {
        if (throw_on_mismatch)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "UpsertMergeTree deduplication token was reused with a different evaluated block");
        return false;
    }
    return true;
}

StateStore::Prepared DiskState::prepare(const String & partition, PartCommit part, const ValidatedPartRows & validated)
{
    ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertStatePrepareMicroseconds);
    if (part.part != validated.getPart())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree input validation identity mismatch");
    const auto & rows = validated.getRows();
    if (tokenAlreadyCommitted(partition, part.token))
    {
        StateStore::Prepared replay;
        replay.replayed_token = true;
        return replay;
    }
    if (partition.empty() || partition.size() > MAX_PARTITION_ID_BYTES || part.rows != rows.size()
        || rows.empty() || rows.size() > MAX_UNIT_ROWS || part.token.client_token.size() > 1024)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid UpsertMergeTree commit unit");
    const auto info = MergeTreePartInfo::fromPartName(part.final_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    if (info.getPartitionId() != partition || info.level != 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree input part has invalid partition or level");
    if (impl->database->get("O" + objectName(part.part)))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Reused UpsertMergeTree operation identity");
    StateStore::Prepared result;
    result.partition = partition;
    result.operation_id = part.part;
    result.disk = std::make_shared<DiskPrepared>();
    const auto csn_key = prefix('C', partition);
    if (impl->database->get(csn_key))
        result.disk->previous_csn = impl->database->integer(csn_key);
    if (result.disk->previous_csn == std::numeric_limits<UInt64>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree commit sequence exhausted");
    result.csn = result.disk->previous_csn + 1;
    auto & batch = result.disk->batch;
    auto dead = std::make_shared<DeleteBitmap>();
    part.dead.clear();
    part.kills.clear();
    impl->database->forEachWinner(partition, rows, [&](const PartRow & row, const String & key, std::optional<std::string_view> incumbent)
    {
        auto candidate = row.value;
        candidate.row = candidate.row.resolve(result.csn);
        if (incumbent)
        {
            BinaryReader in{*incumbent};
            auto old_part = in.id();
            auto old = decodeIndexValue(in.string());
            in.end();
            if (candidate.row.compareWinner(old.row) < 0)
            {
                part.dead.push_back(candidate.row_offset);
                dead->add(candidate.row_offset);
                return;
            }
            part.kills.push_back({old_part, old.row_offset});
        }
        BinaryWriter winner;
        winner.id(part.part);
        winner.string(encodeIndexValue(candidate));
        batch.Put(key, winner.bytes);
        checkBatchBudget(batch);
    });
    std::sort(part.dead.begin(), part.dead.end());
    std::sort(part.kills.begin(), part.kills.end());
    for (size_t position = 0; position < part.kills.size();)
    {
        const auto id = part.kills[position].part;
        const auto old = impl->current->getMask(objectName(id));
        if (!old)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree winner has no visibility mask");
        DeleteBitmap mask;
        mask.merge(*old);
        do
        {
            const auto offset = part.kills[position].offset;
            if (mask.contains(offset))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree attempted to kill an already dead winner");
            mask.add(offset);
            ++position;
        } while (position < part.kills.size() && part.kills[position].part == id);
        result.disk->putMask(objectName(id), mask);
        checkBatchBudget(batch);
    }
    part.token.accepted = part.rows - part.dead.size();
    result.produces_part = part.token.accepted != 0;
    if (!part.token.client_token.empty())
    {
        const auto key = tokenKey(partition, part.token);
        auto retry_bytes = impl->database->integer("retry_bytes");
        const auto cost = tokenCost(key);
        if (impl->database->get(key))
        {
            if (retry_bytes < cost)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree retry byte counter underflow");
            retry_bytes -= cost;
        }
        if (cost > retry_state_budget || retry_bytes > retry_state_budget - cost)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree retry state budget is full; active tokens cannot expire early");
        const auto now = impl->clock();
        if (now > std::numeric_limits<UInt64>::max() - retry_window_seconds)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree retry expiration overflow");
        const auto expires = now + retry_window_seconds;
        BinaryWriter token;
        token.id(part.token.payload_hash);
        token.number(part.token.accepted);
        token.number(expires);
        batch.Put(key, token.bytes);
        batch.Put(expiryKey(expires, key), "");
        batch.Put("retry_bytes", number(retry_bytes + cost));
    }
    if (result.produces_part)
    {
        if (impl->part_count == std::numeric_limits<UInt64>::max())
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree part counter exhausted");
        result.payload = encodePartCommit(part);
        decodePartCommit(result.payload);
        if (authorizedPart(part.final_name))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Reused UpsertMergeTree final part name");
        batch.Put("A" + part.final_name, result.payload);
        result.disk->putMask(objectName(part.part), *dead);
        batch.Put("part_count", number(impl->part_count + 1));
        batch.Put("O" + objectName(result.operation_id), number(result.csn));
    }
    else
    {
        auto token = std::move(part.token);
        part = {};
        part.token = std::move(token);
    }
    batch.Put(csn_key, number(result.csn));
    result.record = std::move(part);
    checkBatchBudget(batch);
    result.disk->masks = std::make_shared<DiskMaskSnapshot>(impl->database);
    result.snapshot = impl->newSnapshot(result.disk->masks);
    return result;
}

StateStore::Prepared DiskState::prepareMerge(
    const String & partition, PartCommit part, const PartRows & rows, const std::vector<String> & sources,
    const PartRows & expired, bool retire_sources, const StateStore::MergeInput * input)
{
    ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertStatePrepareMicroseconds);
    if ((!retire_sources && !input)
        || (input && (!input->snapshot || input->lineage.size() != sources.size() || input->offsets.size() != sources.size()))
        || sources.empty() || sources.size() > 32 || part.rows != rows.size() || rows.size() > MAX_UNIT_ROWS
        || expired.size() > MAX_UNIT_ROWS - rows.size()
        || part.part == ObjectID{} || impl->database->get("O" + objectName(part.part)))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid UpsertMergeTree maintenance unit");
    StateStore::Prepared result;
    result.partition = partition;
    result.operation_id = part.part;
    if (retire_sources)
        result.retired_parts = sources;
    result.disk = std::make_shared<DiskPrepared>();
    const auto csn_key = prefix('C', partition);
    result.disk->previous_csn = impl->database->integer(csn_key);
    if (result.disk->previous_csn == std::numeric_limits<UInt64>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree commit sequence exhausted");
    result.csn = result.disk->previous_csn + 1;
    auto & batch = result.disk->batch;
    std::map<String, const PartRow *> output;
    for (size_t offset = 0; offset < rows.size(); ++offset)
    {
        const auto & row = rows[offset];
        if (row.value.row.self_binding || row.value.row_offset != offset || !output.emplace(row.key, &row).second)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree merge output lineage or duplicate key");
    }
    auto dead = std::make_shared<DeleteBitmap>();
    std::map<String, const PartRow *> expirations;
    for (const auto & row : expired)
    {
        if (row.value.row.self_binding || output.contains(row.key) || !expirations.emplace(row.key, &row).second)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree expiration lineage or duplicate key");
    }
    std::set<String> source_names;
    std::set<String> mapped_output;
    std::set<String> mapped_expirations;
    for (size_t source_index = 0; source_index < sources.size(); ++source_index)
    {
        const auto & name = sources[source_index];
        if (!source_names.insert(name).second || name == part.final_name)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Duplicate UpsertMergeTree merge source or reused output name");
        auto authorization = impl->database->get("A" + name);
        if (!authorization)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge source is no longer active");
        const auto source = decodePartCommit(*authorization);
        const auto info = MergeTreePartInfo::fromPartName(name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
        if (info.getPartitionId() != partition)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge crosses SQL partitions");
        PartRows loaded_rows;
        if (!input)
            loaded_rows = loadPartRows(name);
        const auto & source_rows = input ? input->lineage[source_index] : loaded_rows;
        if (source_rows.size() != source.rows)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree cached source lineage has a different row count");
        const auto source_mask = impl->current->getMask(objectName(source.part));
        if (!source_mask)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge source has no visibility mask");
        const auto original_mask = input ? input->snapshot->getMask(objectName(source.part)) : nullptr;
        if (input && !original_mask)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree rewrite source is absent from its read snapshot");
        DeleteBitmap rewritten_mask;
        rewritten_mask.merge(*source_mask);
        impl->database->forEachWinner(partition, source_rows, [&](const PartRow & row, const String & key, std::optional<std::string_view> value)
        {
            auto it = output.find(row.key);
            /// A partial rewrite leaves masked copies of the same lineage in an active source.
            /// Only the copy visible when the payload was read can authorize an output or expiration.
            const bool was_visible = !original_mask || !original_mask->contains(row.value.row_offset);
            const bool maps_to_output = was_visible && it != output.end() && it->second->value.row == row.value.row;
            auto expiration = expirations.find(row.key);
            const bool expires = was_visible && expiration != expirations.end() && expiration->second->value == row.value;
            if (maps_to_output && !mapped_output.insert(row.key).second)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge maps an output identity twice");
            if (expires && !mapped_expirations.insert(row.key).second)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge maps an expiration identity twice");
            if (!value)
            {
                /// An earlier `TTL` commit can remove the winner while superseded physical rows still await reclamation.
                if (source_mask->contains(row.value.row_offset) && !maps_to_output && !expires)
                    return;
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge source has no durable winner");
            }
            BinaryReader in{*value};
            const auto winner_part = in.id();
            const auto winner = decodeIndexValue(in.string());
            in.end();
            if (winner_part == source.part && winner.row_offset == row.value.row_offset)
            {
                if (!retire_sources && !maps_to_output && !expires)
                    return;
                if ((!maps_to_output && !expires) || winner.row != row.value.row || source_mask->contains(row.value.row_offset))
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge omitted or changed a live source identity");
                if (expires)
                    batch.Delete(key);
                else
                {
                    BinaryWriter updated;
                    updated.id(part.part);
                    updated.string(encodeIndexValue(it->second->value));
                    batch.Put(key, updated.bytes);
                }
                rewritten_mask.add(row.value.row_offset);
                checkBatchBudget(batch);
            }
            else if (maps_to_output)
            {
                /// A concurrent upsert superseded this exact source row after the merge snapshot.
                dead->add(it->second->value.row_offset);
            }
        }, retire_sources ? nullptr : &input->offsets[source_index]);
        if (retire_sources)
        {
            batch.Put("R" + name, *authorization);
            batch.Delete("A" + name);
            result.disk->deleteMask(objectName(source.part));
            batch.Delete("O" + objectName(source.part));
        }
        else
        {
            /// Relocation and source masking share one durable batch; readers cannot see both copies.
            result.disk->putMask(objectName(source.part), rewritten_mask);
        }
    }
    if (mapped_output.size() != output.size())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge invented an output identity");
    if (mapped_expirations.size() != expirations.size())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge invented an expiration identity");
    part.dead.clear();
    for (const auto offset : dead->toVector())
    {
        if (offset >= rows.size())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge mask exceeds output rows");
        part.dead.push_back(static_cast<UInt32>(offset));
    }
    part.kills.clear();
    part.token = {};
    part.token.accepted = rows.size() - part.dead.size();
    result.produces_part = part.token.accepted != 0;
    if (result.produces_part)
    {
        result.payload = encodePartCommit(part);
        decodePartCommit(result.payload);
        const auto info = MergeTreePartInfo::fromPartName(part.final_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
        if (info.getPartitionId() != partition || authorizedPart(part.final_name))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge output partition or name mismatch");
        batch.Put("A" + part.final_name, result.payload);
        result.disk->putMask(objectName(part.part), *dead);
        batch.Put("O" + objectName(part.part), number(result.csn));
    }
    else
        part = {};
    if (result.retired_parts.size() > impl->part_count)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge retired more parts than exist");
    if (result.produces_part && result.retired_parts.empty() && impl->part_count == std::numeric_limits<UInt64>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree part counter exhausted");
    batch.Put("part_count", number(impl->part_count - result.retired_parts.size() + result.produces_part));
    batch.Put(csn_key, number(result.csn));
    result.record = std::move(part);
    checkBatchBudget(batch);
    result.disk->masks = std::make_shared<DiskMaskSnapshot>(impl->database);
    result.snapshot = impl->newSnapshot(result.disk->masks);
    return result;
}

StateStore::Prepared DiskState::prepareTruncate()
{
    ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertStatePrepareMicroseconds);
    StateStore::Prepared result;
    result.disk = std::make_shared<DiskPrepared>();
    result.disk->catalog_change = true;
    result.disk->previous_part_count = impl->part_count;
    auto & batch = result.disk->batch;
    rocksdb::ReadOptions options;
    options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> iterator(impl->database->db->NewIterator(options));
    for (iterator->Seek("A"); iterator->Valid() && iterator->key().starts_with("A"); iterator->Next())
    {
        auto record = decodePartCommit(iterator->value().ToStringView());
        if (!record.rows || iterator->key().ToString() != "A" + record.final_name)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree truncate source identity mismatch");
        if (impl->database->get("R" + record.final_name))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree active part already has retirement authorization");
        result.retired_parts.push_back(record.final_name);
        /// Retirement needs the payload identity, not historical winner kills or retry tokens.
        record.dead.clear();
        record.kills.clear();
        record.token = {};
        record.token.accepted = record.rows;
        batch.Put("R" + record.final_name, encodePartCommit(record));
        checkBatchBudget(batch);
        CurrentMemoryTracker::check();
    }
    check(iterator->status());
    if (result.retired_parts.size() != impl->part_count)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree truncate source count differs from its catalog");

    /// Clear active authorizations, sequence counters, token expiry, masks, operation bindings, tokens and winners.
    /// Range tombstones preserve the old masks for readers holding a pre-truncate `RocksDB` snapshot.
    result.disk->clearMasks();
    for (const char space : {'A', 'C', 'E', 'O', 'T', 'W'})
        check(batch.DeleteRange(String(1, space), String(1, space + 1)));
    batch.Put("part_count", number(0));
    batch.Put("retry_bytes", number(0));
    checkBatchBudget(batch);
    result.disk->masks = std::make_shared<DiskMaskSnapshot>(impl->database);
    result.snapshot = impl->newSnapshot(result.disk->masks);
    return result;
}

StateStore::Prepared DiskState::preparePartition(
    std::vector<PartCommit> additions, const std::vector<ValidatedPartRows> & rows,
    const std::vector<String> & removed, const std::vector<String> & reset_partitions, const std::vector<String> & detached,
    const std::vector<String> & cleared_partitions)
{
    ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertStatePrepareMicroseconds);
    if (additions.size() != rows.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree partition import row count mismatch");
    StateStore::Prepared result;
    result.disk = std::make_shared<DiskPrepared>();
    result.disk->catalog_change = true;
    result.disk->previous_part_count = impl->part_count;
    auto & batch = result.disk->batch;
    const std::set<String> reset(reset_partitions.begin(), reset_partitions.end());
    const std::set<String> cleared(cleared_partitions.begin(), cleared_partitions.end());
    std::set<ObjectID> retired_ids;
    std::set<String> retired_names;
    for (const auto & name : removed)
    {
        const auto value = impl->database->get("A" + name);
        if (!value || !retired_names.insert(name).second)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree partition removal source {}", name);
        auto source = decodePartCommit(*value);
        if (source.final_name != name || !source.rows || impl->database->get("R" + name))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree partition source authorization");
        retired_ids.insert(source.part);
        const auto partition = MergeTreePartInfo::fromPartName(name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING).getPartitionId();
        if (!cleared.contains(partition))
        {
            const auto source_rows = loadPartRows(name);
            impl->database->forEachWinner(partition, source_rows, [&](const PartRow & row, const String & key, std::optional<std::string_view> winner)
            {
                if (!winner)
                    return;
                BinaryReader in{*winner};
                const auto part_id = in.id();
                const auto value_row = decodeIndexValue(in.string());
                in.end();
                if (part_id == source.part && value_row.row_offset == row.value.row_offset)
                    batch.Delete(key);
                checkBatchBudget(batch);
            });
        }
        source.dead.clear();
        source.kills.clear();
        source.token = {};
        source.token.accepted = source.rows;
        batch.Put("R" + name, encodePartCommit(source));
        batch.Delete("A" + name);
        result.disk->deleteMask(objectName(source.part));
        batch.Delete("O" + objectName(source.part));
        result.retired_parts.push_back(name);
        checkBatchBudget(batch);
    }
    for (const auto & name : detached)
    {
        if (!name.starts_with("attaching_")
            || !MergeTreePartInfo::tryParsePartName(name.substr(10), MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
            || name.find('/') != String::npos || name.find('\\') != String::npos)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree detached import name");
        if (impl->database->get("D" + name))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree detached name still has a pending cleanup");
        batch.Put("D" + name, "");
        checkBatchBudget(batch);
    }
    for (const auto & partition : cleared)
    {
        const auto winner_prefix = prefix('W', partition);
        auto upper = winner_prefix;
        while (!upper.empty() && static_cast<unsigned char>(upper.back()) == 255)
            upper.pop_back();
        if (upper.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid UpsertMergeTree partition prefix");
        upper.back() = static_cast<char>(static_cast<unsigned char>(upper.back()) + 1);
        check(batch.DeleteRange(winner_prefix, upper));
    }
    /// A range tombstone removes old winners in constant space. Restore only winners
    /// owned by parts that were committed after selection and remain active.
    rocksdb::ReadOptions survivor_options;
    survivor_options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> survivor(impl->database->db->NewIterator(survivor_options));
    for (survivor->Seek("A"); !cleared.empty() && survivor->Valid() && survivor->key().starts_with("A"); survivor->Next())
    {
        const auto source = decodePartCommit(survivor->value().ToStringView());
        if (retired_names.contains(source.final_name))
            continue;
        const auto partition = MergeTreePartInfo::fromPartName(
            source.final_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING).getPartitionId();
        if (!cleared.contains(partition))
            continue;
        const auto source_rows = loadPartRows(source.final_name);
        impl->database->forEachWinner(partition, source_rows, [&](const PartRow & row, const String & key, std::optional<std::string_view> winner)
        {
            if (!winner)
                return;
            BinaryReader in{*winner};
            const auto id = in.id();
            const auto value = decodeIndexValue(in.string());
            in.end();
            if (id == source.part && value.row_offset == row.value.row_offset)
                batch.Put(key, rocksdb::Slice(winner->data(), winner->size()));
            checkBatchBudget(batch);
            CurrentMemoryTracker::check();
        });
    }
    check(survivor->status());
    auto retry_bytes = impl->database->integer("retry_bytes");
    for (const auto & partition : reset)
    {
        batch.Delete(prefix('C', partition));
        const auto token_prefix = prefix('T', partition);
        rocksdb::ReadOptions options;
        options.fill_cache = false;
        std::unique_ptr<rocksdb::Iterator> iterator(impl->database->db->NewIterator(options));
        for (iterator->Seek(token_prefix); iterator->Valid() && iterator->key().starts_with(token_prefix); iterator->Next())
        {
            const auto key = iterator->key().ToString();
            const auto cost = tokenCost(key);
            if (retry_bytes < cost)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree partition retry byte counter underflow");
            retry_bytes -= cost;
            batch.Delete(expiryKey(tokenExpiry(iterator->value().ToString()), key));
            batch.Delete(key);
            checkBatchBudget(batch);
        }
        check(iterator->status());
    }
    batch.Put("retry_bytes", number(retry_bytes));

    std::map<ObjectID, std::shared_ptr<DeleteBitmap>> masks;
    std::map<String, std::pair<ObjectID, IndexValue>> winners;
    std::map<String, UInt64> sequences;
    for (size_t i = 0; i < additions.size(); ++i)
    {
        auto & part = additions[i];
        if (part.part != rows[i].getPart() || part.rows != rows[i].getRows().size()
            || authorizedPart(part.final_name) || impl->database->get("O" + objectName(part.part)) || masks.contains(part.part))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree partition import identity");
        const auto partition = MergeTreePartInfo::fromPartName(part.final_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING).getPartitionId();
        auto [sequence, inserted] = sequences.try_emplace(partition, 0);
        if (inserted && impl->database->get(prefix('C', partition)))
            sequence->second = impl->database->integer(prefix('C', partition));
        if (sequence->second == std::numeric_limits<UInt64>::max())
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree partition commit sequence exhausted");
        const auto csn = ++sequence->second;
        masks.emplace(part.part, std::make_shared<DeleteBitmap>());
        for (const auto & row : rows[i].getRows())
        {
            const auto key = prefix('W', partition) + row.key;
            auto candidate = row.value;
            candidate.row = candidate.row.resolve(csn);
            auto incumbent = winners.find(key);
            if (incumbent == winners.end() && !reset.contains(partition))
            {
                if (const auto value = impl->database->get(key))
                {
                    BinaryReader in{*value};
                    const auto id = in.id();
                    auto winner = decodeIndexValue(in.string());
                    in.end();
                    if (!retired_ids.contains(id))
                        incumbent = winners.emplace(key, std::make_pair(id, winner)).first;
                }
            }
            if (incumbent != winners.end())
            {
                if (candidate.row.compareWinner(incumbent->second.second.row) < 0)
                {
                    masks.at(part.part)->add(candidate.row_offset);
                    continue;
                }
                const auto id = incumbent->second.first;
                auto [mask, new_mask] = masks.try_emplace(id, nullptr);
                if (new_mask)
                {
                    const auto previous = impl->current->getMask(objectName(id));
                    if (!previous)
                        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree partition winner has no mask");
                    mask->second = std::make_shared<DeleteBitmap>();
                    mask->second->merge(*previous);
                }
                if (mask->second->contains(incumbent->second.second.row_offset))
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree partition winner is already masked");
                mask->second->add(incumbent->second.second.row_offset);
            }
            winners.insert_or_assign(key, std::make_pair(part.part, candidate));
            BinaryWriter value;
            value.id(part.part);
            value.string(encodeIndexValue(candidate));
            batch.Put(key, value.bytes);
            checkBatchBudget(batch);
            CurrentMemoryTracker::check();
        }
        batch.Put("O" + objectName(part.part), number(csn));
    }
    for (auto & part : additions)
    {
        const auto & mask = masks.at(part.part);
        const auto dead = mask->toVector();
        part.dead.assign(dead.begin(), dead.end());
        part.kills.clear();
        part.token = {};
        part.token.accepted = part.rows - part.dead.size();
        if (!part.token.accepted)
        {
            batch.Delete("O" + objectName(part.part));
            masks.erase(part.part);
            continue;
        }
        const auto payload = encodePartCommit(part);
        decodePartCommit(payload);
        batch.Put("A" + part.final_name, payload);
        result.added_parts.push_back(std::move(part));
        checkBatchBudget(batch);
    }
    for (const auto & [id, mask] : masks)
    {
        result.disk->putMask(objectName(id), *mask);
        checkBatchBudget(batch);
    }
    for (const auto & [partition, csn] : sequences)
        batch.Put(prefix('C', partition), number(csn));
    if (result.retired_parts.size() > impl->part_count
        || result.added_parts.size() > std::numeric_limits<UInt64>::max() - (impl->part_count - result.retired_parts.size()))
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree partition part counter overflow");
    batch.Put("part_count", number(impl->part_count - result.retired_parts.size() + result.added_parts.size()));
    checkBatchBudget(batch);
    result.disk->masks = std::make_shared<DiskMaskSnapshot>(impl->database);
    result.snapshot = impl->newSnapshot(result.disk->masks);
    return result;
}

void DiskState::append(const StateStore::Prepared & prepared)
{
    if (!prepared.disk || prepared.disk->durable)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid or already committed UpsertMergeTree disk preparation");
    if (prepared.disk->catalog_change)
    {
        if (prepared.disk->previous_part_count != impl->part_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree partition catalog changed under the commit lock");
    }
    else
    {
        const auto key = prefix('C', prepared.partition);
        auto current = impl->database->get(key) ? impl->database->integer(key) : 0;
        if (current != prepared.disk->previous_csn)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree commit sequence changed under the commit lock");
    }
    const auto generation = impl->database->mask_generation + 1;
    if (!generation)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree mask generation exhausted");
    auto versions = impl->database->mask_versions;
    for (const auto & change : prepared.disk->mask_changes)
    {
        switch (change.kind)
        {
            case MaskChange::Kind::Put:
                versions.set(change.part, generation);
                break;
            case MaskChange::Kind::Delete:
                versions.erase(change.part);
                break;
            case MaskChange::Kind::Clear:
                versions.clear();
                break;
        }
    }
#ifndef NDEBUG
    MaskVersionUpdates mask_updates;
    check(prepared.disk->batch.Iterate(&mask_updates));
    if (mask_updates.changes != prepared.disk->mask_changes)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree mask changes differ from the state batch");
#endif
    rocksdb::WriteOptions options;
    options.sync = true;
    FailPointInjection::pauseFailPoint(FailPoints::upsert_pause_before_state_write);
    {
        ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertStateWriteMicroseconds);
        check(impl->database->db->Write(options, &prepared.disk->batch));
        ProfileEvents::increment(ProfileEvents::UpsertStateWrites);
        ProfileEvents::increment(ProfileEvents::UpsertStateWriteBytes, prepared.disk->batch.GetDataSize());
    }
    prepared.disk->durable = true;
    FailPointInjection::pauseFailPoint(FailPoints::upsert_pause_after_state_sync);
    fiu_do_on(FailPoints::upsert_log_throw_after_append,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree failure after disk-state batch write");
    });
    fiu_do_on(FailPoints::upsert_log_throw_after_sync,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree failure after disk-state sync");
    });
    impl->database->mask_versions = std::move(versions);
    ++impl->database->mask_generation;
    prepared.disk->masks->bind();
}

void DiskState::publish(StateStore::Prepared & prepared) noexcept
{
    impl->current.swap(prepared.snapshot);
    impl->part_count = impl->part_count - prepared.retired_parts.size() + prepared.produces_part + prepared.added_parts.size();
}
#else
struct DiskState::Impl
{
};
struct DiskPrepared
{
};
DiskState::DiskState(const String &, ObjectID, bool, StateClock, const Resources &)
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree disk state requires a build with RocksDB");
}
DiskState::~DiskState() = default;
SnapshotPtr DiskState::snapshot() const
{
    std::unreachable();
}
UInt64 DiskState::authorizedPartCount() const
{
    std::unreachable();
}
void DiskState::checkAdmission() const
{
    std::unreachable();
}
std::optional<ObjectID> DiskState::authorizedPart(const String &) const
{
    std::unreachable();
}
bool DiskState::tokenAlreadyCommitted(const String &, const Token &, bool) const
{
    std::unreachable();
}
StateStore::Prepared DiskState::prepare(const String &, PartCommit, const ValidatedPartRows &)
{
    std::unreachable();
}
StateStore::Prepared DiskState::prepareMerge(const String &, PartCommit, const PartRows &, const std::vector<String> &, const PartRows &, bool, const StateStore::MergeInput *)
{
    std::unreachable();
}
StateStore::Prepared DiskState::prepareTruncate()
{
    std::unreachable();
}
PartRows DiskState::loadPartRows(const String &) const
{
    std::unreachable();
}
void DiskState::pruneRetired()
{
    std::unreachable();
}
StateStore::Prepared DiskState::preparePartition(std::vector<PartCommit>, const std::vector<ValidatedPartRows> &,
    const std::vector<String> &, const std::vector<String> &, const std::vector<String> &, const std::vector<String> &)
{
    std::unreachable();
}
void DiskState::append(const StateStore::Prepared &)
{
    std::unreachable();
}
void DiskState::publish(StateStore::Prepared &) noexcept
{
    std::unreachable();
}
#endif
}
