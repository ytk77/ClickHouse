#include <Storages/MergeTree/UniqueKey/UpsertDiskState.h>
#include <Storages/MergeTree/UniqueKey/UpsertBinary.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/FailPoint.h>
#include <Common/CurrentMemoryTracker.h>
#include "config.h"
#include <filesystem>
#include <limits>
#include <utility>
#include <chrono>

#if USE_ROCKSDB
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>
#endif

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
}

namespace DB::Upsert
{
#if USE_ROCKSDB
namespace
{
constexpr UInt64 retry_window_seconds = 86400;
constexpr UInt64 retry_state_budget = 1ULL << 30;
constexpr UInt64 pending_batch_budget = 128ULL << 20;
constexpr UInt64 maintenance_disk_reserve = 1ULL << 30;

void checkBatchBudget(const rocksdb::WriteBatch & batch)
{
    if (batch.GetDataSize() > pending_batch_budget)
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
    return out.str();
}

struct Database
{
    std::unique_ptr<rocksdb::DB> db;

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
    void forEachWinner(const String & partition, const PartRows & rows, Callback && callback) const
    {
        constexpr size_t lookup_batch_size = 1024;
        const auto key_prefix = prefix('W', partition);
        for (size_t begin = 0; begin < rows.size(); begin += lookup_batch_size)
        {
            const auto count = std::min(lookup_batch_size, rows.size() - begin);
            std::vector<String> keys;
            keys.reserve(count);
            for (size_t i = 0; i < count; ++i)
                keys.push_back(key_prefix + rows[begin + i].key);
            std::vector<rocksdb::Slice> slices;
            slices.reserve(count);
            for (const auto & key : keys)
                slices.emplace_back(key);
            std::vector<rocksdb::PinnableSlice> values(count);
            std::vector<rocksdb::Status> statuses(count);
            db->MultiGet(rocksdb::ReadOptions{}, db->DefaultColumnFamily(), count, slices.data(), values.data(), statuses.data());
            for (size_t i = 0; i < count; ++i)
            {
                std::optional<std::string_view> value;
                if (!statuses[i].IsNotFound())
                {
                    check(statuses[i]);
                    value = std::string_view(values[i].data(), values[i].size());
                }
                callback(rows[begin + i], keys[i], value);
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
        cut = database->db->GetSnapshot();
        if (!cut)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree could not pin its disk snapshot");
    }
    std::shared_ptr<const DeleteBitmap> getMask(const String & part) const override
    {
        if (!cut)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree disk snapshot was read before commit");
        auto value = database->get("M" + part, cut);
        if (!value)
            return nullptr;
        ReadBufferFromString in(*value);
        return DeleteBitmap::deserialize(in);
    }

private:
    std::shared_ptr<Database> database;
    const rocksdb::Snapshot * cut = nullptr;
};
}

struct DiskPrepared
{
    rocksdb::WriteBatch batch;
    std::shared_ptr<DiskMaskSnapshot> masks;
    UInt64 previous_csn = 0;
    bool durable = false;
};

struct DiskState::Impl
{
    std::shared_ptr<Database> database = std::make_shared<Database>();
    SnapshotPtr current;
    String table_path;
    String retirement_cursor = "R";
    UInt64 part_count = 0;
    StateClock clock;

    std::shared_ptr<SnapshotState> newSnapshot(std::shared_ptr<DiskMaskSnapshot> masks)
    {
        auto result = std::make_shared<SnapshotState>();
        result->disk_masks = std::move(masks);
        return result;
    }

    void recover()
    {
        namespace fs = std::filesystem;
        part_count = database->integer("part_count");
        rocksdb::ReadOptions options;
        options.fill_cache = false;
        std::unique_ptr<rocksdb::Iterator> iterator(database->db->NewIterator(options));
        UInt64 found = 0;
        for (iterator->Seek("A"); iterator->Valid() && iterator->key().starts_with("A"); iterator->Next())
        {
            auto record = decodePartCommit(iterator->value().ToStringView());
            if (!record.rows || iterator->key().ToString() != "A" + record.final_name)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree catalog part identity mismatch");
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
            const auto offsets = mask->toVector();
            if (!offsets.empty() && offsets.back() >= record.rows)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree recovered mask exceeds its part");
            if (!final_exists)
            {
                fs::rename(temporary, final);
                syncDirectory(table_path);
            }
            ++found;
        }
        check(iterator->status());
        if (found != part_count)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree durable part count differs from its catalog");
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
        auto masks = std::make_shared<DiskMaskSnapshot>(database);
        masks->bind();
        current = newSnapshot(std::move(masks));
    }
};

DiskState::DiskState(const String & table_path, ObjectID incarnation, bool attach, StateClock clock) : impl(std::make_unique<Impl>())
{
    namespace fs = std::filesystem;
    impl->table_path = table_path;
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
    options.wal_recovery_mode = rocksdb::WALRecoveryMode::kAbsoluteConsistency;
    options.use_fsync = true;
    options.max_open_files = 128;
    options.write_buffer_size = 16U << 20;
    options.max_write_buffer_number = 3;
    options.max_total_wal_size = 64U << 20;
    options.max_manifest_file_size = 16U << 20;
    options.keep_log_file_num = 2;
    options.max_background_jobs = 2;
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = rocksdb::NewLRUCache(64U << 20);
    table_options.cache_index_and_filter_blocks = true;
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

bool DiskState::tokenAlreadyCommitted(const String & partition, const Token & token) const
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "UpsertMergeTree deduplication token was reused with a different evaluated block");
    return true;
}

StateStore::Prepared DiskState::prepare(const String & partition, PartCommit part, const ValidatedPartRows & validated)
{
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
        batch.Put("M" + objectName(id), encodeMask(mask));
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
        batch.Put("M" + objectName(part.part), encodeMask(*dead));
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
    const String & partition, PartCommit part, const PartRows & rows, const std::vector<String> & sources)
{
    if (sources.empty() || sources.size() > 32 || part.rows != rows.size() || rows.size() > MAX_UNIT_ROWS
        || part.part == ObjectID{} || impl->database->get("O" + objectName(part.part)))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid UpsertMergeTree maintenance unit");
    StateStore::Prepared result;
    result.partition = partition;
    result.operation_id = part.part;
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
    std::set<String> source_names;
    std::set<String> mapped_output;
    for (const auto & name : sources)
    {
        if (!source_names.insert(name).second || name == part.final_name)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Duplicate UpsertMergeTree merge source or reused output name");
        auto authorization = impl->database->get("A" + name);
        if (!authorization)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge source is no longer active");
        const auto source = decodePartCommit(*authorization);
        const auto info = MergeTreePartInfo::fromPartName(name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
        if (info.getPartitionId() != partition)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge crosses SQL partitions");
        const auto source_rows = loadPartRows(name);
        impl->database->forEachWinner(partition, source_rows, [&](const PartRow & row, const String & key, std::optional<std::string_view> value)
        {
            if (!value)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge source has no durable winner");
            BinaryReader in{*value};
            const auto winner_part = in.id();
            const auto winner = decodeIndexValue(in.string());
            in.end();
            auto it = output.find(row.key);
            const bool maps_to_output = it != output.end() && it->second->value.row == row.value.row;
            if (winner_part == source.part && winner.row_offset == row.value.row_offset)
            {
                if (!maps_to_output || winner.row != row.value.row)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge omitted or changed a live source identity");
                BinaryWriter updated;
                updated.id(part.part);
                updated.string(encodeIndexValue(it->second->value));
                batch.Put(key, updated.bytes);
                checkBatchBudget(batch);
            }
            else if (maps_to_output)
            {
                /// A concurrent upsert superseded this exact source row after the merge snapshot.
                dead->add(it->second->value.row_offset);
            }
            if (maps_to_output && !mapped_output.insert(row.key).second)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge maps an output identity twice");
        });
        batch.Put("R" + name, *authorization);
        batch.Delete("A" + name);
        batch.Delete("M" + objectName(source.part));
        batch.Delete("O" + objectName(source.part));
    }
    if (mapped_output.size() != output.size())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge invented an output identity");
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
        batch.Put("M" + objectName(part.part), encodeMask(*dead));
        batch.Put("O" + objectName(part.part), number(result.csn));
    }
    else
        part = {};
    if (sources.size() > impl->part_count)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree merge retired more parts than exist");
    batch.Put("part_count", number(impl->part_count - sources.size() + result.produces_part));
    batch.Put(csn_key, number(result.csn));
    result.record = std::move(part);
    checkBatchBudget(batch);
    result.disk->masks = std::make_shared<DiskMaskSnapshot>(impl->database);
    result.snapshot = impl->newSnapshot(result.disk->masks);
    return result;
}

void DiskState::append(const StateStore::Prepared & prepared)
{
    if (!prepared.disk || prepared.disk->durable)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid or already committed UpsertMergeTree disk preparation");
    const auto key = prefix('C', prepared.partition);
    auto current = impl->database->get(key) ? impl->database->integer(key) : 0;
    if (current != prepared.disk->previous_csn)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree commit sequence changed under the commit lock");
    rocksdb::WriteOptions options;
    options.sync = true;
    check(impl->database->db->Write(options, &prepared.disk->batch));
    prepared.disk->durable = true;
    fiu_do_on(FailPoints::upsert_log_throw_after_append,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree failure after disk-state batch write");
    });
    fiu_do_on(FailPoints::upsert_log_throw_after_sync,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree failure after disk-state sync");
    });
    prepared.disk->masks->bind();
}

void DiskState::publish(StateStore::Prepared & prepared) noexcept
{
    impl->current.swap(prepared.snapshot);
    impl->part_count = impl->part_count - prepared.retired_parts.size() + prepared.produces_part;
}
#else
struct DiskState::Impl
{
};
struct DiskPrepared
{
};
DiskState::DiskState(const String &, ObjectID, bool, StateClock)
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
bool DiskState::tokenAlreadyCommitted(const String &, const Token &) const
{
    std::unreachable();
}
StateStore::Prepared DiskState::prepare(const String &, PartCommit, const ValidatedPartRows &)
{
    std::unreachable();
}
StateStore::Prepared DiskState::prepareMerge(const String &, PartCommit, const PartRows &, const std::vector<String> &)
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
