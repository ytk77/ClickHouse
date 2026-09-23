#include <Storages/MergeTree/UniqueKey/UpsertStateStore.h>
#include <Storages/MergeTree/UniqueKey/UpsertDiskState.h>
#include <Storages/MergeTree/UniqueKey/UpsertBinary.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Common/Exception.h>
#include <filesystem>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

namespace DB::Upsert
{
namespace
{
void validateName(const String & name, bool temporary)
{
    if (name.empty() || name.size() > 240 || name == "." || name == ".." || name.find('/') != String::npos
        || name.find('\\') != String::npos || name.find('\0') != String::npos
        || (temporary && !name.starts_with("tmp_upsert_"))
        || (!temporary && !MergeTreePartInfo::tryParsePartName(name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree part directory name");
}
String partitionFile(const String & partition)
{
    return "partition_" + objectName(hashLogRecord(partition)) + ".log";
}
}

String encodePartCommit(const PartCommit & record)
{
    BinaryWriter out;
    out.number(1, 1);
    out.id(record.part);
    out.string(record.temporary_name);
    out.string(record.final_name);
    out.id(record.files_hash);
    out.number(record.rows);
    out.number(record.dead.size(), 4);
    for (auto offset : record.dead)
        out.number(offset, 4);
    out.number(record.kills.size(), 4);
    for (const auto & kill : record.kills)
    {
        out.id(kill.part);
        out.number(kill.offset, 4);
    }
    out.string(record.token.client_token);
    out.number(record.token.unit);
    out.id(record.token.payload_hash);
    out.number(record.token.accepted);
    if (out.bytes.size() > MAX_RECORD_BYTES - 2048)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree commit payload exceeds its byte limit");
    return std::move(out.bytes);
}

PartCommit decodePartCommit(std::string_view bytes)
{
    BinaryReader in{bytes};
    if (in.number(1) != 1)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown UpsertMergeTree commit payload version");
    PartCommit result;
    result.part = in.id();
    result.temporary_name = in.string(240);
    result.final_name = in.string(240);
    result.files_hash = in.id();
    result.rows = in.number();
    auto dead = in.count(MAX_UNIT_ROWS, 4);
    result.dead.reserve(dead);
    for (size_t i = 0; i < dead; ++i)
    {
        auto offset = static_cast<UInt32>(in.number(4));
        if (offset >= result.rows || (!result.dead.empty() && offset <= result.dead.back()))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree output deletion offset");
        result.dead.push_back(offset);
    }
    auto kills = in.count(MAX_UNIT_ROWS, 20);
    result.kills.reserve(kills);
    for (size_t i = 0; i < kills; ++i)
    {
        Kill kill{in.id(), static_cast<UInt32>(in.number(4))};
        if (kill.part == ObjectID{} || (!result.kills.empty() && kill <= result.kills.back()))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree kill identity or ordering");
        result.kills.push_back(kill);
    }
    result.token.client_token = in.string(1024);
    result.token.unit = in.number();
    result.token.payload_hash = in.id();
    result.token.accepted = in.number();
    in.end();
    if (!result.rows)
    {
        if (result.part != ObjectID{} || !result.temporary_name.empty() || !result.final_name.empty()
            || result.files_hash != ObjectID{} || !result.dead.empty() || !result.kills.empty() || result.token.accepted)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree no-op payload");
    }
    else
    {
        validateName(result.temporary_name, true);
        validateName(result.final_name, false);
        if (result.part == ObjectID{} || result.files_hash == ObjectID{} || result.rows > MAX_UNIT_ROWS
            || result.token.accepted != result.rows - result.dead.size() || !result.token.accepted)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree part authorization");
    }
    return result;
}

StateStore::StateStore(String table_path_, ObjectID incarnation_, bool attach, bool use_disk_state, StateClock clock)
    : table_path(std::move(table_path_)), state_path((std::filesystem::path(table_path) / "upsert_state").string()), incarnation(incarnation_)
{
    if (use_disk_state)
    {
        disk = std::make_unique<DiskState>(table_path, incarnation, attach, std::move(clock));
        return;
    }
    namespace fs = std::filesystem;
    if (!attach)
    {
        if (!fs::create_directory(state_path))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree state directory already exists on CREATE");
        syncDirectory(table_path);
    }
    registry = std::make_unique<PartitionLog>(fs::path(state_path) / "registry.log", incarnation, "partition_registry", !attach, 1U << 20);
    for (const auto & record : registry->recover())
    {
        if (record.kind != RecordKind::Noop || record.payload.empty() || record.payload.size() > MAX_PARTITION_ID_BYTES
            || partitions.contains(record.payload) || partitions.size() >= MAX_PARTITIONS)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree partition registry entry");
        auto & entry = partitions[record.payload];
        entry.log = std::make_unique<PartitionLog>(fs::path(state_path) / partitionFile(record.payload), incarnation, record.payload, false);
        recoverPartition(record.payload, entry);
    }
    for (const auto & entry : fs::directory_iterator(table_path))
    {
        const auto name = entry.path().filename().string();
        if (MergeTreePartInfo::tryParsePartName(name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
            && !authorized_parts.contains(name))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree found an unauthorized final part {} before native loading", name);
    }
}

StateStore::~StateStore() = default;

SnapshotPtr StateStore::snapshot() const
{
    return disk ? disk->snapshot() : current_snapshot;
}

const std::map<String, ObjectID> & StateStore::authorizedParts() const
{
    if (disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Disk-state UpsertMergeTree requires paged catalog access");
    return authorized_parts;
}

std::optional<ObjectID> StateStore::authorizedPart(const String & name) const
{
    if (disk)
        return disk->authorizedPart(name);
    auto it = authorized_parts.find(name);
    return it == authorized_parts.end() ? std::nullopt : std::optional<ObjectID>(it->second);
}

UInt64 StateStore::authorizedPartCount() const
{
    return disk ? disk->authorizedPartCount() : authorized_parts.size();
}

void StateStore::checkAdmission() const
{
    if (disk)
    {
        disk->checkAdmission();
        return;
    }
    if (part_count >= MAX_PARTS)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree parts-only limit of {} parts reached; merge is not enabled", MAX_PARTS);
}

StateStore::Partition & StateStore::getOrCreatePartition(const String & partition)
{
    if (auto it = partitions.find(partition); it != partitions.end())
        return it->second;
    if (partitions.size() >= MAX_PARTITIONS)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree partition limit reached");
    const auto path = std::filesystem::path(state_path) / partitionFile(partition);
    /// An unregistered bootstrap log cannot contain an acknowledged operation.
    if (std::filesystem::exists(path))
    {
        if (!readBoundedFile(path, 0).empty())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Nonempty unregistered UpsertMergeTree partition log");
        std::filesystem::remove(path);
        syncDirectory(state_path);
    }
    try
    {
        auto & entry = partitions[partition];
        entry.log = std::make_unique<PartitionLog>(path, incarnation, partition, true);
        entry.log->recover();
        registry->append(RecordKind::Noop, newObjectID(), partition);
        return entry;
    }
    catch (...)
    {
        fenced = true;
        throw;
    }
}

bool StateStore::tokenAlreadyCommitted(const String & partition, const Token & token) const
{
    if (disk)
        return disk->tokenAlreadyCommitted(partition, token);
    if (token.client_token.empty())
        return false;
    auto it = partitions.find(partition);
    if (it == partitions.end())
        return false;
    const auto & tokens = it->second.state->tokens;
    auto old = tokens.find({token.client_token, token.unit});
    if (old == tokens.end())
        return false;
    if (old->second.payload_hash != token.payload_hash)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "UpsertMergeTree deduplication token was reused with a different evaluated block");
    return true;
}

StateStore::Prepared StateStore::decide(const String & partition, PartCommit part, const PartRows & rows, UInt64 csn)
{
    auto & current = *partitions.at(partition).state;
    if (csn <= current.csn || part.rows != rows.size())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree commit sequence or part row count mismatch");
    Prepared result;
    result.partition = partition;
    result.operation_id = part.part;
    result.csn = csn;
    result.state = std::make_shared<PartitionState>(current);
    result.state->csn = csn;
    auto snapshot = std::make_shared<SnapshotState>(*current_snapshot);
    snapshot->partition_csns[partition] = csn;
    auto dead = std::make_shared<DeleteBitmap>();
    part.dead.clear();
    part.kills.clear();
    std::set<String> unique;
    for (const auto & row : rows)
    {
        if (row.value.row.operation_id != part.part || !row.value.row.self_binding || !unique.insert(row.key).second)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree part has invalid lineage or duplicate keys");
        auto candidate = row.value;
        candidate.row = candidate.row.resolve(csn);
        auto it = result.state->winners.find(row.key);
        if (it != result.state->winners.end() && candidate.row.compareWinner(it->second.value.row) < 0)
        {
            part.dead.push_back(candidate.row_offset);
            dead->add(candidate.row_offset);
            continue;
        }
        if (it != result.state->winners.end())
            part.kills.push_back({it->second.part, it->second.value.row_offset});
        else
            result.state->key_bytes += row.key.size() + sizeof(Winner) + 96;
        result.state->winners[row.key] = Winner{part.part, candidate};
    }
    if (result.state->winners.size() > MAX_LIVE_KEYS || result.state->key_bytes > MAX_KEY_BYTES)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree live-key admission limit reached");
    std::sort(part.kills.begin(), part.kills.end());
    std::map<ObjectID, std::shared_ptr<DeleteBitmap>> changed;
    for (const auto & kill : part.kills)
    {
        auto & bitmap = changed[kill.part];
        if (!bitmap)
        {
            bitmap = std::make_shared<DeleteBitmap>();
            bitmap->merge(*snapshot->part_masks.at(objectName(kill.part)));
        }
        if (bitmap->contains(kill.offset))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree attempted to kill an already dead winner");
        bitmap->add(kill.offset);
    }
    for (const auto & [id, bitmap] : changed)
        snapshot->part_masks[objectName(id)] = bitmap;
    part.token.accepted = part.rows - part.dead.size();
    result.produces_part = part.token.accepted != 0;
    if (result.produces_part)
        snapshot->part_masks.emplace(objectName(part.part), std::move(dead));
    else
    {
        Token token = std::move(part.token);
        part = {};
        part.token = std::move(token);
    }
    if (!part.token.client_token.empty())
    {
        if (part.token.client_token.size() > 1024 || result.state->tokens.size() >= MAX_TOKENS)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree token admission limit reached; tokens do not expire in this bounded version");
        if (!result.state->tokens.emplace(std::make_pair(part.token.client_token, part.token.unit), part.token).second)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Duplicate UpsertMergeTree token terminal record");
    }
    result.record = std::move(part);
    result.payload = encodePartCommit(result.record);
    result.snapshot = std::move(snapshot);
    return result;
}

StateStore::Prepared StateStore::prepare(const String & partition, PartCommit part, const PartRows & rows)
{
    if (tokenAlreadyCommitted(partition, part.token))
    {
        Prepared replay;
        replay.replayed_token = true;
        return replay;
    }
    ValidatedPartRows validated(rows, part.part);
    return prepare(partition, std::move(part), validated);
}

StateStore::Prepared StateStore::prepare(const String & partition, PartCommit part, const ValidatedPartRows & validated)
{
    if (part.part != validated.getPart() || part.rows != validated.getRows().size())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree input validation identity mismatch");
    const auto & rows = validated.getRows();
    if (disk)
        return disk->prepare(partition, std::move(part), validated);
    auto & entry = getOrCreatePartition(partition);
    if (tokenAlreadyCommitted(partition, part.token))
    {
        Prepared result;
        result.replayed_token = true;
        return result;
    }
    checkAdmission();
    if (operation_ids.contains(part.part))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Reused UpsertMergeTree operation identity");
    auto prepared = decide(partition, std::move(part), rows, entry.log->nextCSN());
    operation_ids.insert(prepared.operation_id);
    return prepared;
}

void StateStore::append(const Prepared & prepared)
{
    if (disk)
    {
        try
        {
            disk->append(prepared);
        }
        catch (...)
        {
            fenced = true;
            throw;
        }
        return;
    }
    const auto csn = partitions.at(prepared.partition).log->append(
        prepared.produces_part ? RecordKind::Part : RecordKind::Noop, prepared.operation_id, prepared.payload);
    if (csn != prepared.csn)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree commit sequence changed under the commit lock");
}

StateStore::Prepared StateStore::prepareMerge(
    const String & partition, PartCommit part, const PartRows & rows, const std::vector<String> & sources)
{
    if (!disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Legacy UpsertMergeTree does not authorize reclamation");
    return disk->prepareMerge(partition, std::move(part), rows, sources);
}

PartRows StateStore::loadPartRows(const String & name) const
{
    if (!disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Legacy UpsertMergeTree does not authorize maintenance reads");
    return disk->loadPartRows(name);
}

void StateStore::pruneRetired()
{
    if (!disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Legacy UpsertMergeTree does not authorize reclamation");
    disk->pruneRetired();
}

void StateStore::publish(Prepared & prepared) noexcept
{
    if (disk)
    {
        disk->publish(prepared);
        return;
    }
    partitions.at(prepared.partition).state.swap(prepared.state);
    current_snapshot.swap(prepared.snapshot);
    part_count += prepared.produces_part;
}

void StateStore::recoverPartition(const String & partition, Partition & entry)
{
    namespace fs = std::filesystem;
    for (const auto & terminal : entry.log->recover())
    {
        if (terminal.kind != RecordKind::Part && terminal.kind != RecordKind::Noop)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unsupported operation in parts-only UpsertMergeTree log");
        if (!operation_ids.insert(terminal.operation_id).second)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Reused UpsertMergeTree operation identity in replay");
        auto record = decodePartCommit(terminal.payload);
        if ((terminal.kind == RecordKind::Part) != (record.rows != 0))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree terminal kind does not match its payload");
        PartRows rows;
        if (record.rows)
        {
            if (record.part != terminal.operation_id || part_count >= MAX_PARTS)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree part identity or part count");
            auto info = MergeTreePartInfo::fromPartName(record.final_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
            if (info.getPartitionId() != partition || info.level != 0)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree authorized part has an invalid partition or level");
            const fs::path final = fs::path(table_path) / record.final_name;
            const fs::path temporary = fs::path(table_path) / record.temporary_name;
            const bool final_exists = fs::exists(final);
            if (final_exists == fs::exists(temporary))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree requires exactly one authorized payload for {}", record.final_name);
            const auto path = final_exists ? final : temporary;
            if (hashPartDirectory(path, false) != record.files_hash)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree authenticated files mismatch for {}", record.final_name);
            rows = decodePartRows(readBoundedFile(path / ROWS_FILE, MAX_RECORD_BYTES));
            if (!final_exists)
            {
                fs::rename(temporary, final);
                syncDirectory(table_path);
            }
            if (!authorized_parts.emplace(record.final_name, record.part).second)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Duplicate authorized UpsertMergeTree part name");
        }
        const auto expected_dead = record.dead;
        const auto expected_kills = record.kills;
        const auto expected_accepted = record.token.accepted;
        auto prepared = decide(partition, std::move(record), rows, terminal.csn);
        if (prepared.record.dead != expected_dead || prepared.record.kills != expected_kills || prepared.record.token.accepted != expected_accepted)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree recorded visibility disagrees with its original winner tuples");
        publish(prepared);
    }
}
}
