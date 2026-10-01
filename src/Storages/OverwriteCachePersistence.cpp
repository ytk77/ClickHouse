#include <Storages/OverwriteCachePersistence.h>
#include <Storages/OverwriteCacheWait.h>
#include <Common/FailPoint.h>

#include <Columns/IColumn.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace FailPoints
{
    extern const char overwrite_cache_pause_before_persist[];
}

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_RESTORE_TABLE;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The log is framed rather than plainly appended, so that the torn tail an `Async` crash leaves behind
/// is recognizable instead of being read as a record.
constexpr std::string_view manifest_magic = "OverwriteCacheManifest\n";
constexpr std::string_view segment_file_suffix = ".seg";
constexpr std::string_view tmp_directory = "tmp";

/// Written revision-independently: the file has to outlive the server that produced it.
constexpr UInt64 native_revision = 0;

}

OverwriteCachePersistMode parseOverwriteCachePersistMode(const String & value)
{
    if (value == "none")
        return OverwriteCachePersistMode::None;
    if (value == "async")
        return OverwriteCachePersistMode::Async;
    if (value == "sync")
        return OverwriteCachePersistMode::Sync;
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS, "Setting `persist_mode` of storage `OverwriteCache` must be 'none', 'async' or 'sync'");
}

std::string_view toString(OverwriteCachePersistMode mode)
{
    switch (mode)
    {
        case OverwriteCachePersistMode::None:
            return "none";
        case OverwriteCachePersistMode::Async:
            return "async";
        case OverwriteCachePersistMode::Sync:
            return "sync";
    }
}

OverwriteCachePersistence::OverwriteCachePersistence(
    OverwriteCachePersistMode mode_, DiskPtr disk_, String path_, Block header_, String fingerprint_, String log_name_)
    : mode(mode_)
    , disk(std::move(disk_))
    , header(std::move(header_))
    , shared_header(std::make_shared<const Block>(header.cloneEmpty()))
    , fingerprint(std::move(fingerprint_))
    , log(getLogger(log_name_))
    , path(std::move(path_))
{
    if (!isEnabled())
        return;
    if (!disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Storage `OverwriteCache` requires a disk to persist data");
    if (path.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage `OverwriteCache` requires a data path to persist data");
}

OverwriteCachePersistence::~OverwriteCachePersistence()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to shut down `OverwriteCache` persistence");
    }
}

String OverwriteCachePersistence::getPath() const
{
    std::lock_guard lock(path_mutex);
    return path;
}

String OverwriteCachePersistence::segmentFileName(UInt64 segment_id) const
{
    return fmt::format("{:020}{}", segment_id, segment_file_suffix);
}

void OverwriteCachePersistence::createDirectories()
{
    const auto table_path = getPath();
    disk->createDirectories(fs::path(table_path) / tmp_directory);
}

void OverwriteCachePersistence::openManifest(bool rewrite)
{
    const auto manifest_path = fs::path(getPath()) / manifest_file_name;
    if (rewrite || !disk->existsFile(manifest_path))
    {
        manifest_buffer = disk->writeFile(manifest_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
        writeString(manifest_magic, *manifest_buffer);
        writeBinaryLittleEndian(format_version, *manifest_buffer);
        writeStringBinary(fingerprint, *manifest_buffer);
        manifest_buffer->next();
        manifest_buffer->sync();
        return;
    }
    manifest_buffer = disk->writeFile(manifest_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
}

void OverwriteCachePersistence::closeManifest()
{
    if (!manifest_buffer)
        return;
    manifest_buffer->finalize();
    manifest_buffer.reset();
}

void OverwriteCachePersistence::load(const std::function<void(LoadedRecord &&)> & apply)
{
    if (!isEnabled())
        return;

    createDirectories();
    const auto table_path = getPath();
    const auto manifest_path = fs::path(table_path) / manifest_file_name;
    if (!disk->existsFile(manifest_path))
    {
        /// A fresh table, or one whose log was removed. Any segment file left behind belongs to no log.
        for (auto it = disk->iterateDirectory(table_path); it->isValid(); it->next())
        {
            if (it->name().ends_with(segment_file_suffix))
                disk->removeFileIfExists(fs::path(table_path) / it->name());
        }
        return;
    }

    std::unordered_set<UInt64> removed_segments;
    UInt64 valid_bytes = 0;
    const auto records = readManifest(removed_segments, &valid_bytes);

    UInt64 max_segment_id = 0;
    size_t applied_rows = 0;
    size_t deletions = 0;
    std::vector<Record::Segment> loaded_segments;
    std::unordered_set<UInt64> loaded_segment_ids;
    for (const auto & record : records)
    {
        for (const auto & segment : record.added)
            max_segment_id = std::max(max_segment_id, segment.segment_id);

        if (!record.deleted_keys.empty())
        {
            deletions += record.deleted_keys.size();
            LoadedRecord loaded;
            loaded.deleted_keys = record.deleted_keys;
            apply(std::move(loaded));
        }

        for (const auto & segment : record.added)
        {
            if (removed_segments.contains(segment.segment_id))
                continue;

            const auto segment_path = fs::path(table_path) / segmentFileName(segment.segment_id);
            if (!disk->existsFile(segment_path))
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Segment file {} of storage `OverwriteCache` is referenced by {} but is missing",
                    segment_path.string(),
                    manifest_path.string());

            auto file_buffer = disk->readFile(segment_path, getReadSettings());
            CompressedReadBuffer compressed(*file_buffer);
            NativeReader reader(compressed, header, native_revision);
            Block block = reader.read();
            if (block.rows() != segment.rows)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Segment file {} of storage `OverwriteCache` holds {} rows, but {} records {}",
                    segment_path.string(),
                    block.rows(),
                    manifest_path.string(),
                    segment.rows);

            LoadedRecord loaded;
            loaded.segment_id = segment.segment_id;
            loaded.block = std::move(block);
            applied_rows += loaded.block.rows();
            apply(std::move(loaded));

            loaded_segments.push_back(segment);
            loaded_segment_ids.emplace(segment.segment_id);
        }
    }

    if (valid_bytes != disk->getFileSize(manifest_path))
    {
        std::lock_guard state_lock(state_mutex);
        writeManifest(records);
        closeManifest();
    }

    next_segment_id.store(max_segment_id + 1, std::memory_order_relaxed);

    /// A crash between writing a segment and recording it, or between recording a retirement and
    /// deleting the file, leaves a file no live record refers to.
    size_t orphan_files = 0;
    for (auto it = disk->iterateDirectory(table_path); it->isValid(); it->next())
    {
        const auto & name = it->name();
        if (!name.ends_with(segment_file_suffix))
            continue;
        UInt64 segment_id = 0;
        if (!tryParse(segment_id, std::string_view{name}.substr(0, name.size() - segment_file_suffix.size()))
            || !loaded_segment_ids.contains(segment_id))
        {
            disk->removeFileIfExists(fs::path(table_path) / name);
            ++orphan_files;
        }
    }

    LOG_INFO(
        log,
        "Loaded {} segments with {} rows and {} deletions from {} log records, removed {} orphan files",
        loaded_segments.size(),
        applied_rows,
        deletions,
        records.size(),
        orphan_files);

    std::lock_guard state_lock(state_mutex);
    live_segments = std::move(loaded_segments);
    live_segment_ids = std::move(loaded_segment_ids);
    /// Existing records are a baseline, not new work since this instance last checkpointed.
    records_since_checkpoint = 0;
    bytes_since_checkpoint = 0;
    checkpoint_bytes = disk->getFileSize(manifest_path);
}

std::vector<OverwriteCachePersistence::Record>
OverwriteCachePersistence::readManifest(std::unordered_set<UInt64> & removed_segments, UInt64 * valid_bytes) const
{
    const auto manifest_path = fs::path(getPath()) / manifest_file_name;
    auto file_buffer = disk->readFile(manifest_path, getReadSettings());

    String magic;
    magic.resize(manifest_magic.size());
    file_buffer->readStrict(magic.data(), magic.size());
    if (magic != manifest_magic)
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "File {} is not an `OverwriteCache` manifest", manifest_path.string());

    UInt8 version = 0;
    readBinaryLittleEndian(version, *file_buffer);
    if (version != format_version)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Manifest {} of storage `OverwriteCache` has format version {}, but this server writes version {}",
            manifest_path.string(),
            static_cast<UInt16>(version),
            static_cast<UInt16>(format_version));

    String stored_fingerprint;
    readStringBinary(stored_fingerprint, *file_buffer);
    if (stored_fingerprint != fingerprint)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Persisted data of storage `OverwriteCache` was written for a different table definition. "
            "Stored: {}. Current: {}. Attaching would silently reinterpret the stored rows",
            stored_fingerprint,
            fingerprint);

    std::vector<Record> records;
    const UInt64 file_size = disk->getFileSize(manifest_path);
    UInt64 valid_end = file_buffer->count();
    bool truncated = false;
    while (!file_buffer->eof())
    {
        UInt64 payload_size = 0;
        UInt128 expected_checksum{};
        String payload;
        constexpr size_t frame_size = sizeof(UInt64) + sizeof(UInt128);
        if (file_size - file_buffer->count() < frame_size)
        {
            truncated = true;
            break;
        }
        readBinaryLittleEndian(payload_size, *file_buffer);
        readPODBinary(expected_checksum, *file_buffer);
        if (payload_size > file_size - file_buffer->count())
        {
            truncated = true;
            break;
        }
        payload.resize(payload_size);
        file_buffer->readStrict(payload.data(), payload_size);
        if (sipHash128(payload.data(), payload.size()) != expected_checksum)
        {
            if (payload_size == 0 && expected_checksum == UInt128{})
            {
                char buffer[4096];
                while (!file_buffer->eof())
                {
                    const auto count = file_buffer->read(buffer, sizeof(buffer));
                    if (std::any_of(buffer, buffer + count, [](char value) { return value != 0; }))
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Nonzero data follows an invalid `OverwriteCache` manifest frame");
                }
                truncated = true;
                break;
            }
            throw Exception(ErrorCodes::INCORRECT_DATA, "Checksum mismatch in `OverwriteCache` manifest {}", manifest_path.string());
        }

        ReadBufferFromString payload_buffer(payload);
        Record record;
        readBinaryLittleEndian(record.generation, payload_buffer);

        UInt64 count = 0;
        readBinaryLittleEndian(count, payload_buffer);
        if (count > (payload.size() - payload_buffer.count()) / 16)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid segment count in `OverwriteCache` manifest");
        record.added.resize(count);
        for (auto & segment : record.added)
        {
            readBinaryLittleEndian(segment.segment_id, payload_buffer);
            readBinaryLittleEndian(segment.rows, payload_buffer);
        }

        readBinaryLittleEndian(count, payload_buffer);
        if (count > (payload.size() - payload_buffer.count()) / 8)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid retirement count in `OverwriteCache` manifest");
        record.removed.resize(count);
        for (auto & segment_id : record.removed)
        {
            readBinaryLittleEndian(segment_id, payload_buffer);
            removed_segments.emplace(segment_id);
        }

        readBinaryLittleEndian(count, payload_buffer);
        if (count > payload.size() - payload_buffer.count())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid deletion count in `OverwriteCache` manifest");
        record.deleted_keys.resize(count);
        for (auto & key : record.deleted_keys)
            readStringBinary(key, payload_buffer);

        if (!payload_buffer.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Trailing bytes in `OverwriteCache` manifest record");
        records.push_back(std::move(record));
        valid_end = file_buffer->count();
    }

    if (valid_bytes)
        *valid_bytes = valid_end;
    else if (truncated)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Incomplete active `OverwriteCache` manifest");

    if (truncated)
        LOG_WARNING(
            log,
            "Manifest {} of storage `OverwriteCache` ends with an incomplete record. The publications it "
            "covered were not durable and are dropped",
            manifest_path.string());

    return records;
}

void OverwriteCachePersistence::start()
{
    if (!isEnabled())
        return;

    {
        std::lock_guard state_lock(state_mutex);
        openManifest(/*rewrite=*/false);
    }

    std::lock_guard lock(mutex);
    if (writer_thread)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Persistence of storage `OverwriteCache` is already started");
    started = true;
    stopped = false;
    writer_exception = {};
    writer_thread.emplace([this] { writerThread(); });
}

UInt64 OverwriteCachePersistence::Commit::allocatedBytes() const
{
    UInt64 bytes = sizeof(Commit) + added.capacity() * sizeof(AddedSegment)
        + removed.capacity() * sizeof(UInt64) + deleted_keys.capacity() * sizeof(String);
    for (const auto & segment : added)
    {
        bytes += segment.columns.capacity() * sizeof(ColumnPtr);
        for (const auto & column : segment.columns)
            bytes += column->allocatedBytes();
    }
    for (const auto & key : deleted_keys)
        bytes += key.capacity();
    return bytes;
}

UInt64 OverwriteCachePersistence::enqueue(Commit && commit)
{
    if (!isEnabled())
        return 0;
    if (commit.added.empty() && commit.removed.empty() && commit.deleted_keys.empty())
        return 0;

    const UInt64 bytes = commit.allocatedBytes();

    std::unique_lock lock(mutex);
    if (writer_exception)
        std::rethrow_exception(writer_exception);
    if (!started)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Persistence of storage `OverwriteCache` is not started");

    /// Include the incoming commit, including deletion keys. A single oversized commit may proceed
    /// only with an empty queue, otherwise a block larger than the limit could never make progress.
    waitForOverwriteCache(queue_changed, lock, [&]() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return stopped || writer_exception || queued_bytes == 0
            || (bytes <= max_queued_bytes && queued_bytes <= max_queued_bytes - bytes);
    }, ProfileEvents::OverwriteCachePersistenceQueueWaits);
    if (writer_exception)
        std::rethrow_exception(writer_exception);
    if (stopped)
        throw Exception(ErrorCodes::ABORTED, "Persistence of storage `OverwriteCache` is stopped");

    const UInt64 sequence = next_sequence;
    queue.emplace_back(sequence, std::move(commit));
    ++next_sequence;
    queued_bytes += bytes;
    queue_changed.notify_all();
    return sequence;
}

void OverwriteCachePersistence::waitDurable(UInt64 sequence)
{
    if (!isEnabled() || !sequence)
        return;

    std::unique_lock lock(mutex);
    waitForOverwriteCache(durable_changed, lock, [&]() TSA_NO_THREAD_SAFETY_ANALYSIS
    { return stopped || writer_exception || durable_sequence >= sequence; }, ProfileEvents::OverwriteCachePersistenceDurableWaits);
    if (writer_exception)
        std::rethrow_exception(writer_exception);
    /// The caller asked for a durability guarantee, so a shutdown that races the wait is an error rather
    /// than something to pass over quietly.
    if (durable_sequence < sequence)
        throw Exception(
            ErrorCodes::ABORTED, "Persistence of storage `OverwriteCache` stopped before the publication was durable");
}

void OverwriteCachePersistence::setException()
{
    std::lock_guard lock(mutex);
    if (!writer_exception)
        writer_exception = std::current_exception();
    queue_changed.notify_all();
    durable_changed.notify_all();
}

void OverwriteCachePersistence::writerThread()
{
    setThreadName(ThreadName::OVERWRITE_CACHE_PERSIST);

    while (true)
    {
        UInt64 sequence = 0;
        Commit commit;
        UInt64 bytes = 0;
        {
            std::unique_lock lock(mutex);
            queue_changed.wait(lock, [&]() TSA_NO_THREAD_SAFETY_ANALYSIS { return stopped || !queue.empty(); });
            if (queue.empty())
                return;
            sequence = queue.front().first;
            commit = std::move(queue.front().second);
            queue.pop_front();
            bytes = commit.allocatedBytes();
        }

        try
        {
            FailPointInjection::pauseFailPoint(FailPoints::overwrite_cache_pause_before_persist);
            writeCommit(commit);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to persist an `OverwriteCache` publication");
            setException();
            return;
        }

        {
            std::lock_guard lock(mutex);
            queued_bytes -= std::min(queued_bytes, bytes);
            durable_sequence = sequence;
            queue_changed.notify_all();
            durable_changed.notify_all();
        }
    }
}

void OverwriteCachePersistence::writeCommit(const Commit & commit)
{
    std::lock_guard state_lock(state_mutex);
    if (!manifest_buffer)
        throw Exception(ErrorCodes::INCORRECT_DATA, "`OverwriteCache` manifest writer is closed");
    /// A segment file has to be durable before the record that claims it exists, and a retired file may
    /// only be deleted once the record that stops referring to it is durable.
    for (const auto & segment : commit.added)
        writeSegmentFile(segment);

    Record record;
    record.generation = commit.generation;
    record.added.reserve(commit.added.size());
    for (const auto & segment : commit.added)
        record.added.push_back({segment.segment_id, segment.rows});
    record.removed = commit.removed;
    record.deleted_keys = commit.deleted_keys;

    {
        bytes_since_checkpoint += appendManifestRecord(record, *manifest_buffer);
        manifest_buffer->next();
        manifest_buffer->sync();

        for (const auto & segment : record.added)
        {
            live_segments.push_back(segment);
            live_segment_ids.emplace(segment.segment_id);
        }
        for (const auto segment_id : record.removed)
        {
            live_segment_ids.erase(segment_id);
            std::erase_if(live_segments, [&](const Record::Segment & segment) { return segment.segment_id == segment_id; });
        }
        ++records_since_checkpoint;

        if (needsCheckpoint())
            checkpointManifest();
    }

    removeSegmentFiles(record.removed);
}

void OverwriteCachePersistence::writeSegmentFile(const AddedSegment & segment)
{
    const auto table_path = getPath();
    const auto file_name = segmentFileName(segment.segment_id);
    const auto tmp_path = fs::path(table_path) / tmp_directory / file_name;

    Block block = header.cloneEmpty();
    if (block.columns() != segment.columns.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Row segment of storage `OverwriteCache` does not match the table header");
    for (size_t position = 0; position < segment.columns.size(); ++position)
        block.getByPosition(position).column = segment.columns[position]->decompress();

    {
        auto file_buffer = disk->writeFile(tmp_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
        CompressedWriteBuffer compressed(*file_buffer);
        NativeWriter writer(compressed, native_revision, shared_header);
        writer.write(block);
        writer.flush();
        compressed.finalize();
        file_buffer->next();
        file_buffer->sync();
        file_buffer->finalize();
    }

    /// A rename makes the file appear complete or not at all, so a crash never leaves a half-written
    /// segment where the manifest expects a whole one.
    auto directory_guard = disk->getDirectorySyncGuard(table_path);
    disk->replaceFile(tmp_path, fs::path(table_path) / file_name);
}

UInt64 OverwriteCachePersistence::appendManifestRecord(const Record & record, WriteBuffer & out)
{
    WriteBufferFromOwnString payload_buffer;
    writeBinaryLittleEndian(record.generation, payload_buffer);
    writeBinaryLittleEndian(static_cast<UInt64>(record.added.size()), payload_buffer);
    for (const auto & segment : record.added)
    {
        writeBinaryLittleEndian(segment.segment_id, payload_buffer);
        writeBinaryLittleEndian(segment.rows, payload_buffer);
    }
    writeBinaryLittleEndian(static_cast<UInt64>(record.removed.size()), payload_buffer);
    for (const auto segment_id : record.removed)
        writeBinaryLittleEndian(segment_id, payload_buffer);
    writeBinaryLittleEndian(static_cast<UInt64>(record.deleted_keys.size()), payload_buffer);
    for (const auto & key : record.deleted_keys)
        writeStringBinary(key, payload_buffer);

    const auto payload = payload_buffer.str();
    const auto checksum = sipHash128(payload.data(), payload.size());

    writeBinaryLittleEndian(static_cast<UInt64>(payload.size()), out);
    writePODBinary(checksum, out);
    out.write(payload.data(), payload.size());
    return sizeof(UInt64) + sizeof(checksum) + payload.size();
}

void OverwriteCachePersistence::removeSegmentFiles(const std::vector<UInt64> & segment_ids)
{
    if (segment_ids.empty())
        return;

    {
        std::lock_guard lock(mutex);
        if (backup_pins)
        {
            /// A backup already listed these files. Deleting them now would make it fail on a file it was
            /// told to copy, so the removal waits for the backup to finish.
            deferred_removals.insert(deferred_removals.end(), segment_ids.begin(), segment_ids.end());
            return;
        }
    }

    const auto table_path = getPath();
    for (const auto segment_id : segment_ids)
        disk->removeFileIfExists(fs::path(table_path) / segmentFileName(segment_id));
}

bool OverwriteCachePersistence::needsCheckpoint() const
{
    /// Retained tombstones are not new work. Both record and byte growth must justify another pass,
    /// so a large irreducible log cannot be rewritten repeatedly by a stream of tiny commits.
    return records_since_checkpoint > checkpoint_min_records
        && records_since_checkpoint > 2 * live_segments.size()
        && bytes_since_checkpoint >= checkpoint_bytes;
}

void OverwriteCachePersistence::checkpointManifest()
{
    std::unordered_set<UInt64> removed_segments;
    auto records = readManifest(removed_segments);

    /// Drop the added segments a later record retired, and the retirements themselves.
    std::vector<Record> compacted;
    compacted.reserve(records.size());
    bool seen_live_segment = false;
    size_t dropped_deletions = 0;
    for (auto & record : records)
    {
        Record kept;
        kept.generation = record.generation;
        for (const auto & segment : record.added)
        {
            if (!removed_segments.contains(segment.segment_id))
                kept.added.push_back(segment);
        }
        /// A deletion still shadows the key in every surviving segment written before it. One written
        /// before every surviving segment shadows nothing, because replay reaches it first.
        if (seen_live_segment)
            kept.deleted_keys = std::move(record.deleted_keys);
        else
            dropped_deletions += record.deleted_keys.size();
        seen_live_segment = seen_live_segment || !kept.added.empty();

        if (!kept.added.empty() || !kept.deleted_keys.empty())
            compacted.push_back(std::move(kept));
    }

    writeManifest(compacted);

    records_since_checkpoint = 0;
    bytes_since_checkpoint = 0;
    checkpoint_bytes = disk->getFileSize(fs::path(getPath()) / manifest_file_name);
    LOG_INFO(
        log,
        "Collapsed the `OverwriteCache` log from {} to {} records, describing {} segments, and dropped {} deletions that no "
        "surviving segment shadows",
        records.size(),
        compacted.size(),
        live_segments.size(),
        dropped_deletions);
}

void OverwriteCachePersistence::writeManifest(const std::vector<Record> & records)
{
    const auto table_path = getPath();
    const auto tmp_path = fs::path(table_path) / tmp_directory / manifest_file_name;
    {
        auto temporary = disk->writeFile(tmp_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
        writeString(manifest_magic, *temporary);
        writeBinaryLittleEndian(format_version, *temporary);
        writeStringBinary(fingerprint, *temporary);
        for (const auto & record : records)
            appendManifestRecord(record, *temporary);
        /// The temporary file is not visible until replacement; one durability barrier covers it all.
        temporary->next();
        temporary->sync();
        temporary->finalize();

    }

    closeManifest();
    auto directory_guard = disk->getDirectorySyncGuard(table_path);
    disk->replaceFile(tmp_path, fs::path(table_path) / manifest_file_name);
    openManifest(/*rewrite=*/false);
}

void OverwriteCachePersistence::truncate()
{
    std::lock_guard lifecycle_lock(lifecycle_mutex);
    {
        std::lock_guard lock(mutex);
        if (backup_pins)
            throw Exception(ErrorCodes::ABORTED, "Cannot change `OverwriteCache` files while a backup is pinned");
    }
    if (!isEnabled())
        return;

    shutdown();

    const auto table_path = getPath();
    {
        std::lock_guard state_lock(state_mutex);
        writeManifest({});
        closeManifest();
    }
    for (auto it = disk->iterateDirectory(table_path); it->isValid(); it->next())
    {
        if (it->name().ends_with(segment_file_suffix))
            disk->removeFileIfExists(fs::path(table_path) / it->name());
    }

    {
        std::lock_guard state_lock(state_mutex);
        live_segments.clear();
        live_segment_ids.clear();
        records_since_checkpoint = 0;
        bytes_since_checkpoint = 0;
        checkpoint_bytes = 0;
    }
    next_segment_id.store(1, std::memory_order_relaxed);

    {
        std::lock_guard lock(mutex);
        queue.clear();
        queued_bytes = 0;
        durable_sequence = next_sequence - 1;
        deferred_removals.clear();
    }
    {
        std::lock_guard state_lock(state_mutex);
        openManifest(/*rewrite=*/false);
    }

    std::lock_guard lock(mutex);
    started = true;
    stopped = false;
    writer_exception = {};
    writer_thread.emplace([this] { writerThread(); });
}

void OverwriteCachePersistence::removeAllFiles()
{
    std::lock_guard lifecycle_lock(lifecycle_mutex);
    {
        std::lock_guard lock(mutex);
        if (backup_pins)
            throw Exception(ErrorCodes::ABORTED, "Cannot change `OverwriteCache` files while a backup is pinned");
    }
    if (!isEnabled())
        return;

    shutdown();

    const auto table_path = getPath();
    if (disk->existsDirectory(table_path))
    {
        std::lock_guard state_lock(state_mutex);
        writeManifest({});
        closeManifest();
        disk->removeRecursive(table_path);
    }
}

void OverwriteCachePersistence::rename(const String & new_path)
{
    std::lock_guard lifecycle_lock(lifecycle_mutex);
    {
        std::lock_guard lock(mutex);
        if (backup_pins)
            throw Exception(ErrorCodes::ABORTED, "Cannot change `OverwriteCache` files while a backup is pinned");
    }
    if (!isEnabled())
        return;

    std::lock_guard state_lock(state_mutex);
    {
        /// A database that addresses a table by its UUID keeps the same data path across a rename, and
        /// replacing a directory with itself is not a move.
        std::lock_guard lock(path_mutex);
        if (path == new_path)
            return;
    }

    closeManifest();
    {
        std::lock_guard lock(path_mutex);
        disk->replaceFile(path, new_path);
        path = new_path;
    }
    openManifest(/*rewrite=*/false);
}

void OverwriteCachePersistence::shutdown()
{
    if (!isEnabled())
        return;

    {
        std::lock_guard lock(mutex);
        if (stopped)
            return;
        stopped = true;
        queue_changed.notify_all();
        durable_changed.notify_all();
    }

    if (writer_thread)
    {
        writer_thread->join();
        writer_thread.reset();
    }

    std::lock_guard state_lock(state_mutex);
    closeManifest();
}

OverwriteCachePersistence::BackupPin::BackupPin(OverwriteCachePersistence & persistence_) : persistence(persistence_)
{
    std::lock_guard lifecycle_lock(persistence.lifecycle_mutex);
    std::lock_guard lock(persistence.mutex);
    ++persistence.backup_pins;
}

OverwriteCachePersistence::BackupPin::~BackupPin()
{
    std::vector<UInt64> to_remove;
    {
        std::lock_guard lock(persistence.mutex);
        if (--persistence.backup_pins == 0)
            to_remove.swap(persistence.deferred_removals);
    }

    try
    {
        persistence.removeSegmentFiles(to_remove);
    }
    catch (...)
    {
        tryLogCurrentException(persistence.log, "Failed to remove retired `OverwriteCache` segment files");
    }
}

std::vector<String> OverwriteCachePersistence::collectFilesForBackup(String * manifest_contents)
{
    if (!isEnabled())
        return {};

    /// The queue has to be empty for the log to describe everything the table holds, and the log has to
    /// be self-contained so that a restore does not need the retired files it still mentions.
    UInt64 sequence = 0;
    {
        std::lock_guard lock(mutex);
        if (backup_pins == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A `BACKUP` of storage `OverwriteCache` requires a pin");
        sequence = next_sequence - 1;
    }
    waitDurable(sequence);

    std::vector<String> result;
    {
        std::lock_guard state_lock(state_mutex);
        try
        {
            checkpointManifest();
        }
        catch (...)
        {
            setException();
            throw;
        }
        if (manifest_contents)
        {
            auto in = disk->readFile(fs::path(getPath()) / manifest_file_name, getReadSettings());
            readStringUntilEOF(*manifest_contents, *in);
        }
        result.reserve(live_segments.size() + 1);
        result.emplace_back(manifest_file_name);
        for (const auto & segment : live_segments)
            result.push_back(segmentFileName(segment.segment_id));
    }
    return result;
}

void OverwriteCachePersistence::restoreFileFromBackup(const String & file_name, ReadBuffer & in)
{
    if (!isEnabled())
        throw Exception(
            ErrorCodes::CANNOT_RESTORE_TABLE,
            "Storage `OverwriteCache` cannot restore data into a table created with `persist_mode = 'none'`");

    createDirectories();
    if (fs::path(file_name).filename() != file_name || file_name == "." || file_name == "..")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid `OverwriteCache` backup filename");
    const auto destination = fs::path(getPath()) / file_name;
    const auto temporary = fs::path(getPath()) / tmp_directory / file_name;
    auto out = disk->writeFile(temporary, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
    copyData(in, *out);
    out->next();
    out->sync();
    out->finalize();
    auto directory_guard = disk->getDirectorySyncGuard(getPath());
    disk->replaceFile(temporary, destination);
}

}
