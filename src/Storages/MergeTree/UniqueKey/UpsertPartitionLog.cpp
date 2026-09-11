#include <Storages/MergeTree/UniqueKey/UpsertPartitionLog.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Common/ErrnoException.h>
#include <Common/FailPoint.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>

namespace DB
{
namespace FailPoints
{
    extern const char upsert_log_throw_after_append[];
    extern const char upsert_log_throw_after_sync[];
}
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CANNOT_FSYNC;
    extern const int SUPPORT_IS_DISABLED;
    extern const int FAULT_INJECTED;
}

namespace Upsert
{
namespace
{
void syncLog(int fd)
{
    int result;
    do
    {
        result = ::fsync(fd);
    } while (result < 0 && errno == EINTR);
    if (result < 0)
        ErrnoException::throwWithErrno(ErrorCodes::CANNOT_FSYNC, errno, "Cannot sync UpsertMergeTree partition log");
}
}

PartitionLog::PartitionLog(String path_, ObjectID incarnation_, String partition_id_, bool create, size_t max_bytes_)
    : path(std::move(path_)), incarnation(incarnation_), partition_id(std::move(partition_id_)), max_bytes(max_bytes_)
    , cursor(incarnation, partition_id)
{
    if (!max_bytes)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "UpsertMergeTree log byte limit must be positive");
    if (create)
        createDurableFile(path, {});
    fd = ::open(path.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_OPEN_FILE, path, "Cannot open UpsertMergeTree partition log {}", path);
    if (::flock(fd, LOCK_EX | LOCK_NB) != 0)
    {
        const int saved_errno = errno;
        ::close(fd);
        fd = -1;
        ErrnoException::throwFromPathWithErrno(ErrorCodes::CANNOT_OPEN_FILE, path, saved_errno,
            "UpsertMergeTree partition log already has an owner: {}", path);
    }
}

PartitionLog::~PartitionLog()
{
    if (fd >= 0)
        ::close(fd);
}

std::vector<LogRecord> PartitionLog::recover()
{
    std::lock_guard lock(mutex);
    if (recovered || fenced)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree log recovery requires a newly opened log");

    try
    {
        const auto bytes = readBoundedFile(path, max_bytes);
        std::string_view remaining(bytes);
        LogReplayCursor replay(incarnation, partition_id);
        std::vector<LogRecord> records;
        while (!remaining.empty())
        {
            auto decoded = decodeLogRecord(remaining, true);
            if (!decoded)
                break;
            replay.accept(*decoded);
            remaining.remove_prefix(decoded->bytes_consumed);
            records.push_back(std::move(decoded->record));
        }

        const size_t valid_bytes = bytes.size() - remaining.size();
        if (!remaining.empty())
        {
            if (::ftruncate(fd, static_cast<off_t>(valid_bytes)) != 0)
                ErrnoException::throwWithErrno(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, errno,
                    "Cannot truncate incomplete UpsertMergeTree log tail");
            syncLog(fd);
        }
        cursor = std::move(replay);
        bytes_written = valid_bytes;
        recovered = true;
        return records;
    }
    catch (...)
    {
        fenced = true;
        throw;
    }
}

UInt64 PartitionLog::append(RecordKind kind, ObjectID operation_id, String payload)
{
    std::lock_guard lock(mutex);
    if (!recovered || fenced)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree partition log is fenced or has not been recovered");
    LogRecord record{.incarnation = incarnation, .partition_id = partition_id, .csn = cursor.nextCSN(),
        .previous_hash = cursor.tailHash(), .operation_id = operation_id, .kind = kind, .payload = std::move(payload)};
    const auto frame = encodeLogRecord(record);
    if (frame.size() > max_bytes - bytes_written)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "UpsertMergeTree partition log byte limit reached; checkpoint rotation is required");

    /// Prepare the successor cursor before any I/O. The final assignment does not allocate.
    auto next = cursor;
    next.accept(DecodedRecord{record, hashLogRecord(frame), frame.size()});
    try
    {
        std::string_view remaining(frame);
        while (!remaining.empty())
        {
            const auto written = ::write(fd, remaining.data(), remaining.size());
            if (written < 0 && errno == EINTR)
                continue;
            if (written <= 0)
                ErrnoException::throwWithErrno(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, errno,
                    "Cannot append UpsertMergeTree terminal record");
            remaining.remove_prefix(static_cast<size_t>(written));
        }
        fiu_do_on(FailPoints::upsert_log_throw_after_append,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree log failure after append");
        });
        syncLog(fd);
        fiu_do_on(FailPoints::upsert_log_throw_after_sync,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree log failure after sync");
        });
    }
    catch (...)
    {
        fenced = true;
        throw;
    }
    cursor = std::move(next);
    bytes_written += frame.size();
    return record.csn;
}

bool PartitionLog::isFenced() const
{
    std::lock_guard lock(mutex);
    return fenced;
}

UInt64 PartitionLog::nextCSN() const
{
    std::lock_guard lock(mutex);
    if (!recovered || fenced)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "UpsertMergeTree partition log is not writable");
    return cursor.nextCSN();
}
}
}
