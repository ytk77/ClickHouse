#pragma once

#include <Storages/MergeTree/UniqueKey/UpsertLogRecord.h>
#include <mutex>
#include <vector>

namespace DB::Upsert
{

/// Synchronous append-only partition log. Rotation and group commit require separate coordinators.
/// The caller retains every referenced payload before append and publishes only after it returns.
class PartitionLog
{
public:
    PartitionLog(String path_, ObjectID incarnation_, String partition_id_, bool create, size_t max_bytes_ = 256U << 20);
    ~PartitionLog();
    PartitionLog(const PartitionLog &) = delete;
    PartitionLog & operator=(const PartitionLog &) = delete;

    /// Recover the complete prefix, repairing only a truncated unsealed final frame.
    /// Must run before admitting writers or discovering native parts.
    std::vector<LogRecord> recover();

    /// A failure during write or sync fences this instance. Reopen and replay to reconcile it.
    UInt64 append(RecordKind kind, ObjectID operation_id, String payload);
    UInt64 nextCSN() const;
    bool isFenced() const;

private:
    String path;
    ObjectID incarnation;
    String partition_id;
    size_t max_bytes;
    int fd = -1;
    mutable std::mutex mutex;
    LogReplayCursor cursor;
    size_t bytes_written = 0;
    bool recovered = false;
    bool fenced = false;
};

}
