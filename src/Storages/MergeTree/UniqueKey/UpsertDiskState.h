#pragma once

#include <Storages/MergeTree/UniqueKey/UpsertStateStore.h>

namespace DB::Upsert
{
struct DiskPrepared;

/// The `RocksDB` write batch is the sole terminal authority for the disk-state protocol.
/// Payload files must be durable before `append`; callers serialize commits with the parts lock.
class DiskState
{
public:
    DiskState(const String & table_path, ObjectID incarnation, bool attach, StateClock clock = {});
    ~DiskState();
    SnapshotPtr snapshot() const;
    std::optional<ObjectID> authorizedPart(const String & name) const;
    UInt64 authorizedPartCount() const;
    void checkAdmission() const;
    bool tokenAlreadyCommitted(const String & partition, const Token & token) const;
    StateStore::Prepared prepare(const String & partition, PartCommit part, const ValidatedPartRows & rows);
    StateStore::Prepared prepareMerge(const String & partition, PartCommit part, const PartRows & rows, const std::vector<String> & sources);
    PartRows loadPartRows(const String & name) const;
    void pruneRetired();
    void append(const StateStore::Prepared & prepared);
    void publish(StateStore::Prepared & prepared) noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};
}
