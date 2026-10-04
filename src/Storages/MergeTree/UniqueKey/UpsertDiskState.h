#pragma once

#include <Storages/MergeTree/UniqueKey/UpsertResources.h>

#include <Storages/MergeTree/UniqueKey/UpsertStateStore.h>

namespace DB::Upsert
{
struct DiskPrepared;

/// The `RocksDB` write batch is the sole terminal authority for the disk-state protocol.
/// Payload files must be durable before `append`; callers serialize commits with the parts lock.
class DiskState
{
public:
    DiskState(const String & table_path, ObjectID incarnation, bool attach, StateClock clock = {}, const Resources & resources = {});
    ~DiskState();
    /// Called after stopping writers and releasing the published read snapshot, before deleting files.
    void close();
    SnapshotPtr snapshot() const;
    std::optional<ObjectID> authorizedPart(const String & name) const;
    UInt64 authorizedPartCount() const;
    void checkAdmission() const;
    bool tokenAlreadyCommitted(const String & partition, const Token & token, bool throw_on_mismatch = true) const;
    StateStore::Prepared prepare(const String & partition, PartCommit part, const ValidatedPartRows & rows);
    StateStore::Prepared prepareMerge(
        const String & partition, PartCommit part, const PartRows & rows, const std::vector<String> & sources,
        const PartRows & expired, bool retire_sources, const StateStore::MergeInput * input);
    StateStore::Prepared prepareTruncate();
    /// Finish durable truncation while writers are excluded, after releasing retired snapshots.
    void finishTruncate();
    StateStore::Prepared preparePartition(
        std::vector<PartCommit> additions, const std::vector<ValidatedPartRows> & rows,
        const std::vector<String> & removed, const std::vector<String> & reset_partitions, const std::vector<String> & detached,
        const std::vector<String> & cleared_partitions);
    PartRows loadPartRows(const String & name) const;
    void pruneRetired();
    void append(const StateStore::Prepared & prepared);
    void publish(StateStore::Prepared & prepared) noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};
}
