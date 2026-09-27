#pragma once

#include <Storages/MergeTree/UniqueKey/UpsertPartData.h>
#include <Storages/MergeTree/UniqueKey/UpsertPartitionLog.h>
#include <Storages/MergeTree/UniqueKey/UpsertSnapshot.h>
#include <set>
#include <functional>

namespace DB::Upsert
{
class DiskState;
struct DiskPrepared;
using StateClock = std::function<UInt64()>;
struct Token
{
    String client_token;
    UInt64 unit = 0;
    ObjectID payload_hash{};
    UInt64 accepted = 0;
    bool operator==(const Token &) const = default;
};

struct Kill
{
    ObjectID part{};
    UInt32 offset = 0;
    auto operator<=>(const Kill &) const = default;
};

struct PartCommit
{
    ObjectID part{};
    String temporary_name;
    String final_name;
    ObjectID files_hash{};
    UInt64 rows = 0;
    std::vector<UInt32> dead;
    std::vector<Kill> kills;
    Token token;
};
String encodePartCommit(const PartCommit & record);
PartCommit decodePartCommit(std::string_view bytes);

/// Protocol dispatch for legacy parts-only state and disk-backed state with atomic retirement.
/// Public operations require the table's parts lock, except recovery before publication.
class StateStore
{
    struct Winner
    {
        ObjectID part;
        IndexValue value;
    };
    struct PartitionState
    {
        std::map<String, Winner> winners;
        std::map<std::pair<String, UInt64>, Token> tokens;
        UInt64 csn = 0;
        size_t key_bytes = 0;
    };
    struct Partition
    {
        std::unique_ptr<PartitionLog> log;
        std::shared_ptr<PartitionState> state = std::make_shared<PartitionState>();
    };

public:
    struct Prepared
    {
        String partition;
        ObjectID operation_id;
        UInt64 csn = 0;
        PartCommit record;
        String payload;
        SnapshotPtr snapshot;
        bool replayed_token = false;
        bool produces_part = false;
        std::vector<String> retired_parts;
        std::shared_ptr<DiskPrepared> disk;
    private:
        friend class StateStore;
        std::shared_ptr<PartitionState> state;
    };

    StateStore(String table_path_, ObjectID incarnation_, bool attach, bool use_disk_state = false, StateClock clock = {});
    ~StateStore();
    SnapshotPtr snapshot() const;
    const std::map<String, ObjectID> & authorizedParts() const;
    std::optional<ObjectID> authorizedPart(const String & name) const;
    UInt64 authorizedPartCount() const;
    Prepared prepare(const String & partition, PartCommit part, const PartRows & rows);
    Prepared prepare(const String & partition, PartCommit part, const ValidatedPartRows & rows);
    Prepared prepareMerge(const String & partition, PartCommit part, const PartRows & rows, const std::vector<String> & sources);
    PartRows loadPartRows(const String & name) const;
    void pruneRetired();
    void append(const Prepared & prepared);
    void publish(Prepared & prepared) noexcept;
    /// A non-throwing mismatch probe only selects a compatible input boundary; commits always check strictly.
    bool tokenAlreadyCommitted(const String & partition, const Token & token, bool throw_on_mismatch = true) const;
    void checkAdmission() const;
    bool isFenced() const
    {
        return fenced;
    }
    bool usesDiskState() const
    {
        return disk != nullptr;
    }

    static constexpr size_t MAX_PARTS = 4096;
    static constexpr size_t MAX_PARTITIONS = 512;
    static constexpr size_t MAX_LIVE_KEYS = 262144;
    static constexpr size_t MAX_KEY_BYTES = 64U << 20;
    static constexpr size_t MAX_TOKENS = 4096;

private:
    String table_path;
    String state_path;
    ObjectID incarnation;
    std::unique_ptr<PartitionLog> registry;
    std::map<String, Partition> partitions;
    std::map<String, ObjectID> authorized_parts;
    std::set<ObjectID> operation_ids;
    SnapshotPtr current_snapshot = std::make_shared<const SnapshotState>();
    size_t part_count = 0;
    bool fenced = false;
    std::unique_ptr<DiskState> disk;

    Partition & getOrCreatePartition(const String & partition);
    Prepared decide(const String & partition, PartCommit part, const PartRows & rows, UInt64 csn);
    void recoverPartition(const String & partition, Partition & entry);
};
}
