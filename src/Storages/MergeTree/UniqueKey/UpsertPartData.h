#pragma once

#include <Storages/MergeTree/UniqueKey/UpsertRowMetadata.h>
#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <Interpreters/Context_fwd.h>
#include <vector>

namespace DB
{
class IDataPartStorage;
struct StorageInMemoryMetadata;
struct MergeTreeDataPartChecksums;

namespace Upsert
{
inline constexpr auto ORDINAL_COLUMN = "_upsert_block_ordinal";
inline constexpr auto LINEAGE_COLUMN = "_upsert_merge_lineage";
inline constexpr auto ROWS_FILE = "upsert_rows.bin";
/// Eight-byte magic, four-byte row count and four-byte checksum in `upsert_rows.bin`.
inline constexpr size_t PART_ROWS_ENVELOPE_BYTES = 16;
inline constexpr size_t MAX_UNIT_ROWS = 1U << 20;
inline constexpr size_t MAX_UNIT_BYTES = 32U << 20;
inline constexpr size_t MAX_PENDING_STATE_BYTES = 128U << 20;

struct PartRow
{
    String key;
    IndexValue value;
    bool operator==(const PartRow &) const = default;
};
using PartRows = std::vector<PartRow>;

/// Immutable input validation can run before taking the table parts lock.
class ValidatedPartRows
{
public:
    ValidatedPartRows(PartRows rows_, ObjectID part_);
    const PartRows & getRows() const
    {
        return rows;
    }
    ObjectID getPart() const
    {
        return part;
    }

private:
    PartRows rows;
    ObjectID part;
};


ObjectID newObjectID();
String objectName(ObjectID id);
String encodePartRows(const PartRows & rows);
PartRows decodePartRows(std::string_view bytes);

/// Take a bounded contiguous slice. Small inputs retain their columns without copying.
Block cutInputBlock(const Block & block, size_t offset, size_t max_rows = MAX_UNIT_ROWS, size_t max_bytes = MAX_UNIT_BYTES);

/// Fold before the native sort, retaining original input ordinals for equal-version ties.
/// Return the encoded size of `upsert_rows.bin`, including its envelope.
size_t foldBlock(Block & block, const StorageInMemoryMetadata & metadata, const String & version_column, size_t max_key_bytes);
/// Bound winner replacements, authorization and the new mask; existing source masks remain state-dependent.
size_t estimateInsertStateBytes(size_t lineage_bytes, size_t rows, size_t partition_bytes, size_t token_bytes);
ObjectID hashInputBlock(const Block & block);

void writePartIndex(
    IDataPartStorage & storage, const Block & block, const StorageInMemoryMetadata & metadata,
    const String & version_column, ObjectID operation_id, const IColumn::Permutation * permutation,
    size_t max_key_bytes, ContextPtr context, MergeTreeDataPartChecksums & checksums);

/// Authenticate all immutable part files, including sidecars. Optional barriers precede authorization.
ObjectID hashPartDirectory(const String & path, bool sync_files);
}
}
