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
inline constexpr size_t MAX_UNIT_ROWS = 1U << 20;
inline constexpr size_t MAX_UNIT_BYTES = 32U << 20;

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

/// Fold before the native sort, retaining original input ordinals for equal-version ties.
void foldBlock(Block & block, const StorageInMemoryMetadata & metadata, const String & version_column, size_t max_key_bytes);
ObjectID hashInputBlock(const Block & block);

void writePartIndex(
    IDataPartStorage & storage, const Block & block, const StorageInMemoryMetadata & metadata,
    const String & version_column, ObjectID operation_id, const IColumn::Permutation * permutation,
    size_t max_key_bytes, ContextPtr context, MergeTreeDataPartChecksums & checksums);

/// Authenticate all immutable part files, including sidecars. Optional barriers precede authorization.
ObjectID hashPartDirectory(const String & path, bool sync_files);
}
}
