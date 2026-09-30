#include <Storages/MergeTree/UniqueKey/UpsertTableManifest.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Storages/MergeTree/UniqueKey/UpsertLogRecord.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Parsers/IAST.h>
#include <Common/Exception.h>
#include <Core/UUID.h>
#include <IO/WriteHelpers.h>
#include <filesystem>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace Upsert
{
namespace
{
const String legacy_prefix = "UpsertMergeTree\nrow_metadata=1\nkey_encoding=1\ncommit_protocol=parts_sync_v1\n";
const String disk_prefix = "UpsertMergeTree\nrow_metadata=1\nkey_encoding=1\ncommit_protocol=disk_state_v2\n";
const String mutable_ttl_prefix = disk_prefix + "ttl_policy=metadata_v1\n";
const String mutable_schema_prefix = mutable_ttl_prefix + "schema_policy=metadata_v1\n";

String schemaFingerprint(const String & disk_name, const StorageInMemoryMetadata & metadata, const String & version_column,
    bool include_ttl, bool include_columns = true)
{
    String schema;
    /// Length delimiters make the fingerprint independent of embedded newlines in identifiers.
    auto append = [&](const String & field)
    {
        schema += std::to_string(field.size()) + ":" + field;
    };
    append(disk_name);
    if (include_columns)
        append(metadata.columns.toString(/*include_comments=*/false));
    append(version_column);
    if (!include_columns && !version_column.empty())
        append(metadata.columns.getPhysical(version_column).type->getName());
    for (const auto * key : {&metadata.unique_key, &metadata.partition_key, &metadata.sorting_key, &metadata.primary_key})
    {
        append(key->expression_list_ast ? key->expression_list_ast->formatWithSecretsOneLine() : String{});
        append(std::to_string(key->data_types.size()));
        for (const auto & type : key->data_types)
            append(type->getName());
    }
    if (include_ttl && metadata.table_ttl.definition_ast)
        append(metadata.table_ttl.definition_ast->formatWithSecretsOneLine());
    return schema;
}

LogRecord readManifest(const String & path)
{
    const auto contents = readBoundedFile(path, MAX_RECORD_BYTES);
    const auto decoded = decodeLogRecord(contents, false);
    if (!decoded || decoded->bytes_consumed != contents.size() || decoded->record.kind != RecordKind::Noop
        || decoded->record.partition_id != "table_manifest" || decoded->record.csn != 1
        || decoded->record.previous_hash != ObjectID{} || decoded->record.operation_id != decoded->record.incarnation)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree table manifest");
    return decoded->record;
}
}

ObjectID initializeTableManifest(
    const String & table_path, const String & disk_name, const String & disk_path,
    const StorageInMemoryMetadata & metadata, const String & version_column, bool attach, bool & use_disk_state)
{
    const auto path = (std::filesystem::path(table_path) / "upsert_manifest.bin").string();
    if (attach)
    {
        const auto record = readManifest(path);
        use_disk_state = record.payload.starts_with(disk_prefix);
        const bool mutable_ttl = record.payload.starts_with(mutable_ttl_prefix);
        const bool mutable_schema = record.payload.starts_with(mutable_schema_prefix);
        const auto & protocol_prefix = mutable_schema ? mutable_schema_prefix
            : (mutable_ttl ? mutable_ttl_prefix : (use_disk_state ? disk_prefix : legacy_prefix));
        if (record.payload != protocol_prefix + schemaFingerprint(disk_name, metadata, version_column, !mutable_ttl, !mutable_schema))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree table manifest does not match its schema or disk");
        return record.incarnation;
    }

    const auto uuid = UUIDHelpers::generateV4();
    use_disk_state = true;
    ObjectID incarnation{UUIDHelpers::getHighBytes(uuid), UUIDHelpers::getLowBytes(uuid)};
    LogRecord manifest{.incarnation = incarnation, .partition_id = "table_manifest", .csn = 1,
        .operation_id = incarnation, .kind = RecordKind::Noop,
        .payload = mutable_ttl_prefix + schemaFingerprint(disk_name, metadata, version_column, false)};
    createDurableFile(path, encodeLogRecord(manifest));
    syncDirectoryAncestors(table_path, disk_path);
    return incarnation;
}

void enableMutableTTL(
    const String & table_path, const String & disk_name, const StorageInMemoryMetadata & metadata, const String & version_column)
{
    const auto path = (std::filesystem::path(table_path) / "upsert_manifest.bin").string();
    auto record = readManifest(path);
    const auto target = mutable_ttl_prefix + schemaFingerprint(disk_name, metadata, version_column, false);
    if (record.payload == target
        || record.payload == mutable_schema_prefix + schemaFingerprint(disk_name, metadata, version_column, false, false))
    {
        /// A previous upgrade may have replaced the file but failed its directory barrier.
        syncDirectory(table_path);
        return;
    }
    if (record.payload != disk_prefix + schemaFingerprint(disk_name, metadata, version_column, true))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree TTL migration manifest does not match its schema or disk");

    /// Upgrade before changing SQL metadata: both sides of this atomic replacement accept the current policy.
    /// Afterward the database's metadata commit is the sole authority for mutable `TTL` rules.
    record.payload = target;
    const auto temporary = path + "." + toString(UUIDHelpers::generateV4()) + ".tmp";
    createDurableFile(temporary, encodeLogRecord(record));
    std::filesystem::rename(temporary, path);
    syncDirectory(table_path);
}

void enableMutableSchema(
    const String & table_path, const String & disk_name, const StorageInMemoryMetadata & metadata, const String & version_column)
{
    enableMutableTTL(table_path, disk_name, metadata, version_column);
    const auto path = (std::filesystem::path(table_path) / "upsert_manifest.bin").string();
    auto record = readManifest(path);
    const auto target = mutable_schema_prefix + schemaFingerprint(disk_name, metadata, version_column, false, false);
    if (record.payload == target)
        return;
    record.payload = target;
    const auto temporary = path + "." + toString(UUIDHelpers::generateV4()) + ".tmp";
    createDurableFile(temporary, encodeLogRecord(record));
    std::filesystem::rename(temporary, path);
    syncDirectory(table_path);
}
}
}
