#include <Storages/MergeTree/UniqueKey/UpsertTableManifest.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Storages/MergeTree/UniqueKey/UpsertLogRecord.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Parsers/IAST.h>
#include <Common/Exception.h>
#include <Core/UUID.h>
#include <filesystem>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace Upsert
{
ObjectID initializeTableManifest(
    const String & table_path, const String & disk_name, const String & disk_path,
    const StorageInMemoryMetadata & metadata, const String & version_column, bool attach, bool & use_disk_state)
{
    const String legacy_prefix = "UpsertMergeTree\nrow_metadata=1\nkey_encoding=1\ncommit_protocol=parts_sync_v1\n";
    const String disk_prefix = "UpsertMergeTree\nrow_metadata=1\nkey_encoding=1\ncommit_protocol=disk_state_v2\n";
    String schema;
    /// Length delimiters make the fingerprint independent of embedded newlines in identifiers.
    auto append = [&](const String & field)
    {
        schema += std::to_string(field.size()) + ":" + field;
    };
    append(disk_name);
    append(metadata.columns.toString(/*include_comments=*/false));
    append(version_column);
    for (const auto * key : {&metadata.unique_key, &metadata.partition_key, &metadata.sorting_key, &metadata.primary_key})
    {
        append(key->expression_list_ast ? key->expression_list_ast->formatWithSecretsOneLine() : String{});
        append(std::to_string(key->data_types.size()));
        for (const auto & type : key->data_types)
            append(type->getName());
    }

    const auto path = (std::filesystem::path(table_path) / "upsert_manifest.bin").string();
    if (attach)
    {
        const auto contents = readBoundedFile(path, MAX_RECORD_BYTES);
        const auto decoded = decodeLogRecord(contents, false);
        use_disk_state = decoded && decoded->record.payload.starts_with(disk_prefix);
        const auto & protocol_prefix = use_disk_state ? disk_prefix : legacy_prefix;
        if (!decoded || decoded->bytes_consumed != contents.size() || decoded->record.kind != RecordKind::Noop
            || decoded->record.partition_id != "table_manifest" || decoded->record.csn != 1
            || decoded->record.previous_hash != ObjectID{} || decoded->record.operation_id != decoded->record.incarnation
            || decoded->record.payload != protocol_prefix + schema)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree table manifest does not match its schema or disk");
        return decoded->record.incarnation;
    }

    const auto uuid = UUIDHelpers::generateV4();
    use_disk_state = true;
    ObjectID incarnation{UUIDHelpers::getHighBytes(uuid), UUIDHelpers::getLowBytes(uuid)};
    LogRecord manifest{.incarnation = incarnation, .partition_id = "table_manifest", .csn = 1,
        .operation_id = incarnation, .kind = RecordKind::Noop, .payload = disk_prefix + schema};
    createDurableFile(path, encodeLogRecord(manifest));
    syncDirectoryAncestors(table_path, disk_path);
    return incarnation;
}
}
}
