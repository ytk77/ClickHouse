#include <Storages/MergeTree/UniqueKey/UpsertPartData.h>
#include <Storages/MergeTree/UniqueKey/UpsertBinary.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>
#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Core/UUID.h>
#include <Common/ErrnoException.h>
#include <Common/SipHash.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/HashingReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <filesystem>
#include <map>
#include <numeric>
#include <zlib.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
}

namespace DB::Upsert
{
namespace
{
class VersionColumn
{
public:
    explicit VersionColumn(const ColumnWithTypeAndName & version)
        : column(*version.column), is_datetime64(WhichDataType(version.type).isDateTime64())
    {
    }

    UInt64 get(size_t row) const
    {
        /// Preserve signed `DateTime64` ticks and their full scale in the unsigned winner order.
        /// `UInt64` and `DateTime` retain their original unsigned values.
        if (is_datetime64)
            return static_cast<UInt64>(column.getInt(row)) ^ (UInt64{1} << 63);
        return column.getUInt(row);
    }

private:
    const IColumn & column;
    const bool is_datetime64;
};

VectorWithMemoryTracking<String> encodeKeys(const Block & block, const StorageInMemoryMetadata & metadata, size_t limit)
{
    Columns columns;
    for (const auto & name : metadata.getUniqueKeyColumns())
        columns.push_back(block.getByName(name).column);
    VectorWithMemoryTracking<String> keys;
    UniqueKeyEncoding::encodeBlock(columns, nullptr, limit, keys);
    return keys;
}
UInt32 crc(std::string_view bytes)
{
    return static_cast<UInt32>(crc32(0, reinterpret_cast<const Bytef *>(bytes.data()), static_cast<uInt>(bytes.size())));
}
}

ObjectID newObjectID()
{
    const auto uuid = UUIDHelpers::generateV4();
    return {UUIDHelpers::getHighBytes(uuid), UUIDHelpers::getLowBytes(uuid)};
}

String objectName(ObjectID id)
{
    UUID uuid;
    UUIDHelpers::getHighBytes(uuid) = id[0];
    UUIDHelpers::getLowBytes(uuid) = id[1];
    return toString(uuid);
}

String encodePartRows(const PartRows & rows)
{
    if (rows.empty() || rows.size() > MAX_UNIT_ROWS)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Invalid UpsertMergeTree part row count");
    BinaryWriter out;
    out.bytes = "UMPROW01";
    out.number(rows.size(), 4);
    for (size_t i = 0; i < rows.size(); ++i)
    {
        if (rows[i].value.row_offset != i)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "UpsertMergeTree row metadata is not in physical order");
        out.string(rows[i].key);
        out.string(encodeIndexValue(rows[i].value));
    }
    if (out.bytes.size() > MAX_RECORD_BYTES - 4)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree row metadata exceeds its byte limit");
    out.number(crc(out.bytes), 4);
    return std::move(out.bytes);
}

PartRows decodePartRows(std::string_view bytes)
{
    if (bytes.size() < PART_ROWS_ENVELOPE_BYTES || bytes.size() > MAX_RECORD_BYTES || !bytes.starts_with("UMPROW01"))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree row metadata header");
    BinaryReader checksum{bytes.substr(bytes.size() - 4)};
    if (checksum.number(4) != crc(bytes.substr(0, bytes.size() - 4)))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree row metadata checksum mismatch");
    BinaryReader in{bytes.substr(8, bytes.size() - 12)};
    const auto count = in.count(MAX_UNIT_ROWS, 50);
    if (!count)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Empty UpsertMergeTree part metadata");
    PartRows rows;
    rows.reserve(count);
    for (size_t i = 0; i < count; ++i)
    {
        auto key = in.string(MAX_UNIT_BYTES);
        auto value = decodeIndexValue(in.string(50));
        if (value.row_offset != i)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree physical row offset mismatch");
        rows.push_back({std::move(key), value});
    }
    in.end();
    return rows;
}

ValidatedPartRows::ValidatedPartRows(PartRows rows_, ObjectID part_) : rows(std::move(rows_)), part(part_)
{
    if (rows.empty() || rows.size() > MAX_UNIT_ROWS || part == ObjectID{})
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree input row count or identity");
    std::vector<std::string_view> keys;
    keys.reserve(rows.size());
    std::vector<bool> offsets(rows.size(), false);
    for (const auto & row : rows)
    {
        if (row.value.row.operation_id != part || !row.value.row.self_binding || row.value.row.write_csn != 0
            || row.value.row_offset >= rows.size() || offsets[row.value.row_offset])
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree input has invalid lineage or offsets");
        offsets[row.value.row_offset] = true;
        keys.emplace_back(row.key);
    }
    std::sort(keys.begin(), keys.end());
    if (std::adjacent_find(keys.begin(), keys.end()) != keys.end())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree input has duplicate keys");
}

Block cutInputBlock(const Block & block, size_t offset, size_t max_rows, size_t max_bytes)
{
    chassert(offset < block.rows() && max_rows && max_bytes);
    if (!offset && block.rows() <= max_rows && block.bytes() <= max_bytes)
        return block;

    const size_t end = offset + std::min(max_rows, block.rows() - offset);
    size_t rows = 0;
    size_t bytes = 0;
    for (size_t row = offset; row < end; ++row)
    {
        size_t row_bytes = 0;
        for (const auto & column : block)
        {
            if (rows && column.column->isConst())
                continue;
            const size_t column_bytes = column.column->byteSizeAt(row);
            if (column_bytes > max_bytes - row_bytes)
            {
                row_bytes = max_bytes + 1;
                break;
            }
            row_bytes += column_bytes;
        }
        if (row_bytes > max_bytes - bytes)
            break;
        bytes += row_bytes;
        ++rows;
    }
    if (!rows)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "UpsertMergeTree cannot split input row {}: a single row exceeds the {} byte unit limit", offset, max_bytes);

    while (true)
    {
        auto result = block.cloneEmpty();
        for (size_t i = 0; i < block.columns(); ++i)
        {
            /// Copy the range into an empty column so `LowCardinality` dictionaries are compacted too.
            auto column = result.getByPosition(i).column->assumeMutable();
            column->insertRangeFrom(*block.getByPosition(i).column, offset, rows);
            result.getByPosition(i).column = std::move(column);
        }
        if (result.bytes() <= max_bytes)
            return result;
        /// Per-value accounting can omit column-level overhead. Check the actual slice before admission.
        if (rows == 1)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "UpsertMergeTree cannot split input row {}: its column representation uses {} bytes, exceeding the {} byte unit limit",
                offset, result.bytes(), max_bytes);
        rows /= 2;
    }
}

size_t foldBlock(Block & block, const StorageInMemoryMetadata & metadata, const String & version_column, size_t max_key_bytes)
{
    auto keys = encodeKeys(block, metadata, max_key_bytes);
    std::map<String, size_t> winners;
    std::optional<VersionColumn> versions;
    if (!version_column.empty())
        versions.emplace(block.getByName(version_column));
    const auto & ordinals = *block.getByName(ORDINAL_COLUMN).column;
    for (size_t row = 0; row < block.rows(); ++row)
    {
        auto [it, inserted] = winners.emplace(keys[row], row);
        if (!inserted)
        {
            const auto old = it->second;
            if (!versions || versions->get(row) > versions->get(old)
                || (versions->get(row) == versions->get(old) && ordinals.getUInt(row) > ordinals.getUInt(old)))
                it->second = row;
        }
    }
    IColumn::Filter keep(block.rows(), UInt8{0});
    size_t lineage_bytes = PART_ROWS_ENVELOPE_BYTES;
    for (const auto & [key, row] : winners)
    {
        keep[row] = 1;
        /// Two four-byte string lengths plus the 42- or 50-byte encoded `IndexValue`.
        lineage_bytes += key.size() + 8 + (versions ? 50 : 42);
    }
    if (winners.size() != block.rows())
        for (auto & column : block)
            column.column = column.column->filter(keep, static_cast<ssize_t>(winners.size()));
    return lineage_bytes;
}

size_t estimateInsertStateBytes(size_t lineage_bytes, size_t rows, size_t partition_bytes, size_t token_bytes)
{
    /// Relative to lineage, each winner `Put` adds the partition prefix, physical part ID,
    /// record framing and a possible 20-byte kill. Three bytes cover key-length varint growth.
    /// The remaining allowance covers bounded names, counters, retry records and a dense new mask.
    const size_t mask_bytes = 16 + ((rows + 65535) / 65536) * (8192 + 8);
    return lineage_bytes + rows * (43 + partition_bytes) + 4096 + 3 * token_bytes + 4 * partition_bytes + mask_bytes;
}

ObjectID hashInputBlock(const Block & block)
{
    SipHash hash;
    for (const auto & column : block)
    {
        hash.update(column.name.size());
        hash.update(column.name);
        hash.update(column.type->getName());
        for (size_t row = 0; row < block.rows(); ++row)
            column.column->updateHashWithValue(row, hash);
    }
    ObjectID result;
    hash.get128(result[0], result[1]);
    return result;
}

void writePartIndex(
    IDataPartStorage & storage, const Block & block, const StorageInMemoryMetadata & metadata,
    const String & version_column, ObjectID operation_id, const IColumn::Permutation * permutation,
    size_t max_key_bytes, ContextPtr context, MergeTreeDataPartChecksums & checksums)
{
    const auto keys = encodeKeys(block, metadata, max_key_bytes);
    const auto & ordinals = *block.getByName(ORDINAL_COLUMN).column;
    std::optional<VersionColumn> versions;
    if (!version_column.empty())
        versions.emplace(block.getByName(version_column));
    PartRows rows;
    rows.reserve(block.rows());
    for (size_t offset = 0; offset < block.rows(); ++offset)
    {
        const auto source = permutation ? (*permutation)[offset] : offset;
        RowMetadata row{.operation_id = operation_id, .block_ordinal = ordinals.getUInt(source), .self_binding = true};
        if (versions)
            row.version = versions->get(source);
        if (block.has(LINEAGE_COLUMN))
        {
            const auto value = block.getByName(LINEAGE_COLUMN).column->getDataAt(source);
            const auto original = decodeIndexValue(value).row;
            if (original.self_binding || original.version != row.version)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree maintenance lineage");
            row = original;
        }
        rows.push_back({keys[source], {static_cast<UInt32>(offset), row}});
    }
    const auto bytes = encodePartRows(rows);
    auto out = storage.writeFile(ROWS_FILE, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());
    HashingWriteBuffer hashing(*out);
    hashing.write(bytes.data(), bytes.size());
    const auto hash = hashing.getHash();
    hashing.finalize();
    out->finalize();
    /// The coordinator synchronizes the complete finalized part before durable authorization.
    checksums.addFile(ROWS_FILE, bytes.size(), hash);

    std::vector<size_t> order(rows.size());
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](size_t lhs, size_t rhs)
    {
        return rows[lhs].key < rows[rhs].key;
    });
    SSTIndexWriter writer(storage, context);
    for (auto row : order)
        writer.addUpsertEncoded(rows[row].key, encodeIndexValue(rows[row].value));
    writer.finalizeToStorage();

    ReadBufferFromFile sst(std::filesystem::path(storage.getFullPath()) / SSTIndexWriter::FILE_NAME);
    HashingReadBuffer sst_hash(sst);
    sst_hash.ignoreAll();
    checksums.addFile(SSTIndexWriter::FILE_NAME, sst_hash.count(), sst_hash.getHash());
}

ObjectID hashPartDirectory(const String & path, bool sync_files)
{
    namespace fs = std::filesystem;
    if (fs::is_symlink(path) || !fs::is_directory(path))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree part directory {}", path);
    std::vector<fs::path> files;
    std::vector<fs::path> directories{path};
    for (const auto & entry : fs::recursive_directory_iterator(path))
    {
        if (entry.is_symlink())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Symbolic link in UpsertMergeTree part {}", path);
        if (entry.is_directory())
            directories.push_back(entry.path());
        else if (entry.is_regular_file())
            files.push_back(entry.path());
        else
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Non-regular file in UpsertMergeTree part {}", path);
    }
    /// Preserve `std::filesystem::path` component ordering in the persisted inventory hash.
    std::sort(files.begin(), files.end());
    BinaryWriter inventory;
    std::vector<char> buffer(65536);
    std::sort(directories.begin(), directories.end());
    for (const auto & directory : directories)
        inventory.string(fs::relative(directory, path).string() + "/");
    for (const auto & file : files)
    {
        const int fd = ::open(file.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
        if (fd < 0)
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_OPEN_FILE, file.string(), "Cannot authenticate UpsertMergeTree part file");
        try
        {
            SipHash hash;
            UInt64 size = 0;
            while (true)
            {
                const auto bytes = ::read(fd, buffer.data(), buffer.size());
                if (bytes < 0 && errno == EINTR)
                    continue;
                if (bytes < 0)
                    ErrnoException::throwWithErrno(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno, "Cannot authenticate UpsertMergeTree part file");
                if (!bytes)
                    break;
                hash.update(buffer.data(), static_cast<size_t>(bytes));
                size += static_cast<UInt64>(bytes);
            }
            ObjectID digest;
            hash.get128(digest[0], digest[1]);
            inventory.string(fs::relative(file, path).string());
            inventory.number(size);
            inventory.id(digest);
        }
        catch (...)
        {
            ::close(fd);
            throw;
        }
        ::close(fd);
    }
    if (sync_files)
    {
        /// Finalize and authenticate every file before issuing one group of payload barriers.
        std::vector<String> paths;
        paths.reserve(files.size());
        for (const auto & file : files)
            paths.push_back(file.string());
        syncFiles(paths);
        for (auto it = directories.rbegin(); it != directories.rend(); ++it)
            syncDirectory(it->string());
    }
    return hashLogRecord(inventory.bytes);
}
}
