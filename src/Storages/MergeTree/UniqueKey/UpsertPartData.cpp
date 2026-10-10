#include <Storages/MergeTree/UniqueKey/UpsertPartData.h>
#include <Storages/MergeTree/UniqueKey/UpsertInputFold.h>
#include <Storages/MergeTree/UniqueKey/UpsertMetrics.h>
#include <Storages/MergeTree/UniqueKey/UpsertBinary.h>
#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>
#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <DataTypes/IDataType.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnSparse.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Core/UUID.h>
#include <Common/ErrnoException.h>
#include <Common/SipHash.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/scope_guard_safe.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/IAST.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/ICompressionCodec.h>
#include <city.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <filesystem>
#include <map>
#include <zlib.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
extern const int MEMORY_LIMIT_EXCEEDED;
extern const int CANNOT_ALLOCATE_MEMORY;
extern const int QUERY_WAS_CANCELLED;
extern const int TIMEOUT_EXCEEDED;
extern const int ABORTED;
}

namespace DB::Upsert
{
namespace
{
/// `UMPCOL01` omits the value length, `UMRV` magic/version and physical offset.
constexpr UInt64 COMPACT_ELIDED_BYTES_PER_ROW = 4 + 4 + 1 + 4;

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

void appendKeyColumns(const ColumnPtr & input, Columns & columns)
{
    auto column = recursiveRemoveSparse(input->convertToFullIfWrapped());
    if (const auto * tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        for (const auto & element : tuple->getColumns())
            appendKeyColumns(element, columns);
    }
    else
        columns.push_back(std::move(column));
}

VectorWithMemoryTracking<String> encodeKeys(const Block & block, const StorageInMemoryMetadata & metadata, size_t limit)
{
    ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertKeyEncodeMicroseconds);
    Columns columns;
    for (const auto & name : metadata.getUniqueKeyColumns())
        appendKeyColumns(block.getByName(name).column, columns);
    VectorWithMemoryTracking<String> keys;
    if (columns.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UpsertMergeTree key has no scalar elements");
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

String encodePartRows(const PartRows & rows, bool compact)
{
    ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertLineageEncodeMicroseconds);
    if (rows.empty() || rows.size() > MAX_PART_ROWS)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Invalid UpsertMergeTree part row count");
    size_t encoded_size = PART_ROWS_ENVELOPE_BYTES;
    String value;
    for (const auto & row : rows)
    {
        if (row.value.row_offset % 65536 == 0)
            CurrentMemoryTracker::check();
        if (row.key.size() > MAX_UNIT_BYTES)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree part key exceeds its byte limit");
        encodeIndexValue(row.value, value);
        const auto size = row.key.size() + 8 + value.size();
        if (size > MAX_PART_LINEAGE_BYTES - encoded_size)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "UpsertMergeTree part lineage exceeds its byte limit");
        encoded_size += size;
    }
    const bool large = rows.size() > MAX_UNIT_ROWS || encoded_size > MAX_RECORD_BYTES;
    BinaryWriter out;
    out.bytes.reserve(compact ? encoded_size - COMPACT_ELIDED_BYTES_PER_ROW * rows.size() : encoded_size);
    CurrentMemoryTracker::check();
    out.bytes = compact ? "UMPCOL01" : large ? "UMPROW02" : "UMPROW01";
    out.number(rows.size(), 4);
    for (size_t i = 0; i < rows.size(); ++i)
    {
        if (i % 65536 == 0)
            CurrentMemoryTracker::check();
        if (rows[i].value.row_offset != i)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "UpsertMergeTree row metadata is not in physical order");
        out.number(rows[i].key.size(), 4);
        out.bytes += rows[i].key;
        if (!compact)
        {
            encodeIndexValue(rows[i].value, value);
            out.number(value.size(), 4);
            out.bytes += value;
        }
    }
    if (compact)
    {
        /// `UMPCOL01` preserves full keys and stores metadata in contiguous columns.
        for (const auto & row : rows)
            out.number((row.value.row.version ? 1 : 0) | (row.value.row.self_binding ? 2 : 0), 1);
        for (const auto & row : rows)
            out.id(row.value.row.operation_id);
        for (const auto & row : rows)
            out.number(row.value.row.block_ordinal);
        for (const auto & row : rows)
            out.number(row.value.row.write_csn);
        for (const auto & row : rows)
            if (row.value.row.version)
                out.number(*row.value.row.version);
    }
    out.number(crc(out.bytes), 4);
    return std::move(out.bytes);
}

static PartRows decodeRawPartRows(std::string_view bytes, size_t decoded_limit, QueryStatus * process)
{
    ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertLineageDecodeMicroseconds);
    const bool large = bytes.starts_with("UMPROW02");
    if (bytes.size() < PART_ROWS_ENVELOPE_BYTES || bytes.size() > std::min(decoded_limit, large ? MAX_PART_LINEAGE_BYTES : MAX_RECORD_BYTES)
        || (!large && !bytes.starts_with("UMPROW01")))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree row metadata header");
    BinaryReader checksum{bytes.substr(bytes.size() - 4)};
    if (checksum.number(4) != crc(bytes.substr(0, bytes.size() - 4)))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree row metadata checksum mismatch");
    BinaryReader in{bytes.substr(8, bytes.size() - 12)};
    const auto count = in.count(large ? MAX_PART_ROWS : MAX_UNIT_ROWS, 50);
    if (!count)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Empty UpsertMergeTree part metadata");
    PartRows rows;
    rows.reserve(count);
    CurrentMemoryTracker::check();
    for (size_t i = 0; i < count; ++i)
    {
        if (i % 65536 == 0)
        {
            if (process)
                process->checkTimeLimit();
            CurrentMemoryTracker::check();
        }
        auto key = in.string(MAX_UNIT_BYTES);
        auto value = decodeIndexValue(in.string(50));
        if (value.row_offset != i)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree physical row offset mismatch");
        rows.push_back({std::move(key), value});
    }
    in.end();
    return rows;
}

static PartRows decodeCompactPartRows(std::string_view bytes, QueryStatus * process)
{
    ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertLineageDecodeMicroseconds);
    if (bytes.size() < PART_ROWS_ENVELOPE_BYTES || !bytes.starts_with("UMPCOL01"))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree compact lineage header");
    BinaryReader checksum{bytes.substr(bytes.size() - 4)};
    if (checksum.number(4) != crc(bytes.substr(0, bytes.size() - 4)))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree compact lineage checksum mismatch");
    BinaryReader in{bytes.substr(8, bytes.size() - 12)};
    const auto count = in.count(MAX_PART_ROWS, 37);
    if (!count)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Empty UpsertMergeTree compact lineage");
    PartRows rows;
    rows.reserve(count);
    CurrentMemoryTracker::check();
    const auto check = [process](size_t row)
    {
        if (row % 65536 == 0)
        {
            if (process)
                process->checkTimeLimit();
            CurrentMemoryTracker::check();
        }
    };
    for (size_t i = 0; i < count; ++i)
    {
        check(i);
        rows.push_back({in.string(MAX_UNIT_BYTES), {.row_offset = static_cast<UInt32>(i), .row = {}}});
    }
    for (size_t i = 0; i < count; ++i)
    {
        check(i);
        const auto flags = in.number(1);
        if (flags & ~UInt64{3})
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown UpsertMergeTree compact lineage flags");
        rows[i].value.row.self_binding = flags & 2;
        if (flags & 1)
            rows[i].value.row.version = 0;
    }
    for (size_t i = 0; i < count; ++i)
    {
        check(i);
        rows[i].value.row.operation_id = in.id();
    }
    for (size_t i = 0; i < count; ++i)
    {
        check(i);
        rows[i].value.row.block_ordinal = in.number();
    }
    for (size_t i = 0; i < count; ++i)
    {
        check(i);
        rows[i].value.row.write_csn = in.number();
    }
    String validation_scratch;
    for (size_t i = 0; i < count; ++i)
    {
        check(i);
        if (rows[i].value.row.version)
            rows[i].value.row.version = in.number();
        /// Reuse the legacy validator for identity and `SELF`/resolved `CSN` semantics.
        encodeIndexValue(rows[i].value, validation_scratch);
    }
    in.end();
    return rows;
}

namespace
{
constexpr size_t LINEAGE_COMPRESSED_HEADER_SIZE = 32;
constexpr size_t LINEAGE_BLOCK_SIZE = 1U << 20;
constexpr size_t FRAME_CHECKSUM_SIZE = 16;

struct LineageHeader
{
    size_t rows;
    size_t decoded_size;
    size_t logical_size;
    UInt8 codec;
    UInt8 inner_format;
};

LineageHeader compressedLineageHeader(std::string_view bytes, size_t decoded_limit)
{
    if (bytes.size() < LINEAGE_COMPRESSED_HEADER_SIZE || !bytes.starts_with("UMPROW03"))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree compressed lineage header");
    BinaryReader in{bytes.substr(8, LINEAGE_COMPRESSED_HEADER_SIZE - 8)};
    const auto version = in.number(1);
    const auto codec = in.number(1);
    const auto inner_format = in.number(1);
    const auto flags = in.number(1);
    const auto rows = in.number(4);
    const auto decoded_size = in.number();
    const auto reserved = in.number(4);
    const auto checksum = in.number(4);
    if (version != 1 || inner_format > 1 || flags || reserved
        || (codec != static_cast<UInt8>(CompressionMethodByte::ZSTD)
            && codec != static_cast<UInt8>(CompressionMethodByte::LZ4)
            && codec != static_cast<UInt8>(CompressionMethodByte::NONE))
        || checksum != crc(bytes.substr(0, LINEAGE_COMPRESSED_HEADER_SIZE - 4))
        || !rows || rows > MAX_PART_ROWS || decoded_size > decoded_limit
        || decoded_size < PART_ROWS_ENVELOPE_BYTES + rows * (inner_format == 0 ? UInt64{50} : UInt64{37}))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree compressed lineage bounds or checksum");
    /// Only inner format 1 elides exactly 13 bytes per row from the legacy logical representation.
    const UInt64 elided = inner_format == 1 ? rows * COMPACT_ELIDED_BYTES_PER_ROW : 0;
    if (elided > decoded_limit - decoded_size)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree compact lineage exceeds its logical byte limit");
    return {static_cast<size_t>(rows), static_cast<size_t>(decoded_size), static_cast<size_t>(decoded_size + elided),
        static_cast<UInt8>(codec), static_cast<UInt8>(inner_format)};
}

String decompressPartRows(std::string_view bytes, size_t decoded_limit, size_t & expected_rows, UInt8 & inner_format, QueryStatus * process)
{
    const auto header = compressedLineageHeader(bytes, decoded_limit);
    const auto codec = CompressionCodecFactory::instance().get(header.codec);
    const auto frames = bytes.substr(LINEAGE_COMPRESSED_HEADER_SIZE);
    size_t decoded_total = 0;
    /// Validate every frame before allocating the decoded payload; no declared size is trusted.
    for (size_t offset = 0; offset < frames.size();)
    {
        if (frames.size() - offset < FRAME_CHECKSUM_SIZE + COMPRESSED_BLOCK_HEADER_SIZE)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Truncated UpsertMergeTree lineage compression frame");
        const auto * block = frames.data() + offset + FRAME_CHECKSUM_SIZE;
        BinaryReader sizes{std::string_view(block + 1, 8)};
        const auto compressed = static_cast<UInt32>(sizes.number(4));
        const auto decoded = static_cast<UInt32>(sizes.number(4));
        if (ICompressionCodec::readMethod(block) != header.codec || !decoded || decoded > LINEAGE_BLOCK_SIZE
            || compressed < COMPRESSED_BLOCK_HEADER_SIZE || compressed > codec->getCompressedReserveSize(decoded)
            || compressed > frames.size() - offset - FRAME_CHECKSUM_SIZE
            || decoded > header.decoded_size - decoded_total)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree lineage compression frame bounds");
        decoded_total += decoded;
        offset += FRAME_CHECKSUM_SIZE + compressed;
    }
    if (decoded_total != header.decoded_size)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree lineage decoded size mismatch");
    String decoded(decoded_total + codec->getAdditionalSizeAtTheEndOfBuffer(), '\0');
    CurrentMemoryTracker::check();
    size_t output_offset = 0;
    size_t frame_count = 0;
    for (size_t offset = 0; offset < frames.size();)
    {
        if (process && frame_count++ % 64 == 0)
            process->checkTimeLimit();
        const auto * block = frames.data() + offset + FRAME_CHECKSUM_SIZE;
        const auto compressed = codec->readCompressedBlockSize(block);
        BinaryReader checksum{frames.substr(offset, FRAME_CHECKSUM_SIZE)};
        const auto low = checksum.number();
        const auto high = checksum.number();
        const auto actual = CityHash_v1_0_2::CityHash128(block, compressed);
        if (low != actual.low64 || high != actual.high64)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree lineage compression frame checksum mismatch");
        const auto produced = codec->decompress(block, compressed, decoded.data() + output_offset);
        if (produced != codec->readDecompressedBlockSize(block))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree lineage frame decoded length mismatch");
        output_offset += produced;
        offset += FRAME_CHECKSUM_SIZE + compressed;
        CurrentMemoryTracker::check();
    }
    decoded.resize(decoded_total);
    if (header.inner_format == 1 ? !decoded.starts_with("UMPCOL01")
        : (!decoded.starts_with("UMPROW01") && !decoded.starts_with("UMPROW02")))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree compressed lineage inner format");
    expected_rows = header.rows;
    inner_format = header.inner_format;
    return decoded;
}
}

size_t partRowsStoredSizeLimit(size_t decoded_limit)
{
    if (decoded_limit > MAX_PART_LINEAGE_BYTES)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Invalid UpsertMergeTree decoded lineage limit");
    static const size_t overhead = []
    {
        size_t result = 0;
        for (const auto method : {CompressionMethodByte::ZSTD, CompressionMethodByte::LZ4, CompressionMethodByte::NONE})
        {
            const auto codec = CompressionCodecFactory::instance().get(static_cast<UInt8>(method));
            result = std::max(result, FRAME_CHECKSUM_SIZE + codec->getCompressedReserveSize(LINEAGE_BLOCK_SIZE) - LINEAGE_BLOCK_SIZE);
        }
        return result;
    }();
    return LINEAGE_COMPRESSED_HEADER_SIZE + decoded_limit + ((decoded_limit + LINEAGE_BLOCK_SIZE - 1) / LINEAGE_BLOCK_SIZE) * overhead;
}

String compressPartRows(std::string_view bytes, int level)
{
    const bool compact = bytes.starts_with("UMPCOL01");
    const bool large = bytes.starts_with("UMPROW02");
    if ((!compact && !large && !bytes.starts_with("UMPROW01")) || bytes.size() < PART_ROWS_ENVELOPE_BYTES
        || bytes.size() > (large || compact ? MAX_PART_LINEAGE_BYTES : MAX_RECORD_BYTES))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid UpsertMergeTree lineage compression input");
    BinaryReader row_count{bytes.substr(8, 4)};
    BinaryWriter header;
    header.bytes = "UMPROW03";
    header.number(1, 1);
    header.number(static_cast<UInt8>(CompressionMethodByte::ZSTD), 1);
    header.number(compact ? 1 : 0, 1);
    header.number(0, 1);
    const auto count = row_count.number(4);
    if (!count || count > MAX_PART_ROWS || (compact && count > (MAX_PART_LINEAGE_BYTES - bytes.size()) / COMPACT_ELIDED_BYTES_PER_ROW))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid UpsertMergeTree lineage compression logical size or rows");
    header.number(count, 4);
    header.number(bytes.size());
    header.number(0, 4);
    header.number(crc(header.bytes), 4);
    WriteBufferFromOwnString out;
    out.write(header.bytes.data(), header.bytes.size());
    CompressedWriteBuffer compressed(out, CompressionCodecFactory::instance().get("ZSTD", level), LINEAGE_BLOCK_SIZE);
    compressed.write(bytes.data(), bytes.size());
    compressed.finalize();
    out.finalize();
    return out.str();
}

PartRows decodePartRows(std::string_view bytes, size_t decoded_limit, QueryStatus * process)
{
    if (!bytes.starts_with("UMPROW03"))
        return decodeRawPartRows(bytes, decoded_limit, process);
    if (bytes.size() > partRowsStoredSizeLimit(decoded_limit))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree compressed lineage exceeds its stored byte limit");
    try
    {
        size_t expected_rows = 0;
        UInt8 inner_format = 0;
        const auto decoded = decompressPartRows(bytes, decoded_limit, expected_rows, inner_format, process);
        auto rows = inner_format == 1 ? decodeCompactPartRows(decoded, process) : decodeRawPartRows(decoded, decoded_limit, process);
        if (rows.size() != expected_rows)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree compressed lineage row count mismatch");
        return rows;
    }
    catch (const Exception & exception)
    {
        if (exception.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED || exception.code() == ErrorCodes::CANNOT_ALLOCATE_MEMORY
            || exception.code() == ErrorCodes::QUERY_WAS_CANCELLED || exception.code() == ErrorCodes::TIMEOUT_EXCEEDED
            || exception.code() == ErrorCodes::ABORTED || exception.code() == ErrorCodes::CORRUPTED_DATA)
            throw;
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Cannot decode UpsertMergeTree compressed lineage: {}", exception.message());
    }

}

size_t readPartRowsDecodedSize(const String & path, size_t stored_size, size_t rows)
{
    char header[LINEAGE_COMPRESSED_HEADER_SIZE];
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0)
        throw ErrnoException(ErrorCodes::CANNOT_OPEN_FILE, "Cannot open UpsertMergeTree lineage planner header {}", path);
    SCOPE_EXIT({ ::close(fd); });
    ssize_t read_size;
    do
    {
        read_size = ::pread(fd, header, sizeof(header), 0);
    }
    while (read_size < 0 && errno == EINTR);
    if (read_size < 0)
        throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read UpsertMergeTree lineage planner header {}", path);
    const std::string_view prefix(header, static_cast<size_t>(read_size));
    size_t decoded_size = stored_size;
    if (prefix.starts_with("UMPROW03"))
    {
        const auto parsed = compressedLineageHeader(prefix, MAX_PART_LINEAGE_BYTES);
        if (parsed.rows != rows || stored_size > partRowsStoredSizeLimit(MAX_PART_LINEAGE_BYTES))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree lineage planner size or row count");
        decoded_size = parsed.logical_size;
    }
    else if ((!prefix.starts_with("UMPROW01") && !prefix.starts_with("UMPROW02"))
        || stored_size > (prefix.starts_with("UMPROW02") ? MAX_PART_LINEAGE_BYTES : MAX_RECORD_BYTES))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree lineage planner magic or size");
    if (decoded_size < PART_ROWS_ENVELOPE_BYTES + rows * UInt64{50})
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree lineage planner size is too small");
    return decoded_size;
}

ValidatedPartRows::ValidatedPartRows(PartRows rows_, ObjectID part_) : rows(std::move(rows_)), part(part_)
{
    ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertLineageValidationMicroseconds);
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

size_t foldBlock(Block & block, const StorageInMemoryMetadata & metadata, const String & version_column, size_t max_key_bytes, QueryStatus * process)
{
    ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertInputFoldMicroseconds);
    auto keys = encodeKeys(block, metadata, max_key_bytes);
    std::optional<VersionColumn> versions;
    if (!version_column.empty())
        versions.emplace(block.getByName(version_column));
    const auto & ordinals = *block.getByName(ORDINAL_COLUMN).column;
    const auto winners = selectInputWinners(std::span<const String>(keys.data(), keys.size()),
        [&](size_t row, size_t old)
        {
            return !versions || versions->get(row) > versions->get(old)
                || (versions->get(row) == versions->get(old) && ordinals.getUInt(row) > ordinals.getUInt(old));
        },
        [&]
        {
            if (process)
                process->checkTimeLimit();
            CurrentMemoryTracker::check();
        });
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

MergeTreeDataPartChecksum ttlPolicyChecksum(const StorageInMemoryMetadata & metadata)
{
    const auto policy = metadata.table_ttl.definition_ast ? metadata.table_ttl.definition_ast->formatWithSecretsOneLine() : String{};
    WriteBufferFromOwnString out;
    HashingWriteBuffer hashing(out);
    hashing.write(policy.data(), policy.size());
    hashing.finalize();
    out.finalize();
    return {policy.size(), hashing.getHash()};
}

size_t writePartIndex(
    IDataPartStorage & storage, const Block & block, const StorageInMemoryMetadata & metadata,
    const String & version_column, ObjectID operation_id, const IColumn::Permutation * permutation,
    size_t max_key_bytes, ContextPtr context, MergeTreeDataPartChecksums & checksums, bool compress_lineage, int compression_level, bool compact_lineage)
{
    ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::UpsertPartIndexMicroseconds);
    const auto keys = encodeKeys(block, metadata, max_key_bytes);
    CurrentMemoryTracker::check();
    const auto & ordinals = *block.getByName(ORDINAL_COLUMN).column;
    std::optional<VersionColumn> versions;
    if (!version_column.empty())
        versions.emplace(block.getByName(version_column));
    PartRows rows;
    rows.reserve(block.rows());
    for (size_t offset = 0; offset < block.rows(); ++offset)
    {
        if (offset % 65536 == 0)
            CurrentMemoryTracker::check();
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
    if (compact_lineage && !compress_lineage)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Compact UpsertMergeTree lineage requires compression");
    auto bytes = encodePartRows(rows, compact_lineage);
    const auto decoded_size = bytes.size() + (compact_lineage ? rows.size() * COMPACT_ELIDED_BYTES_PER_ROW : 0);
    if (compress_lineage)
        bytes = compressPartRows(bytes, compression_level);
    auto out = storage.writeFile(ROWS_FILE, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());
    HashingWriteBuffer hashing(*out);
    hashing.write(bytes.data(), bytes.size());
    const auto hash = hashing.getHash();
    hashing.finalize();
    out->finalize();
    /// The coordinator synchronizes the complete finalized part before durable authorization.
    checksums.addFile(ROWS_FILE, bytes.size(), hash);

    if (metadata.table_ttl.definition_ast)
    {
        const auto policy = metadata.table_ttl.definition_ast->formatWithSecretsOneLine();
        auto ttl_out = storage.writeFile(TTL_FILE, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());
        HashingWriteBuffer ttl_hashing(*ttl_out);
        ttl_hashing.write(policy.data(), policy.size());
        const auto ttl_hash = ttl_hashing.getHash();
        ttl_hashing.finalize();
        ttl_out->finalize();
        checksums.addFile(TTL_FILE, policy.size(), ttl_hash);
    }

    /// `UpsertMergeTree` winner lookups use the state database, so new parts need no duplicate `SST`.
    /// Old parts retain their authenticated `SST`; any future reader must authenticate its inventory entry.
    return decoded_size;
}

namespace
{
using InventoryFiles = std::map<String, std::pair<UInt64, ObjectID>>;

InventoryFiles parseInventoryFiles(const String & inventory)
{
    InventoryFiles files;
    BinaryReader in{inventory};
    while (!in.bytes.empty())
    {
        const auto name = in.string();
        if (name.ends_with('/'))
            continue;
        const auto size = in.number();
        const auto digest = in.id();
        if (!files.emplace(name, std::make_pair(size, digest)).second)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Duplicate UpsertMergeTree inventory entry");
    }
    return files;
}

ObjectID hashPartDirectoryImpl(const String & path, bool sync_files, String * result_inventory, const String * trusted_inventory)
{
    namespace fs = std::filesystem;
    Stopwatch inventory_watch;
    const auto expected_files = trusted_inventory ? parseInventoryFiles(*trusted_inventory) : InventoryFiles{};
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
        const auto relative = fs::relative(file, path).string();
        if (trusted_inventory)
        {
            const auto expected = expected_files.find(relative);
            if (expected == expected_files.end() || fs::file_size(file) != expected->second.first)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree inventory size or name mismatch for {}", file.string());
            if (file.extension() == ".bin" || file.filename() == SSTIndexWriter::FILE_NAME)
            {
                inventory.string(relative);
                inventory.number(expected->second.first);
                inventory.id(expected->second.second);
                continue;
            }
        }
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
    ProfileEvents::increment(ProfileEvents::UpsertInventoryHashMicroseconds, inventory_watch.elapsedMicroseconds());
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
    const auto digest = hashLogRecord(inventory.bytes);
    if (result_inventory)
        *result_inventory = std::move(inventory.bytes);
    return digest;
}
}

ObjectID hashPartDirectory(const String & path, bool sync_files, String * inventory)
{
    return hashPartDirectoryImpl(path, sync_files, inventory, nullptr);
}

void verifyPartMetadata(const String & path, ObjectID expected, const String & inventory)
{
    if (inventory.empty() || inventory.size() > MAX_RECORD_BYTES || hashLogRecord(inventory) != expected
        || hashPartDirectoryImpl(path, false, nullptr, &inventory) != expected)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree authenticated inventory or metadata mismatch for {}", path);
}

String readInventoryFile(const String & part_path, const String & relative, const String & inventory, size_t max_bytes)
{
    const auto files = parseInventoryFiles(inventory);
    const auto expected = files.find(relative);
    if (expected == files.end())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree inventory lacks sidecar {}", relative);
    auto bytes = readBoundedFile(std::filesystem::path(part_path) / relative, max_bytes);
    /// `hashLogRecord` uses the same `SipHash` digest as the complete part inventory.
    if (bytes.size() != expected->second.first || hashLogRecord(bytes) != expected->second.second)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree authenticated sidecar mismatch for {}", relative);
    return bytes;
}
}
