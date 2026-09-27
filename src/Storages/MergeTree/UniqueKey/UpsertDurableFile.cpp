#include <Storages/MergeTree/UniqueKey/UpsertDurableFile.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/setThreadName.h>
#include <IO/SharedThreadPools.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <filesystem>

namespace ProfileEvents
{
extern const Event FileSync;
extern const Event FileSyncElapsedMicroseconds;
extern const Event DirectorySync;
extern const Event DirectorySyncElapsedMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CANNOT_FSYNC;
    extern const int CORRUPTED_DATA;
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
extern const char upsert_throw_before_payload_sync[];
}

namespace Upsert
{
namespace
{
class FileDescriptor
{
public:
    explicit FileDescriptor(const String & path, int flags)
        : fd(::open(path.c_str(), flags | O_CLOEXEC | O_NOFOLLOW, 0600)), directory(flags & O_DIRECTORY)
    {
        if (fd < 0)
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_OPEN_FILE, path, "Cannot open UpsertMergeTree file {}", path);
    }
    ~FileDescriptor()
    {
        ::close(fd);
    }
    FileDescriptor(const FileDescriptor &) = delete;
    FileDescriptor & operator=(const FileDescriptor &) = delete;
    int get() const
    {
        return fd;
    }
    void sync() const
    {
        ProfileEvents::increment(directory ? ProfileEvents::DirectorySync : ProfileEvents::FileSync);
        Stopwatch watch;
        int result;
        do
        {
            result = ::fsync(fd);
        } while (result < 0 && errno == EINTR);
        ProfileEvents::increment(directory ? ProfileEvents::DirectorySyncElapsedMicroseconds : ProfileEvents::FileSyncElapsedMicroseconds,
            watch.elapsedMicroseconds());
        if (result < 0)
            ErrnoException::throwWithErrno(ErrorCodes::CANNOT_FSYNC, errno, "Cannot sync UpsertMergeTree file or directory");
    }

private:
    int fd;
    bool directory;
};
}

void syncDirectory(const String & path)
{
    FileDescriptor directory(path, O_RDONLY | O_DIRECTORY);
    directory.sync();
}

void syncFiles(const std::vector<String> & paths)
{
    auto sync = [](const String & path)
    {
        FileDescriptor file(path, O_RDONLY);
        fiu_do_on(FailPoints::upsert_throw_before_payload_sync,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected UpsertMergeTree failure before payload sync");
        });
        file.sync();
    };
    if (paths.size() <= 1 || !getIOThreadPool().isInitialized())
    {
        for (const auto & path : paths)
            sync(path);
        return;
    }
    ThreadPoolCallbackRunnerLocal<void> runner(getIOThreadPool().get(), ThreadName::DEFAULT_THREAD_POOL);
    for (const auto & path : paths)
        runner.enqueueAndKeepTrack([path, sync]
        {
            sync(path);
        });
    runner.waitForAllToFinishAndRethrowFirstError();
}

void createDurableFile(const String & path, std::string_view contents)
{
    FileDescriptor file(path, O_WRONLY | O_CREAT | O_EXCL);
    while (!contents.empty())
    {
        const auto written = ::write(file.get(), contents.data(), contents.size());
        if (written < 0 && errno == EINTR)
            continue;
        if (written <= 0)
            ErrnoException::throwWithErrno(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, errno, "Cannot write UpsertMergeTree file {}", path);
        contents.remove_prefix(static_cast<size_t>(written));
    }
    file.sync();
    syncDirectory(std::filesystem::path(path).parent_path());
}

String readBoundedFile(const String & path, size_t max_bytes)
{
    FileDescriptor file(path, O_RDONLY);
    struct stat info{};
    if (::fstat(file.get(), &info) != 0)
        ErrnoException::throwWithErrno(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno, "Cannot stat UpsertMergeTree file {}", path);
    if (!S_ISREG(info.st_mode) || info.st_size < 0 || static_cast<UInt64>(info.st_size) > max_bytes)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid UpsertMergeTree file type or size: {}", path);
    String contents(static_cast<size_t>(info.st_size), '\0');
    size_t offset = 0;
    while (offset < contents.size())
    {
        const auto count = ::read(file.get(), contents.data() + offset, contents.size() - offset);
        if (count < 0 && errno == EINTR)
            continue;
        if (count < 0)
            ErrnoException::throwWithErrno(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno, "Cannot read UpsertMergeTree file {}", path);
        if (!count)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected end of UpsertMergeTree file {}", path);
        offset += static_cast<size_t>(count);
    }
    char extra;
    ssize_t count;
    do
    {
        count = ::read(file.get(), &extra, 1);
    } while (count < 0 && errno == EINTR);
    if (count < 0)
        ErrnoException::throwWithErrno(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, errno, "Cannot read UpsertMergeTree file {}", path);
    if (count != 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "UpsertMergeTree file changed while reading: {}", path);
    return contents;
}
}
}
