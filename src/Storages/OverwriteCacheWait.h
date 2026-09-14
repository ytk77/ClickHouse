#pragma once

#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>

#include <chrono>
#include <condition_variable>
#include <mutex>

namespace ProfileEvents
{
    extern const Event OverwriteCacheInsertAdmissionWaits;
    extern const Event OverwriteCachePublicationWaits;
    extern const Event OverwriteCachePersistenceQueueWaits;
    extern const Event OverwriteCachePersistenceDurableWaits;
    extern const Event OverwriteCacheWriterLockWaits;
}

namespace DB
{

/// Query cancellation does not notify storage condition variables. Bound the interval between checks
/// without delaying a notification from the operation we are waiting for.
template <typename Predicate>
void waitForOverwriteCache(std::condition_variable & changed, std::unique_lock<std::mutex> & lock, Predicate ready,
    ProfileEvents::Event wait_event)
{
    bool counted = false;
    while (true)
    {
        CurrentThread::checkIfNotCancelled();
        if (ready())
            return;
        if (!counted)
        {
            ProfileEvents::increment(wait_event);
            counted = true;
        }
        changed.wait_for(lock, std::chrono::milliseconds(100));
    }
}

inline std::unique_lock<std::timed_mutex> lockOverwriteCacheWriter(std::timed_mutex & mutex)
{
    std::unique_lock lock(mutex, std::defer_lock);
    CurrentThread::checkIfNotCancelled();
    if (!lock.try_lock())
    {
        ProfileEvents::increment(ProfileEvents::OverwriteCacheWriterLockWaits);
        do
        {
            CurrentThread::checkIfNotCancelled();
        }
        while (!lock.try_lock_for(std::chrono::milliseconds(100)));
    }
    CurrentThread::checkIfNotCancelled();
    return lock;
}

}
