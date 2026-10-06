#pragma once

#include <base/types.h>
#include <Common/CurrentMetrics.h>
#include <atomic>

namespace CurrentMetrics
{
    extern const Metric UpsertForegroundOperationWaiters;
}

namespace DB::Upsert
{
class ForegroundOperationWait
{
public:
    explicit ForegroundOperationWait(std::atomic<UInt64> & waiting_, bool enabled = true)
        : waiting(waiting_), pending(enabled)
    {
        if (pending)
        {
            waiting.fetch_add(1, std::memory_order_relaxed);
            CurrentMetrics::add(CurrentMetrics::UpsertForegroundOperationWaiters);
        }
    }
    ForegroundOperationWait(const ForegroundOperationWait &) = delete;
    ForegroundOperationWait & operator=(const ForegroundOperationWait &) = delete;
    void acquired()
    {
        if (pending)
        {
            waiting.fetch_sub(1, std::memory_order_relaxed);
            CurrentMetrics::sub(CurrentMetrics::UpsertForegroundOperationWaiters);
            pending = false;
        }
    }
    ~ForegroundOperationWait()
    {
        acquired();
    }
private:
    std::atomic<UInt64> & waiting;
    bool pending;
};
}
