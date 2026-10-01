#pragma once

#include <base/types.h>
#include <algorithm>
#include <atomic>

namespace DB::Upsert
{
/// Only the maintenance task changes cycle state. Publishers may call `notify` concurrently.
class MaintenanceSchedule
{
    std::atomic<UInt64> generation{0};
    std::atomic<bool> idle{false};
    UInt64 cycle_generation = 0;
    UInt64 observed_generation = 0;
    UInt64 idle_delay_ms = 1000;
    bool cycle_did_work = false;

public:
    /// The caller schedules the task immediately when this returns true.
    bool notify()
    {
        generation.fetch_add(1);
        return idle.exchange(false);
    }

    void beginCycle()
    {
        cycle_generation = generation.load();
        if (cycle_generation != observed_generation)
            idle_delay_ms = 1000;
        observed_generation = cycle_generation;
        cycle_did_work = false;
        idle.store(false);
    }

    UInt64 finishSlice(bool completed_cycle, bool did_work, bool has_ttl)
    {
        cycle_did_work |= did_work;
        if (!completed_cycle)
            return 100;
        if (cycle_did_work)
        {
            idle_delay_ms = 1000;
            return 100;
        }
        /// A healthy cycle with new publications gets a bounded cooldown, not an immediate full rescan.
        if (generation.load() != cycle_generation)
        {
            idle_delay_ms = 1000;
            return 1000;
        }
        /// Publish the idle flag before checking for commits made during this cycle.
        /// A later publisher calls `schedule`, which takes precedence over `scheduleAfter`.
        idle.store(true);
        if (generation.load() != cycle_generation)
        {
            idle.store(false);
            idle_delay_ms = 1000;
            return 1000;
        }
        /// Time-driven expiration and resuming TTL processing must not wait for a long idle backoff.
        const auto delay = has_ttl ? UInt64{1000} : idle_delay_ms;
        idle_delay_ms = std::min<UInt64>(idle_delay_ms * 2, 30000);
        return delay;
    }
};
}
