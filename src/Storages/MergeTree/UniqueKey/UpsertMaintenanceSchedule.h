#pragma once

#include <base/types.h>
#include <algorithm>
#include <atomic>
#include <chrono>

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
    UInt64 memory_failure_delay_ms = 0;
    UInt64 admission_delay_ms = 0;
    UInt64 precheck_delay_ms = 0;
    UInt64 last_idle_delay_ms = 1000;
    UInt64 admission_denials = 0;
    std::chrono::steady_clock::time_point retry_after{};

public:
    /// Only the maintenance task accesses retry state. Notifications cannot bypass this deadline.
    UInt64 remainingRetryDelay() const
    {
        const auto remaining = retry_after - std::chrono::steady_clock::now();
        if (remaining <= std::chrono::steady_clock::duration::zero())
            return 0;
        return std::chrono::ceil<std::chrono::milliseconds>(remaining).count();
    }

    UInt64 memoryFailure()
    {
        idle.store(false);
        memory_failure_delay_ms = memory_failure_delay_ms ? std::min<UInt64>(60000, memory_failure_delay_ms * 2) : 1000;
        retry_after = std::chrono::steady_clock::now() + std::chrono::milliseconds(memory_failure_delay_ms);
        return memory_failure_delay_ms;
    }

    UInt64 precheckDeferral(UInt64 jitter_ms)
    {
        idle.store(false);
        precheck_delay_ms = precheck_delay_ms ? std::min<UInt64>(10000, precheck_delay_ms * 2) : 1000;
        const UInt64 idle_floor = generation.load() == cycle_generation ? last_idle_delay_ms : 1000;
        const auto delay = std::max(precheck_delay_ms, idle_floor) + jitter_ms;
        retry_after = std::chrono::steady_clock::now() + std::chrono::milliseconds(delay);
        return delay;
    }

    UInt64 admissionFailure(UInt64 jitter_ms)
    {
        idle.store(false);
        ++admission_denials;
        admission_delay_ms = admission_delay_ms ? std::min<UInt64>(10000, admission_delay_ms * 2) : 1000;
        const auto delay = admission_delay_ms + jitter_ms;
        retry_after = std::chrono::steady_clock::now() + std::chrono::milliseconds(delay);
        return delay;
    }

    UInt64 consecutiveAdmissionDenials() const
    {
        return admission_denials;
    }

    void admissionSucceeded()
    {
        admission_delay_ms = 0;
        precheck_delay_ms = 0;
        admission_denials = 0;
        retry_after = {};
    }

    void rewriteSucceeded()
    {
        memory_failure_delay_ms = 0;
        retry_after = {};
    }

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
        last_idle_delay_ms = 1000;
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
        last_idle_delay_ms = delay;
        idle_delay_ms = std::min<UInt64>(idle_delay_ms * 2, 30000);
        return delay;
    }
};
}
