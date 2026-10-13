#pragma once

#include <Common/MemoryTrackerBlockerInThread.h>
#include <memory>

namespace DB::Upsert
{
/// Global accounting for shared control blocks, including release on another query's thread.
template <typename T>
struct GlobalAllocator
{
    using value_type = T;
    GlobalAllocator() = default;
    template <typename U>
    GlobalAllocator(const GlobalAllocator<U> &) {}

    T * allocate(size_t size)
    {
        MemoryTrackerBlockerInThread blocker(VariableContext::User);
        return std::allocator<T>{}.allocate(size);
    }
    void deallocate(T * pointer, size_t size) noexcept
    {
        MemoryTrackerBlockerInThread blocker(VariableContext::User);
        std::allocator<T>{}.deallocate(pointer, size);
    }
    template <typename U>
    bool operator==(const GlobalAllocator<U> &) const { return true; }
};
}
