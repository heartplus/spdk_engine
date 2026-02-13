// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <cstddef>
#include <new>
#include <type_traits>

namespace spdk_kv {

// Single-threaded context object pool.
// Pre-allocates N slots to avoid heap allocation on the hot path.
// Falls back to heap allocation when the pool is exhausted.
//
// Template parameters:
//   T - Context type to pool
//   N - Number of pre-allocated slots (default 64)
//
// Usage:
//   CtxPool<MyCtx, 32> pool;
//   MyCtx* ctx = pool.Alloc(arg1, arg2);  // placement-new from pool
//   pool.Free(ctx);                         // destruct + return to pool
template <typename T, size_t N = 64>
class CtxPool {
public:
    CtxPool() : free_count_(N) {
        for (size_t i = 0; i < N; i++) {
            free_list_[i] = &storage_[i];
        }
    }

    ~CtxPool() = default;

    // Non-copyable, non-movable
    CtxPool(const CtxPool&) = delete;
    CtxPool& operator=(const CtxPool&) = delete;

    // Allocate a context object from the pool.
    // Uses aggregate initialization (brace-init) for the object.
    template <typename... Args>
    T* Alloc(Args&&... args) {
        void* slot;
        if (free_count_ > 0) {
            slot = free_list_[--free_count_];
        } else {
            // Pool exhausted: fall back to heap
            slot = ::operator new(sizeof(T));
        }
        return new (slot) T{std::forward<Args>(args)...};
    }

    // Free a context object back to the pool.
    void Free(T* ptr) {
        if (!ptr) {
            return;
        }
        // Destruct the object (handles std::function cleanup etc.)
        if constexpr (!std::is_trivially_destructible_v<T>) {
            ptr->~T();
        }
        if (IsFromPool(ptr)) {
            free_list_[free_count_++] = ptr;
        } else {
            ::operator delete(ptr);
        }
    }

    size_t Available() const { return free_count_; }
    static constexpr size_t Capacity() { return N; }

private:
    bool IsFromPool(const T* ptr) const {
        auto* base = reinterpret_cast<const char*>(&storage_[0]);
        auto* end = reinterpret_cast<const char*>(&storage_[N]);
        auto* p = reinterpret_cast<const char*>(ptr);
        return p >= base && p < end;
    }

    std::aligned_storage_t<sizeof(T), alignof(T)> storage_[N];
    void* free_list_[N];
    size_t free_count_;
};

// Specialization hint: for types that need explicit default-construction
// (e.g., WritevCtx with iovec array), use Alloc() with no arguments
// then manually assign fields.

}  // namespace spdk_kv
