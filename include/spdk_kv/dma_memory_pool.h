// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <spdk/env.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

#include <pthread.h>
#include <sched.h>

#ifdef __linux__
#include <numa.h>
#include <numaif.h>
#endif

#include "spdk_kv/types.h"

namespace spdk_kv {

// NUMA-aware DMA memory pool with fixed-size block allocation
class DmaMemoryPool {
public:
    DmaMemoryPool(size_t block_size, size_t block_count, int numa_node = -1)
            : pool_base_(nullptr),
              block_size_(block_size),
              block_count_(block_count),
              numa_node_(numa_node) {
#ifdef __linux__
        if (numa_node_ < 0) {
            numa_node_ = get_current_numa_node();
        }
#endif

        // Allocate NUMA-local DMA memory
        size_t total_size = block_size_ * block_count_;
        pool_base_ = allocate_numa_memory(total_size, numa_node_);

        if (!pool_base_) {
            // Fallback to standard SPDK allocation
            pool_base_ = spdk_dma_malloc(total_size, kPageSize, nullptr);
            if (!pool_base_) {
                // Last resort: aligned_alloc
                pool_base_ = std::aligned_alloc(kPageSize, total_size);
                if (pool_base_) {
                    std::memset(pool_base_, 0, total_size);
                }
            }
        }

        if (!pool_base_) {
            return;  // Caller should check is_valid()
        }

        // initialize free list
        free_list_.reserve(block_count_);
        for (size_t i = 0; i < block_count_; i++) {
            free_list_.push_back(static_cast<char*>(pool_base_) + i * block_size_);
        }
    }

    ~DmaMemoryPool() {
        free_numa_memory(pool_base_, block_size_ * block_count_);
    }

    // Non-copyable
    DmaMemoryPool(const DmaMemoryPool&) = delete;
    DmaMemoryPool& operator=(const DmaMemoryPool&) = delete;

    // Check if pool was initialized successfully
    bool is_valid() const { return pool_base_ != nullptr; }

    // Allocate one block from the pool
    void* alloc() {
        if (free_list_.empty()) {
            return nullptr;
        }
        void* ptr = free_list_.back();
        free_list_.pop_back();
        return ptr;
    }

    // Return a block to the pool
    void free(void* ptr) {
        if (ptr) {
            free_list_.push_back(ptr);
        }
    }

    // Accessors
    size_t block_size() const { return block_size_; }
    size_t available() const { return free_list_.size(); }
    size_t total_blocks() const { return block_count_; }
    int numa_node() const { return numa_node_; }
    void* pool_base() const { return pool_base_; }
    size_t pool_size() const { return block_size_ * block_count_; }

private:
    static int get_current_numa_node() {
#ifdef __linux__
        return numa_node_of_cpu(sched_getcpu());
#else
        return 0;
#endif
    }

    static void* allocate_numa_memory(size_t size, int numa_node) {
        void* ptr = spdk_dma_malloc_socket(size, kPageSize, nullptr, numa_node);

#ifdef __linux__
        if (ptr && numa_node >= 0) {
            unsigned long nodemask = 1UL << numa_node;
            mbind(ptr, size, MPOL_BIND, &nodemask, sizeof(nodemask) * 8,
                  MPOL_MF_STRICT | MPOL_MF_MOVE);
        }
#endif

        return ptr;
    }

    static void free_numa_memory(void* ptr, size_t /*size*/) {
        if (ptr) {
            spdk_dma_free(ptr);
        }
    }

    void* pool_base_;
    size_t block_size_;
    size_t block_count_;
    int numa_node_;
    std::vector<void*> free_list_;
};

// CPU core pinning and NUMA affinity utility
class EngineThread {
public:
    // Pin current thread to a specific CPU core
    static bool pin_to_core(int core_id) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);

        int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
        if (rc != 0) {
            return false;
        }

#ifdef __linux__
        // set memory allocation policy to local NUMA node
        int numa_node = numa_node_of_cpu(core_id);
        numa_set_preferred(numa_node);
#endif

        return true;
    }

    // get NUMA node for a given CPU core
    static int get_numa_node(int core_id) {
#ifdef __linux__
        return numa_node_of_cpu(core_id);
#else
        (void)core_id;
        return 0;
#endif
    }

    // get current CPU core
    static int get_current_core() {
        return sched_getcpu();
    }

    // get NUMA node of current thread
    static int get_current_numa_node() {
#ifdef __linux__
        return numa_node_of_cpu(sched_getcpu());
#else
        return 0;
#endif
    }
};

// Predefined memory pools (NUMA-aware) for common allocation patterns
struct MemoryPools {
    int numa_node;

    // AppendBuffer pool (2MB x 4, multi-buffer rotation)
    DmaMemoryPool append_pool;

    // Read buffer pool (64KB x 256)
    DmaMemoryPool read_pool;

    // Small buffer pool for header reads etc (4KB x 1024)
    DmaMemoryPool small_pool;

    explicit MemoryPools(int core_id)
            : numa_node(EngineThread::get_numa_node(core_id)),
              append_pool(2 * 1024 * 1024, 4, numa_node),
              read_pool(64 * 1024, 256, numa_node),
              small_pool(4 * 1024, 1024, numa_node) {}

    bool is_valid() const {
        return append_pool.is_valid() && read_pool.is_valid() && small_pool.is_valid();
    }
};

// Reference-counted DMA buffer for safe RDMA/SPDK buffer lifecycle management
//
// Use case: When a buffer is used by both SPDK IO and RDMA send simultaneously,
// the buffer must not be returned to the pool until both operations complete.
// RefCountedBuffer ensures the buffer is released only when the last reference
// is dropped.
class RefCountedBuffer {
public:
    RefCountedBuffer(void* data, size_t size, DmaMemoryPool* pool)
            : data_(data), size_(size), pool_(pool), ref_count_(1) {}

    // Prevent copying
    RefCountedBuffer(const RefCountedBuffer&) = delete;
    RefCountedBuffer& operator=(const RefCountedBuffer&) = delete;

    // Increment reference count
    void add_ref() {
        ref_count_.fetch_add(1, std::memory_order_relaxed);
    }

    // Decrement reference count; returns true if buffer was freed
    bool release() {
        if (ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            // Last reference: return buffer to pool
            if (pool_) {
                pool_->free(data_);
            }
            delete this;
            return true;
        }
        return false;
    }

    void* data() { return data_; }
    const void* data() const { return data_; }
    size_t size() const { return size_; }
    int ref_count() const { return ref_count_.load(std::memory_order_relaxed); }

private:
    ~RefCountedBuffer() = default;  // Only release() can destroy

    void* data_;
    size_t size_;
    DmaMemoryPool* pool_;
    std::atomic<int> ref_count_;
};

}  // namespace spdk_kv
