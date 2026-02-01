// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <spdk/env.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <thread>

#include <sched.h>
#include <sys/mman.h>

#ifdef __linux__
#include <numa.h>
#include <numaif.h>
#endif

namespace spdk_kv {

// 1GB and 2MB page size constants
constexpr size_t kHugePage1G = 1ULL << 30;  // 1GB
constexpr size_t kHugePage2M = 2ULL << 20;  // 2MB

// MAP_HUGE_1GB / MAP_HUGE_2MB may not be defined on all systems
#ifndef MAP_HUGE_1GB
#define MAP_HUGE_1GB (30 << MAP_HUGE_SHIFT)
#endif

#ifndef MAP_HUGE_2MB
#define MAP_HUGE_2MB (21 << MAP_HUGE_SHIFT)
#endif

class HugePageAllocator {
public:
    enum class PageSize : uint64_t {
        PAGE_4K = 4096,
        PAGE_2M = 2 * 1024 * 1024,
        PAGE_1G = 1024 * 1024 * 1024
    };

    enum class WarmupMode {
        SYNC,     // Synchronous warmup (memset at allocation time)
        ASYNC,    // Async warmup (background thread)
        LAZY,     // Lazy warmup (madvise hint)
        ON_LOAD   // Warmup on load (IndexLoader reads trigger page faults)
    };

    // Allocate 1GB hugepage memory with optional NUMA binding and warmup
    static void* Alloc1GHugepage(size_t size, int numa_node = -1,
                                 WarmupMode warmup = WarmupMode::ON_LOAD) {
        // Align up to 1GB
        size_t aligned_size = (size + kHugePage1G - 1) & ~(kHugePage1G - 1);

        void* ptr = nullptr;

        // Try 1GB hugepages via mmap + MAP_HUGETLB
        int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_1GB;
        ptr = mmap(nullptr, aligned_size, PROT_READ | PROT_WRITE, flags, -1, 0);

        if (ptr == MAP_FAILED) {
            // Fallback to 2MB hugepages
            flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_2MB;
            ptr = mmap(nullptr, aligned_size, PROT_READ | PROT_WRITE, flags, -1, 0);
        }

        if (ptr == MAP_FAILED) {
            return nullptr;
        }

#ifdef __linux__
        // NUMA binding
        if (numa_node >= 0) {
            unsigned long nodemask = 1UL << numa_node;
            mbind(ptr, aligned_size, MPOL_BIND, &nodemask,
                  sizeof(nodemask) * 8, MPOL_MF_STRICT | MPOL_MF_MOVE);
        }
#endif

        ApplyWarmup(ptr, aligned_size, warmup, numa_node);

        return ptr;
    }

    // Free hugepage memory
    static void FreeHugepage(void* ptr, size_t size) {
        if (!ptr) {
            return;
        }
        size_t aligned_size = (size + kHugePage1G - 1) & ~(kHugePage1G - 1);
        munmap(ptr, aligned_size);
    }

    // Allocate via SPDK's DMA allocation (uses 2MB hugepages by default)
    static void* AllocSpdkHugepage(size_t size, int numa_node) {
        void* ptr = spdk_dma_malloc_socket(size, kHugePage1G, nullptr, numa_node);
        if (ptr) {
            std::memset(ptr, 0, size);
        }
        return ptr;
    }

private:
    static void ApplyWarmup(void* ptr, size_t size, WarmupMode warmup, int numa_node) {
        switch (warmup) {
        case WarmupMode::SYNC:
            // Synchronous warmup: block until all pages are faulted in
            std::memset(ptr, 0, size);
            break;

        case WarmupMode::ASYNC:
            StartAsyncWarmup(ptr, size, numa_node);
            break;

        case WarmupMode::LAZY:
            // Hint to kernel that we'll need this memory soon
            madvise(ptr, size, MADV_WILLNEED);
            break;

        case WarmupMode::ON_LOAD:
            // No action: IndexLoader reads will naturally trigger page faults
            break;
        }
    }

    static void StartAsyncWarmup(void* ptr, size_t size, int numa_node) {
        std::thread warmup_thread([ptr, size, numa_node]() {
#ifdef __linux__
            if (numa_node >= 0) {
                cpu_set_t cpuset;
                CPU_ZERO(&cpuset);

                struct bitmask* cpus = numa_allocate_cpumask();
                numa_node_to_cpus(numa_node, cpus);

                for (int i = 0; i < numa_num_possible_cpus(); i++) {
                    if (numa_bitmask_isbitset(cpus, i)) {
                        CPU_SET(i, &cpuset);
                        break;
                    }
                }
                numa_free_cpumask(cpus);

                pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
            }
#else
            (void)numa_node;
#endif

            // Warmup in 1GB chunks
            size_t chunk_size = kHugePage1G;
            char* base = static_cast<char*>(ptr);

            for (size_t offset = 0; offset < size; offset += chunk_size) {
                size_t this_chunk = std::min(chunk_size, size - offset);
                std::memset(base + offset, 0, this_chunk);
            }
        });

        warmup_thread.detach();
    }
};

// MemIndex memory allocator with fallback chain:
// 1GB hugepages -> SPDK 2MB hugepages -> SPDK 4KB pages
class MemIndexAllocator {
public:
    enum class AllocMethod {
        HUGEPAGE_1G,
        SPDK_HUGEPAGE,
        SPDK_4K,
        ALIGNED_ALLOC  // Standard aligned_alloc fallback
    };

    struct AllocResult {
        void* ptr;
        size_t size;
        AllocMethod method;
    };

    // Allocate index memory with automatic fallback
    // For high-QPS production: use SYNC warmup to guarantee 0 page faults at runtime
    static AllocResult AllocateIndexMemory(
            size_t size, int numa_node = -1,
            HugePageAllocator::WarmupMode warmup = HugePageAllocator::WarmupMode::SYNC) {
        AllocResult result = {nullptr, size, AllocMethod::HUGEPAGE_1G};

        // 1. Try 1GB hugepages
        result.ptr = HugePageAllocator::Alloc1GHugepage(size, numa_node, warmup);
        if (result.ptr) {
            result.method = AllocMethod::HUGEPAGE_1G;
            return result;
        }

        // 2. Fallback to SPDK 2MB hugepages
        result.ptr = HugePageAllocator::AllocSpdkHugepage(size, numa_node);
        if (result.ptr) {
            result.method = AllocMethod::SPDK_HUGEPAGE;
            return result;
        }

        // 3. Fallback to SPDK with 4KB alignment
        result.ptr = spdk_dma_malloc_socket(size, 4096, nullptr, numa_node);
        if (result.ptr) {
            result.method = AllocMethod::SPDK_4K;
            return result;
        }

        // 4. Last resort: standard aligned_alloc
        result.ptr = std::aligned_alloc(64, size);
        if (result.ptr) {
            std::memset(result.ptr, 0, size);
            result.method = AllocMethod::ALIGNED_ALLOC;
        }

        return result;
    }

    // Free index memory based on allocation method
    static void FreeIndexMemory(void* ptr, size_t size, AllocMethod method) {
        if (!ptr) {
            return;
        }

        switch (method) {
        case AllocMethod::HUGEPAGE_1G:
            HugePageAllocator::FreeHugepage(ptr, size);
            break;
        case AllocMethod::SPDK_HUGEPAGE:
        case AllocMethod::SPDK_4K:
            spdk_dma_free(ptr);
            break;
        case AllocMethod::ALIGNED_ALLOC:
            std::free(ptr);
            break;
        }
    }
};

// =========== Warmup Strategy Guide ===========
//
// | Scenario                     | Recommended Mode | Reason                                     |
// |-----------------------------|------------------|--------------------------------------------|
// | Normal startup (checkpoint) | SYNC             | 0 page faults at runtime, stable latency   |
// | Full rebuild                | SYNC             | 0 page faults at runtime, stable latency   |
// | Testing / benchmarks        | SYNC             | Stable perf data, warmup before test        |
// | Hot standby failover        | ASYNC            | Background warmup, no impact on switch time |
//
// At 1M QPS, ON_LOAD mode causes 1GB hugepage page faults during DMA writes,
// leading to millisecond-level CPU stalls. Use SYNC for production.

}  // namespace spdk_kv
