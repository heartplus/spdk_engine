// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#pragma once

#include <cstdint>
#include <cstddef>

namespace spdk_kv {

// Configuration parameters
struct Config {
    // Capacity configuration
    uint64_t max_capacity = 8ULL * 1024 * 1024 * 1024 * 1024;  // 8TB
    uint64_t data_file_size = 8ULL * 1024 * 1024 * 1024;       // 8GB

    // Index configuration
    uint64_t max_entries = 2000000000ULL;  // 2 billion
    double index_load_factor = 0.55;       // Load factor
    size_t index_entry_size = 20;          // Entry size (bytes)

    // IO configuration
    uint32_t alignment = 4096;                     // Alignment unit
    uint32_t max_value_size = 256 * 1024 * 1024;   // 256MB max value

    // AppendBuffer configuration
    size_t append_buffer_size = 2 * 1024 * 1024;     // 2MB per buffer
    size_t append_buffer_min_count = 4;              // Minimum buffer count
    size_t append_buffer_max_count = 16;             // Maximum buffer count
    size_t backpressure_high_water = 12;             // Trigger backpressure
    size_t backpressure_low_water = 6;               // Resume accepting requests
    uint32_t flush_timeout_ms = 10;                  // Flush timeout
    size_t flush_entry_threshold = 128;              // Entry count threshold
    size_t flush_buffer_threshold = 16;              // Buffer count threshold

    // Compaction configuration
    double compaction_threshold = 0.5;               // Garbage ratio threshold
    uint32_t compaction_max_iops = 1000;             // Max IOPS for compaction
    uint64_t compaction_max_cycles = 50000;          // Max cycles per poll
    int compaction_max_retry = 3;                    // IO retry count
    uint64_t compaction_retry_delay_us = 1000;       // Retry delay
    double bitmap_creation_threshold = 0.3;          // Bitmap creation threshold

    // Memory pool configuration
    size_t read_buffer_count = 256;
    size_t read_buffer_size = 64 * 1024;             // 64KB

    // NUMA configuration
    int cpu_core = -1;                               // -1 for auto select
    int numa_node = -1;                              // -1 for auto select

    // Checkpoint configuration
    uint32_t checkpoint_interval_sec = 300;          // 5 minutes
    uint64_t checkpoint_bytes = 10ULL * 1024 * 1024 * 1024;  // 10GB
    uint32_t checkpoint_dirty_segment_threshold = 8;
    size_t checkpoint_segment_size = 1ULL * 1024 * 1024 * 1024;  // 1GB
    size_t checkpoint_segment_count = 80;            // 80 segments

    // Default constructor
    Config() = default;

    // Validation
    bool validate() const {
        if (max_capacity == 0) return false;
        if (data_file_size == 0) return false;
        if (data_file_size > max_capacity) return false;
        if (max_entries == 0) return false;
        if (index_load_factor <= 0 || index_load_factor >= 1.0) return false;
        if (alignment == 0 || (alignment & (alignment - 1)) != 0) return false;
        if (append_buffer_size == 0) return false;
        if (append_buffer_min_count == 0) return false;
        if (append_buffer_max_count < append_buffer_min_count) return false;
        return true;
    }
};

// Create options
struct CreateOpts {
    Config config;
    bool force = false;  // Force create even if exists
};

// Open options
struct OpenOpts {
    Config config;
    bool read_only = false;
};

// Default constants
constexpr size_t MEM_INDEX_SEGMENT_SIZE = 1ULL * 1024 * 1024 * 1024;  // 1GB
constexpr size_t MEM_INDEX_SEGMENT_COUNT = 80;  // 80 segments per area
constexpr uint64_t SUPERBLOCK_PRIMARY_OFFSET = 0;
constexpr uint64_t SUPERBLOCK_BACKUP_OFFSET = 4 * 1024 * 1024;

}  // namespace spdk_kv
