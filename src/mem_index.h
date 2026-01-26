// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#pragma once

#include <spdk_kv/types.h>
#include <spdk_kv/config.h>
#include <cstdint>
#include <cstddef>
#include <atomic>

namespace spdk_kv {

// Forward declaration
class DmaMemoryPool;

// Robin Hood Hashing based Memory Index
class MemIndex {
public:
    // Create with specified capacity
    explicit MemIndex(uint64_t max_entries, double load_factor = 0.55, int numa_node = -1);
    ~MemIndex();

    // Disable copy
    MemIndex(const MemIndex&) = delete;
    MemIndex& operator=(const MemIndex&) = delete;

    // Compute hash and tag
    static void compute_hash(uint64_t key, uint64_t* hash, uint8_t* tag);

    // Find entry by key
    // Returns nullptr if not found
    MemIndexEntry* find(uint64_t key);

    // Insert or update entry
    // Returns true on success, false if rehash needed
    bool upsert(uint64_t key, const MemIndexEntry& new_entry);

    // Remove entry (mark as deleted)
    bool remove(uint64_t key);

    // Allocate sequence number
    uint32_t allocate_sequence();

    // Set global sequence (for recovery)
    void set_global_sequence(uint32_t seq);

    // Get global sequence
    uint32_t get_global_sequence() const { return global_sequence_; }

    // Sequence comparison (handles wraparound)
    static bool sequence_newer(uint32_t a, uint32_t b);

    // Statistics
    uint64_t size() const { return size_; }
    uint64_t capacity() const { return capacity_; }
    double load_factor() const { return static_cast<double>(size_) / capacity_; }

    // Serialization
    size_t serialize(void* buffer, size_t buf_size);
    bool deserialize(const void* buffer, size_t data_size);

    // Direct access (for memory dump load)
    MemIndexEntry* entries() { return entries_; }
    uint8_t* psl() { return psl_; }

    // Mark segment as dirty
    void mark_segment_dirty(uint64_t bucket_index);

    // Get dirty segments bitmap
    uint64_t* get_dirty_bitmap() { return dirty_bitmap_; }
    void clear_dirty_bitmap();

private:
    // Find internal with precomputed hash
    MemIndexEntry* find_internal(uint64_t key, uint64_t idx, uint8_t tag);

    // Next power of 2
    static uint64_t next_power_of_2(uint64_t n);

    // xxHash64 implementation
    static uint64_t xxhash64(uint64_t key);

    MemIndexEntry* entries_;      // Entry array
    uint8_t* psl_;                // Probe Sequence Length array
    uint64_t capacity_;           // Capacity (power of 2)
    uint64_t size_;               // Current entry count
    uint32_t global_sequence_;    // Global sequence number
    int numa_node_;               // NUMA node

    // Dirty tracking (1 bit per segment, 80 segments = 2 uint64_t)
    uint64_t dirty_bitmap_[2];
};

// Serialized MemIndex format
struct SerializedMemIndex {
    struct Header {
        uint32_t magic;              // 0x4D494458 ("MIDX")
        uint32_t version;
        uint64_t entry_count;        // Valid entry count
        uint64_t capacity;           // Hash table capacity
        uint64_t global_sequence;    // Global sequence
        uint32_t checksum;           // Data checksum
        uint8_t padding[28];         // Padding to 64 bytes
    };

    // Compact entry (no PSL stored)
    struct CompactEntry {
        uint64_t key;
        uint32_t file_id      : 10;
        uint32_t offset_index : 21;
        uint32_t deleted      : 1;
        uint16_t tag          : 8;
        uint16_t page_count   : 8;
        uint32_t sequence;
    };

    // Members
    Header header;
    // Note: Entries follow the header in serialized form
    // Use reinterpret_cast to access them after the header
};
static_assert(sizeof(SerializedMemIndex::Header) == 64, "Header must be 64 bytes");

}  // namespace spdk_kv
