// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#include "mem_index.h"
#include <cstring>
#include <cstdlib>
#include <stdexcept>

#ifdef WITH_SPDK
#include <spdk/env.h>
#endif

namespace spdk_kv {

// xxHash64 constants
constexpr uint64_t PRIME64_1 = 0x9E3779B185EBCA87ULL;
constexpr uint64_t PRIME64_2 = 0xC2B2AE3D27D4EB4FULL;
constexpr uint64_t PRIME64_3 = 0x165667B19E3779F9ULL;
constexpr uint64_t PRIME64_4 = 0x85EBCA77C2B2AE63ULL;
constexpr uint64_t PRIME64_5 = 0x27D4EB2F165667C5ULL;

uint64_t MemIndex::xxhash64(uint64_t key) {
    // Simplified xxHash64 for single uint64_t
    uint64_t h64 = PRIME64_5 + 8;
    h64 ^= key * PRIME64_2;
    h64 = ((h64 << 31) | (h64 >> 33)) * PRIME64_1;
    h64 ^= h64 >> 33;
    h64 *= PRIME64_2;
    h64 ^= h64 >> 29;
    h64 *= PRIME64_3;
    h64 ^= h64 >> 32;
    return h64;
}

uint64_t MemIndex::next_power_of_2(uint64_t n) {
    if (n == 0) return 1;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    return n + 1;
}

MemIndex::MemIndex(uint64_t max_entries, double load_factor, int numa_node)
    : capacity_(next_power_of_2(static_cast<uint64_t>(max_entries / load_factor)))
    , size_(0)
    , global_sequence_(0)
    , numa_node_(numa_node) {

    size_t entries_size = capacity_ * sizeof(MemIndexEntry);
    size_t psl_size = capacity_;
    size_t total_size = entries_size + psl_size;

#ifdef WITH_SPDK
    // Use SPDK DMA memory allocation
    void* mem = spdk_dma_zmalloc(total_size, 64, nullptr);
    if (!mem) {
        throw std::runtime_error("Failed to allocate index memory via SPDK");
    }
#else
    // Use aligned_alloc for non-SPDK builds
    void* mem = aligned_alloc(64, total_size);
    if (!mem) {
        throw std::runtime_error("Failed to allocate index memory");
    }
    memset(mem, 0, total_size);
#endif

    entries_ = static_cast<MemIndexEntry*>(mem);
    psl_ = static_cast<uint8_t*>(mem) + entries_size;

    // Initialize dirty bitmap
    memset(dirty_bitmap_, 0, sizeof(dirty_bitmap_));
}

MemIndex::~MemIndex() {
    if (entries_) {
#ifdef WITH_SPDK
        spdk_dma_free(entries_);
#else
        free(entries_);
#endif
        entries_ = nullptr;
        psl_ = nullptr;
    }
}

void MemIndex::compute_hash(uint64_t key, uint64_t* hash, uint8_t* tag) {
    *hash = xxhash64(key);
    *tag = (*hash >> 56) & 0xFF;  // Top 8 bits as tag
}

MemIndexEntry* MemIndex::find(uint64_t key) {
    uint64_t hash;
    uint8_t tag;
    compute_hash(key, &hash, &tag);

    uint64_t idx = hash & (capacity_ - 1);
    return find_internal(key, idx, tag);
}

MemIndexEntry* MemIndex::find_internal(uint64_t key, uint64_t idx, uint8_t tag) {
    uint8_t dist = 0;

    // Prefetch first bucket
    __builtin_prefetch(&entries_[idx], 0, 3);

    while (true) {
        // If current PSL < our probe distance, key doesn't exist
        if (psl_[idx] < dist) {
            return nullptr;
        }

        MemIndexEntry& entry = entries_[idx];

        // Fast tag filter, then full key compare
        if (entry.tag == tag && entry.key == key && !entry.deleted) {
            return &entry;
        }

        // Linear probing
        idx = (idx + 1) & (capacity_ - 1);
        dist++;

        // Prefetch next position
        __builtin_prefetch(&entries_[(idx + 1) & (capacity_ - 1)], 0, 3);

        // Safety check
        if (dist > 128) {
            return nullptr;
        }
    }
}

bool MemIndex::upsert(uint64_t key, const MemIndexEntry& new_entry) {
    uint64_t hash;
    uint8_t tag;
    compute_hash(key, &hash, &tag);

    uint64_t idx = hash & (capacity_ - 1);
    uint8_t dist = 0;

    MemIndexEntry entry_to_insert = new_entry;
    entry_to_insert.tag = tag;

    while (true) {
        // Empty slot, insert directly
        if (psl_[idx] == 0 && entries_[idx].key == 0) {
            entries_[idx] = entry_to_insert;
            psl_[idx] = dist + 1;  // PSL starts from 1
            size_++;
            mark_segment_dirty(idx);
            return true;
        }

        // Same key found, update
        if (entries_[idx].key == key) {
            // Only update if new sequence is greater
            if (sequence_newer(entry_to_insert.sequence, entries_[idx].sequence)) {
                entries_[idx] = entry_to_insert;
                mark_segment_dirty(idx);
            }
            return true;
        }

        // Robin Hood: if current entry has smaller PSL, swap
        if (psl_[idx] < dist + 1) {
            std::swap(entries_[idx], entry_to_insert);
            std::swap(psl_[idx], dist);
            mark_segment_dirty(idx);
            dist++;
        }

        idx = (idx + 1) & (capacity_ - 1);
        dist++;

        // Load too high
        if (dist > 128) {
            return false;  // Need rehash
        }
    }
}

bool MemIndex::remove(uint64_t key) {
    MemIndexEntry* entry = find(key);
    if (entry) {
        entry->deleted = 1;
        uint64_t idx = entry - entries_;
        mark_segment_dirty(idx);
        return true;
    }
    return false;
}

uint32_t MemIndex::allocate_sequence() {
    return ++global_sequence_;
}

void MemIndex::set_global_sequence(uint32_t seq) {
    global_sequence_ = seq;
}

bool MemIndex::sequence_newer(uint32_t a, uint32_t b) {
    // Use signed difference to handle wraparound
    return static_cast<int32_t>(a - b) > 0;
}

void MemIndex::mark_segment_dirty(uint64_t bucket_index) {
    // Calculate segment ID (assuming 80 segments)
    uint32_t segment_id = bucket_index / (capacity_ / MEM_INDEX_SEGMENT_COUNT);
    if (segment_id < 64) {
        dirty_bitmap_[0] |= (1ULL << segment_id);
    } else if (segment_id < 128) {
        dirty_bitmap_[1] |= (1ULL << (segment_id - 64));
    }
}

void MemIndex::clear_dirty_bitmap() {
    dirty_bitmap_[0] = 0;
    dirty_bitmap_[1] = 0;
}

size_t MemIndex::serialize(void* buffer, size_t buf_size) {
    auto* output = static_cast<SerializedMemIndex*>(buffer);

    // Fill header
    output->header.magic = MEM_INDEX_MAGIC;
    output->header.version = 1;
    output->header.capacity = capacity_;
    output->header.global_sequence = global_sequence_;

    // Count and copy non-empty, non-deleted entries
    uint64_t count = 0;
    auto* entries_out = reinterpret_cast<SerializedMemIndex::CompactEntry*>(
        static_cast<char*>(buffer) + sizeof(SerializedMemIndex::Header));

    for (uint64_t i = 0; i < capacity_; i++) {
        if (psl_[i] > 0 && !entries_[i].deleted) {
            auto& src = entries_[i];
            auto& dst = entries_out[count];
            dst.key = src.key;
            dst.file_id = src.file_id;
            dst.offset_index = src.offset_index;
            dst.deleted = 0;
            dst.tag = src.tag;
            dst.page_count = src.page_count;
            dst.sequence = src.sequence;
            count++;
        }
    }
    output->header.entry_count = count;

    // Calculate checksum (simple sum for now)
    size_t data_size = sizeof(SerializedMemIndex::Header) +
                       count * sizeof(SerializedMemIndex::CompactEntry);
    uint32_t checksum = 0;
    auto* data = reinterpret_cast<uint32_t*>(entries_out);
    for (size_t i = 0; i < count * sizeof(SerializedMemIndex::CompactEntry) / 4; i++) {
        checksum ^= data[i];
    }
    output->header.checksum = checksum;

    return data_size;
}

bool MemIndex::deserialize(const void* buffer, size_t data_size) {
    auto* input = static_cast<const SerializedMemIndex*>(buffer);

    // Validate header
    if (input->header.magic != MEM_INDEX_MAGIC) {
        return false;
    }

    auto* entries_in = reinterpret_cast<const SerializedMemIndex::CompactEntry*>(
        static_cast<const char*>(buffer) + sizeof(SerializedMemIndex::Header));

    // Insert all entries
    for (uint64_t i = 0; i < input->header.entry_count; i++) {
        auto& src = entries_in[i];
        MemIndexEntry entry;
        entry.key = src.key;
        entry.file_id = src.file_id;
        entry.offset_index = src.offset_index;
        entry.deleted = src.deleted;
        entry.tag = src.tag;
        entry.page_count = src.page_count;
        entry.sequence = src.sequence;
        entry.reserved = 0;

        if (!upsert(src.key, entry)) {
            return false;
        }
    }

    global_sequence_ = input->header.global_sequence;
    return true;
}

}  // namespace spdk_kv
