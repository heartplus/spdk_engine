// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <atomic>
#include <bitset>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include "spdk_kv/entry.h"
#include "spdk_kv/types.h"

namespace spdk_kv {

// Simple xxhash64-like implementation for hashing
class HashUtil {
public:
    static uint64_t Hash(uint64_t key) {
        // FNV-1a style hash for simplicity
        uint64_t hash = 14695981039346656037ULL;
        for (int i = 0; i < 8; i++) {
            hash ^= (key >> (i * 8)) & 0xFF;
            hash *= 1099511628211ULL;
        }
        return hash;
    }

    static void ComputeHash(uint64_t key, uint64_t* hash, uint8_t* tag) {
        *hash = Hash(key);
        *tag = static_cast<uint8_t>((*hash >> 56) & 0xFF);
    }
};

// Memory index using Robin Hood Hashing
class MemIndex {
public:
    // Create index with specified capacity
    explicit MemIndex(uint64_t max_entries, double load_factor = kDefaultLoadFactor)
            : capacity_(NextPowerOf2(static_cast<uint64_t>(max_entries / load_factor))),
              size_(0),
              global_sequence_(0),
              entries_(nullptr),
              psl_(nullptr) {
        // Allocate and zero-initialize
        entries_ =
                static_cast<MemIndexEntry*>(aligned_alloc(64, capacity_ * sizeof(MemIndexEntry)));
        if (entries_) {
            std::memset(entries_, 0, capacity_ * sizeof(MemIndexEntry));
        }

        // PSL array (Probe Sequence Length)
        psl_ = static_cast<uint8_t*>(std::calloc(capacity_, 1));
    }

    ~MemIndex() {
        if (entries_) {
            std::free(entries_);
        }
        if (psl_) {
            std::free(psl_);
        }
    }

    // Non-copyable
    MemIndex(const MemIndex&) = delete;
    MemIndex& operator=(const MemIndex&) = delete;

    // Find entry by key
    MemIndexEntry* Find(uint64_t key) {
        if (!entries_ || !psl_) return nullptr;

        uint64_t hash;
        uint8_t tag;
        HashUtil::ComputeHash(key, &hash, &tag);

        uint64_t idx = hash & (capacity_ - 1);
        uint8_t dist = 0;

        // Prefetch hash bucket
        __builtin_prefetch(&entries_[idx], 0, 3);

        while (true) {
            // If current PSL is less than our probe distance, key doesn't exist
            if (psl_[idx] < dist) {
                return nullptr;
            }

            MemIndexEntry& entry = entries_[idx];

            // Fast tag filtering
            if (entry.tag == tag && entry.key == key && !entry.is_deleted()) {
                return &entry;
            }

            // Linear probe next position
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

    // Insert or update entry (Robin Hood Hashing)
    bool Upsert(uint64_t key, const MemIndexEntry& new_entry) {
        if (!entries_ || !psl_) return false;

        uint64_t hash;
        uint8_t tag;
        HashUtil::ComputeHash(key, &hash, &tag);

        uint64_t idx = hash & (capacity_ - 1);
        uint8_t dist = 0;

        MemIndexEntry entry_to_insert = new_entry;
        entry_to_insert.tag = tag;

        while (true) {
            // Empty slot, insert directly
            if (psl_[idx] == 0 && entries_[idx].is_empty()) {
                entries_[idx] = entry_to_insert;
                psl_[idx] = dist + 1;  // PSL starts from 1
                size_++;
                return true;
            }

            // Found same key, update
            if (entries_[idx].key == key) {
                // Only update if new sequence is newer
                if (SequenceNewer(entry_to_insert.sequence, entries_[idx].sequence)) {
                    entries_[idx] = entry_to_insert;
                }
                return true;
            }

            // Robin Hood: if current entry's PSL is smaller, swap
            if (psl_[idx] < dist + 1) {
                std::swap(entries_[idx], entry_to_insert);
                std::swap(psl_[idx], dist);
                dist++;  // Swapped entry continues probing
            }

            idx = (idx + 1) & (capacity_ - 1);
            dist++;

            // Load too high check
            if (dist > 128) {
                return false;  // Need rehash
            }
        }
    }

    // Mark entry as deleted
    bool Remove(uint64_t key) {
        MemIndexEntry* entry = Find(key);
        if (entry) {
            entry->deleted = 1;
            return true;
        }
        return false;
    }

    // Allocate new sequence number
    uint32_t AllocateSequence() { return ++global_sequence_; }

    // Set global sequence (for recovery)
    void SetGlobalSequence(uint32_t seq) { global_sequence_ = seq; }

    // Get global sequence
    uint32_t GetGlobalSequence() const { return global_sequence_; }

    // Compare sequences (handles 32-bit wraparound)
    static bool SequenceNewer(uint32_t a, uint32_t b) { return static_cast<int32_t>(a - b) > 0; }

    // Statistics
    uint64_t Size() const { return size_; }
    uint64_t Capacity() const { return capacity_; }
    double LoadFactor() const { return static_cast<double>(size_) / capacity_; }

    // Access internals (for serialization)
    MemIndexEntry* Entries() { return entries_; }
    const MemIndexEntry* Entries() const { return entries_; }
    uint8_t* Psl() { return psl_; }
    const uint8_t* Psl() const { return psl_; }

    // Rebuild PSL array (after memory dump load)
    void RebuildPslArray() {
        std::memset(psl_, 0, capacity_);
        size_ = 0;

        for (uint64_t i = 0; i < capacity_; i++) {
            if (!entries_[i].is_empty()) {
                uint64_t hash;
                uint8_t tag;
                HashUtil::ComputeHash(entries_[i].key, &hash, &tag);
                uint64_t ideal_idx = hash & (capacity_ - 1);

                // Calculate actual distance
                uint64_t distance =
                        (i >= ideal_idx) ? (i - ideal_idx) : (capacity_ - ideal_idx + i);
                psl_[i] = static_cast<uint8_t>(distance + 1);  // PSL starts from 1

                if (!entries_[i].is_deleted()) {
                    size_++;
                }
            }
        }
    }

    // Mark segment as dirty (for incremental checkpoint)
    void MarkDirty(uint64_t bucket_index) {
        uint32_t segment_id =
                static_cast<uint32_t>(bucket_index / (capacity_ / kMemIndexSegmentCount));
        if (segment_id < kMemIndexSegmentCount) {
            dirty_segments_.set(segment_id);
        }
    }

    // Check if segment is dirty
    bool IsSegmentDirty(uint32_t segment_id) const {
        return segment_id < kMemIndexSegmentCount && dirty_segments_.test(segment_id);
    }

    // Clear all dirty flags
    void ClearDirtyFlags() { dirty_segments_.reset(); }

    // Get dirty segment count
    size_t DirtySegmentCount() const { return dirty_segments_.count(); }

    // Snapshot dirty segments
    std::bitset<kMemIndexSegmentCount> SnapshotDirtySegments() const { return dirty_segments_; }

private:
    static uint64_t NextPowerOf2(uint64_t n) {
        n--;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n |= n >> 32;
        return n + 1;
    }

    uint64_t capacity_;
    uint64_t size_;
    uint32_t global_sequence_;
    MemIndexEntry* entries_;
    uint8_t* psl_;
    std::bitset<kMemIndexSegmentCount> dirty_segments_;
};

}  // namespace spdk_kv
