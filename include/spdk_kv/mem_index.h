// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <algorithm>
#include <atomic>
#include <bitset>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#ifdef __x86_64__
#include <cpuid.h>
#endif

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "spdk_kv/crc32.h"
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

// Runtime SIMD capability detection
class SIMDCapability {
public:
    static bool HasAvx2() {
#ifdef __x86_64__
        static bool checked = false;
        static bool supported = false;
        if (!checked) {
            unsigned int eax, ebx, ecx, edx;
            if (__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
                supported = (ebx & (1 << 5)) != 0;  // AVX2 bit
            }
            checked = true;
        }
        return supported;
#else
        return false;
#endif
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
        if (!entries_ || !psl_) {
            return nullptr;
        }

        uint64_t hash;
        uint8_t tag;
        HashUtil::ComputeHash(key, &hash, &tag);

        uint64_t idx = hash & (capacity_ - 1);

        // Prefetch hash bucket
        __builtin_prefetch(&entries_[idx], 0, 3);

        return FindInternal(key, idx, tag);
    }

    // Batch find with prefetch pipeline
    void BatchFind(const uint64_t* keys, size_t count, MemIndexEntry** results) {
        if (!entries_ || !psl_ || !keys || !results) {
            for (size_t i = 0; i < count; i++) {
                results[i] = nullptr;
            }
            return;
        }

        static constexpr size_t kPrefetchDistance = 8;

        // Pre-compute all hashes, indices, tags
        // Use heap allocation for large counts, stack for small
        constexpr size_t kStackLimit = 256;
        uint64_t stack_indices[kStackLimit];
        uint8_t stack_tags[kStackLimit];

        uint64_t* indices = count <= kStackLimit
                                    ? stack_indices
                                    : new uint64_t[count];
        uint8_t* tags = count <= kStackLimit
                                ? stack_tags
                                : new uint8_t[count];

        for (size_t i = 0; i < count; i++) {
            uint64_t hash;
            HashUtil::ComputeHash(keys[i], &hash, &tags[i]);
            indices[i] = hash & (capacity_ - 1);
        }

        // Prefetch first kPrefetchDistance entries
        for (size_t i = 0; i < std::min(count, kPrefetchDistance); i++) {
            __builtin_prefetch(&entries_[indices[i]], 0, 3);
        }

        // Pipeline: prefetch ahead, then find
        for (size_t i = 0; i < count; i++) {
            if (i + kPrefetchDistance < count) {
                __builtin_prefetch(&entries_[indices[i + kPrefetchDistance]], 0, 3);
            }

            results[i] = FindInternal(keys[i], indices[i], tags[i]);
        }

        if (count > kStackLimit) {
            delete[] indices;
            delete[] tags;
        }
    }

#ifdef __AVX2__
    // AVX2 accelerated find (compares 4 entries at once via tag matching)
    MemIndexEntry* FindAvx2(uint64_t key) {
        if (!entries_ || !psl_) {
            return nullptr;
        }

        uint64_t hash;
        uint8_t tag;
        HashUtil::ComputeHash(key, &hash, &tag);

        uint64_t idx = hash & (capacity_ - 1);

        // Broadcast target tag to all bytes of a 256-bit register
        __m256i target_tag = _mm256_set1_epi8(static_cast<char>(tag));

        while (true) {
            // Align index to 4-entry boundary
            uint64_t aligned_idx = idx & ~3ULL;

            // Gather 4 consecutive entry tags
            alignas(32) uint8_t tags[32] = {0};
            for (int i = 0; i < 4 && (aligned_idx + i) < capacity_; i++) {
                tags[i] = entries_[aligned_idx + i].tag;
            }

            // Load tags and compare
            __m256i entry_tags = _mm256_loadu_si256(reinterpret_cast<__m256i*>(tags));
            __m256i cmp_result = _mm256_cmpeq_epi8(entry_tags, target_tag);
            int mask = _mm256_movemask_epi8(cmp_result);

            // Check low 4 bits for matches
            int match_mask = mask & 0xF;
            if (match_mask) {
                while (match_mask) {
                    int bit_pos = __builtin_ctz(match_mask);
                    uint64_t check_idx = aligned_idx + bit_pos;

                    if (check_idx < capacity_ &&
                        entries_[check_idx].key == key &&
                        !entries_[check_idx].is_deleted()) {
                        return &entries_[check_idx];
                    }
                    match_mask &= (match_mask - 1);
                }
            }

            // Check PSL to decide whether to continue probing
            uint8_t dist = static_cast<uint8_t>(idx - aligned_idx) + 1;
            bool should_continue = false;
            for (int i = 0; i < 4 && (aligned_idx + i) < capacity_; i++) {
                if (psl_[aligned_idx + i] >= dist + i) {
                    should_continue = true;
                    break;
                }
            }

            if (!should_continue) {
                return nullptr;
            }

            idx = (aligned_idx + 4) & (capacity_ - 1);

            // Full-circle check
            if (idx == (hash & (capacity_ - 1))) {
                return nullptr;
            }
        }
    }

    // AVX2 batch find with prefetch
    void BatchFindAvx2(const uint64_t* keys, size_t count, MemIndexEntry** results) {
        if (!entries_ || !psl_ || !keys || !results) {
            for (size_t i = 0; i < count; i++) {
                results[i] = nullptr;
            }
            return;
        }

        static constexpr size_t kBatchSize = 8;

        for (size_t batch_start = 0; batch_start < count; batch_start += kBatchSize) {
            size_t batch_count = std::min(kBatchSize, count - batch_start);

            // Pre-compute hashes and prefetch target positions
            alignas(32) uint64_t hashes[kBatchSize];
            alignas(32) uint8_t tags[kBatchSize];

            for (size_t i = 0; i < batch_count; i++) {
                HashUtil::ComputeHash(keys[batch_start + i], &hashes[i], &tags[i]);
            }

            for (size_t i = 0; i < batch_count; i++) {
                uint64_t idx = hashes[i] & (capacity_ - 1);
                __builtin_prefetch(&entries_[idx], 0, 3);
            }

            // Dispatch each to FindAvx2
            for (size_t i = 0; i < batch_count; i++) {
                results[batch_start + i] = FindAvx2(keys[batch_start + i]);
            }
        }
    }

#else
    // Non-AVX2 fallback: delegate to scalar implementations
    MemIndexEntry* FindAvx2(uint64_t key) { return Find(key); }

    void BatchFindAvx2(const uint64_t* keys, size_t count, MemIndexEntry** results) {
        BatchFind(keys, count, results);
    }
#endif

    // Insert or update entry (Robin Hood Hashing)
    bool Upsert(uint64_t key, const MemIndexEntry& new_entry) {
        if (!entries_ || !psl_) {
            return false;
        }

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

    // Serialize the full MemIndex to buffer (for non-incremental full checkpoint)
    // Returns bytes written, or 0 on error.
    // Format: SerializedMemIndexHeader + compact entries (only non-empty entries)
    size_t Serialize(void* buffer, size_t buf_size) {
        if (!entries_ || !psl_ || !buffer) {
            return 0;
        }

        auto* output = static_cast<SerializedMemIndexHeader*>(buffer);
        size_t header_size = sizeof(SerializedMemIndexHeader);

        if (buf_size < header_size) {
            return 0;
        }

        // Fill header
        output->magic = kMemIndexMagic;
        output->version = 1;
        output->capacity = capacity_;
        output->global_sequence = global_sequence_;

        // Copy non-empty entries (including deleted ones, for completeness)
        char* ptr = static_cast<char*>(buffer) + header_size;
        size_t max_entries = (buf_size - header_size) / sizeof(MemIndexEntry);
        uint64_t count = 0;

        for (uint64_t i = 0; i < capacity_ && count < max_entries; i++) {
            if (psl_[i] > 0) {
                std::memcpy(ptr, &entries_[i], sizeof(MemIndexEntry));
                ptr += sizeof(MemIndexEntry);
                count++;
            }
        }

        output->entry_count = count;

        // Calculate checksum over entry data
        size_t data_size = count * sizeof(MemIndexEntry);
        output->checksum =
                Crc32::Calculate(static_cast<char*>(buffer) + header_size, data_size);

        std::memset(output->padding, 0, sizeof(output->padding));

        return header_size + data_size;
    }

    // Deserialize compact entries from buffer into this MemIndex via upsert.
    // Returns number of entries loaded, or 0 on error.
    // Uses batch prefetch for performance optimization.
    size_t Deserialize(const void* buffer, size_t data_size) {
        if (!entries_ || !psl_ || !buffer || data_size < sizeof(SerializedMemIndexHeader)) {
            return 0;
        }

        auto* header = static_cast<const SerializedMemIndexHeader*>(buffer);

        // Validate magic and version
        if (header->magic != kMemIndexMagic) {
            return 0;
        }

        // Validate checksum
        size_t entry_data_size = data_size - sizeof(SerializedMemIndexHeader);
        const char* entry_data =
                static_cast<const char*>(buffer) + sizeof(SerializedMemIndexHeader);
        uint32_t computed = Crc32::Calculate(entry_data, entry_data_size);
        if (computed != header->checksum) {
            return 0;
        }

        uint64_t count = header->entry_count;
        if (count * sizeof(MemIndexEntry) > entry_data_size) {
            return 0;
        }

        auto* entries = reinterpret_cast<const MemIndexEntry*>(entry_data);

        // Batch upsert with prefetch optimization
        constexpr size_t kBatchSize = 64;
        for (uint64_t i = 0; i < count; i += kBatchSize) {
            // Prefetch next batch
            for (size_t j = 0; j < kBatchSize && i + j + kBatchSize < count; j++) {
                uint64_t key = entries[i + j + kBatchSize].key;
                uint64_t hash = HashUtil::Hash(key);
                uint64_t idx = hash & (capacity_ - 1);
                __builtin_prefetch(&entries_[idx], 1, 3);
            }

            // Insert current batch
            for (size_t j = 0; j < kBatchSize && i + j < count; j++) {
                const MemIndexEntry& src = entries[i + j];
                if (!src.is_deleted()) {
                    Upsert(src.key, src);
                }
            }
        }

        // Restore global sequence
        global_sequence_ = header->global_sequence;

        return static_cast<size_t>(count);
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
    // Core probing logic (no hash computation)
    MemIndexEntry* FindInternal(uint64_t key, uint64_t idx, uint8_t tag) {
        uint8_t dist = 0;

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
