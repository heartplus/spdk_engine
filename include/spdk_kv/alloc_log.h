// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <spdk/blob.h>

#include <cstdint>
#include <functional>
#include <vector>

#include "spdk_kv/entry.h"
#include "spdk_kv/types.h"

namespace spdk_kv {

// AllocLog manager
// Manages a rolling buffer of blob allocation records in the superblock blob.
// Ensures crash consistency: AllocLog entry is persisted before DataFileHeader,
// so recovery can identify and release dangling blobs.
class AllocLogManager {
public:
    AllocLogManager();
    ~AllocLogManager();

    // Non-copyable
    AllocLogManager(const AllocLogManager&) = delete;
    AllocLogManager& operator=(const AllocLogManager&) = delete;

    // Initialize with SPDK resources and persisted state from superblock
    void Initialize(spdk_blob* superblock_blob, spdk_io_channel* channel,
                    spdk_blob_store* blobstore, uint32_t head, uint32_t tail,
                    uint32_t sequence);

    // Write a new allocation log entry (async)
    // The entry is written to the in-memory page, then the 4KB page is flushed to NVMe.
    // Callback is invoked after the page write completes.
    void WriteEntry(uint64_t blob_id, uint16_t file_id, uint64_t file_size,
                    std::function<void(int status)> callback);

    // Load valid entries from disk during recovery.
    // Scans from head forward, validating CRC and sequence monotonicity.
    // Only returns entries with sequence > checkpoint_sequence (post-checkpoint entries).
    void LoadEntries(uint32_t checkpoint_sequence,
                     std::function<void(int status, const std::vector<AllocLogEntry>& entries)>
                             callback);

    // Reclaim all entries (called after checkpoint persists file_mappings).
    // Advances head to tail, logically emptying the log.
    void Reclaim();

    // Check if near full (should trigger checkpoint before next alloc)
    bool IsNearFull() const;

    // Check if completely full
    bool IsFull() const;

    // Get used entry count
    uint32_t UsedCount() const { return tail_ - head_; }

    // Getters for superblock persistence
    uint32_t GetHead() const { return head_; }
    uint32_t GetTail() const { return tail_; }
    uint32_t GetSequence() const { return sequence_; }

    // Check if initialized
    bool IsInitialized() const { return initialized_; }

private:
    // Extracted static callback for LoadEntries async read
    struct LoadEntriesReadCtx {
        AllocLogManager* mgr;
        uint32_t checkpoint_seq;
        std::function<void(int, const std::vector<AllocLogEntry>&)> callback;
    };
    static void OnAllocLogPageRead(void* arg, int bserrno);

    // SPDK resources
    spdk_blob* superblock_blob_;
    spdk_io_channel* channel_;
    spdk_blob_store* blobstore_;

    // Rolling buffer state (logical indices, physical = index % kAllocLogCapacity)
    uint32_t head_;      // Oldest valid entry
    uint32_t tail_;      // Next write position
    uint32_t sequence_;  // Last allocated sequence number

    // In-memory copy of AllocLog page (4KB, holds kAllocLogCapacity entries)
    void* page_buf_;
    bool initialized_;
};

}  // namespace spdk_kv
