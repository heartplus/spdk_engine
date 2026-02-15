// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <spdk/blob.h>

#include <cstdint>
#include <functional>
#include <vector>

#include "spdk_kv/ctx_pool.h"
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

    // initialize with SPDK resources and persisted state from superblock
    void initialize(spdk_blob* superblock_blob, spdk_io_channel* channel,
                    spdk_blob_store* blobstore, uint32_t head, uint32_t tail,
                    uint32_t sequence);

    // write a new allocation log entry (async)
    // The entry is written to the in-memory page, then the 4KB page is flushed to NVMe.
    // Callback is invoked after the page write completes.
    void write_entry(uint64_t blob_id, uint16_t file_id, uint64_t file_size,
                    std::function<void(int status)> callback);

    // Load valid entries from disk during recovery.
    // Scans from head forward, validating CRC and sequence monotonicity.
    // Only returns entries with sequence > checkpoint_sequence (post-checkpoint entries).
    void load_entries(uint32_t checkpoint_sequence,
                     std::function<void(int status, const std::vector<AllocLogEntry>& entries)>
                             callback);

    // reclaim all entries (called after checkpoint persists file_mappings).
    // Advances head to tail, logically emptying the log.
    void reclaim();

    // Check if near full (should trigger checkpoint before next alloc)
    bool is_near_full() const;

    // Check if completely full
    bool is_full() const;

    // get used entry count
    uint32_t used_count() const { return tail_ - head_; }

    // Getters for superblock persistence
    uint32_t get_head() const { return head_; }
    uint32_t get_tail() const { return tail_; }
    uint32_t get_sequence() const { return sequence_; }

    // Check if initialized
    bool is_initialized() const { return initialized_; }

    // write completion context (moved from local scope for pooling)
    struct AllocLogWriteCtx {
        std::function<void(int)> callback;
        AllocLogManager* mgr;
    };

private:
    // Extracted static callback for load_entries async read
    struct LoadEntriesReadCtx {
        AllocLogManager* mgr;
        uint32_t checkpoint_seq;
        std::function<void(int, const std::vector<AllocLogEntry>&)> callback;
    };
    static void on_alloc_log_page_read(void* arg, int bserrno);

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

    // Context object pools
    CtxPool<AllocLogWriteCtx, 8> write_ctx_pool_;
    CtxPool<LoadEntriesReadCtx, 4> load_entries_ctx_pool_;
};

}  // namespace spdk_kv
