// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <spdk/blob.h>
#include <spdk/env.h>
#include <spdk/nvme.h>

#include <cstdint>
#include <functional>
#include <queue>
#include <vector>

#include "spdk_kv/append_buffer.h"
#include "spdk_kv/ctx_pool.h"
#include "spdk_kv/entry.h"
#include "spdk_kv/types.h"

namespace spdk_kv {

// Forward declarations
class Engine;

// IO completion callback
using IoCompletionCallback = std::function<void(int status)>;

// Write context for tracking pending writes
struct WriteContext {
    uint64_t key;
    uint32_t sequence;
    uint32_t buffer_offset;
    uint32_t aligned_size;
    uint8_t flags;  // bit0 = is_compaction
    IoCompletionCallback callback;

    // Old position info (for compaction)
    uint16_t old_file_id;
    uint32_t old_offset_index;
    MemIndexEntry new_entry;

    spdk_blob* blob;  // Blob handle for this write

    bool is_compaction() const { return (flags & 0x01) != 0; }
};

// Read context
struct ReadContext {
    uint64_t key;
    void* buffer;
    uint32_t buffer_size;
    uint32_t actual_len;
    IoCompletionCallback callback;
    uint16_t file_id;
    uint64_t offset;
    uint32_t pages;
    spdk_blob* blob;  // Blob handle for this read
};

// IO Submitter for batching and submitting IO operations
class IoSubmitter {
public:
    static constexpr size_t kBatchSize = 32;

    IoSubmitter();
    ~IoSubmitter();

    // Non-copyable
    IoSubmitter(const IoSubmitter&) = delete;
    IoSubmitter& operator=(const IoSubmitter&) = delete;

    // Initialize with SPDK resources
    bool Initialize(spdk_nvme_ctrlr* ctrlr, spdk_nvme_ns* ns, spdk_blob_store* blobstore,
                    uint32_t io_queue_size = 256);

    // Get IO channel
    spdk_io_channel* GetChannel() { return io_channel_; }

    // Submit a write request
    void SubmitWrite(AppendBuffer* buffer, uint64_t file_offset,
                     const std::vector<WriteContext>& contexts, IoCompletionCallback callback);

    // Submit a read request with blob handle
    void SubmitBlobRead(spdk_blob* blob, uint64_t offset, uint32_t pages, void* buffer,
                        IoCompletionCallback callback);

    // Process completions (call in polling loop)
    // Returns number of completions processed
    size_t ProcessCompletions(size_t max_completions = 32);

    // Get pending write count
    size_t PendingWriteCount() const { return pending_writes_.size(); }

    // Check if there are pending IOs
    bool HasPendingIo() const { return !pending_writes_.empty(); }

    // Flush pending writes
    void FlushPendingWrites();

    // IO completion context types (moved from local scope for pooling)
    struct ReadCompletionCtx {
        IoCompletionCallback callback;
        IoSubmitter* submitter;
    };
    struct WriteCompletionCtx {
        IoCompletionCallback callback;
        IoSubmitter* submitter;
    };

private:
    // Batch submission
    void SubmitBatch();

    // Internal completion handlers
    void OnWriteComplete(int status, const std::vector<WriteContext>& contexts);
    void OnReadComplete(int status, ReadContext* ctx);

    // SPDK resources
    spdk_nvme_ctrlr* ctrlr_;
    spdk_nvme_ns* ns_;
    spdk_nvme_qpair* qpair_;
    spdk_blob_store* blobstore_;
    spdk_io_channel* io_channel_;

    // Pending operations
    struct PendingWrite {
        AppendBuffer* buffer;
        uint64_t offset;
        std::vector<WriteContext> contexts;
        IoCompletionCallback callback;
    };
    std::queue<PendingWrite> pending_writes_;

    // Context object pools
    CtxPool<ReadCompletionCtx, 64> read_completion_ctx_pool_;
    CtxPool<WriteCompletionCtx, 32> write_completion_ctx_pool_;
};

// Flush trigger for determining when to submit IO
class FlushTrigger {
public:
    // Thresholds
    static constexpr size_t kBufferThreshold = 16;  // Buffer count in submit_ring
    static constexpr size_t kEntryThreshold = 128;  // Entry count in current buffer

    FlushTrigger() = default;

    // Check if immediate flush is needed
    bool ShouldFlushImmediately(size_t pending_buffers, size_t pending_entries) const {
        return pending_buffers >= kBufferThreshold || pending_entries >= kEntryThreshold;
    }

    // Check if timeout flush is needed
    bool ShouldFlushOnTimeout(uint64_t last_flush_ns, uint64_t current_ns,
                              uint64_t timeout_ns = 10000000) const {  // 10ms default
        return (current_ns - last_flush_ns) >= timeout_ns;
    }
};

}  // namespace spdk_kv
