// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#pragma once

#include <spdk_kv/types.h>
#include "append_buffer.h"
#include <cstdint>
#include <vector>
#include <queue>

#ifdef WITH_SPDK
#include <spdk/blob.h>
#include <spdk/thread.h>
#endif

namespace spdk_kv {

// Forward declarations
class Engine;
struct FileInfo;

// Write context for async IO
struct WriteContext {
    Engine* engine;
    AppendBuffer* buffer;
    FileInfo* file;
    uint64_t offset;
    size_t size;
    std::vector<PendingWrite> pending_writes;
    spdk_kv_cb callback;
    void* cb_arg;
};

// Read context for async IO
struct ReadContext {
    Engine* engine;
    uint64_t key;
    void* buffer;
    size_t buffer_size;
    uint32_t actual_size;
    FileInfo* file;
    uint64_t offset;
    size_t size;
    spdk_kv_cb callback;
    void* cb_arg;
};

// IO Submitter for batch IO operations
class IoSubmitter {
public:
    static constexpr size_t BATCH_SIZE = 32;

    IoSubmitter(Engine* engine);
    ~IoSubmitter();

    // Initialize with IO channel
#ifdef WITH_SPDK
    bool init(struct spdk_io_channel* channel);
#else
    bool init(void* channel);
#endif

    // Poll for pending submissions
    void poll();

    // Submit write request
    void submit_write(AppendBuffer* buffer, FileInfo* file, uint64_t offset,
                      std::vector<PendingWrite>&& pending_writes);

    // Submit read request
    void submit_read(uint64_t key, void* buffer, size_t buffer_size,
                     FileInfo* file, uint64_t offset, size_t size,
                     spdk_kv_cb cb, void* cb_arg);

    // Get number of pending IOs
    size_t pending_count() const { return pending_writes_.size() + pending_reads_.size(); }

    // Process completions
    void process_completions();

private:
    // Submit batch of writes
    void submit_write_batch();

    // Submit batch of reads
    void submit_read_batch();

    Engine* engine_;
#ifdef WITH_SPDK
    struct spdk_io_channel* io_channel_;
#else
    void* io_channel_;
#endif

    std::queue<WriteContext*> pending_writes_;
    std::queue<ReadContext*> pending_reads_;
    std::vector<WriteContext*> inflight_writes_;
    std::vector<ReadContext*> inflight_reads_;
};

// Flush trigger for controlling when to submit buffers
class FlushTrigger {
public:
    static constexpr size_t BUFFER_THRESHOLD = 16;
    static constexpr size_t ENTRY_THRESHOLD = 128;

    bool should_flush_immediately(size_t pending_buffers, size_t pending_entries) const {
        return pending_buffers >= BUFFER_THRESHOLD ||
               pending_entries >= ENTRY_THRESHOLD;
    }
};

}  // namespace spdk_kv
