// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <spdk/blob.h>

#include <bitset>
#include <cstdint>
#include <functional>
#include <vector>

#include "spdk_kv/append_buffer.h"
#include "spdk_kv/ctx_pool.h"
#include "spdk_kv/entry.h"
#include "spdk_kv/mem_index.h"
#include "spdk_kv/types.h"

namespace spdk_kv {

// Checkpoint trigger configuration
struct CheckpointTrigger {
    // Periodic trigger (default 300 seconds)
    uint64_t interval_ns = 300ULL * 1000 * 1000 * 1000;

    // write amount trigger (default 10GB)
    uint64_t bytes_threshold = 10ULL * 1024 * 1024 * 1024;

    // Dirty segment count trigger (default 8, about 10%)
    uint32_t dirty_segment_threshold = 8;

    // Last checkpoint time (nanoseconds since epoch)
    uint64_t last_checkpoint_time_ns = 0;

    // Bytes written since last checkpoint
    uint64_t bytes_since_last_checkpoint = 0;

    // Check if checkpoint should be triggered
    bool should_checkpoint(size_t dirty_segment_count, uint64_t current_time_ns) const {
        if (current_time_ns - last_checkpoint_time_ns >= interval_ns) {
            return true;
        }
        if (bytes_since_last_checkpoint >= bytes_threshold) {
            return true;
        }
        if (dirty_segment_count >= dirty_segment_threshold) {
            return true;
        }
        return false;
    }

    // reset counters after checkpoint
    void reset(uint64_t current_time_ns) {
        last_checkpoint_time_ns = current_time_ns;
        bytes_since_last_checkpoint = 0;
    }

    // Update bytes written
    void add_bytes(uint64_t bytes) { bytes_since_last_checkpoint += bytes; }
};

// Checkpoint callback type
using CheckpointCallback = std::function<void(int status)>;

// Superblock update callback type
// Called after segment data is synced, to atomically update superblock with snapshot values.
// Parameters: checkpoint_seq, active_buffer_positions, buffer_count, completion callback
using SuperblockUpdateCallback = std::function<void(
        uint32_t checkpoint_seq, const ActiveBufferPos* positions, uint8_t count,
        std::function<void(int status)> on_complete)>;

// Incremental checkpoint manager
class IncrementalCheckpoint {
public:
    // Checkpoint state machine
    enum class State {
        kIdle,
        kSnapshoting,
        kWritingSegments,
        kSyncingData,
        kUpdatingSuperblock,
        kComplete,
        kFailed
    };

    IncrementalCheckpoint(MemIndex* mem_index, uint64_t capacity);
    ~IncrementalCheckpoint();

    // Non-copyable
    IncrementalCheckpoint(const IncrementalCheckpoint&) = delete;
    IncrementalCheckpoint& operator=(const IncrementalCheckpoint&) = delete;

    // Mark a segment as dirty (called when index is modified)
    void mark_dirty(uint64_t bucket_index);

    // get dirty segment count
    size_t dirty_segment_count() const { return dirty_segments_.count(); }

    // Check if any segment is dirty
    bool has_dirty_segments() const { return dirty_segments_.any(); }

    // Snapshot dirty segments (for COW during checkpoint)
    std::bitset<kMemIndexSegmentCount> snapshot_dirty_segments() const { return dirty_segments_; }

    // start async checkpoint
    void start_checkpoint(CheckpointCallback callback);

    // poll checkpoint progress (call in main loop)
    // Returns true if checkpoint is in progress
    bool poll();

    // Check if checkpoint is in progress
    bool is_in_progress() const { return state_ != State::kIdle; }

    // get current state
    State get_state() const { return state_; }

    // get checkpoint sequence (increments after each successful checkpoint)
    uint64_t get_checkpoint_sequence() const { return checkpoint_sequence_; }

    // set global sequence for snapshot
    void set_global_sequence(uint32_t seq) { global_sequence_ = seq; }
    uint32_t get_global_sequence() const { return global_sequence_; }

    // get/set active buffer positions (for recovery)
    void set_active_buffer_positions(const ActiveBufferPos* positions, uint8_t count);
    const ActiveBufferPos* get_active_buffer_positions() const { return active_buffer_positions_; }
    uint8_t get_active_buffer_count() const { return active_buffer_count_; }

    // set AppendBufferManager for active buffer position snapshot
    void set_append_buffer_manager(AppendBufferManager* mgr) { buffer_manager_ = mgr; }

    // set superblock update callback (called after sync to persist checkpoint atomically)
    void set_superblock_update_callback(SuperblockUpdateCallback cb) {
        superblock_update_cb_ = std::move(cb);
    }

    // set SPDK resources for real IO
    void set_spdk_resources(spdk_blob* mem_index_blob, spdk_io_channel* channel,
                          spdk_blob_store* blobstore);

    // serialize a segment to buffer
    // Returns bytes written, or 0 on error
    size_t serialize_segment(uint32_t segment_id, void* buffer, size_t buffer_size);

    // deserialize a segment from buffer
    bool deserialize_segment(uint32_t segment_id, const void* buffer, size_t data_size);

private:
    // Snapshot active buffer positions from AppendBufferManager
    void snapshot_active_buffer_positions();

    // Segment write completion context (moved from local scope for pooling)
    struct SegmentWriteCtx {
        IncrementalCheckpoint* ckpt;
        void* dma_buf;
    };

    // Internal helpers
    void process_next_segment();
    void on_segment_written(int status);
    void on_sync_complete(int status);
    void on_superblock_updated(int status);
    void complete_checkpoint(int status);

    // clear dirty flags
    void clear_dirty_flags() { dirty_segments_.reset(); }

    // Members
    MemIndex* mem_index_;
    uint64_t capacity_;

    // Dirty tracking
    std::bitset<kMemIndexSegmentCount> dirty_segments_;
    std::bitset<kMemIndexSegmentCount> checkpoint_dirty_snapshot_;

    // Segment versions
    uint64_t segment_versions_[kMemIndexSegmentCount];

    // Checkpoint state
    State state_;
    uint64_t checkpoint_sequence_;
    uint32_t global_sequence_;
    uint32_t checkpoint_start_sequence_;

    // Active buffer positions snapshot
    ActiveBufferPos active_buffer_positions_[kMaxBufferCount];
    uint8_t active_buffer_count_;

    // Pending segments to write
    std::vector<uint32_t> pending_segments_;
    size_t current_segment_idx_;

    // Callback
    CheckpointCallback callback_;

    // DMA buffer for segment serialization
    std::vector<char> segment_buffer_;

    // SPDK resources for real IO
    spdk_blob* mem_index_blob_;
    spdk_io_channel* io_channel_;
    spdk_blob_store* blobstore_;

    // Pending IO state for async segment writes
    bool io_pending_;

    // AppendBufferManager reference for snapshotting active buffer positions
    AppendBufferManager* buffer_manager_;

    // Superblock update callback (called after sync to atomically persist checkpoint)
    SuperblockUpdateCallback superblock_update_cb_;

    // Context object pool
    CtxPool<SegmentWriteCtx, 8> segment_write_ctx_pool_;
};

}  // namespace spdk_kv
