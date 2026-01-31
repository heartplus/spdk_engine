// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <spdk/blob.h>

#include <bitset>
#include <cstdint>
#include <functional>
#include <vector>

#include "spdk_kv/entry.h"
#include "spdk_kv/mem_index.h"
#include "spdk_kv/types.h"

namespace spdk_kv {

// Checkpoint trigger configuration
struct CheckpointTrigger {
    // Periodic trigger (default 300 seconds)
    uint64_t interval_ns = 300ULL * 1000 * 1000 * 1000;

    // Write amount trigger (default 10GB)
    uint64_t bytes_threshold = 10ULL * 1024 * 1024 * 1024;

    // Dirty segment count trigger (default 8, about 10%)
    uint32_t dirty_segment_threshold = 8;

    // Last checkpoint time (nanoseconds since epoch)
    uint64_t last_checkpoint_time_ns = 0;

    // Bytes written since last checkpoint
    uint64_t bytes_since_last_checkpoint = 0;

    // Check if checkpoint should be triggered
    bool ShouldCheckpoint(size_t dirty_segment_count, uint64_t current_time_ns) const {
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

    // Reset counters after checkpoint
    void Reset(uint64_t current_time_ns) {
        last_checkpoint_time_ns = current_time_ns;
        bytes_since_last_checkpoint = 0;
    }

    // Update bytes written
    void AddBytes(uint64_t bytes) { bytes_since_last_checkpoint += bytes; }
};

// Checkpoint callback type
using CheckpointCallback = std::function<void(int status)>;

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
    void MarkDirty(uint64_t bucket_index);

    // Get dirty segment count
    size_t DirtySegmentCount() const { return dirty_segments_.count(); }

    // Check if any segment is dirty
    bool HasDirtySegments() const { return dirty_segments_.any(); }

    // Snapshot dirty segments (for COW during checkpoint)
    std::bitset<kMemIndexSegmentCount> SnapshotDirtySegments() const { return dirty_segments_; }

    // Start async checkpoint
    // In simulation mode, this is synchronous
    void StartCheckpoint(CheckpointCallback callback);

    // Poll checkpoint progress (call in main loop)
    // Returns true if checkpoint is in progress
    bool Poll();

    // Check if checkpoint is in progress
    bool IsInProgress() const { return state_ != State::kIdle; }

    // Get current state
    State GetState() const { return state_; }

    // Get checkpoint sequence (increments after each successful checkpoint)
    uint64_t GetCheckpointSequence() const { return checkpoint_sequence_; }

    // Set global sequence for snapshot
    void SetGlobalSequence(uint32_t seq) { global_sequence_ = seq; }
    uint32_t GetGlobalSequence() const { return global_sequence_; }

    // Get/Set active buffer positions (for recovery)
    void SetActiveBufferPositions(const ActiveBufferPos* positions, uint8_t count);
    const ActiveBufferPos* GetActiveBufferPositions() const { return active_buffer_positions_; }
    uint8_t GetActiveBufferCount() const { return active_buffer_count_; }

    // Set SPDK resources for real IO
    void SetSpdkResources(spdk_blob* mem_index_blob, spdk_io_channel* channel,
                          spdk_blob_store* blobstore);

    // Serialize a segment to buffer
    // Returns bytes written, or 0 on error
    size_t SerializeSegment(uint32_t segment_id, void* buffer, size_t buffer_size);

    // Deserialize a segment from buffer
    bool DeserializeSegment(uint32_t segment_id, const void* buffer, size_t data_size);

private:
    // Internal helpers
    void ProcessNextSegment();
    void OnSegmentWritten(int status);
    void OnSyncComplete(int status);
    void OnSuperblockUpdated(int status);
    void CompleteCheckpoint(int status);

    // Clear dirty flags
    void ClearDirtyFlags() { dirty_segments_.reset(); }

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
};

}  // namespace spdk_kv
