// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#pragma once

#include <spdk_kv/types.h>
#include <cstdint>
#include <vector>

namespace spdk_kv {

// Forward declarations
class Engine;

// Checkpoint state machine states
enum class CheckpointState {
    IDLE,
    SNAPSHOT_DIRTY,
    WRITING_SEGMENTS,
    WAIT_SEGMENT_WRITE,
    SYNCING,
    UPDATING_SUPERBLOCK,
    DONE,
    ERROR
};

// Checkpoint trigger configuration
struct CheckpointTriggerConfig {
    uint64_t interval_ns = 300ULL * 1000 * 1000 * 1000;  // 300 seconds
    uint64_t bytes_threshold = 10ULL * 1024 * 1024 * 1024;  // 10GB
    uint32_t dirty_segment_threshold = 8;
};

// Checkpoint manager
class Checkpoint {
public:
    Checkpoint(Engine* engine);
    ~Checkpoint() = default;

    // Start async checkpoint
    void start_async(spdk_kv_cb cb, void* cb_arg);

    // Poll for checkpoint progress
    void poll();

    // Check if checkpoint is in progress
    bool is_in_progress() const;

    // Mark segment as dirty
    void mark_segment_dirty(uint32_t segment_id);

    // Check if checkpoint should be triggered
    bool should_checkpoint(uint64_t bytes_since_last, uint64_t ns_since_last);

    // Get state
    CheckpointState state() const { return state_; }

private:
    void snapshot_state();
    void snapshot_active_buffer_positions();
    void step();
    void collect_dirty_segments();
    void write_next_segment();
    void on_segment_write_complete(int status);
    void sync_metadata();
    void update_superblock();
    void complete_checkpoint(int status);

    Engine* engine_;
    CheckpointState state_;
    CheckpointTriggerConfig trigger_config_;

    // Dirty bitmap (2x64 bits = 128 segments)
    uint64_t dirty_bitmap_[2];
    uint64_t checkpoint_dirty_snapshot_[2];

    // Pending segments to write
    std::vector<uint32_t> pending_segments_;
    size_t current_segment_idx_;

    // Checkpoint state
    uint32_t checkpoint_start_sequence_;
    uint32_t checkpoint_buffer_count_;

    // Write buffer for segment data
    void* segment_write_buffer_;
    size_t segment_buffer_size_;

    // Completion callback
    spdk_kv_cb callback_;
    void* cb_arg_;
};

}  // namespace spdk_kv
