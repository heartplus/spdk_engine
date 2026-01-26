// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#include "checkpoint.h"
#include "engine.h"
#include <cstring>
#include <ctime>

#ifdef WITH_SPDK
#include <spdk/blob.h>
#endif

namespace spdk_kv {

Checkpoint::Checkpoint(Engine* engine)
    : engine_(engine)
    , state_(CheckpointState::IDLE)
    , current_segment_idx_(0)
    , checkpoint_start_sequence_(0)
    , checkpoint_buffer_count_(0)
    , segment_write_buffer_(nullptr)
    , segment_buffer_size_(0)
    , callback_(nullptr)
    , cb_arg_(nullptr) {
    memset(dirty_bitmap_, 0, sizeof(dirty_bitmap_));
    memset(checkpoint_dirty_snapshot_, 0, sizeof(checkpoint_dirty_snapshot_));
}

void Checkpoint::start_async(spdk_kv_cb cb, void* cb_arg) {
    if (state_ != CheckpointState::IDLE) {
        if (cb) cb(cb_arg, -EBUSY);
        return;
    }

    callback_ = cb;
    cb_arg_ = cb_arg;

    // Atomic snapshot collection
    snapshot_state();

    state_ = CheckpointState::SNAPSHOT_DIRTY;
    step();
}

void Checkpoint::poll() {
    if (state_ == CheckpointState::IDLE || state_ == CheckpointState::DONE) {
        return;
    }
    step();
}

bool Checkpoint::is_in_progress() const {
    return state_ != CheckpointState::IDLE && state_ != CheckpointState::DONE;
}

void Checkpoint::mark_segment_dirty(uint32_t segment_id) {
    if (segment_id < 64) {
        dirty_bitmap_[0] |= (1ULL << segment_id);
    } else if (segment_id < 128) {
        dirty_bitmap_[1] |= (1ULL << (segment_id - 64));
    }
}

bool Checkpoint::should_checkpoint(uint64_t bytes_since_last, uint64_t ns_since_last) {
    if (ns_since_last >= trigger_config_.interval_ns) return true;
    if (bytes_since_last >= trigger_config_.bytes_threshold) return true;

    // Count dirty segments
    uint32_t dirty_count = __builtin_popcountll(dirty_bitmap_[0]) +
                           __builtin_popcountll(dirty_bitmap_[1]);
    if (dirty_count >= trigger_config_.dirty_segment_threshold) return true;

    return false;
}

void Checkpoint::snapshot_state() {
    // 1. Record global sequence
    checkpoint_start_sequence_ = engine_->mem_index()->get_global_sequence();

    // 2. Snapshot active buffer positions
    snapshot_active_buffer_positions();

    // 3. Copy-on-Write: snapshot dirty bitmap
    checkpoint_dirty_snapshot_[0] = dirty_bitmap_[0];
    checkpoint_dirty_snapshot_[1] = dirty_bitmap_[1];
    dirty_bitmap_[0] = 0;
    dirty_bitmap_[1] = 0;
}

void Checkpoint::snapshot_active_buffer_positions() {
    // Capture current write positions
    if (engine_->buffer_manager()->active_buffer()) {
        checkpoint_buffer_count_ = 1;
    }
}

void Checkpoint::step() {
    switch (state_) {
    case CheckpointState::SNAPSHOT_DIRTY:
        collect_dirty_segments();
        break;

    case CheckpointState::WRITING_SEGMENTS:
        write_next_segment();
        break;

    case CheckpointState::WAIT_SEGMENT_WRITE:
        // Wait for IO completion
        break;

    case CheckpointState::SYNCING:
        sync_metadata();
        break;

    case CheckpointState::UPDATING_SUPERBLOCK:
        update_superblock();
        break;

    case CheckpointState::DONE:
        complete_checkpoint(0);
        break;

    case CheckpointState::ERROR:
        complete_checkpoint(-EIO);
        break;

    default:
        break;
    }
}

void Checkpoint::collect_dirty_segments() {
    pending_segments_.clear();

    for (uint32_t i = 0; i < 64; i++) {
        if (checkpoint_dirty_snapshot_[0] & (1ULL << i)) {
            pending_segments_.push_back(i);
        }
    }
    for (uint32_t i = 0; i < 64; i++) {
        if (checkpoint_dirty_snapshot_[1] & (1ULL << i)) {
            pending_segments_.push_back(i + 64);
        }
    }

    if (pending_segments_.empty()) {
        state_ = CheckpointState::DONE;
    } else {
        current_segment_idx_ = 0;
        state_ = CheckpointState::WRITING_SEGMENTS;
    }
}

void Checkpoint::write_next_segment() {
    if (current_segment_idx_ >= pending_segments_.size()) {
        state_ = CheckpointState::SYNCING;
        return;
    }

    (void)pending_segments_[current_segment_idx_];  // Suppress unused warning

    // Serialize segment to buffer
    // In real implementation, would serialize MemIndex segment

#ifdef WITH_SPDK
    // Async write segment
    // spdk_blob_io_write(...)
    state_ = CheckpointState::WAIT_SEGMENT_WRITE;
#else
    // Simulate write completion
    current_segment_idx_++;
    // Stay in WRITING_SEGMENTS to continue
#endif
}

void Checkpoint::on_segment_write_complete(int status) {
    if (status != 0) {
        state_ = CheckpointState::ERROR;
        return;
    }
    current_segment_idx_++;
    state_ = CheckpointState::WRITING_SEGMENTS;
}

void Checkpoint::sync_metadata() {
#ifdef WITH_SPDK
    // spdk_blob_sync_md(...)
#endif
    state_ = CheckpointState::UPDATING_SUPERBLOCK;
}

void Checkpoint::update_superblock() {
    // Update superblock with checkpoint info
    state_ = CheckpointState::DONE;
}

void Checkpoint::complete_checkpoint(int status) {
    state_ = CheckpointState::IDLE;
    if (callback_) {
        callback_(cb_arg_, status);
    }
    callback_ = nullptr;
    cb_arg_ = nullptr;
}

}  // namespace spdk_kv
