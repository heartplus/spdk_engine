// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#include "spdk_kv/checkpoint.h"

#include <algorithm>
#include <cstring>

#include "spdk_kv/crc32.h"

namespace spdk_kv {

IncrementalCheckpoint::IncrementalCheckpoint(MemIndex* mem_index, uint64_t capacity)
        : mem_index_(mem_index),
          capacity_(capacity),
          state_(State::kIdle),
          checkpoint_sequence_(0),
          global_sequence_(0),
          checkpoint_start_sequence_(0),
          active_buffer_count_(0),
          current_segment_idx_(0) {
    std::memset(segment_versions_, 0, sizeof(segment_versions_));
    std::memset(active_buffer_positions_, 0, sizeof(active_buffer_positions_));

    // Allocate segment buffer (1GB max, but we use smaller chunks for simulation)
    segment_buffer_.resize(64 * 1024 * 1024);  // 64MB for simulation
}

IncrementalCheckpoint::~IncrementalCheckpoint() {}

void IncrementalCheckpoint::MarkDirty(uint64_t bucket_index) {
    uint32_t segment_id =
            static_cast<uint32_t>(bucket_index / (capacity_ / kMemIndexSegmentCount));
    if (segment_id < kMemIndexSegmentCount) {
        dirty_segments_.set(segment_id);
    }
}

void IncrementalCheckpoint::SetActiveBufferPositions(const ActiveBufferPos* positions,
                                                     uint8_t count) {
    active_buffer_count_ = std::min(count, static_cast<uint8_t>(kMaxBufferCount));
    for (uint8_t i = 0; i < active_buffer_count_; i++) {
        active_buffer_positions_[i] = positions[i];
    }
}

void IncrementalCheckpoint::StartCheckpoint(CheckpointCallback callback) {
    if (state_ != State::kIdle) {
        callback(-1);  // Already in progress
        return;
    }

    callback_ = std::move(callback);
    state_ = State::kSnapshoting;

    // === Atomic snapshot collection ===
    // 1. Record global sequence at checkpoint start
    checkpoint_start_sequence_ = global_sequence_;

    // 2. Copy-on-Write: snapshot current dirty bitmap
    checkpoint_dirty_snapshot_ = dirty_segments_;
    dirty_segments_.reset();  // Clear for new writes

    // 3. Collect dirty segments
    pending_segments_.clear();
    for (uint32_t i = 0; i < kMemIndexSegmentCount; i++) {
        if (checkpoint_dirty_snapshot_.test(i)) {
            pending_segments_.push_back(i);
        }
    }

    if (pending_segments_.empty()) {
        // No dirty segments, complete immediately
        CompleteCheckpoint(0);
        return;
    }

    // Start writing segments
    state_ = State::kWritingSegments;
    current_segment_idx_ = 0;
    ProcessNextSegment();
}

bool IncrementalCheckpoint::Poll() {
    // In simulation mode, checkpoint is synchronous
    // In real SPDK mode, this would check IO completion
    return state_ != State::kIdle;
}

void IncrementalCheckpoint::ProcessNextSegment() {
    if (current_segment_idx_ >= pending_segments_.size()) {
        // All segments written, sync data
        state_ = State::kSyncingData;
        OnSyncComplete(0);  // Simulation: immediate completion
        return;
    }

    uint32_t seg_id = pending_segments_[current_segment_idx_];

    // Serialize segment data
    size_t data_size = SerializeSegment(seg_id, segment_buffer_.data(), segment_buffer_.size());

    if (data_size == 0) {
        // Serialization failed
        CompleteCheckpoint(-1);
        return;
    }

    // In simulation mode, we don't actually write to disk
    // In real SPDK mode, this would be:
    // spdk_blob_io_write(mem_index_blob_, channel_, segment_buffer_.data(),
    //                    seg_id * kMemIndexSegmentSize / kPageSize,
    //                    data_size / kPageSize, on_segment_written, this);

    OnSegmentWritten(0);  // Simulation: immediate completion
}

void IncrementalCheckpoint::OnSegmentWritten(int status) {
    if (status != 0) {
        CompleteCheckpoint(status);
        return;
    }

    current_segment_idx_++;
    ProcessNextSegment();
}

void IncrementalCheckpoint::OnSyncComplete(int status) {
    if (status != 0) {
        CompleteCheckpoint(status);
        return;
    }

    // Update segment versions
    for (uint32_t seg_id : pending_segments_) {
        segment_versions_[seg_id]++;
    }

    // Update superblock (in real SPDK mode, this would be async)
    state_ = State::kUpdatingSuperblock;
    OnSuperblockUpdated(0);  // Simulation: immediate completion
}

void IncrementalCheckpoint::OnSuperblockUpdated(int status) {
    CompleteCheckpoint(status);
}

void IncrementalCheckpoint::CompleteCheckpoint(int status) {
    if (status == 0) {
        checkpoint_sequence_++;
        state_ = State::kComplete;
    } else {
        state_ = State::kFailed;
    }

    // Call user callback
    if (callback_) {
        callback_(status);
        callback_ = nullptr;
    }

    // Return to idle state
    state_ = State::kIdle;
}

size_t IncrementalCheckpoint::SerializeSegment(uint32_t segment_id, void* buffer,
                                               size_t buffer_size) {
    if (segment_id >= kMemIndexSegmentCount || !mem_index_) {
        return 0;
    }

    // Calculate segment bounds
    size_t entries_per_segment = capacity_ / kMemIndexSegmentCount;
    size_t start_idx = segment_id * entries_per_segment;
    size_t end_idx = start_idx + entries_per_segment;

    // Header
    SegmentHeader header;
    header.magic = kSegmentMagic;
    header.segment_id = segment_id;
    header.version = segment_versions_[segment_id] + 1;
    header.entry_count = 0;
    header.dirty_sequence = checkpoint_start_sequence_;
    header.checksum = 0;
    std::memset(header.padding, 0, sizeof(header.padding));

    // Calculate required size
    size_t header_size = sizeof(SegmentHeader);
    size_t entry_size = sizeof(MemIndexEntry);
    size_t max_entries = (buffer_size - header_size) / entry_size;

    if (max_entries < entries_per_segment) {
        return 0;  // Buffer too small
    }

    // Copy entries
    char* ptr = static_cast<char*>(buffer) + header_size;
    const MemIndexEntry* entries = mem_index_->Entries();
    const uint8_t* psl = mem_index_->Psl();
    size_t entry_count = 0;

    for (size_t i = start_idx; i < end_idx && i < capacity_; i++) {
        // Include all entries with valid PSL (including deleted ones for recovery)
        if (psl[i] > 0) {
            std::memcpy(ptr, &entries[i], entry_size);
            ptr += entry_size;
            entry_count++;
        }
    }

    header.entry_count = entry_count;

    // Calculate checksum
    size_t data_size = entry_count * entry_size;
    header.checksum = Crc32::Calculate(static_cast<char*>(buffer) + header_size, data_size);

    // Write header
    std::memcpy(buffer, &header, header_size);

    return header_size + data_size;
}

bool IncrementalCheckpoint::DeserializeSegment(uint32_t segment_id, const void* buffer,
                                               size_t data_size) {
    if (segment_id >= kMemIndexSegmentCount || !mem_index_ || data_size < sizeof(SegmentHeader)) {
        return false;
    }

    const auto* header = static_cast<const SegmentHeader*>(buffer);

    // Validate header
    if (header->magic != kSegmentMagic || header->segment_id != segment_id) {
        return false;
    }

    // Validate checksum
    size_t entry_data_size = data_size - sizeof(SegmentHeader);
    const char* entry_data = static_cast<const char*>(buffer) + sizeof(SegmentHeader);
    uint32_t computed_checksum = Crc32::Calculate(entry_data, entry_data_size);

    if (computed_checksum != header->checksum) {
        return false;
    }

    // Calculate segment bounds
    size_t entries_per_segment = capacity_ / kMemIndexSegmentCount;
    size_t start_idx = segment_id * entries_per_segment;

    // Load entries
    MemIndexEntry* entries = mem_index_->Entries();
    size_t entry_size = sizeof(MemIndexEntry);
    size_t num_entries = entry_data_size / entry_size;

    // Note: This is a simplified version. In reality, we would need to handle
    // the case where entries in the segment file might not be in order
    for (size_t i = 0; i < num_entries && i < entries_per_segment; i++) {
        const auto* src = reinterpret_cast<const MemIndexEntry*>(entry_data + i * entry_size);
        entries[start_idx + i] = *src;
    }

    // Update segment version
    segment_versions_[segment_id] = header->version;

    return true;
}

}  // namespace spdk_kv
