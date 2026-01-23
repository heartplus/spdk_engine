// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <queue>
#include <vector>

#include "spdk_kv/types.h"

namespace spdk_kv {

// Forward declarations
class DmaMemoryPool;

// Single append buffer
class AppendBuffer {
public:
    explicit AppendBuffer(void* data, size_t capacity)
            : data_(data),
              capacity_(capacity),
              used_(0),
              entry_count_(0),
              epoch_(0),
              next_slot_id_(0),
              active_rdma_slot_count_(0) {
        std::memset(rdma_slots_, 0, sizeof(rdma_slots_));
    }

    // Reserve space in the buffer
    void* Reserve(size_t len) {
        if (used_ + len > capacity_) {
            return nullptr;
        }
        void* ptr = static_cast<char*>(data_) + used_;
        used_ += len;
        entry_count_++;
        return ptr;
    }

    // Append data to the buffer
    int64_t Append(const void* data, size_t len) {
        if (used_ + len > capacity_) {
            return -1;
        }
        void* dst = static_cast<char*>(data_) + used_;
        std::memcpy(dst, data, len);
        int64_t offset = static_cast<int64_t>(used_);
        used_ += len;
        entry_count_++;
        return offset;
    }

    // Get offset of a pointer within the buffer
    uint32_t GetOffset(void* ptr) const {
        return static_cast<uint32_t>(static_cast<char*>(ptr) - static_cast<char*>(data_));
    }

    // Reset buffer for reuse
    void Reset() {
        used_ = 0;
        entry_count_ = 0;
        epoch_++;
        next_slot_id_ = 0;
        active_rdma_slot_count_ = 0;
        std::memset(rdma_slots_, 0, sizeof(rdma_slots_));
    }

    // Accessors
    void* Data() { return data_; }
    const void* Data() const { return data_; }
    size_t Capacity() const { return capacity_; }
    size_t Used() const { return used_; }
    size_t EntryCount() const { return entry_count_; }
    uint32_t Epoch() const { return epoch_; }

    bool IsFull() const { return used_ >= kFlushThreshold; }
    bool IsEmpty() const { return used_ == 0; }

    // RDMA slot management
    static constexpr size_t kMaxSlotsPerBuffer = 64;

    struct RdmaSlotState {
        bool is_allocated;
        bool rdma_write_complete;
        uint32_t offset;
        uint32_t size;
    };

    void* AllocRdmaSlot(uint32_t size, uint32_t* out_slot_id) {
        if (next_slot_id_ >= kMaxSlotsPerBuffer) {
            return nullptr;
        }
        void* slot = Reserve(size);
        if (slot) {
            uint32_t slot_id = next_slot_id_++;
            rdma_slots_[slot_id] = {true, false, GetOffset(slot), size};
            active_rdma_slot_count_++;
            *out_slot_id = slot_id;
        }
        return slot;
    }

    void MarkRdmaComplete(uint32_t slot_id) {
        if (slot_id < kMaxSlotsPerBuffer) {
            rdma_slots_[slot_id].rdma_write_complete = true;
        }
    }

    void ReleaseRdmaSlot(uint32_t slot_id) {
        if (slot_id < kMaxSlotsPerBuffer && rdma_slots_[slot_id].is_allocated) {
            rdma_slots_[slot_id].is_allocated = false;
            active_rdma_slot_count_--;
        }
    }

    bool CanReset() const { return active_rdma_slot_count_ == 0; }

    bool CanSubmit() const {
        for (uint32_t i = 0; i < kMaxSlotsPerBuffer; i++) {
            if (rdma_slots_[i].is_allocated && !rdma_slots_[i].rdma_write_complete) {
                return false;
            }
        }
        return true;
    }

private:
    void* data_;
    size_t capacity_;
    size_t used_;
    size_t entry_count_;
    uint32_t epoch_;

    // RDMA slot management
    uint32_t next_slot_id_;
    uint32_t active_rdma_slot_count_;
    RdmaSlotState rdma_slots_[kMaxSlotsPerBuffer];
};

// Simple ring buffer for buffer management (single producer/single consumer)
template <typename T, size_t N>
class SimpleRing {
public:
    SimpleRing() : head_(0), tail_(0) {}

    bool Enqueue(T item) {
        size_t next_head = (head_ + 1) % N;
        if (next_head == tail_) {
            return false;  // Full
        }
        buffer_[head_] = item;
        head_ = next_head;
        return true;
    }

    bool Dequeue(T* item) {
        if (tail_ == head_) {
            return false;  // Empty
        }
        *item = buffer_[tail_];
        tail_ = (tail_ + 1) % N;
        return true;
    }

    size_t Count() const {
        if (head_ >= tail_) {
            return head_ - tail_;
        }
        return N - tail_ + head_;
    }

    bool IsEmpty() const { return head_ == tail_; }

private:
    T buffer_[N];
    size_t head_;
    size_t tail_;
};

// Append buffer manager
class AppendBufferManager {
public:
    AppendBufferManager() : active_buffer_(nullptr), backpressure_active_(false) {}

    ~AppendBufferManager() {
        // Clean up allocated buffers
        for (auto* buf : all_buffers_) {
            delete buf;
        }
    }

    // Initialize with pre-allocated buffers
    bool Initialize(size_t buffer_count, size_t buffer_size) {
        buffer_size_ = buffer_size;

        for (size_t i = 0; i < buffer_count; i++) {
            void* mem = std::aligned_alloc(kPageSize, buffer_size);
            if (!mem) {
                return false;
            }
            auto* buf = new AppendBuffer(mem, buffer_size);
            all_buffers_.push_back(buf);
            complete_ring_.Enqueue(buf);
        }

        // Get active buffer
        complete_ring_.Dequeue(&active_buffer_);
        return active_buffer_ != nullptr;
    }

    // Reserve space with backpressure check
    void* ReserveWithBackpressure(size_t len, int* error) {
        if (CheckBackpressure()) {
            *error = static_cast<int>(KvError::kBackpressure);
            return nullptr;
        }

        void* slot = Reserve(len);
        if (!slot) {
            *error = static_cast<int>(KvError::kBackpressure);
        } else {
            *error = 0;
        }
        return slot;
    }

    // Submit current buffer to IO queue
    void SubmitCurrentBuffer() {
        if (!active_buffer_ || active_buffer_->IsEmpty()) {
            return;
        }

        submit_ring_.Enqueue(active_buffer_);

        // Try to get free buffer
        if (!complete_ring_.Dequeue(&active_buffer_)) {
            active_buffer_ = nullptr;
        }
    }

    // Get pending buffers for IO submission
    size_t GetPendingBuffers(AppendBuffer** buffers, size_t max_count) {
        size_t count = 0;
        while (count < max_count) {
            AppendBuffer* buf;
            if (!submit_ring_.Dequeue(&buf)) {
                break;
            }
            buffers[count++] = buf;
        }
        return count;
    }

    // Return buffer after IO complete
    void ReturnBuffer(AppendBuffer* buf) {
        if (!buf->CanReset()) {
            pending_reset_queue_.push(buf);
            return;
        }

        buf->Reset();
        complete_ring_.Enqueue(buf);

        if (active_buffer_ == nullptr) {
            complete_ring_.Dequeue(&active_buffer_);
        }
    }

    // Check pending resets (call in polling loop)
    void CheckPendingResets() {
        while (!pending_reset_queue_.empty()) {
            AppendBuffer* buf = pending_reset_queue_.front();
            if (buf->CanReset()) {
                pending_reset_queue_.pop();
                buf->Reset();
                complete_ring_.Enqueue(buf);
            } else {
                break;  // Avoid infinite loop
            }
        }
    }

    // Backpressure management
    bool CheckBackpressure() {
        size_t pending = PendingCount();

        if (backpressure_active_) {
            if (pending <= kBackpressureLowWater) {
                backpressure_active_ = false;
            }
        } else {
            if (pending >= kBackpressureHighWater) {
                backpressure_active_ = true;
            }
        }
        return backpressure_active_;
    }

    bool IsBackpressureActive() const { return backpressure_active_; }
    size_t PendingCount() const { return submit_ring_.Count(); }

    AppendBuffer* GetActiveBuffer() { return active_buffer_; }
    uint32_t CurrentBufferEpoch() const { return active_buffer_ ? active_buffer_->Epoch() : 0; }

    size_t CurrentEntryCount() const { return active_buffer_ ? active_buffer_->EntryCount() : 0; }

private:
    void* Reserve(size_t len) {
        if (active_buffer_ == nullptr || active_buffer_->Used() + len > kFlushThreshold) {
            return nullptr;
        }
        return active_buffer_->Reserve(len);
    }

    size_t buffer_size_ = kAppendBufferSize;
    SimpleRing<AppendBuffer*, kMaxBufferCount + 1> submit_ring_;
    SimpleRing<AppendBuffer*, kMaxBufferCount + 1> complete_ring_;
    AppendBuffer* active_buffer_;
    bool backpressure_active_;
    std::queue<AppendBuffer*> pending_reset_queue_;
    std::vector<AppendBuffer*> all_buffers_;  // For cleanup
};

}  // namespace spdk_kv
