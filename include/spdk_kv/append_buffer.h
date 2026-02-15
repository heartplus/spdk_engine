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

    // reserve space in the buffer
    void* reserve(size_t len) {
        if (used_ + len > capacity_) {
            return nullptr;
        }
        void* ptr = static_cast<char*>(data_) + used_;
        used_ += len;
        entry_count_++;
        return ptr;
    }

    // append data to the buffer
    int64_t append(const void* data, size_t len) {
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

    // get offset of a pointer within the buffer
    uint32_t get_offset(void* ptr) const {
        return static_cast<uint32_t>(static_cast<char*>(ptr) - static_cast<char*>(data_));
    }

    // reset buffer for reuse
    void reset() {
        used_ = 0;
        entry_count_ = 0;
        epoch_++;
        next_slot_id_ = 0;
        active_rdma_slot_count_ = 0;
        std::memset(rdma_slots_, 0, sizeof(rdma_slots_));
    }

    // Accessors
    void* data() { return data_; }
    const void* data() const { return data_; }
    size_t capacity() const { return capacity_; }
    size_t used() const { return used_; }
    size_t entry_count() const { return entry_count_; }
    uint32_t epoch() const { return epoch_; }

    bool is_full() const { return used_ >= kFlushThreshold; }
    bool is_empty() const { return used_ == 0; }

    // RDMA slot management
    static constexpr size_t kMaxSlotsPerBuffer = 64;

    struct RdmaSlotState {
        bool is_allocated;
        bool rdma_write_complete;
        uint32_t offset;
        uint32_t size;
    };

    void* alloc_rdma_slot(uint32_t size, uint32_t* out_slot_id) {
        if (next_slot_id_ >= kMaxSlotsPerBuffer) {
            return nullptr;
        }
        void* slot = reserve(size);
        if (slot) {
            uint32_t slot_id = next_slot_id_++;
            rdma_slots_[slot_id] = {true, false, get_offset(slot), size};
            active_rdma_slot_count_++;
            *out_slot_id = slot_id;
        }
        return slot;
    }

    void mark_rdma_complete(uint32_t slot_id) {
        if (slot_id < kMaxSlotsPerBuffer) {
            rdma_slots_[slot_id].rdma_write_complete = true;
        }
    }

    void release_rdma_slot(uint32_t slot_id) {
        if (slot_id < kMaxSlotsPerBuffer && rdma_slots_[slot_id].is_allocated) {
            rdma_slots_[slot_id].is_allocated = false;
            active_rdma_slot_count_--;
        }
    }

    bool can_reset() const { return active_rdma_slot_count_ == 0; }

    bool can_submit() const {
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

    bool enqueue(T item) {
        size_t next_head = (head_ + 1) % N;
        if (next_head == tail_) {
            return false;  // Full
        }
        buffer_[head_] = item;
        head_ = next_head;
        return true;
    }

    bool dequeue(T* item) {
        if (tail_ == head_) {
            return false;  // Empty
        }
        *item = buffer_[tail_];
        tail_ = (tail_ + 1) % N;
        return true;
    }

    size_t count() const {
        if (head_ >= tail_) {
            return head_ - tail_;
        }
        return N - tail_ + head_;
    }

    bool is_empty() const { return head_ == tail_; }

private:
    T buffer_[N];
    size_t head_;
    size_t tail_;
};

// append buffer manager
class AppendBufferManager {
public:
    using BackpressureCallback = void (*)(void* arg);

    AppendBufferManager()
            : active_buffer_(nullptr),
              backpressure_active_(false),
              current_buffer_count_(0),
              was_backpressure_active_(false) {}

    ~AppendBufferManager() {
        // Clean up allocated buffers
        for (auto* buf : all_buffers_) {
            delete buf;
        }
    }

    // initialize with pre-allocated buffers
    bool initialize(size_t buffer_count, size_t buffer_size) {
        buffer_size_ = buffer_size;

        for (size_t i = 0; i < buffer_count; i++) {
            void* mem = std::aligned_alloc(kPageSize, buffer_size);
            if (!mem) {
                return false;
            }
            auto* buf = new AppendBuffer(mem, buffer_size);
            all_buffers_.push_back(buf);
            complete_ring_.enqueue(buf);
        }

        current_buffer_count_ = buffer_count;

        // get active buffer
        complete_ring_.dequeue(&active_buffer_);
        return active_buffer_ != nullptr;
    }

    // reserve space with backpressure check
    void* reserve_with_backpressure(size_t len, int* error) {
        if (check_backpressure()) {
            *error = static_cast<int>(KvError::kBackpressure);
            return nullptr;
        }

        void* slot = reserve(len);
        if (!slot) {
            *error = static_cast<int>(KvError::kBackpressure);
        } else {
            *error = 0;
        }
        return slot;
    }

    // Submit current buffer to IO queue
    void submit_current_buffer() {
        if (!active_buffer_ || active_buffer_->is_empty()) {
            return;
        }

        submit_ring_.enqueue(active_buffer_);

        // Try to get free buffer
        if (!complete_ring_.dequeue(&active_buffer_)) {
            active_buffer_ = nullptr;
        }
    }

    // get pending buffers for IO submission
    size_t get_pending_buffers(AppendBuffer** buffers, size_t max_count) {
        size_t count = 0;
        while (count < max_count) {
            AppendBuffer* buf;
            if (!submit_ring_.dequeue(&buf)) {
                break;
            }
            buffers[count++] = buf;
        }
        return count;
    }

    // Return buffer after IO complete
    void return_buffer(AppendBuffer* buf) {
        if (!buf->can_reset()) {
            pending_reset_queue_.push(buf);
            return;
        }

        buf->reset();
        complete_ring_.enqueue(buf);

        if (active_buffer_ == nullptr) {
            complete_ring_.dequeue(&active_buffer_);
        }
    }

    // Check pending resets (call in polling loop)
    void check_pending_resets() {
        while (!pending_reset_queue_.empty()) {
            AppendBuffer* buf = pending_reset_queue_.front();
            if (buf->can_reset()) {
                pending_reset_queue_.pop();
                buf->reset();
                complete_ring_.enqueue(buf);
            } else {
                break;  // Avoid infinite loop
            }
        }
    }

    // Dynamically expand buffer count
    bool try_expand() {
        if (current_buffer_count_ >= kMaxBufferCount) {
            return false;
        }

        void* mem = std::aligned_alloc(kPageSize, buffer_size_);
        if (!mem) {
            return false;
        }

        auto* buf = new AppendBuffer(mem, buffer_size_);
        all_buffers_.push_back(buf);
        complete_ring_.enqueue(buf);
        current_buffer_count_++;

        if (active_buffer_ == nullptr) {
            complete_ring_.dequeue(&active_buffer_);
        }

        return true;
    }

    // Backpressure management
    bool check_backpressure() {
        size_t pending = pending_count();

        if (backpressure_active_) {
            if (pending <= kBackpressureLowWater) {
                backpressure_active_ = false;
            }
        } else {
            if (pending >= kBackpressureHighWater) {
                if (!try_expand()) {
                    backpressure_active_ = true;
                }
            }
        }
        return backpressure_active_;
    }

    bool is_backpressure_active() const { return backpressure_active_; }
    size_t pending_count() const { return submit_ring_.count(); }

    AppendBuffer* get_active_buffer() { return active_buffer_; }
    uint32_t current_buffer_epoch() const { return active_buffer_ ? active_buffer_->epoch() : 0; }

    size_t current_entry_count() const { return active_buffer_ ? active_buffer_->entry_count() : 0; }

    // Register a callback to be invoked when backpressure is relieved
    void register_resume_callback(BackpressureCallback cb, void* arg) {
        resume_callbacks_.enqueue({cb, arg});
    }

    // Check and fire resume notifications on backpressure transition
    void process_resume_notifications() {
        if (!was_backpressure_active_ && !backpressure_active_) {
            return;
        }

        if (was_backpressure_active_ && !backpressure_active_) {
            ResumeNotification notification;
            while (resume_callbacks_.dequeue(&notification)) {
                notification.callback(notification.arg);
            }
        }

        was_backpressure_active_ = backpressure_active_;
    }

    // RDMA flow control adapter
    class RdmaFlowController {
    public:
        explicit RdmaFlowController(AppendBufferManager* mgr) : buffer_mgr_(mgr) {}

        bool can_accept_request() { return !buffer_mgr_->is_backpressure_active(); }

        bool try_process_or_defer(void* rdma_ctx, BackpressureCallback on_resume) {
            if (can_accept_request()) {
                return true;
            }
            buffer_mgr_->register_resume_callback(on_resume, rdma_ctx);
            return false;
        }

    private:
        AppendBufferManager* buffer_mgr_;
    };

    RdmaFlowController* create_flow_controller() { return new RdmaFlowController(this); }

private:
    void* reserve(size_t len) {
        if (active_buffer_ == nullptr || active_buffer_->used() + len > kFlushThreshold) {
            return nullptr;
        }
        return active_buffer_->reserve(len);
    }

    struct ResumeNotification {
        BackpressureCallback callback;
        void* arg;
    };

    size_t buffer_size_ = kAppendBufferSize;
    SimpleRing<AppendBuffer*, kMaxBufferCount + 1> submit_ring_;
    SimpleRing<AppendBuffer*, kMaxBufferCount + 1> complete_ring_;
    AppendBuffer* active_buffer_;
    bool backpressure_active_;
    size_t current_buffer_count_;
    bool was_backpressure_active_;
    SimpleRing<ResumeNotification, 256> resume_callbacks_;
    std::queue<AppendBuffer*> pending_reset_queue_;
    std::vector<AppendBuffer*> all_buffers_;  // For cleanup
};

}  // namespace spdk_kv
