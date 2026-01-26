// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#include "append_buffer.h"
#include <cstring>
#include <cstdlib>

#ifdef WITH_SPDK
#include <spdk/env.h>
#endif

namespace spdk_kv {

// AppendBuffer implementation

AppendBuffer::AppendBuffer(void* data, size_t size)
    : data_(data)
    , size_(size)
    , used_(0)
    , epoch_(0)
    , entry_count_(0)
    , next_slot_id_(0)
    , active_rdma_slot_count_(0) {
    memset(rdma_slots_, 0, sizeof(rdma_slots_));
}

int64_t AppendBuffer::append(const void* data, size_t len) {
    if (used_ + len > size_) {
        return -1;
    }

    void* dst = static_cast<char*>(data_) + used_;
    memcpy(dst, data, len);
    int64_t offset = static_cast<int64_t>(used_);
    used_ += len;
    return offset;
}

void* AppendBuffer::reserve(size_t len) {
    if (used_ + len > size_) {
        return nullptr;
    }

    void* ptr = static_cast<char*>(data_) + used_;
    used_ += len;
    return ptr;
}

uint32_t AppendBuffer::get_offset(void* ptr) const {
    return static_cast<uint32_t>(static_cast<char*>(ptr) - static_cast<char*>(data_));
}

void AppendBuffer::reset() {
    used_ = 0;
    epoch_++;
    entry_count_ = 0;
    next_slot_id_ = 0;
    active_rdma_slot_count_ = 0;
    memset(rdma_slots_, 0, sizeof(rdma_slots_));
}

void* AppendBuffer::alloc_rdma_slot(uint32_t size, uint32_t* out_slot_id) {
    void* slot = reserve(size);
    if (slot) {
        uint32_t slot_id = next_slot_id_++;
        if (slot_id < MAX_SLOTS_PER_BUFFER) {
            rdma_slots_[slot_id] = {
                true,   // is_allocated
                false,  // rdma_write_complete
                get_offset(slot),
                size
            };
            active_rdma_slot_count_++;
            *out_slot_id = slot_id;
        } else {
            // Too many slots, reset counter
            next_slot_id_ = 0;
            *out_slot_id = 0;
        }
    }
    return slot;
}

void AppendBuffer::mark_rdma_complete(uint32_t slot_id) {
    if (slot_id < MAX_SLOTS_PER_BUFFER) {
        rdma_slots_[slot_id].rdma_write_complete = true;
    }
}

void AppendBuffer::release_rdma_slot(uint32_t slot_id) {
    if (slot_id < MAX_SLOTS_PER_BUFFER && rdma_slots_[slot_id].is_allocated) {
        rdma_slots_[slot_id].is_allocated = false;
        if (active_rdma_slot_count_ > 0) {
            active_rdma_slot_count_--;
        }
    }
}

bool AppendBuffer::can_submit() const {
    for (uint32_t i = 0; i < MAX_SLOTS_PER_BUFFER; i++) {
        if (rdma_slots_[i].is_allocated && !rdma_slots_[i].rdma_write_complete) {
            return false;
        }
    }
    return true;
}

// AppendBufferManager implementation

AppendBufferManager::AppendBufferManager(size_t buffer_size,
                                         size_t min_count,
                                         size_t max_count)
    : buffer_size_(buffer_size)
    , min_buffer_count_(min_count)
    , max_buffer_count_(max_count)
    , current_buffer_count_(0)
    , backpressure_active_(false)
    , was_backpressure_active_(false)
    , active_buffer_(nullptr) {
}

AppendBufferManager::~AppendBufferManager() {
    for (auto* buf : all_buffers_) {
        free_dma_buffer(buf->data());
        delete buf;
    }
    all_buffers_.clear();
}

bool AppendBufferManager::init() {
    // Allocate minimum number of buffers
    for (size_t i = 0; i < min_buffer_count_; i++) {
        void* mem = alloc_dma_buffer(buffer_size_);
        if (!mem) {
            return false;
        }

        auto* buf = new AppendBuffer(mem, buffer_size_);
        all_buffers_.push_back(buf);
        free_queue_.push(buf);
        current_buffer_count_++;
    }

    // Get first active buffer
    if (!free_queue_.empty()) {
        active_buffer_ = free_queue_.front();
        free_queue_.pop();
    }

    return active_buffer_ != nullptr;
}

void* AppendBufferManager::alloc_dma_buffer(size_t size) {
#ifdef WITH_SPDK
    return spdk_dma_zmalloc(size, ALIGNMENT, nullptr);
#else
    void* ptr = aligned_alloc(ALIGNMENT, size);
    if (ptr) {
        memset(ptr, 0, size);
    }
    return ptr;
#endif
}

void AppendBufferManager::free_dma_buffer(void* buf) {
#ifdef WITH_SPDK
    spdk_dma_free(buf);
#else
    free(buf);
#endif
}

void* AppendBufferManager::reserve_with_backpressure(size_t len, int* error) {
    if (check_backpressure()) {
        *error = ERR_BACKPRESSURE;
        return nullptr;
    }

    void* slot = reserve(len);
    if (!slot) {
        *error = ERR_BACKPRESSURE;
    }
    return slot;
}

void* AppendBufferManager::reserve(size_t len) {
    if (active_buffer_ == nullptr) {
        return nullptr;
    }

    if (active_buffer_->used() + len > FLUSH_THRESHOLD) {
        return nullptr;
    }

    return active_buffer_->reserve(len);
}

void AppendBufferManager::submit_current_buffer() {
    if (active_buffer_ == nullptr || active_buffer_->used() == 0) {
        return;
    }

    // Put current buffer in submit queue
    submit_queue_.push(active_buffer_);

    // Try to get a free buffer
    if (!free_queue_.empty()) {
        active_buffer_ = free_queue_.front();
        free_queue_.pop();
    } else {
        active_buffer_ = nullptr;
    }
}

size_t AppendBufferManager::get_pending_buffers(AppendBuffer** buffers, size_t max_count) {
    size_t count = 0;
    while (count < max_count && !submit_queue_.empty()) {
        buffers[count++] = submit_queue_.front();
        submit_queue_.pop();
    }
    return count;
}

void AppendBufferManager::return_buffer(AppendBuffer* buf) {
    if (!buf->can_reset()) {
        pending_reset_queue_.push(buf);
        return;
    }

    buf->reset();
    free_queue_.push(buf);

    if (active_buffer_ == nullptr) {
        active_buffer_ = free_queue_.front();
        free_queue_.pop();
    }
}

void AppendBufferManager::check_pending_resets() {
    size_t count = pending_reset_queue_.size();
    for (size_t i = 0; i < count; i++) {
        AppendBuffer* buf = pending_reset_queue_.front();
        pending_reset_queue_.pop();

        if (buf->can_reset()) {
            buf->reset();
            free_queue_.push(buf);
        } else {
            pending_reset_queue_.push(buf);
        }
    }

    // Try to get active buffer if needed
    if (active_buffer_ == nullptr && !free_queue_.empty()) {
        active_buffer_ = free_queue_.front();
        free_queue_.pop();
    }
}

bool AppendBufferManager::check_backpressure() {
    size_t pending = pending_count();

    if (backpressure_active_) {
        if (pending <= BACKPRESSURE_LOW_WATER) {
            backpressure_active_ = false;
        }
    } else {
        if (pending >= BACKPRESSURE_HIGH_WATER) {
            if (!try_expand()) {
                backpressure_active_ = true;
            }
        }
    }
    return backpressure_active_;
}

bool AppendBufferManager::try_expand() {
    if (current_buffer_count_ >= max_buffer_count_) {
        return false;
    }

    void* mem = alloc_dma_buffer(buffer_size_);
    if (!mem) {
        return false;
    }

    auto* buf = new AppendBuffer(mem, buffer_size_);
    all_buffers_.push_back(buf);
    free_queue_.push(buf);
    current_buffer_count_++;
    return true;
}

uint32_t AppendBufferManager::current_buffer_epoch() const {
    return active_buffer_ ? active_buffer_->epoch() : 0;
}

uint32_t AppendBufferManager::get_offset(void* ptr) const {
    return active_buffer_ ? active_buffer_->get_offset(ptr) : 0;
}

size_t AppendBufferManager::pending_count() const {
    return submit_queue_.size() + pending_reset_queue_.size();
}

size_t AppendBufferManager::current_entry_count() const {
    return active_buffer_ ? active_buffer_->entry_count() : 0;
}

void AppendBufferManager::inc_entry_count() {
    if (active_buffer_) {
        active_buffer_->inc_entry_count();
    }
}

void AppendBufferManager::register_resume_callback(BackpressureCallback cb, void* arg) {
    resume_queue_.push({cb, arg});
}

void AppendBufferManager::process_resume_notifications() {
    if (!was_backpressure_active_ && !backpressure_active_) {
        return;
    }

    if (was_backpressure_active_ && !backpressure_active_) {
        // Backpressure just released, notify all waiters
        while (!resume_queue_.empty()) {
            auto notification = resume_queue_.front();
            resume_queue_.pop();
            if (notification.callback) {
                notification.callback(notification.arg);
            }
        }
    }

    was_backpressure_active_ = backpressure_active_;
}

}  // namespace spdk_kv
