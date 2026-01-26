// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#pragma once

#include <spdk_kv/types.h>
#include <spdk_kv/config.h>
#include <cstdint>
#include <cstddef>
#include <atomic>
#include <queue>
#include <vector>

#ifdef WITH_SPDK
#include <spdk/env.h>
#endif

namespace spdk_kv {

// Forward declarations
class DmaMemoryPool;

// Single append buffer
class AppendBuffer {
public:
    static constexpr size_t MAX_SLOTS_PER_BUFFER = 64;

    AppendBuffer(void* data, size_t size);
    ~AppendBuffer() = default;

    // Append data to buffer
    int64_t append(const void* data, size_t len);

    // Reserve space in buffer
    void* reserve(size_t len);

    // Get offset of pointer within buffer
    uint32_t get_offset(void* ptr) const;

    // Reset buffer for reuse
    void reset();

    // Accessors
    void* data() { return data_; }
    const void* data() const { return data_; }
    size_t size() const { return size_; }
    size_t used() const { return used_; }
    size_t available() const { return size_ - used_; }
    uint32_t epoch() const { return epoch_; }
    size_t entry_count() const { return entry_count_; }

    // RDMA slot management
    struct RdmaSlotState {
        bool is_allocated;
        bool rdma_write_complete;
        uint32_t offset;
        uint32_t size;
    };

    void* alloc_rdma_slot(uint32_t size, uint32_t* out_slot_id);
    void mark_rdma_complete(uint32_t slot_id);
    void release_rdma_slot(uint32_t slot_id);
    bool can_reset() const { return active_rdma_slot_count_ == 0; }
    bool can_submit() const;

    // Increment entry count
    void inc_entry_count() { entry_count_++; }

private:
    void* data_;
    size_t size_;
    size_t used_;
    uint32_t epoch_;
    size_t entry_count_;

    // RDMA slots
    RdmaSlotState rdma_slots_[MAX_SLOTS_PER_BUFFER];
    uint32_t next_slot_id_;
    uint32_t active_rdma_slot_count_;
};

// Append buffer manager with dynamic buffer allocation
class AppendBufferManager {
public:
    static constexpr size_t BUFFER_SIZE = 2 * 1024 * 1024;  // 2MB
    static constexpr size_t MIN_BUFFER_COUNT = 4;
    static constexpr size_t MAX_BUFFER_COUNT = 16;
    static constexpr size_t FLUSH_THRESHOLD = BUFFER_SIZE - 64 * 1024;
    static constexpr size_t BACKPRESSURE_HIGH_WATER = 12;
    static constexpr size_t BACKPRESSURE_LOW_WATER = 6;
    static constexpr int ERR_BACKPRESSURE = -11;  // EAGAIN

    AppendBufferManager(size_t buffer_size = BUFFER_SIZE,
                        size_t min_count = MIN_BUFFER_COUNT,
                        size_t max_count = MAX_BUFFER_COUNT);
    ~AppendBufferManager();

    // Initialize buffers
    bool init();

    // Reserve space with backpressure check
    void* reserve_with_backpressure(size_t len, int* error);

    // Submit current buffer to IO queue
    void submit_current_buffer();

    // Get pending buffers for IO submission
    size_t get_pending_buffers(AppendBuffer** buffers, size_t max_count);

    // Return buffer after IO completion
    void return_buffer(AppendBuffer* buf);

    // Check pending buffers for reset
    void check_pending_resets();

    // Backpressure management
    bool check_backpressure();
    bool is_backpressure_active() const { return backpressure_active_; }

    // Get current buffer epoch
    uint32_t current_buffer_epoch() const;

    // Get offset within active buffer
    uint32_t get_offset(void* ptr) const;

    // Get pending buffer count
    size_t pending_count() const;

    // Get current entry count
    size_t current_entry_count() const;

    // Increment entry count
    void inc_entry_count();

    // Get active buffer
    AppendBuffer* active_buffer() { return active_buffer_; }

    // Resume notification callback
    using BackpressureCallback = void (*)(void* arg);
    void register_resume_callback(BackpressureCallback cb, void* arg);
    void process_resume_notifications();

private:
    // Reserve space in active buffer
    void* reserve(size_t len);

    // Try to expand buffer count
    bool try_expand();

    // Allocate DMA buffer
    void* alloc_dma_buffer(size_t size);
    void free_dma_buffer(void* buf);

    size_t buffer_size_;
    size_t min_buffer_count_;
    size_t max_buffer_count_;
    size_t current_buffer_count_;
    bool backpressure_active_;
    bool was_backpressure_active_;

    AppendBuffer* active_buffer_;
    std::vector<AppendBuffer*> all_buffers_;
    std::queue<AppendBuffer*> free_queue_;
    std::queue<AppendBuffer*> submit_queue_;
    std::queue<AppendBuffer*> pending_reset_queue_;

    // Resume notifications
    struct ResumeNotification {
        BackpressureCallback callback;
        void* arg;
    };
    std::queue<ResumeNotification> resume_queue_;
};

}  // namespace spdk_kv
