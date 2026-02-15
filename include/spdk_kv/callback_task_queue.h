// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <cstddef>
#include <cstdint>

namespace spdk_kv {

// Callback task queue for deferring work from IO callbacks to the main polling loop.
//
// In SPDK's callback model, IO completion callbacks should be lightweight.
// Heavy operations (RDMA send, user callbacks, index updates) should be
// enqueued here and processed in the main polling loop instead.
class CallbackTaskQueue {
public:
    struct Task {
        enum class Type : uint8_t {
            RDMA_SEND,       // RDMA send operation
            INDEX_UPDATE,    // Index update
            USER_CALLBACK    // User callback execution
        };

        Type type;
        void* context;
        void (*execute)(void* ctx);  // Execution function pointer
    };

    CallbackTaskQueue() : head_(0), tail_(0) {}

    ~CallbackTaskQueue() = default;

    // Non-copyable
    CallbackTaskQueue(const CallbackTaskQueue&) = delete;
    CallbackTaskQueue& operator=(const CallbackTaskQueue&) = delete;

    // enqueue a task (called from IO callbacks, non-blocking)
    bool enqueue(const Task& task) {
        size_t next_head = (head_ + 1) % kCapacity;
        if (next_head == tail_) {
            return false;  // Queue full
        }
        tasks_[head_] = task;
        head_ = next_head;
        return true;
    }

    // Process tasks in batch (called from main polling loop)
    // Returns number of tasks processed
    size_t process_tasks(size_t max_tasks = 64) {
        size_t processed = 0;
        while (processed < max_tasks && tail_ != head_) {
            Task& task = tasks_[tail_];
            if (task.execute) {
                task.execute(task.context);
            }
            tail_ = (tail_ + 1) % kCapacity;
            processed++;
        }
        return processed;
    }

    // Check if queue is empty
    bool is_empty() const { return head_ == tail_; }

    // get number of pending tasks
    size_t count() const {
        if (head_ >= tail_) {
            return head_ - tail_;
        }
        return kCapacity - tail_ + head_;
    }

private:
    static constexpr size_t kCapacity = 4096;

    Task tasks_[kCapacity];
    size_t head_;
    size_t tail_;
};

}  // namespace spdk_kv
