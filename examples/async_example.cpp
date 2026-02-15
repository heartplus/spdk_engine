// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

// Async API example with SPSC cross-thread communication
// - Main thread: polling loop, executes tasks from SPSC queue
// - Worker thread: sends tasks via SPSC, controls in-flight count

#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "spdk_kv/spdk_kv.h"

using namespace spdk_kv;

// =============================================================================
// Lock-free SPSC Queue (Single Producer Single Consumer)
// =============================================================================
template <typename T, size_t capacity>
class SpscQueue {
public:
    SpscQueue() : head_(0), tail_(0) {}

    // Producer: try to enqueue an item
    bool try_push(const T& item) {
        size_t head = head_.load(std::memory_order_relaxed);
        size_t next_head = (head + 1) % capacity;

        if (next_head == tail_.load(std::memory_order_acquire)) {
            return false;  // Queue is full
        }

        buffer_[head] = item;
        head_.store(next_head, std::memory_order_release);
        return true;
    }

    // Consumer: try to dequeue an item
    bool try_pop(T& item) {
        size_t tail = tail_.load(std::memory_order_relaxed);

        if (tail == head_.load(std::memory_order_acquire)) {
            return false;  // Queue is empty
        }

        item = buffer_[tail];
        tail_.store((tail + 1) % capacity, std::memory_order_release);
        return true;
    }

    bool is_empty() const {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

    size_t size() const {
        size_t head = head_.load(std::memory_order_acquire);
        size_t tail = tail_.load(std::memory_order_acquire);
        return (head >= tail) ? (head - tail) : (capacity - tail + head);
    }

private:
    alignas(64) std::atomic<size_t> head_;
    alignas(64) std::atomic<size_t> tail_;
    T buffer_[capacity];
};

// =============================================================================
// Task definitions
// =============================================================================
enum class TaskType { kPut, kGet, kDelete, kStop };

struct Task {
    TaskType type;
    uint64_t key;
    std::vector<char> value;      // For put: value to write
    std::vector<char>* read_buf;  // For get: buffer to read into
    uint32_t value_len;
    uint64_t task_id;
};

struct TaskResult {
    uint64_t task_id;
    TaskType type;
    int status;
    uint32_t actual_len;  // For get operations
};

// =============================================================================
// Global queues and state
// =============================================================================
constexpr size_t kQueueCapacity = 4096;

// Task queue: worker thread -> main thread
SpscQueue<Task, kQueueCapacity> g_task_queue;

// Result queue: main thread -> worker thread
SpscQueue<TaskResult, kQueueCapacity> g_result_queue;

// Engine pointer (set by main thread)
Engine* g_engine = nullptr;

// Statistics
std::atomic<uint64_t> g_submitted{0};
std::atomic<uint64_t> g_completed{0};
std::atomic<bool> g_running{true};

// =============================================================================
// Completion context for async callbacks
// =============================================================================
struct CompletionContext {
    uint64_t task_id;
    TaskType type;
    SegmentBuf* segment_buf;  // For get: holds output buffer pointers
    void* dma_buffer;         // For get: DMA buffer to be freed after completion
};

void on_put_complete(void* arg, int status) {
    auto* ctx = static_cast<CompletionContext*>(arg);
    TaskResult result{ctx->task_id, ctx->type, status, 0};

    // Send result back to worker thread
    while (!g_result_queue.try_push(result)) {
        // Result queue full, spin wait (should rarely happen)
    }

    delete ctx;
    g_completed++;
}

void on_get_complete(void* arg, int status, uint32_t actual_len) {
    auto* ctx = static_cast<CompletionContext*>(arg);
    TaskResult result{ctx->task_id, ctx->type, status, actual_len};

    // free the DMA buffer that was pre-allocated for get_async
    if (ctx->dma_buffer) {
        DmaAllocator::free(ctx->dma_buffer);
    }
    delete ctx->segment_buf;

    while (!g_result_queue.try_push(result)) {
        // Result queue full, spin wait
    }

    delete ctx;
    g_completed++;
}

void on_delete_complete(void* arg, int status) {
    auto* ctx = static_cast<CompletionContext*>(arg);
    TaskResult result{ctx->task_id, ctx->type, status, 0};

    while (!g_result_queue.try_push(result)) {
        // Result queue full, spin wait
    }

    delete ctx;
    g_completed++;
}

// =============================================================================
// Main thread: polling loop
// =============================================================================
void main_thread_loop() {
    std::cout << "[Main] Polling loop started" << std::endl;

    while (g_running.load(std::memory_order_relaxed)) {
        // 1. Process tasks from task queue
        Task task;
        while (g_task_queue.try_pop(task)) {
            if (task.type == TaskType::kStop) {
                std::cout << "[Main] Received stop signal" << std::endl;
                g_running.store(false, std::memory_order_relaxed);
                break;
            }

            auto* ctx = new CompletionContext{task.task_id, task.type, nullptr, nullptr};

            switch (task.type) {
                case TaskType::kPut:
                    g_engine->put_buffered(task.key, task.value.data(), task.value_len,
                                          on_put_complete, ctx);
                    break;

                case TaskType::kGet: {
                    // Pre-allocate DMA buffer for get_async (zero-copy read)
                    // Use the expected read buffer size, aligned to 4KB
                    uint32_t buf_size = align_up(
                            static_cast<uint64_t>(task.read_buf->size()) + kSegmentDataOffset,
                            kPageSize);
                    void* dma_buf = DmaAllocator::alloc(buf_size, kPageSize);
                    if (!dma_buf) {
                        // Allocation failed, report error via result queue
                        TaskResult result{task.task_id, task.type,
                                          static_cast<int>(KvError::kInternalError), 0};
                        while (!g_result_queue.try_push(result)) {}
                        delete ctx;
                        g_completed++;
                        break;
                    }

                    // Setup SegmentBuf with pre-allocated DMA buffer
                    ctx->segment_buf = new SegmentBuf{};
                    ctx->segment_buf->cnt_ = 1;
                    ctx->segment_buf->buffers_[0].iov_base = dma_buf;
                    ctx->segment_buf->buffers_[0].iov_len = buf_size;
                    ctx->dma_buffer = dma_buf;

                    g_engine->get_async(task.key, ctx->segment_buf, on_get_complete, ctx);
                    break;
                }

                case TaskType::kDelete:
                    g_engine->delete_async(task.key, on_delete_complete, ctx);
                    break;

                default:
                    delete ctx;
                    break;
            }
        }

        // 2. poll engine for completions
        g_engine->poll();
    }

    // Drain remaining tasks
    std::cout << "[Main] Draining remaining tasks..." << std::endl;
    while (g_submitted.load() > g_completed.load()) {
        g_engine->poll();
    }

    std::cout << "[Main] Polling loop ended" << std::endl;
}

// =============================================================================
// Worker thread: task generator and in-flight controller
// =============================================================================
struct WorkerConfig {
    int total_ops;
    int max_inflight;
    int put_ratio;  // percentage (0-100)
    int get_ratio;  // percentage (0-100)
    int del_ratio;  // percentage (0-100)
};

// find a valid key for read/delete (search backwards from current position)
uint64_t find_valid_key(const std::vector<bool>& key_written, int ops_submitted) {
    for (int j = ops_submitted; j > 0; j--) {
        if (key_written[j]) {
            return static_cast<uint64_t>(j);
        }
    }
    return 0;
}

// create a put task
void make_put_task(Task& task, int ops_submitted, std::vector<bool>& key_written) {
    task.type = TaskType::kPut;
    task.key = ops_submitted + 1;
    std::string val = "value_" + std::to_string(task.key) + "_data_padding_for_test";
    task.value.assign(val.begin(), val.end());
    task.value_len = static_cast<uint32_t>(val.size());
    task.read_buf = nullptr;
    key_written[task.key] = true;
}

// create a get task, returns false if fallback to put is needed
bool make_get_task(Task& task, int ops_submitted, std::vector<bool>& key_written,
                 std::vector<char>* free_buf) {
    uint64_t get_key = find_valid_key(key_written, ops_submitted);
    if (get_key > 0 && free_buf) {
        task.type = TaskType::kGet;
        task.key = get_key;
        task.read_buf = free_buf;
        return true;
    }
    return false;
}

// create a delete task, returns false if fallback to put is needed
bool make_delete_task(Task& task, int ops_submitted, std::vector<bool>& key_written) {
    uint64_t del_key = find_valid_key(key_written, ops_submitted);
    if (del_key > 0) {
        task.type = TaskType::kDelete;
        task.key = del_key;
        key_written[del_key] = false;
        return true;
    }
    return false;
}

void worker_thread_loop(const WorkerConfig& config) {
    std::cout << "[Worker] Started (total_ops=" << config.total_ops
              << ", max_inflight=" << config.max_inflight << ")" << std::endl;

    // Allocate read buffers for get operations
    std::vector<std::vector<char>> read_buffers(config.max_inflight, std::vector<char>(4096));
    std::vector<bool> buffer_in_use(config.max_inflight, false);

    auto get_free_buffer = [&]() -> std::vector<char>* {
        for (int i = 0; i < config.max_inflight; i++) {
            if (!buffer_in_use[i]) {
                buffer_in_use[i] = true;
                return &read_buffers[i];
            }
        }
        return nullptr;
    };

    [[maybe_unused]] auto release_buffer = [&](std::vector<char>* buf) {
        for (int i = 0; i < config.max_inflight; i++) {
            if (&read_buffers[i] == buf) {
                buffer_in_use[i] = false;
                return;
            }
        }
    };

    uint64_t task_id = 0;
    int ops_submitted = 0;
    int ops_completed_local = 0;
    int inflight = 0;

    // Statistics
    int put_success = 0, put_fail = 0;
    int get_success = 0, get_fail = 0;
    int del_success = 0, del_fail = 0;

    // Track which keys have been written (for valid gets)
    std::vector<bool> key_written(config.total_ops + 1, false);

    auto start_time = std::chrono::steady_clock::now();

    while (ops_completed_local < config.total_ops || inflight > 0) {
        // 1. Process results from main thread
        TaskResult result;
        while (g_result_queue.try_pop(result)) {
            inflight--;
            ops_completed_local++;

            switch (result.type) {
                case TaskType::kPut:
                    if (result.status == 0) {
                        put_success++;
                    } else {
                        put_fail++;
                    }
                    break;

                case TaskType::kGet:
                    if (result.status == 0) {
                        get_success++;
                    } else {
                        get_fail++;
                    }
                    // release buffer (simplified - in real code track which buffer was used)
                    break;

                case TaskType::kDelete:
                    if (result.status == 0) {
                        del_success++;
                    } else {
                        del_fail++;
                    }
                    break;

                default:
                    break;
            }
        }

        // 2. Submit new tasks if under limit
        while (ops_submitted < config.total_ops && inflight < config.max_inflight) {
            Task task;
            task.task_id = task_id++;

            // Determine operation type based on ratios
            int type_rand = ops_submitted % 100;

            if (type_rand < config.put_ratio) {
                make_put_task(task, ops_submitted, key_written);
            } else if (type_rand < config.put_ratio + config.get_ratio) {
                if (!make_get_task(task, ops_submitted, key_written, get_free_buffer())) {
                    make_put_task(task, ops_submitted, key_written);
                }
            } else {
                if (!make_delete_task(task, ops_submitted, key_written)) {
                    make_put_task(task, ops_submitted, key_written);
                }
            }

            // Try to submit task
            if (g_task_queue.try_push(task)) {
                g_submitted++;
                ops_submitted++;
                inflight++;
            } else {
                // Queue full, wait a bit
                break;
            }
        }

        // Small yield to avoid busy spinning
        if (inflight >= config.max_inflight || ops_submitted >= config.total_ops) {
            std::this_thread::yield();
        }
    }

    auto end_time = std::chrono::steady_clock::now();
    auto duration_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // Send stop signal
    Task stop_task;
    stop_task.type = TaskType::kStop;
    while (!g_task_queue.try_push(stop_task)) {
        std::this_thread::yield();
    }

    std::cout << std::endl;
    std::cout << "[Worker] Completed!" << std::endl;
    std::cout << "[Worker] Statistics:" << std::endl;
    std::cout << "  Total operations: " << ops_completed_local << std::endl;
    std::cout << "  Duration: " << duration_ms << " ms" << std::endl;
    if (duration_ms > 0) {
        std::cout << "  Throughput: " << (ops_completed_local * 1000.0 / duration_ms) << " ops/sec"
                  << std::endl;
    }
    std::cout << "  put: " << put_success << " success, " << put_fail << " fail" << std::endl;
    std::cout << "  get: " << get_success << " success, " << get_fail << " fail" << std::endl;
    std::cout << "  del: " << del_success << " success, " << del_fail << " fail" << std::endl;
}

// =============================================================================
// Usage
// =============================================================================
void print_usage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --bdev <name>      SPDK bdev name (e.g., Malloc0, Nvme0n1)" << std::endl;
    std::cout << "  --num-ops <n>      Total number of operations (default: 10000)" << std::endl;
    std::cout << "  --max-inflight <n> Max in-flight operations (default: 64)" << std::endl;
    std::cout << "  --put-ratio <n>    put operation ratio % (default: 70)" << std::endl;
    std::cout << "  --get-ratio <n>    get operation ratio % (default: 25)" << std::endl;
    std::cout << "  --del-ratio <n>    del operation ratio % (default: 5)" << std::endl;
    std::cout << "  --help             Show this help" << std::endl;
}

// =============================================================================
// Main
// =============================================================================
int main(int argc, char** argv) {
    std::string bdev_name;

    WorkerConfig config;
    config.total_ops = 10000;
    config.max_inflight = 64;
    config.put_ratio = 70;
    config.get_ratio = 25;
    config.del_ratio = 5;

    // Parse arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--bdev" && i + 1 < argc) {
            bdev_name = argv[++i];
        } else if (arg == "--num-ops" && i + 1 < argc) {
            config.total_ops = std::stoi(argv[++i]);
        } else if (arg == "--max-inflight" && i + 1 < argc) {
            config.max_inflight = std::stoi(argv[++i]);
        } else if (arg == "--put-ratio" && i + 1 < argc) {
            config.put_ratio = std::stoi(argv[++i]);
        } else if (arg == "--get-ratio" && i + 1 < argc) {
            config.get_ratio = std::stoi(argv[++i]);
        } else if (arg == "--del-ratio" && i + 1 < argc) {
            config.del_ratio = std::stoi(argv[++i]);
        } else if (arg == "--help") {
            print_usage(argv[0]);
            return 0;
        }
    }

    // Normalize ratios
    int total_ratio = config.put_ratio + config.get_ratio + config.del_ratio;
    if (total_ratio != 100) {
        std::cout << "Warning: Ratios don't sum to 100, normalizing..." << std::endl;
        config.put_ratio = config.put_ratio * 100 / total_ratio;
        config.get_ratio = config.get_ratio * 100 / total_ratio;
        config.del_ratio = 100 - config.put_ratio - config.get_ratio;
    }

    std::cout << "========================================" << std::endl;
    std::cout << "  SPDK KV Engine - SPSC Async Example" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << std::endl;

    std::cout << "Initializing SPDK environment..." << std::endl;

    SpdkEnvOpts spdk_opts;
    spdk_opts.name = "spsc_example";
    spdk_opts.bdev_name = bdev_name;

    auto& env = SpdkEnv::instance();
    if (env.initialize(spdk_opts)) {
        std::cout << "SPDK initialized with bdev: " << bdev_name << std::endl;
    } else {
        std::cerr << "Failed to initialize SPDK" << std::endl;
        return -1;
    }

    std::cout << "Running in SPDK mode" << std::endl;

    std::cout << std::endl;
    std::cout << "Configuration:" << std::endl;
    std::cout << "  Total operations: " << config.total_ops << std::endl;
    std::cout << "  Max in-flight:    " << config.max_inflight << std::endl;
    std::cout << "  put ratio:        " << config.put_ratio << "%" << std::endl;
    std::cout << "  get ratio:        " << config.get_ratio << "%" << std::endl;
    std::cout << "  del ratio:     " << config.del_ratio << "%" << std::endl;
    std::cout << std::endl;

    // create engine
    Engine engine;
    CreateOpts create_opts;
    create_opts.config.max_entries = static_cast<uint64_t>(std::max(100000, config.total_ops * 2));
    create_opts.config.data_file_size = 256 * 1024 * 1024;  // 256MB

    std::cout << "Creating engine..." << std::endl;
    KvError err = engine.create("/tmp/spdk_kv_spsc", create_opts);
    if (err != KvError::kSuccess) {
        std::cerr << "Failed to create engine: " << static_cast<int>(err) << std::endl;
        return 1;
    }
    std::cout << "Engine created!" << std::endl;
    std::cout << std::endl;

    g_engine = &engine;
    g_running.store(true);
    g_submitted.store(0);
    g_completed.store(0);

    // start worker thread
    std::cout << "Starting threads..." << std::endl;
    std::thread worker_thread(worker_thread_loop, config);

    // Main thread runs polling loop
    main_thread_loop();

    // Wait for worker to finish
    worker_thread.join();

    std::cout << std::endl;
    std::cout << "--- Final Statistics ---" << std::endl;
    std::cout << "Entry count: " << engine.get_entry_count() << std::endl;
    std::cout << "Total data bytes: " << engine.get_total_data_bytes() << std::endl;
    std::cout << "Index load factor: " << engine.get_index_load_factor() << std::endl;
    std::cout << std::endl;

    // close engine
    std::cout << "Closing engine..." << std::endl;
    engine.close();
    std::cout << "Engine closed!" << std::endl;

    std::cout << "Cleaning up SPDK environment..." << std::endl;
    SpdkEnv::instance().cleanup();

    std::cout << std::endl;
    std::cout << "=== SPSC Example completed! ===" << std::endl;
    return 0;
}
