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
template <typename T, size_t Capacity>
class SpscQueue {
public:
    SpscQueue() : head_(0), tail_(0) {}

    // Producer: try to enqueue an item
    bool TryPush(const T& item) {
        size_t head = head_.load(std::memory_order_relaxed);
        size_t next_head = (head + 1) % Capacity;

        if (next_head == tail_.load(std::memory_order_acquire)) {
            return false;  // Queue is full
        }

        buffer_[head] = item;
        head_.store(next_head, std::memory_order_release);
        return true;
    }

    // Consumer: try to dequeue an item
    bool TryPop(T& item) {
        size_t tail = tail_.load(std::memory_order_relaxed);

        if (tail == head_.load(std::memory_order_acquire)) {
            return false;  // Queue is empty
        }

        item = buffer_[tail];
        tail_.store((tail + 1) % Capacity, std::memory_order_release);
        return true;
    }

    bool IsEmpty() const {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

    size_t Size() const {
        size_t head = head_.load(std::memory_order_acquire);
        size_t tail = tail_.load(std::memory_order_acquire);
        return (head >= tail) ? (head - tail) : (Capacity - tail + head);
    }

private:
    alignas(64) std::atomic<size_t> head_;
    alignas(64) std::atomic<size_t> tail_;
    T buffer_[Capacity];
};

// =============================================================================
// Task definitions
// =============================================================================
enum class TaskType { kPut, kGet, kDelete, kStop };

struct Task {
    TaskType type;
    uint64_t key;
    std::vector<char> value;      // For Put: value to write
    std::vector<char>* read_buf;  // For Get: buffer to read into
    uint32_t value_len;
    uint64_t task_id;
};

struct TaskResult {
    uint64_t task_id;
    TaskType type;
    int status;
    uint32_t actual_len;  // For Get operations
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
};

void OnPutComplete(void* arg, int status) {
    auto* ctx = static_cast<CompletionContext*>(arg);
    TaskResult result{ctx->task_id, ctx->type, status, 0};

    // Send result back to worker thread
    while (!g_result_queue.TryPush(result)) {
        // Result queue full, spin wait (should rarely happen)
    }

    delete ctx;
    g_completed++;
}

void OnGetComplete(void* arg, int status, uint32_t actual_len) {
    auto* ctx = static_cast<CompletionContext*>(arg);
    TaskResult result{ctx->task_id, ctx->type, status, actual_len};

    while (!g_result_queue.TryPush(result)) {
        // Result queue full, spin wait
    }

    delete ctx;
    g_completed++;
}

void OnDeleteComplete(void* arg, int status) {
    auto* ctx = static_cast<CompletionContext*>(arg);
    TaskResult result{ctx->task_id, ctx->type, status, 0};

    while (!g_result_queue.TryPush(result)) {
        // Result queue full, spin wait
    }

    delete ctx;
    g_completed++;
}

// =============================================================================
// Main thread: polling loop
// =============================================================================
void MainThreadLoop() {
    std::cout << "[Main] Polling loop started" << std::endl;

    while (g_running.load(std::memory_order_relaxed)) {
        // 1. Process tasks from task queue
        Task task;
        while (g_task_queue.TryPop(task)) {
            if (task.type == TaskType::kStop) {
                std::cout << "[Main] Received stop signal" << std::endl;
                g_running.store(false, std::memory_order_relaxed);
                break;
            }

            auto* ctx = new CompletionContext{task.task_id, task.type};

            switch (task.type) {
                case TaskType::kPut:
                    g_engine->PutAsync(task.key, task.value.data(), task.value_len, OnPutComplete,
                                       ctx);
                    break;

                case TaskType::kGet:
                    g_engine->GetAsync(task.key, task.read_buf->data(),
                                       static_cast<uint32_t>(task.read_buf->size()), OnGetComplete,
                                       ctx);
                    break;

                case TaskType::kDelete:
                    g_engine->DeleteAsync(task.key, OnDeleteComplete, ctx);
                    break;

                default:
                    delete ctx;
                    break;
            }
        }

        // 2. Poll engine for completions
        g_engine->Poll();
    }

    // Drain remaining tasks
    std::cout << "[Main] Draining remaining tasks..." << std::endl;
    while (g_submitted.load() > g_completed.load()) {
        g_engine->Poll();
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

// Find a valid key for read/delete (search backwards from current position)
uint64_t FindValidKey(const std::vector<bool>& key_written, int ops_submitted) {
    for (int j = ops_submitted; j > 0; j--) {
        if (key_written[j]) {
            return static_cast<uint64_t>(j);
        }
    }
    return 0;
}

// Create a put task
void MakePutTask(Task& task, int ops_submitted, std::vector<bool>& key_written) {
    task.type = TaskType::kPut;
    task.key = ops_submitted + 1;
    std::string val = "value_" + std::to_string(task.key) + "_data_padding_for_test";
    task.value.assign(val.begin(), val.end());
    task.value_len = static_cast<uint32_t>(val.size());
    task.read_buf = nullptr;
    key_written[task.key] = true;
}

// Create a get task, returns false if fallback to put is needed
bool MakeGetTask(Task& task, int ops_submitted, std::vector<bool>& key_written,
                 std::vector<char>* free_buf) {
    uint64_t get_key = FindValidKey(key_written, ops_submitted);
    if (get_key > 0 && free_buf) {
        task.type = TaskType::kGet;
        task.key = get_key;
        task.read_buf = free_buf;
        return true;
    }
    return false;
}

// Create a delete task, returns false if fallback to put is needed
bool MakeDeleteTask(Task& task, int ops_submitted, std::vector<bool>& key_written) {
    uint64_t del_key = FindValidKey(key_written, ops_submitted);
    if (del_key > 0) {
        task.type = TaskType::kDelete;
        task.key = del_key;
        key_written[del_key] = false;
        return true;
    }
    return false;
}

void WorkerThreadLoop(const WorkerConfig& config) {
    std::cout << "[Worker] Started (total_ops=" << config.total_ops
              << ", max_inflight=" << config.max_inflight << ")" << std::endl;

    // Allocate read buffers for Get operations
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
        while (g_result_queue.TryPop(result)) {
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
                    // Release buffer (simplified - in real code track which buffer was used)
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
                MakePutTask(task, ops_submitted, key_written);
            } else if (type_rand < config.put_ratio + config.get_ratio) {
                if (!MakeGetTask(task, ops_submitted, key_written, get_free_buffer())) {
                    MakePutTask(task, ops_submitted, key_written);
                }
            } else {
                if (!MakeDeleteTask(task, ops_submitted, key_written)) {
                    MakePutTask(task, ops_submitted, key_written);
                }
            }

            // Try to submit task
            if (g_task_queue.TryPush(task)) {
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
    while (!g_task_queue.TryPush(stop_task)) {
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
    std::cout << "  Put: " << put_success << " success, " << put_fail << " fail" << std::endl;
    std::cout << "  Get: " << get_success << " success, " << get_fail << " fail" << std::endl;
    std::cout << "  Delete: " << del_success << " success, " << del_fail << " fail" << std::endl;
}

// =============================================================================
// Usage
// =============================================================================
void PrintUsage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --bdev <name>      SPDK bdev name (e.g., Malloc0, Nvme0n1)" << std::endl;
    std::cout << "  --num-ops <n>      Total number of operations (default: 10000)" << std::endl;
    std::cout << "  --max-inflight <n> Max in-flight operations (default: 64)" << std::endl;
    std::cout << "  --put-ratio <n>    Put operation ratio % (default: 70)" << std::endl;
    std::cout << "  --get-ratio <n>    Get operation ratio % (default: 25)" << std::endl;
    std::cout << "  --del-ratio <n>    Delete operation ratio % (default: 5)" << std::endl;
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
            PrintUsage(argv[0]);
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

    auto& env = SpdkEnv::Instance();
    if (env.Initialize(spdk_opts)) {
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
    std::cout << "  Put ratio:        " << config.put_ratio << "%" << std::endl;
    std::cout << "  Get ratio:        " << config.get_ratio << "%" << std::endl;
    std::cout << "  Delete ratio:     " << config.del_ratio << "%" << std::endl;
    std::cout << std::endl;

    // Create engine
    Engine engine;
    CreateOpts create_opts;
    create_opts.config.max_entries = static_cast<uint64_t>(std::max(100000, config.total_ops * 2));
    create_opts.config.data_file_size = 256 * 1024 * 1024;  // 256MB

    std::cout << "Creating engine..." << std::endl;
    KvError err = engine.Create("/tmp/spdk_kv_spsc", create_opts);
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

    // Start worker thread
    std::cout << "Starting threads..." << std::endl;
    std::thread worker_thread(WorkerThreadLoop, config);

    // Main thread runs polling loop
    MainThreadLoop();

    // Wait for worker to finish
    worker_thread.join();

    std::cout << std::endl;
    std::cout << "--- Final Statistics ---" << std::endl;
    std::cout << "Entry count: " << engine.GetEntryCount() << std::endl;
    std::cout << "Total data bytes: " << engine.GetTotalDataBytes() << std::endl;
    std::cout << "Index load factor: " << engine.GetIndexLoadFactor() << std::endl;
    std::cout << std::endl;

    // Close engine
    std::cout << "Closing engine..." << std::endl;
    engine.Close();
    std::cout << "Engine closed!" << std::endl;

    std::cout << "Cleaning up SPDK environment..." << std::endl;
    SpdkEnv::Instance().Cleanup();

    std::cout << std::endl;
    std::cout << "=== SPSC Example completed! ===" << std::endl;
    return 0;
}
