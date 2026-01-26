// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

// Async API example for SPDK KV Engine
// Supports both simulation mode and real SPDK mode

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "spdk_kv/spdk_kv.h"

using namespace spdk_kv;

// Completion context
struct CompletionContext {
    std::atomic<int> pending{0};
    std::atomic<int> success_count{0};
    std::atomic<int> error_count{0};

    void Reset(int num_pending) {
        pending = num_pending;
        success_count = 0;
        error_count = 0;
    }

    void WaitForCompletion(Engine& engine, int timeout_ms = 10000) {
        auto start = std::chrono::steady_clock::now();
        while (pending > 0) {
            engine.Poll();

            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start);
            if (elapsed.count() > timeout_ms) {
                std::cerr << "Timeout waiting for completions! Remaining: " << pending.load()
                          << std::endl;
                break;
            }
        }
    }
};

// Put callback
void OnPutComplete(void* arg, int status) {
    auto* ctx = static_cast<CompletionContext*>(arg);
    if (status == 0) {
        ctx->success_count++;
    } else {
        ctx->error_count++;
        std::cerr << "Put failed with status: " << status << std::endl;
    }
    ctx->pending--;
}

// Get callback
void OnGetComplete(void* arg, int status, uint32_t actual_len) {
    auto* ctx = static_cast<CompletionContext*>(arg);
    if (status == 0) {
        ctx->success_count++;
        (void)actual_len;
    } else {
        ctx->error_count++;
        std::cerr << "Get failed with status: " << status << std::endl;
    }
    ctx->pending--;
}

// Delete callback
void OnDeleteComplete(void* arg, int status) {
    auto* ctx = static_cast<CompletionContext*>(arg);
    if (status == 0) {
        ctx->success_count++;
    } else {
        ctx->error_count++;
        std::cerr << "Delete failed with status: " << status << std::endl;
    }
    ctx->pending--;
}

void PrintUsage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --bdev <name>    SPDK bdev name (e.g., Malloc0, Nvme0n1)" << std::endl;
    std::cout << "  --simulation     Force simulation mode (default if no bdev)" << std::endl;
    std::cout << "  --num-ops <n>    Number of operations (default: 100)" << std::endl;
    std::cout << "  --help           Show this help" << std::endl;
}

int main(int argc, char** argv) {
    std::string bdev_name;
    [[maybe_unused]] bool force_simulation = false;
    int num_ops = 100;

    // Parse arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--bdev" && i + 1 < argc) {
            bdev_name = argv[++i];
        } else if (arg == "--simulation") {
            force_simulation = true;
        } else if (arg == "--num-ops" && i + 1 < argc) {
            num_ops = std::stoi(argv[++i]);
        } else if (arg == "--help") {
            PrintUsage(argv[0]);
            return 0;
        }
    }

    std::cout << "=== SPDK KV Engine Async Example ===" << std::endl;
    std::cout << std::endl;

    bool spdk_mode = false;

#ifdef WITH_SPDK
    // Initialize SPDK environment if bdev is specified
    if (!bdev_name.empty() && !force_simulation) {
        std::cout << "Initializing SPDK environment..." << std::endl;

        SpdkEnvOpts spdk_opts;
        spdk_opts.name = "async_example";
        spdk_opts.bdev_name = bdev_name;

        auto& env = SpdkEnv::Instance();
        if (env.Initialize(spdk_opts)) {
            std::cout << "SPDK initialized successfully with bdev: " << bdev_name << std::endl;
            spdk_mode = true;
        } else {
            std::cerr << "Failed to initialize SPDK. Falling back to simulation mode." << std::endl;
        }
    }
#endif

    if (!spdk_mode) {
        std::cout << "Running in SIMULATION mode" << std::endl;
        std::cout << "(Use --bdev <name> to run with real SPDK)" << std::endl;
    } else {
        std::cout << "Running in SPDK mode" << std::endl;
    }
    std::cout << std::endl;

    // Create engine
    Engine engine;
    CreateOpts create_opts;
    create_opts.config.max_entries = 100000;
    create_opts.config.data_file_size = 64 * 1024 * 1024;  // 64MB

    std::cout << "Creating engine..." << std::endl;
    KvError err = engine.Create("/tmp/spdk_kv_async", create_opts);
    if (err != KvError::kSuccess) {
        std::cerr << "Failed to create engine: " << static_cast<int>(err) << std::endl;
        return 1;
    }
    std::cout << "Engine created!" << std::endl;
    std::cout << std::endl;

    // Completion context
    CompletionContext ctx;

    // =========================================================================
    // Async Put Operations
    // =========================================================================
    std::cout << "--- Async Put Operations (" << num_ops << " ops) ---" << std::endl;
    auto start_time = std::chrono::steady_clock::now();

    ctx.Reset(num_ops);
    for (int i = 0; i < num_ops; i++) {
        uint64_t key = i + 1;
        std::string value = "async_value_" + std::to_string(i) + "_padding_data_to_make_it_longer";
        engine.PutAsync(key, value.data(), static_cast<uint32_t>(value.size()), OnPutComplete,
                        &ctx);
    }

    // Wait for all completions
    ctx.WaitForCompletion(engine);

    auto end_time = std::chrono::steady_clock::now();
    auto duration_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    std::cout << "Put results: " << ctx.success_count << " success, " << ctx.error_count
              << " errors" << std::endl;
    if (duration_us > 0) {
        std::cout << "Put throughput: " << (num_ops * 1000000.0 / duration_us) << " ops/sec"
                  << std::endl;
    }
    std::cout << std::endl;

    // =========================================================================
    // Async Get Operations
    // =========================================================================
    std::cout << "--- Async Get Operations (" << num_ops << " ops) ---" << std::endl;
    start_time = std::chrono::steady_clock::now();

    ctx.Reset(num_ops);
    std::vector<std::vector<char>> get_buffers(num_ops, std::vector<char>(1024));

    for (int i = 0; i < num_ops; i++) {
        uint64_t key = i + 1;
        engine.GetAsync(key, get_buffers[i].data(), static_cast<uint32_t>(get_buffers[i].size()),
                        OnGetComplete, &ctx);
    }

    // Wait for all completions
    ctx.WaitForCompletion(engine);

    end_time = std::chrono::steady_clock::now();
    duration_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    std::cout << "Get results: " << ctx.success_count << " success, " << ctx.error_count
              << " errors" << std::endl;
    if (duration_us > 0) {
        std::cout << "Get throughput: " << (num_ops * 1000000.0 / duration_us) << " ops/sec"
                  << std::endl;
    }
    std::cout << std::endl;

    // Verify some values
    std::cout << "Verifying values..." << std::endl;
    int verify_count = std::min(5, num_ops);
    for (int i = 0; i < verify_count; i++) {
        std::string expected = "async_value_" + std::to_string(i) + "_padding_data_to_make_it_longer";
        std::string actual(get_buffers[i].data(), expected.size());
        if (actual == expected) {
            std::cout << "  Key " << (i + 1) << ": OK" << std::endl;
        } else {
            std::cout << "  Key " << (i + 1) << ": MISMATCH (expected: " << expected
                      << ", got: " << actual << ")" << std::endl;
        }
    }
    std::cout << std::endl;

    // =========================================================================
    // Async Delete Operations
    // =========================================================================
    int delete_count = std::min(10, num_ops);
    std::cout << "--- Async Delete Operations (" << delete_count << " ops) ---" << std::endl;
    start_time = std::chrono::steady_clock::now();

    ctx.Reset(delete_count);
    for (int i = 0; i < delete_count; i++) {
        uint64_t key = i + 1;
        engine.DeleteAsync(key, OnDeleteComplete, &ctx);
    }

    // Wait for all completions
    ctx.WaitForCompletion(engine);

    end_time = std::chrono::steady_clock::now();
    duration_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    std::cout << "Delete results: " << ctx.success_count << " success, " << ctx.error_count
              << " errors" << std::endl;
    std::cout << std::endl;

    // =========================================================================
    // Verify deletions
    // =========================================================================
    std::cout << "--- Verifying Deletions ---" << std::endl;
    for (int i = 0; i < delete_count; i++) {
        uint64_t key = i + 1;
        char buf[1024];
        uint32_t actual_len;
        KvError result = engine.Get(key, buf, sizeof(buf), &actual_len);
        if (result == KvError::kKeyNotFound) {
            std::cout << "  Key " << key << ": correctly deleted" << std::endl;
        } else {
            std::cout << "  Key " << key << ": still exists (error!)" << std::endl;
        }
    }
    std::cout << std::endl;

    // =========================================================================
    // Mixed workload example
    // =========================================================================
    std::cout << "--- Mixed Workload Example ---" << std::endl;
    int mixed_ops = num_ops / 2;
    ctx.Reset(mixed_ops * 2);  // half puts, half gets

    start_time = std::chrono::steady_clock::now();

    // Submit interleaved puts and gets
    for (int i = 0; i < mixed_ops; i++) {
        uint64_t put_key = num_ops + i + 1;
        uint64_t get_key = delete_count + i + 1;  // Get existing keys

        std::string value = "mixed_value_" + std::to_string(i);
        engine.PutAsync(put_key, value.data(), static_cast<uint32_t>(value.size()), OnPutComplete,
                        &ctx);
        engine.GetAsync(get_key, get_buffers[i].data(),
                        static_cast<uint32_t>(get_buffers[i].size()), OnGetComplete, &ctx);
    }

    // Wait for all completions
    ctx.WaitForCompletion(engine);

    end_time = std::chrono::steady_clock::now();
    duration_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

    std::cout << "Mixed workload: " << ctx.success_count << " success, " << ctx.error_count
              << " errors" << std::endl;
    if (duration_us > 0) {
        std::cout << "Mixed throughput: " << (mixed_ops * 2 * 1000000.0 / duration_us) << " ops/sec"
                  << std::endl;
    }
    std::cout << std::endl;

    // =========================================================================
    // Statistics
    // =========================================================================
    std::cout << "--- Final Statistics ---" << std::endl;
    std::cout << "Entry count: " << engine.GetEntryCount() << std::endl;
    std::cout << "Total data bytes: " << engine.GetTotalDataBytes() << std::endl;
    std::cout << "Index load factor: " << engine.GetIndexLoadFactor() << std::endl;
    std::cout << std::endl;

    // =========================================================================
    // Close engine
    // =========================================================================
    std::cout << "Closing engine..." << std::endl;
    engine.Close();
    std::cout << "Engine closed!" << std::endl;

#ifdef WITH_SPDK
    // Cleanup SPDK environment
    if (spdk_mode) {
        std::cout << "Cleaning up SPDK environment..." << std::endl;
        SpdkEnv::Instance().Cleanup();
    }
#endif

    std::cout << std::endl;
    std::cout << "=== Async Example completed! ===" << std::endl;
    return 0;
}
