// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

// Async API example for SPDK KV Engine
// Note: In simulation mode, callbacks are called immediately (synchronously)

#include <atomic>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

#include "spdk_kv/spdk_kv.h"

using namespace spdk_kv;

// Completion context
struct CompletionContext {
    std::atomic<int> pending{0};
    std::atomic<int> success_count{0};
    std::atomic<int> error_count{0};
};

// Put callback
void OnPutComplete(void* arg, int status) {
    auto* ctx = static_cast<CompletionContext*>(arg);
    if (status == 0) {
        ctx->success_count++;
    } else {
        ctx->error_count++;
    }
    ctx->pending--;
}

// Get callback
void OnGetComplete(void* arg, int status, uint32_t actual_len) {
    auto* ctx = static_cast<CompletionContext*>(arg);
    if (status == 0) {
        ctx->success_count++;
        // In real async mode, we would process the data here
        (void)actual_len;  // Suppress unused warning
    } else {
        ctx->error_count++;
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
    }
    ctx->pending--;
}

int main() {
    std::cout << "=== SPDK KV Engine Async Example ===" << std::endl;
    std::cout << std::endl;
    std::cout << "Note: In simulation mode, callbacks are called immediately." << std::endl;
    std::cout << "In real SPDK mode, you would call Poll() to process completions." << std::endl;
    std::cout << std::endl;

    // Create engine
    Engine engine;
    CreateOpts create_opts;
    create_opts.config.max_entries = 100000;
    create_opts.config.data_file_size = 64 * 1024 * 1024;

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

    // Async Put operations
    std::cout << "--- Async Put Operations ---" << std::endl;
    const int kNumOps = 100;
    ctx.pending = kNumOps;
    ctx.success_count = 0;
    ctx.error_count = 0;

    for (int i = 0; i < kNumOps; i++) {
        uint64_t key = i + 1;
        std::string value = "async_value_" + std::to_string(i);
        engine.PutAsync(key, value.data(), static_cast<uint32_t>(value.size()), OnPutComplete,
                        &ctx);
    }

    // In real SPDK mode, we would poll here
    // while (ctx.pending > 0) {
    //     engine.Poll();
    // }

    std::cout << "Put operations: " << ctx.success_count << " success, " << ctx.error_count
              << " errors" << std::endl;
    std::cout << std::endl;

    // Async Get operations
    std::cout << "--- Async Get Operations ---" << std::endl;
    ctx.pending = kNumOps;
    ctx.success_count = 0;
    ctx.error_count = 0;

    std::vector<std::vector<char>> buffers(kNumOps, std::vector<char>(1024));
    for (int i = 0; i < kNumOps; i++) {
        uint64_t key = i + 1;
        engine.GetAsync(key, buffers[i].data(), static_cast<uint32_t>(buffers[i].size()),
                        OnGetComplete, &ctx);
    }

    std::cout << "Get operations: " << ctx.success_count << " success, " << ctx.error_count
              << " errors" << std::endl;
    std::cout << std::endl;

    // Async Delete operations
    std::cout << "--- Async Delete Operations ---" << std::endl;
    const int kDeleteCount = 10;
    ctx.pending = kDeleteCount;
    ctx.success_count = 0;
    ctx.error_count = 0;

    for (int i = 0; i < kDeleteCount; i++) {
        uint64_t key = i + 1;
        engine.DeleteAsync(key, OnDeleteComplete, &ctx);
    }

    std::cout << "Delete operations: " << ctx.success_count << " success, " << ctx.error_count
              << " errors" << std::endl;
    std::cout << std::endl;

    // Statistics
    std::cout << "--- Statistics ---" << std::endl;
    std::cout << "Entry count: " << engine.GetEntryCount() << std::endl;
    std::cout << "Total data bytes: " << engine.GetTotalDataBytes() << std::endl;
    std::cout << std::endl;

    // Demonstrate polling pattern (simulation)
    std::cout << "--- Polling Pattern Example ---" << std::endl;
    std::cout << "In real SPDK mode, the main loop would look like:" << std::endl;
    std::cout << R"(
    while (running) {
        // Process completions
        engine.Poll();

        // Submit new requests
        if (has_pending_requests()) {
            submit_requests();
        }

        // Other processing...
    }
)" << std::endl;

    // Close
    std::cout << "Closing engine..." << std::endl;
    engine.Close();
    std::cout << "Engine closed!" << std::endl;
    std::cout << std::endl;

    std::cout << "=== Async Example completed! ===" << std::endl;
    return 0;
}
