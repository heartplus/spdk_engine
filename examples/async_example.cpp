// SPDX-License-Identifier: Apache-2.0
// Async operations example for SPDK KV Engine

// Include internal headers for direct Engine access
#include "../src/engine.h"
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <atomic>
#include <vector>

using namespace spdk_kv;

// Counters for async operations
static std::atomic<uint64_t> completed_puts{0};
static std::atomic<uint64_t> completed_gets{0};
static std::atomic<uint64_t> failed_ops{0};

// Callback contexts
struct PutContext {
    uint64_t key;
    void* value;
    size_t len;
};

struct GetContext {
    uint64_t key;
    void* buffer;
    size_t buf_len;
};

// Callbacks
static void on_put_complete(void* arg, int status) {
    auto* ctx = static_cast<PutContext*>(arg);
    if (status == 0) {
        completed_puts++;
    } else {
        failed_ops++;
        printf("PUT failed for key %lu: %d\n", ctx->key, status);
    }
    delete ctx;
}

static void on_get_complete(void* arg, int status) {
    auto* ctx = static_cast<GetContext*>(arg);
    if (status == 0) {
        completed_gets++;
    } else {
        failed_ops++;
    }
    delete ctx;
}

int main(int argc, char** argv) {
    printf("SPDK KV Engine Async Example\n");
    printf("============================\n\n");

    const char* dev_name = "simulation";
    if (argc > 1) {
        dev_name = argv[1];
    }

    // Configuration
    const uint64_t num_keys = 1000;
    const size_t value_size = 4096;

    // Create engine
    CreateOpts create_opts;
    create_opts.config.max_entries = num_keys * 2;
    create_opts.config.data_file_size = 1ULL * 1024 * 1024 * 1024;

    auto* engine = new Engine();

    int create_status = 0;
    int rc = engine->create(dev_name, &create_opts,
        [](void* arg, int status) {
            *static_cast<int*>(arg) = status;
        }, &create_status);

    if (rc != 0) {
        printf("Failed to start engine creation: %d\n", rc);
        delete engine;
        return 1;
    }

    // Wait for engine to be ready
    while (!engine->is_ready()) {
        engine->poll();
    }

    printf("Engine ready!\n\n");

    // Allocate value buffers
    std::vector<void*> values(num_keys);
    for (uint64_t i = 0; i < num_keys; i++) {
        values[i] = engine->alloc_buffer(value_size);
        if (!values[i]) {
            printf("Failed to allocate buffer %lu\n", i);
            // Clean up
            for (uint64_t j = 0; j < i; j++) {
                engine->free_buffer(values[j]);
            }
            delete engine;
            return 1;
        }
        snprintf(static_cast<char*>(values[i]), value_size,
                 "Value for key %lu - async example", i);
    }

    printf("Submitting %lu async PUT operations...\n", num_keys);

    // Submit all PUTs
    for (uint64_t i = 0; i < num_keys; i++) {
        auto* ctx = new PutContext{i, values[i], value_size};
        rc = engine->put(i, values[i], value_size, on_put_complete, ctx);

        if (rc != 0) {
            printf("PUT submission failed for key %lu: %d\n", i, rc);
            failed_ops++;
            delete ctx;
        }

        // Poll periodically to process completions
        if (i % 100 == 0) {
            engine->poll();
        }
    }

    // Poll until all PUTs complete
    printf("Waiting for PUT completions...\n");
    while (completed_puts.load() + failed_ops.load() < num_keys) {
        engine->poll();
    }

    printf("Completed %lu PUTs, %lu failed\n\n",
           completed_puts.load(), failed_ops.load());

    // Reset counters for GETs
    uint64_t expected_gets = completed_puts.load();
    failed_ops = 0;

    // Allocate read buffers
    std::vector<void*> read_buffers(num_keys);
    for (uint64_t i = 0; i < num_keys; i++) {
        read_buffers[i] = engine->alloc_buffer(value_size);
        if (!read_buffers[i]) {
            // Clean up on failure
            for (uint64_t j = 0; j < i; j++) {
                engine->free_buffer(read_buffers[j]);
            }
            // Also clean up value buffers
            for (auto* v : values) {
                engine->free_buffer(v);
            }
            delete engine;
            return 1;
        }
    }

    printf("Submitting %lu async GET operations...\n", expected_gets);

    // Submit all GETs
    for (uint64_t i = 0; i < expected_gets; i++) {
        auto* ctx = new GetContext{i, read_buffers[i], value_size};
        rc = engine->get(i, read_buffers[i], value_size, on_get_complete, ctx);

        if (rc != 0) {
            if (rc != static_cast<int>(KvError::KEY_NOT_FOUND)) {
                printf("GET submission failed for key %lu: %d\n", i, rc);
            }
            failed_ops++;
            delete ctx;
        }

        if (i % 100 == 0) {
            engine->poll();
        }
    }

    // Poll until all GETs complete
    printf("Waiting for GET completions...\n");
    while (completed_gets.load() + failed_ops.load() < expected_gets) {
        engine->poll();
    }

    printf("Completed %lu GETs, %lu failed\n\n",
           completed_gets.load(), failed_ops.load());

    // Verify some values
    printf("Verifying sample values...\n");
    bool all_match = true;
    for (uint64_t i = 0; i < std::min(num_keys, 5UL); i++) {
        char expected[256];
        snprintf(expected, sizeof(expected),
                 "Value for key %lu - async example", i);

        if (strncmp(static_cast<char*>(read_buffers[i]), expected, strlen(expected)) != 0) {
            printf("Value mismatch for key %lu\n", i);
            all_match = false;
        }
    }
    if (all_match) {
        printf("All sample values verified correctly!\n");
    }

    // Clean up
    printf("\nCleaning up...\n");
    for (auto* v : values) {
        engine->free_buffer(v);
    }
    for (auto* buf : read_buffers) {
        engine->free_buffer(buf);
    }

    bool close_complete = false;
    engine->close([](void* arg, int /* status */) {
        *static_cast<bool*>(arg) = true;
    }, &close_complete);

    while (!close_complete) {
        // Wait
    }

    printf("Example completed successfully!\n");

    return 0;
}
