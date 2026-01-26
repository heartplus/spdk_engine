// SPDX-License-Identifier: Apache-2.0
// Basic usage example for SPDK KV Engine

// Include internal headers for direct Engine access
#include "../src/engine.h"
#include <cstdio>
#include <cstring>
#include <cstdlib>

using namespace spdk_kv;

// Global state for async operations
static bool operation_complete = false;
static int operation_status = 0;

// Callback for async operations
static void on_complete(void* /* arg */, int status) {
    operation_status = status;
    operation_complete = true;
    printf("Operation completed with status: %d\n", status);
}

// Wait for operation to complete
static void wait_complete(spdk_kv_handle handle) {
    while (!operation_complete) {
        spdk_kv_poll(handle);
    }
    operation_complete = false;
}

int main(int argc, char** argv) {
    printf("SPDK KV Engine Basic Example\n");
    printf("============================\n\n");

    // Device name (use "simulation" for non-SPDK mode)
    const char* dev_name = "simulation";
    if (argc > 1) {
        dev_name = argv[1];
    }

    // Create options
    CreateOpts create_opts;
    create_opts.config.max_entries = 1000000;  // 1M entries for testing
    create_opts.config.data_file_size = 1ULL * 1024 * 1024 * 1024;  // 1GB

    // Create engine
    printf("Creating engine on device: %s\n", dev_name);
    spdk_kv_handle handle = nullptr;

    // For simulation mode, we'll create and use an Engine directly
    auto* engine = new Engine();
    int rc = engine->create(dev_name, &create_opts, on_complete, nullptr);
    if (rc != 0) {
        printf("Failed to create engine: %d\n", rc);
        delete engine;
        return 1;
    }

    // Wait for creation to complete
    while (!engine->is_ready() && !operation_complete) {
        engine->poll();
    }

    if (operation_status != 0) {
        printf("Engine creation failed: %d\n", operation_status);
        delete engine;
        return 1;
    }

    handle = engine;
    printf("Engine created successfully!\n\n");

    // Allocate aligned buffer for value
    const size_t value_size = 4096;  // 4KB
    void* value_buf = spdk_kv_alloc_buffer(handle, value_size);
    if (!value_buf) {
        printf("Failed to allocate buffer\n");
        delete engine;
        return 1;
    }

    // Test PUT operation
    printf("Testing PUT operation...\n");
    uint64_t key = 12345;
    memset(value_buf, 'A', value_size);
    strcpy(static_cast<char*>(value_buf), "Hello, SPDK KV Engine!");

    operation_complete = false;
    rc = spdk_kv_put(handle, key, value_buf, value_size, on_complete, nullptr);
    if (rc != 0) {
        printf("PUT failed immediately: %d\n", rc);
    } else {
        wait_complete(handle);
        if (operation_status == 0) {
            printf("PUT successful: key=%lu, value_len=%zu\n", key, value_size);
        } else {
            printf("PUT failed: %d\n", operation_status);
        }
    }

    // Test GET operation
    printf("\nTesting GET operation...\n");
    void* read_buf = spdk_kv_alloc_buffer(handle, value_size);
    if (!read_buf) {
        printf("Failed to allocate read buffer\n");
        spdk_kv_free_buffer(handle, value_buf);
        delete engine;
        return 1;
    }
    memset(read_buf, 0, value_size);

    operation_complete = false;
    rc = spdk_kv_get(handle, key, read_buf, value_size, on_complete, nullptr);
    if (rc != 0) {
        printf("GET failed immediately: %d\n", rc);
    } else {
        wait_complete(handle);
        if (operation_status == 0) {
            printf("GET successful: key=%lu, value=\"%s\"\n",
                   key, static_cast<char*>(read_buf));
        } else {
            printf("GET failed: %d\n", operation_status);
        }
    }

    // Test multiple PUT operations
    printf("\nTesting multiple PUT operations...\n");
    for (uint64_t i = 0; i < 10; i++) {
        snprintf(static_cast<char*>(value_buf), value_size,
                 "Value for key %lu", i);

        operation_complete = false;
        rc = spdk_kv_put(handle, i, value_buf, value_size, on_complete, nullptr);
        if (rc == 0) {
            wait_complete(handle);
        }
    }
    printf("Inserted 10 key-value pairs\n");

    // Test DELETE operation
    printf("\nTesting DELETE operation...\n");
    operation_complete = false;
    rc = spdk_kv_del(handle, key, on_complete, nullptr);
    if (rc != 0) {
        printf("DELETE failed immediately: %d\n", rc);
    } else {
        wait_complete(handle);
        if (operation_status == 0) {
            printf("DELETE successful: key=%lu\n", key);
        } else {
            printf("DELETE failed: %d\n", operation_status);
        }
    }

    // Verify key is deleted
    printf("\nVerifying key is deleted...\n");
    operation_complete = false;
    rc = spdk_kv_get(handle, key, read_buf, value_size, on_complete, nullptr);
    if (rc == static_cast<int>(KvError::KEY_NOT_FOUND)) {
        printf("Key correctly not found after deletion\n");
    } else if (rc != 0) {
        printf("GET returned error: %d (expected KEY_NOT_FOUND)\n", rc);
    }

    // Get statistics
    printf("\nEngine statistics:\n");
    spdk_kv_stats stats;
    if (spdk_kv_get_stats(handle, &stats) == 0) {
        printf("  Total PUTs: %lu\n", stats.total_puts);
        printf("  Total GETs: %lu\n", stats.total_gets);
        printf("  Total DELs: %lu\n", stats.total_dels);
    }

    // Clean up
    printf("\nClosing engine...\n");
    spdk_kv_free_buffer(handle, value_buf);
    spdk_kv_free_buffer(handle, read_buf);

    operation_complete = false;
    rc = spdk_kv_close(handle, on_complete, nullptr);
    if (rc == 0) {
        // For simulation mode, poll until complete
        while (!operation_complete) {
            // In real SPDK, we'd call spdk_thread_poll
        }
    }

    printf("Engine closed successfully!\n");
    printf("\nExample completed.\n");

    return 0;
}
