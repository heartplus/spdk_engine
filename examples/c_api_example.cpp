// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

// C API example for SPDK KV Engine

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "spdk_kv/engine.h"

using namespace spdk_kv;

int main() {
    printf("=== SPDK KV Engine C API Example ===\n\n");

    spdk_kv_handle handle = nullptr;
    int ret;

    // Create options
    struct spdk_kv_create_opts create_opts;
    memset(&create_opts, 0, sizeof(create_opts));
    create_opts.max_capacity = 8ULL * 1024 * 1024 * 1024 * 1024;  // 8TB
    create_opts.data_file_size = 64 * 1024 * 1024;                // 64MB for testing
    create_opts.max_entries = 1000000;                            // 1M entries
    create_opts.index_load_factor = 0.55;
    create_opts.force = 0;

    // Create engine
    printf("Creating engine...\n");
    ret = spdk_kv_create("/tmp/spdk_kv_c_test", &create_opts, &handle);
    if (ret != 0) {
        fprintf(stderr, "Failed to create engine: %d\n", ret);
        return 1;
    }
    printf("Engine created!\n\n");

    // Put some values
    printf("--- Put Operations ---\n");
    for (int i = 1; i <= 10; i++) {
        char value[64];
        snprintf(value, sizeof(value), "c_value_%d", i);

        ret = spdk_kv_put(handle, static_cast<uint64_t>(i), value,
                          static_cast<uint32_t>(strlen(value)));
        if (ret != 0) {
            fprintf(stderr, "Put failed for key %d: %d\n", i, ret);
            spdk_kv_close(handle);
            return 1;
        }
        printf("Put key %d: %s\n", i, value);
    }
    printf("\n");

    // Get some values
    printf("--- Get Operations ---\n");
    for (int i = 1; i <= 10; i++) {
        char buffer[256];
        uint32_t actual_len = 0;

        ret = spdk_kv_get(handle, static_cast<uint64_t>(i), buffer, sizeof(buffer), &actual_len);
        if (ret != 0) {
            fprintf(stderr, "Get failed for key %d: %d\n", i, ret);
            spdk_kv_close(handle);
            return 1;
        }
        buffer[actual_len] = '\0';
        printf("Get key %d: %s (len=%u)\n", i, buffer, actual_len);
    }
    printf("\n");

    // Delete a key
    printf("--- Delete Operation ---\n");
    ret = spdk_kv_del(handle, 5);
    if (ret != 0) {
        fprintf(stderr, "Delete failed: %d\n", ret);
        spdk_kv_close(handle);
        return 1;
    }
    printf("Deleted key 5\n");

    // Try to get deleted key
    {
        char buffer[256];
        uint32_t actual_len = 0;
        ret = spdk_kv_get(handle, 5, buffer, sizeof(buffer), &actual_len);
        if (ret == -1) {  // KEY_NOT_FOUND
            printf("Key 5 not found (expected after delete)\n");
        } else {
            fprintf(stderr, "Key 5 should not exist!\n");
        }
    }
    printf("\n");

    // Statistics
    printf("--- Statistics ---\n");
    printf("Entry count: %lu\n", static_cast<unsigned long>(spdk_kv_get_entry_count(handle)));
    printf("Total bytes: %lu\n", static_cast<unsigned long>(spdk_kv_get_total_bytes(handle)));
    printf("\n");

    // Close engine
    printf("Closing engine...\n");
    ret = spdk_kv_close(handle);
    if (ret != 0) {
        fprintf(stderr, "Close failed: %d\n", ret);
        return 1;
    }
    printf("Engine closed!\n\n");

    printf("=== C API Example completed! ===\n");
    return 0;
}
