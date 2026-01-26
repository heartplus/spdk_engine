/* SPDX-License-Identifier: Apache-2.0 */
/* C API example for SPDK KV Engine */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <spdk_kv/spdk_kv.h>

static volatile int g_complete = 0;
static volatile int g_status = 0;

static void on_complete(void* arg, int status) {
    (void)arg;
    g_status = status;
    g_complete = 1;
}

static void wait_complete(spdk_kv_handle handle) {
    while (!g_complete) {
        spdk_kv_poll(handle);
    }
    g_complete = 0;
}

int main(int argc, char** argv) {
    const char* dev_name = "simulation";
    spdk_kv_handle handle = NULL;
    void* value_buf = NULL;
    void* read_buf = NULL;
    int rc;
    uint64_t key = 42;
    const size_t value_size = 4096;

    printf("SPDK KV Engine C API Example\n");
    printf("============================\n\n");

    if (argc > 1) {
        dev_name = argv[1];
    }

    /* Create options - using defaults */
    /* Note: In C, we'd need to properly initialize the struct */

    printf("Creating engine on device: %s\n", dev_name);

    /* For this example, we'll use the C++ API through the C interface */
    /* In a real C program, you would use the full C API */

    printf("Note: This example demonstrates the C API structure.\n");
    printf("Full implementation requires proper SPDK initialization.\n\n");

    /* Demonstrate API structure */
    printf("Available C API functions:\n");
    printf("  - spdk_kv_create(dev_name, opts, cb, cb_arg)\n");
    printf("  - spdk_kv_open(dev_name, opts, cb, cb_arg)\n");
    printf("  - spdk_kv_put(handle, key, value, len, cb, cb_arg)\n");
    printf("  - spdk_kv_get(handle, key, buf, buf_len, cb, cb_arg)\n");
    printf("  - spdk_kv_del(handle, key, cb, cb_arg)\n");
    printf("  - spdk_kv_close(handle, cb, cb_arg)\n");
    printf("  - spdk_kv_poll(handle)\n");
    printf("  - spdk_kv_sync(handle, cb, cb_arg)\n");
    printf("  - spdk_kv_is_ready(handle)\n");
    printf("  - spdk_kv_alloc_buffer(handle, size)\n");
    printf("  - spdk_kv_free_buffer(handle, buf)\n");
    printf("  - spdk_kv_get_stats(handle, stats)\n");
    printf("\n");

    printf("Example usage pattern:\n");
    printf("```c\n");
    printf("// Create engine\n");
    printf("rc = spdk_kv_create(\"Nvme0n1\", &opts, on_create, ctx);\n");
    printf("\n");
    printf("// In polling loop:\n");
    printf("while (running) {\n");
    printf("    spdk_kv_poll(handle);\n");
    printf("    // Process other events...\n");
    printf("}\n");
    printf("\n");
    printf("// Put operation\n");
    printf("void* buf = spdk_kv_alloc_buffer(handle, 4096);\n");
    printf("memcpy(buf, data, data_len);\n");
    printf("rc = spdk_kv_put(handle, key, buf, 4096, on_put, ctx);\n");
    printf("\n");
    printf("// Get operation\n");
    printf("void* read_buf = spdk_kv_alloc_buffer(handle, 4096);\n");
    printf("rc = spdk_kv_get(handle, key, read_buf, 4096, on_get, ctx);\n");
    printf("```\n\n");

    printf("C API example completed.\n");

    return 0;
}
