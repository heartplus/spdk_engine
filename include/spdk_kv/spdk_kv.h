// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#pragma once

#include "types.h"
#include "errors.h"
#include "config.h"

#ifdef __cplusplus
extern "C" {
#endif

// Create a new engine instance
// @param dev_name: NVMe device name (e.g., "Nvme0n1")
// @param opts: Creation options
// @param cb: Completion callback
// @param cb_arg: Callback argument
// @return 0 on success, negative error code on failure
int spdk_kv_create(const char* dev_name,
                   spdk_kv::CreateOpts* opts,
                   spdk_kv::spdk_kv_cb cb,
                   void* cb_arg);

// Open an existing engine
// @param dev_name: NVMe device name
// @param opts: Open options
// @param cb: Completion callback (receives engine handle via cb_arg)
// @param cb_arg: Callback argument
// @return 0 on success, negative error code on failure
int spdk_kv_open(const char* dev_name,
                 spdk_kv::OpenOpts* opts,
                 spdk_kv::spdk_kv_cb cb,
                 void* cb_arg);

// Put a key-value pair
// @param handle: Engine handle
// @param key: Key (uint64_t)
// @param value: Value buffer (must be aligned to 4KB)
// @param value_len: Value length (must be > 0)
// @param cb: Completion callback
// @param cb_arg: Callback argument
// @return 0 on success, negative error code on failure
int spdk_kv_put(spdk_kv::spdk_kv_handle handle,
                uint64_t key,
                void* value,
                uint32_t value_len,
                spdk_kv::spdk_kv_cb cb,
                void* cb_arg);

// Get a value by key
// @param handle: Engine handle
// @param key: Key (uint64_t)
// @param value_buf: Buffer to store value (must be aligned to 4KB)
// @param buf_len: Buffer length
// @param cb: Completion callback
// @param cb_arg: Callback argument
// @return 0 on success, negative error code on failure
int spdk_kv_get(spdk_kv::spdk_kv_handle handle,
                uint64_t key,
                void* value_buf,
                uint32_t buf_len,
                spdk_kv::spdk_kv_cb cb,
                void* cb_arg);

// Delete a key
// @param handle: Engine handle
// @param key: Key (uint64_t)
// @param cb: Completion callback
// @param cb_arg: Callback argument
// @return 0 on success, negative error code on failure
int spdk_kv_del(spdk_kv::spdk_kv_handle handle,
                uint64_t key,
                spdk_kv::spdk_kv_cb cb,
                void* cb_arg);

// Close the engine
// @param handle: Engine handle
// @param cb: Completion callback
// @param cb_arg: Callback argument
// @return 0 on success, negative error code on failure
int spdk_kv_close(spdk_kv::spdk_kv_handle handle,
                  spdk_kv::spdk_kv_cb cb,
                  void* cb_arg);

// Poll for completions (must be called in main loop)
// @param handle: Engine handle
// @return Number of completions processed
int spdk_kv_poll(spdk_kv::spdk_kv_handle handle);

// Sync all pending writes
// @param handle: Engine handle
// @param cb: Completion callback
// @param cb_arg: Callback argument
// @return 0 on success, negative error code on failure
int spdk_kv_sync(spdk_kv::spdk_kv_handle handle,
                 spdk_kv::spdk_kv_cb cb,
                 void* cb_arg);

// Get engine statistics
struct spdk_kv_stats {
    uint64_t total_entries;
    uint64_t total_data_bytes;
    uint64_t total_garbage_bytes;
    uint64_t total_puts;
    uint64_t total_gets;
    uint64_t total_dels;
    uint64_t pending_writes;
    double garbage_ratio;
};

int spdk_kv_get_stats(spdk_kv::spdk_kv_handle handle, spdk_kv_stats* stats);

// Check if engine is ready
bool spdk_kv_is_ready(spdk_kv::spdk_kv_handle handle);

// Allocate DMA-safe buffer
void* spdk_kv_alloc_buffer(spdk_kv::spdk_kv_handle handle, size_t size);

// Free DMA-safe buffer
void spdk_kv_free_buffer(spdk_kv::spdk_kv_handle handle, void* buf);

#ifdef __cplusplus
}
#endif

// C++ API wrapper
#ifdef __cplusplus

namespace spdk_kv {

class KvEngine {
public:
    KvEngine() : handle_(nullptr) {}
    ~KvEngine() {
        if (handle_) {
            // Note: should call close before destructor
        }
    }

    // Disable copy
    KvEngine(const KvEngine&) = delete;
    KvEngine& operator=(const KvEngine&) = delete;

    // Move operations
    KvEngine(KvEngine&& other) noexcept : handle_(other.handle_) {
        other.handle_ = nullptr;
    }
    KvEngine& operator=(KvEngine&& other) noexcept {
        if (this != &other) {
            handle_ = other.handle_;
            other.handle_ = nullptr;
        }
        return *this;
    }

    // Static factory methods
    static int create(const char* dev_name, CreateOpts* opts,
                      spdk_kv_cb cb, void* cb_arg) {
        return spdk_kv_create(dev_name, opts, cb, cb_arg);
    }

    static int open(const char* dev_name, OpenOpts* opts,
                    spdk_kv_cb cb, void* cb_arg) {
        return spdk_kv_open(dev_name, opts, cb, cb_arg);
    }

    // Instance methods
    int put(uint64_t key, void* value, uint32_t len,
            spdk_kv_cb cb, void* cb_arg) {
        return spdk_kv_put(handle_, key, value, len, cb, cb_arg);
    }

    int get(uint64_t key, void* value_buf, uint32_t buf_len,
            spdk_kv_cb cb, void* cb_arg) {
        return spdk_kv_get(handle_, key, value_buf, buf_len, cb, cb_arg);
    }

    int del(uint64_t key, spdk_kv_cb cb, void* cb_arg) {
        return spdk_kv_del(handle_, key, cb, cb_arg);
    }

    int close(spdk_kv_cb cb, void* cb_arg) {
        int ret = spdk_kv_close(handle_, cb, cb_arg);
        if (ret == 0) {
            handle_ = nullptr;
        }
        return ret;
    }

    int poll() {
        return spdk_kv_poll(handle_);
    }

    int sync(spdk_kv_cb cb, void* cb_arg) {
        return spdk_kv_sync(handle_, cb, cb_arg);
    }

    bool is_ready() const {
        return spdk_kv_is_ready(handle_);
    }

    void* alloc_buffer(size_t size) {
        return spdk_kv_alloc_buffer(handle_, size);
    }

    void free_buffer(void* buf) {
        spdk_kv_free_buffer(handle_, buf);
    }

    void set_handle(spdk_kv_handle handle) { handle_ = handle; }
    spdk_kv_handle get_handle() const { return handle_; }

private:
    spdk_kv_handle handle_;
};

}  // namespace spdk_kv

#endif  // __cplusplus
