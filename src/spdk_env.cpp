// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#include <spdk_kv/spdk_kv.h>
#include "engine.h"

#ifdef WITH_SPDK
#include <spdk/env.h>
#include <spdk/bdev.h>
#include <spdk/blob.h>
#include <spdk/blob_bdev.h>
#include <spdk/log.h>
#include <spdk/thread.h>
#include <spdk/event.h>
#endif

namespace spdk_kv {

#ifdef WITH_SPDK

// SPDK blob store operations
struct BlobStoreContext {
    Engine* engine;
    spdk_kv_cb callback;
    void* cb_arg;
    struct spdk_blob_store* bs;
    struct spdk_bs_dev* bs_dev;
    int status;
};

// Callback for blob store load complete
static void bs_load_complete(void* cb_arg, struct spdk_blob_store* bs, int bserrno) {
    auto* ctx = static_cast<BlobStoreContext*>(cb_arg);

    if (bserrno != 0) {
        SPDK_ERRLOG("Failed to load blob store: %d\n", bserrno);
        ctx->status = bserrno;
        if (ctx->callback) {
            ctx->callback(ctx->cb_arg, bserrno);
        }
        delete ctx;
        return;
    }

    ctx->bs = bs;
    // Continue initialization...

    if (ctx->callback) {
        ctx->callback(ctx->cb_arg, 0);
    }
    delete ctx;
}

// Callback for blob store init complete
static void bs_init_complete(void* cb_arg, struct spdk_blob_store* bs, int bserrno) {
    auto* ctx = static_cast<BlobStoreContext*>(cb_arg);

    if (bserrno != 0) {
        SPDK_ERRLOG("Failed to init blob store: %d\n", bserrno);
        ctx->status = bserrno;
        if (ctx->callback) {
            ctx->callback(ctx->cb_arg, bserrno);
        }
        delete ctx;
        return;
    }

    ctx->bs = bs;
    // Continue with superblock creation...

    if (ctx->callback) {
        ctx->callback(ctx->cb_arg, 0);
    }
    delete ctx;
}

// Initialize SPDK blob store for existing device
int init_blob_store_load(const char* bdev_name, spdk_kv_cb cb, void* cb_arg) {
    auto* ctx = new BlobStoreContext;
    ctx->callback = cb;
    ctx->cb_arg = cb_arg;
    ctx->status = 0;

    // Create bs_dev from bdev
    int rc = spdk_bdev_create_bs_dev_ext(bdev_name, nullptr, nullptr, &ctx->bs_dev);
    if (rc != 0) {
        delete ctx;
        return rc;
    }

    // Load blob store
    spdk_bs_load(ctx->bs_dev, nullptr, bs_load_complete, ctx);
    return 0;
}

// Initialize SPDK blob store for new device
int init_blob_store_create(const char* bdev_name, spdk_kv_cb cb, void* cb_arg) {
    auto* ctx = new BlobStoreContext;
    ctx->callback = cb;
    ctx->cb_arg = cb_arg;
    ctx->status = 0;

    // Create bs_dev from bdev
    int rc = spdk_bdev_create_bs_dev_ext(bdev_name, nullptr, nullptr, &ctx->bs_dev);
    if (rc != 0) {
        delete ctx;
        return rc;
    }

    // Initialize blob store
    struct spdk_bs_opts opts;
    spdk_bs_opts_init(&opts, sizeof(opts));

    spdk_bs_init(ctx->bs_dev, &opts, bs_init_complete, ctx);
    return 0;
}

// Unload blob store
void unload_blob_store(struct spdk_blob_store* bs, spdk_kv_cb cb, void* cb_arg) {
    spdk_bs_unload(bs, [](void* arg, int bserrno) {
        auto* cb_ctx = static_cast<std::pair<spdk_kv_cb, void*>*>(arg);
        if (cb_ctx->first) {
            cb_ctx->first(cb_ctx->second, bserrno);
        }
        delete cb_ctx;
    }, new std::pair<spdk_kv_cb, void*>(cb, cb_arg));
}

#endif  // WITH_SPDK

}  // namespace spdk_kv

// C API implementations
extern "C" {

int spdk_kv_create(const char* dev_name,
                   spdk_kv::CreateOpts* opts,
                   spdk_kv::spdk_kv_cb cb,
                   void* cb_arg) {
    auto* engine = new spdk_kv::Engine();
    int rc = engine->create(dev_name, opts, cb, cb_arg);
    if (rc != 0) {
        delete engine;
        return rc;
    }
    return 0;
}

int spdk_kv_open(const char* dev_name,
                 spdk_kv::OpenOpts* opts,
                 spdk_kv::spdk_kv_cb cb,
                 void* cb_arg) {
    auto* engine = new spdk_kv::Engine();
    int rc = engine->open(dev_name, opts, cb, cb_arg);
    if (rc != 0) {
        delete engine;
        return rc;
    }
    return 0;
}

int spdk_kv_put(spdk_kv::spdk_kv_handle handle,
                uint64_t key,
                void* value,
                uint32_t value_len,
                spdk_kv::spdk_kv_cb cb,
                void* cb_arg) {
    if (!handle) return static_cast<int>(spdk_kv::KvError::INVALID_ARGUMENT);
    return handle->put(key, value, value_len, cb, cb_arg);
}

int spdk_kv_get(spdk_kv::spdk_kv_handle handle,
                uint64_t key,
                void* value_buf,
                uint32_t buf_len,
                spdk_kv::spdk_kv_cb cb,
                void* cb_arg) {
    if (!handle) return static_cast<int>(spdk_kv::KvError::INVALID_ARGUMENT);
    return handle->get(key, value_buf, buf_len, cb, cb_arg);
}

int spdk_kv_del(spdk_kv::spdk_kv_handle handle,
                uint64_t key,
                spdk_kv::spdk_kv_cb cb,
                void* cb_arg) {
    if (!handle) return static_cast<int>(spdk_kv::KvError::INVALID_ARGUMENT);
    return handle->del(key, cb, cb_arg);
}

int spdk_kv_close(spdk_kv::spdk_kv_handle handle,
                  spdk_kv::spdk_kv_cb cb,
                  void* cb_arg) {
    if (!handle) return static_cast<int>(spdk_kv::KvError::INVALID_ARGUMENT);
    int rc = handle->close(cb, cb_arg);
    if (rc == 0) {
        delete handle;
    }
    return rc;
}

int spdk_kv_poll(spdk_kv::spdk_kv_handle handle) {
    if (!handle) return 0;
    return handle->poll();
}

int spdk_kv_sync(spdk_kv::spdk_kv_handle handle,
                 spdk_kv::spdk_kv_cb cb,
                 void* cb_arg) {
    if (!handle) return static_cast<int>(spdk_kv::KvError::INVALID_ARGUMENT);
    return handle->sync(cb, cb_arg);
}

int spdk_kv_get_stats(spdk_kv::spdk_kv_handle handle, spdk_kv_stats* stats) {
    if (!handle || !stats) return static_cast<int>(spdk_kv::KvError::INVALID_ARGUMENT);

    stats->total_puts = handle->total_puts();
    stats->total_gets = handle->total_gets();
    stats->total_dels = handle->total_dels();
    // TODO: Fill in other stats

    return 0;
}

bool spdk_kv_is_ready(spdk_kv::spdk_kv_handle handle) {
    if (!handle) return false;
    return handle->is_ready();
}

void* spdk_kv_alloc_buffer(spdk_kv::spdk_kv_handle handle, size_t size) {
    if (!handle) return nullptr;
    return handle->alloc_buffer(size);
}

void spdk_kv_free_buffer(spdk_kv::spdk_kv_handle handle, void* buf) {
    if (handle) {
        handle->free_buffer(buf);
    }
}

}  // extern "C"
