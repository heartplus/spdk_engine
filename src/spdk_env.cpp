// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#include "spdk_kv/spdk_env.h"

#include <cstdlib>
#include <cstring>
#include <stdexcept>

namespace spdk_kv {

// ============================================================================
// SpdkEnv implementation
// ============================================================================

SpdkEnv& SpdkEnv::instance() {
    static SpdkEnv instance;
    return instance;
}

SpdkEnv::SpdkEnv()
        : ctrlr_(nullptr),
          ns_(nullptr),
          qpair_(nullptr),
          blobstore_(nullptr),
          io_channel_(nullptr),
          bdev_(nullptr),
          bdev_desc_(nullptr),
          initialized_(false),
          polling_(false) {}

SpdkEnv::~SpdkEnv() {
    if (initialized_) {
        cleanup();
    }
}

bool SpdkEnv::initialize(const SpdkEnvOpts& opts) {
    if (initialized_) {
        return true;
    }

    opts_ = opts;

    // initialize SPDK environment
    struct spdk_env_opts spdk_opts;
    spdk_env_opts_init(&spdk_opts);

    spdk_opts.name = opts.name.c_str();
    if (opts.shm_id > 0) {
        spdk_opts.shm_id = static_cast<int>(opts.shm_id);
    }
    if (opts.mem_size > 0) {
        spdk_opts.mem_size = static_cast<int>(opts.mem_size);
    }
    if (opts.no_pci) {
        spdk_opts.no_pci = true;
    }

    if (spdk_env_init(&spdk_opts) < 0) {
        return false;
    }

    // open bdev - bdev_name is required
    if (opts.bdev_name.empty()) {
        spdk_env_fini();
        return false;
    }

    bdev_ = spdk_bdev_get_by_name(opts.bdev_name.c_str());
    if (!bdev_) {
        spdk_env_fini();
        return false;
    }

    int rc = spdk_bdev_open_ext(opts.bdev_name.c_str(), true, nullptr, nullptr, &bdev_desc_);
    if (rc < 0) {
        spdk_env_fini();
        return false;
    }

    // create blob store device from bdev
    struct spdk_bs_dev* bs_dev = nullptr;
    rc = spdk_bdev_create_bs_dev_ext(opts.bdev_name.c_str(), nullptr, nullptr, &bs_dev);
    if (rc < 0 || !bs_dev) {
        spdk_bdev_close(bdev_desc_);
        spdk_env_fini();
        return false;
    }

    // Load blob store
    // This is done asynchronously, we use a synchronization mechanism here
    struct BsLoadCtx {
        spdk_blob_store* bs;
        int rc;
        bool done;
    };
    BsLoadCtx load_ctx = {nullptr, 0, false};

    spdk_bs_load(bs_dev, nullptr, on_blob_store_load_complete, &load_ctx);

    // Wait for completion (in real usage, this would be in the event loop)
    while (!load_ctx.done) {
        // Process events
    }

    if (load_ctx.rc != 0 || !load_ctx.bs) {
        spdk_bdev_close(bdev_desc_);
        spdk_env_fini();
        return false;
    }

    blobstore_ = load_ctx.bs;
    io_channel_ = spdk_bs_alloc_io_channel(blobstore_);

    initialized_ = true;
    return true;
}

void SpdkEnv::cleanup() {
    if (!initialized_) {
        return;
    }

    if (io_channel_) {
        spdk_bs_free_io_channel(io_channel_);
        io_channel_ = nullptr;
    }

    if (blobstore_) {
        // Unload blob store
        struct BsUnloadCtx {
            bool done;
        };
        BsUnloadCtx unload_ctx = {false};

        spdk_bs_unload(
                blobstore_,
                [](void* arg, int bserrno) {
                    auto* ctx = static_cast<BsUnloadCtx*>(arg);
                    (void)bserrno;
                    ctx->done = true;
                },
                &unload_ctx);

        while (!unload_ctx.done) {
            // Process events
        }

        blobstore_ = nullptr;
    }

    if (bdev_desc_) {
        spdk_bdev_close(bdev_desc_);
        bdev_desc_ = nullptr;
    }

    if (qpair_) {
        spdk_nvme_ctrlr_free_io_qpair(qpair_);
        qpair_ = nullptr;
    }

    spdk_env_fini();

    initialized_ = false;
}

void SpdkEnv::poll() {
    // Process completions
    if (qpair_) {
        spdk_nvme_qpair_process_completions(qpair_, 0);
    }
}

void SpdkEnv::start_polling() { polling_ = true; }

void SpdkEnv::stop_polling() { polling_ = false; }

int SpdkEnv::process_completions(uint32_t max_completions) {
    if (!qpair_) {
        return 0;
    }
    return spdk_nvme_qpair_process_completions(qpair_, max_completions);
}

void* SpdkEnv::alloc_dma_buffer(size_t size, size_t alignment) {
    return spdk_dma_zmalloc(size, alignment, nullptr);
}

void SpdkEnv::free_dma_buffer(void* buffer) { spdk_dma_free(buffer); }

void SpdkEnv::allocate_blob(uint64_t size, std::function<void(uint64_t blob_id)> callback) {
    if (!blobstore_) {
        callback(SPDK_BLOBID_INVALID);
        return;
    }

    auto* ctx = blob_alloc_ctx_pool_.alloc(std::move(callback));

    // calculate clusters needed
    uint64_t cluster_size = spdk_bs_get_cluster_size(blobstore_);
    uint64_t clusters = (size + cluster_size - 1) / cluster_size;

    struct spdk_blob_opts opts;
    spdk_blob_opts_init(&opts, sizeof(opts));
    opts.num_clusters = clusters;

    spdk_bs_create_blob_ext(blobstore_, &opts, on_blob_create_complete, ctx);
}

void SpdkEnv::open_blob(uint64_t blob_id, std::function<void(spdk_blob* blob)> callback) {
    if (!blobstore_) {
        callback(nullptr);
        return;
    }

    auto* ctx = blob_open_ctx_pool_.alloc(std::move(callback));

    spdk_bs_open_blob(blobstore_, blob_id, on_blob_open_complete, ctx);
}

void SpdkEnv::close_blob(spdk_blob* blob, std::function<void(int status)> callback) {
    if (!blob) {
        callback(-1);
        return;
    }

    auto* ctx = blob_close_ctx_pool_.alloc(std::move(callback));

    spdk_blob_close(blob, on_blob_close_complete, ctx);
}

void SpdkEnv::delete_blob(uint64_t blob_id, std::function<void(int status)> callback) {
    if (!blobstore_) {
        callback(-1);
        return;
    }

    auto* ctx = blob_delete_ctx_pool_.alloc(std::move(callback));

    spdk_bs_delete_blob(blobstore_, blob_id, on_blob_delete_complete, ctx);
}

void SpdkEnv::resize_blob(spdk_blob* blob, uint64_t clusters,
                         std::function<void(int status)> callback) {
    if (!blob) {
        callback(-1);
        return;
    }

    auto* ctx = blob_resize_ctx_pool_.alloc(std::move(callback));

    spdk_blob_resize(blob, clusters, on_blob_resize_complete, ctx);
}

void SpdkEnv::sync_blob_md(spdk_blob* blob, std::function<void(int status)> callback) {
    if (!blob) {
        callback(-1);
        return;
    }

    auto* ctx = blob_sync_ctx_pool_.alloc(std::move(callback));

    spdk_blob_sync_md(blob, on_blob_sync_complete, ctx);
}

// Static callbacks
void SpdkEnv::on_bdev_init_complete(void* arg, int rc) {
    (void)arg;
    (void)rc;
}

void SpdkEnv::on_blob_store_load_complete(void* arg, struct spdk_blob_store* bs, int bserrno) {
    // BsLoadCtx is defined in initialize() with same layout
    struct BsLoadCtx {
        spdk_blob_store* bs;
        int rc;
        bool done;
    };
    auto* ctx = static_cast<BsLoadCtx*>(arg);
    ctx->bs = bs;
    ctx->rc = bserrno;
    ctx->done = true;
}

void SpdkEnv::on_blob_create_complete(void* arg, spdk_blob_id blobid, int bserrno) {
    auto* ctx = static_cast<BlobAllocCtx*>(arg);
    auto cb = std::move(ctx->callback);
    instance().blob_alloc_ctx_pool_.free(ctx);
    if (bserrno == 0) {
        cb(blobid);
    } else {
        cb(SPDK_BLOBID_INVALID);
    }
}

void SpdkEnv::on_blob_open_complete(void* arg, struct spdk_blob* blob, int bserrno) {
    auto* ctx = static_cast<BlobOpenCtx*>(arg);
    auto cb = std::move(ctx->callback);
    instance().blob_open_ctx_pool_.free(ctx);
    if (bserrno == 0) {
        cb(blob);
    } else {
        cb(nullptr);
    }
}

void SpdkEnv::on_blob_close_complete(void* arg, int bserrno) {
    auto* ctx = static_cast<BlobCloseCtx*>(arg);
    auto cb = std::move(ctx->callback);
    instance().blob_close_ctx_pool_.free(ctx);
    cb(bserrno);
}

void SpdkEnv::on_blob_delete_complete(void* arg, int bserrno) {
    auto* ctx = static_cast<BlobDeleteCtx*>(arg);
    auto cb = std::move(ctx->callback);
    instance().blob_delete_ctx_pool_.free(ctx);
    cb(bserrno);
}

void SpdkEnv::on_blob_resize_complete(void* arg, int bserrno) {
    auto* ctx = static_cast<BlobResizeCtx*>(arg);
    auto cb = std::move(ctx->callback);
    instance().blob_resize_ctx_pool_.free(ctx);
    cb(bserrno);
}

void SpdkEnv::on_blob_sync_complete(void* arg, int bserrno) {
    auto* ctx = static_cast<BlobSyncCtx*>(arg);
    auto cb = std::move(ctx->callback);
    instance().blob_sync_ctx_pool_.free(ctx);
    cb(bserrno);
}

// ============================================================================
// DmaAllocator implementation
// ============================================================================

void* DmaAllocator::alloc(size_t size, size_t alignment) {
    return spdk_dma_malloc(size, alignment, nullptr);
}

void DmaAllocator::free(void* ptr) {
    if (!ptr) {
        return;
    }
    spdk_dma_free(ptr);
}

void* DmaAllocator::alloc_zeroed(size_t size, size_t alignment) {
    return spdk_dma_zmalloc(size, alignment, nullptr);
}

}  // namespace spdk_kv
