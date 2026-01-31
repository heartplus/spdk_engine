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

SpdkEnv& SpdkEnv::Instance() {
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
        Cleanup();
    }
}

bool SpdkEnv::Initialize(const SpdkEnvOpts& opts) {
    if (initialized_) {
        return true;
    }

    opts_ = opts;

    // Initialize SPDK environment
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

    // Open bdev - bdev_name is required
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

    // Create blob store device from bdev
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

    spdk_bs_load(bs_dev, nullptr, OnBlobStoreLoadComplete, &load_ctx);

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

void SpdkEnv::Cleanup() {
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

void SpdkEnv::Poll() {
    // Process completions
    if (qpair_) {
        spdk_nvme_qpair_process_completions(qpair_, 0);
    }
}

void SpdkEnv::StartPolling() { polling_ = true; }

void SpdkEnv::StopPolling() { polling_ = false; }

int SpdkEnv::ProcessCompletions(uint32_t max_completions) {
    if (!qpair_) {
        return 0;
    }
    return spdk_nvme_qpair_process_completions(qpair_, max_completions);
}

void* SpdkEnv::AllocDmaBuffer(size_t size, size_t alignment) {
    return spdk_dma_zmalloc(size, alignment, nullptr);
}

void SpdkEnv::FreeDmaBuffer(void* buffer) { spdk_dma_free(buffer); }

void SpdkEnv::AllocateBlob(uint64_t size, std::function<void(uint64_t blob_id)> callback) {
    if (!blobstore_) {
        callback(SPDK_BLOBID_INVALID);
        return;
    }

    struct AllocCtx {
        std::function<void(uint64_t)> callback;
    };
    auto* ctx = new AllocCtx{std::move(callback)};

    // Calculate clusters needed
    uint64_t cluster_size = spdk_bs_get_cluster_size(blobstore_);
    uint64_t clusters = (size + cluster_size - 1) / cluster_size;

    struct spdk_blob_opts opts;
    spdk_blob_opts_init(&opts, sizeof(opts));
    opts.num_clusters = clusters;

    spdk_bs_create_blob_ext(blobstore_, &opts, OnBlobCreateComplete, ctx);
}

void SpdkEnv::OpenBlob(uint64_t blob_id, std::function<void(spdk_blob* blob)> callback) {
    if (!blobstore_) {
        callback(nullptr);
        return;
    }

    struct OpenCtx {
        std::function<void(spdk_blob*)> callback;
    };
    auto* ctx = new OpenCtx{std::move(callback)};

    spdk_bs_open_blob(blobstore_, blob_id, OnBlobOpenComplete, ctx);
}

void SpdkEnv::CloseBlob(spdk_blob* blob, std::function<void(int status)> callback) {
    if (!blob) {
        callback(-1);
        return;
    }

    struct CloseCtx {
        std::function<void(int)> callback;
    };
    auto* ctx = new CloseCtx{std::move(callback)};

    spdk_blob_close(blob, OnBlobCloseComplete, ctx);
}

void SpdkEnv::DeleteBlob(uint64_t blob_id, std::function<void(int status)> callback) {
    if (!blobstore_) {
        callback(-1);
        return;
    }

    struct DeleteCtx {
        std::function<void(int)> callback;
    };
    auto* ctx = new DeleteCtx{std::move(callback)};

    spdk_bs_delete_blob(blobstore_, blob_id, OnBlobDeleteComplete, ctx);
}

void SpdkEnv::ResizeBlob(spdk_blob* blob, uint64_t clusters,
                         std::function<void(int status)> callback) {
    if (!blob) {
        callback(-1);
        return;
    }

    struct ResizeCtx {
        std::function<void(int)> callback;
    };
    auto* ctx = new ResizeCtx{std::move(callback)};

    spdk_blob_resize(blob, clusters, OnBlobResizeComplete, ctx);
}

void SpdkEnv::SyncBlobMd(spdk_blob* blob, std::function<void(int status)> callback) {
    if (!blob) {
        callback(-1);
        return;
    }

    struct SyncCtx {
        std::function<void(int)> callback;
    };
    auto* ctx = new SyncCtx{std::move(callback)};

    spdk_blob_sync_md(blob, OnBlobSyncComplete, ctx);
}

// Static callbacks
void SpdkEnv::OnBdevInitComplete(void* arg, int rc) {
    (void)arg;
    (void)rc;
}

void SpdkEnv::OnBlobStoreLoadComplete(void* arg, struct spdk_blob_store* bs, int bserrno) {
    // BsLoadCtx is defined in Initialize() with same layout
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

void SpdkEnv::OnBlobCreateComplete(void* arg, spdk_blob_id blobid, int bserrno) {
    struct AllocCtx {
        std::function<void(uint64_t)> callback;
    };
    auto* ctx = static_cast<AllocCtx*>(arg);
    if (bserrno == 0) {
        ctx->callback(blobid);
    } else {
        ctx->callback(SPDK_BLOBID_INVALID);
    }
    delete ctx;
}

void SpdkEnv::OnBlobOpenComplete(void* arg, struct spdk_blob* blob, int bserrno) {
    struct OpenCtx {
        std::function<void(spdk_blob*)> callback;
    };
    auto* ctx = static_cast<OpenCtx*>(arg);
    if (bserrno == 0) {
        ctx->callback(blob);
    } else {
        ctx->callback(nullptr);
    }
    delete ctx;
}

void SpdkEnv::OnBlobCloseComplete(void* arg, int bserrno) {
    struct CloseCtx {
        std::function<void(int)> callback;
    };
    auto* ctx = static_cast<CloseCtx*>(arg);
    ctx->callback(bserrno);
    delete ctx;
}

void SpdkEnv::OnBlobDeleteComplete(void* arg, int bserrno) {
    struct DeleteCtx {
        std::function<void(int)> callback;
    };
    auto* ctx = static_cast<DeleteCtx*>(arg);
    ctx->callback(bserrno);
    delete ctx;
}

void SpdkEnv::OnBlobResizeComplete(void* arg, int bserrno) {
    struct ResizeCtx {
        std::function<void(int)> callback;
    };
    auto* ctx = static_cast<ResizeCtx*>(arg);
    ctx->callback(bserrno);
    delete ctx;
}

void SpdkEnv::OnBlobSyncComplete(void* arg, int bserrno) {
    struct SyncCtx {
        std::function<void(int)> callback;
    };
    auto* ctx = static_cast<SyncCtx*>(arg);
    ctx->callback(bserrno);
    delete ctx;
}

// ============================================================================
// DmaAllocator implementation
// ============================================================================

void* DmaAllocator::Alloc(size_t size, size_t alignment) {
    return spdk_dma_malloc(size, alignment, nullptr);
}

void DmaAllocator::Free(void* ptr) {
    if (!ptr) {
        return;
    }
    spdk_dma_free(ptr);
}

void* DmaAllocator::AllocZeroed(size_t size, size_t alignment) {
    return spdk_dma_zmalloc(size, alignment, nullptr);
}

}  // namespace spdk_kv
