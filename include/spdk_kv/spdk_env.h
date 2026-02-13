// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <spdk/bdev.h>
#include <spdk/blob.h>
#include <spdk/blob_bdev.h>
#include <spdk/env.h>
#include <spdk/event.h>
#include <spdk/nvme.h>
#include <spdk/stdinc.h>
#include <spdk/thread.h>

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "spdk_kv/ctx_pool.h"
#include "spdk_kv/types.h"

namespace spdk_kv {

// SPDK initialization options
struct SpdkEnvOpts {
    std::string name = "spdk_kv";
    std::string config_file = "";
    std::string bdev_name = "";
    size_t shm_id = 0;
    size_t mem_size = 0;  // 0 = default
    int core_mask = 0;    // 0 = use first core
    bool no_pci = false;
    size_t num_hugepages = 0;
};

// SPDK environment manager
// Handles SPDK initialization, blob store creation, and resource management
class SpdkEnv {
public:
    // Singleton access
    static SpdkEnv& Instance();

    // Initialize SPDK environment
    // Returns true on success
    bool Initialize(const SpdkEnvOpts& opts);

    // Cleanup SPDK environment
    void Cleanup();

    // Check if initialized
    bool IsInitialized() const { return initialized_; }

    // Get blob store
    spdk_blob_store* GetBlobStore() { return blobstore_; }

    // Get IO channel
    spdk_io_channel* GetIoChannel() { return io_channel_; }

    // Get NVMe controller
    spdk_nvme_ctrlr* GetController() { return ctrlr_; }

    // Get NVMe namespace
    spdk_nvme_ns* GetNamespace() { return ns_; }

    // Get NVMe queue pair
    spdk_nvme_qpair* GetQpair() { return qpair_; }

    // Allocate blob
    // Callback is called with blob_id on success, SPDK_BLOBID_INVALID on failure
    void AllocateBlob(uint64_t size, std::function<void(uint64_t blob_id)> callback);

    // Open blob
    void OpenBlob(uint64_t blob_id, std::function<void(spdk_blob* blob)> callback);

    // Close blob
    void CloseBlob(spdk_blob* blob, std::function<void(int status)> callback);

    // Delete blob
    void DeleteBlob(uint64_t blob_id, std::function<void(int status)> callback);

    // Resize blob
    void ResizeBlob(spdk_blob* blob, uint64_t clusters, std::function<void(int status)> callback);

    // Sync blob metadata
    void SyncBlobMd(spdk_blob* blob, std::function<void(int status)> callback);

    // Process completions (call in polling loop)
    int ProcessCompletions(uint32_t max_completions = 32);

    // Allocate DMA memory
    void* AllocDmaBuffer(size_t size, size_t alignment = kPageSize);

    // Free DMA memory
    void FreeDmaBuffer(void* buffer);

    // Polling loop - process all pending events
    void Poll();

    // Start polling thread
    void StartPolling();

    // Stop polling thread
    void StopPolling();

    // Blob operation context types (moved from local scope for pooling)
    struct BlobAllocCtx {
        std::function<void(uint64_t)> callback;
    };
    struct BlobOpenCtx {
        std::function<void(spdk_blob*)> callback;
    };
    struct BlobCloseCtx {
        std::function<void(int)> callback;
    };
    struct BlobDeleteCtx {
        std::function<void(int)> callback;
    };
    struct BlobResizeCtx {
        std::function<void(int)> callback;
    };
    struct BlobSyncCtx {
        std::function<void(int)> callback;
    };

private:
    SpdkEnv();
    ~SpdkEnv();

    // Non-copyable
    SpdkEnv(const SpdkEnv&) = delete;
    SpdkEnv& operator=(const SpdkEnv&) = delete;

    // Internal initialization callbacks
    static void OnBdevInitComplete(void* arg, int rc);
    static void OnBlobStoreLoadComplete(void* arg, struct spdk_blob_store* bs, int bserrno);
    static void OnBlobCreateComplete(void* arg, spdk_blob_id blobid, int bserrno);
    static void OnBlobOpenComplete(void* arg, struct spdk_blob* blob, int bserrno);
    static void OnBlobCloseComplete(void* arg, int bserrno);
    static void OnBlobDeleteComplete(void* arg, int bserrno);
    static void OnBlobResizeComplete(void* arg, int bserrno);
    static void OnBlobSyncComplete(void* arg, int bserrno);

    // SPDK resources
    spdk_nvme_ctrlr* ctrlr_;
    spdk_nvme_ns* ns_;
    spdk_nvme_qpair* qpair_;
    spdk_blob_store* blobstore_;
    spdk_io_channel* io_channel_;
    spdk_bdev* bdev_;
    spdk_bdev_desc* bdev_desc_;

    bool initialized_;
    bool polling_;
    SpdkEnvOpts opts_;

    // Context object pools
    CtxPool<BlobAllocCtx, 16> blob_alloc_ctx_pool_;
    CtxPool<BlobOpenCtx, 16> blob_open_ctx_pool_;
    CtxPool<BlobCloseCtx, 16> blob_close_ctx_pool_;
    CtxPool<BlobDeleteCtx, 16> blob_delete_ctx_pool_;
    CtxPool<BlobResizeCtx, 8> blob_resize_ctx_pool_;
    CtxPool<BlobSyncCtx, 8> blob_sync_ctx_pool_;
};

// DMA memory allocator for SPDK
class DmaAllocator {
public:
    static void* Alloc(size_t size, size_t alignment = kPageSize);
    static void Free(void* ptr);
    static void* AllocZeroed(size_t size, size_t alignment = kPageSize);
};

}  // namespace spdk_kv
