// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <spdk/blob.h>

#include <cstdint>
#include <functional>
#include <vector>

#include "spdk_kv/ctx_pool.h"
#include "spdk_kv/entry.h"
#include "spdk_kv/mem_index.h"
#include "spdk_kv/types.h"

namespace spdk_kv {

// Recovery callback type
using RecoveryCallback = std::function<void(KvError status)>;

// Scan range for incremental recovery
struct ScanRange {
    uint16_t file_id;
    uint64_t start_page;  // start page index
    uint64_t end_page;    // End page index (exclusive)
};

// Index loader for recovery
class IndexLoader {
public:
    // Recovery state machine
    enum class State {
        kInit,
        kLoadingSuperblockPrimary,
        kLoadingSuperblockBackup,
        kLoadingMemIndexA,
        kLoadingMemIndexB,
        kComparingVersions,
        kDeserializing,
        kScanningDataFiles,
        kRebuildingIncremental,
        kFinalizingRecovery,
        kDone,
        kError
    };

    IndexLoader(MemIndex* mem_index);
    ~IndexLoader();

    // Non-copyable
    IndexLoader(const IndexLoader&) = delete;
    IndexLoader& operator=(const IndexLoader&) = delete;

    // start async recovery
    void start_recovery(RecoveryCallback callback);

    // poll recovery progress (call in main loop)
    // Returns true if recovery is in progress
    bool poll();

    // Check if recovery is complete
    bool is_complete() const { return state_ == State::kDone || state_ == State::kError; }

    // Check if recovery succeeded
    bool is_success() const { return state_ == State::kDone; }

    // get current state
    State get_state() const { return state_; }

    // get last error
    KvError get_last_error() const { return last_error_; }

    // get recovered superblock
    const Superblock& get_superblock() const { return superblock_; }

    // get recovered max sequence
    uint32_t get_recovered_max_sequence() const { return recovered_max_sequence_; }

    // set superblock directly (for pre-loaded superblock)
    void set_superblock(const Superblock& superblock) { superblock_ = superblock; }

    // set SPDK resources for real IO (blob-based recovery)
    void set_spdk_resources(spdk_blob_store* blobstore, spdk_io_channel* channel,
                          spdk_blob* superblock_blob, spdk_blob* mem_index_blob_a,
                          spdk_blob* mem_index_blob_b) {
        blobstore_ = blobstore;
        io_channel_ = channel;
        superblock_blob_ = superblock_blob;
        mem_index_blob_a_ = mem_index_blob_a;
        mem_index_blob_b_ = mem_index_blob_b;
    }

    // set data blob handles for each file (used during incremental recovery scan)
    struct DataBlobInfo {
        uint16_t file_id;
        spdk_blob* blob;
        uint64_t size;  // File size in bytes
    };
    void set_data_blobs(const std::vector<DataBlobInfo>& blobs) { data_blobs_ = blobs; }

    // Parse and rebuild entries from a buffer
    // Returns the number of entries processed
    size_t parse_and_rebuild_entries(const void* buffer, size_t buffer_size, uint16_t file_id,
                                  uint64_t base_offset);

private:
    // State machine transitions
    void transition_to(State new_state);

    // Load operations
    void load_superblock_primary();
    void load_superblock_backup();
    void load_mem_index_area(int area);
    void compare_and_select_area();
    void deserialize_mem_index(int area);
    void start_incremental_rebuild();
    void finalize_recovery();

    // Memory dump load (fast path)
    bool can_use_memory_dump_load() const;
    void load_as_memory_dump(int area);
    void rebuild_psl_array();

    // upsert load (slow path, when capacity changed)
    void load_by_upsert(int area);

    // SPDK blob-based loading
    void load_superblock_from_blob();
    void load_mem_index_area_from_blob(int area);
    void read_next_mem_index_chunk();
    void on_mem_index_chunk_loaded(int status);
    void on_mem_index_area_load_complete();
    void load_as_memory_dump_from_blob(int area);
    void load_by_upsert_from_buffer();

    // Scan data blobs for incremental recovery (real SPDK path)
    void scan_next_file_blob();
    void scan_blob_chunk();
    void on_scan_blob_chunk_loaded(int status);

    // Extracted static callbacks for SPDK async IO
    struct SuperblockReadCtx {
        IndexLoader* loader;
        void* buf;
        size_t size;
        bool is_backup;
    };
    static void on_superblock_read_complete(void* arg, int bserrno);
    static void on_superblock_backup_read_complete(void* arg, int bserrno);

    struct SegLoadCtx {
        IndexLoader* loader;
    };
    static void on_direct_segment_loaded(void* arg, int bserrno);

    struct ChunkCtx {
        IndexLoader* loader;
        void* dma_buf;
        size_t chunk_size;
    };

    struct ScanCtx {
        IndexLoader* loader;
        void* dma_buf;
        size_t read_size;
    };
    static void on_scan_chunk_read(void* arg, int bserrno);

    // Check if SPDK resources are available
    bool has_spdk_resources() const { return blobstore_ != nullptr && io_channel_ != nullptr; }

    // get physical end of a data blob (in pages)
    uint64_t get_blob_physical_end(spdk_blob* blob) const;

    // Build scan ranges from blob info
    std::vector<ScanRange> build_scan_ranges_from_blobs();

    // Validation
    bool validate_superblock(const Superblock& sb);
    bool validate_checksum(const void* data, size_t size, uint32_t expected);

    // Members
    MemIndex* mem_index_;
    State state_;
    KvError last_error_;
    RecoveryCallback callback_;

    // Superblock
    Superblock superblock_;

    // MemIndex areas
    uint64_t mem_index_versions_[2];
    int selected_area_;

    // Incremental recovery
    uint16_t checkpoint_file_id_;
    uint64_t checkpoint_page_index_;
    uint32_t recovered_max_sequence_;

    // Scan state
    std::vector<ScanRange> scan_ranges_;
    size_t current_scan_idx_;
    uint64_t current_scan_offset_;

    // Buffers
    std::vector<char> superblock_buffer_;
    std::vector<char> mem_index_buffer_;
    std::vector<char> scan_buffer_;

    // SPDK resources for real IO
    spdk_blob_store* blobstore_;
    spdk_io_channel* io_channel_;
    spdk_blob* superblock_blob_;
    spdk_blob* mem_index_blob_a_;
    spdk_blob* mem_index_blob_b_;

    // data blob handles for incremental scan
    std::vector<DataBlobInfo> data_blobs_;

    // MemIndex area loading state
    uint64_t mem_index_read_offset_;
    uint64_t mem_index_total_size_;
    int current_load_area_;

    // Segment loading state (for memory dump)
    uint32_t pending_segment_loads_;
    bool has_load_error_;

    // DMA buffer for blob IO
    void* dma_buffer_;
    size_t dma_buffer_size_;

    // Context object pools
    CtxPool<SuperblockReadCtx, 2> superblock_read_ctx_pool_;
    CtxPool<ChunkCtx, 4> chunk_ctx_pool_;
    CtxPool<SegLoadCtx, 128> seg_load_ctx_pool_;
    CtxPool<ScanCtx, 4> scan_ctx_pool_;
};

}  // namespace spdk_kv
