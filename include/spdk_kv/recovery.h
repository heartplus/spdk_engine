// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <spdk/blob.h>

#include <cstdint>
#include <functional>
#include <vector>

#include "spdk_kv/entry.h"
#include "spdk_kv/mem_index.h"
#include "spdk_kv/types.h"

namespace spdk_kv {

// Recovery callback type
using RecoveryCallback = std::function<void(KvError status)>;

// Scan range for incremental recovery
struct ScanRange {
    uint16_t file_id;
    uint64_t start_page;  // Start page index
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

    // Start async recovery
    void StartRecovery(RecoveryCallback callback);

    // Poll recovery progress (call in main loop)
    // Returns true if recovery is in progress
    bool Poll();

    // Check if recovery is complete
    bool IsComplete() const { return state_ == State::kDone || state_ == State::kError; }

    // Check if recovery succeeded
    bool IsSuccess() const { return state_ == State::kDone; }

    // Get current state
    State GetState() const { return state_; }

    // Get last error
    KvError GetLastError() const { return last_error_; }

    // Get recovered superblock
    const Superblock& GetSuperblock() const { return superblock_; }

    // Get recovered max sequence
    uint32_t GetRecoveredMaxSequence() const { return recovered_max_sequence_; }

    // Set superblock directly (for pre-loaded superblock)
    void SetSuperblock(const Superblock& superblock) { superblock_ = superblock; }

    // Set SPDK resources for real IO (blob-based recovery)
    void SetSpdkResources(spdk_blob_store* blobstore, spdk_io_channel* channel,
                          spdk_blob* superblock_blob, spdk_blob* mem_index_blob_a,
                          spdk_blob* mem_index_blob_b) {
        blobstore_ = blobstore;
        io_channel_ = channel;
        superblock_blob_ = superblock_blob;
        mem_index_blob_a_ = mem_index_blob_a;
        mem_index_blob_b_ = mem_index_blob_b;
    }

    // Set data blob handles for each file (used during incremental recovery scan)
    struct DataBlobInfo {
        uint16_t file_id;
        spdk_blob* blob;
        uint64_t size;  // File size in bytes
    };
    void SetDataBlobs(const std::vector<DataBlobInfo>& blobs) { data_blobs_ = blobs; }

    // Parse and rebuild entries from a buffer
    // Returns the number of entries processed
    size_t ParseAndRebuildEntries(const void* buffer, size_t buffer_size, uint16_t file_id,
                                  uint64_t base_offset);

private:
    // State machine transitions
    void TransitionTo(State new_state);

    // Load operations
    void LoadSuperblockPrimary();
    void LoadSuperblockBackup();
    void LoadMemIndexArea(int area);
    void CompareAndSelectArea();
    void DeserializeMemIndex(int area);
    void StartIncrementalRebuild();
    void FinalizeRecovery();

    // Memory dump load (fast path)
    bool CanUseMemoryDumpLoad() const;
    void LoadAsMemoryDump(int area);
    void RebuildPslArray();

    // Upsert load (slow path, when capacity changed)
    void LoadByUpsert(int area);

    // SPDK blob-based loading
    void LoadSuperblockFromBlob();
    void LoadMemIndexAreaFromBlob(int area);
    void ReadNextMemIndexChunk();
    void OnMemIndexChunkLoaded(int status);
    void OnMemIndexAreaLoadComplete();
    void LoadAsMemoryDumpFromBlob(int area);
    void LoadByUpsertFromBuffer();

    // Scan data blobs for incremental recovery (real SPDK path)
    void ScanNextFileBlob();
    void ScanBlobChunk();
    void OnScanBlobChunkLoaded(int status);

    // Check if SPDK resources are available
    bool HasSpdkResources() const { return blobstore_ != nullptr && io_channel_ != nullptr; }

    // Get physical end of a data blob (in pages)
    uint64_t GetBlobPhysicalEnd(spdk_blob* blob) const;

    // Build scan ranges from blob info
    std::vector<ScanRange> BuildScanRangesFromBlobs();

    // Validation
    bool ValidateSuperblock(const Superblock& sb);
    bool ValidateChecksum(const void* data, size_t size, uint32_t expected);

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

    // Data blob handles for incremental scan
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
};

}  // namespace spdk_kv
