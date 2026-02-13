// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#include "spdk_kv/recovery.h"

#include <algorithm>
#include <cstring>

#include "spdk_kv/crc32.h"
#include "spdk_kv/spdk_env.h"

namespace spdk_kv {

IndexLoader::IndexLoader(MemIndex* mem_index)
        : mem_index_(mem_index),
          state_(State::kInit),
          last_error_(KvError::kSuccess),
          selected_area_(-1),
          checkpoint_file_id_(0),
          checkpoint_page_index_(0),
          recovered_max_sequence_(0),
          current_scan_idx_(0),
          current_scan_offset_(0),
          blobstore_(nullptr),
          io_channel_(nullptr),
          superblock_blob_(nullptr),
          mem_index_blob_a_(nullptr),
          mem_index_blob_b_(nullptr),
          mem_index_read_offset_(0),
          mem_index_total_size_(0),
          current_load_area_(0),
          pending_segment_loads_(0),
          has_load_error_(false),
          dma_buffer_(nullptr),
          dma_buffer_size_(0) {
    mem_index_versions_[0] = 0;
    mem_index_versions_[1] = 0;
    std::memset(&superblock_, 0, sizeof(superblock_));

    // Allocate buffers
    superblock_buffer_.resize(sizeof(Superblock));
    mem_index_buffer_.resize(64 * 1024 * 1024);  // 64MB
    scan_buffer_.resize(1024 * 1024);             // 1MB
}

IndexLoader::~IndexLoader() {
    if (dma_buffer_) {
        DmaAllocator::Free(dma_buffer_);
        dma_buffer_ = nullptr;
    }
}

void IndexLoader::StartRecovery(RecoveryCallback callback) {
    if (state_ != State::kInit) {
        callback(KvError::kInvalidState);
        return;
    }

    callback_ = std::move(callback);
    TransitionTo(State::kLoadingSuperblockPrimary);
    LoadSuperblockPrimary();
}

bool IndexLoader::Poll() {
    if (IsComplete()) {
        return false;
    }

    // In real SPDK mode, poll for IO completions
    if (HasSpdkResources()) {
        SpdkEnv::Instance().Poll();
    }

    return !IsComplete();
}

void IndexLoader::TransitionTo(State new_state) { state_ = new_state; }

// =========================================================================
// Superblock Loading
// =========================================================================

void IndexLoader::LoadSuperblockPrimary() {
    if (HasSpdkResources() && superblock_blob_) {
        // Async read from blob
        LoadSuperblockFromBlob();
    } else {
        // Superblock already set via SetSuperblock()
        if (ValidateSuperblock(superblock_)) {
            TransitionTo(State::kLoadingMemIndexA);
            LoadMemIndexArea(0);
        } else {
            TransitionTo(State::kLoadingSuperblockBackup);
            LoadSuperblockBackup();
        }
    }
}

void IndexLoader::LoadSuperblockFromBlob() {
    // Allocate DMA buffer for superblock read
    size_t sb_aligned_size = AlignUp(sizeof(Superblock), kPageSize);
    void* read_buf = DmaAllocator::AllocZeroed(sb_aligned_size, kPageSize);
    if (!read_buf) {
        last_error_ = KvError::kInternalError;
        TransitionTo(State::kError);
        if (callback_) {
            callback_(last_error_);
        }
        return;
    }

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(blobstore_);
    uint64_t length_units = sb_aligned_size / io_unit_size;

    auto* ctx = superblock_read_ctx_pool_.Alloc(this, read_buf, sb_aligned_size, false);

    spdk_blob_io_read(superblock_blob_, io_channel_, read_buf,
                      kSuperblockPrimaryOffset / io_unit_size, length_units,
                      OnSuperblockReadComplete, ctx);
}

void IndexLoader::LoadSuperblockBackup() {
    // Fallback: validate pre-loaded superblock
    if (ValidateSuperblock(superblock_)) {
        TransitionTo(State::kLoadingMemIndexA);
        LoadMemIndexArea(0);
    } else {
        last_error_ = KvError::kCorruption;
        TransitionTo(State::kError);
        if (callback_) {
            callback_(last_error_);
        }
    }
}

// =========================================================================
// MemIndex Area Loading
// =========================================================================

void IndexLoader::LoadMemIndexArea(int area) {
    if (HasSpdkResources()) {
        // Async read from blob
        LoadMemIndexAreaFromBlob(area);
    } else {
        // No SPDK resources: use superblock metadata
        if (area == 0) {
            mem_index_versions_[0] = superblock_.checkpoint_sequence;
        } else {
            mem_index_versions_[1] = 0;  // Assume B is older
        }

        if (area == 0) {
            TransitionTo(State::kLoadingMemIndexB);
            LoadMemIndexArea(1);
        } else {
            TransitionTo(State::kComparingVersions);
            CompareAndSelectArea();
        }
    }
}

void IndexLoader::LoadMemIndexAreaFromBlob(int area) {
    spdk_blob* blob = (area == 0) ? mem_index_blob_a_ : mem_index_blob_b_;
    if (!blob) {
        // No blob for this area, mark as invalid
        mem_index_versions_[area] = 0;
        if (area == 0) {
            TransitionTo(State::kLoadingMemIndexB);
            LoadMemIndexArea(1);
        } else {
            TransitionTo(State::kComparingVersions);
            CompareAndSelectArea();
        }
        return;
    }

    // Read the header first (64 bytes, but aligned to page)
    mem_index_total_size_ = superblock_.mem_index_size;
    if (mem_index_total_size_ == 0) {
        mem_index_total_size_ = kMemIndexSegmentSize * kMemIndexSegmentCount;
    }

    // Ensure buffer is large enough for the header at minimum
    if (mem_index_buffer_.size() < sizeof(SerializedMemIndexHeader)) {
        mem_index_buffer_.resize(sizeof(SerializedMemIndexHeader) + kPageSize);
    }

    current_load_area_ = area;
    mem_index_read_offset_ = 0;

    // Start chunked reading (2MB per chunk)
    ReadNextMemIndexChunk();
}

void IndexLoader::ReadNextMemIndexChunk() {
    uint64_t remaining = mem_index_total_size_ - mem_index_read_offset_;
    size_t chunk_size = std::min(remaining, static_cast<uint64_t>(2 * 1024 * 1024));

    if (chunk_size == 0) {
        OnMemIndexAreaLoadComplete();
        return;
    }

    // Ensure buffer is large enough
    if (mem_index_buffer_.size() < mem_index_read_offset_ + chunk_size) {
        mem_index_buffer_.resize(mem_index_read_offset_ + chunk_size);
    }

    // Allocate DMA buffer for this chunk
    size_t aligned_chunk = AlignUp(chunk_size, kPageSize);
    void* dma_buf = DmaAllocator::AllocZeroed(aligned_chunk, kPageSize);
    if (!dma_buf) {
        // Fall back: mark area as invalid
        mem_index_versions_[current_load_area_] = 0;
        OnMemIndexAreaLoadComplete();
        return;
    }

    spdk_blob* blob =
            (current_load_area_ == 0) ? mem_index_blob_a_ : mem_index_blob_b_;

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(blobstore_);
    uint64_t offset_units = mem_index_read_offset_ / io_unit_size;
    uint64_t length_units = aligned_chunk / io_unit_size;

    auto* ctx = chunk_ctx_pool_.Alloc(this, dma_buf, chunk_size);

    spdk_blob_io_read(
            blob, io_channel_, dma_buf, offset_units, length_units,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<ChunkCtx*>(arg);
                auto* self = ctx->loader;

                if (bserrno == 0) {
                    // Copy from DMA buffer to mem_index_buffer_
                    std::memcpy(self->mem_index_buffer_.data() + self->mem_index_read_offset_,
                                ctx->dma_buf, ctx->chunk_size);
                }

                DmaAllocator::Free(ctx->dma_buf);
                self->chunk_ctx_pool_.Free(ctx);

                self->OnMemIndexChunkLoaded(bserrno);
            },
            ctx);
}

void IndexLoader::OnMemIndexChunkLoaded(int status) {
    if (status != 0) {
        // Read error, mark area as invalid
        mem_index_versions_[current_load_area_] = 0;
        OnMemIndexAreaLoadComplete();
        return;
    }

    mem_index_read_offset_ += 2 * 1024 * 1024;
    if (mem_index_read_offset_ > mem_index_total_size_) {
        mem_index_read_offset_ = mem_index_total_size_;
    }
    ReadNextMemIndexChunk();
}

void IndexLoader::OnMemIndexAreaLoadComplete() {
    // Validate header
    if (mem_index_buffer_.size() >= sizeof(SerializedMemIndexHeader)) {
        auto* header =
                reinterpret_cast<SerializedMemIndexHeader*>(mem_index_buffer_.data());

        if (header->magic != kMemIndexMagic) {
            mem_index_versions_[current_load_area_] = 0;
        } else {
            mem_index_versions_[current_load_area_] = header->global_sequence;

            // Validate checksum
            size_t entry_data_size =
                    header->entry_count * sizeof(MemIndexEntry);
            if (mem_index_buffer_.size() >=
                sizeof(SerializedMemIndexHeader) + entry_data_size) {
                uint32_t computed = Crc32::Calculate(
                        mem_index_buffer_.data() + sizeof(SerializedMemIndexHeader),
                        entry_data_size);
                if (computed != header->checksum) {
                    mem_index_versions_[current_load_area_] = 0;
                }
            } else {
                mem_index_versions_[current_load_area_] = 0;
            }
        }
    } else {
        mem_index_versions_[current_load_area_] = 0;
    }

    if (current_load_area_ == 0) {
        TransitionTo(State::kLoadingMemIndexB);
        LoadMemIndexArea(1);
    } else {
        TransitionTo(State::kComparingVersions);
        CompareAndSelectArea();
    }
}

// =========================================================================
// Compare and Deserialize
// =========================================================================

void IndexLoader::CompareAndSelectArea() {
    // Select area with higher version
    selected_area_ = (mem_index_versions_[0] >= mem_index_versions_[1]) ? 0 : 1;

    if (mem_index_versions_[selected_area_] == 0) {
        // No valid MemIndex area, need full rebuild from data files
        TransitionTo(State::kScanningDataFiles);
        StartIncrementalRebuild();
    } else {
        TransitionTo(State::kDeserializing);
        DeserializeMemIndex(selected_area_);
    }
}

void IndexLoader::DeserializeMemIndex(int area) {
    if (CanUseMemoryDumpLoad()) {
        if (HasSpdkResources()) {
            LoadAsMemoryDumpFromBlob(area);
            return;  // Async path continues in callbacks
        } else {
            LoadAsMemoryDump(area);
        }
    } else {
        if (HasSpdkResources()) {
            LoadByUpsertFromBuffer();
        } else {
            LoadByUpsert(area);
        }
    }

    // Continue to incremental rebuild
    TransitionTo(State::kRebuildingIncremental);
    StartIncrementalRebuild();
}

bool IndexLoader::CanUseMemoryDumpLoad() const {
    if (!mem_index_ || mem_index_buffer_.size() < sizeof(SerializedMemIndexHeader)) {
        return false;
    }

    auto* header =
            reinterpret_cast<const SerializedMemIndexHeader*>(mem_index_buffer_.data());

    // Can use memory dump if capacity matches and version is compatible
    // Memory dump requires that the hash table layout is identical
    return header->capacity == mem_index_->Capacity() && header->version == 1;
}

void IndexLoader::LoadAsMemoryDump(int area) {
    (void)area;
    // No SPDK resources: fall back to upsert
    LoadByUpsert(area);
}

void IndexLoader::LoadAsMemoryDumpFromBlob(int area) {
    // Direct memory dump load: read raw entry data from blob directly into
    // MemIndex entries array. This is the fastest recovery path.

    auto* header =
            reinterpret_cast<SerializedMemIndexHeader*>(mem_index_buffer_.data());

    spdk_blob* blob = (area == 0) ? mem_index_blob_a_ : mem_index_blob_b_;
    if (!blob) {
        // Fall back to upsert
        LoadByUpsertFromBuffer();
        TransitionTo(State::kRebuildingIncremental);
        StartIncrementalRebuild();
        return;
    }

    // Read entry data directly into MemIndex entries array
    // The serialized format stores entries after the header, segment by segment
    size_t entries_size = header->capacity * sizeof(MemIndexEntry);
    size_t segment_size = entries_size / kMemIndexSegmentCount;

    pending_segment_loads_ = kMemIndexSegmentCount;
    has_load_error_ = false;
    current_load_area_ = area;

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(blobstore_);

    for (uint32_t seg = 0; seg < kMemIndexSegmentCount; seg++) {
        // Calculate source offset in blob (after header, segment-by-segment)
        uint64_t blob_offset = sizeof(SerializedMemIndexHeader) + seg * segment_size;
        uint64_t offset_units = blob_offset / io_unit_size;
        uint64_t length_units = segment_size / io_unit_size;

        // Target: directly into MemIndex entries array
        void* target_addr =
                reinterpret_cast<char*>(mem_index_->Entries()) + seg * segment_size;

        auto* ctx = seg_load_ctx_pool_.Alloc(this);

        spdk_blob_io_read(blob, io_channel_, target_addr, offset_units, length_units,
                          OnDirectSegmentLoaded, ctx);
    }
}

void IndexLoader::RebuildPslArray() {
    if (!mem_index_) {
        return;
    }
    mem_index_->RebuildPslArray();
}

void IndexLoader::LoadByUpsert(int area) {
    (void)area;
    // No SPDK resources: mem_index is already populated externally
    // With SPDK resources, use LoadByUpsertFromBuffer() instead
}

void IndexLoader::LoadByUpsertFromBuffer() {
    // Upsert load from mem_index_buffer_: iterate serialized entries and insert via upsert.
    // This path is used when capacity has changed (can't use memory dump).
    // Uses batch prefetch for performance optimization.

    if (mem_index_buffer_.size() < sizeof(SerializedMemIndexHeader)) {
        return;
    }

    auto* header =
            reinterpret_cast<const SerializedMemIndexHeader*>(mem_index_buffer_.data());
    if (header->magic != kMemIndexMagic) {
        return;
    }

    const auto* entries = reinterpret_cast<const MemIndexEntry*>(
            mem_index_buffer_.data() + sizeof(SerializedMemIndexHeader));
    uint64_t count = header->entry_count;

    // Batch upsert with prefetch optimization
    constexpr size_t kBatchSize = 64;
    uint64_t capacity = mem_index_->Capacity();

    for (uint64_t i = 0; i < count; i += kBatchSize) {
        // Prefetch next batch
        for (size_t j = 0; j < kBatchSize && i + j + kBatchSize < count; j++) {
            uint64_t key = entries[i + j + kBatchSize].key;
            uint64_t hash = HashUtil::Hash(key);
            uint64_t idx = hash & (capacity - 1);
            __builtin_prefetch(&mem_index_->Entries()[idx], 1, 3);
        }

        // Insert current batch
        for (size_t j = 0; j < kBatchSize && i + j < count; j++) {
            const MemIndexEntry& src = entries[i + j];
            if (!src.is_deleted()) {
                mem_index_->Upsert(src.key, src);
            }
        }
    }

    // Restore global sequence
    mem_index_->SetGlobalSequence(header->global_sequence);
}

// =========================================================================
// Incremental Recovery (Data File Scanning)
// =========================================================================

void IndexLoader::StartIncrementalRebuild() {
    // Get checkpoint info from superblock
    checkpoint_file_id_ = superblock_.checkpoint_file_id;
    checkpoint_page_index_ = superblock_.checkpoint_page_index;
    recovered_max_sequence_ = superblock_.checkpoint_global_seq;

    // Build scan ranges from blob info
    if (!data_blobs_.empty()) {
        scan_ranges_ = BuildScanRangesFromBlobs();
    }

    if (scan_ranges_.empty()) {
        TransitionTo(State::kFinalizingRecovery);
        FinalizeRecovery();
        return;
    }

    current_scan_idx_ = 0;

    if (HasSpdkResources() && !data_blobs_.empty()) {
        ScanNextFileBlob();
    } else {
        // No SPDK resources and no data blobs: finalize directly
        TransitionTo(State::kFinalizingRecovery);
        FinalizeRecovery();
    }
}

uint64_t IndexLoader::GetBlobPhysicalEnd(spdk_blob* blob) const {
    if (!blob || !blobstore_) {
        return 0;
    }
    uint64_t num_clusters = spdk_blob_get_num_clusters(blob);
    uint64_t cluster_size = spdk_bs_get_cluster_size(blobstore_);
    return (num_clusters * cluster_size) / kPageSize;
}

std::vector<ScanRange> IndexLoader::BuildScanRangesFromBlobs() {
    std::vector<ScanRange> ranges;

    for (const auto& blob_info : data_blobs_) {
        ScanRange range;
        range.file_id = blob_info.file_id;

        if (blob_info.file_id == checkpoint_file_id_) {
            range.start_page = checkpoint_page_index_;
        } else if (blob_info.file_id > checkpoint_file_id_) {
            range.start_page = sizeof(DataFileHeader) / kPageSize;
        } else {
            continue;  // Files before checkpoint are already in index
        }

        // End at physical blob end (NOT superblock snapshot point)
        // This ensures we don't miss writes that completed after checkpoint snapshot
        range.end_page = GetBlobPhysicalEnd(blob_info.blob);

        // Also consider the file size from blob_info if available
        uint64_t size_pages = blob_info.size / kPageSize;
        if (size_pages > 0 && size_pages < range.end_page) {
            range.end_page = size_pages;
        }

        if (range.end_page > range.start_page) {
            ranges.push_back(range);
        }
    }

    std::sort(ranges.begin(), ranges.end(),
              [](const ScanRange& a, const ScanRange& b) { return a.file_id < b.file_id; });

    return ranges;
}

// --- SPDK blob-based file scanning ---

void IndexLoader::ScanNextFileBlob() {
    if (current_scan_idx_ >= scan_ranges_.size()) {
        TransitionTo(State::kFinalizingRecovery);
        FinalizeRecovery();
        return;
    }

    const auto& range = scan_ranges_[current_scan_idx_];
    current_scan_offset_ = range.start_page * kPageSize;

    ScanBlobChunk();
}

void IndexLoader::ScanBlobChunk() {
    const auto& range = scan_ranges_[current_scan_idx_];
    uint64_t end_offset = range.end_page * kPageSize;

    if (current_scan_offset_ >= end_offset) {
        // Current file scan complete
        current_scan_idx_++;
        ScanNextFileBlob();
        return;
    }

    // Find the blob for this file
    spdk_blob* blob = nullptr;
    for (const auto& bi : data_blobs_) {
        if (bi.file_id == range.file_id) {
            blob = bi.blob;
            break;
        }
    }

    if (!blob) {
        current_scan_idx_++;
        ScanNextFileBlob();
        return;
    }

    // Read a 1MB chunk
    size_t read_size = std::min(static_cast<uint64_t>(1024 * 1024), end_offset - current_scan_offset_);
    size_t aligned_read = AlignUp(read_size, kPageSize);

    // Allocate DMA buffer for scan
    void* dma_buf = DmaAllocator::AllocZeroed(aligned_read, kPageSize);
    if (!dma_buf) {
        current_scan_idx_++;
        ScanNextFileBlob();
        return;
    }

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(blobstore_);
    uint64_t offset_units = current_scan_offset_ / io_unit_size;
    uint64_t length_units = aligned_read / io_unit_size;

    auto* ctx = scan_ctx_pool_.Alloc(this, dma_buf, read_size);

    spdk_blob_io_read(blob, io_channel_, dma_buf, offset_units, length_units,
                      OnScanChunkRead, ctx);
}

// =========================================================================
// Entry Parsing
// =========================================================================

size_t IndexLoader::ParseAndRebuildEntries(const void* buffer, size_t buffer_size, uint16_t file_id,
                                           uint64_t base_offset) {
    const char* ptr = static_cast<const char*>(buffer);
    size_t offset = 0;
    size_t entries_processed = 0;

    while (offset + sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) <= buffer_size) {
        const auto* header = reinterpret_cast<const EntryHeader*>(ptr + offset);

        // Validate magic
        if (header->magic != kEntryMagic) {
            // Invalid magic, try next aligned position
            size_t next_aligned = AlignUp(offset + 1, kPageSize);
            if (next_aligned >= buffer_size) {
                break;
            }
            offset = next_aligned;
            continue;
        }

        // Parse entry
        uint64_t key = *reinterpret_cast<const uint64_t*>(ptr + offset + sizeof(EntryHeader));
        uint32_t value_len = *reinterpret_cast<const uint32_t*>(ptr + offset + sizeof(EntryHeader) +
                                                                sizeof(uint64_t));

        // Calculate entry size (4KB aligned)
        size_t entry_size = AlignUp(sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) +
                                            value_len + sizeof(uint32_t),
                                    kPageSize);

        // Boundary check
        if (offset + entry_size > buffer_size) {
            break;
        }

        // Validate checksum
        uint32_t stored_checksum =
                *reinterpret_cast<const uint32_t*>(ptr + offset + entry_size - sizeof(uint32_t));
        uint32_t computed_checksum = Crc32::Calculate(ptr + offset, entry_size - sizeof(uint32_t));

        if (stored_checksum != computed_checksum) {
            // CRC mismatch: in Bitcask append-only model, subsequent data is untrusted
            // Stop scanning this file to avoid loading corrupted data
            break;
        }

        // Build index entry using the persisted sequence number (not a new one)
        MemIndexEntry new_entry;
        new_entry.key = key;
        new_entry.file_id = file_id;
        new_entry.offset_index = static_cast<uint32_t>((base_offset + offset) / kPageSize);
        new_entry.page_count = static_cast<uint16_t>(entry_size / kPageSize);
        new_entry.deleted = (header->flags & kFlagDeleted) ? 1 : 0;
        new_entry.sequence = header->sequence;  // Use persisted sequence

        // Compute tag
        uint64_t hash;
        HashUtil::ComputeHash(key, &hash, &new_entry.tag);

        // Track max sequence for global sequence sync
        if (header->sequence > recovered_max_sequence_) {
            recovered_max_sequence_ = header->sequence;
        }

        // Upsert to index (sequence comparison ensures correct ordering)
        if (new_entry.deleted) {
            mem_index_->Remove(key);
        } else {
            mem_index_->Upsert(key, new_entry);
        }

        entries_processed++;
        offset += entry_size;
    }

    return entries_processed;
}

// =========================================================================
// Recovery Finalization
// =========================================================================

void IndexLoader::FinalizeRecovery() {
    // Sync global sequence number:
    // new_global_seq = max(checkpoint_seq, recovered_max_seq) + 1
    // This ensures new writes always get a sequence number larger than
    // any existing record, preventing ordering conflicts.
    uint32_t checkpoint_seq = superblock_.checkpoint_global_seq;
    uint32_t new_global_seq = std::max(checkpoint_seq, recovered_max_sequence_) + 1;
    mem_index_->SetGlobalSequence(new_global_seq);

    TransitionTo(State::kDone);

    if (callback_) {
        callback_(KvError::kSuccess);
    }
}

// =========================================================================
// Validation Helpers
// =========================================================================

bool IndexLoader::ValidateSuperblock(const Superblock& sb) {
    if (sb.magic != kSuperblockMagic) {
        return false;
    }

    // Validate checksum
    uint32_t stored = sb.checksum;
    uint32_t computed = Crc32::Calculate(&sb, sizeof(Superblock) - sizeof(uint32_t));
    if (stored != computed) {
        return false;
    }

    return true;
}

bool IndexLoader::ValidateChecksum(const void* data, size_t size, uint32_t expected) {
    uint32_t computed = Crc32::Calculate(data, size);
    return computed == expected;
}

// =========================================================================
// Extracted static callbacks (from long lambdas)
// =========================================================================

void IndexLoader::OnSuperblockReadComplete(void* arg, int bserrno) {
    auto* ctx = static_cast<SuperblockReadCtx*>(arg);
    auto* self = ctx->loader;

    if (bserrno == 0) {
        auto* sb = static_cast<Superblock*>(ctx->buf);
        if (sb->magic == kSuperblockMagic) {
            uint32_t stored_checksum = sb->checksum;
            uint32_t computed =
                    Crc32::Calculate(sb, sizeof(Superblock) - sizeof(uint32_t));
            if (stored_checksum == computed) {
                std::memcpy(&self->superblock_, sb, sizeof(Superblock));
                DmaAllocator::Free(ctx->buf);
                self->superblock_read_ctx_pool_.Free(ctx);
                self->TransitionTo(State::kLoadingMemIndexA);
                self->LoadMemIndexArea(0);
                return;
            }
        }
    }

    // Primary failed, try backup
    if (!ctx->is_backup) {
        ctx->is_backup = true;
        uint64_t io_unit_size = spdk_bs_get_io_unit_size(self->blobstore_);
        uint64_t length_units = ctx->size / io_unit_size;

        self->TransitionTo(State::kLoadingSuperblockBackup);
        spdk_blob_io_read(self->superblock_blob_, self->io_channel_, ctx->buf,
                          kSuperblockBackupOffset / io_unit_size, length_units,
                          OnSuperblockBackupReadComplete, ctx);
    }
}

void IndexLoader::OnSuperblockBackupReadComplete(void* arg, int bserrno) {
    auto* ctx = static_cast<SuperblockReadCtx*>(arg);
    auto* self = ctx->loader;

    if (bserrno == 0) {
        auto* sb = static_cast<Superblock*>(ctx->buf);
        if (sb->magic == kSuperblockMagic) {
            uint32_t stored = sb->checksum;
            uint32_t computed =
                    Crc32::Calculate(sb, sizeof(Superblock) - sizeof(uint32_t));
            if (stored == computed) {
                std::memcpy(&self->superblock_, sb, sizeof(Superblock));
                DmaAllocator::Free(ctx->buf);
                self->superblock_read_ctx_pool_.Free(ctx);
                self->TransitionTo(State::kLoadingMemIndexA);
                self->LoadMemIndexArea(0);
                return;
            }
        }
    }

    // Both superblocks invalid
    DmaAllocator::Free(ctx->buf);
    self->superblock_read_ctx_pool_.Free(ctx);
    self->last_error_ = KvError::kCorruption;
    self->TransitionTo(State::kError);
    if (self->callback_) {
        self->callback_(self->last_error_);
    }
}

void IndexLoader::OnDirectSegmentLoaded(void* arg, int bserrno) {
    auto* ctx = static_cast<SegLoadCtx*>(arg);
    auto* self = ctx->loader;
    self->seg_load_ctx_pool_.Free(ctx);

    if (bserrno != 0) {
        self->has_load_error_ = true;
    }

    if (--self->pending_segment_loads_ == 0) {
        if (self->has_load_error_) {
            // Fall back to upsert from buffer
            self->LoadByUpsertFromBuffer();
        } else {
            // Rebuild PSL array (not persisted)
            self->RebuildPslArray();

            // Restore global sequence
            auto* hdr = reinterpret_cast<SerializedMemIndexHeader*>(
                    self->mem_index_buffer_.data());
            self->mem_index_->SetGlobalSequence(hdr->global_sequence);
        }

        // Continue to incremental rebuild
        self->TransitionTo(State::kRebuildingIncremental);
        self->StartIncrementalRebuild();
    }
}

void IndexLoader::OnScanChunkRead(void* arg, int bserrno) {
    auto* ctx = static_cast<ScanCtx*>(arg);
    auto* self = ctx->loader;

    if (bserrno != 0) {
        // Read error, skip remaining of this file
        DmaAllocator::Free(ctx->dma_buf);
        self->scan_ctx_pool_.Free(ctx);
        self->current_scan_idx_++;
        self->ScanNextFileBlob();
        return;
    }

    // Parse entries from the read chunk
    const auto& range = self->scan_ranges_[self->current_scan_idx_];
    size_t entries_found = self->ParseAndRebuildEntries(
            ctx->dma_buf, ctx->read_size, range.file_id, self->current_scan_offset_);

    DmaAllocator::Free(ctx->dma_buf);
    size_t read_size = ctx->read_size;
    self->scan_ctx_pool_.Free(ctx);

    if (entries_found == 0) {
        // No valid entries found, skip to next file
        self->current_scan_idx_++;
        self->ScanNextFileBlob();
    } else {
        // Continue scanning this file
        self->current_scan_offset_ += read_size;
        self->ScanBlobChunk();
    }
}

}  // namespace spdk_kv
