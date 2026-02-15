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
        DmaAllocator::free(dma_buffer_);
        dma_buffer_ = nullptr;
    }
}

void IndexLoader::start_recovery(RecoveryCallback callback) {
    if (state_ != State::kInit) {
        callback(KvError::kInvalidState);
        return;
    }

    callback_ = std::move(callback);
    transition_to(State::kLoadingSuperblockPrimary);
    load_superblock_primary();
}

bool IndexLoader::poll() {
    if (is_complete()) {
        return false;
    }

    // In real SPDK mode, poll for IO completions
    if (has_spdk_resources()) {
        SpdkEnv::instance().poll();
    }

    return !is_complete();
}

void IndexLoader::transition_to(State new_state) { state_ = new_state; }

// =========================================================================
// Superblock Loading
// =========================================================================

void IndexLoader::load_superblock_primary() {
    if (has_spdk_resources() && superblock_blob_) {
        // Async read from blob
        load_superblock_from_blob();
    } else {
        // Superblock already set via set_superblock()
        if (validate_superblock(superblock_)) {
            transition_to(State::kLoadingMemIndexA);
            load_mem_index_area(0);
        } else {
            transition_to(State::kLoadingSuperblockBackup);
            load_superblock_backup();
        }
    }
}

void IndexLoader::load_superblock_from_blob() {
    // Allocate DMA buffer for superblock read
    size_t sb_aligned_size = align_up(sizeof(Superblock), kPageSize);
    void* read_buf = DmaAllocator::alloc_zeroed(sb_aligned_size, kPageSize);
    if (!read_buf) {
        last_error_ = KvError::kInternalError;
        transition_to(State::kError);
        if (callback_) {
            callback_(last_error_);
        }
        return;
    }

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(blobstore_);
    uint64_t length_units = sb_aligned_size / io_unit_size;

    auto* ctx = superblock_read_ctx_pool_.alloc(this, read_buf, sb_aligned_size, false);

    spdk_blob_io_read(superblock_blob_, io_channel_, read_buf,
                      kSuperblockPrimaryOffset / io_unit_size, length_units,
                      on_superblock_read_complete, ctx);
}

void IndexLoader::load_superblock_backup() {
    // Fallback: validate pre-loaded superblock
    if (validate_superblock(superblock_)) {
        transition_to(State::kLoadingMemIndexA);
        load_mem_index_area(0);
    } else {
        last_error_ = KvError::kCorruption;
        transition_to(State::kError);
        if (callback_) {
            callback_(last_error_);
        }
    }
}

// =========================================================================
// MemIndex Area Loading
// =========================================================================

void IndexLoader::load_mem_index_area(int area) {
    if (has_spdk_resources()) {
        // Async read from blob
        load_mem_index_area_from_blob(area);
    } else {
        // No SPDK resources: use superblock metadata
        if (area == 0) {
            mem_index_versions_[0] = superblock_.checkpoint_sequence;
        } else {
            mem_index_versions_[1] = 0;  // Assume B is older
        }

        if (area == 0) {
            transition_to(State::kLoadingMemIndexB);
            load_mem_index_area(1);
        } else {
            transition_to(State::kComparingVersions);
            compare_and_select_area();
        }
    }
}

void IndexLoader::load_mem_index_area_from_blob(int area) {
    spdk_blob* blob = (area == 0) ? mem_index_blob_a_ : mem_index_blob_b_;
    if (!blob) {
        // No blob for this area, mark as invalid
        mem_index_versions_[area] = 0;
        if (area == 0) {
            transition_to(State::kLoadingMemIndexB);
            load_mem_index_area(1);
        } else {
            transition_to(State::kComparingVersions);
            compare_and_select_area();
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

    // start chunked reading (2MB per chunk)
    read_next_mem_index_chunk();
}

void IndexLoader::read_next_mem_index_chunk() {
    uint64_t remaining = mem_index_total_size_ - mem_index_read_offset_;
    size_t chunk_size = std::min(remaining, static_cast<uint64_t>(2 * 1024 * 1024));

    if (chunk_size == 0) {
        on_mem_index_area_load_complete();
        return;
    }

    // Ensure buffer is large enough
    if (mem_index_buffer_.size() < mem_index_read_offset_ + chunk_size) {
        mem_index_buffer_.resize(mem_index_read_offset_ + chunk_size);
    }

    // Allocate DMA buffer for this chunk
    size_t aligned_chunk = align_up(chunk_size, kPageSize);
    void* dma_buf = DmaAllocator::alloc_zeroed(aligned_chunk, kPageSize);
    if (!dma_buf) {
        // Fall back: mark area as invalid
        mem_index_versions_[current_load_area_] = 0;
        on_mem_index_area_load_complete();
        return;
    }

    spdk_blob* blob =
            (current_load_area_ == 0) ? mem_index_blob_a_ : mem_index_blob_b_;

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(blobstore_);
    uint64_t offset_units = mem_index_read_offset_ / io_unit_size;
    uint64_t length_units = aligned_chunk / io_unit_size;

    auto* ctx = chunk_ctx_pool_.alloc(this, dma_buf, chunk_size);

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

                DmaAllocator::free(ctx->dma_buf);
                self->chunk_ctx_pool_.free(ctx);

                self->on_mem_index_chunk_loaded(bserrno);
            },
            ctx);
}

void IndexLoader::on_mem_index_chunk_loaded(int status) {
    if (status != 0) {
        // Read error, mark area as invalid
        mem_index_versions_[current_load_area_] = 0;
        on_mem_index_area_load_complete();
        return;
    }

    mem_index_read_offset_ += 2 * 1024 * 1024;
    if (mem_index_read_offset_ > mem_index_total_size_) {
        mem_index_read_offset_ = mem_index_total_size_;
    }
    read_next_mem_index_chunk();
}

void IndexLoader::on_mem_index_area_load_complete() {
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
                uint32_t computed = Crc32::calculate(
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
        transition_to(State::kLoadingMemIndexB);
        load_mem_index_area(1);
    } else {
        transition_to(State::kComparingVersions);
        compare_and_select_area();
    }
}

// =========================================================================
// Compare and deserialize
// =========================================================================

void IndexLoader::compare_and_select_area() {
    // Select area with higher version
    selected_area_ = (mem_index_versions_[0] >= mem_index_versions_[1]) ? 0 : 1;

    if (mem_index_versions_[selected_area_] == 0) {
        // No valid MemIndex area, need full rebuild from data files
        transition_to(State::kScanningDataFiles);
        start_incremental_rebuild();
    } else {
        transition_to(State::kDeserializing);
        deserialize_mem_index(selected_area_);
    }
}

void IndexLoader::deserialize_mem_index(int area) {
    if (can_use_memory_dump_load()) {
        if (has_spdk_resources()) {
            load_as_memory_dump_from_blob(area);
            return;  // Async path continues in callbacks
        } else {
            load_as_memory_dump(area);
        }
    } else {
        if (has_spdk_resources()) {
            load_by_upsert_from_buffer();
        } else {
            load_by_upsert(area);
        }
    }

    // Continue to incremental rebuild
    transition_to(State::kRebuildingIncremental);
    start_incremental_rebuild();
}

bool IndexLoader::can_use_memory_dump_load() const {
    if (!mem_index_ || mem_index_buffer_.size() < sizeof(SerializedMemIndexHeader)) {
        return false;
    }

    auto* header =
            reinterpret_cast<const SerializedMemIndexHeader*>(mem_index_buffer_.data());

    // Can use memory dump if capacity matches and version is compatible
    // Memory dump requires that the hash table layout is identical
    return header->capacity == mem_index_->capacity() && header->version == 1;
}

void IndexLoader::load_as_memory_dump(int area) {
    (void)area;
    // No SPDK resources: fall back to upsert
    load_by_upsert(area);
}

void IndexLoader::load_as_memory_dump_from_blob(int area) {
    // Direct memory dump load: read raw entry data from blob directly into
    // MemIndex entries array. This is the fastest recovery path.

    auto* header =
            reinterpret_cast<SerializedMemIndexHeader*>(mem_index_buffer_.data());

    spdk_blob* blob = (area == 0) ? mem_index_blob_a_ : mem_index_blob_b_;
    if (!blob) {
        // Fall back to upsert
        load_by_upsert_from_buffer();
        transition_to(State::kRebuildingIncremental);
        start_incremental_rebuild();
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
        // calculate source offset in blob (after header, segment-by-segment)
        uint64_t blob_offset = sizeof(SerializedMemIndexHeader) + seg * segment_size;
        uint64_t offset_units = blob_offset / io_unit_size;
        uint64_t length_units = segment_size / io_unit_size;

        // Target: directly into MemIndex entries array
        void* target_addr =
                reinterpret_cast<char*>(mem_index_->entries()) + seg * segment_size;

        auto* ctx = seg_load_ctx_pool_.alloc(this);

        spdk_blob_io_read(blob, io_channel_, target_addr, offset_units, length_units,
                          on_direct_segment_loaded, ctx);
    }
}

void IndexLoader::rebuild_psl_array() {
    if (!mem_index_) {
        return;
    }
    mem_index_->rebuild_psl_array();
}

void IndexLoader::load_by_upsert(int area) {
    (void)area;
    // No SPDK resources: mem_index is already populated externally
    // With SPDK resources, use load_by_upsert_from_buffer() instead
}

void IndexLoader::load_by_upsert_from_buffer() {
    // upsert load from mem_index_buffer_: iterate serialized entries and insert via upsert.
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
    uint64_t capacity = mem_index_->capacity();

    for (uint64_t i = 0; i < count; i += kBatchSize) {
        // Prefetch next batch
        for (size_t j = 0; j < kBatchSize && i + j + kBatchSize < count; j++) {
            uint64_t key = entries[i + j + kBatchSize].key;
            uint64_t hash = HashUtil::hash(key);
            uint64_t idx = hash & (capacity - 1);
            __builtin_prefetch(&mem_index_->entries()[idx], 1, 3);
        }

        // Insert current batch
        for (size_t j = 0; j < kBatchSize && i + j < count; j++) {
            const MemIndexEntry& src = entries[i + j];
            if (!src.is_deleted()) {
                mem_index_->upsert(src.key, src);
            }
        }
    }

    // Restore global sequence
    mem_index_->set_global_sequence(header->global_sequence);
}

// =========================================================================
// Incremental Recovery (data File Scanning)
// =========================================================================

void IndexLoader::start_incremental_rebuild() {
    // get checkpoint info from superblock
    checkpoint_file_id_ = superblock_.checkpoint_file_id;
    checkpoint_page_index_ = superblock_.checkpoint_page_index;
    recovered_max_sequence_ = superblock_.checkpoint_global_seq;

    // Build scan ranges from blob info
    if (!data_blobs_.empty()) {
        scan_ranges_ = build_scan_ranges_from_blobs();
    }

    if (scan_ranges_.empty()) {
        transition_to(State::kFinalizingRecovery);
        finalize_recovery();
        return;
    }

    current_scan_idx_ = 0;

    if (has_spdk_resources() && !data_blobs_.empty()) {
        scan_next_file_blob();
    } else {
        // No SPDK resources and no data blobs: finalize directly
        transition_to(State::kFinalizingRecovery);
        finalize_recovery();
    }
}

uint64_t IndexLoader::get_blob_physical_end(spdk_blob* blob) const {
    if (!blob || !blobstore_) {
        return 0;
    }
    uint64_t num_clusters = spdk_blob_get_num_clusters(blob);
    uint64_t cluster_size = spdk_bs_get_cluster_size(blobstore_);
    return (num_clusters * cluster_size) / kPageSize;
}

std::vector<ScanRange> IndexLoader::build_scan_ranges_from_blobs() {
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
        range.end_page = get_blob_physical_end(blob_info.blob);

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

void IndexLoader::scan_next_file_blob() {
    if (current_scan_idx_ >= scan_ranges_.size()) {
        transition_to(State::kFinalizingRecovery);
        finalize_recovery();
        return;
    }

    const auto& range = scan_ranges_[current_scan_idx_];
    current_scan_offset_ = range.start_page * kPageSize;

    scan_blob_chunk();
}

void IndexLoader::scan_blob_chunk() {
    const auto& range = scan_ranges_[current_scan_idx_];
    uint64_t end_offset = range.end_page * kPageSize;

    if (current_scan_offset_ >= end_offset) {
        // Current file scan complete
        current_scan_idx_++;
        scan_next_file_blob();
        return;
    }

    // find the blob for this file
    spdk_blob* blob = nullptr;
    for (const auto& bi : data_blobs_) {
        if (bi.file_id == range.file_id) {
            blob = bi.blob;
            break;
        }
    }

    if (!blob) {
        current_scan_idx_++;
        scan_next_file_blob();
        return;
    }

    // Read a 1MB chunk
    size_t read_size = std::min(static_cast<uint64_t>(1024 * 1024), end_offset - current_scan_offset_);
    size_t aligned_read = align_up(read_size, kPageSize);

    // Allocate DMA buffer for scan
    void* dma_buf = DmaAllocator::alloc_zeroed(aligned_read, kPageSize);
    if (!dma_buf) {
        current_scan_idx_++;
        scan_next_file_blob();
        return;
    }

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(blobstore_);
    uint64_t offset_units = current_scan_offset_ / io_unit_size;
    uint64_t length_units = aligned_read / io_unit_size;

    auto* ctx = scan_ctx_pool_.alloc(this, dma_buf, read_size);

    spdk_blob_io_read(blob, io_channel_, dma_buf, offset_units, length_units,
                      on_scan_chunk_read, ctx);
}

// =========================================================================
// Entry Parsing
// =========================================================================

size_t IndexLoader::parse_and_rebuild_entries(const void* buffer, size_t buffer_size, uint16_t file_id,
                                           uint64_t base_offset) {
    const char* ptr = static_cast<const char*>(buffer);
    size_t offset = 0;
    size_t entries_processed = 0;

    while (offset + sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) <= buffer_size) {
        const auto* header = reinterpret_cast<const EntryHeader*>(ptr + offset);

        // Validate magic
        if (header->magic != kEntryMagic) {
            // Invalid magic, try next aligned position
            size_t next_aligned = align_up(offset + 1, kPageSize);
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

        // calculate entry size (4KB aligned)
        size_t entry_size = align_up(sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) +
                                            value_len + sizeof(uint32_t),
                                    kPageSize);

        // Boundary check
        if (offset + entry_size > buffer_size) {
            break;
        }

        // Validate checksum
        uint32_t stored_checksum =
                *reinterpret_cast<const uint32_t*>(ptr + offset + entry_size - sizeof(uint32_t));
        uint32_t computed_checksum = Crc32::calculate(ptr + offset, entry_size - sizeof(uint32_t));

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
        HashUtil::compute_hash(key, &hash, &new_entry.tag);

        // Track max sequence for global sequence sync
        if (header->sequence > recovered_max_sequence_) {
            recovered_max_sequence_ = header->sequence;
        }

        // upsert to index (sequence comparison ensures correct ordering)
        if (new_entry.deleted) {
            mem_index_->remove(key);
        } else {
            mem_index_->upsert(key, new_entry);
        }

        entries_processed++;
        offset += entry_size;
    }

    return entries_processed;
}

// =========================================================================
// Recovery Finalization
// =========================================================================

void IndexLoader::finalize_recovery() {
    // Sync global sequence number:
    // new_global_seq = max(checkpoint_seq, recovered_max_seq) + 1
    // This ensures new writes always get a sequence number larger than
    // any existing record, preventing ordering conflicts.
    uint32_t checkpoint_seq = superblock_.checkpoint_global_seq;
    uint32_t new_global_seq = std::max(checkpoint_seq, recovered_max_sequence_) + 1;
    mem_index_->set_global_sequence(new_global_seq);

    transition_to(State::kDone);

    if (callback_) {
        callback_(KvError::kSuccess);
    }
}

// =========================================================================
// Validation Helpers
// =========================================================================

bool IndexLoader::validate_superblock(const Superblock& sb) {
    if (sb.magic != kSuperblockMagic) {
        return false;
    }

    // Validate checksum
    uint32_t stored = sb.checksum;
    uint32_t computed = Crc32::calculate(&sb, sizeof(Superblock) - sizeof(uint32_t));
    if (stored != computed) {
        return false;
    }

    return true;
}

bool IndexLoader::validate_checksum(const void* data, size_t size, uint32_t expected) {
    uint32_t computed = Crc32::calculate(data, size);
    return computed == expected;
}

// =========================================================================
// Extracted static callbacks (from long lambdas)
// =========================================================================

void IndexLoader::on_superblock_read_complete(void* arg, int bserrno) {
    auto* ctx = static_cast<SuperblockReadCtx*>(arg);
    auto* self = ctx->loader;

    if (bserrno == 0) {
        auto* sb = static_cast<Superblock*>(ctx->buf);
        if (sb->magic == kSuperblockMagic) {
            uint32_t stored_checksum = sb->checksum;
            uint32_t computed =
                    Crc32::calculate(sb, sizeof(Superblock) - sizeof(uint32_t));
            if (stored_checksum == computed) {
                std::memcpy(&self->superblock_, sb, sizeof(Superblock));
                DmaAllocator::free(ctx->buf);
                self->superblock_read_ctx_pool_.free(ctx);
                self->transition_to(State::kLoadingMemIndexA);
                self->load_mem_index_area(0);
                return;
            }
        }
    }

    // Primary failed, try backup
    if (!ctx->is_backup) {
        ctx->is_backup = true;
        uint64_t io_unit_size = spdk_bs_get_io_unit_size(self->blobstore_);
        uint64_t length_units = ctx->size / io_unit_size;

        self->transition_to(State::kLoadingSuperblockBackup);
        spdk_blob_io_read(self->superblock_blob_, self->io_channel_, ctx->buf,
                          kSuperblockBackupOffset / io_unit_size, length_units,
                          on_superblock_backup_read_complete, ctx);
    }
}

void IndexLoader::on_superblock_backup_read_complete(void* arg, int bserrno) {
    auto* ctx = static_cast<SuperblockReadCtx*>(arg);
    auto* self = ctx->loader;

    if (bserrno == 0) {
        auto* sb = static_cast<Superblock*>(ctx->buf);
        if (sb->magic == kSuperblockMagic) {
            uint32_t stored = sb->checksum;
            uint32_t computed =
                    Crc32::calculate(sb, sizeof(Superblock) - sizeof(uint32_t));
            if (stored == computed) {
                std::memcpy(&self->superblock_, sb, sizeof(Superblock));
                DmaAllocator::free(ctx->buf);
                self->superblock_read_ctx_pool_.free(ctx);
                self->transition_to(State::kLoadingMemIndexA);
                self->load_mem_index_area(0);
                return;
            }
        }
    }

    // Both superblocks invalid
    DmaAllocator::free(ctx->buf);
    self->superblock_read_ctx_pool_.free(ctx);
    self->last_error_ = KvError::kCorruption;
    self->transition_to(State::kError);
    if (self->callback_) {
        self->callback_(self->last_error_);
    }
}

void IndexLoader::on_direct_segment_loaded(void* arg, int bserrno) {
    auto* ctx = static_cast<SegLoadCtx*>(arg);
    auto* self = ctx->loader;
    self->seg_load_ctx_pool_.free(ctx);

    if (bserrno != 0) {
        self->has_load_error_ = true;
    }

    if (--self->pending_segment_loads_ == 0) {
        if (self->has_load_error_) {
            // Fall back to upsert from buffer
            self->load_by_upsert_from_buffer();
        } else {
            // Rebuild PSL array (not persisted)
            self->rebuild_psl_array();

            // Restore global sequence
            auto* hdr = reinterpret_cast<SerializedMemIndexHeader*>(
                    self->mem_index_buffer_.data());
            self->mem_index_->set_global_sequence(hdr->global_sequence);
        }

        // Continue to incremental rebuild
        self->transition_to(State::kRebuildingIncremental);
        self->start_incremental_rebuild();
    }
}

void IndexLoader::on_scan_chunk_read(void* arg, int bserrno) {
    auto* ctx = static_cast<ScanCtx*>(arg);
    auto* self = ctx->loader;

    if (bserrno != 0) {
        // Read error, skip remaining of this file
        DmaAllocator::free(ctx->dma_buf);
        self->scan_ctx_pool_.free(ctx);
        self->current_scan_idx_++;
        self->scan_next_file_blob();
        return;
    }

    // Parse entries from the read chunk
    const auto& range = self->scan_ranges_[self->current_scan_idx_];
    size_t entries_found = self->parse_and_rebuild_entries(
            ctx->dma_buf, ctx->read_size, range.file_id, self->current_scan_offset_);

    DmaAllocator::free(ctx->dma_buf);
    size_t read_size = ctx->read_size;
    self->scan_ctx_pool_.free(ctx);

    if (entries_found == 0) {
        // No valid entries found, skip to next file
        self->current_scan_idx_++;
        self->scan_next_file_blob();
    } else {
        // Continue scanning this file
        self->current_scan_offset_ += read_size;
        self->scan_blob_chunk();
    }
}

}  // namespace spdk_kv
