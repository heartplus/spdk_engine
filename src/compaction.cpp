// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#include "spdk_kv/compaction.h"

#include <chrono>
#include <cstring>

#include "spdk_kv/crc32.h"
#include "spdk_kv/engine.h"

namespace spdk_kv {

// ============================================================================
// SparseBitmap implementation
// ============================================================================

SparseBitmap::SparseBitmap(size_t total_bits)
        : total_bits_(total_bits), chunk_count_((total_bits + kChunkBits - 1) / kChunkBits) {}

void SparseBitmap::Set(size_t idx) {
    if (idx >= total_bits_) {
        return;
    }

    size_t chunk_idx = idx / kChunkBits;
    size_t bit_idx = idx % kChunkBits;

    auto it = chunks_.find(chunk_idx);
    if (it == chunks_.end()) {
        chunks_[chunk_idx] = std::make_unique<Chunk>();
    }
    chunks_[chunk_idx]->Set(bit_idx);
}

void SparseBitmap::Clear(size_t idx) {
    if (idx >= total_bits_) {
        return;
    }

    size_t chunk_idx = idx / kChunkBits;
    size_t bit_idx = idx % kChunkBits;

    auto it = chunks_.find(chunk_idx);
    if (it != chunks_.end()) {
        it->second->Clear(bit_idx);
        // Optionally free empty chunks
        if (it->second->IsEmpty()) {
            chunks_.erase(it);
        }
    }
}

bool SparseBitmap::Test(size_t idx) const {
    if (idx >= total_bits_) {
        return false;
    }

    size_t chunk_idx = idx / kChunkBits;
    auto it = chunks_.find(chunk_idx);
    if (it == chunks_.end()) {
        return false;
    }
    return it->second->Test(idx % kChunkBits);
}

size_t SparseBitmap::MemoryUsage() const { return chunks_.size() * sizeof(Chunk) + sizeof(*this); }

size_t SparseBitmap::PopCount() const {
    size_t count = 0;
    for (const auto& pair : chunks_) {
        count += pair.second->PopCount();
    }
    return count;
}

// ============================================================================
// FileMetadata implementation
// ============================================================================

void FileMetadata::MaybeCreateBitmap() {
    if (!valid_bitmap && GarbageRatio() >= kBitmapCreationThreshold) {
        // 8GB / 4KB = 2M pages
        size_t total_pages = kDefaultFileSize / kPageSize;
        valid_bitmap = std::make_unique<SparseBitmap>(total_pages);
    }
}

void FileMetadata::MarkValid(uint32_t offset_index, uint16_t page_count, uint32_t bytes) {
    if (valid_bitmap) {
        for (uint16_t i = 0; i < page_count; i++) {
            valid_bitmap->Set(offset_index + i);
        }
    }
    valid_entries++;
    valid_bytes += bytes;
}

void FileMetadata::MarkInvalid(uint32_t offset_index, uint16_t page_count, uint32_t bytes) {
    MaybeCreateBitmap();

    if (valid_bitmap) {
        for (uint16_t i = 0; i < page_count; i++) {
            valid_bitmap->Clear(offset_index + i);
        }
    }
    if (valid_entries > 0) {
        valid_entries--;
    }
    if (valid_bytes >= bytes) {
        valid_bytes -= bytes;
    }
}

bool FileMetadata::IsPageValid(uint32_t offset_index) const {
    if (!valid_bitmap) {
        // No bitmap, assume valid (conservative)
        return true;
    }
    return valid_bitmap->Test(offset_index);
}

// ============================================================================
// RateLimiter implementation
// ============================================================================

RateLimiter::RateLimiter(uint32_t max_iops)
        : max_iops_(max_iops), tokens_(max_iops), last_update_ns_(0) {
    // Calculate token interval in nanoseconds
    token_interval_ns_ = 1000000000ULL / max_iops;
}

void RateLimiter::SetRate(uint32_t iops) {
    max_iops_ = iops;
    if (iops > 0) {
        token_interval_ns_ = 1000000000ULL / iops;
    }
}

bool RateLimiter::Allow() {
    if (max_iops_ == 0) {
        return true;
    }

    auto now = std::chrono::steady_clock::now();
    uint64_t now_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();

    // Add tokens based on elapsed time
    if (last_update_ns_ > 0) {
        uint64_t elapsed = now_ns - last_update_ns_;
        uint64_t new_tokens = elapsed / token_interval_ns_;
        tokens_ = std::min(tokens_ + new_tokens, static_cast<uint64_t>(max_iops_));
    }
    last_update_ns_ = now_ns;

    // Check if we have tokens
    if (tokens_ > 0) {
        tokens_--;
        return true;
    }
    return false;
}

void RateLimiter::Reset() {
    tokens_ = max_iops_;
    last_update_ns_ = 0;
}

// ============================================================================
// CompactionTask implementation
// ============================================================================

CompactionTask::CompactionTask(uint16_t src_file_id, Engine* engine)
        : engine_(engine),
          src_file_id_(src_file_id),
          dest_file_id_(0),
          src_file_info_(nullptr),
          dest_file_info_(nullptr),
          src_meta_(nullptr),
          dest_meta_(nullptr),
          state_(State::kInit),
          last_error_(0),
          retry_count_(0),
          retry_start_time_ns_(0),
          current_offset_(sizeof(DataFileHeader)),
          dest_offset_(0),
          entries_processed_(0),
          entries_migrated_(0),
          read_buffer_(nullptr),
          write_buffer_(nullptr),
          write_buffer_used_(0),
          bytes_read_(0),
          io_pending_(false),
          retry_target_state_(State::kInit) {}

CompactionTask::~CompactionTask() { FreeDmaBuffers(); }

void CompactionTask::Step() {
    if (io_pending_) {
        return;
    }

    switch (state_) {
        case State::kInit:
            Init();
            break;
        case State::kMarkCompacting:
            MarkCompacting();
            break;
        case State::kWaitMarkComplete:
            // Wait for IO completion
            break;
        case State::kReadChunk:
            ReadNextChunk();
            break;
        case State::kWaitReadComplete:
            // Wait for IO completion
            break;
        case State::kProcessEntries:
            ProcessEntries();
            break;
        case State::kWriteChunk:
            WriteChunk();
            break;
        case State::kWaitWriteComplete:
            // Wait for IO completion
            break;
        case State::kUpdateIndices:
            UpdateIndices();
            break;
        case State::kFinalize:
            Finalize();
            break;
        case State::kMarkDeleted:
            MarkDeleted();
            break;
        case State::kWaitDeleteComplete:
            // Wait for IO completion
            break;
        case State::kRetryWait:
            CheckRetryTimeout();
            break;
        case State::kRollback:
            StartRollback();
            break;
        case State::kRollbackMarkSealed:
            RollbackMarkSealed();
            break;
        case State::kWaitRollbackComplete:
            // Wait for IO completion
            break;
        case State::kFailed:
        case State::kDone:
            // Terminal states
            break;
    }
}

void CompactionTask::Init() {
    // Allocate DMA buffers
    read_buffer_ = DmaAllocator::Alloc(kChunkSize, kPageSize);
    write_buffer_ = DmaAllocator::Alloc(kChunkSize, kPageSize);
    if (!read_buffer_ || !write_buffer_) {
        last_error_ = -ENOMEM;
        state_ = State::kFailed;
        return;
    }

    // Resolve source file info and metadata
    src_file_info_ = engine_->GetFile(src_file_id_);
    src_meta_ = engine_->GetFileMetadata(src_file_id_);
    if (!src_file_info_ || !src_meta_) {
        last_error_ = -ENOENT;
        state_ = State::kFailed;
        return;
    }

    // Allocate destination file
    dest_file_info_ = engine_->AllocateNewFile();
    if (!dest_file_info_) {
        last_error_ = -ENOSPC;
        state_ = State::kFailed;
        return;
    }
    dest_file_id_ = dest_file_info_->file_id;
    dest_meta_ = engine_->GetFileMetadata(dest_file_id_);

    dest_offset_ = sizeof(DataFileHeader);
    migrated_entries_.clear();

    state_ = State::kMarkCompacting;
}

void CompactionTask::MarkCompacting() {
    src_meta_->state = FileState::kCompacting;
    src_file_info_->state = FileState::kCompacting;

    io_pending_ = true;
    WriteFileHeader(src_file_info_, FileState::kCompacting, [this](int status) {
        io_pending_ = false;
        if (status == 0) {
            state_ = State::kReadChunk;
        } else {
            HandleIoError(State::kMarkCompacting, status);
        }
    });
}

void CompactionTask::SkipInvalidPages() {
    if (!src_meta_->valid_bitmap) {
        return;
    }

    uint64_t src_end = src_file_info_->write_offset;
    // Skip pages that are marked invalid
    while (current_offset_ < src_end) {
        uint32_t page_idx = static_cast<uint32_t>(current_offset_ / kPageSize);
        if (src_meta_->IsPageValid(page_idx)) {
            break;
        }
        current_offset_ += kPageSize;
    }
}

void CompactionTask::ReadNextChunk() {
    // Skip invalid pages
    SkipInvalidPages();

    uint64_t src_end = src_file_info_->write_offset;
    if (current_offset_ >= src_end) {
        // All data processed
        if (write_buffer_used_ > 0) {
            state_ = State::kWriteChunk;
        } else {
            state_ = State::kFinalize;
        }
        return;
    }

    size_t read_size = std::min(kChunkSize, static_cast<size_t>(src_end - current_offset_));
    // Align read size up to page boundary
    read_size = AlignUp(read_size, kPageSize);

    io_pending_ = true;
    engine_->SubmitBlobRead(
            src_file_info_, current_offset_, read_buffer_, static_cast<uint32_t>(read_size),
            [this, read_size](int status) {
                io_pending_ = false;
                if (status == 0) {
                    bytes_read_ = read_size;
                    state_ = State::kProcessEntries;
                } else {
                    HandleIoError(State::kReadChunk, status);
                }
            });
}

bool CompactionTask::ValidateEntry(const void* entry_data, size_t max_size) {
    if (max_size < sizeof(EntryHeader)) {
        return false;
    }

    const auto* header = static_cast<const EntryHeader*>(entry_data);
    if (header->magic != kEntryMagic) {
        return false;
    }

    // Get value length
    const char* ptr = static_cast<const char*>(entry_data);
    uint32_t value_len =
            *reinterpret_cast<const uint32_t*>(ptr + sizeof(EntryHeader) + sizeof(uint64_t));

    // Calculate entry size
    size_t entry_size = AlignUp(sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) +
                                        value_len + sizeof(uint32_t),
                                kPageSize);

    if (entry_size > max_size) {
        return false;
    }

    // Validate checksum
    uint32_t stored_checksum =
            *reinterpret_cast<const uint32_t*>(ptr + entry_size - sizeof(uint32_t));
    uint32_t computed_checksum = Crc32::Calculate(ptr, entry_size - sizeof(uint32_t));

    return stored_checksum == computed_checksum;
}

void CompactionTask::ProcessEntries() {
    const char* ptr = static_cast<char*>(read_buffer_);
    char* wptr = static_cast<char*>(write_buffer_);
    size_t offset = 0;
    MemIndex* mem_index = engine_->GetMemIndex();

    while (offset < bytes_read_) {
        // Validate entry
        if (!ValidateEntry(ptr + offset, bytes_read_ - offset)) {
            // Skip to next page
            offset = AlignUp(offset + 1, kPageSize);
            continue;
        }

        const auto* header = reinterpret_cast<const EntryHeader*>(ptr + offset);
        uint64_t key = *reinterpret_cast<const uint64_t*>(ptr + offset + sizeof(EntryHeader));
        uint32_t value_len = *reinterpret_cast<const uint32_t*>(ptr + offset + sizeof(EntryHeader) +
                                                                sizeof(uint64_t));

        size_t entry_size = AlignUp(sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) +
                                            value_len + sizeof(uint32_t),
                                    kPageSize);

        entries_processed_++;

        // Check if this entry is still valid in the current index
        MemIndexEntry* current = mem_index->Find(key);
        bool should_migrate = false;

        if (current && !current->is_deleted()) {
            // Check if current index points to this entry in the source file
            if (current->file_id == src_file_id_) {
                uint32_t entry_offset_index =
                        static_cast<uint32_t>((current_offset_ + offset) / kPageSize);
                if (current->offset_index == entry_offset_index) {
                    should_migrate = true;
                }
            }
        }

        if (should_migrate && !(header->flags & kFlagDeleted)) {
            // Check if write buffer has space
            if (write_buffer_used_ + entry_size > kChunkSize) {
                // Write buffer full - flush it first, then re-read this chunk
                // (current_offset_ is NOT advanced, so ReadNextChunk will re-read)
                state_ = State::kWriteChunk;
                return;
            }

            // Copy entry to write buffer
            std::memcpy(wptr + write_buffer_used_, ptr + offset, entry_size);

            // Record migration info
            MigratedEntry migrated;
            migrated.key = key;
            migrated.old_file_id = src_file_id_;
            migrated.old_offset_index =
                    static_cast<uint32_t>((current_offset_ + offset) / kPageSize);
            migrated.new_file_id = dest_file_id_;
            migrated.new_offset_index =
                    static_cast<uint32_t>((dest_offset_ + write_buffer_used_) / kPageSize);
            migrated.page_count = static_cast<uint16_t>(entry_size / kPageSize);
            migrated.sequence = header->sequence;
            migrated_entries_.push_back(migrated);

            write_buffer_used_ += entry_size;
            entries_migrated_++;
        }

        offset += entry_size;
    }

    // All entries in this chunk processed, advance to next chunk
    current_offset_ += bytes_read_;
    state_ = State::kReadChunk;
}

void CompactionTask::WriteChunk() {
    if (write_buffer_used_ == 0) {
        state_ = State::kReadChunk;
        return;
    }

    io_pending_ = true;
    engine_->SubmitBlobWrite(
            dest_file_info_, dest_offset_, write_buffer_,
            static_cast<uint32_t>(write_buffer_used_), [this](int status) {
                io_pending_ = false;
                if (status == 0) {
                    retry_count_ = 0;
                    state_ = State::kUpdateIndices;
                } else {
                    HandleIoError(State::kWriteChunk, status);
                }
            });
}

void CompactionTask::UpdateIndices() {
    MemIndex* mem_index = engine_->GetMemIndex();

    // Update memory indices for migrated entries
    for (const auto& migrated : migrated_entries_) {
        MemIndexEntry* current = mem_index->Find(migrated.key);
        if (current) {
            // Only update if still pointing to old location
            // (prevents overwriting a newer user write that happened during compaction)
            if (current->file_id == migrated.old_file_id &&
                current->offset_index == migrated.old_offset_index) {
                current->file_id = migrated.new_file_id;
                current->offset_index = migrated.new_offset_index;
                current->page_count = migrated.page_count;
            }
        }
    }

    // Move to committed list for potential rollback
    committed_updates_.insert(committed_updates_.end(), migrated_entries_.begin(),
                              migrated_entries_.end());
    migrated_entries_.clear();

    // Advance destination offset and update dest file write_offset
    dest_offset_ += write_buffer_used_;
    dest_file_info_->write_offset = dest_offset_;
    dest_file_info_->size = dest_offset_;
    write_buffer_used_ = 0;

    // Continue reading next chunk
    state_ = State::kReadChunk;
}

void CompactionTask::Finalize() {
    // Flush any remaining write buffer
    if (write_buffer_used_ > 0) {
        state_ = State::kWriteChunk;
        return;
    }

    // All data migrated and indices updated, mark source file as deleted
    state_ = State::kMarkDeleted;
}

void CompactionTask::MarkDeleted() {
    src_meta_->state = FileState::kDeleted;
    src_meta_->valid_entries = 0;
    src_meta_->valid_bytes = 0;

    io_pending_ = true;
    WriteFileHeader(src_file_info_, FileState::kDeleted, [this](int status) {
        io_pending_ = false;
        if (status == 0) {
            // Clear committed updates (no longer needed since compaction succeeded)
            committed_updates_.clear();
            migrated_entries_.clear();
            FreeDmaBuffers();
            state_ = State::kDone;
        } else {
            HandleIoError(State::kMarkDeleted, status);
        }
    });
}

void CompactionTask::CheckRetryTimeout() {
    auto now = std::chrono::steady_clock::now();
    uint64_t now_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();

    if (now_ns - retry_start_time_ns_ >= kRetryDelayUs * 1000) {
        // Retry delay elapsed, resume at the state that failed
        state_ = retry_target_state_;
    }
}

void CompactionTask::HandleIoError(State retry_state, int error_code) {
    retry_count_++;
    last_error_ = error_code;

    if (IsRetryableError(error_code) && retry_count_ < kMaxRetryCount) {
        // Retryable error: wait and then retry the failed operation
        retry_target_state_ = retry_state;
        auto now = std::chrono::steady_clock::now();
        retry_start_time_ns_ =
                std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
        state_ = State::kRetryWait;
    } else {
        // Non-retryable error or max retries exceeded: rollback
        state_ = State::kRollback;
    }
}

bool CompactionTask::IsRetryableError(int error_code) {
    switch (error_code) {
        case -EAGAIN:
        case -EBUSY:
        case -ENOMEM:
            return true;
        default:
            return false;
    }
}

void CompactionTask::StartRollback() {
    MemIndex* mem_index = engine_->GetMemIndex();
    (void)mem_index;

    // 1. Revert all committed index updates (restore indices to point at source file)
    for (const auto& update : committed_updates_) {
        RevertIndexUpdate(update);
    }
    committed_updates_.clear();

    // Also revert any pending (not yet committed) migrations
    for (const auto& update : migrated_entries_) {
        RevertIndexUpdate(update);
    }
    migrated_entries_.clear();

    // 2. Clean up the partially-written destination file
    if (dest_file_info_ && dest_offset_ > sizeof(DataFileHeader)) {
        DeleteGarbageFile();
    } else {
        state_ = State::kRollbackMarkSealed;
    }
}

void CompactionTask::RevertIndexUpdate(const MigratedEntry& update) {
    MemIndexEntry* existing = engine_->GetMemIndex()->Find(update.key);
    if (!existing) {
        return;
    }

    // Only revert if the index currently points to the new (dest) location
    if (existing->file_id == update.new_file_id &&
        existing->offset_index == update.new_offset_index) {
        existing->file_id = update.old_file_id;
        existing->offset_index = update.old_offset_index;
        existing->page_count = update.page_count;
    }
}

void CompactionTask::DeleteGarbageFile() {
    // Mark dest file and metadata as deleted
    if (dest_file_info_) {
        dest_file_info_->state = FileState::kDeleted;
    }
    if (dest_meta_) {
        dest_meta_->state = FileState::kDeleted;
        dest_meta_->valid_entries = 0;
        dest_meta_->valid_bytes = 0;
    }

    // Async close and delete the destination blob
    io_pending_ = true;
    engine_->CompactionRemoveFile(dest_file_id_, [this](bool /*success*/) {
        io_pending_ = false;
        state_ = State::kRollbackMarkSealed;
    });
}

void CompactionTask::RollbackMarkSealed() {
    // Restore source file to SEALED state (it can be compacted again later)
    if (src_file_info_) {
        src_file_info_->state = FileState::kSealed;
    }
    if (src_meta_) {
        src_meta_->state = FileState::kSealed;
    }

    // Async update the file header to persist sealed state
    io_pending_ = true;
    WriteFileHeader(src_file_info_, FileState::kSealed, [this](int /*status*/) {
        io_pending_ = false;

        // Clean up remaining data
        migrated_entries_.clear();
        committed_updates_.clear();
        write_buffer_used_ = 0;
        FreeDmaBuffers();

        state_ = State::kFailed;
    });
}

void CompactionTask::FreeDmaBuffers() {
    if (read_buffer_) {
        DmaAllocator::Free(read_buffer_);
        read_buffer_ = nullptr;
    }
    if (write_buffer_) {
        DmaAllocator::Free(write_buffer_);
        write_buffer_ = nullptr;
    }
}

void CompactionTask::WriteFileHeader(FileInfo* file, FileState new_state,
                                     std::function<void(int status)> callback) {
    void* dma_buf = DmaAllocator::AllocZeroed(kPageSize, kPageSize);
    if (!dma_buf) {
        if (callback) {
            callback(-ENOMEM);
        }
        return;
    }

    auto* header = static_cast<DataFileHeader*>(dma_buf);
    header->magic = kDataFileHeaderMagic;
    header->version = 1;
    header->file_id = file->file_id;
    header->state = new_state;
    header->create_time = 0;
    header->checksum = Crc32::Calculate(header, sizeof(*header) - sizeof(header->checksum));

    engine_->SubmitBlobWrite(file, 0, dma_buf, kPageSize,
                             [dma_buf, callback](int status) {
                                 DmaAllocator::Free(dma_buf);
                                 if (callback) {
                                     callback(status);
                                 }
                             });
}

// ============================================================================
// CompactionScheduler implementation
// ============================================================================

CompactionScheduler::CompactionScheduler(Engine* engine)
        : engine_(engine),
          rate_limiter_(kMaxIopsPerSec),
          compaction_paused_(false),
          pending_foreground_count_(0) {}

CompactionScheduler::~CompactionScheduler() {}

void CompactionScheduler::ScheduleCompaction(uint16_t file_id) {
    auto task = std::make_unique<CompactionTask>(file_id, engine_);
    pending_tasks_.push(std::move(task));
}

void CompactionScheduler::Poll() {
    // Priority control
    if (compaction_paused_) {
        if (pending_foreground_count_ <= kResumeThreshold) {
            compaction_paused_ = false;
        } else {
            return;
        }
    } else {
        if (pending_foreground_count_ >= kPauseThreshold) {
            compaction_paused_ = true;
            return;
        }

        // Throttle based on load
        if (pending_foreground_count_ >= kThrottleThreshold) {
            rate_limiter_.SetRate(kMaxIopsPerSec / 4);
        } else if (pending_foreground_count_ >= kResumeThreshold) {
            rate_limiter_.SetRate(kMaxIopsPerSec / 2);
        } else {
            rate_limiter_.SetRate(kMaxIopsPerSec);
        }
    }

    // Rate limiting
    if (!rate_limiter_.Allow()) {
        return;
    }

    // No tasks
    if (pending_tasks_.empty() && !active_task_) {
        return;
    }

    // CPU time tracking
    uint64_t start_cycles = Rdtsc();

    // Get or continue task
    if (!active_task_ && !pending_tasks_.empty()) {
        active_task_ = std::move(pending_tasks_.front());
        pending_tasks_.pop();
    }

    // Execute task steps
    while (active_task_ && !active_task_->IsComplete()) {
        // Check CPU time
        uint64_t elapsed = Rdtsc() - start_cycles;
        if (elapsed > kMaxCyclesPerPoll) {
            break;
        }

        active_task_->Step();
    }

    // Handle completed task
    if (active_task_ && active_task_->IsComplete()) {
        if (active_task_->IsFailed()) {
            // Log failure, maybe reschedule
        }
        active_task_.reset();
    }
}

std::vector<FileMetadata*> CompactionScheduler::SelectFilesForCompaction(double min_garbage_ratio) {
    std::vector<FileMetadata*> candidates;

    const auto& file_metadata = engine_->GetFileMetadataMap();

    for (auto& pair : file_metadata) {
        FileMetadata* file = const_cast<FileMetadata*>(&pair.second);
        if (file->NeedsCompaction() && file->GarbageRatio() >= min_garbage_ratio) {
            candidates.push_back(file);
        }
    }

    // Sort by garbage ratio (highest first)
    std::sort(candidates.begin(), candidates.end(), [](FileMetadata* a, FileMetadata* b) {
        return a->GarbageRatio() > b->GarbageRatio();
    });

    return candidates;
}

}  // namespace spdk_kv
