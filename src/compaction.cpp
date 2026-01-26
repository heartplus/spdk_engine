// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#include "spdk_kv/compaction.h"

#include <chrono>
#include <cstring>

#include "spdk_kv/crc32.h"

namespace spdk_kv {

// ============================================================================
// SparseBitmap implementation
// ============================================================================

SparseBitmap::SparseBitmap(size_t total_bits)
        : total_bits_(total_bits),
          chunk_count_((total_bits + kChunkBits - 1) / kChunkBits) {}

void SparseBitmap::Set(size_t idx) {
    if (idx >= total_bits_) return;

    size_t chunk_idx = idx / kChunkBits;
    size_t bit_idx = idx % kChunkBits;

    auto it = chunks_.find(chunk_idx);
    if (it == chunks_.end()) {
        chunks_[chunk_idx] = std::make_unique<Chunk>();
    }
    chunks_[chunk_idx]->Set(bit_idx);
}

void SparseBitmap::Clear(size_t idx) {
    if (idx >= total_bits_) return;

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
    if (idx >= total_bits_) return false;

    size_t chunk_idx = idx / kChunkBits;
    auto it = chunks_.find(chunk_idx);
    if (it == chunks_.end()) {
        return false;
    }
    return it->second->Test(idx % kChunkBits);
}

size_t SparseBitmap::MemoryUsage() const {
    return chunks_.size() * sizeof(Chunk) + sizeof(*this);
}

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
    if (valid_entries > 0) valid_entries--;
    if (valid_bytes >= bytes) valid_bytes -= bytes;
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
    if (max_iops_ == 0) return true;

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

CompactionTask::CompactionTask(FileMetadata* src_file, MemIndex* mem_index)
        : src_file_(src_file),
          dest_file_(nullptr),
          mem_index_(mem_index),
          state_(State::kInit),
          last_error_(0),
          retry_count_(0),
          retry_start_time_ns_(0),
          current_offset_(sizeof(DataFileHeader)),
          dest_offset_(0),
          entries_processed_(0),
          entries_migrated_(0),
          src_data_(nullptr),
          src_size_(0),
          write_buffer_used_(0),
          io_pending_(false) {
    read_buffer_.resize(kChunkSize);
    write_buffer_.resize(kChunkSize);
}

CompactionTask::~CompactionTask() {}

void CompactionTask::Step() {
    if (io_pending_) return;

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
    // Allocate destination file
    // In real SPDK mode, this would call engine_->allocate_data_file()
    // For simulation, dest_file_ should be set via SetDestFile()

    if (!dest_file_) {
        // Create a new FileMetadata for destination
        // This should be handled by the scheduler in practice
    }

    dest_data_.clear();
    dest_offset_ = sizeof(DataFileHeader);
    migrated_entries_.clear();

    state_ = State::kMarkCompacting;
}

void CompactionTask::MarkCompacting() {
    src_file_->state = FileState::kCompacting;

    // In real SPDK mode, update file header asynchronously
    // For simulation, immediate transition
    state_ = State::kReadChunk;
}

void CompactionTask::SkipInvalidPages() {
    if (!src_file_->valid_bitmap) return;

    // Skip pages that are marked invalid
    while (current_offset_ < src_size_) {
        uint32_t page_idx = static_cast<uint32_t>(current_offset_ / kPageSize);
        if (src_file_->IsPageValid(page_idx)) {
            break;
        }
        current_offset_ += kPageSize;
    }
}

void CompactionTask::ReadNextChunk() {
    // Skip invalid pages
    SkipInvalidPages();

    if (current_offset_ >= src_size_) {
        // All data processed
        if (write_buffer_used_ > 0) {
            state_ = State::kWriteChunk;
        } else {
            state_ = State::kFinalize;
        }
        return;
    }

    size_t read_size = std::min(kChunkSize, src_size_ - current_offset_);

    // In simulation mode, copy from source data
    if (src_data_ && current_offset_ + read_size <= src_size_) {
        std::memcpy(read_buffer_.data(), src_data_ + current_offset_, read_size);
    }

    // Process immediately in simulation
    state_ = State::kProcessEntries;
}

bool CompactionTask::ValidateEntry(const void* entry_data, size_t max_size) {
    if (max_size < sizeof(EntryHeader)) return false;

    const auto* header = static_cast<const EntryHeader*>(entry_data);
    if (header->magic != kEntryMagic) return false;

    // Get value length
    const char* ptr = static_cast<const char*>(entry_data);
    uint32_t value_len =
            *reinterpret_cast<const uint32_t*>(ptr + sizeof(EntryHeader) + sizeof(uint64_t));

    // Calculate entry size
    size_t entry_size =
            AlignUp(sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) + value_len +
                            sizeof(uint32_t),
                    kPageSize);

    if (entry_size > max_size) return false;

    // Validate checksum
    uint32_t stored_checksum = *reinterpret_cast<const uint32_t*>(ptr + entry_size - sizeof(uint32_t));
    uint32_t computed_checksum = Crc32::Calculate(ptr, entry_size - sizeof(uint32_t));

    return stored_checksum == computed_checksum;
}

void CompactionTask::ProcessEntries() {
    const char* ptr = read_buffer_.data();
    size_t offset = 0;
    size_t buffer_size = std::min(kChunkSize, src_size_ - (current_offset_ - kChunkSize));

    while (offset < buffer_size) {
        // Validate entry
        if (!ValidateEntry(ptr + offset, buffer_size - offset)) {
            // Skip to next page
            offset = AlignUp(offset + 1, kPageSize);
            continue;
        }

        const auto* header = reinterpret_cast<const EntryHeader*>(ptr + offset);
        uint64_t key = *reinterpret_cast<const uint64_t*>(ptr + offset + sizeof(EntryHeader));
        uint32_t value_len = *reinterpret_cast<const uint32_t*>(
                ptr + offset + sizeof(EntryHeader) + sizeof(uint64_t));

        size_t entry_size =
                AlignUp(sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) + value_len +
                                sizeof(uint32_t),
                        kPageSize);

        entries_processed_++;

        // Check if this entry is still valid in the current index
        MemIndexEntry* current = mem_index_->Find(key);
        bool should_migrate = false;

        if (current && !current->is_deleted()) {
            // Check if current index points to this entry
            if (current->file_id == src_file_->file_id) {
                uint32_t current_offset_index = static_cast<uint32_t>(current_offset_ / kPageSize);
                if (current->offset_index == current_offset_index + offset / kPageSize) {
                    should_migrate = true;
                }
            }
        }

        if (should_migrate && !(header->flags & kFlagDeleted)) {
            // Check if write buffer has space
            if (write_buffer_used_ + entry_size > write_buffer_.size()) {
                // Write current buffer first
                state_ = State::kWriteChunk;
                return;
            }

            // Copy entry to write buffer
            std::memcpy(write_buffer_.data() + write_buffer_used_, ptr + offset, entry_size);

            // Record migration info
            MigratedEntry migrated;
            migrated.key = key;
            migrated.old_file_id = src_file_->file_id;
            migrated.old_offset_index =
                    static_cast<uint32_t>((current_offset_ - kChunkSize + offset) / kPageSize);
            migrated.new_file_id = dest_file_ ? dest_file_->file_id : 0;
            migrated.new_offset_index = static_cast<uint32_t>((dest_offset_ + write_buffer_used_) / kPageSize);
            migrated.page_count = static_cast<uint16_t>(entry_size / kPageSize);
            migrated.sequence = header->sequence;
            migrated_entries_.push_back(migrated);

            write_buffer_used_ += entry_size;
            entries_migrated_++;
        }

        offset += entry_size;
    }

    current_offset_ += buffer_size;
    state_ = State::kReadChunk;
}

void CompactionTask::WriteChunk() {
    if (write_buffer_used_ == 0) {
        state_ = State::kReadChunk;
        return;
    }

    // In simulation mode, append to dest_data_
    size_t old_size = dest_data_.size();
    dest_data_.resize(old_size + write_buffer_used_);
    std::memcpy(dest_data_.data() + old_size, write_buffer_.data(), write_buffer_used_);

    dest_offset_ += write_buffer_used_;
    write_buffer_used_ = 0;

    // Continue reading
    state_ = State::kReadChunk;
}

void CompactionTask::UpdateIndices() {
    // Update memory indices for migrated entries
    for (const auto& migrated : migrated_entries_) {
        MemIndexEntry* current = mem_index_->Find(migrated.key);
        if (current) {
            // Only update if still pointing to old location
            if (current->file_id == migrated.old_file_id &&
                current->offset_index == migrated.old_offset_index) {
                current->file_id = migrated.new_file_id;
                current->offset_index = migrated.new_offset_index;
            }
        }
    }

    state_ = State::kFinalize;
}

void CompactionTask::Finalize() {
    // Flush any remaining write buffer
    if (write_buffer_used_ > 0) {
        state_ = State::kWriteChunk;
        return;
    }

    // Update indices
    state_ = State::kUpdateIndices;
}

void CompactionTask::MarkDeleted() {
    src_file_->state = FileState::kDeleted;

    // In real SPDK mode, this would:
    // 1. Update file header
    // 2. Free blob

    state_ = State::kDone;
}

void CompactionTask::CheckRetryTimeout() {
    auto now = std::chrono::steady_clock::now();
    uint64_t now_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();

    if (now_ns - retry_start_time_ns_ >= kRetryDelayUs * 1000) {
        retry_count_++;
        if (retry_count_ >= kMaxRetryCount) {
            state_ = State::kRollback;
        } else {
            // Retry the failed operation
            // The specific state to retry depends on what failed
            state_ = State::kReadChunk;
        }
    }
}

void CompactionTask::StartRollback() {
    // Rollback: restore source file to SEALED state
    state_ = State::kRollbackMarkSealed;
}

void CompactionTask::RollbackMarkSealed() {
    src_file_->state = FileState::kSealed;

    // Discard migrated entries
    migrated_entries_.clear();
    dest_data_.clear();

    state_ = State::kFailed;
}

// ============================================================================
// CompactionScheduler implementation
// ============================================================================

CompactionScheduler::CompactionScheduler(MemIndex* mem_index)
        : mem_index_(mem_index),
          rate_limiter_(kMaxIopsPerSec),
          compaction_paused_(false),
          pending_foreground_count_(0),
          file_metadata_(nullptr) {}

CompactionScheduler::~CompactionScheduler() {}

void CompactionScheduler::ScheduleCompaction(FileMetadata* file) {
    auto task = std::make_unique<CompactionTask>(file, mem_index_);
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

    if (!file_metadata_) return candidates;

    for (auto& pair : *file_metadata_) {
        FileMetadata* file = &pair.second;
        if (file->NeedsCompaction() && file->GarbageRatio() >= min_garbage_ratio) {
            candidates.push_back(file);
        }
    }

    // Sort by garbage ratio (highest first)
    std::sort(candidates.begin(), candidates.end(),
              [](FileMetadata* a, FileMetadata* b) { return a->GarbageRatio() > b->GarbageRatio(); });

    return candidates;
}

}  // namespace spdk_kv
