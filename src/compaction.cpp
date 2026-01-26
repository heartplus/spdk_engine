// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#include "compaction.h"
#include "engine.h"
#include <cstring>
#include <ctime>
#include <algorithm>

#ifdef WITH_SPDK
#include <spdk/blob.h>
#endif

namespace spdk_kv {

// RateLimiter implementation
RateLimiter::RateLimiter(uint32_t max_iops)
    : max_iops_(max_iops)
    , current_rate_(max_iops)
    , tokens_(max_iops)
    , last_update_time_(0) {}

bool RateLimiter::allow() {
    update_tokens();
    if (tokens_ > 0) {
        tokens_--;
        return true;
    }
    return false;
}

void RateLimiter::set_rate(uint32_t rate) {
    current_rate_ = rate;
}

void RateLimiter::update_tokens() {
    uint64_t now = get_current_time_us();
    uint64_t elapsed = now - last_update_time_;

    if (elapsed >= 1000000) {  // 1 second
        tokens_ = current_rate_;
        last_update_time_ = now;
    } else {
        // Add proportional tokens
        uint32_t new_tokens = static_cast<uint32_t>((elapsed * current_rate_) / 1000000);
        tokens_ = std::min(tokens_ + new_tokens, current_rate_);
    }
}

uint64_t RateLimiter::get_current_time_us() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}

// CompactionTask implementation
CompactionTask::CompactionTask(FileInfo* source_file, Engine* engine)
    : source_file_(source_file)
    , engine_(engine)
    , state_(CompactionTaskState::INIT)
    , current_read_offset_(sizeof(DataFileHeader))
    , total_bytes_to_read_(0)
    , read_buffer_(nullptr)
    , target_file_(nullptr)
    , write_offset_(0)
    , write_buffer_(nullptr)
    , write_buffer_used_(0)
    , retry_count_(0)
    , retry_start_time_(0)
    , last_error_(0) {}

CompactionTask::~CompactionTask() {
    if (read_buffer_) {
        engine_->free_buffer(read_buffer_);
    }
    if (write_buffer_) {
        engine_->free_buffer(write_buffer_);
    }
}

void CompactionTask::step() {
    switch (state_) {
    case CompactionTaskState::INIT:
        init();
        break;
    case CompactionTaskState::MARK_COMPACTING:
        mark_compacting();
        break;
    case CompactionTaskState::READ_CHUNK:
        read_next_chunk();
        break;
    case CompactionTaskState::PROCESS_ENTRIES:
        process_entries();
        break;
    case CompactionTaskState::WRITE_CHUNK:
        write_chunk();
        break;
    case CompactionTaskState::UPDATE_INDICES:
        update_indices();
        break;
    case CompactionTaskState::FINALIZE:
        finalize();
        break;
    case CompactionTaskState::MARK_DELETED:
        mark_deleted();
        break;
    case CompactionTaskState::ROLLBACK:
        start_rollback();
        break;
    case CompactionTaskState::ROLLBACK_MARK_SEALED:
        complete_rollback();
        break;
    default:
        break;
    }
}

bool CompactionTask::is_complete() const {
    return state_ == CompactionTaskState::DONE ||
           state_ == CompactionTaskState::FAILED;
}

bool CompactionTask::is_failed() const {
    return state_ == CompactionTaskState::FAILED;
}

void CompactionTask::init() {
    // Allocate destination file
    target_file_ = engine_->allocate_data_file();
    if (!target_file_) {
        state_ = CompactionTaskState::FAILED;
        last_error_ = -ENOSPC;
        return;
    }

    // Allocate read/write buffers
    read_buffer_ = engine_->alloc_buffer(CHUNK_SIZE);
    write_buffer_ = engine_->alloc_buffer(CHUNK_SIZE);

    if (!read_buffer_ || !write_buffer_) {
        state_ = CompactionTaskState::FAILED;
        last_error_ = -ENOMEM;
        return;
    }

    total_bytes_to_read_ = source_file_->size;
    state_ = CompactionTaskState::MARK_COMPACTING;
}

void CompactionTask::mark_compacting() {
    source_file_->state = FileState::COMPACTING;
    state_ = CompactionTaskState::READ_CHUNK;
}

void CompactionTask::read_next_chunk() {
    if (current_read_offset_ >= total_bytes_to_read_) {
        if (write_buffer_used_ > 0) {
            state_ = CompactionTaskState::WRITE_CHUNK;
        } else {
            state_ = CompactionTaskState::FINALIZE;
        }
        return;
    }

#ifdef WITH_SPDK
    size_t read_size = std::min(CHUNK_SIZE, total_bytes_to_read_ - current_read_offset_);
    // Async read from source blob
    spdk_blob_io_read(source_file_->blob,
                      engine_->io_channel(),
                      read_buffer_,
                      (source_file_->header.total_bytes + current_read_offset_) / ALIGNMENT,
                      read_size / ALIGNMENT,
                      [](void* arg, int bserrno) {
                          auto* task = static_cast<CompactionTask*>(arg);
                          task->on_read_complete(bserrno);
                      },
                      this);
#else
    // Simulate read completion
    on_read_complete(0);
#endif
}

void CompactionTask::process_entries() {
    // Process entries from read buffer
    size_t offset = 0;
    size_t buffer_end = std::min(CHUNK_SIZE, total_bytes_to_read_ - current_read_offset_ + CHUNK_SIZE);

    while (offset < buffer_end) {
        auto* header = reinterpret_cast<EntryHeader*>(
            static_cast<char*>(read_buffer_) + offset);

        if (header->magic != ENTRY_MAGIC) {
            break;
        }

        uint64_t key = *reinterpret_cast<uint64_t*>(
            static_cast<char*>(read_buffer_) + offset + sizeof(EntryHeader));
        uint32_t value_len = *reinterpret_cast<uint32_t*>(
            static_cast<char*>(read_buffer_) + offset + sizeof(EntryHeader) + sizeof(uint64_t));

        size_t entry_size = ALIGN_UP(
            sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) + value_len + sizeof(uint32_t),
            ALIGNMENT);

        // Verify entry is still valid in index
        auto* index_entry = engine_->mem_index()->find(key);
        bool is_valid = index_entry &&
                       index_entry->file_id == source_file_->file_id &&
                       index_entry->offset_index == (current_read_offset_ + offset) / ALIGNMENT;

        if (is_valid && !(header->flags & FLAG_DELETED)) {
            // Check if write buffer has space
            if (write_buffer_used_ + entry_size > CHUNK_SIZE) {
                state_ = CompactionTaskState::WRITE_CHUNK;
                return;
            }

            // Copy to write buffer
            memcpy(static_cast<char*>(write_buffer_) + write_buffer_used_,
                   static_cast<char*>(read_buffer_) + offset,
                   entry_size);

            // Record pending index update
            pending_index_updates_.push_back({
                key,
                source_file_->file_id,
                static_cast<uint32_t>((current_read_offset_ + offset) / ALIGNMENT),
                target_file_->file_id,
                static_cast<uint32_t>((write_offset_ + write_buffer_used_) / ALIGNMENT),
                static_cast<uint16_t>((entry_size + ALIGNMENT - 1) / ALIGNMENT)
            });

            write_buffer_used_ += entry_size;
        }

        offset += entry_size;
    }

    current_read_offset_ += offset;
    state_ = CompactionTaskState::READ_CHUNK;
}

void CompactionTask::write_chunk() {
    if (write_buffer_used_ == 0) {
        state_ = CompactionTaskState::READ_CHUNK;
        return;
    }

#ifdef WITH_SPDK
    // Async write to target blob
    spdk_blob_io_write(target_file_->blob,
                       engine_->io_channel(),
                       write_buffer_,
                       write_offset_ / ALIGNMENT,
                       write_buffer_used_ / ALIGNMENT,
                       [](void* arg, int bserrno) {
                           auto* task = static_cast<CompactionTask*>(arg);
                           task->on_write_complete(bserrno);
                       },
                       this);
#else
    // Simulate write completion
    on_write_complete(0);
#endif
}

void CompactionTask::update_indices() {
    for (auto& update : pending_index_updates_) {
        auto* existing = engine_->mem_index()->find(update.key);

        if (existing &&
            existing->file_id == update.old_file_id &&
            existing->offset_index == update.old_offset_index) {

            MemIndexEntry new_entry = *existing;
            new_entry.file_id = update.new_file_id;
            new_entry.offset_index = update.new_offset_index;
            new_entry.page_count = update.page_count;

            engine_->mem_index()->upsert(update.key, new_entry);
        }
    }

    pending_index_updates_.clear();
    write_offset_ += write_buffer_used_;
    write_buffer_used_ = 0;

    state_ = CompactionTaskState::READ_CHUNK;
}

void CompactionTask::finalize() {
    if (write_buffer_used_ > 0) {
        state_ = CompactionTaskState::WRITE_CHUNK;
        return;
    }

    state_ = CompactionTaskState::MARK_DELETED;
}

void CompactionTask::mark_deleted() {
    source_file_->state = FileState::DELETED;

    // Free buffers
    if (read_buffer_) {
        engine_->free_buffer(read_buffer_);
        read_buffer_ = nullptr;
    }
    if (write_buffer_) {
        engine_->free_buffer(write_buffer_);
        write_buffer_ = nullptr;
    }

    state_ = CompactionTaskState::DONE;
}

void CompactionTask::handle_error(int status) {
    retry_count_++;
    last_error_ = status;

    if (retry_count_ < MAX_RETRIES &&
        (status == -EAGAIN || status == -EBUSY || status == -ENOMEM)) {
        retry_start_time_ = RateLimiter::get_current_time_us();
        state_ = CompactionTaskState::RETRY_WAIT;
    } else {
        state_ = CompactionTaskState::ROLLBACK;
    }
}

void CompactionTask::start_rollback() {
    // Revert committed index updates
    for (auto& update : pending_index_updates_) {
        auto* existing = engine_->mem_index()->find(update.key);

        if (existing &&
            existing->file_id == update.new_file_id &&
            existing->offset_index == update.new_offset_index) {

            MemIndexEntry old_entry = *existing;
            old_entry.file_id = update.old_file_id;
            old_entry.offset_index = update.old_offset_index;
            old_entry.page_count = update.page_count;

            engine_->mem_index()->upsert(update.key, old_entry);
        }
    }
    pending_index_updates_.clear();

    state_ = CompactionTaskState::ROLLBACK_MARK_SEALED;
}

void CompactionTask::complete_rollback() {
    source_file_->state = FileState::SEALED;

    if (read_buffer_) {
        engine_->free_buffer(read_buffer_);
        read_buffer_ = nullptr;
    }
    if (write_buffer_) {
        engine_->free_buffer(write_buffer_);
        write_buffer_ = nullptr;
    }

    state_ = CompactionTaskState::FAILED;
}

void CompactionTask::on_read_complete(int status) {
    if (status != 0) {
        handle_error(status);
        return;
    }
    state_ = CompactionTaskState::PROCESS_ENTRIES;
}

void CompactionTask::on_write_complete(int status) {
    if (status != 0) {
        handle_error(status);
        return;
    }
    state_ = CompactionTaskState::UPDATE_INDICES;
}

void CompactionTask::read_complete_cb(void* arg, int status) {
    auto* task = static_cast<CompactionTask*>(arg);
    task->on_read_complete(status);
}

void CompactionTask::write_complete_cb(void* arg, int status) {
    auto* task = static_cast<CompactionTask*>(arg);
    task->on_write_complete(status);
}

// CompactionScheduler implementation
CompactionScheduler::CompactionScheduler(Engine* engine)
    : engine_(engine)
    , rate_limiter_(MAX_IOPS_PER_SEC)
    , compaction_paused_(false) {}

void CompactionScheduler::schedule_compaction(FileInfo* file) {
    auto task = std::make_unique<CompactionTask>(file, engine_);
    pending_tasks_.push(std::move(task));
}

void CompactionScheduler::poll() {
    size_t pending_requests = engine_->pending_foreground_count();

    // Priority control
    if (compaction_paused_) {
        if (pending_requests <= RESUME_THRESHOLD) {
            compaction_paused_ = false;
        } else {
            return;
        }
    } else {
        if (pending_requests >= PAUSE_THRESHOLD) {
            compaction_paused_ = true;
            return;
        }

        // Adjust rate based on load
        if (pending_requests >= THROTTLE_THRESHOLD) {
            rate_limiter_.set_rate(MAX_IOPS_PER_SEC / 4);
        } else if (pending_requests >= RESUME_THRESHOLD) {
            rate_limiter_.set_rate(MAX_IOPS_PER_SEC / 2);
        } else {
            rate_limiter_.set_rate(MAX_IOPS_PER_SEC);
        }
    }

    // Rate limit check
    if (!rate_limiter_.allow()) {
        return;
    }

    // No tasks
    if (pending_tasks_.empty() && !active_task_) {
        return;
    }

    // Get task
    if (!active_task_ && !pending_tasks_.empty()) {
        active_task_ = std::move(pending_tasks_.front());
        pending_tasks_.pop();
    }

    // Execute task step
    uint64_t start_cycles = rdtsc();
    while (active_task_ && !active_task_->is_complete()) {
        if (engine_->has_pending_foreground_requests()) {
            break;
        }

        uint64_t elapsed = rdtsc() - start_cycles;
        if (elapsed > MAX_CYCLES_PER_POLL) {
            break;
        }

        active_task_->step();
    }

    // Task complete
    if (active_task_ && active_task_->is_complete()) {
        active_task_.reset();
    }
}

}  // namespace spdk_kv
