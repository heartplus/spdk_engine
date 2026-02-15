// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <cerrno>
#include <cstdint>
#include <functional>
#include <memory>
#include <queue>
#include <unordered_map>
#include <vector>

#include "spdk_kv/entry.h"
#include "spdk_kv/mem_index.h"
#include "spdk_kv/spdk_env.h"
#include "spdk_kv/types.h"

namespace spdk_kv {

// Forward declarations
class Engine;

// Sparse bitmap using Roaring Bitmap concept
class SparseBitmap {
public:
    static constexpr size_t kChunkBits = 32768;  // 4KB chunk, 32768 bits
    static constexpr size_t kBytesPerChunk = kChunkBits / 8;

    explicit SparseBitmap(size_t total_bits);
    ~SparseBitmap() = default;

    // set bit at index
    void set(size_t idx);

    // clear bit at index
    void clear(size_t idx);

    // test bit at index
    bool test(size_t idx) const;

    // get memory usage
    size_t memory_usage() const;

    // get count of set bits (approximate)
    size_t pop_count() const;

private:
    struct Chunk {
        uint64_t data[kChunkBits / 64] = {0};

        void set(size_t idx) { data[idx / 64] |= (1ULL << (idx % 64)); }
        void clear(size_t idx) { data[idx / 64] &= ~(1ULL << (idx % 64)); }
        bool test(size_t idx) const { return (data[idx / 64] & (1ULL << (idx % 64))) != 0; }
        bool is_empty() const {
            for (auto d : data) {
                if (d) {
                    return false;
                }
            }
            return true;
        }
        size_t pop_count() const {
            size_t count = 0;
            for (auto d : data) {
                count += __builtin_popcountll(d);
            }
            return count;
        }
    };

    size_t total_bits_;
    size_t chunk_count_;
    std::unordered_map<size_t, std::unique_ptr<Chunk>> chunks_;
};

// File metadata with garbage tracking
struct FileMetadata {
    uint16_t file_id;
    FileState state;
    uint64_t total_entries;
    uint64_t valid_entries;
    uint64_t total_bytes;
    uint64_t valid_bytes;

    // Bitmap creation threshold (garbage ratio)
    static constexpr double kBitmapCreationThreshold = 0.3;

    // Lazy-loaded bitmap (nullptr if not needed)
    std::unique_ptr<SparseBitmap> valid_bitmap;

    FileMetadata()
            : file_id(0),
              state(FileState::kActive),
              total_entries(0),
              valid_entries(0),
              total_bytes(0),
              valid_bytes(0) {}

    // calculate garbage ratio
    double garbage_ratio() const {
        if (total_bytes == 0) {
            return 0.0;
        }
        return 1.0 - static_cast<double>(valid_bytes) / total_bytes;
    }

    // Check if compaction is needed
    bool needs_compaction() const {
        return state == FileState::kSealed && garbage_ratio() >= kBitmapCreationThreshold;
    }

    // create bitmap if needed
    void maybe_create_bitmap();

    // Mark pages as valid
    void mark_valid(uint32_t offset_index, uint16_t page_count, uint32_t bytes);

    // Mark pages as invalid (garbage)
    void mark_invalid(uint32_t offset_index, uint16_t page_count, uint32_t bytes);

    // Check if a page is valid
    bool is_page_valid(uint32_t offset_index) const;

    // get bitmap memory usage
    size_t bitmap_memory_usage() const { return valid_bitmap ? valid_bitmap->memory_usage() : 0; }
};

// Rate limiter for compaction IO
class RateLimiter {
public:
    explicit RateLimiter(uint32_t max_iops);

    // set rate limit
    void set_rate(uint32_t iops);

    // Check if operation is allowed
    bool allow();

    // reset state
    void reset();

private:
    uint32_t max_iops_;
    uint64_t tokens_;
    uint64_t last_update_ns_;
    uint64_t token_interval_ns_;
};

// Forward declarations
struct FileInfo;

// Compaction task
class CompactionTask {
public:
    // Task state machine
    enum class State {
        kInit,
        kMarkCompacting,
        kWaitMarkComplete,
        kReadChunk,
        kWaitReadComplete,
        kProcessEntries,
        kWriteChunk,
        kWaitWriteComplete,
        kUpdateIndices,
        kFinalize,
        kMarkDeleted,
        kWaitDeleteComplete,
        kRetryWait,
        kRollback,
        kRollbackMarkSealed,
        kWaitRollbackComplete,
        kFailed,
        kDone
    };

    // Retry configuration
    static constexpr int kMaxRetryCount = 3;
    static constexpr uint64_t kRetryDelayUs = 1000;    // 1ms
    static constexpr size_t kChunkSize = 1024 * 1024;  // 1MB

    CompactionTask(uint16_t src_file_id, Engine* engine);
    ~CompactionTask();

    // Execute one step (non-blocking)
    void step();

    // Check if task is complete
    bool is_complete() const { return state_ == State::kDone || state_ == State::kFailed; }

    // Check if task failed
    bool is_failed() const { return state_ == State::kFailed; }

    // get last error
    int get_last_error() const { return last_error_; }

    // get source file ID
    uint16_t get_source_file_id() const { return src_file_id_; }

    // get migrated entries info (for index update)
    struct MigratedEntry {
        uint64_t key;
        uint16_t old_file_id;
        uint32_t old_offset_index;
        uint16_t new_file_id;
        uint32_t new_offset_index;
        uint16_t page_count;
        uint32_t sequence;
    };
    const std::vector<MigratedEntry>& get_migrated_entries() const { return migrated_entries_; }

private:
    // State handlers
    void init();
    void mark_compacting();
    void read_next_chunk();
    void process_entries();
    void write_chunk();
    void update_indices();
    void finalize();
    void mark_deleted();
    void check_retry_timeout();
    void start_rollback();
    void rollback_mark_sealed();

    // Skip invalid pages using bitmap
    void skip_invalid_pages();

    // Validate entry
    bool validate_entry(const void* entry_data, size_t max_size);

    // IO error handling with retry support
    void handle_io_error(State retry_state, int error_code);

    // Check if an error code is retryable
    static bool is_retryable_error(int error_code);

    // Revert a single committed index update during rollback
    void revert_index_update(const MigratedEntry& update);

    // del partially-written garbage destination file during rollback
    void delete_garbage_file();

    // free DMA read/write buffers
    void free_dma_buffers();

    // write file header to blob at offset 0
    void write_file_header(FileInfo* file, FileState new_state,
                         std::function<void(int status)> callback);

    // Members
    Engine* engine_;
    uint16_t src_file_id_;
    uint16_t dest_file_id_;
    FileInfo* src_file_info_;
    FileInfo* dest_file_info_;
    FileMetadata* src_meta_;
    FileMetadata* dest_meta_;
    State state_;
    int last_error_;
    int retry_count_;
    uint64_t retry_start_time_ns_;

    // Progress tracking
    uint64_t current_offset_;
    uint64_t dest_offset_;
    size_t entries_processed_;
    size_t entries_migrated_;

    // Migrated entries for index update (current batch, pending write)
    std::vector<MigratedEntry> migrated_entries_;

    // Committed updates (already applied to index, needed for rollback)
    std::vector<MigratedEntry> committed_updates_;

    // DMA buffers for IO
    void* read_buffer_;
    void* write_buffer_;
    size_t write_buffer_used_;
    size_t bytes_read_;
    bool io_pending_;

    // Retry state tracking
    State retry_target_state_;
};

// Compaction scheduler
class CompactionScheduler {
public:
    // Configuration
    static constexpr size_t kUnitSize = 1024 * 1024;      // 1MB per unit
    static constexpr uint64_t kMaxCyclesPerPoll = 50000;  // ~25us @2GHz
    static constexpr uint32_t kMaxIopsPerSec = 1000;

    // Priority control thresholds
    static constexpr size_t kPauseThreshold = 1000;    // Pause when > 1000 pending
    static constexpr size_t kResumeThreshold = 100;    // Resume when < 100 pending
    static constexpr size_t kThrottleThreshold = 500;  // Throttle when > 500 pending

    explicit CompactionScheduler(Engine* engine);
    ~CompactionScheduler();

    // Non-copyable
    CompactionScheduler(const CompactionScheduler&) = delete;
    CompactionScheduler& operator=(const CompactionScheduler&) = delete;

    // Schedule a file for compaction
    void schedule_compaction(uint16_t file_id);

    // poll compaction progress (call in main loop)
    void poll();

    // set pending foreground request count (for priority control)
    void set_pending_foreground_count(size_t count) { pending_foreground_count_ = count; }

    // Check if compaction is paused
    bool is_paused() const { return compaction_paused_; }

    // get number of pending tasks
    size_t pending_task_count() const { return pending_tasks_.size(); }

    // Check if any task is active
    bool has_active_task() const { return active_task_ != nullptr; }

    // Select files for compaction based on garbage ratio
    std::vector<FileMetadata*> select_files_for_compaction(double min_garbage_ratio = 0.3);

private:
    static inline uint64_t rdtsc() {
#if defined(__x86_64__) || defined(__i386__)
        uint32_t lo, hi;
        __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi));
        return (static_cast<uint64_t>(hi) << 32) | lo;
#else
        // Fallback for non-x86 architectures
        return 0;
#endif
    }

    Engine* engine_;
    RateLimiter rate_limiter_;
    std::queue<std::unique_ptr<CompactionTask>> pending_tasks_;
    std::unique_ptr<CompactionTask> active_task_;
    bool compaction_paused_;
    size_t pending_foreground_count_;
};

}  // namespace spdk_kv
