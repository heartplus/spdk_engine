// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <queue>
#include <unordered_map>
#include <vector>

#include "spdk_kv/entry.h"
#include "spdk_kv/mem_index.h"
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

    // Set bit at index
    void Set(size_t idx);

    // Clear bit at index
    void Clear(size_t idx);

    // Test bit at index
    bool Test(size_t idx) const;

    // Get memory usage
    size_t MemoryUsage() const;

    // Get count of set bits (approximate)
    size_t PopCount() const;

private:
    struct Chunk {
        uint64_t data[kChunkBits / 64] = {0};

        void Set(size_t idx) { data[idx / 64] |= (1ULL << (idx % 64)); }
        void Clear(size_t idx) { data[idx / 64] &= ~(1ULL << (idx % 64)); }
        bool Test(size_t idx) const { return (data[idx / 64] & (1ULL << (idx % 64))) != 0; }
        bool IsEmpty() const {
            for (auto d : data) {
                if (d) return false;
            }
            return true;
        }
        size_t PopCount() const {
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

    // Calculate garbage ratio
    double GarbageRatio() const {
        if (total_bytes == 0) return 0.0;
        return 1.0 - static_cast<double>(valid_bytes) / total_bytes;
    }

    // Check if compaction is needed
    bool NeedsCompaction() const {
        return state == FileState::kSealed && GarbageRatio() >= kBitmapCreationThreshold;
    }

    // Create bitmap if needed
    void MaybeCreateBitmap();

    // Mark pages as valid
    void MarkValid(uint32_t offset_index, uint16_t page_count, uint32_t bytes);

    // Mark pages as invalid (garbage)
    void MarkInvalid(uint32_t offset_index, uint16_t page_count, uint32_t bytes);

    // Check if a page is valid
    bool IsPageValid(uint32_t offset_index) const;

    // Get bitmap memory usage
    size_t BitmapMemoryUsage() const { return valid_bitmap ? valid_bitmap->MemoryUsage() : 0; }
};

// Rate limiter for compaction IO
class RateLimiter {
public:
    explicit RateLimiter(uint32_t max_iops);

    // Set rate limit
    void SetRate(uint32_t iops);

    // Check if operation is allowed
    bool Allow();

    // Reset state
    void Reset();

private:
    uint32_t max_iops_;
    uint64_t tokens_;
    uint64_t last_update_ns_;
    uint64_t token_interval_ns_;
};

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

    CompactionTask(FileMetadata* src_file, MemIndex* mem_index);
    ~CompactionTask();

    // Execute one step (non-blocking in SPDK mode)
    void Step();

    // Check if task is complete
    bool IsComplete() const { return state_ == State::kDone || state_ == State::kFailed; }

    // Check if task failed
    bool IsFailed() const { return state_ == State::kFailed; }

    // Get last error
    int GetLastError() const { return last_error_; }

    // Get source file
    FileMetadata* GetSourceFile() { return src_file_; }

    // For simulation mode: set destination file
    void SetDestFile(FileMetadata* dest_file) { dest_file_ = dest_file; }

    // For simulation mode: set source data
    void SetSourceData(const char* data, size_t size) {
        src_data_ = data;
        src_size_ = size;
    }

    // For simulation mode: get destination data
    const std::vector<char>& GetDestData() const { return dest_data_; }

    // Get migrated entries info (for index update)
    struct MigratedEntry {
        uint64_t key;
        uint16_t old_file_id;
        uint32_t old_offset_index;
        uint16_t new_file_id;
        uint32_t new_offset_index;
        uint16_t page_count;
        uint32_t sequence;
    };
    const std::vector<MigratedEntry>& GetMigratedEntries() const { return migrated_entries_; }

private:
    // State handlers
    void Init();
    void MarkCompacting();
    void ReadNextChunk();
    void ProcessEntries();
    void WriteChunk();
    void UpdateIndices();
    void Finalize();
    void MarkDeleted();
    void CheckRetryTimeout();
    void StartRollback();
    void RollbackMarkSealed();

    // Skip invalid pages using bitmap
    void SkipInvalidPages();

    // Validate entry
    bool ValidateEntry(const void* entry_data, size_t max_size);

    // Members
    FileMetadata* src_file_;
    FileMetadata* dest_file_;
    MemIndex* mem_index_;
    State state_;
    int last_error_;
    int retry_count_;
    uint64_t retry_start_time_ns_;

    // Progress tracking
    uint64_t current_offset_;
    uint64_t dest_offset_;
    size_t entries_processed_;
    size_t entries_migrated_;

    // Migrated entries for index update
    std::vector<MigratedEntry> migrated_entries_;

    // Simulation mode data
    const char* src_data_;
    size_t src_size_;
    std::vector<char> dest_data_;
    std::vector<char> read_buffer_;
    std::vector<char> write_buffer_;
    size_t write_buffer_used_;
    bool io_pending_;
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

    CompactionScheduler(MemIndex* mem_index);
    ~CompactionScheduler();

    // Non-copyable
    CompactionScheduler(const CompactionScheduler&) = delete;
    CompactionScheduler& operator=(const CompactionScheduler&) = delete;

    // Schedule a file for compaction
    void ScheduleCompaction(FileMetadata* file);

    // Poll compaction progress (call in main loop)
    void Poll();

    // Set pending foreground request count (for priority control)
    void SetPendingForegroundCount(size_t count) { pending_foreground_count_ = count; }

    // Check if compaction is paused
    bool IsPaused() const { return compaction_paused_; }

    // Get number of pending tasks
    size_t PendingTaskCount() const { return pending_tasks_.size(); }

    // Check if any task is active
    bool HasActiveTask() const { return active_task_ != nullptr; }

    // For simulation: set file metadata map
    void SetFileMetadata(std::unordered_map<uint16_t, FileMetadata>* file_metadata) {
        file_metadata_ = file_metadata;
    }

    // Select files for compaction based on garbage ratio
    std::vector<FileMetadata*> SelectFilesForCompaction(double min_garbage_ratio = 0.3);

private:
    static inline uint64_t Rdtsc() {
#if defined(__x86_64__) || defined(__i386__)
        uint32_t lo, hi;
        __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi));
        return (static_cast<uint64_t>(hi) << 32) | lo;
#else
        // Fallback for non-x86 architectures
        return 0;
#endif
    }

    MemIndex* mem_index_;
    RateLimiter rate_limiter_;
    std::queue<std::unique_ptr<CompactionTask>> pending_tasks_;
    std::unique_ptr<CompactionTask> active_task_;
    bool compaction_paused_;
    size_t pending_foreground_count_;

    // File metadata reference (for simulation)
    std::unordered_map<uint16_t, FileMetadata>* file_metadata_;
};

}  // namespace spdk_kv
