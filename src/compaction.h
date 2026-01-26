// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#pragma once

#include <spdk_kv/types.h>
#include <cstdint>
#include <vector>
#include <queue>
#include <memory>

namespace spdk_kv {

// Forward declarations
class Engine;
struct FileInfo;

// Compaction task state
enum class CompactionTaskState {
    INIT,
    MARK_COMPACTING,
    WAIT_MARK_COMPLETE,
    READ_CHUNK,
    WAIT_READ_COMPLETE,
    PROCESS_ENTRIES,
    WRITE_CHUNK,
    WAIT_WRITE_COMPLETE,
    UPDATE_INDICES,
    FINALIZE,
    MARK_DELETED,
    WAIT_DELETE_COMPLETE,
    RETRY_WAIT,
    ROLLBACK,
    ROLLBACK_MARK_SEALED,
    WAIT_ROLLBACK_COMPLETE,
    FAILED,
    DONE
};

// Index update record for compaction
struct IndexUpdate {
    uint64_t key;
    uint16_t old_file_id;
    uint32_t old_offset_index;
    uint16_t new_file_id;
    uint32_t new_offset_index;
    uint16_t page_count;
};

// Rate limiter for compaction IO
class RateLimiter {
public:
    RateLimiter(uint32_t max_iops);

    bool allow();
    void set_rate(uint32_t rate);
    static uint64_t get_current_time_us();

private:
    void update_tokens();

    uint32_t max_iops_;
    uint32_t current_rate_;
    uint32_t tokens_;
    uint64_t last_update_time_;
};

// Compaction task
class CompactionTask {
public:
    static constexpr size_t CHUNK_SIZE = 1 * 1024 * 1024;  // 1MB
    static constexpr int MAX_RETRIES = 3;
    static constexpr uint64_t RETRY_DELAY_US = 100000;  // 100ms

    CompactionTask(FileInfo* source_file, Engine* engine);
    ~CompactionTask();

    void step();
    bool is_complete() const;
    bool is_failed() const;

private:
    void init();
    void mark_compacting();
    void read_next_chunk();
    void process_entries();
    void write_chunk();
    void update_indices();
    void finalize();
    void mark_deleted();
    void handle_error(int status);
    void start_rollback();
    void complete_rollback();

    void on_read_complete(int status);
    void on_write_complete(int status);

    static void read_complete_cb(void* arg, int status);
    static void write_complete_cb(void* arg, int status);

    FileInfo* source_file_;
    Engine* engine_;
    CompactionTaskState state_;

    // Read state
    uint64_t current_read_offset_;
    uint64_t total_bytes_to_read_;
    void* read_buffer_;

    // Write state
    FileInfo* target_file_;
    uint64_t write_offset_;
    void* write_buffer_;
    size_t write_buffer_used_;

    // Index updates
    std::vector<IndexUpdate> pending_index_updates_;

    // Retry state
    int retry_count_;
    uint64_t retry_start_time_;
    int last_error_;
};

// Compaction scheduler
class CompactionScheduler {
public:
    static constexpr size_t UNIT_SIZE = 1 * 1024 * 1024;
    static constexpr uint64_t MAX_CYCLES_PER_POLL = 50000;
    static constexpr uint32_t MAX_IOPS_PER_SEC = 1000;

    static constexpr size_t PAUSE_THRESHOLD = 1000;
    static constexpr size_t RESUME_THRESHOLD = 100;
    static constexpr size_t THROTTLE_THRESHOLD = 500;

    CompactionScheduler(Engine* engine);
    ~CompactionScheduler() = default;

    void schedule_compaction(FileInfo* file);
    void poll();

private:
    static inline uint64_t rdtsc() {
        uint32_t lo, hi;
        __asm__ volatile ("rdtsc" : "=a" (lo), "=d" (hi));
        return (static_cast<uint64_t>(hi) << 32) | lo;
    }

    Engine* engine_;
    RateLimiter rate_limiter_;
    std::queue<std::unique_ptr<CompactionTask>> pending_tasks_;
    std::unique_ptr<CompactionTask> active_task_;
    bool compaction_paused_;
};

}  // namespace spdk_kv
