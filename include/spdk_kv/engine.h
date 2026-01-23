// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "spdk_kv/append_buffer.h"
#include "spdk_kv/crc32.h"
#include "spdk_kv/entry.h"
#include "spdk_kv/mem_index.h"
#include "spdk_kv/types.h"

namespace spdk_kv {

// Engine configuration
struct EngineConfig {
    // Capacity config
    uint64_t max_capacity = kMaxCapacity;        // 8TB
    uint64_t data_file_size = kDefaultFileSize;  // 8GB

    // Index config
    uint64_t max_entries = 2000000000ULL;           // 2 billion
    double index_load_factor = kDefaultLoadFactor;  // 0.55

    // IO config
    size_t append_buffer_size = kAppendBufferSize;  // 2MB
    size_t append_buffer_count = kMinBufferCount;   // 4

    // Checkpoint config
    uint64_t checkpoint_interval_ns = 300ULL * 1000 * 1000 * 1000;     // 300s
    uint64_t checkpoint_bytes_threshold = 10ULL * 1024 * 1024 * 1024;  // 10GB
    uint32_t checkpoint_dirty_segment_threshold = 8;
};

// Create options
struct CreateOpts {
    EngineConfig config;
    bool force = false;  // Force create even if exists
};

// Open options
struct OpenOpts {
    bool read_only = false;
    bool recover = true;
};

// File info (runtime)
struct FileInfo {
    uint16_t file_id;
    uint64_t blob_id;  // In simulation mode, this is just a local ID
    FileState state;
    uint64_t size;
    uint64_t write_offset;
    std::vector<char> data;  // In-memory storage for simulation

    bool IsWritable() const { return state == FileState::kActive; }
    bool IsReadable() const {
        return state == FileState::kActive || state == FileState::kSealed ||
               state == FileState::kCompacting;
    }
};

// KV Engine class (simulation mode - no actual SPDK)
class Engine {
public:
    Engine();
    ~Engine();

    // Non-copyable
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    // Lifecycle operations (synchronous for simulation)
    KvError Create(const std::string& path, const CreateOpts& opts);
    KvError Open(const std::string& path, const OpenOpts& opts);
    KvError Close();

    // KV operations (synchronous for simulation)
    KvError Put(uint64_t key, const void* value, uint32_t value_len);
    KvError Get(uint64_t key, void* value_buf, uint32_t buf_len, uint32_t* actual_len);
    KvError Delete(uint64_t key);

    // Async-style operations (callbacks called immediately in simulation)
    void PutAsync(uint64_t key, const void* value, uint32_t value_len, KvCallback cb, void* cb_arg);
    void GetAsync(uint64_t key, void* value_buf, uint32_t buf_len, KvGetCallback cb, void* cb_arg);
    void DeleteAsync(uint64_t key, KvCallback cb, void* cb_arg);

    // Polling (no-op in simulation, but provided for API compatibility)
    void Poll();

    // Status
    EngineState GetState() const { return state_; }
    bool IsReady() const { return state_ == EngineState::kReady; }

    // Statistics
    uint64_t GetEntryCount() const;
    uint64_t GetTotalDataBytes() const;
    double GetIndexLoadFactor() const;

    // For internal/testing use
    MemIndex* GetMemIndex() { return mem_index_.get(); }
    const MemIndex* GetMemIndex() const { return mem_index_.get(); }

private:
    // Internal helpers
    KvError InitializeNew(const CreateOpts& opts);
    KvError LoadExisting(const OpenOpts& opts);
    KvError Recover();

    // File management
    FileInfo* GetActiveFile();
    FileInfo* GetFile(uint16_t file_id);
    FileInfo* AllocateNewFile();

    // Entry building
    void BuildEntryInplace(void* slot, uint64_t key, const void* value, uint32_t len, uint32_t seq,
                           bool is_tombstone = false);

    // Index update
    void UpdateIndexOnWriteComplete(uint64_t key, const MemIndexEntry& entry, bool is_compaction,
                                    uint16_t old_file_id, uint32_t old_offset_index);

    // State
    EngineState state_;
    std::string path_;
    EngineConfig config_;

    // Index
    std::unique_ptr<MemIndex> mem_index_;

    // Append buffer
    std::unique_ptr<AppendBufferManager> buffer_manager_;

    // File management
    std::vector<std::unique_ptr<FileInfo>> files_;
    uint16_t active_file_id_;
    uint16_t next_file_id_;

    // Statistics
    uint64_t total_data_bytes_;
    uint64_t total_garbage_bytes_;

    // Superblock (in-memory)
    Superblock superblock_;
};

// C-style API wrapper (for compatibility)
extern "C" {

// Handle type for C API
typedef void* spdk_kv_handle;

// Callback type for C API
typedef void (*spdk_kv_cb)(void* cb_arg, int status);
typedef void (*spdk_kv_get_cb)(void* cb_arg, int status, uint32_t actual_len);

// Create options for C API
struct spdk_kv_create_opts {
    uint64_t max_capacity;
    uint64_t data_file_size;
    uint64_t max_entries;
    double index_load_factor;
    int force;
};

// Open options for C API
struct spdk_kv_open_opts {
    int read_only;
    int recover;
};

// Synchronous C API
int spdk_kv_create(const char* path, struct spdk_kv_create_opts* opts, spdk_kv_handle* handle);
int spdk_kv_open(const char* path, struct spdk_kv_open_opts* opts, spdk_kv_handle* handle);
int spdk_kv_close(spdk_kv_handle handle);
int spdk_kv_put(spdk_kv_handle handle, uint64_t key, const void* value, uint32_t value_len);
int spdk_kv_get(spdk_kv_handle handle, uint64_t key, void* value_buf, uint32_t buf_len,
                uint32_t* actual_len);
int spdk_kv_del(spdk_kv_handle handle, uint64_t key);

// Async C API (callbacks called immediately in simulation mode)
void spdk_kv_put_async(spdk_kv_handle handle, uint64_t key, const void* value, uint32_t value_len,
                       spdk_kv_cb cb, void* cb_arg);
void spdk_kv_get_async(spdk_kv_handle handle, uint64_t key, void* value_buf, uint32_t buf_len,
                       spdk_kv_get_cb cb, void* cb_arg);
void spdk_kv_del_async(spdk_kv_handle handle, uint64_t key, spdk_kv_cb cb, void* cb_arg);

// Polling (no-op in simulation)
void spdk_kv_poll(spdk_kv_handle handle);

// Statistics
uint64_t spdk_kv_get_entry_count(spdk_kv_handle handle);
uint64_t spdk_kv_get_total_bytes(spdk_kv_handle handle);

}  // extern "C"

}  // namespace spdk_kv
