// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#pragma once

#include <spdk_kv/spdk_kv.h>
#include <spdk_kv/types.h>
#include <spdk_kv/config.h>
#include "mem_index.h"
#include "append_buffer.h"
#include "io_submitter.h"

#include <memory>
#include <vector>
#include <queue>
#include <string>
#include <unordered_map>

#ifdef WITH_SPDK
#include <spdk/bdev.h>
#include <spdk/blob.h>
#include <spdk/io_channel.h>
#include <spdk/thread.h>
#endif

namespace spdk_kv {

// Forward declarations
class CompactionScheduler;
class Checkpoint;
class Recovery;

// File info structure
struct FileInfo {
    uint16_t file_id;
    FileState state;
    uint64_t size;
    uint64_t write_offset;
    uint64_t entry_count;
    uint64_t valid_bytes;
    uint64_t total_bytes;

#ifdef WITH_SPDK
    struct spdk_blob* blob;
    spdk_blob_id blob_id;
#else
    void* blob;
    uint64_t blob_id;
#endif

    // Data file header
    DataFileHeader header;
};

// Wait queue entry
struct WaitQueueEntry {
    uint64_t key;
    void* value;
    uint32_t len;
    uint32_t seq;
    spdk_kv_cb callback;
    void* cb_arg;
};

// Engine implementation
class Engine {
public:
    Engine();
    ~Engine();

    // Create new engine
    int create(const char* dev_name, CreateOpts* opts, spdk_kv_cb cb, void* cb_arg);

    // Open existing engine
    int open(const char* dev_name, OpenOpts* opts, spdk_kv_cb cb, void* cb_arg);

    // Close engine
    int close(spdk_kv_cb cb, void* cb_arg);

    // Put key-value pair
    int put(uint64_t key, void* value, uint32_t value_len, spdk_kv_cb cb, void* cb_arg);

    // Get value by key
    int get(uint64_t key, void* value_buf, uint32_t buf_len, spdk_kv_cb cb, void* cb_arg);

    // Delete key
    int del(uint64_t key, spdk_kv_cb cb, void* cb_arg);

    // Sync pending writes
    int sync(spdk_kv_cb cb, void* cb_arg);

    // Poll for completions
    int poll();

    // Check if engine is ready
    bool is_ready() const { return state_ == EngineState::READY; }

    // Get current state
    EngineState state() const { return state_; }

    // Get configuration
    const Config& config() const { return config_; }

    // Accessors for internal components
    MemIndex* mem_index() { return mem_index_.get(); }
    AppendBufferManager* buffer_manager() { return buffer_manager_.get(); }
    IoSubmitter* io_submitter() { return io_submitter_.get(); }

    // Callback when write completes
    void on_write_complete(const PendingWrite& pw, int status);

    // Get file by ID
    FileInfo* get_file(uint16_t file_id);

    // Allocate new data file
    FileInfo* allocate_data_file();

    // Remove file mapping
    void remove_file_mapping(uint16_t file_id);

    // Get pending foreground count
    size_t pending_foreground_count() const;

    // Check for pending foreground requests
    bool has_pending_foreground_requests() const;

    // Allocate DMA buffer
    void* alloc_buffer(size_t size);
    void free_buffer(void* buf);

    // Statistics
    uint64_t total_puts() const { return stats_.total_puts; }
    uint64_t total_gets() const { return stats_.total_gets; }
    uint64_t total_dels() const { return stats_.total_dels; }

    // CRC32 calculation (public for use by recovery and checkpoint)
    static uint32_t crc32(const void* data, size_t len);

#ifdef WITH_SPDK
    // Get blob store
    struct spdk_blob_store* blob_store() { return blob_store_; }
#endif

private:
    // Can accept new requests
    bool can_accept_request() const { return state_ == EngineState::READY; }

    // Build entry in place (local put)
    void build_entry_inplace(void* slot, uint64_t key, void* value,
                             uint32_t len, uint32_t seq);

    // Build tombstone entry
    void build_tombstone_inplace(void* slot, uint64_t key, uint32_t seq);

    // Process wait queue
    void process_wait_queue();

    // Check checkpoint trigger
    void check_checkpoint_trigger();

    // Process append buffers
    void process_append_buffers();

    // Update index on write complete
    void update_index_on_write_complete(const PendingWrite& pw);

    // Initialize SPDK environment
    int init_spdk_env(const char* dev_name);

    // Load superblock
    int load_superblock();

    // Save superblock
    int save_superblock();

    // Create initial layout
    int create_initial_layout();

    // State
    EngineState state_;
    Config config_;
    std::string device_name_;

    // Superblock
    Superblock superblock_;

    // Memory index
    std::unique_ptr<MemIndex> mem_index_;

    // Buffer manager
    std::unique_ptr<AppendBufferManager> buffer_manager_;

    // IO submitter
    std::unique_ptr<IoSubmitter> io_submitter_;

    // Compaction scheduler
    std::unique_ptr<CompactionScheduler> compaction_scheduler_;

    // Checkpoint manager
    std::unique_ptr<Checkpoint> checkpoint_;

    // File management
    std::unordered_map<uint16_t, std::unique_ptr<FileInfo>> files_;
    FileInfo* active_file_;
    uint16_t next_file_id_;

    // Pending writes tracking
    std::vector<PendingWrite> pending_writes_;

    // Wait queue for backpressure
    std::queue<WaitQueueEntry> wait_queue_;

    // Flush trigger
    FlushTrigger flush_trigger_;

    // Statistics
    struct Stats {
        uint64_t total_puts = 0;
        uint64_t total_gets = 0;
        uint64_t total_dels = 0;
        uint64_t total_entries = 0;
        uint64_t total_data_bytes = 0;
        uint64_t total_garbage_bytes = 0;
    } stats_;

    // Callbacks
    spdk_kv_cb pending_callback_;
    void* pending_cb_arg_;

#ifdef WITH_SPDK
    // SPDK resources
    struct spdk_bdev* bdev_;
    struct spdk_bdev_desc* bdev_desc_;
    struct spdk_io_channel* io_channel_;
    struct spdk_blob_store* blob_store_;
    struct spdk_bs_dev* bs_dev_;
    struct spdk_thread* thread_;

    // Blob IDs
    spdk_blob_id superblock_blob_id_;
    spdk_blob_id mem_index_blob_a_id_;
    spdk_blob_id mem_index_blob_b_id_;
#endif
};

}  // namespace spdk_kv
