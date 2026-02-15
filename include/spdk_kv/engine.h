// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "spdk_kv/alloc_log.h"
#include "spdk_kv/append_buffer.h"
#include "spdk_kv/callback_task_queue.h"
#include "spdk_kv/checkpoint.h"
#include "spdk_kv/compaction.h"
#include "spdk_kv/crc32.h"
#include "spdk_kv/ctx_pool.h"
#include "spdk_kv/dma_memory_pool.h"
#include "spdk_kv/entry.h"
#include "spdk_kv/io_submitter.h"
#include "spdk_kv/mem_index.h"
#include "spdk_kv/spdk_env.h"
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
    uint32_t io_queue_size = 256;                   // NVMe IO queue depth

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
    uint64_t blob_id;
    FileState state;
    uint64_t size;
    uint64_t write_offset;

    spdk_blob* blob;   // SPDK blob handle
    bool blob_opened;  // Whether blob is opened

    FileInfo()
            : file_id(0),
              blob_id(0),
              state(FileState::kActive),
              size(0),
              write_offset(0),
              blob(nullptr),
              blob_opened(false) {}

    bool IsWritable() const { return state == FileState::kActive; }
    bool IsReadable() const {
        return state == FileState::kActive || state == FileState::kSealed ||
               state == FileState::kCompacting;
    }
};

struct SegmentBuf {
    iovec buffers_[16];
    size_t cnt_ = 0;
};

// KV Engine class
class Engine {
    friend class CompactionTask;
    friend class CompactionScheduler;

public:
    Engine();
    ~Engine();

    // Non-copyable
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    // Lifecycle operations (synchronous)
    KvError Create(const std::string& path, const CreateOpts& opts);
    KvError Open(const std::string& path, const OpenOpts& opts);
    KvError Close();

    // KV operations (synchronous)
    KvError Put(uint64_t key, const void* value, uint32_t value_len);
    KvError Get(uint64_t key, void* value_buf, uint32_t buf_len, uint32_t* actual_len);
    KvError Delete(uint64_t key);

    // 第一个segment，最前面的256byte 为用户数据，不能写入
    // 从256 到 512byte 之间，可以用来存放entry header + key + value_len + crcofvalue
    // 写入盘时，从0开始写入整个segment
    void PutAsync(uint64_t key, SegmentBuf input_buf, KvCallback cb, void* cb_arg);
    // output buf中为输出的内存地址
    // 每段内存均为4K地址对齐，可以用来直接DMA读写
    // 实际读取的值长度通过cb参数返回，用户根据实际长度使用对应的内存区域
    // 要求从nvme 上读取的数据，包含前面256byte的头部，以及 256-512byte的entry header + key +
    // value_len + crc等信息 用户会自行剔除前512byte的头部，获取真正的value数据
    void GetAsync(uint64_t key, SegmentBuf* output, KvGetCallback cb, void* cb_arg);
    void DeleteAsync(uint64_t key, KvCallback cb, void* cb_arg);

    // Polling (call in main loop to process IO completions)
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

    // Callback task queue (for deferred operations from IO callbacks)
    CallbackTaskQueue& GetTaskQueue() { return task_queue_; }

    // Checkpoint operations
    void StartCheckpoint(KvCallback cb, void* cb_arg);
    bool IsCheckpointInProgress() const;
    void CheckCheckpointTrigger();

    // Compaction operations
    void ScheduleCompaction(uint16_t file_id);
    void SetCompactionEnabled(bool enabled) { compaction_enabled_ = enabled; }
    bool IsCompactionEnabled() const { return compaction_enabled_; }
    size_t GetGarbageRatio() const;

    // Get pending foreground request count (for compaction priority)
    size_t GetPendingForegroundCount() const { return pending_foreground_count_; }

    // Get file metadata (for compaction)
    FileMetadata* GetFileMetadata(uint16_t file_id);
    const std::unordered_map<uint16_t, FileMetadata>& GetFileMetadataMap() const;

    // Remove a file after compaction (close blob, delete blob, erase metadata)
    void CompactionRemoveFile(uint16_t file_id, std::function<void(bool)> callback);

    // RDMA two-phase commit interface
    void BuildEntryInplaceRdma(void* slot, uint64_t key, uint32_t len, uint32_t seq,
                               bool is_tombstone = false);
    int AllocRdmaSlot(uint32_t value_len, RdmaSlot* out_slot);
    int CommitRdmaSlot(uint32_t slot_id, uint32_t epoch);
    void PutRdma(uint64_t key, void* dma_buffer, uint32_t value_offset, uint32_t len, uint32_t seq,
                 KvCallback cb, void* cb_arg);

    // Vectored put (placeholder, delegates to PutAsync)
    void PutVectored(uint64_t key, void* value, uint32_t len, KvCallback cb, void* cb_arg);

    // AppendBuffer-based IO merge path
    void PutBuffered(uint64_t key, const void* value, uint32_t value_len, KvCallback cb,
                     void* cb_arg);

private:
    // Internal helpers
    KvError InitializeNew(const CreateOpts& opts);
    KvError LoadExisting(const OpenOpts& opts);
    KvError Recover();
    KvError LoadSuperblock();
    KvError RebuildFileInfo();
    KvError RecoverMemIndex();

    // File management
    FileInfo* GetActiveFile();
    FileInfo* GetFile(uint16_t file_id);
    FileInfo* AllocateNewFile();

    // SPDK blob management
    void AllocateBlobForFile(FileInfo* file, std::function<void(bool success)> callback);
    void OpenBlobForFile(FileInfo* file, std::function<void(bool success)> callback);
    void CloseBlobForFile(FileInfo* file, std::function<void(bool success)> callback);

    // SPDK async IO operations
    void SubmitBlobWrite(FileInfo* file, uint64_t offset, void* data, uint32_t length,
                         std::function<void(int status)> callback);
    void SubmitBlobWritev(FileInfo* file, uint64_t offset, const SegmentBuf& buf,
                          uint32_t total_length, std::function<void(int status)> callback);
    void SubmitBlobRead(FileInfo* file, uint64_t offset, void* buffer, uint32_t length,
                        std::function<void(int status)> callback);
    void SubmitBlobReadv(FileInfo* file, uint64_t offset, const SegmentBuf& buf,
                         uint32_t total_length, std::function<void(int status)> callback);

    // Get SPDK blob for a file
    spdk_blob* GetBlobForFile(uint16_t file_id);

    // Superblock blob management
    KvError CreateSuperblockBlob();
    KvError WriteSuperblock();
    void UpdateSuperblockFileMapping(FileInfo* file);

    // Entry building
    void BuildEntryInplace(void* slot, uint64_t key, const void* value, uint32_t len, uint32_t seq,
                           bool is_tombstone = false);

    // Async IO completion contexts and handlers
    struct GetReadCompletionCtx {
        Engine* engine;
        SegmentBuf* output;  // User-provided buffer (4KB aligned)
        uint32_t read_size;  // Total bytes to read
        KvGetCallback cb;
        void* cb_arg;
    };
    void HandleGetReadCompletion(int status, GetReadCompletionCtx* ctx);

    struct OpenBlobForFileCtx {
        Engine* engine;
        FileInfo* file;
        std::function<void(bool success)> callback;
    };
    void HandleBlobOpenedForFile(spdk_blob* blob, OpenBlobForFileCtx* ctx);

    // Index update
    void UpdateIndexOnWriteComplete(uint64_t key, const MemIndexEntry& entry, bool is_compaction,
                                    uint16_t old_file_id, uint32_t old_offset_index);

    // Pending write request queued during async file allocation
    struct PendingWriteRequest {
        uint64_t key;
        void* dma_buffer;
        SegmentBuf segment_buf;
        bool is_segment_write;
        uint32_t aligned_size;
        uint32_t sequence;
        uint16_t page_count;
        uint8_t tag;
        KvCallback cb;
        void* cb_arg;
        bool is_delete;
        uint64_t old_garbage_size;
    };

    // Async file allocation
    void AllocateNewFileAsync();
    void OnNewFileAllocated(bool success);
    void ProcessPendingWriteQueue();
    void SubmitQueuedWrite(PendingWriteRequest& req, FileInfo* file);

    // Fill PendingWriteRequest helpers
    void FillPendingWriteReq(PendingWriteRequest& req, uint64_t key, void* dma_buffer,
                             uint32_t aligned_size, uint32_t sequence, uint16_t page_count,
                             uint8_t tag, KvCallback cb, void* cb_arg, bool is_delete,
                             uint64_t old_garbage_size);
    void FillPendingWriteReq(PendingWriteRequest& req, uint64_t key, const SegmentBuf& segment_buf,
                             uint32_t aligned_size, uint32_t sequence, uint16_t page_count,
                             uint8_t tag, KvCallback cb, void* cb_arg);
    void FillPendingWriteReq(PendingWriteRequest& req, const WaitQueueEntry& wqe);

    // Superblock update callback for checkpoint atomicity
    void OnSuperblockUpdate(uint32_t checkpoint_seq, const ActiveBufferPos* positions,
                            uint8_t count, std::function<void(int)> on_complete);

    // Queued write completion handler
    struct QueuedWriteCompletionCtx {
        Engine* engine;
        uint64_t key;
        MemIndexEntry entry;
        bool is_delete;
        uint64_t old_garbage_size;
        KvCallback cb;
        void* cb_arg;
    };
    void OnQueuedWriteComplete(int status, QueuedWriteCompletionCtx* ctx);

    // PutAsync write completion handler
    struct PutAsyncWriteCompletionCtx {
        Engine* engine;
        uint64_t key;
        MemIndexEntry entry;
        KvCallback cb;
        void* cb_arg;
    };
    void OnPutAsyncWriteComplete(int status, PutAsyncWriteCompletionCtx* ctx);

    // Blob IO completion contexts (moved from local scope for pooling)
    struct BlobWriteCtx {
        std::function<void(int)> callback;
        Engine* engine;
    };
    struct BlobWritevCtx {
        std::function<void(int)> callback;
        Engine* engine;
        iovec iovs[16];
    };
    struct BlobReadCtx {
        std::function<void(int)> callback;
        Engine* engine;
    };
    struct BlobReadvCtx {
        std::function<void(int)> callback;
        Engine* engine;
        iovec iovs[16];
    };

    // CompactionRemoveFile blob close handler
    void OnCompactionBlobClosed(bool close_ok, uint16_t file_id, FileInfo* file,
                                std::function<void(bool)> callback);

    // AllocateBlobForFile blob allocated handler
    void OnBlobAllocated(uint64_t blob_id, FileInfo* file,
                         std::function<void(bool)> callback);

    // AllocLog integration
    void InitAllocLogManager();
    void ProcessAllocLogRecovery();
    void ReleaseDanglingBlobs();
    static void OnCheckpointForAllocComplete(void* arg, int status);
    void ResumeAllocationAfterCheckpoint(int status);

    // RDMA slot management
    uint32_t AllocateSlotId();
    uint64_t GetBufferRkey(void* buffer_addr);

    // Backpressure resume and wait queue
    static void OnBackpressureResume(void* arg);
    void ProcessWaitQueue();

    // Buffer IO submission and completion
    void SubmitBufferIo(AppendBuffer* buffer);
    void OnBufferIoComplete(int status, BufferIoContext* ctx);

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

    // Checkpoint manager
    std::unique_ptr<IncrementalCheckpoint> checkpoint_manager_;
    CheckpointTrigger checkpoint_trigger_;

    // Compaction scheduler
    std::unique_ptr<CompactionScheduler> compaction_scheduler_;
    bool compaction_enabled_;

    // File metadata for compaction tracking
    std::unordered_map<uint16_t, FileMetadata> file_metadata_;

    // IO Submitter for async IO operations
    std::unique_ptr<IoSubmitter> io_submitter_;

    // Pending async operations (for tracking in-flight requests)
    size_t pending_foreground_count_;

    // Async file allocation state
    bool allocating_new_file_;
    bool pending_checkpoint_for_alloc_;
    std::vector<PendingWriteRequest> pending_write_queue_;

    // AllocLog manager (tracks blob allocations between checkpoints)
    std::unique_ptr<AllocLogManager> alloc_log_manager_;

    // Pending blob operations count
    size_t pending_blob_ops_;

    // Superblock blob
    spdk_blob* superblock_blob_;
    spdk_blob_id superblock_blob_id_;

    // RDMA slot tracking
    std::unordered_map<uint32_t, AppendSlot> append_slots_;
    uint32_t next_global_slot_id_;

    // Flush trigger for IO batching
    FlushTrigger flush_trigger_;
    uint64_t last_flush_ns_;

    // Backpressure wait queue
    std::queue<WaitQueueEntry> wait_queue_;

    // Per-buffer pending write tracking for IO merge
    std::unordered_map<AppendBuffer*, std::vector<BufferedPendingWrite>> buffer_pending_writes_;

    // Callback task queue for deferred IO callback work
    CallbackTaskQueue task_queue_;

    // NUMA-aware memory pools (optional, created when core_id is set)
    std::unique_ptr<MemoryPools> memory_pools_;

    // Context object pools (avoid heap allocation on IO hot path)
    CtxPool<GetReadCompletionCtx, 64> get_read_ctx_pool_;
    CtxPool<OpenBlobForFileCtx, 8> open_blob_ctx_pool_;
    CtxPool<QueuedWriteCompletionCtx, 64> queued_write_ctx_pool_;
    CtxPool<PutAsyncWriteCompletionCtx, 64> put_async_ctx_pool_;
    CtxPool<BlobWriteCtx, 128> blob_write_ctx_pool_;
    CtxPool<BlobWritevCtx, 64> blob_writev_ctx_pool_;
    CtxPool<BlobReadCtx, 128> blob_read_ctx_pool_;
    CtxPool<BlobReadvCtx, 64> blob_readv_ctx_pool_;
    CtxPool<BufferIoContext, 32> buffer_io_ctx_pool_;
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

// Async C API
void spdk_kv_put_async(spdk_kv_handle handle, uint64_t key, const void* value, uint32_t value_len,
                       spdk_kv_cb cb, void* cb_arg);
void spdk_kv_get_async(spdk_kv_handle handle, uint64_t key, void* value_buf, uint32_t buf_len,
                       spdk_kv_get_cb cb, void* cb_arg);
void spdk_kv_del_async(spdk_kv_handle handle, uint64_t key, spdk_kv_cb cb, void* cb_arg);

// Polling
void spdk_kv_poll(spdk_kv_handle handle);

// Statistics
uint64_t spdk_kv_get_entry_count(spdk_kv_handle handle);
uint64_t spdk_kv_get_total_bytes(spdk_kv_handle handle);

}  // extern "C"

}  // namespace spdk_kv
