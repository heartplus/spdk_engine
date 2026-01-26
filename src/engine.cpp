// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#include "engine.h"
#include "checkpoint.h"
#include "compaction.h"
#include <cstring>
#include <cstdlib>
#include <algorithm>

#ifdef WITH_SPDK
#include <spdk/env.h>
#include <spdk/bdev.h>
#include <spdk/blob.h>
#include <spdk/log.h>
#include <spdk/thread.h>
#endif

namespace spdk_kv {

// CRC32 lookup table
static uint32_t crc32_table[256];
static bool crc32_table_initialized = false;

static void init_crc32_table() {
    if (crc32_table_initialized) return;

    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320 : 0);
        }
        crc32_table[i] = crc;
    }
    crc32_table_initialized = true;
}

uint32_t Engine::crc32(const void* data, size_t len) {
    init_crc32_table();

    const uint8_t* buf = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFF;

    for (size_t i = 0; i < len; i++) {
        crc = crc32_table[(crc ^ buf[i]) & 0xFF] ^ (crc >> 8);
    }

    return crc ^ 0xFFFFFFFF;
}

Engine::Engine()
    : state_(EngineState::UNINITIALIZED)
    , active_file_(nullptr)
    , next_file_id_(0)
    , pending_callback_(nullptr)
    , pending_cb_arg_(nullptr)
#ifdef WITH_SPDK
    , bdev_(nullptr)
    , bdev_desc_(nullptr)
    , io_channel_(nullptr)
    , blob_store_(nullptr)
    , bs_dev_(nullptr)
    , thread_(nullptr)
#endif
{
    memset(&superblock_, 0, sizeof(superblock_));
}

Engine::~Engine() {
    if (state_ != EngineState::CLOSED && state_ != EngineState::UNINITIALIZED) {
        // Should call close before destructor
    }
}

int Engine::create(const char* dev_name, CreateOpts* opts, spdk_kv_cb cb, void* cb_arg) {
    if (state_ != EngineState::UNINITIALIZED) {
        return static_cast<int>(KvError::ENGINE_NOT_READY);
    }

    state_ = EngineState::OPENING;
    device_name_ = dev_name;

    if (opts) {
        config_ = opts->config;
    }

    if (!config_.validate()) {
        state_ = EngineState::ERROR;
        return static_cast<int>(KvError::INVALID_ARGUMENT);
    }

    pending_callback_ = cb;
    pending_cb_arg_ = cb_arg;

#ifdef WITH_SPDK
    // Initialize SPDK environment
    int rc = init_spdk_env(dev_name);
    if (rc != 0) {
        state_ = EngineState::ERROR;
        return rc;
    }

    // Create initial layout (async)
    rc = create_initial_layout();
    if (rc != 0) {
        state_ = EngineState::ERROR;
        return rc;
    }
#else
    // Non-SPDK mode: simulate creation
    superblock_.magic = SUPERBLOCK_MAGIC;
    superblock_.version = 1;
    superblock_.sequence = 0;
    superblock_.create_time = time(nullptr);
    superblock_.total_capacity = config_.max_capacity;
    superblock_.data_file_size = config_.data_file_size;
    superblock_.alignment_unit = config_.alignment;

    // Initialize components
    mem_index_ = std::make_unique<MemIndex>(config_.max_entries, config_.index_load_factor);

    buffer_manager_ = std::make_unique<AppendBufferManager>(
        config_.append_buffer_size,
        config_.append_buffer_min_count,
        config_.append_buffer_max_count);

    if (!buffer_manager_->init()) {
        state_ = EngineState::ERROR;
        return static_cast<int>(KvError::INTERNAL_ERROR);
    }

    io_submitter_ = std::make_unique<IoSubmitter>(this);
    io_submitter_->init(nullptr);

    // Create first data file (simulated)
    active_file_ = allocate_data_file();

    state_ = EngineState::READY;

    if (cb) {
        cb(cb_arg, 0);
    }
#endif

    return 0;
}

int Engine::open(const char* dev_name, OpenOpts* opts, spdk_kv_cb cb, void* cb_arg) {
    if (state_ != EngineState::UNINITIALIZED) {
        return static_cast<int>(KvError::ENGINE_NOT_READY);
    }

    state_ = EngineState::OPENING;
    device_name_ = dev_name;

    if (opts) {
        config_ = opts->config;
    }

    pending_callback_ = cb;
    pending_cb_arg_ = cb_arg;

#ifdef WITH_SPDK
    // Initialize SPDK environment
    int rc = init_spdk_env(dev_name);
    if (rc != 0) {
        state_ = EngineState::ERROR;
        return rc;
    }

    // Load and verify superblock
    rc = load_superblock();
    if (rc != 0) {
        state_ = EngineState::ERROR;
        return rc;
    }

    // Enter recovery state
    state_ = EngineState::RECOVERING;

    // Recovery will be async, callback when done
#else
    // Non-SPDK mode: simulate open
    superblock_.magic = SUPERBLOCK_MAGIC;
    superblock_.version = 1;

    // Initialize components
    mem_index_ = std::make_unique<MemIndex>(config_.max_entries, config_.index_load_factor);

    buffer_manager_ = std::make_unique<AppendBufferManager>(
        config_.append_buffer_size,
        config_.append_buffer_min_count,
        config_.append_buffer_max_count);

    if (!buffer_manager_->init()) {
        state_ = EngineState::ERROR;
        return static_cast<int>(KvError::INTERNAL_ERROR);
    }

    io_submitter_ = std::make_unique<IoSubmitter>(this);
    io_submitter_->init(nullptr);

    active_file_ = allocate_data_file();

    state_ = EngineState::READY;

    if (cb) {
        cb(cb_arg, 0);
    }
#endif

    return 0;
}

int Engine::close(spdk_kv_cb cb, void* cb_arg) {
    if (state_ != EngineState::READY) {
        return static_cast<int>(KvError::ENGINE_NOT_READY);
    }

    state_ = EngineState::CLOSING;

    // Flush all pending writes
    if (buffer_manager_) {
        buffer_manager_->submit_current_buffer();
    }

    // Save superblock
    save_superblock();

#ifdef WITH_SPDK
    // Close SPDK resources
    if (io_channel_) {
        spdk_put_io_channel(io_channel_);
        io_channel_ = nullptr;
    }

    if (bdev_desc_) {
        spdk_bdev_close(bdev_desc_);
        bdev_desc_ = nullptr;
    }
#endif

    state_ = EngineState::CLOSED;

    if (cb) {
        cb(cb_arg, 0);
    }

    return 0;
}

int Engine::put(uint64_t key, void* value, uint32_t value_len,
                spdk_kv_cb cb, void* cb_arg) {
    if (!can_accept_request()) {
        return static_cast<int>(KvError::ENGINE_NOT_READY);
    }

    if (value == nullptr || value_len == 0) {
        return static_cast<int>(KvError::INVALID_ARGUMENT);
    }

    if (value_len > config_.max_value_size) {
        return static_cast<int>(KvError::VALUE_TOO_LARGE);
    }

    // Allocate sequence number
    uint32_t seq = mem_index_->allocate_sequence();

    // Calculate aligned size
    size_t header_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);
    size_t entry_size = header_size + value_len + sizeof(uint32_t);  // +checksum
    size_t aligned_size = ALIGN_UP(entry_size, ALIGNMENT);

    // Try to reserve space with backpressure check
    int error = 0;
    void* slot = buffer_manager_->reserve_with_backpressure(aligned_size, &error);

    if (slot == nullptr) {
        if (error == AppendBufferManager::ERR_BACKPRESSURE) {
            // Add to wait queue
            wait_queue_.push({key, value, value_len, seq, cb, cb_arg});
            buffer_manager_->register_resume_callback(
                [](void* arg) {
                    auto* engine = static_cast<Engine*>(arg);
                    engine->process_wait_queue();
                }, this);
            return 0;
        }

        // Buffer full, submit and retry
        buffer_manager_->submit_current_buffer();

        slot = buffer_manager_->reserve_with_backpressure(aligned_size, &error);
        if (slot == nullptr) {
            wait_queue_.push({key, value, value_len, seq, cb, cb_arg});
            return 0;
        }
    }

    // Build entry in place
    build_entry_inplace(slot, key, value, value_len, seq);

    // Track pending write
    PendingWrite pw;
    pw.key = key;
    pw.sequence = seq;
    pw.buffer_offset = buffer_manager_->get_offset(slot);
    pw.aligned_size = static_cast<uint32_t>(aligned_size);
    pw.flags = 0;
    pw.callback = cb;
    pw.cb_arg = cb_arg;

    // Prepare new index entry
    pw.new_entry.key = key;
    pw.new_entry.file_id = active_file_->file_id;
    pw.new_entry.offset_index = (active_file_->write_offset + pw.buffer_offset) / ALIGNMENT;
    pw.new_entry.deleted = 0;
    pw.new_entry.sequence = seq;
    pw.new_entry.page_count = (aligned_size + ALIGNMENT - 1) / ALIGNMENT;

    uint64_t hash;
    MemIndex::compute_hash(key, &hash, &pw.new_entry.tag);

    pending_writes_.push_back(pw);
    buffer_manager_->inc_entry_count();
    stats_.total_puts++;

    return 0;
}

int Engine::get(uint64_t key, void* value_buf, uint32_t buf_len,
                spdk_kv_cb cb, void* cb_arg) {
    if (!can_accept_request()) {
        return static_cast<int>(KvError::ENGINE_NOT_READY);
    }

    if (value_buf == nullptr || buf_len == 0) {
        return static_cast<int>(KvError::INVALID_ARGUMENT);
    }

    // Find in memory index
    MemIndexEntry* entry = mem_index_->find(key);
    if (!entry || entry->deleted) {
        return static_cast<int>(KvError::KEY_NOT_FOUND);
    }

    // Get file info
    FileInfo* file = get_file(entry->file_id);
    if (!file) {
        return static_cast<int>(KvError::INTERNAL_ERROR);
    }

    // Calculate read offset and size
    uint64_t offset = static_cast<uint64_t>(entry->offset_index) * ALIGNMENT;
    size_t read_size = static_cast<size_t>(entry->page_count) * ALIGNMENT;

    // Submit read request
    io_submitter_->submit_read(key, value_buf, buf_len, file, offset, read_size, cb, cb_arg);

    stats_.total_gets++;

    return 0;
}

int Engine::del(uint64_t key, spdk_kv_cb cb, void* cb_arg) {
    if (!can_accept_request()) {
        return static_cast<int>(KvError::ENGINE_NOT_READY);
    }

    // Check if key exists
    MemIndexEntry* entry = mem_index_->find(key);
    if (!entry || entry->deleted) {
        return static_cast<int>(KvError::KEY_NOT_FOUND);
    }

    // Allocate sequence
    uint32_t seq = mem_index_->allocate_sequence();

    // Calculate tombstone size
    size_t tombstone_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);
    size_t aligned_size = ALIGN_UP(tombstone_size, ALIGNMENT);

    // Reserve space
    int error = 0;
    void* slot = buffer_manager_->reserve_with_backpressure(aligned_size, &error);

    if (slot == nullptr) {
        if (error == AppendBufferManager::ERR_BACKPRESSURE) {
            // For simplicity, return error (could also queue)
            return static_cast<int>(KvError::BACKPRESSURE);
        }

        buffer_manager_->submit_current_buffer();
        slot = buffer_manager_->reserve_with_backpressure(aligned_size, &error);
        if (slot == nullptr) {
            return static_cast<int>(KvError::BACKPRESSURE);
        }
    }

    // Build tombstone
    build_tombstone_inplace(slot, key, seq);

    // Mark as deleted in memory index
    mem_index_->remove(key);

    // Track pending write
    PendingWrite pw;
    pw.key = key;
    pw.sequence = seq;
    pw.buffer_offset = buffer_manager_->get_offset(slot);
    pw.aligned_size = static_cast<uint32_t>(aligned_size);
    pw.flags = 0;
    pw.callback = cb;
    pw.cb_arg = cb_arg;

    pw.new_entry.key = key;
    pw.new_entry.deleted = 1;

    pending_writes_.push_back(pw);
    buffer_manager_->inc_entry_count();
    stats_.total_dels++;

    return 0;
}

int Engine::sync(spdk_kv_cb cb, void* cb_arg) {
    if (!can_accept_request()) {
        return static_cast<int>(KvError::ENGINE_NOT_READY);
    }

    // Submit current buffer
    buffer_manager_->submit_current_buffer();

    // For now, just call callback (real implementation would wait for IO)
    if (cb) {
        cb(cb_arg, 0);
    }

    return 0;
}

int Engine::poll() {
    if (state_ != EngineState::READY) {
        return 0;
    }

    int completions = 0;

#ifdef WITH_SPDK
    // Process SPDK completions
    if (io_channel_) {
        // SPDK polling happens through thread polling
    }
#endif

    // Process append buffers
    process_append_buffers();

    // IO submitter poll
    io_submitter_->poll();

    // Check pending buffer resets
    buffer_manager_->check_pending_resets();

    // Process resume notifications
    buffer_manager_->process_resume_notifications();

    // Process wait queue
    process_wait_queue();

    // Check checkpoint trigger
    check_checkpoint_trigger();

    return completions;
}

void Engine::build_entry_inplace(void* slot, uint64_t key, void* value,
                                 uint32_t len, uint32_t seq) {
    char* ptr = static_cast<char*>(slot);

    // Header (16 bytes)
    auto* header = reinterpret_cast<EntryHeader*>(ptr);
    header->magic = ENTRY_MAGIC;
    header->version = 1;
    header->flags = 0;
    header->reserved = 0;
    header->sequence = seq;
    header->padding = 0;
    ptr += sizeof(EntryHeader);

    // Key (8 bytes)
    *reinterpret_cast<uint64_t*>(ptr) = key;
    ptr += sizeof(uint64_t);

    // Value length (4 bytes)
    *reinterpret_cast<uint32_t*>(ptr) = len;
    ptr += sizeof(uint32_t);

    // Value
    memcpy(ptr, value, len);
    ptr += len;

    // Padding
    size_t used = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) + len;
    size_t aligned = ALIGN_UP(used + sizeof(uint32_t), ALIGNMENT);
    size_t padding = aligned - used - sizeof(uint32_t);
    if (padding > 0) {
        memset(ptr, 0, padding);
        ptr += padding;
    }

    // Checksum
    *reinterpret_cast<uint32_t*>(ptr) = crc32(slot, ptr - static_cast<char*>(slot));
}

void Engine::build_tombstone_inplace(void* slot, uint64_t key, uint32_t seq) {
    char* ptr = static_cast<char*>(slot);

    // Header
    auto* header = reinterpret_cast<EntryHeader*>(ptr);
    header->magic = ENTRY_MAGIC;
    header->version = 1;
    header->flags = FLAG_DELETED;
    header->reserved = 0;
    header->sequence = seq;
    header->padding = 0;
    ptr += sizeof(EntryHeader);

    // Key
    *reinterpret_cast<uint64_t*>(ptr) = key;
    ptr += sizeof(uint64_t);

    // Padding to alignment
    size_t used = sizeof(EntryHeader) + sizeof(uint64_t);
    size_t aligned = ALIGN_UP(used + sizeof(uint32_t), ALIGNMENT);
    size_t padding = aligned - used - sizeof(uint32_t);
    if (padding > 0) {
        memset(ptr, 0, padding);
        ptr += padding;
    }

    // Checksum
    *reinterpret_cast<uint32_t*>(ptr) = crc32(slot, ptr - static_cast<char*>(slot));
}

void Engine::process_wait_queue() {
    while (!wait_queue_.empty() && !buffer_manager_->is_backpressure_active()) {
        auto entry = wait_queue_.front();
        wait_queue_.pop();

        // Re-try the put operation
        put(entry.key, entry.value, entry.len, entry.callback, entry.cb_arg);
    }
}

void Engine::check_checkpoint_trigger() {
    // TODO: Implement checkpoint triggering logic
}

void Engine::process_append_buffers() {
    // Check if immediate flush needed
    size_t pending_buffers = buffer_manager_->pending_count();
    size_t pending_entries = buffer_manager_->current_entry_count();

    // In simulation mode, always submit if there are pending writes
    // In real SPDK mode, respect the flush trigger thresholds
    bool should_flush = flush_trigger_.should_flush_immediately(pending_buffers, pending_entries);

    // If we have pending writes and active buffer has data, submit it
    if (!pending_writes_.empty() && buffer_manager_->active_buffer() &&
        buffer_manager_->active_buffer()->used() > 0) {
        should_flush = true;
    }

    if (should_flush) {
        buffer_manager_->submit_current_buffer();
    }

    // Get pending buffers and submit
    AppendBuffer* buffers[IoSubmitter::BATCH_SIZE];
    size_t count = buffer_manager_->get_pending_buffers(buffers, IoSubmitter::BATCH_SIZE);

    for (size_t i = 0; i < count; i++) {
        // Collect pending writes for this buffer
        std::vector<PendingWrite> buffer_writes;
        for (auto it = pending_writes_.begin(); it != pending_writes_.end();) {
            // Check if write is in this buffer (simplified check)
            if (it->buffer_offset < buffers[i]->used()) {
                buffer_writes.push_back(*it);
                it = pending_writes_.erase(it);
            } else {
                ++it;
            }
        }

        // Submit to IO
        io_submitter_->submit_write(buffers[i], active_file_,
                                    active_file_->write_offset,
                                    std::move(buffer_writes));

        active_file_->write_offset += buffers[i]->used();
    }
}

void Engine::on_write_complete(const PendingWrite& pw, int status) {
    if (status == 0) {
        // Update index
        update_index_on_write_complete(pw);
    }

    // Call user callback
    if (pw.callback) {
        pw.callback(pw.cb_arg, status);
    }
}

void Engine::update_index_on_write_complete(const PendingWrite& pw) {
    MemIndexEntry* existing = mem_index_->find(pw.key);

    if (existing == nullptr) {
        mem_index_->upsert(pw.key, pw.new_entry);
        return;
    }

    if (pw.is_compaction()) {
        // Compaction: only update if index still points to old location
        if (existing->file_id == pw.old_file_id &&
            existing->offset_index == pw.old_offset_index) {
            mem_index_->upsert(pw.key, pw.new_entry);
        }
    } else {
        // User write: use sequence comparison
        if (MemIndex::sequence_newer(pw.sequence, existing->sequence)) {
            mem_index_->upsert(pw.key, pw.new_entry);
        }
    }
}

FileInfo* Engine::get_file(uint16_t file_id) {
    auto it = files_.find(file_id);
    if (it != files_.end()) {
        return it->second.get();
    }
    return nullptr;
}

FileInfo* Engine::allocate_data_file() {
    auto file = std::make_unique<FileInfo>();
    file->file_id = next_file_id_++;
    file->state = FileState::ACTIVE;
    file->size = config_.data_file_size;
    file->write_offset = sizeof(DataFileHeader);
    file->entry_count = 0;
    file->valid_bytes = 0;
    file->total_bytes = 0;

    // Initialize header
    file->header.magic = DATA_FILE_MAGIC;
    file->header.version = 1;
    file->header.file_id = file->file_id;
    file->header.state = FileState::ACTIVE;
    file->header.create_time = time(nullptr);
    file->header.sealed_time = 0;
    file->header.entry_count = 0;
    file->header.valid_bytes = 0;
    file->header.total_bytes = 0;

#ifdef WITH_SPDK
    // Create blob for this file (async in real implementation)
    // For now, blob creation will happen asynchronously
    // The blob pointer will be set when creation completes
    file->blob = nullptr;
    file->blob_id = 0;

    // In a real async implementation:
    // create_data_blob(file.get(), on_blob_created, file.get());
#else
    file->blob = nullptr;
    file->blob_id = file->file_id;
#endif

    FileInfo* ptr = file.get();
    files_[file->file_id] = std::move(file);
    return ptr;
}

void Engine::remove_file_mapping(uint16_t file_id) {
    files_.erase(file_id);
}

size_t Engine::pending_foreground_count() const {
    return pending_writes_.size() + wait_queue_.size();
}

bool Engine::has_pending_foreground_requests() const {
    return !pending_writes_.empty() || !wait_queue_.empty();
}

void* Engine::alloc_buffer(size_t size) {
#ifdef WITH_SPDK
    return spdk_dma_zmalloc(size, ALIGNMENT, nullptr);
#else
    void* ptr = aligned_alloc(ALIGNMENT, size);
    if (ptr) {
        memset(ptr, 0, size);
    }
    return ptr;
#endif
}

void Engine::free_buffer(void* buf) {
    if (buf) {
#ifdef WITH_SPDK
        spdk_dma_free(buf);
#else
        free(buf);
#endif
    }
}

#ifdef WITH_SPDK

// Context for async blob operations
struct BlobCreateContext {
    Engine* engine;
    spdk_kv_cb callback;
    void* cb_arg;
    struct spdk_blob* blob;
    spdk_blob_id blob_id;
    int phase;  // Track which phase of creation we're in
};

// Callback for blob open
static void on_blob_open_complete(void* arg, struct spdk_blob* blob, int bserrno) {
    auto* ctx = static_cast<BlobCreateContext*>(arg);
    if (bserrno != 0) {
        SPDK_ERRLOG("Failed to open blob: %d\n", bserrno);
        if (ctx->callback) {
            ctx->callback(ctx->cb_arg, bserrno);
        }
        delete ctx;
        return;
    }
    ctx->blob = blob;
    // Continue with next operation
}

// Callback for blob create
static void on_blob_create_complete(void* arg, spdk_blob_id blobid, int bserrno) {
    auto* ctx = static_cast<BlobCreateContext*>(arg);
    if (bserrno != 0) {
        SPDK_ERRLOG("Failed to create blob: %d\n", bserrno);
        if (ctx->callback) {
            ctx->callback(ctx->cb_arg, bserrno);
        }
        delete ctx;
        return;
    }
    ctx->blob_id = blobid;
    // Open the created blob
    spdk_bs_open_blob(ctx->engine->blob_store(), blobid, on_blob_open_complete, ctx);
}

// Callback for superblock read (used when full blob IO is enabled)
__attribute__((unused))
static void on_superblock_read_complete(void* arg, int bserrno) {
    auto* ctx = static_cast<BlobCreateContext*>(arg);
    if (bserrno != 0) {
        SPDK_ERRLOG("Failed to read superblock: %d\n", bserrno);
        if (ctx->callback) {
            ctx->callback(ctx->cb_arg, bserrno);
        }
        delete ctx;
        return;
    }
    if (ctx->callback) {
        ctx->callback(ctx->cb_arg, 0);
    }
    delete ctx;
}

// Callback for superblock write (used when full blob IO is enabled)
__attribute__((unused))
static void on_superblock_write_complete(void* arg, int bserrno) {
    auto* ctx = static_cast<BlobCreateContext*>(arg);
    if (bserrno != 0) {
        SPDK_ERRLOG("Failed to write superblock: %d\n", bserrno);
    }
    if (ctx->callback) {
        ctx->callback(ctx->cb_arg, bserrno);
    }
    delete ctx;
}

int Engine::init_spdk_env(const char* dev_name) {
    // Get bdev
    bdev_ = spdk_bdev_get_by_name(dev_name);
    if (!bdev_) {
        return static_cast<int>(KvError::INVALID_ARGUMENT);
    }

    // Open bdev
    int rc = spdk_bdev_open_ext(dev_name, true, nullptr, nullptr, &bdev_desc_);
    if (rc != 0) {
        return static_cast<int>(KvError::IO_ERROR);
    }

    // Get IO channel
    io_channel_ = spdk_bdev_get_io_channel(bdev_desc_);
    if (!io_channel_) {
        spdk_bdev_close(bdev_desc_);
        return static_cast<int>(KvError::IO_ERROR);
    }

    // Create bs_dev from bdev for blob store
    rc = spdk_bdev_create_bs_dev_ext(dev_name, nullptr, nullptr, &bs_dev_);
    if (rc != 0) {
        spdk_put_io_channel(io_channel_);
        spdk_bdev_close(bdev_desc_);
        return static_cast<int>(KvError::IO_ERROR);
    }

    return 0;
}

// Callback for getting super blob ID
static void on_get_super_complete(void* arg, spdk_blob_id blobid, int bserrno) {
    auto* ctx = static_cast<BlobCreateContext*>(arg);
    if (bserrno != 0) {
        SPDK_ERRLOG("Failed to get super blob: %d\n", bserrno);
        if (ctx->callback) {
            ctx->callback(ctx->cb_arg, bserrno);
        }
        delete ctx;
        return;
    }
    ctx->blob_id = blobid;
    // Continue with opening the superblock blob
    spdk_bs_open_blob(ctx->engine->blob_store(), blobid, on_blob_open_complete, ctx);
}

int Engine::load_superblock() {
    if (!blob_store_ || !io_channel_) {
        return static_cast<int>(KvError::ENGINE_NOT_READY);
    }

    // Get super blob ID asynchronously
    auto* ctx = new BlobCreateContext{this, pending_callback_, pending_cb_arg_, nullptr, 0, 0};
    spdk_bs_get_super(blob_store_, on_get_super_complete, ctx);

    // Operation is async, return success and wait for callback
    return 0;
}

int Engine::save_superblock() {
    if (!blob_store_ || !io_channel_) {
        return static_cast<int>(KvError::ENGINE_NOT_READY);
    }

    // Update superblock fields
    superblock_.sequence++;
    superblock_.total_entries = stats_.total_entries;
    superblock_.total_data_bytes = stats_.total_data_bytes;
    superblock_.total_garbage_bytes = stats_.total_garbage_bytes;
    superblock_.checksum = crc32(&superblock_, sizeof(Superblock) - sizeof(uint32_t));

    // In a real implementation, this would async write to the superblock blob
    return 0;
}

int Engine::create_initial_layout() {
    if (!bs_dev_) {
        return static_cast<int>(KvError::ENGINE_NOT_READY);
    }

    // Initialize blob store - this is async
    struct spdk_bs_opts opts;
    spdk_bs_opts_init(&opts, sizeof(opts));

    // Set cluster size (default 1MB)
    opts.cluster_sz = 1024 * 1024;

    // The blob store initialization will be async
    // spdk_bs_init(bs_dev_, &opts, on_bs_init_complete, this);

    // For now, initialize synchronously for the simulation
    // Initialize memory index
    mem_index_ = std::make_unique<MemIndex>(config_.max_entries, config_.index_load_factor);

    // Initialize buffer manager
    buffer_manager_ = std::make_unique<AppendBufferManager>(
        config_.append_buffer_size,
        config_.append_buffer_min_count,
        config_.append_buffer_max_count);

    if (!buffer_manager_->init()) {
        return static_cast<int>(KvError::INTERNAL_ERROR);
    }

    // Initialize IO submitter
    io_submitter_ = std::make_unique<IoSubmitter>(this);
    io_submitter_->init(io_channel_);

    // Initialize superblock
    superblock_.magic = SUPERBLOCK_MAGIC;
    superblock_.version = 1;
    superblock_.sequence = 0;
    superblock_.create_time = time(nullptr);
    superblock_.data_file_size = config_.data_file_size;
    superblock_.alignment_unit = ALIGNMENT;
    superblock_.file_count = 0;
    superblock_.active_file_id = 0;

    // Allocate first data file
    active_file_ = allocate_data_file();
    if (!active_file_) {
        return static_cast<int>(KvError::NO_SPACE);
    }

    state_ = EngineState::READY;

    if (pending_callback_) {
        pending_callback_(pending_cb_arg_, 0);
    }

    return 0;
}

// Helper to create a blob for a data file
void Engine::create_data_blob(FileInfo* file, spdk_kv_cb cb, void* cb_arg) {
    (void)file;  // File info will be updated in callback

    if (!blob_store_) {
        if (cb) cb(cb_arg, static_cast<int>(KvError::ENGINE_NOT_READY));
        return;
    }

    struct spdk_blob_opts opts;
    spdk_blob_opts_init(&opts, sizeof(opts));

    // Set blob size based on data file size
    uint64_t num_clusters = config_.data_file_size / (1024 * 1024);  // 1MB clusters
    opts.num_clusters = num_clusters;

    auto* ctx = new BlobCreateContext{this, cb, cb_arg, nullptr, 0, 0};
    spdk_bs_create_blob_ext(blob_store_, &opts, on_blob_create_complete, ctx);
}

#else
int Engine::init_spdk_env(const char* dev_name) {
    (void)dev_name;
    return 0;
}

int Engine::load_superblock() {
    return 0;
}

int Engine::save_superblock() {
    return 0;
}

int Engine::create_initial_layout() {
    return 0;
}
#endif

}  // namespace spdk_kv
